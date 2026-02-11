package api

import (
	"context"
	"regexp"
	"sort"
	"sync"
	"time"

	"duck-demo/internal/domain"
)

// mockCatalogRepo implements domain.CatalogRepository using in-memory state.
// Used in handler tests to avoid requiring a real DuckLake-attached DuckDB.
type mockCatalogRepo struct {
	mu       sync.Mutex
	schemas  map[string]domain.SchemaDetail   // keyed by schema name
	tables   map[string]domain.TableDetail    // keyed by "schema.table"
	columns  map[string][]domain.ColumnDetail // keyed by "schema.table"
	metadata map[string]map[string]string     // keyed by "type:name" -> properties
	nextID   int64
}

func newMockCatalogRepo() *mockCatalogRepo {
	return &mockCatalogRepo{
		schemas:  make(map[string]domain.SchemaDetail),
		tables:   make(map[string]domain.TableDetail),
		columns:  make(map[string][]domain.ColumnDetail),
		metadata: make(map[string]map[string]string),
		nextID:   1,
	}
}

var mockIdentifierRe = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)

func (m *mockCatalogRepo) validateIdentifier(name string) error {
	if name == "" {
		return domain.ErrValidation("name is required")
	}
	if len(name) > 128 {
		return domain.ErrValidation("name must be at most 128 characters")
	}
	if !mockIdentifierRe.MatchString(name) {
		return domain.ErrValidation("name must match [a-zA-Z_][a-zA-Z0-9_]*")
	}
	return nil
}

func (m *mockCatalogRepo) GetCatalogInfo(_ context.Context) (*domain.CatalogInfo, error) {
	return &domain.CatalogInfo{
		Name:      "lake",
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}, nil
}

func (m *mockCatalogRepo) GetMetastoreSummary(_ context.Context) (*domain.MetastoreSummary, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	return &domain.MetastoreSummary{
		CatalogName:    "lake",
		MetastoreType:  "DuckLake (SQLite)",
		StorageBackend: "S3",
		SchemaCount:    int64(len(m.schemas)),
		TableCount:     int64(len(m.tables)),
	}, nil
}

func (m *mockCatalogRepo) CreateSchema(_ context.Context, name, comment, owner string) (*domain.SchemaDetail, error) {
	if err := m.validateIdentifier(name); err != nil {
		return nil, err
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.schemas[name]; exists {
		return nil, domain.ErrConflict("schema %q already exists", name)
	}

	now := time.Now()
	s := domain.SchemaDetail{
		SchemaID:    m.nextID,
		Name:        name,
		CatalogName: "lake",
		Comment:     comment,
		Owner:       owner,
		CreatedAt:   now,
		UpdatedAt:   now,
	}
	m.nextID++
	m.schemas[name] = s
	return &s, nil
}

func (m *mockCatalogRepo) GetSchema(_ context.Context, name string) (*domain.SchemaDetail, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	s, exists := m.schemas[name]
	if !exists {
		return nil, domain.ErrNotFound("schema %q not found", name)
	}
	return &s, nil
}

func (m *mockCatalogRepo) ListSchemas(_ context.Context, page domain.PageRequest) ([]domain.SchemaDetail, int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Collect and sort by name
	names := make([]string, 0, len(m.schemas))
	for n := range m.schemas {
		names = append(names, n)
	}
	sort.Strings(names)

	total := int64(len(names))
	offset := page.Offset()
	limit := page.Limit()

	if offset >= len(names) {
		return []domain.SchemaDetail{}, total, nil
	}

	end := offset + limit
	if end > len(names) {
		end = len(names)
	}

	result := make([]domain.SchemaDetail, 0, end-offset)
	for _, n := range names[offset:end] {
		result = append(result, m.schemas[n])
	}
	return result, total, nil
}

func (m *mockCatalogRepo) UpdateSchema(_ context.Context, name string, comment *string, props map[string]string) (*domain.SchemaDetail, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	s, exists := m.schemas[name]
	if !exists {
		return nil, domain.ErrNotFound("schema %q not found", name)
	}

	if comment != nil {
		s.Comment = *comment
	}
	if props != nil {
		s.Properties = props
	}
	s.UpdatedAt = time.Now()
	m.schemas[name] = s
	return &s, nil
}

func (m *mockCatalogRepo) DeleteSchema(_ context.Context, name string, force bool) error {
	if err := m.validateIdentifier(name); err != nil {
		return err
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.schemas[name]; !exists {
		return domain.ErrNotFound("schema %q not found", name)
	}

	// Check if schema has tables
	if !force {
		for key := range m.tables {
			// key is "schema.table"
			if len(key) > len(name) && key[:len(name)+1] == name+"." {
				return domain.ErrConflict("schema %q is not empty; use force=true to cascade delete", name)
			}
		}
	}

	// Remove tables in this schema
	for key := range m.tables {
		if len(key) > len(name) && key[:len(name)+1] == name+"." {
			delete(m.tables, key)
			delete(m.columns, key)
		}
	}

	delete(m.schemas, name)
	return nil
}

func (m *mockCatalogRepo) CreateTable(_ context.Context, schemaName string, req domain.CreateTableRequest, owner string) (*domain.TableDetail, error) {
	if err := m.validateIdentifier(schemaName); err != nil {
		return nil, err
	}
	if err := m.validateIdentifier(req.Name); err != nil {
		return nil, err
	}
	if len(req.Columns) == 0 {
		return nil, domain.ErrValidation("at least one column is required")
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.schemas[schemaName]; !exists {
		return nil, domain.ErrNotFound("schema %q not found", schemaName)
	}

	key := schemaName + "." + req.Name
	if _, exists := m.tables[key]; exists {
		return nil, domain.ErrConflict("table %q already exists in schema %q", req.Name, schemaName)
	}

	cols := make([]domain.ColumnDetail, len(req.Columns))
	for i, c := range req.Columns {
		cols[i] = domain.ColumnDetail{
			Name:     c.Name,
			Type:     c.Type,
			Position: i,
		}
	}

	now := time.Now()
	t := domain.TableDetail{
		TableID:     m.nextID,
		Name:        req.Name,
		SchemaName:  schemaName,
		CatalogName: "lake",
		TableType:   "MANAGED",
		Columns:     cols,
		Comment:     req.Comment,
		Owner:       owner,
		CreatedAt:   now,
		UpdatedAt:   now,
	}
	m.nextID++
	m.tables[key] = t
	m.columns[key] = cols
	return &t, nil
}

func (m *mockCatalogRepo) GetTable(_ context.Context, schemaName, tableName string) (*domain.TableDetail, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.schemas[schemaName]; !exists {
		return nil, domain.ErrNotFound("schema %q not found", schemaName)
	}

	key := schemaName + "." + tableName
	t, exists := m.tables[key]
	if !exists {
		return nil, domain.ErrNotFound("table %q not found in schema %q", tableName, schemaName)
	}
	t.Columns = m.columns[key]
	return &t, nil
}

func (m *mockCatalogRepo) ListTables(_ context.Context, schemaName string, page domain.PageRequest) ([]domain.TableDetail, int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.schemas[schemaName]; !exists {
		return nil, 0, domain.ErrNotFound("schema %q not found", schemaName)
	}

	prefix := schemaName + "."
	var keys []string
	for key := range m.tables {
		if len(key) > len(prefix) && key[:len(prefix)] == prefix {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)

	total := int64(len(keys))
	offset := page.Offset()
	limit := page.Limit()

	if offset >= len(keys) {
		return []domain.TableDetail{}, total, nil
	}

	end := offset + limit
	if end > len(keys) {
		end = len(keys)
	}

	result := make([]domain.TableDetail, 0, end-offset)
	for _, k := range keys[offset:end] {
		result = append(result, m.tables[k])
	}
	return result, total, nil
}

func (m *mockCatalogRepo) DeleteTable(_ context.Context, schemaName, tableName string) error {
	if err := m.validateIdentifier(schemaName); err != nil {
		return err
	}
	if err := m.validateIdentifier(tableName); err != nil {
		return err
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.schemas[schemaName]; !exists {
		return domain.ErrNotFound("schema %q not found", schemaName)
	}

	key := schemaName + "." + tableName
	if _, exists := m.tables[key]; !exists {
		return domain.ErrNotFound("table %q not found in schema %q", tableName, schemaName)
	}

	delete(m.tables, key)
	delete(m.columns, key)
	return nil
}

func (m *mockCatalogRepo) ListColumns(_ context.Context, schemaName, tableName string, page domain.PageRequest) ([]domain.ColumnDetail, int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.schemas[schemaName]; !exists {
		return nil, 0, domain.ErrNotFound("schema %q not found", schemaName)
	}

	key := schemaName + "." + tableName
	if _, exists := m.tables[key]; !exists {
		return nil, 0, domain.ErrNotFound("table %q not found in schema %q", tableName, schemaName)
	}

	cols := m.columns[key]
	total := int64(len(cols))
	offset := page.Offset()
	limit := page.Limit()

	if offset >= len(cols) {
		return []domain.ColumnDetail{}, total, nil
	}

	end := offset + limit
	if end > len(cols) {
		end = len(cols)
	}

	return cols[offset:end], total, nil
}

func (m *mockCatalogRepo) UpdateTable(_ context.Context, _, _ string, _ *string, _ map[string]string, _ *string) (*domain.TableDetail, error) {
	panic("unexpected call to mockCatalogRepo.UpdateTable")
}

func (m *mockCatalogRepo) UpdateCatalog(_ context.Context, _ *string) (*domain.CatalogInfo, error) {
	panic("unexpected call to mockCatalogRepo.UpdateCatalog")
}

func (m *mockCatalogRepo) UpdateColumn(_ context.Context, _, _, _ string, _ *string, _ map[string]string) (*domain.ColumnDetail, error) {
	panic("unexpected call to mockCatalogRepo.UpdateColumn")
}

// addSchema is a test helper to prepopulate the mock with a schema.
func (m *mockCatalogRepo) addSchema(name string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := time.Now()
	m.schemas[name] = domain.SchemaDetail{
		SchemaID:    m.nextID,
		Name:        name,
		CatalogName: "lake",
		CreatedAt:   now,
		UpdatedAt:   now,
	}
	m.nextID++
}

// addTable is a test helper to prepopulate the mock with a table.
func (m *mockCatalogRepo) addTable(schemaName, tableName string, cols []domain.ColumnDetail) {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := schemaName + "." + tableName
	now := time.Now()
	m.tables[key] = domain.TableDetail{
		TableID:     m.nextID,
		Name:        tableName,
		SchemaName:  schemaName,
		CatalogName: "lake",
		TableType:   "MANAGED",
		Columns:     cols,
		CreatedAt:   now,
		UpdatedAt:   now,
	}
	m.columns[key] = cols
	m.nextID++
}
