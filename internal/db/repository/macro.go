package repository

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"duck-demo/internal/db/dbstore"
	"duck-demo/internal/domain"
)

// Compile-time check.
var _ domain.MacroRepository = (*MacroRepo)(nil)

// MacroRepo implements MacroRepository using SQLite.
type MacroRepo struct {
	q  *dbstore.Queries
	db *sql.DB
}

// NewMacroRepo creates a new MacroRepo.
func NewMacroRepo(db *sql.DB) *MacroRepo {
	return &MacroRepo{q: dbstore.New(db), db: db}
}

// Create inserts a new SQL macro.
func (r *MacroRepo) Create(ctx context.Context, m *domain.Macro) (*domain.Macro, error) {
	paramsJSON, err := json.Marshal(m.Parameters)
	if err != nil {
		return nil, fmt.Errorf("marshal parameters: %w", err)
	}

	row, err := r.q.CreateMacro(ctx, dbstore.CreateMacroParams{
		ID:          newID(),
		Name:        m.Name,
		MacroType:   m.MacroType,
		Parameters:  string(paramsJSON),
		Body:        m.Body,
		Description: m.Description,
		CatalogName: m.CatalogName,
		ProjectName: m.ProjectName,
		Visibility:  m.Visibility,
		Owner:       m.Owner,
		Properties:  mustJSONString(m.Properties),
		Tags:        mustJSONArray(m.Tags),
		Status:      m.Status,
		CreatedBy:   m.CreatedBy,
	})
	if err != nil {
		return nil, mapDBError(err)
	}
	created := macroFromDB(row)
	if err := r.upsertCatalogMetadata(ctx, created); err != nil {
		return nil, err
	}
	if _, err := r.createRevision(ctx, created, created.CreatedBy); err != nil {
		return nil, err
	}
	return created, nil
}

// GetByName returns a macro by name.
func (r *MacroRepo) GetByName(ctx context.Context, name string) (*domain.Macro, error) {
	row, err := r.q.GetMacroByName(ctx, name)
	if err != nil {
		return nil, mapDBError(err)
	}
	return macroFromDB(row), nil
}

// List returns a paginated list of macros.
func (r *MacroRepo) List(ctx context.Context, page domain.PageRequest) ([]domain.Macro, int64, error) {
	total, err := r.q.CountMacros(ctx)
	if err != nil {
		return nil, 0, err
	}

	rows, err := r.q.ListMacros(ctx, dbstore.ListMacrosParams{
		Limit:  int64(page.Limit()),
		Offset: int64(page.Offset()),
	})
	if err != nil {
		return nil, 0, err
	}

	macros := make([]domain.Macro, 0, len(rows))
	for _, row := range rows {
		macros = append(macros, *macroFromDB(row))
	}
	return macros, total, nil
}

// Update applies partial updates to a macro using read-modify-write.
func (r *MacroRepo) Update(ctx context.Context, name string, req domain.UpdateMacroRequest) (*domain.Macro, error) {
	current, err := r.GetByName(ctx, name)
	if err != nil {
		return nil, err
	}

	body := current.Body
	if req.Body != nil {
		body = *req.Body
	}
	description := current.Description
	if req.Description != nil {
		description = *req.Description
	}
	params := current.Parameters
	if req.Parameters != nil {
		params = req.Parameters
	}
	status := current.Status
	if req.Status != nil {
		status = *req.Status
	}
	catalogName := current.CatalogName
	if req.CatalogName != nil {
		catalogName = *req.CatalogName
	}
	projectName := current.ProjectName
	if req.ProjectName != nil {
		projectName = *req.ProjectName
	}
	visibility := current.Visibility
	if req.Visibility != nil {
		visibility = *req.Visibility
	}
	owner := current.Owner
	if req.Owner != nil {
		owner = *req.Owner
	}
	properties := current.Properties
	if req.Properties != nil {
		properties = req.Properties
	}
	tags := current.Tags
	if req.Tags != nil {
		tags = req.Tags
	}
	paramsJSON, err := json.Marshal(params)
	if err != nil {
		return nil, fmt.Errorf("marshal parameters: %w", err)
	}
	changedDefinition := body != current.Body ||
		description != current.Description ||
		!equalStrings(params, current.Parameters) ||
		status != current.Status

	err = r.q.UpdateMacro(ctx, dbstore.UpdateMacroParams{
		Body:        body,
		Description: description,
		Parameters:  string(paramsJSON),
		Status:      status,
		CatalogName: catalogName,
		ProjectName: projectName,
		Visibility:  visibility,
		Owner:       owner,
		Properties:  mustJSONString(properties),
		Tags:        mustJSONArray(tags),
		Name:        name,
	})
	if err != nil {
		return nil, mapDBError(err)
	}
	updated, err := r.GetByName(ctx, name)
	if err != nil {
		return nil, err
	}
	if err := r.upsertCatalogMetadata(ctx, updated); err != nil {
		return nil, err
	}
	if changedDefinition {
		if _, err := r.createRevision(ctx, updated, updated.CreatedBy); err != nil {
			return nil, err
		}
	}
	return updated, nil
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// Delete removes a macro by name.
func (r *MacroRepo) Delete(ctx context.Context, name string) error {
	if err := mapDBError(r.q.DeleteMacro(ctx, name)); err != nil {
		return err
	}
	if err := r.q.SoftDeleteCatalogMetadata(ctx, dbstore.SoftDeleteCatalogMetadataParams{
		SecurableType: "macro",
		SecurableName: macroCatalogSecurableName(name),
	}); err != nil {
		return fmt.Errorf("soft delete macro catalog metadata: %w", err)
	}
	return nil
}

// ListAll returns all macros ordered by name.
func (r *MacroRepo) ListAll(ctx context.Context) ([]domain.Macro, error) {
	rows, err := r.q.ListAllMacros(ctx)
	if err != nil {
		return nil, err
	}

	macros := make([]domain.Macro, 0, len(rows))
	for _, row := range rows {
		macros = append(macros, *macroFromDB(row))
	}
	return macros, nil
}

// ListRevisions returns macro revisions ordered by descending version.
func (r *MacroRepo) ListRevisions(ctx context.Context, macroName string) ([]domain.MacroRevision, error) {
	rows, err := r.q.ListMacroRevisions(ctx, macroName)
	if err != nil {
		return nil, mapDBError(err)
	}
	out := make([]domain.MacroRevision, 0, len(rows))
	for _, row := range rows {
		out = append(out, *macroRevisionFromDB(row))
	}
	return out, nil
}

// GetRevisionByVersion returns a specific revision version.
func (r *MacroRepo) GetRevisionByVersion(ctx context.Context, macroName string, version int) (*domain.MacroRevision, error) {
	row, err := r.q.GetMacroRevisionByVersion(ctx, dbstore.GetMacroRevisionByVersionParams{
		MacroName: macroName,
		Version:   int64(version),
	})
	if err != nil {
		return nil, mapDBError(err)
	}
	return macroRevisionFromDB(row), nil
}

// === Private mappers ===

func macroFromDB(row dbstore.Macro) *domain.Macro {
	createdAt, err := time.Parse("2006-01-02 15:04:05", row.CreatedAt)
	if err != nil {
		slog.Default().Warn("failed to parse macro created_at", "value", row.CreatedAt, "error", err)
	}
	updatedAt, err := time.Parse("2006-01-02 15:04:05", row.UpdatedAt)
	if err != nil {
		slog.Default().Warn("failed to parse macro updated_at", "value", row.UpdatedAt, "error", err)
	}

	var params []string
	_ = json.Unmarshal([]byte(row.Parameters), &params)
	if params == nil {
		params = []string{}
	}

	var props map[string]string
	_ = json.Unmarshal([]byte(row.Properties), &props)
	if props == nil {
		props = map[string]string{}
	}

	var tags []string
	_ = json.Unmarshal([]byte(row.Tags), &tags)
	if tags == nil {
		tags = []string{}
	}

	return &domain.Macro{
		ID:          row.ID,
		Name:        row.Name,
		MacroType:   row.MacroType,
		Parameters:  params,
		Body:        row.Body,
		Description: row.Description,
		CatalogName: row.CatalogName,
		ProjectName: row.ProjectName,
		Visibility:  row.Visibility,
		Owner:       row.Owner,
		Properties:  props,
		Tags:        tags,
		Status:      row.Status,
		CreatedBy:   row.CreatedBy,
		CreatedAt:   createdAt,
		UpdatedAt:   updatedAt,
	}
}

func mustJSONString(m map[string]string) string {
	if m == nil {
		return "{}"
	}
	b, err := json.Marshal(m)
	if err != nil {
		return "{}"
	}
	return string(b)
}

func mustJSONArray(v []string) string {
	if v == nil {
		return "[]"
	}
	b, err := json.Marshal(v)
	if err != nil {
		return "[]"
	}
	return string(b)
}

func (r *MacroRepo) upsertCatalogMetadata(ctx context.Context, macro *domain.Macro) error {
	if macro == nil {
		return nil
	}
	if err := r.q.UpsertCatalogMetadata(ctx, dbstore.UpsertCatalogMetadataParams{
		SecurableType: "macro",
		SecurableName: macroCatalogSecurableName(macro.Name),
		Comment:       sql.NullString{String: macro.Description, Valid: true},
		Properties:    sql.NullString{String: mustJSONString(macro.Properties), Valid: true},
		Owner:         sql.NullString{String: macro.Owner, Valid: true},
	}); err != nil {
		return fmt.Errorf("upsert macro catalog metadata: %w", err)
	}
	return nil
}

func macroCatalogSecurableName(name string) string {
	return "macro." + strings.TrimSpace(name)
}

func (r *MacroRepo) createRevision(ctx context.Context, m *domain.Macro, createdBy string) (*domain.MacroRevision, error) {
	version, err := r.q.GetLatestMacroRevisionVersion(ctx, m.ID)
	if err != nil {
		return nil, mapDBError(err)
	}
	payload := struct {
		Parameters  []string `json:"parameters"`
		Body        string   `json:"body"`
		Description string   `json:"description"`
		Status      string   `json:"status"`
	}{
		Parameters:  m.Parameters,
		Body:        m.Body,
		Description: m.Description,
		Status:      m.Status,
	}
	b, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("marshal macro revision payload: %w", err)
	}
	contentHash := fmt.Sprintf("sha256:%x", sha256.Sum256(b))

	row, err := r.q.CreateMacroRevision(ctx, dbstore.CreateMacroRevisionParams{
		ID:          newID(),
		MacroID:     m.ID,
		MacroName:   m.Name,
		Version:     version + 1,
		ContentHash: contentHash,
		Parameters:  mustJSONArray(m.Parameters),
		Body:        m.Body,
		Description: m.Description,
		Status:      m.Status,
		CreatedBy:   createdBy,
	})
	if err != nil {
		return nil, mapDBError(err)
	}
	return macroRevisionFromDB(row), nil
}

func macroRevisionFromDB(row dbstore.MacroRevision) *domain.MacroRevision {
	createdAt, err := time.Parse("2006-01-02 15:04:05", row.CreatedAt)
	if err != nil {
		slog.Default().Warn("failed to parse macro revision created_at", "value", row.CreatedAt, "error", err)
	}

	var params []string
	_ = json.Unmarshal([]byte(row.Parameters), &params)
	if params == nil {
		params = []string{}
	}

	return &domain.MacroRevision{
		ID:          row.ID,
		MacroID:     row.MacroID,
		MacroName:   row.MacroName,
		Version:     int(row.Version),
		ContentHash: row.ContentHash,
		Parameters:  params,
		Body:        row.Body,
		Description: row.Description,
		Status:      row.Status,
		CreatedBy:   row.CreatedBy,
		CreatedAt:   createdAt,
	}
}
