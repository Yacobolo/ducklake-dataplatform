package repository

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	dbstore "duck-demo/internal/db/dbstore"
	"duck-demo/internal/domain"
)

// ExternalTableRepo implements domain.ExternalTableRepository using the
// application-owned SQLite tables for external table metadata.
type ExternalTableRepo struct {
	db *sql.DB
	q  *dbstore.Queries
}

// NewExternalTableRepo creates a new ExternalTableRepo.
func NewExternalTableRepo(db *sql.DB) *ExternalTableRepo {
	return &ExternalTableRepo{db: db, q: dbstore.New(db)}
}

var _ domain.ExternalTableRepository = (*ExternalTableRepo)(nil)

// Create inserts a new external table record and its columns.
func (r *ExternalTableRepo) Create(ctx context.Context, et *domain.ExternalTableRecord) (*domain.ExternalTableRecord, error) {
	catalogName := et.CatalogName
	if catalogName == "" {
		catalogName = "lake"
	}
	row, err := r.q.CreateExternalTable(ctx, dbstore.CreateExternalTableParams{
		ID:           newID(),
		SchemaName:   et.SchemaName,
		TableName:    et.TableName,
		FileFormat:   et.FileFormat,
		SourcePath:   et.SourcePath,
		LocationName: et.LocationName,
		Comment:      et.Comment,
		Owner:        et.Owner,
		CatalogName:  catalogName,
	})
	if err != nil {
		return nil, mapDBError(err)
	}

	// Insert columns
	for _, col := range et.Columns {
		if err := r.q.InsertExternalTableColumn(ctx, dbstore.InsertExternalTableColumnParams{
			ID:              newID(),
			ExternalTableID: row.ID,
			ColumnName:      col.ColumnName,
			ColumnType:      col.ColumnType,
			Position:        int64(col.Position),
		}); err != nil {
			return nil, fmt.Errorf("insert column %q: %w", col.ColumnName, err)
		}
	}

	return r.GetByID(ctx, row.ID)
}

// GetByName retrieves an external table by schema and table name.
func (r *ExternalTableRepo) GetByName(ctx context.Context, schemaName, tableName string) (*domain.ExternalTableRecord, error) {
	row, err := r.q.GetExternalTableByName(ctx, dbstore.GetExternalTableByNameParams{
		SchemaName: schemaName,
		TableName:  tableName,
	})
	if err != nil {
		return nil, mapDBError(err)
	}
	et := r.toDomain(row)
	cols, err := r.loadColumns(ctx, et.ID)
	if err != nil {
		return nil, err
	}
	et.Columns = cols
	return et, nil
}

// GetByID retrieves an external table by its raw SQLite ID.
func (r *ExternalTableRepo) GetByID(ctx context.Context, id string) (*domain.ExternalTableRecord, error) {
	row, err := r.q.GetExternalTableByID(ctx, id)
	if err != nil {
		return nil, mapDBError(err)
	}
	et := r.toDomain(row)
	cols, err := r.loadColumns(ctx, et.ID)
	if err != nil {
		return nil, err
	}
	et.Columns = cols
	return et, nil
}

// GetByTableName retrieves an external table by table name only (for engine lookup).
func (r *ExternalTableRepo) GetByTableName(ctx context.Context, tableName string) (*domain.ExternalTableRecord, error) {
	row, err := r.q.GetExternalTableByTableName(ctx, tableName)
	if err != nil {
		return nil, mapDBError(err)
	}
	et := r.toDomain(row)
	cols, err := r.loadColumns(ctx, et.ID)
	if err != nil {
		return nil, err
	}
	et.Columns = cols
	return et, nil
}

// List returns a paginated list of external tables in a schema.
func (r *ExternalTableRepo) List(ctx context.Context, schemaName string, page domain.PageRequest) ([]domain.ExternalTableRecord, int64, error) {
	total, err := r.q.CountExternalTables(ctx, schemaName)
	if err != nil {
		return nil, 0, err
	}

	rows, err := r.q.ListExternalTables(ctx, dbstore.ListExternalTablesParams{
		SchemaName: schemaName,
		Limit:      int64(page.Limit()),
		Offset:     int64(page.Offset()),
	})
	if err != nil {
		return nil, 0, err
	}

	result := make([]domain.ExternalTableRecord, 0, len(rows))
	for _, row := range rows {
		et := r.toDomain(row)
		cols, _ := r.loadColumns(ctx, et.ID)
		et.Columns = cols
		result = append(result, *et)
	}

	return result, total, nil
}

// ListAll returns all non-deleted external tables (for startup VIEW restore).
func (r *ExternalTableRepo) ListAll(ctx context.Context) ([]domain.ExternalTableRecord, error) {
	rows, err := r.q.ListAllExternalTables(ctx)
	if err != nil {
		return nil, err
	}

	result := make([]domain.ExternalTableRecord, 0, len(rows))
	for _, row := range rows {
		et := r.toDomain(row)
		cols, _ := r.loadColumns(ctx, et.ID)
		et.Columns = cols
		result = append(result, *et)
	}

	return result, nil
}

// Delete soft-deletes an external table by schema and table name.
func (r *ExternalTableRepo) Delete(ctx context.Context, schemaName, tableName string) error {
	return r.q.SoftDeleteExternalTable(ctx, dbstore.SoftDeleteExternalTableParams{
		SchemaName: schemaName,
		TableName:  tableName,
	})
}

// DeleteBySchema soft-deletes all external tables in a schema.
func (r *ExternalTableRepo) DeleteBySchema(ctx context.Context, schemaName string) error {
	return r.q.SoftDeleteExternalTablesBySchema(ctx, schemaName)
}

// --- helpers ---

func (r *ExternalTableRepo) loadColumns(ctx context.Context, externalTableID string) ([]domain.ExternalTableColumn, error) {
	rows, err := r.q.ListExternalTableColumns(ctx, externalTableID)
	if err != nil {
		return nil, err
	}
	cols := make([]domain.ExternalTableColumn, len(rows))
	for i, row := range rows {
		cols[i] = domain.ExternalTableColumn{
			ID:              row.ID,
			ExternalTableID: row.ExternalTableID,
			ColumnName:      row.ColumnName,
			ColumnType:      row.ColumnType,
			Position:        int(row.Position),
		}
	}
	return cols, nil
}

func (r *ExternalTableRepo) toDomain(row dbstore.ExternalTable) *domain.ExternalTableRecord {
	et := &domain.ExternalTableRecord{
		ID:           row.ID,
		CatalogName:  row.CatalogName,
		SchemaName:   row.SchemaName,
		TableName:    row.TableName,
		FileFormat:   row.FileFormat,
		SourcePath:   row.SourcePath,
		LocationName: row.LocationName,
		Comment:      row.Comment,
		Owner:        row.Owner,
	}
	et.CreatedAt, _ = time.Parse("2006-01-02 15:04:05", row.CreatedAt)
	et.UpdatedAt, _ = time.Parse("2006-01-02 15:04:05", row.UpdatedAt)
	if row.DeletedAt.Valid {
		t, _ := time.Parse("2006-01-02 15:04:05", row.DeletedAt.String)
		et.DeletedAt = &t
	}
	return et
}
