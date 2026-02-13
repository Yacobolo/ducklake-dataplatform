package repository

import (
	"context"
	"database/sql"
	"encoding/json"

	dbstore "duck-demo/internal/db/dbstore"
	"duck-demo/internal/db/mapper"
	"duck-demo/internal/domain"
)

// ViewRepo implements domain.ViewRepository using SQLite.
type ViewRepo struct {
	q *dbstore.Queries
}

// NewViewRepo creates a new ViewRepo.
func NewViewRepo(db *sql.DB) *ViewRepo {
	return &ViewRepo{q: dbstore.New(db)}
}

// Create inserts a new view into the database.
func (r *ViewRepo) Create(ctx context.Context, view *domain.ViewDetail) (*domain.ViewDetail, error) {
	propsJSON, _ := json.Marshal(view.Properties)
	sourcesJSON, _ := json.Marshal(view.SourceTables)

	row, err := r.q.CreateView(ctx, dbstore.CreateViewParams{
		ID:             newID(),
		SchemaID:       view.SchemaID,
		Name:           view.Name,
		ViewDefinition: view.ViewDefinition,
		Comment:        sql.NullString{String: stringFromPtr(view.Comment), Valid: view.Comment != nil},
		Properties:     sql.NullString{String: string(propsJSON), Valid: true},
		Owner:          view.Owner,
		SourceTables:   sql.NullString{String: string(sourcesJSON), Valid: true},
	})
	if err != nil {
		return nil, mapDBError(err)
	}
	return mapper.ViewFromDB(row), nil
}

// GetByName returns a view by schema ID and name.
func (r *ViewRepo) GetByName(ctx context.Context, schemaID string, viewName string) (*domain.ViewDetail, error) {
	row, err := r.q.GetViewByName(ctx, dbstore.GetViewByNameParams{
		SchemaID: schemaID,
		Name:     viewName,
	})
	if err != nil {
		return nil, mapDBError(err)
	}
	return mapper.ViewFromDB(row), nil
}

// List returns a paginated list of views in a schema.
func (r *ViewRepo) List(ctx context.Context, schemaID string, page domain.PageRequest) ([]domain.ViewDetail, int64, error) {
	total, err := r.q.CountViews(ctx, schemaID)
	if err != nil {
		return nil, 0, err
	}
	rows, err := r.q.ListViews(ctx, dbstore.ListViewsParams{
		SchemaID: schemaID,
		Limit:    int64(page.Limit()),
		Offset:   int64(page.Offset()),
	})
	if err != nil {
		return nil, 0, err
	}
	views := make([]domain.ViewDetail, len(rows))
	for i, row := range rows {
		views[i] = *mapper.ViewFromDB(row)
	}
	return views, total, nil
}

// Delete removes a view by schema ID and name.
func (r *ViewRepo) Delete(ctx context.Context, schemaID string, viewName string) error {
	return r.q.DeleteView(ctx, dbstore.DeleteViewParams{
		SchemaID: schemaID,
		Name:     viewName,
	})
}

// Update applies partial updates to a view's metadata and definition.
func (r *ViewRepo) Update(ctx context.Context, schemaID string, viewName string, comment *string, props map[string]string, viewDef *string) (*domain.ViewDetail, error) {
	// Verify view exists
	existing, err := r.GetByName(ctx, schemaID, viewName)
	if err != nil {
		return nil, err
	}

	// Build partial update
	newComment := existing.Comment
	if comment != nil {
		newComment = comment
	}

	newProps := existing.Properties
	if props != nil {
		newProps = props
	}
	propsJSON, _ := json.Marshal(newProps)

	newViewDef := existing.ViewDefinition
	if viewDef != nil {
		newViewDef = *viewDef
	}

	// Also re-extract source tables if view definition changed
	newSourceTables := existing.SourceTables
	sourcesJSON, _ := json.Marshal(newSourceTables)

	err = r.q.UpdateView(ctx, dbstore.UpdateViewParams{
		Comment:        sql.NullString{String: stringFromPtr(newComment), Valid: newComment != nil},
		Properties:     sql.NullString{String: string(propsJSON), Valid: true},
		ViewDefinition: newViewDef,
		SourceTables:   sql.NullString{String: string(sourcesJSON), Valid: true},
		SchemaID:       schemaID,
		Name:           viewName,
	})
	if err != nil {
		return nil, mapDBError(err)
	}

	return r.GetByName(ctx, schemaID, viewName)
}

func stringFromPtr(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}
