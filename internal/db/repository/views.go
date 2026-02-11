package repository

import (
	"context"
	"database/sql"
	"encoding/json"

	dbstore "duck-demo/internal/db/dbstore"
	"duck-demo/internal/db/mapper"
	"duck-demo/internal/domain"
)

type ViewRepo struct {
	q  *dbstore.Queries
	db *sql.DB
}

func NewViewRepo(db *sql.DB) *ViewRepo {
	return &ViewRepo{q: dbstore.New(db), db: db}
}

func (r *ViewRepo) Create(ctx context.Context, view *domain.ViewDetail) (*domain.ViewDetail, error) {
	propsJSON, _ := json.Marshal(view.Properties)
	sourcesJSON, _ := json.Marshal(view.SourceTables)

	row, err := r.q.CreateView(ctx, dbstore.CreateViewParams{
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

func (r *ViewRepo) GetByName(ctx context.Context, schemaID int64, viewName string) (*domain.ViewDetail, error) {
	row, err := r.q.GetViewByName(ctx, dbstore.GetViewByNameParams{
		SchemaID: schemaID,
		Name:     viewName,
	})
	if err != nil {
		return nil, mapDBError(err)
	}
	return mapper.ViewFromDB(row), nil
}

func (r *ViewRepo) List(ctx context.Context, schemaID int64, page domain.PageRequest) ([]domain.ViewDetail, int64, error) {
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

func (r *ViewRepo) Delete(ctx context.Context, schemaID int64, viewName string) error {
	return r.q.DeleteView(ctx, dbstore.DeleteViewParams{
		SchemaID: schemaID,
		Name:     viewName,
	})
}

func stringFromPtr(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}
