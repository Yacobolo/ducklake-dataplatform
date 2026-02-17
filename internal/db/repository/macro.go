package repository

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
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
	return macroFromDB(row), nil
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
	paramsJSON, err := json.Marshal(params)
	if err != nil {
		return nil, fmt.Errorf("marshal parameters: %w", err)
	}

	err = r.q.UpdateMacro(ctx, dbstore.UpdateMacroParams{
		Body:        body,
		Description: description,
		Parameters:  string(paramsJSON),
		Name:        name,
	})
	if err != nil {
		return nil, mapDBError(err)
	}
	return r.GetByName(ctx, name)
}

// Delete removes a macro by name.
func (r *MacroRepo) Delete(ctx context.Context, name string) error {
	return mapDBError(r.q.DeleteMacro(ctx, name))
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
