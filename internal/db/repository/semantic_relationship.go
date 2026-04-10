package repository

import (
	"context"
	"database/sql"

	"duck-demo/internal/db/dbstore"
	"duck-demo/internal/domain"
)

// Compile-time check.
var _ domain.SemanticRelationshipRepository = (*SemanticRelationshipRepo)(nil)

// SemanticRelationshipRepo implements SemanticRelationshipRepository using SQLite.
type SemanticRelationshipRepo struct {
	db *sql.DB
	q  *dbstore.Queries
}

// NewSemanticRelationshipRepo creates a new SemanticRelationshipRepo.
func NewSemanticRelationshipRepo(db *sql.DB) *SemanticRelationshipRepo {
	return &SemanticRelationshipRepo{db: db, q: dbstore.New(db)}
}

// Create inserts a new semantic relationship.
func (r *SemanticRelationshipRepo) Create(ctx context.Context, rel *domain.SemanticRelationship) (*domain.SemanticRelationship, error) {
	row, err := r.q.CreateSemanticRelationship(ctx, dbstore.CreateSemanticRelationshipParams{
		ID:               newID(),
		Name:             rel.Name,
		FromSemanticID:   rel.FromSemanticID,
		ToSemanticID:     rel.ToSemanticID,
		RelationshipType: rel.RelationshipType,
		JoinSql:          rel.JoinSQL,
		Cost:             int64(rel.Cost),
		MaxHops:          int64(rel.MaxHops),
		CreatedBy:        rel.CreatedBy,
	})
	if err != nil {
		return nil, mapDBError(err)
	}
	return semanticRelationshipFromDB(row), nil
}

// GetByID returns a semantic relationship by ID.
func (r *SemanticRelationshipRepo) GetByID(ctx context.Context, id string) (*domain.SemanticRelationship, error) {
	row, err := r.q.GetSemanticRelationshipByID(ctx, id)
	if err != nil {
		return nil, mapDBError(err)
	}
	return semanticRelationshipFromDB(row), nil
}

// GetByName returns a semantic relationship by source semantic model and path name.
func (r *SemanticRelationshipRepo) GetByName(ctx context.Context, fromSemanticID, name string) (*domain.SemanticRelationship, error) {
	row, err := r.q.GetSemanticRelationshipByName(ctx, dbstore.GetSemanticRelationshipByNameParams{
		FromSemanticID: fromSemanticID,
		Name:           name,
	})
	if err != nil {
		return nil, mapDBError(err)
	}
	return semanticRelationshipFromDB(row), nil
}

// List returns paginated semantic relationships.
func (r *SemanticRelationshipRepo) List(ctx context.Context, page domain.PageRequest) ([]domain.SemanticRelationship, int64, error) {
	total, err := r.q.CountSemanticRelationships(ctx)
	if err != nil {
		return nil, 0, err
	}

	rows, err := r.q.ListSemanticRelationships(ctx, dbstore.ListSemanticRelationshipsParams{
		Limit:  int64(page.Limit()),
		Offset: int64(page.Offset()),
	})
	if err != nil {
		return nil, 0, err
	}

	rels := make([]domain.SemanticRelationship, 0, len(rows))
	for _, row := range rows {
		rels = append(rels, *semanticRelationshipFromDB(row))
	}
	return rels, total, nil
}

// ListByModel returns all semantic relationships owned by a semantic model.
func (r *SemanticRelationshipRepo) ListByModel(ctx context.Context, semanticModelID string) ([]domain.SemanticRelationship, error) {
	rows, err := r.db.QueryContext(ctx, `
SELECT id, name, from_semantic_id, to_semantic_id, relationship_type, join_sql, cost, max_hops, created_by, created_at, updated_at
FROM semantic_relationships
WHERE from_semantic_id = ?
ORDER BY name
`, semanticModelID)
	if err != nil {
		return nil, mapDBError(err)
	}
	defer func() {
		_ = rows.Close()
	}()

	rels := make([]domain.SemanticRelationship, 0)
	for rows.Next() {
		var row dbstore.SemanticRelationship
		if err := rows.Scan(
			&row.ID,
			&row.Name,
			&row.FromSemanticID,
			&row.ToSemanticID,
			&row.RelationshipType,
			&row.JoinSql,
			&row.Cost,
			&row.MaxHops,
			&row.CreatedBy,
			&row.CreatedAt,
			&row.UpdatedAt,
		); err != nil {
			return nil, mapDBError(err)
		}
		rels = append(rels, *semanticRelationshipFromDB(row))
	}
	if err := rows.Err(); err != nil {
		return nil, mapDBError(err)
	}
	return rels, nil
}

// Update applies partial updates to a semantic relationship using read-modify-write.
func (r *SemanticRelationshipRepo) Update(ctx context.Context, id string, req domain.UpdateSemanticRelationshipRequest) (*domain.SemanticRelationship, error) {
	current, err := r.GetByID(ctx, id)
	if err != nil {
		return nil, err
	}

	relType := current.RelationshipType
	if req.RelationshipType != nil {
		relType = *req.RelationshipType
	}
	joinSQL := current.JoinSQL
	if req.JoinSQL != nil {
		joinSQL = *req.JoinSQL
	}
	cost := current.Cost
	if req.Cost != nil {
		cost = *req.Cost
	}
	maxHops := current.MaxHops
	if req.MaxHops != nil {
		maxHops = *req.MaxHops
	}

	err = r.q.UpdateSemanticRelationship(ctx, dbstore.UpdateSemanticRelationshipParams{
		RelationshipType: relType,
		JoinSql:          joinSQL,
		Cost:             int64(cost),
		MaxHops:          int64(maxHops),
		ID:               id,
	})
	if err != nil {
		return nil, mapDBError(err)
	}
	return r.GetByID(ctx, id)
}

// Delete removes a semantic relationship by ID.
func (r *SemanticRelationshipRepo) Delete(ctx context.Context, id string) error {
	return mapDBError(r.q.DeleteSemanticRelationship(ctx, id))
}
