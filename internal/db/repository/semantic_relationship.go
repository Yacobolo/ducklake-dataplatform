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
	q *dbstore.Queries
}

// NewSemanticRelationshipRepo creates a new SemanticRelationshipRepo.
func NewSemanticRelationshipRepo(db *sql.DB) *SemanticRelationshipRepo {
	return &SemanticRelationshipRepo{q: dbstore.New(db)}
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
		IsDefault:        boolToInt(rel.IsDefault),
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

// GetByName returns a semantic relationship by unique name.
func (r *SemanticRelationshipRepo) GetByName(ctx context.Context, name string) (*domain.SemanticRelationship, error) {
	row, err := r.q.GetSemanticRelationshipByName(ctx, name)
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
	isDefault := current.IsDefault
	if req.IsDefault != nil {
		isDefault = *req.IsDefault
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
		IsDefault:        boolToInt(isDefault),
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
