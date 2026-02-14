package repository

import (
	"context"
	"database/sql"

	dbstore "duck-demo/internal/db/dbstore"
	"duck-demo/internal/db/mapper"
	"duck-demo/internal/domain"
)

// TagRepo implements domain.TagRepository using SQLite.
type TagRepo struct {
	q  *dbstore.Queries
	db *sql.DB
}

// NewTagRepo creates a new TagRepo.
func NewTagRepo(db *sql.DB) *TagRepo {
	return &TagRepo{q: dbstore.New(db), db: db}
}

// CreateTag inserts a new tag definition into the database.
func (r *TagRepo) CreateTag(ctx context.Context, tag *domain.Tag) (*domain.Tag, error) {
	row, err := r.q.CreateTag(ctx, dbstore.CreateTagParams{
		ID:        newID(),
		Key:       tag.Key,
		Value:     mapper.NullStrFromPtr(tag.Value),
		CreatedBy: tag.CreatedBy,
	})
	if err != nil {
		return nil, mapDBError(err)
	}
	return mapper.TagFromDB(row), nil
}

// GetTag returns a tag by its ID.
func (r *TagRepo) GetTag(ctx context.Context, id string) (*domain.Tag, error) {
	row, err := r.q.GetTag(ctx, id)
	if err != nil {
		return nil, mapDBError(err)
	}
	return mapper.TagFromDB(row), nil
}

// ListTags returns a paginated list of tags.
func (r *TagRepo) ListTags(ctx context.Context, page domain.PageRequest) ([]domain.Tag, int64, error) {
	total, err := r.q.CountTags(ctx)
	if err != nil {
		return nil, 0, err
	}
	rows, err := r.q.ListTags(ctx, dbstore.ListTagsParams{
		Limit:  int64(page.Limit()),
		Offset: int64(page.Offset()),
	})
	if err != nil {
		return nil, 0, err
	}
	tags := make([]domain.Tag, len(rows))
	for i, row := range rows {
		tags[i] = *mapper.TagFromDB(row)
	}
	return tags, total, nil
}

// DeleteTag removes a tag by ID. Returns NotFoundError if the tag does not exist.
func (r *TagRepo) DeleteTag(ctx context.Context, id string) error {
	result, err := r.db.ExecContext(ctx, "DELETE FROM tags WHERE id = ?", id)
	if err != nil {
		return mapDBError(err)
	}
	rows, _ := result.RowsAffected()
	if rows == 0 {
		return domain.ErrNotFound("tag %q not found", id)
	}
	return nil
}

// AssignTag assigns a tag to a securable object.
func (r *TagRepo) AssignTag(ctx context.Context, assignment *domain.TagAssignment) (*domain.TagAssignment, error) {
	row, err := r.q.CreateTagAssignment(ctx, dbstore.CreateTagAssignmentParams{
		ID:            newID(),
		TagID:         assignment.TagID,
		SecurableType: assignment.SecurableType,
		SecurableID:   assignment.SecurableID,
		ColumnName:    mapper.NullStrFromPtr(assignment.ColumnName),
		AssignedBy:    assignment.AssignedBy,
	})
	if err != nil {
		return nil, mapDBError(err)
	}
	return mapper.TagAssignmentFromDB(row), nil
}

// UnassignTag removes a tag assignment by ID.
func (r *TagRepo) UnassignTag(ctx context.Context, id string) error {
	return r.q.DeleteTagAssignment(ctx, id)
}

// ListTagsForSecurable returns all tags assigned to a securable object.
func (r *TagRepo) ListTagsForSecurable(ctx context.Context, securableType string, securableID string, columnName *string) ([]domain.Tag, error) {
	rows, err := r.q.ListTagsForSecurable(ctx, dbstore.ListTagsForSecurableParams{
		SecurableType: securableType,
		SecurableID:   securableID,
		Column3:       mapper.InterfaceFromPtr(columnName),
		ColumnName:    mapper.StringFromPtr(columnName),
	})
	if err != nil {
		return nil, err
	}
	tags := make([]domain.Tag, len(rows))
	for i, row := range rows {
		tags[i] = *mapper.TagFromDB(row)
	}
	return tags, nil
}

// ListAssignmentsForTag returns all assignments for a given tag.
func (r *TagRepo) ListAssignmentsForTag(ctx context.Context, tagID string) ([]domain.TagAssignment, error) {
	rows, err := r.q.ListAssignmentsForTag(ctx, tagID)
	if err != nil {
		return nil, err
	}
	assignments := make([]domain.TagAssignment, len(rows))
	for i, row := range rows {
		assignments[i] = *mapper.TagAssignmentFromDB(row)
	}
	return assignments, nil
}
