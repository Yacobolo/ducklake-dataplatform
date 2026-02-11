package repository

import (
	"context"
	"database/sql"

	dbstore "duck-demo/internal/db/dbstore"
	"duck-demo/internal/db/mapper"
	"duck-demo/internal/domain"
)

type TagRepo struct {
	q  *dbstore.Queries
	db *sql.DB
}

func NewTagRepo(db *sql.DB) *TagRepo {
	return &TagRepo{q: dbstore.New(db), db: db}
}

func (r *TagRepo) CreateTag(ctx context.Context, tag *domain.Tag) (*domain.Tag, error) {
	row, err := r.q.CreateTag(ctx, dbstore.CreateTagParams{
		Key:       tag.Key,
		Value:     mapper.NullStrFromPtr(tag.Value),
		CreatedBy: tag.CreatedBy,
	})
	if err != nil {
		return nil, mapDBError(err)
	}
	return mapper.TagFromDB(row), nil
}

func (r *TagRepo) GetTag(ctx context.Context, id int64) (*domain.Tag, error) {
	row, err := r.q.GetTag(ctx, id)
	if err != nil {
		return nil, mapDBError(err)
	}
	return mapper.TagFromDB(row), nil
}

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

func (r *TagRepo) DeleteTag(ctx context.Context, id int64) error {
	return r.q.DeleteTag(ctx, id)
}

func (r *TagRepo) AssignTag(ctx context.Context, assignment *domain.TagAssignment) (*domain.TagAssignment, error) {
	row, err := r.q.CreateTagAssignment(ctx, dbstore.CreateTagAssignmentParams{
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

func (r *TagRepo) UnassignTag(ctx context.Context, id int64) error {
	return r.q.DeleteTagAssignment(ctx, id)
}

func (r *TagRepo) ListTagsForSecurable(ctx context.Context, securableType string, securableID int64, columnName *string) ([]domain.Tag, error) {
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

func (r *TagRepo) ListAssignmentsForTag(ctx context.Context, tagID int64) ([]domain.TagAssignment, error) {
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
