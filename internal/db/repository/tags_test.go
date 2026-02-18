package repository

import (
	"context"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	internaldb "duck-demo/internal/db"
	"duck-demo/internal/domain"
)

func tagPtrStr(s string) *string { return &s }

func setupTagRepo(t *testing.T) *TagRepo {
	t.Helper()
	writeDB, _ := internaldb.OpenTestSQLite(t)
	return NewTagRepo(writeDB)
}

func TestTagRepo_CreateAndGetTag(t *testing.T) {
	repo := setupTagRepo(t)
	ctx := context.Background()

	created, err := repo.CreateTag(ctx, &domain.Tag{
		Key:       "env",
		Value:     tagPtrStr("prod"),
		CreatedBy: "admin",
	})
	require.NoError(t, err)
	require.NotNil(t, created)
	assert.NotEmpty(t, created.ID)
	assert.Equal(t, "env", created.Key)
	assert.Equal(t, tagPtrStr("prod"), created.Value)
	assert.Equal(t, "admin", created.CreatedBy)
	assert.False(t, created.CreatedAt.IsZero())

	got, err := repo.GetTag(ctx, created.ID)
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, created.ID, got.ID)
	assert.Equal(t, "env", got.Key)
	assert.Equal(t, tagPtrStr("prod"), got.Value)
	assert.Equal(t, "admin", got.CreatedBy)
	assert.False(t, got.CreatedAt.IsZero())
}

func TestTagRepo_ListTags(t *testing.T) {
	repo := setupTagRepo(t)
	ctx := context.Background()

	// Count pre-seeded tags so the test is independent of migration seed data.
	_, baseTotal, err := repo.ListTags(ctx, domain.PageRequest{})
	require.NoError(t, err)

	_, err = repo.CreateTag(ctx, &domain.Tag{
		Key:       "env",
		Value:     tagPtrStr("prod"),
		CreatedBy: "admin",
	})
	require.NoError(t, err)

	_, err = repo.CreateTag(ctx, &domain.Tag{
		Key:       "team",
		Value:     tagPtrStr("platform"),
		CreatedBy: "admin",
	})
	require.NoError(t, err)

	tags, total, err := repo.ListTags(ctx, domain.PageRequest{})
	require.NoError(t, err)
	assert.Equal(t, baseTotal+2, total)
	assert.Len(t, tags, int(baseTotal)+2)
}

func TestTagRepo_DeleteTag(t *testing.T) {
	repo := setupTagRepo(t)
	ctx := context.Background()

	created, err := repo.CreateTag(ctx, &domain.Tag{
		Key:       "env",
		Value:     tagPtrStr("staging"),
		CreatedBy: "admin",
	})
	require.NoError(t, err)

	err = repo.DeleteTag(ctx, created.ID)
	require.NoError(t, err)

	_, err = repo.GetTag(ctx, created.ID)
	require.Error(t, err)
	var notFound *domain.NotFoundError
	assert.ErrorAs(t, err, &notFound)
}

func TestTagRepo_AssignAndUnassign(t *testing.T) {
	repo := setupTagRepo(t)
	ctx := context.Background()

	tag, err := repo.CreateTag(ctx, &domain.Tag{
		Key:       "env",
		Value:     tagPtrStr("prod"),
		CreatedBy: "admin",
	})
	require.NoError(t, err)

	assignment, err := repo.AssignTag(ctx, &domain.TagAssignment{
		TagID:         tag.ID,
		SecurableType: "table",
		SecurableID:   "tbl-1",
		AssignedBy:    "admin",
	})
	require.NoError(t, err)
	require.NotNil(t, assignment)
	assert.NotEmpty(t, assignment.ID)
	assert.Equal(t, tag.ID, assignment.TagID)
	assert.Equal(t, "table", assignment.SecurableType)
	assert.Equal(t, "tbl-1", assignment.SecurableID)
	assert.Equal(t, "admin", assignment.AssignedBy)
	assert.False(t, assignment.AssignedAt.IsZero())

	err = repo.UnassignTag(ctx, assignment.ID)
	require.NoError(t, err)

	assignments, err := repo.ListAssignmentsForTag(ctx, tag.ID)
	require.NoError(t, err)
	assert.Empty(t, assignments)
}

func TestTagRepo_AssignTag_MacroSecurable(t *testing.T) {
	repo := setupTagRepo(t)
	ctx := context.Background()

	tag, err := repo.CreateTag(ctx, &domain.Tag{
		Key:       "domain",
		Value:     tagPtrStr("shared"),
		CreatedBy: "admin",
	})
	require.NoError(t, err)

	assignment, err := repo.AssignTag(ctx, &domain.TagAssignment{
		TagID:         tag.ID,
		SecurableType: domain.TagSecurableTypeMacro,
		SecurableID:   "macro-1",
		AssignedBy:    "admin",
	})
	require.NoError(t, err)
	assert.Equal(t, domain.TagSecurableTypeMacro, assignment.SecurableType)
}

func TestTagRepo_AssignTag_InvalidSecurableType(t *testing.T) {
	repo := setupTagRepo(t)
	ctx := context.Background()

	tag, err := repo.CreateTag(ctx, &domain.Tag{
		Key:       "domain",
		Value:     tagPtrStr("shared"),
		CreatedBy: "admin",
	})
	require.NoError(t, err)

	_, err = repo.AssignTag(ctx, &domain.TagAssignment{
		TagID:         tag.ID,
		SecurableType: "invalid",
		SecurableID:   "obj-1",
		AssignedBy:    "admin",
	})
	require.Error(t, err)
	var validationErr *domain.ValidationError
	require.ErrorAs(t, err, &validationErr)
}

func TestTagRepo_ListTagsForSecurable(t *testing.T) {
	repo := setupTagRepo(t)
	ctx := context.Background()

	tag1, err := repo.CreateTag(ctx, &domain.Tag{
		Key:       "env",
		Value:     tagPtrStr("prod"),
		CreatedBy: "admin",
	})
	require.NoError(t, err)

	tag2, err := repo.CreateTag(ctx, &domain.Tag{
		Key:       "team",
		Value:     tagPtrStr("platform"),
		CreatedBy: "admin",
	})
	require.NoError(t, err)

	_, err = repo.AssignTag(ctx, &domain.TagAssignment{
		TagID:         tag1.ID,
		SecurableType: "table",
		SecurableID:   "tbl-1",
		AssignedBy:    "admin",
	})
	require.NoError(t, err)

	_, err = repo.AssignTag(ctx, &domain.TagAssignment{
		TagID:         tag2.ID,
		SecurableType: "table",
		SecurableID:   "tbl-1",
		AssignedBy:    "admin",
	})
	require.NoError(t, err)

	tags, err := repo.ListTagsForSecurable(ctx, "table", "tbl-1", nil)
	require.NoError(t, err)
	assert.Len(t, tags, 2)
}

func TestTagRepo_ListTagsForSecurable_WithColumnName(t *testing.T) {
	repo := setupTagRepo(t)
	ctx := context.Background()

	tag, err := repo.CreateTag(ctx, &domain.Tag{
		Key:       "data-owner",
		Value:     tagPtrStr("finance"),
		CreatedBy: "admin",
	})
	require.NoError(t, err)

	colName := tagPtrStr("email")
	_, err = repo.AssignTag(ctx, &domain.TagAssignment{
		TagID:         tag.ID,
		SecurableType: "table",
		SecurableID:   "tbl-1",
		ColumnName:    colName,
		AssignedBy:    "admin",
	})
	require.NoError(t, err)

	// With matching column name should return the tag.
	tags, err := repo.ListTagsForSecurable(ctx, "table", "tbl-1", tagPtrStr("email"))
	require.NoError(t, err)
	assert.Len(t, tags, 1)
	assert.Equal(t, tag.ID, tags[0].ID)

	// With nil column name the SQL filter (? IS NULL OR ...) matches all
	// assignments, so the column-specific tag is still returned.
	tags, err = repo.ListTagsForSecurable(ctx, "table", "tbl-1", nil)
	require.NoError(t, err)
	assert.Len(t, tags, 1)
	assert.Equal(t, tag.ID, tags[0].ID)

	// With a non-matching column name returns empty.
	tags, err = repo.ListTagsForSecurable(ctx, "table", "tbl-1", tagPtrStr("phone"))
	require.NoError(t, err)
	assert.Empty(t, tags)
}

func TestTagRepo_ListAssignmentsForTag(t *testing.T) {
	repo := setupTagRepo(t)
	ctx := context.Background()

	tag, err := repo.CreateTag(ctx, &domain.Tag{
		Key:       "env",
		Value:     tagPtrStr("prod"),
		CreatedBy: "admin",
	})
	require.NoError(t, err)

	_, err = repo.AssignTag(ctx, &domain.TagAssignment{
		TagID:         tag.ID,
		SecurableType: "table",
		SecurableID:   "tbl-1",
		AssignedBy:    "admin",
	})
	require.NoError(t, err)

	_, err = repo.AssignTag(ctx, &domain.TagAssignment{
		TagID:         tag.ID,
		SecurableType: "schema",
		SecurableID:   "sch-1",
		AssignedBy:    "admin",
	})
	require.NoError(t, err)

	assignments, err := repo.ListAssignmentsForTag(ctx, tag.ID)
	require.NoError(t, err)
	assert.Len(t, assignments, 2)
}
