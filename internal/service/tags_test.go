package service

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"duck-demo/internal/domain"
)

// === CreateTag ===

func TestTagService_CreateTag(t *testing.T) {
	t.Run("happy_path", func(t *testing.T) {
		repo := &mockTagRepo{
			createTagFn: func(_ context.Context, tag *domain.Tag) (*domain.Tag, error) {
				return &domain.Tag{
					ID:        1,
					Key:       tag.Key,
					Value:     tag.Value,
					CreatedBy: tag.CreatedBy,
					CreatedAt: time.Now(),
				}, nil
			},
		}
		audit := &mockAuditRepo{}
		svc := NewTagService(repo, audit)

		val := "production"
		result, err := svc.CreateTag(ctxWithPrincipal("alice"), &domain.Tag{Key: "env", Value: &val})

		require.NoError(t, err)
		assert.Equal(t, "env", result.Key)
		assert.Equal(t, "alice", result.CreatedBy)
		assert.True(t, audit.hasAction("CREATE_TAG"))
	})

	t.Run("repo_error", func(t *testing.T) {
		repo := &mockTagRepo{
			createTagFn: func(_ context.Context, _ *domain.Tag) (*domain.Tag, error) {
				return nil, errTest
			},
		}
		audit := &mockAuditRepo{}
		svc := NewTagService(repo, audit)

		_, err := svc.CreateTag(ctxWithPrincipal("alice"), &domain.Tag{Key: "env"})

		require.Error(t, err)
		assert.ErrorIs(t, err, errTest)
		assert.Empty(t, audit.entries, "audit should not be logged on error")
	})
}

// === GetTag ===

func TestTagService_GetTag(t *testing.T) {
	t.Run("happy_path", func(t *testing.T) {
		repo := &mockTagRepo{
			getTagFn: func(_ context.Context, id int64) (*domain.Tag, error) {
				return &domain.Tag{ID: id, Key: "env"}, nil
			},
		}
		svc := NewTagService(repo, &mockAuditRepo{})

		result, err := svc.GetTag(context.Background(), 1)

		require.NoError(t, err)
		assert.Equal(t, int64(1), result.ID)
	})

	t.Run("not_found", func(t *testing.T) {
		repo := &mockTagRepo{
			getTagFn: func(_ context.Context, _ int64) (*domain.Tag, error) {
				return nil, domain.ErrNotFound("tag not found")
			},
		}
		svc := NewTagService(repo, &mockAuditRepo{})

		_, err := svc.GetTag(context.Background(), 999)

		require.Error(t, err)
		var notFound *domain.NotFoundError
		assert.True(t, errors.As(err, &notFound))
	})
}

// === ListTags ===

func TestTagService_ListTags(t *testing.T) {
	t.Run("happy_path", func(t *testing.T) {
		repo := &mockTagRepo{
			listTagsFn: func(_ context.Context, _ domain.PageRequest) ([]domain.Tag, int64, error) {
				return []domain.Tag{{ID: 1, Key: "env"}, {ID: 2, Key: "pii"}}, 2, nil
			},
		}
		svc := NewTagService(repo, &mockAuditRepo{})

		tags, total, err := svc.ListTags(context.Background(), domain.PageRequest{MaxResults: 100})

		require.NoError(t, err)
		assert.Equal(t, int64(2), total)
		assert.Len(t, tags, 2)
	})

	t.Run("empty", func(t *testing.T) {
		repo := &mockTagRepo{
			listTagsFn: func(_ context.Context, _ domain.PageRequest) ([]domain.Tag, int64, error) {
				return []domain.Tag{}, 0, nil
			},
		}
		svc := NewTagService(repo, &mockAuditRepo{})

		tags, total, err := svc.ListTags(context.Background(), domain.PageRequest{MaxResults: 100})

		require.NoError(t, err)
		assert.Equal(t, int64(0), total)
		assert.Empty(t, tags)
	})
}

// === DeleteTag ===

func TestTagService_DeleteTag(t *testing.T) {
	t.Run("happy_path", func(t *testing.T) {
		repo := &mockTagRepo{
			deleteTagFn: func(_ context.Context, _ int64) error {
				return nil
			},
		}
		audit := &mockAuditRepo{}
		svc := NewTagService(repo, audit)

		err := svc.DeleteTag(ctxWithPrincipal("alice"), 1)

		require.NoError(t, err)
		assert.True(t, audit.hasAction("DELETE_TAG"))
	})

	t.Run("repo_error", func(t *testing.T) {
		repo := &mockTagRepo{
			deleteTagFn: func(_ context.Context, _ int64) error {
				return errTest
			},
		}
		audit := &mockAuditRepo{}
		svc := NewTagService(repo, audit)

		err := svc.DeleteTag(ctxWithPrincipal("alice"), 1)

		require.Error(t, err)
		assert.ErrorIs(t, err, errTest)
		assert.Empty(t, audit.entries, "audit should not be logged on error")
	})
}

// === AssignTag ===

func TestTagService_AssignTag(t *testing.T) {
	t.Run("happy_path", func(t *testing.T) {
		var captured *domain.TagAssignment
		repo := &mockTagRepo{
			assignTagFn: func(_ context.Context, a *domain.TagAssignment) (*domain.TagAssignment, error) {
				captured = a
				return &domain.TagAssignment{
					ID:            1,
					TagID:         a.TagID,
					SecurableType: a.SecurableType,
					SecurableID:   a.SecurableID,
					AssignedBy:    a.AssignedBy,
					AssignedAt:    time.Now(),
				}, nil
			},
		}
		audit := &mockAuditRepo{}
		svc := NewTagService(repo, audit)

		result, err := svc.AssignTag(ctxWithPrincipal("bob"), &domain.TagAssignment{
			TagID:         1,
			SecurableType: "table",
			SecurableID:   42,
		})

		require.NoError(t, err)
		assert.Equal(t, "bob", captured.AssignedBy)
		assert.Equal(t, "bob", result.AssignedBy)
		assert.True(t, audit.hasAction("ASSIGN_TAG"))
	})

	t.Run("repo_error", func(t *testing.T) {
		repo := &mockTagRepo{
			assignTagFn: func(_ context.Context, _ *domain.TagAssignment) (*domain.TagAssignment, error) {
				return nil, errTest
			},
		}
		audit := &mockAuditRepo{}
		svc := NewTagService(repo, audit)

		_, err := svc.AssignTag(ctxWithPrincipal("bob"), &domain.TagAssignment{TagID: 1})

		require.Error(t, err)
		assert.ErrorIs(t, err, errTest)
		assert.Empty(t, audit.entries)
	})
}

// === UnassignTag ===

func TestTagService_UnassignTag(t *testing.T) {
	t.Run("happy_path", func(t *testing.T) {
		repo := &mockTagRepo{
			unassignTagFn: func(_ context.Context, _ int64) error {
				return nil
			},
		}
		audit := &mockAuditRepo{}
		svc := NewTagService(repo, audit)

		err := svc.UnassignTag(ctxWithPrincipal("alice"), 1)

		require.NoError(t, err)
		assert.True(t, audit.hasAction("UNASSIGN_TAG"))
	})

	t.Run("repo_error", func(t *testing.T) {
		repo := &mockTagRepo{
			unassignTagFn: func(_ context.Context, _ int64) error {
				return errTest
			},
		}
		audit := &mockAuditRepo{}
		svc := NewTagService(repo, audit)

		err := svc.UnassignTag(ctxWithPrincipal("alice"), 1)

		require.Error(t, err)
		assert.ErrorIs(t, err, errTest)
		assert.Empty(t, audit.entries)
	})
}

// === ListTagsForSecurable ===

func TestTagService_ListTagsForSecurable(t *testing.T) {
	repo := &mockTagRepo{
		listTagsForSecurableFn: func(_ context.Context, _ string, _ int64, _ *string) ([]domain.Tag, error) {
			return []domain.Tag{{ID: 1, Key: "env"}}, nil
		},
	}
	svc := NewTagService(repo, &mockAuditRepo{})

	tags, err := svc.ListTagsForSecurable(context.Background(), "table", 1, nil)

	require.NoError(t, err)
	assert.Len(t, tags, 1)
}

// === ListAssignmentsForTag ===

func TestTagService_ListAssignmentsForTag(t *testing.T) {
	repo := &mockTagRepo{
		listAssignmentsForTagFn: func(_ context.Context, _ int64) ([]domain.TagAssignment, error) {
			return []domain.TagAssignment{{ID: 1, TagID: 1, SecurableType: "table"}}, nil
		},
	}
	svc := NewTagService(repo, &mockAuditRepo{})

	assignments, err := svc.ListAssignmentsForTag(context.Background(), 1)

	require.NoError(t, err)
	assert.Len(t, assignments, 1)
}

// === Classification Validation ===

func TestValidateClassificationTag(t *testing.T) {
	tests := []struct {
		name    string
		key     string
		value   *string
		wantErr bool
	}{
		{"valid classification pii", "classification", strPtr("pii"), false},
		{"valid classification sensitive", "classification", strPtr("sensitive"), false},
		{"valid classification confidential", "classification", strPtr("confidential"), false},
		{"valid classification public", "classification", strPtr("public"), false},
		{"valid classification personal_data", "classification", strPtr("personal_data"), false},
		{"valid sensitivity high", "sensitivity", strPtr("high"), false},
		{"valid sensitivity medium", "sensitivity", strPtr("medium"), false},
		{"valid sensitivity low", "sensitivity", strPtr("low"), false},
		{"invalid classification value", "classification", strPtr("invalid"), true},
		{"invalid sensitivity value", "sensitivity", strPtr("extreme"), true},
		{"classification without value", "classification", nil, true},
		{"sensitivity without value", "sensitivity", nil, true},
		{"custom tag allowed", "env", strPtr("production"), false},
		{"custom tag without value allowed", "team", nil, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tag := &domain.Tag{Key: tt.key, Value: tt.value}
			err := validateClassificationTag(tag)
			if tt.wantErr {
				require.Error(t, err)
				var validationErr *domain.ValidationError
				assert.True(t, errors.As(err, &validationErr))
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// === CreateTag with Classification Validation ===

func TestTagService_CreateTag_ClassificationValidation(t *testing.T) {
	t.Run("valid classification passes", func(t *testing.T) {
		repo := &mockTagRepo{
			createTagFn: func(_ context.Context, tag *domain.Tag) (*domain.Tag, error) {
				return &domain.Tag{ID: 1, Key: tag.Key, Value: tag.Value, CreatedBy: tag.CreatedBy, CreatedAt: time.Now()}, nil
			},
		}
		audit := &mockAuditRepo{}
		svc := NewTagService(repo, audit)

		val := "pii"
		result, err := svc.CreateTag(ctxWithPrincipal("alice"), &domain.Tag{Key: "classification", Value: &val})

		require.NoError(t, err)
		assert.Equal(t, "classification", result.Key)
	})

	t.Run("invalid classification rejected", func(t *testing.T) {
		repo := &mockTagRepo{
			// createTagFn should NOT be called
		}
		audit := &mockAuditRepo{}
		svc := NewTagService(repo, audit)

		val := "invalid_class"
		_, err := svc.CreateTag(ctxWithPrincipal("alice"), &domain.Tag{Key: "classification", Value: &val})

		require.Error(t, err)
		var validationErr *domain.ValidationError
		assert.True(t, errors.As(err, &validationErr))
		assert.Empty(t, audit.entries, "audit should not be logged on validation error")
	})
}

func strPtr(s string) *string { return &s }
