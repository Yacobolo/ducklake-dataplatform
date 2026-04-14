package macro

import (
	"context"
	"fmt"
	"testing"

	"duck-demo/internal/domain"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type stubAuditRepo struct{}

func (stubAuditRepo) Insert(context.Context, *domain.AuditEntry) error { return nil }
func (stubAuditRepo) List(context.Context, domain.AuditFilter) ([]domain.AuditEntry, int64, error) {
	return nil, 0, nil
}

type fakeMacroRepo struct {
	items []domain.Macro
}

func (r *fakeMacroRepo) Create(_ context.Context, m *domain.Macro) (*domain.Macro, error) {
	item := *m
	item.ID = fmt.Sprintf("macro-%d", len(r.items)+1)
	r.items = append(r.items, item)
	return &item, nil
}

func (r *fakeMacroRepo) GetByName(_ context.Context, name string) (*domain.Macro, error) {
	for i := range r.items {
		if r.items[i].Name == name {
			item := r.items[i]
			return &item, nil
		}
	}
	return nil, domain.ErrNotFound("macro %q not found", name)
}

func (r *fakeMacroRepo) List(_ context.Context, page domain.PageRequest) ([]domain.Macro, int64, error) {
	total := int64(len(r.items))
	start := page.Offset()
	if start >= len(r.items) {
		return []domain.Macro{}, total, nil
	}
	end := start + page.Limit()
	if end > len(r.items) {
		end = len(r.items)
	}
	return append([]domain.Macro(nil), r.items[start:end]...), total, nil
}

func (r *fakeMacroRepo) Update(_ context.Context, name string, req domain.UpdateMacroRequest) (*domain.Macro, error) {
	item, err := r.GetByName(context.Background(), name)
	if err != nil {
		return nil, err
	}
	if req.ProjectName != nil {
		item.ProjectName = *req.ProjectName
	}
	return item, nil
}

func (r *fakeMacroRepo) Delete(_ context.Context, _ string) error { return nil }
func (r *fakeMacroRepo) ListAll(_ context.Context) ([]domain.Macro, error) {
	return append([]domain.Macro(nil), r.items...), nil
}
func (r *fakeMacroRepo) ListRevisions(context.Context, string) ([]domain.MacroRevision, error) {
	return nil, nil
}
func (r *fakeMacroRepo) GetRevisionByVersion(context.Context, string, int) (*domain.MacroRevision, error) {
	return nil, nil
}

func TestService_ListFilteredByProject(t *testing.T) {
	t.Parallel()

	repo := &fakeMacroRepo{}
	svc := NewService(repo, stubAuditRepo{})
	ctx := context.Background()

	_, err := svc.Create(ctx, "alice", domain.CreateMacroRequest{
		Name:        "safe_sum",
		ProjectName: "analytics",
		MacroType:   domain.MacroTypeScalar,
		Visibility:  domain.MacroVisibilityProject,
		Body:        "SELECT 1",
	})
	require.NoError(t, err)

	_, err = svc.Create(ctx, "alice", domain.CreateMacroRequest{
		Name:        "safe_avg",
		ProjectName: "analytics",
		MacroType:   domain.MacroTypeScalar,
		Visibility:  domain.MacroVisibilityProject,
		Body:        "SELECT 2",
	})
	require.NoError(t, err)

	_, err = svc.Create(ctx, "alice", domain.CreateMacroRequest{
		Name:        "finance_rollup",
		ProjectName: "finance",
		MacroType:   domain.MacroTypeScalar,
		Visibility:  domain.MacroVisibilityProject,
		Body:        "SELECT 3",
	})
	require.NoError(t, err)

	filtered, total, err := svc.ListFiltered(ctx, strPtr("analytics"), domain.PageRequest{MaxResults: 10})
	require.NoError(t, err)
	require.Len(t, filtered, 2)
	assert.Equal(t, int64(2), total)
	assert.Equal(t, "analytics", filtered[0].ProjectName)
	assert.Equal(t, "analytics", filtered[1].ProjectName)

	paged, total, err := svc.ListFiltered(ctx, strPtr("analytics"), domain.PageRequest{
		MaxResults: 1,
		PageToken:  domain.EncodePageToken(1),
	})
	require.NoError(t, err)
	require.Len(t, paged, 1)
	assert.Equal(t, int64(2), total)
	assert.Equal(t, "analytics", paged[0].ProjectName)
}

func strPtr(v string) *string {
	return &v
}
