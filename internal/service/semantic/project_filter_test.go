package semantic

import (
	"context"
	"fmt"
	"testing"

	"duck-demo/internal/domain"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fakeSemanticModelRepo struct {
	items []domain.SemanticModel
}

func (r *fakeSemanticModelRepo) Create(_ context.Context, m *domain.SemanticModel) (*domain.SemanticModel, error) {
	item := *m
	item.ID = fmt.Sprintf("semantic-%d", len(r.items)+1)
	r.items = append(r.items, item)
	return &item, nil
}

func (r *fakeSemanticModelRepo) GetByID(_ context.Context, id string) (*domain.SemanticModel, error) {
	for i := range r.items {
		if r.items[i].ID == id {
			item := r.items[i]
			return &item, nil
		}
	}
	return nil, domain.ErrNotFound("semantic model %q not found", id)
}

func (r *fakeSemanticModelRepo) GetByName(_ context.Context, name string) (*domain.SemanticModel, error) {
	for i := range r.items {
		if r.items[i].Name == name {
			item := r.items[i]
			return &item, nil
		}
	}
	return nil, domain.ErrNotFound("semantic model %q not found", name)
}

func (r *fakeSemanticModelRepo) List(_ context.Context, page domain.PageRequest) ([]domain.SemanticModel, int64, error) {
	total := int64(len(r.items))
	start := page.Offset()
	if start >= len(r.items) {
		return []domain.SemanticModel{}, total, nil
	}
	end := start + page.Limit()
	if end > len(r.items) {
		end = len(r.items)
	}
	return append([]domain.SemanticModel(nil), r.items[start:end]...), total, nil
}

func (r *fakeSemanticModelRepo) Update(_ context.Context, id string, _ domain.UpdateSemanticModelRequest) (*domain.SemanticModel, error) {
	return r.GetByID(context.Background(), id)
}

func (r *fakeSemanticModelRepo) Delete(_ context.Context, _ string) error { return nil }
func (r *fakeSemanticModelRepo) ListAll(_ context.Context) ([]domain.SemanticModel, error) {
	return append([]domain.SemanticModel(nil), r.items...), nil
}

func TestService_ListSemanticModelsFiltersByProjectAssociation(t *testing.T) {
	t.Parallel()

	repo := &fakeSemanticModelRepo{}
	svc := NewService(repo, nil, nil, nil)
	ctx := context.Background()

	_, err := svc.CreateSemanticModel(ctx, "alice", domain.CreateSemanticModelRequest{
		ProjectName:  "analytics",
		Name:         "orders_semantic",
		BaseModelRef: "analytics.orders",
	})
	require.NoError(t, err)

	_, err = svc.CreateSemanticModel(ctx, "alice", domain.CreateSemanticModelRequest{
		ProjectName:  "finance",
		Name:         "ledger_semantic",
		BaseModelRef: "finance.ledger",
	})
	require.NoError(t, err)

	items, total, err := svc.ListSemanticModels(ctx, strPtr("analytics"), domain.PageRequest{MaxResults: 10})
	require.NoError(t, err)
	require.Len(t, items, 1)
	assert.Equal(t, int64(1), total)
	assert.Equal(t, "orders_semantic", items[0].Name)
}

func strPtr(v string) *string {
	return &v
}
