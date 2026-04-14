package catalog

import (
	"context"
	"fmt"

	"github.com/Yacobolo/quackstack/internal/domain"
	"github.com/Yacobolo/quackstack/internal/testutil"
)

// errTest is a sentinel error for test scenarios.
var errTest = fmt.Errorf("test error")

func ctxWithPrincipal(name string) context.Context {
	return domain.WithPrincipal(context.Background(), domain.ContextPrincipal{Name: name, Type: "user"})
}

// Type aliases for convenience — keeps test code short.
type mockViewRepo = testutil.MockViewRepo
type mockSearchRepo = testutil.MockSearchRepo
type mockCatalogRepo = testutil.MockCatalogRepo
type mockAuthService = testutil.MockAuthService
type mockAuditRepo = testutil.MockAuditRepo

// mockCatalogRepoFactory wraps a mockCatalogRepo to implement CatalogRepoFactory.
type mockCatalogRepoFactory struct {
	repo *mockCatalogRepo
}

func (f *mockCatalogRepoFactory) ForCatalog(_ context.Context, _ string) (domain.CatalogRepository, error) {
	return f.repo, nil
}
