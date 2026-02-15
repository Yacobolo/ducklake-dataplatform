package governance

import (
	"context"
	"fmt"

	"duck-demo/internal/domain"
	"duck-demo/internal/testutil"
)

// errTest is a sentinel error for test scenarios.
var errTest = fmt.Errorf("test error")

func ctxWithPrincipal(name string) context.Context {
	return domain.WithPrincipal(context.Background(), domain.ContextPrincipal{Name: name, Type: "user"})
}

func strPtr(s string) *string { return &s }

// adminCtx returns a context with an admin principal for testing.
func adminCtx() context.Context {
	return domain.WithPrincipal(context.Background(), domain.ContextPrincipal{
		ID: "admin-id", Name: "admin-user", IsAdmin: true, Type: "user",
	})
}

// nonAdminCtx returns a context with a non-admin principal for testing.
func nonAdminCtx() context.Context {
	return domain.WithPrincipal(context.Background(), domain.ContextPrincipal{
		ID: "non-admin-id", Name: "regular-user", IsAdmin: false, Type: "user",
	})
}

// Type aliases for convenience — keeps test code short.
type mockTagRepo = testutil.MockTagRepo
type mockLineageRepo = testutil.MockLineageRepo
type mockQueryHistoryRepo = testutil.MockQueryHistoryRepo
type mockAuditRepo = testutil.MockAuditRepo
