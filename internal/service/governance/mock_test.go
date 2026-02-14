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

// Type aliases for convenience — keeps test code short.
type mockTagRepo = testutil.MockTagRepo
type mockLineageRepo = testutil.MockLineageRepo
type mockQueryHistoryRepo = testutil.MockQueryHistoryRepo
type mockAuditRepo = testutil.MockAuditRepo
