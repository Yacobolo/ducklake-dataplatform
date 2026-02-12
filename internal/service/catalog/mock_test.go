package catalog

import (
	"context"
	"fmt"

	"duck-demo/internal/middleware"
	"duck-demo/internal/testutil"
)

// errTest is a sentinel error for test scenarios.
var errTest = fmt.Errorf("test error")

func ctxWithPrincipal(name string) context.Context {
	return middleware.WithPrincipal(context.Background(), name)
}

// Type aliases for convenience — keeps test code short.
type mockViewRepo = testutil.MockViewRepo
type mockSearchRepo = testutil.MockSearchRepo
type mockCatalogRepo = testutil.MockCatalogRepo
type mockAuthService = testutil.MockAuthService
type mockAuditRepo = testutil.MockAuditRepo
