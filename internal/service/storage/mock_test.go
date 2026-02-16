package storage

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

// Type aliases for convenience — keeps test code short.
type mockStorageCredentialRepo = testutil.MockStorageCredentialRepo
type mockAuthService = testutil.MockAuthService
type mockAuditRepo = testutil.MockAuditRepo
