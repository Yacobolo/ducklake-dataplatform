package compute

import (
	"fmt"

	"github.com/Yacobolo/quackstack/internal/testutil"
)

// errTest is a sentinel error for test scenarios.
var errTest = fmt.Errorf("test error")

// Type aliases for convenience — keeps test code short.
type mockAuthService = testutil.MockAuthService
type mockAuditRepo = testutil.MockAuditRepo
type mockComputeEndpointRepo = testutil.MockComputeEndpointRepo
