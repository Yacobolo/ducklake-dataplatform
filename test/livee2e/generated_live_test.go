//go:build livee2e

package livee2e

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/getkin/kin-openapi/openapi3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	internalapi "github.com/Yacobolo/quackstack/internal/api"
)

func TestLive_Smoke(t *testing.T) {
	t.Parallel()

	for _, path := range []string{"/healthz", "/docs", "/openapi.json"} {
		resp, data, err := suite.doJSON(context.Background(), http.MethodGet, path, "", nil)
		require.NoError(t, err)
		assert.Lessf(t, resp.StatusCode, 500, "smoke %s failed: %s", path, string(data))
		assert.GreaterOrEqualf(t, resp.StatusCode, 200, "smoke %s failed: %s", path, string(data))
		assert.Lessf(t, resp.StatusCode, 400, "smoke %s failed: %s", path, string(data))
	}
}

func TestLive_CLIBasicDiscovery(t *testing.T) {
	t.Parallel()

	listOut := suite.runCLI(t, "--host", suite.host, "--token", suite.token, "--output", "json", "api", "list")
	assert.Contains(t, string(listOut), `"operation_id"`)

	describeOut := suite.runCLI(t, "--host", suite.host, "--token", suite.token, "--output", "json", "api", "describe", "createSchema")
	assert.Contains(t, string(describeOut), `"operation_id": "createSchema"`)

	specOut := suite.runCLI(t, "--host", suite.host, "api", "spec", "--source", "embedded", "--format", "json")
	var payload map[string]any
	require.NoError(t, json.Unmarshal(specOut, &payload))
	assert.Equal(t, "3.0.0", payload["openapi"])
}

func TestGeneratedLive_ProtectedOperationsRequireAuth(t *testing.T) {
	ops := mergedOperations(t)
	executed := 0
	limit := suite.operationLimit()

	for _, op := range ops {
		if !suite.operationFilterAllowed(op.ref.Tags) {
			continue
		}
		if len(op.ref.BodyFields) > 0 {
			continue
		}
		if _, ok := op.documentedCode[401]; !ok {
			if _, forbidden := op.documentedCode[403]; !forbidden {
				continue
			}
		}
		path, ok := suite.resolvePath(op)
		if !ok {
			continue
		}

		t.Run(op.ref.OperationID, func(t *testing.T) {
			resp, data, err := suite.doJSON(context.Background(), op.ref.Method, path, "", nil)
			require.NoError(t, err)
			assert.Lessf(t, resp.StatusCode, 500, "unexpected server error for %s %s: %s", op.ref.Method, path, string(data))
			assert.Containsf(t, []int{http.StatusUnauthorized, http.StatusForbidden}, resp.StatusCode, "expected auth failure for %s %s: %s", op.ref.Method, path, string(data))
			if resp.StatusCode >= 500 {
				suite.recordFinding("server_bug", "error", op, path, resp.StatusCode, "unauthenticated protected operation returned server error")
			}
		})
		executed++
		if limit > 0 && executed >= limit {
			break
		}
	}

	if limit > 0 {
		require.Greater(t, executed, 0, "expected at least one generated auth check to run with E2E_LIVE_LIMIT")
	} else {
		require.GreaterOrEqual(t, executed, 5, "expected at least a handful of generated auth checks to run")
	}
}

func TestGeneratedLive_StrictContractOwnedReadsStayDocumented(t *testing.T) {
	if !strictContractOwnedEnabled() {
		t.Skip("strict contract-owned lane is disabled; set E2E_LIVE_STRICT=true to enable")
	}
	ops := strictContractOwnedOperations(t)
	require.NotEmpty(t, ops, "expected at least one strict contract-owned operation")

	for _, op := range ops {
		if !strings.EqualFold(op.ref.Method, http.MethodGet) {
			continue
		}
		path, ok := suite.buildQueryPath(op, nil)
		if !ok {
			continue
		}

		t.Run(op.ref.OperationID, func(t *testing.T) {
			resp, data, err := suite.doJSON(context.Background(), op.ref.Method, path, suite.token, nil)
			require.NoError(t, err)
			assert.Lessf(t, resp.StatusCode, 500, "strict contract-owned read hit server error for %s %s: %s", op.ref.Method, path, string(data))
			if !bodyStatusAllowed(op, resp.StatusCode) {
				suite.recordFinding("openapi_drift", "error", op, path, resp.StatusCode, "strict contract-owned operation returned undocumented status")
			}
			require.Truef(t, bodyStatusAllowed(op, resp.StatusCode), "strict contract-owned operation returned undocumented status %d for %s: %s", resp.StatusCode, path, string(data))
		})
	}
}

func TestGeneratedLive_StrictContractOwnedMalformedBodiesFail(t *testing.T) {
	if !strictContractOwnedEnabled() {
		t.Skip("strict contract-owned lane is disabled; set E2E_LIVE_STRICT=true to enable")
	}
	ops := strictContractOwnedOperations(t)
	require.NotEmpty(t, ops, "expected at least one strict contract-owned operation")

	for _, op := range ops {
		switch strings.ToUpper(op.ref.Method) {
		case http.MethodPost, http.MethodPut, http.MethodPatch:
		default:
			continue
		}
		body, required, ok := suite.inferBodyPayload(op)
		if !ok || len(required) == 0 {
			continue
		}
		path, ok := suite.resolvePath(op)
		if !ok {
			continue
		}
		delete(body, required[0])

		t.Run(op.ref.OperationID, func(t *testing.T) {
			resp, data, err := suite.doJSON(context.Background(), op.ref.Method, path, suite.token, body)
			require.NoError(t, err)
			if resp.StatusCode >= 500 && resp.StatusCode != http.StatusNotImplemented && resp.StatusCode != http.StatusBadGateway {
				suite.recordFinding("server_bug", "error", op, path, resp.StatusCode, "strict contract-owned malformed-body request returned server error")
			}
			require.Lessf(t, resp.StatusCode, 500, "strict contract-owned malformed-body request hit server error for %s %s: %s", op.ref.Method, path, string(data))
			if resp.StatusCode < 400 {
				suite.recordFinding("validation_gap", "error", op, path, resp.StatusCode, "strict contract-owned malformed-body request succeeded")
			}
			require.GreaterOrEqualf(t, resp.StatusCode, 400, "strict contract-owned malformed-body request unexpectedly succeeded for %s %s: %s", op.ref.Method, path, string(data))
			if !bodyStatusAllowed(op, resp.StatusCode) {
				suite.recordFinding("openapi_drift", "error", op, path, resp.StatusCode, "strict contract-owned malformed-body request returned undocumented status")
			}
			require.Truef(t, bodyStatusAllowed(op, resp.StatusCode), "strict contract-owned malformed-body request returned undocumented status %d for %s: %s", resp.StatusCode, path, string(data))
		})
	}
}

func TestGeneratedLive_ProtectedOperationsAuthMatrix(t *testing.T) {
	ops := mergedOperations(t)
	executed := 0
	limit := suite.operationLimit()

	for _, op := range ops {
		if !suite.operationFilterAllowed(op.ref.Tags) {
			continue
		}
		if !strings.EqualFold(op.ref.Method, http.MethodGet) {
			continue
		}
		if len(op.ref.BodyFields) > 0 {
			continue
		}
		if _, ok := op.documentedCode[401]; !ok {
			if _, forbidden := op.documentedCode[403]; !forbidden {
				continue
			}
		}
		path, ok := suite.buildQueryPath(op, nil)
		if !ok {
			continue
		}

		t.Run(op.ref.OperationID, func(t *testing.T) {
			anonResp, anonData, err := suite.doJSON(context.Background(), op.ref.Method, path, "", nil)
			require.NoError(t, err)
			require.Contains(t, []int{http.StatusUnauthorized, http.StatusForbidden}, anonResp.StatusCode)

			userResp, userData, err := suite.doJSON(context.Background(), op.ref.Method, path, suite.userToken, nil)
			require.NoError(t, err)
			assert.Lessf(t, userResp.StatusCode, 500, "non-admin request caused server error for %s %s: %s", op.ref.Method, path, string(userData))

			adminResp, adminData, err := suite.doJSON(context.Background(), op.ref.Method, path, suite.token, nil)
			require.NoError(t, err)
			assert.Lessf(t, adminResp.StatusCode, 500, "admin request caused server error for %s %s: %s", op.ref.Method, path, string(adminData))

			if userResp.StatusCode >= 500 {
				suite.recordFinding("server_bug", "error", op, path, userResp.StatusCode, "non-admin auth matrix request returned server error")
			}
			if isAdminOnlyOperation(op) && adminResp.StatusCode < 400 && userResp.StatusCode < 300 {
				suite.recordFinding("auth_gap", "warning", op, path, userResp.StatusCode, "non-admin principal was allowed on a sensitive protected operation")
				t.Logf("auth matrix observed possible non-admin auth gap for %s %s", op.ref.Method, path)
			}
			if adminResp.StatusCode < 300 && !bodyStatusAllowed(op, userResp.StatusCode) && userResp.StatusCode >= 400 && userResp.StatusCode < 500 {
				suite.recordFinding("openapi_drift", "info", op, path, userResp.StatusCode, "non-admin auth matrix returned undocumented client status")
			}
			_ = anonData
		})
		executed++
		if limit > 0 && executed >= limit {
			break
		}
	}

	if limit > 0 {
		require.Greater(t, executed, 0, "expected at least one generated auth matrix check to run with E2E_LIVE_LIMIT")
	} else {
		require.GreaterOrEqual(t, executed, 10, "expected generated auth matrix coverage to execute")
	}
}

func TestGeneratedLive_CrossPrincipalOperationsRejectUnauthorizedScopes(t *testing.T) {
	ops := mergedOperations(t)
	executed := 0
	limit := suite.operationLimit()

	for _, op := range ops {
		if !suite.operationFilterAllowed(op.ref.Tags) {
			continue
		}
		extra, expectedStatus, ok := suite.crossPrincipalProbe(op)
		if !ok {
			continue
		}
		path, ok := suite.buildQueryPath(op, extra)
		if !ok {
			continue
		}

		t.Run(op.ref.OperationID, func(t *testing.T) {
			resp, data, err := suite.doJSON(context.Background(), op.ref.Method, path, suite.userToken, nil)
			require.NoError(t, err)
			assert.Lessf(t, resp.StatusCode, 500, "cross-principal request caused server error for %s %s: %s", op.ref.Method, path, string(data))
			if resp.StatusCode >= 500 {
				suite.recordFinding("server_bug", "error", op, path, resp.StatusCode, "cross-principal request returned server error")
			}
			if resp.StatusCode < 300 {
				suite.recordFinding("auth_gap", "warning", op, path, resp.StatusCode, "non-admin principal was allowed to access another principal's scoped resource")
			}
			require.Equalf(t, expectedStatus, resp.StatusCode, "expected cross-principal probe to be rejected for %s %s: %s", op.ref.Method, path, string(data))
		})
		executed++
		if limit > 0 && executed >= limit {
			break
		}
	}

	if limit > 0 {
		require.GreaterOrEqual(t, executed, 1, "expected at least one cross-principal probe to run with E2E_LIVE_LIMIT")
	} else {
		require.GreaterOrEqual(t, executed, 1, "expected generated cross-principal coverage to execute")
	}
}

func strictContractOwnedOperations(t *testing.T) []liveOperation {
	t.Helper()

	contracts := internalapi.GetAPIGenOperationContracts()
	ops := mergedOperations(t)
	out := make([]liveOperation, 0, len(ops))
	for _, op := range ops {
		contract, ok := contracts[op.ref.OperationID]
		if !ok || contract.Manual {
			continue
		}
		out = append(out, op)
	}
	return out
}

func strictContractOwnedEnabled() bool {
	return strings.EqualFold(strings.TrimSpace(os.Getenv("E2E_LIVE_STRICT")), "true")
}

func hasTag(tags []string, want string) bool {
	for _, tag := range tags {
		if strings.EqualFold(tag, want) {
			return true
		}
	}
	return false
}

func TestGeneratedLive_AuthenticatedReadsStayWithinDocumentedContract(t *testing.T) {
	ops := mergedOperations(t)
	executed := 0
	limit := suite.operationLimit()

	for _, op := range ops {
		if !suite.operationFilterAllowed(op.ref.Tags) {
			continue
		}
		if !strings.EqualFold(op.ref.Method, http.MethodGet) {
			continue
		}
		path, ok := suite.buildQueryPath(op, nil)
		if !ok {
			continue
		}
		if hasRequiredBody(op) {
			continue
		}

		t.Run(op.ref.OperationID, func(t *testing.T) {
			resp, data, err := suite.doJSON(context.Background(), http.MethodGet, path, suite.token, nil)
			require.NoError(t, err)
			assert.Lessf(t, resp.StatusCode, 500, "unexpected server error for GET %s: %s", path, string(data))
			if !bodyStatusAllowed(op, resp.StatusCode) {
				suite.recordFinding("openapi_drift", "info", op, path, resp.StatusCode, "authenticated read returned undocumented status")
			}
			assert.Truef(t, bodyStatusAllowed(op, resp.StatusCode), "undocumented status %d for %s: %s", resp.StatusCode, path, string(data))
		})
		executed++
		if limit > 0 && executed >= limit {
			break
		}
	}

	if limit > 0 {
		require.Greater(t, executed, 0, "expected at least one generated authenticated read check to run with E2E_LIVE_LIMIT")
	} else {
		require.GreaterOrEqual(t, executed, 10, "expected generated authenticated read coverage to execute")
	}
}

func TestGeneratedLive_MalformedRequiredBodiesFailCleanly(t *testing.T) {
	ops := mergedOperations(t)
	executed := 0
	limit := suite.operationLimit()

	for _, op := range ops {
		if !suite.operationFilterAllowed(op.ref.Tags) {
			continue
		}
		switch strings.ToUpper(op.ref.Method) {
		case http.MethodPost, http.MethodPut, http.MethodPatch:
		default:
			continue
		}
		body, required, ok := suite.inferBodyPayload(op)
		if !ok || len(required) == 0 {
			continue
		}
		path, ok := suite.resolvePath(op)
		if !ok {
			continue
		}
		delete(body, required[0])

		t.Run(op.ref.OperationID, func(t *testing.T) {
			resp, data, err := suite.doJSON(context.Background(), op.ref.Method, path, suite.token, body)
			require.NoError(t, err)
			if resp.StatusCode >= 500 && resp.StatusCode != http.StatusNotImplemented && resp.StatusCode != http.StatusBadGateway {
				suite.recordFinding("server_bug", "error", op, path, resp.StatusCode, "malformed required-body request returned server error")
				t.Fatalf("unexpected server error for malformed %s %s: %d %s", op.ref.Method, path, resp.StatusCode, string(data))
			}
			if resp.StatusCode < 400 {
				suite.recordFinding("validation_gap", "warning", op, path, resp.StatusCode, "malformed required-body request succeeded")
				t.Logf("generated malformed-body probe observed permissive success %d for %s %s", resp.StatusCode, op.ref.Method, path)
				return
			}
			if !bodyStatusAllowed(op, resp.StatusCode) {
				suite.recordFinding("openapi_drift", "info", op, path, resp.StatusCode, "malformed required-body request returned undocumented status")
				t.Logf("generated malformed-body probe observed undocumented status %d for %s %s", resp.StatusCode, op.ref.Method, path)
			}
		})
		executed++
		if limit > 0 && executed >= limit {
			break
		}
	}

	if limit > 0 {
		require.Greater(t, executed, 0, "expected at least one generated malformed-body check to run with E2E_LIVE_LIMIT")
	} else {
		require.GreaterOrEqual(t, executed, 5, "expected generated malformed-body coverage to execute")
	}
}

func TestGeneratedLive_InvalidBodyVariantsReportCleanly(t *testing.T) {
	ops := mergedOperations(t)
	executed := 0
	limit := suite.operationLimit()

	for _, op := range ops {
		if !suite.operationFilterAllowed(op.ref.Tags) {
			continue
		}
		switch strings.ToUpper(op.ref.Method) {
		case http.MethodPost, http.MethodPut, http.MethodPatch:
		default:
			continue
		}
		path, ok := suite.resolvePath(op)
		if !ok {
			continue
		}
		variants := invalidBodyVariants(op)
		if len(variants) == 0 {
			continue
		}

		t.Run(op.ref.OperationID, func(t *testing.T) {
			for _, variant := range variants {
				variant := variant
				t.Run(variant.name, func(t *testing.T) {
					resp, data, err := suite.doJSON(context.Background(), op.ref.Method, path, suite.token, variant.body)
					require.NoError(t, err)
					if resp.StatusCode >= 500 && resp.StatusCode != http.StatusNotImplemented && resp.StatusCode != http.StatusBadGateway {
						suite.recordFinding("server_bug", "error", op, path, resp.StatusCode, fmt.Sprintf("invalid body variant %q returned server error", variant.name))
						t.Fatalf("unexpected server error for invalid %s variant %s %s: %d %s", op.ref.Method, variant.name, path, resp.StatusCode, string(data))
					}
					if resp.StatusCode < 400 {
						suite.recordFinding("validation_gap", "warning", op, path, resp.StatusCode, fmt.Sprintf("invalid body variant %q succeeded", variant.name))
						t.Logf("generated invalid-body variant %q observed permissive success %d for %s %s", variant.name, resp.StatusCode, op.ref.Method, path)
						return
					}
					if !bodyStatusAllowed(op, resp.StatusCode) {
						suite.recordFinding("openapi_drift", "info", op, path, resp.StatusCode, fmt.Sprintf("invalid body variant %q returned undocumented status", variant.name))
					}
				})
			}
		})
		executed++
		if limit > 0 && executed >= limit {
			break
		}
	}

	if limit > 0 {
		require.Greater(t, executed, 0, "expected at least one generated invalid-body variant check to run with E2E_LIVE_LIMIT")
	} else {
		require.GreaterOrEqual(t, executed, 5, "expected generated invalid-body variant coverage to execute")
	}
}

func TestGeneratedLive_PaginationProbes(t *testing.T) {
	ops := mergedOperations(t)
	executed := 0
	limit := suite.operationLimit()

	for _, op := range ops {
		if !suite.operationFilterAllowed(op.ref.Tags) {
			continue
		}
		if !strings.EqualFold(op.ref.Method, http.MethodGet) || !hasParam(op, "max_results") {
			continue
		}
		path, ok := suite.buildQueryPath(op, nil)
		if !ok {
			continue
		}

		t.Run(op.ref.OperationID, func(t *testing.T) {
			resp, data, err := suite.doJSON(context.Background(), http.MethodGet, path, suite.token, nil)
			require.NoError(t, err)
			require.Equalf(t, http.StatusOK, resp.StatusCode, "pagination probe failed for %s: %s", path, string(data))

			var payload map[string]any
			require.NoError(t, json.Unmarshal(data, &payload))

			nextToken, _ := payload["next_page_token"].(string)
			if nextToken == "" {
				return
			}

			followup, ok := suite.buildQueryPath(op, map[string][]string{"page_token": {nextToken}})
			require.True(t, ok)
			resp, data, err = suite.doJSON(context.Background(), http.MethodGet, followup, suite.token, nil)
			require.NoError(t, err)
			assert.Equalf(t, http.StatusOK, resp.StatusCode, "follow-up pagination probe failed for %s: %s", path, string(data))
		})
		executed++
		if limit > 0 && executed >= limit {
			break
		}
	}

	if limit > 0 {
		require.Greater(t, executed, 0, "expected at least one generated pagination probe to run with E2E_LIVE_LIMIT")
	} else {
		require.GreaterOrEqual(t, executed, 5, "expected generated pagination coverage to execute")
	}
}

func TestGeneratedLive_InvalidPaginationProbes(t *testing.T) {
	ops := mergedOperations(t)
	executed := 0
	limit := suite.operationLimit()

	for _, op := range ops {
		if !suite.operationFilterAllowed(op.ref.Tags) {
			continue
		}
		if !strings.EqualFold(op.ref.Method, http.MethodGet) || !hasParam(op, "max_results") {
			continue
		}
		basePath, ok := suite.buildQueryPath(op, nil)
		if !ok {
			continue
		}

		t.Run(op.ref.OperationID, func(t *testing.T) {
			for _, tc := range []struct {
				name  string
				extra map[string][]string
			}{
				{name: "negative_max_results", extra: map[string][]string{"max_results": {"-1"}}},
				{name: "invalid_page_token", extra: map[string][]string{"page_token": {"__livee2e_invalid_page_token__"}}},
			} {
				tc := tc
				t.Run(tc.name, func(t *testing.T) {
					path, ok := suite.buildQueryPath(op, tc.extra)
					require.True(t, ok)
					resp, data, err := suite.doJSON(context.Background(), http.MethodGet, path, suite.token, nil)
					require.NoError(t, err)
					if resp.StatusCode >= 500 {
						suite.recordFinding("server_bug", "error", op, path, resp.StatusCode, fmt.Sprintf("invalid pagination variant %q returned server error", tc.name))
						t.Fatalf("unexpected server error for invalid pagination %s on %s: %d %s", tc.name, basePath, resp.StatusCode, string(data))
					}
					if resp.StatusCode < 400 {
						suite.recordFinding("validation_gap", "warning", op, path, resp.StatusCode, fmt.Sprintf("invalid pagination variant %q succeeded", tc.name))
						t.Logf("generated invalid pagination variant %q observed permissive success %d for %s", tc.name, resp.StatusCode, path)
						return
					}
					if !bodyStatusAllowed(op, resp.StatusCode) {
						suite.recordFinding("openapi_drift", "info", op, path, resp.StatusCode, fmt.Sprintf("invalid pagination variant %q returned undocumented status", tc.name))
					}
				})
			}
		})
		executed++
		if limit > 0 && executed >= limit {
			break
		}
	}

	if limit > 0 {
		require.Greater(t, executed, 0, "expected at least one generated invalid pagination probe to run with E2E_LIVE_LIMIT")
	} else {
		require.GreaterOrEqual(t, executed, 5, "expected generated invalid pagination coverage to execute")
	}
}

func TestGeneratedLive_DuplicateCreateProbes(t *testing.T) {
	ops := mergedOperations(t)
	executed := 0
	limit := suite.operationLimit()

	for _, op := range ops {
		if !suite.operationFilterAllowed(op.ref.Tags) {
			continue
		}
		if !strings.EqualFold(op.ref.Method, http.MethodPost) {
			continue
		}
		if _, ok := op.documentedCode[409]; !ok {
			continue
		}
		if !eligibleDuplicateCreate(op) {
			continue
		}
		path, ok := suite.resolvePath(op)
		if !ok {
			continue
		}
		body, _, ok := suite.inferBodyPayload(op)
		if !ok {
			continue
		}
		body, ok = suite.makeDuplicateProbeBody(op, body)
		if !ok {
			continue
		}

		t.Run(op.ref.OperationID, func(t *testing.T) {
			resp, data, err := suite.doJSON(context.Background(), http.MethodPost, path, suite.token, body)
			require.NoError(t, err)
			if resp.StatusCode >= 500 {
				suite.recordFinding("server_bug", "error", op, path, resp.StatusCode, "initial duplicate-create probe returned server error")
				t.Fatalf("unexpected server error on initial create for %s: %d %s", path, resp.StatusCode, string(data))
			}
			require.Contains(t, []int{http.StatusCreated, http.StatusConflict, http.StatusOK, http.StatusAccepted}, resp.StatusCode, "unexpected initial duplicate-create status for %s: %s", path, string(data))

			resp, data, err = suite.doJSON(context.Background(), http.MethodPost, path, suite.token, body)
			require.NoError(t, err)
			if resp.StatusCode >= 500 {
				suite.recordFinding("server_bug", "error", op, path, resp.StatusCode, "duplicate-create probe returned server error")
				t.Fatalf("unexpected server error on duplicate create for %s: %d %s", path, resp.StatusCode, string(data))
			}
			if resp.StatusCode < 400 {
				suite.recordFinding("validation_gap", "warning", op, path, resp.StatusCode, "duplicate-create probe succeeded")
				t.Logf("generated duplicate-create probe observed permissive success %d for %s", resp.StatusCode, path)
				return
			}
			if resp.StatusCode != http.StatusConflict && !bodyStatusAllowed(op, resp.StatusCode) {
				suite.recordFinding("openapi_drift", "info", op, path, resp.StatusCode, "duplicate-create probe returned undocumented status")
			}
		})
		executed++
		if limit > 0 && executed >= limit {
			break
		}
	}

	if limit > 0 {
		require.Greater(t, executed, 0, "expected at least one generated duplicate-create probe to run with E2E_LIVE_LIMIT")
	} else {
		require.GreaterOrEqual(t, executed, 1, "expected generated duplicate-create coverage to execute")
	}
}

func TestGeneratedLive_FixtureCoverageSummary(t *testing.T) {
	t.Parallel()

	assert.NotEmpty(t, suite.host)
	assert.NotEmpty(t, suite.token)
	assert.NotEmpty(t, suite.userToken)
	assert.NotEmpty(t, suite.fixtures["adminPrincipalId"])
	assert.NotEmpty(t, suite.fixtures["principalId"])
	assert.NotEmpty(t, suite.fixtures["groupId"])
	assert.NotEmpty(t, suite.fixtures["tagId"])
	t.Logf("generated live fixtures: %v", suite.fixtures)
	t.Logf("generated live host: %s", suite.host)
	if suite.findingsPath != "" {
		t.Logf("generated findings output: %s", suite.findingsPath)
	}
	if suite.logPath != "" {
		t.Logf("managed task dev log: %s", suite.logPath)
	}
}

func TestGeneratedLive_FindingsSummary(t *testing.T) {
	t.Parallel()

	suite.mu.Lock()
	findings := append([]liveFinding(nil), suite.findings...)
	suite.mu.Unlock()

	counts := map[string]int{}
	for _, finding := range findings {
		counts[finding.Category]++
	}
	keys := make([]string, 0, len(counts))
	for key := range counts {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		t.Logf("generated finding %s: %d", key, counts[key])
	}
}

func TestGeneratedLive_OpenAPISpecMatchesEmbeddedDiscoveryDocument(t *testing.T) {
	t.Parallel()

	resp, data, err := suite.doJSON(context.Background(), http.MethodGet, "/openapi.json", "", nil)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)

	rootDir, err := repoRoot()
	require.NoError(t, err)
	content, err := os.ReadFile(filepath.Join(rootDir, "internal", "api", "gen", "openapi.yaml"))
	require.NoError(t, err)
	jsonBytes := yamlToJSONBytes(t, content)
	var liveSpec map[string]any
	var embeddedSpec map[string]any
	require.NoError(t, json.Unmarshal(data, &liveSpec))
	require.NoError(t, json.Unmarshal(jsonBytes, &embeddedSpec))

	require.Equal(t, embeddedSpec["openapi"], liveSpec["openapi"])
	require.Equal(t, embeddedSpec["paths"].(map[string]any) != nil, liveSpec["paths"].(map[string]any) != nil)
}

func TestGeneratedLive_SeededContractEndpoints(t *testing.T) {
	cases := []struct {
		name string
		path string
	}{
		{name: "principal_get", path: fmt.Sprintf("/v1/principals/%s", suite.fixtures["principalId"])},
		{name: "group_get", path: fmt.Sprintf("/v1/groups/%s", suite.fixtures["groupId"])},
		{name: "tag_assignments_list", path: fmt.Sprintf("/v1/tags/%s/assignments", suite.fixtures["tagId"])},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			resp, data, err := suite.doJSON(context.Background(), http.MethodGet, tc.path, suite.token, nil)
			require.NoError(t, err)
			assert.Lessf(t, resp.StatusCode, 500, "seeded contract endpoint failed: %s", string(data))
		})
	}
}

type invalidBodyVariant struct {
	name string
	body map[string]any
}

func invalidBodyVariants(op liveOperation) []invalidBodyVariant {
	validBody, required, ok := suite.inferBodyPayload(op)
	if !ok || len(validBody) == 0 {
		return nil
	}
	variants := make([]invalidBodyVariant, 0, 4)
	if len(required) > 0 {
		missing := cloneMap(validBody)
		delete(missing, required[0])
		variants = append(variants, invalidBodyVariant{name: "missing_required", body: missing})
	}
	content := op.spec.RequestBody.Value.Content["application/json"]
	if content.Schema == nil || content.Schema.Value == nil {
		return dedupeInvalidVariants(variants)
	}
	schema := content.Schema.Value
	for _, field := range schema.Required {
		prop := schema.Properties[field]
		if prop == nil || prop.Value == nil {
			continue
		}
		if len(prop.Value.Enum) > 0 {
			enumVariant := cloneMap(validBody)
			enumVariant[field] = "__livee2e_invalid_enum__"
			variants = append(variants, invalidBodyVariant{name: "invalid_enum_" + field, body: enumVariant})
			break
		}
	}
	for _, field := range schema.Required {
		prop := schema.Properties[field]
		if prop == nil || prop.Value == nil || prop.Value.Type == nil {
			continue
		}
		wrong, ok := incompatibleValue(prop.Value)
		if !ok {
			continue
		}
		wrongTypeVariant := cloneMap(validBody)
		wrongTypeVariant[field] = wrong
		variants = append(variants, invalidBodyVariant{name: "wrong_type_" + field, body: wrongTypeVariant})
		break
	}
	for _, field := range schema.Required {
		prop := schema.Properties[field]
		if prop == nil || prop.Value == nil || prop.Value.Type == nil || !prop.Value.Type.Is("string") {
			continue
		}
		switch prop.Value.Format {
		case "uuid":
			formatVariant := cloneMap(validBody)
			formatVariant[field] = "not-a-uuid"
			variants = append(variants, invalidBodyVariant{name: "bad_uuid_" + field, body: formatVariant})
		case "date-time":
			formatVariant := cloneMap(validBody)
			formatVariant[field] = "not-a-date-time"
			variants = append(variants, invalidBodyVariant{name: "bad_datetime_" + field, body: formatVariant})
		case "date":
			formatVariant := cloneMap(validBody)
			formatVariant[field] = "not-a-date"
			variants = append(variants, invalidBodyVariant{name: "bad_date_" + field, body: formatVariant})
		}
	}
	return dedupeInvalidVariants(variants)
}

func dedupeInvalidVariants(in []invalidBodyVariant) []invalidBodyVariant {
	seen := map[string]struct{}{}
	out := make([]invalidBodyVariant, 0, len(in))
	for _, variant := range in {
		if _, ok := seen[variant.name]; ok {
			continue
		}
		seen[variant.name] = struct{}{}
		out = append(out, variant)
	}
	return out
}

func incompatibleValue(schema *openapi3.Schema) (any, bool) {
	if schema == nil || schema.Type == nil {
		return nil, false
	}
	switch {
	case schema.Type.Is("string"):
		return map[string]any{"invalid": true}, true
	case schema.Type.Is("integer"), schema.Type.Is("number"):
		return "not-a-number", true
	case schema.Type.Is("boolean"):
		return "not-a-boolean", true
	case schema.Type.Is("array"):
		return "not-an-array", true
	case schema.Type.Is("object"):
		return "not-an-object", true
	}
	return nil, false
}

func cloneMap(in map[string]any) map[string]any {
	out := make(map[string]any, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

func isAdminOnlyOperation(op liveOperation) bool {
	return strings.EqualFold(strings.TrimSpace(op.contract.AuthzMode), "admin_only")
}

func (s *liveSuite) crossPrincipalProbe(op liveOperation) (map[string][]string, int, bool) {
	if !strings.EqualFold(op.ref.Method, http.MethodGet) {
		return nil, 0, false
	}
	adminPrincipalID := strings.TrimSpace(s.fixtures["adminPrincipalId"])
	userPrincipalID := strings.TrimSpace(s.fixtures["principalId"])
	if adminPrincipalID == "" || userPrincipalID == "" || adminPrincipalID == userPrincipalID {
		return nil, 0, false
	}

	switch op.ref.OperationID {
	case "listAPIKeys":
		return map[string][]string{"principal_id": {adminPrincipalID}}, http.StatusForbidden, true
	default:
		return nil, 0, false
	}
}

func (s *liveSuite) makeDuplicateProbeBody(op liveOperation, body map[string]any) (map[string]any, bool) {
	candidate := cloneMap(body)
	suffix := fmt.Sprintf("livee2e-dup-%s-%s", strings.ToLower(op.ref.OperationID), s.runID)
	set := false
	for _, field := range []string{"name", "slug", "username", "key"} {
		if _, ok := candidate[field]; ok {
			candidate[field] = suffix
			set = true
		}
	}
	if value, ok := candidate["value"]; ok {
		if _, isString := value.(string); isString {
			candidate["value"] = "generated"
		}
	}
	if set {
		return candidate, true
	}
	return nil, false
}

func eligibleDuplicateCreate(op liveOperation) bool {
	if strings.Contains(strings.ToLower(op.ref.Path), "/auth/") {
		return false
	}
	for _, tag := range op.ref.Tags {
		switch strings.ToLower(tag) {
		case "auth", "security":
			return false
		}
	}
	return true
}
