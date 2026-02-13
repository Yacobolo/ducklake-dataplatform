//go:build integration

package integration

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// === CRUD ===

func TestHTTP_ComputeEndpointCRUD(t *testing.T) {
	env := setupHTTPServer(t, httpTestOpts{WithComputeEndpoints: true})

	steps := []struct {
		name string
		fn   func(t *testing.T)
	}{
		{"create_remote_endpoint", func(t *testing.T) {
			body := map[string]interface{}{
				"name":       "crud-remote",
				"url":        "http://localhost:9999",
				"type":       "REMOTE",
				"size":       "MEDIUM",
				"auth_token": "secret-123",
			}
			resp := doRequest(t, "POST", env.Server.URL+"/v1/compute-endpoints", env.Keys.Admin, body)
			require.Equal(t, 201, resp.StatusCode)

			var result map[string]interface{}
			decodeJSON(t, resp, &result)
			assert.Equal(t, "crud-remote", result["name"])
			assert.Equal(t, "REMOTE", result["type"])
			assert.Equal(t, "INACTIVE", result["status"])
			assert.NotEmpty(t, result["external_id"])
			assert.Equal(t, "admin_user", result["owner"])
			// auth_token must NOT appear in response
			_, hasToken := result["auth_token"]
			assert.False(t, hasToken, "auth_token must not be in API response")
		}},
		{"create_local_endpoint", func(t *testing.T) {
			body := map[string]interface{}{
				"name":       "crud-local",
				"url":        "local://embedded",
				"type":       "LOCAL",
				"auth_token": "unused",
			}
			resp := doRequest(t, "POST", env.Server.URL+"/v1/compute-endpoints", env.Keys.Admin, body)
			require.Equal(t, 201, resp.StatusCode)
			_ = resp.Body.Close()
		}},
		{"get_by_name", func(t *testing.T) {
			resp := doRequest(t, "GET", env.Server.URL+"/v1/compute-endpoints/crud-remote", env.Keys.Admin, nil)
			require.Equal(t, 200, resp.StatusCode)

			var result map[string]interface{}
			decodeJSON(t, resp, &result)
			assert.Equal(t, "crud-remote", result["name"])
			assert.Equal(t, "REMOTE", result["type"])
		}},
		{"list", func(t *testing.T) {
			resp := doRequest(t, "GET", env.Server.URL+"/v1/compute-endpoints", env.Keys.Admin, nil)
			require.Equal(t, 200, resp.StatusCode)

			var result map[string]interface{}
			decodeJSON(t, resp, &result)
			data := result["data"].([]interface{})
			require.GreaterOrEqual(t, len(data), 2)
		}},
		{"update_status_active", func(t *testing.T) {
			body := map[string]interface{}{"status": "ACTIVE"}
			resp := doRequest(t, "PATCH", env.Server.URL+"/v1/compute-endpoints/crud-remote", env.Keys.Admin, body)
			require.Equal(t, 200, resp.StatusCode)

			var result map[string]interface{}
			decodeJSON(t, resp, &result)
			assert.Equal(t, "ACTIVE", result["status"])
		}},
		{"update_fields", func(t *testing.T) {
			body := map[string]interface{}{"size": "LARGE"}
			resp := doRequest(t, "PATCH", env.Server.URL+"/v1/compute-endpoints/crud-remote", env.Keys.Admin, body)
			require.Equal(t, 200, resp.StatusCode)

			var result map[string]interface{}
			decodeJSON(t, resp, &result)
			assert.Equal(t, "LARGE", result["size"])
		}},
		{"delete", func(t *testing.T) {
			resp := doRequest(t, "DELETE", env.Server.URL+"/v1/compute-endpoints/crud-remote", env.Keys.Admin, nil)
			require.Equal(t, 204, resp.StatusCode)
			_ = resp.Body.Close()
		}},
		{"get_after_delete_404", func(t *testing.T) {
			resp := doRequest(t, "GET", env.Server.URL+"/v1/compute-endpoints/crud-remote", env.Keys.Admin, nil)
			assert.Equal(t, 404, resp.StatusCode)
			_ = resp.Body.Close()
		}},
	}

	for _, s := range steps {
		if !t.Run(s.name, s.fn) {
			t.FailNow()
		}
	}
}

// === Authorization ===

func TestHTTP_ComputeEndpointAuthorization(t *testing.T) {
	env := setupHTTPServer(t, httpTestOpts{WithComputeEndpoints: true})

	// Pre-create as admin
	body := map[string]interface{}{
		"name": "auth-ep", "url": "http://x", "type": "REMOTE", "auth_token": "s",
	}
	resp := doRequest(t, "POST", env.Server.URL+"/v1/compute-endpoints", env.Keys.Admin, body)
	require.Equal(t, 201, resp.StatusCode)
	_ = resp.Body.Close()

	tests := []struct {
		name       string
		method     string
		path       string
		apiKey     string
		body       interface{}
		wantStatus int
	}{
		{"analyst_create_403", "POST", "/v1/compute-endpoints", env.Keys.Analyst,
			map[string]interface{}{"name": "x", "url": "http://x", "type": "REMOTE", "auth_token": "s"}, 403},
		{"noaccess_create_403", "POST", "/v1/compute-endpoints", env.Keys.NoAccess,
			map[string]interface{}{"name": "x", "url": "http://x", "type": "REMOTE", "auth_token": "s"}, 403},
		{"analyst_delete_403", "DELETE", "/v1/compute-endpoints/auth-ep", env.Keys.Analyst, nil, 403},
		{"analyst_update_403", "PATCH", "/v1/compute-endpoints/auth-ep", env.Keys.Analyst,
			map[string]interface{}{"status": "ACTIVE"}, 403},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			resp := doRequest(t, tc.method, env.Server.URL+tc.path, tc.apiKey, tc.body)
			assert.Equal(t, tc.wantStatus, resp.StatusCode)
			_ = resp.Body.Close()
		})
	}
}

// === Validation ===

func TestHTTP_ComputeEndpointValidation(t *testing.T) {
	env := setupHTTPServer(t, httpTestOpts{WithComputeEndpoints: true})

	tests := []struct {
		name       string
		body       map[string]interface{}
		wantStatus int
	}{
		{"empty_name", map[string]interface{}{"name": "", "url": "http://x", "type": "REMOTE"}, 400},
		{"empty_url", map[string]interface{}{"name": "x", "url": "", "type": "REMOTE"}, 400},
		{"invalid_type", map[string]interface{}{"name": "x", "url": "http://x", "type": "INVALID"}, 400},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			resp := doRequest(t, "POST", env.Server.URL+"/v1/compute-endpoints", env.Keys.Admin, tc.body)
			assert.Equal(t, tc.wantStatus, resp.StatusCode)
			_ = resp.Body.Close()
		})
	}
}

// === Duplicate ===

func TestHTTP_ComputeDuplicateEndpoint(t *testing.T) {
	env := setupHTTPServer(t, httpTestOpts{WithComputeEndpoints: true})

	body := map[string]interface{}{
		"name": "dup-ep", "url": "http://x", "type": "REMOTE", "auth_token": "s",
	}

	resp := doRequest(t, "POST", env.Server.URL+"/v1/compute-endpoints", env.Keys.Admin, body)
	require.Equal(t, 201, resp.StatusCode)
	_ = resp.Body.Close()

	resp = doRequest(t, "POST", env.Server.URL+"/v1/compute-endpoints", env.Keys.Admin, body)
	assert.Equal(t, 409, resp.StatusCode)
	_ = resp.Body.Close()
}

// === Not Found ===

func TestHTTP_ComputeEndpointNotFound(t *testing.T) {
	env := setupHTTPServer(t, httpTestOpts{WithComputeEndpoints: true})

	resp := doRequest(t, "GET", env.Server.URL+"/v1/compute-endpoints/nonexistent", env.Keys.Admin, nil)
	assert.Equal(t, 404, resp.StatusCode)
	_ = resp.Body.Close()
}

// === Assignments ===

func TestHTTP_ComputeAssignmentCRUD(t *testing.T) {
	env := setupHTTPServer(t, httpTestOpts{WithComputeEndpoints: true})

	// Create endpoint
	resp := doRequest(t, "POST", env.Server.URL+"/v1/compute-endpoints", env.Keys.Admin,
		map[string]interface{}{
			"name": "assign-ep", "url": "http://x", "type": "REMOTE", "auth_token": "s",
		})
	require.Equal(t, 201, resp.StatusCode)
	_ = resp.Body.Close()

	// Look up admin_user principal ID
	adminID := lookupPrincipalID(t, env, "admin_user")
	require.NotZero(t, adminID)

	var assignmentID string

	t.Run("create_assignment", func(t *testing.T) {
		body := map[string]interface{}{
			"principal_id":   adminID,
			"principal_type": "user",
			"is_default":     true,
		}
		resp := doRequest(t, "POST", env.Server.URL+"/v1/compute-endpoints/assign-ep/assignments", env.Keys.Admin, body)
		require.Equal(t, 201, resp.StatusCode)

		var result map[string]interface{}
		decodeJSON(t, resp, &result)
		assert.Equal(t, adminID, result["principal_id"])
		assert.Equal(t, true, result["is_default"])
		assignmentID = result["id"].(string)
	})

	t.Run("list_assignments", func(t *testing.T) {
		resp := doRequest(t, "GET", env.Server.URL+"/v1/compute-endpoints/assign-ep/assignments", env.Keys.Admin, nil)
		require.Equal(t, 200, resp.StatusCode)

		var result map[string]interface{}
		decodeJSON(t, resp, &result)
		data := result["data"].([]interface{})
		require.Len(t, data, 1)
	})

	t.Run("delete_assignment", func(t *testing.T) {
		resp := doRequest(t, "DELETE",
			fmt.Sprintf("%s/v1/compute-endpoints/assign-ep/assignments/%s",
				env.Server.URL, assignmentID),
			env.Keys.Admin, nil)
		require.Equal(t, 204, resp.StatusCode)
		_ = resp.Body.Close()
	})

	t.Run("list_assignments_empty", func(t *testing.T) {
		resp := doRequest(t, "GET", env.Server.URL+"/v1/compute-endpoints/assign-ep/assignments", env.Keys.Admin, nil)
		require.Equal(t, 200, resp.StatusCode)

		var result map[string]interface{}
		decodeJSON(t, resp, &result)
		data := result["data"].([]interface{})
		assert.Len(t, data, 0)
	})
}

// === E2E: Remote Query ===

func TestHTTP_ComputeRemoteQueryE2E(t *testing.T) {
	// 1. Start in-process compute agent
	agentEnv := startTestAgent(t)

	// 2. Start control plane with compute endpoint support
	env := setupHTTPServer(t, httpTestOpts{WithComputeEndpoints: true})

	// 3. Create + activate REMOTE endpoint pointing to agent
	setupRemoteEndpoint(t, env, agentEnv, "e2e-remote")

	// 4. Assign admin_user to the remote endpoint
	adminID := lookupPrincipalID(t, env, "admin_user")
	assignToEndpoint(t, env, "e2e-remote", adminID, "user")

	// 5. Execute a query via POST /v1/query — should route through remote agent
	t.Run("remote_select_42", func(t *testing.T) {
		queryBody := map[string]interface{}{"sql": "SELECT 42 AS answer"}
		resp := doRequest(t, "POST", env.Server.URL+"/v1/query", env.Keys.Admin, queryBody)
		require.Equal(t, 200, resp.StatusCode)

		var result map[string]interface{}
		decodeJSON(t, resp, &result)

		columns := result["columns"].([]interface{})
		require.Len(t, columns, 1)
		assert.Equal(t, "answer", columns[0])

		rows := result["rows"].([]interface{})
		require.Len(t, rows, 1)
		row := rows[0].([]interface{})
		// Remote materializes as VARCHAR, so value is a string "42"
		assert.Equal(t, "42", fmt.Sprintf("%v", row[0]))

		rowCount := result["row_count"].(float64)
		assert.Equal(t, float64(1), rowCount)
	})

	// 6. Multi-row query through remote
	t.Run("remote_range_query", func(t *testing.T) {
		queryBody := map[string]interface{}{"sql": "SELECT i FROM range(5) t(i)"}
		resp := doRequest(t, "POST", env.Server.URL+"/v1/query", env.Keys.Admin, queryBody)
		require.Equal(t, 200, resp.StatusCode)

		var result map[string]interface{}
		decodeJSON(t, resp, &result)

		rowCount := result["row_count"].(float64)
		assert.Equal(t, float64(5), rowCount)
	})

	// 7. Health check through the control plane API
	t.Run("health_check_via_api", func(t *testing.T) {
		resp := doRequest(t, "GET", env.Server.URL+"/v1/compute-endpoints/e2e-remote/health",
			env.Keys.Admin, nil)
		require.Equal(t, 200, resp.StatusCode)

		var result map[string]interface{}
		decodeJSON(t, resp, &result)
		assert.Equal(t, "ok", result["status"])
		assert.Equal(t, "e2e-remote", result["endpoint_name"])
	})
}

// === E2E: Local Endpoint ===

func TestHTTP_ComputeLocalEndpointE2E(t *testing.T) {
	env := setupHTTPServer(t, httpTestOpts{WithComputeEndpoints: true})

	// Create LOCAL endpoint
	resp := doRequest(t, "POST", env.Server.URL+"/v1/compute-endpoints", env.Keys.Admin,
		map[string]interface{}{
			"name":       "local-ep",
			"url":        "local://embedded",
			"type":       "LOCAL",
			"auth_token": "unused",
		})
	require.Equal(t, 201, resp.StatusCode)
	_ = resp.Body.Close()

	// Activate
	resp = doRequest(t, "PATCH", env.Server.URL+"/v1/compute-endpoints/local-ep",
		env.Keys.Admin, map[string]interface{}{"status": "ACTIVE"})
	require.Equal(t, 200, resp.StatusCode)
	_ = resp.Body.Close()

	// Assign admin
	adminID := lookupPrincipalID(t, env, "admin_user")
	assignToEndpoint(t, env, "local-ep", adminID, "user")

	// Query — should work via local executor (result types are native, not VARCHAR)
	queryBody := map[string]interface{}{"sql": "SELECT 42 AS answer"}
	resp = doRequest(t, "POST", env.Server.URL+"/v1/query", env.Keys.Admin, queryBody)
	require.Equal(t, 200, resp.StatusCode)

	var result map[string]interface{}
	decodeJSON(t, resp, &result)
	rows := result["rows"].([]interface{})
	require.Len(t, rows, 1)
	row := rows[0].([]interface{})
	// Local DuckDB returns native int, JSON-serialized as float64
	assert.Equal(t, float64(42), row[0])
}

// === E2E: Agent Unreachable ===

func TestHTTP_ComputeRemoteAgentDown(t *testing.T) {
	env := setupHTTPServer(t, httpTestOpts{WithComputeEndpoints: true})

	// Create endpoint pointing to a non-existent URL
	resp := doRequest(t, "POST", env.Server.URL+"/v1/compute-endpoints", env.Keys.Admin,
		map[string]interface{}{
			"name":       "dead-agent",
			"url":        "http://127.0.0.1:1",
			"type":       "REMOTE",
			"auth_token": "secret",
		})
	require.Equal(t, 201, resp.StatusCode)
	_ = resp.Body.Close()

	// Activate
	resp = doRequest(t, "PATCH", env.Server.URL+"/v1/compute-endpoints/dead-agent",
		env.Keys.Admin, map[string]interface{}{"status": "ACTIVE"})
	require.Equal(t, 200, resp.StatusCode)
	_ = resp.Body.Close()

	// Assign admin_user
	adminID := lookupPrincipalID(t, env, "admin_user")
	assignToEndpoint(t, env, "dead-agent", adminID, "user")

	// Query should fail (resolver health check fails → error propagated)
	queryBody := map[string]interface{}{"sql": "SELECT 1"}
	resp = doRequest(t, "POST", env.Server.URL+"/v1/query", env.Keys.Admin, queryBody)
	// The engine error gets mapped to 403 by the query handler (see handler.go)
	assert.NotEqual(t, 200, resp.StatusCode)
	_ = resp.Body.Close()

	// Health check via API should return 502 (agent unreachable)
	resp = doRequest(t, "GET", env.Server.URL+"/v1/compute-endpoints/dead-agent/health",
		env.Keys.Admin, nil)
	assert.Equal(t, 502, resp.StatusCode)
	_ = resp.Body.Close()
}

// === E2E: Unassign Reverts to Local ===

func TestHTTP_ComputeUnassignReverts(t *testing.T) {
	agentEnv := startTestAgent(t)
	env := setupHTTPServer(t, httpTestOpts{WithComputeEndpoints: true})

	// Setup remote endpoint + assignment
	setupRemoteEndpoint(t, env, agentEnv, "revert-ep")
	adminID := lookupPrincipalID(t, env, "admin_user")

	// Create assignment, capture ID
	resp := doRequest(t, "POST", env.Server.URL+"/v1/compute-endpoints/revert-ep/assignments",
		env.Keys.Admin, map[string]interface{}{
			"principal_id": adminID, "principal_type": "user", "is_default": true,
		})
	require.Equal(t, 201, resp.StatusCode)
	var assignResult map[string]interface{}
	decodeJSON(t, resp, &assignResult)
	assignmentID := assignResult["id"].(string)

	// Query via remote — should succeed
	resp = doRequest(t, "POST", env.Server.URL+"/v1/query", env.Keys.Admin,
		map[string]interface{}{"sql": "SELECT 42 AS answer"})
	require.Equal(t, 200, resp.StatusCode)
	_ = resp.Body.Close()

	// Unassign
	resp = doRequest(t, "DELETE",
		fmt.Sprintf("%s/v1/compute-endpoints/revert-ep/assignments/%s", env.Server.URL, assignmentID),
		env.Keys.Admin, nil)
	require.Equal(t, 204, resp.StatusCode)
	_ = resp.Body.Close()

	// Query again — should fall back to local DuckDB and succeed
	resp = doRequest(t, "POST", env.Server.URL+"/v1/query", env.Keys.Admin,
		map[string]interface{}{"sql": "SELECT 99 AS local_answer"})
	require.Equal(t, 200, resp.StatusCode)

	var result map[string]interface{}
	decodeJSON(t, resp, &result)
	columns := result["columns"].([]interface{})
	assert.Equal(t, "local_answer", columns[0])
	// After unassign, local DuckDB returns native int → float64
	rows := result["rows"].([]interface{})
	row := rows[0].([]interface{})
	assert.Equal(t, float64(99), row[0])
}

// === Health Check: LOCAL always ok ===

func TestHTTP_ComputeLocalHealthCheck(t *testing.T) {
	env := setupHTTPServer(t, httpTestOpts{WithComputeEndpoints: true})

	// Create LOCAL endpoint
	resp := doRequest(t, "POST", env.Server.URL+"/v1/compute-endpoints", env.Keys.Admin,
		map[string]interface{}{
			"name": "health-local", "url": "local://", "type": "LOCAL", "auth_token": "x",
		})
	require.Equal(t, 201, resp.StatusCode)
	_ = resp.Body.Close()

	// Health check should return 200 with status "ok"
	resp = doRequest(t, "GET", env.Server.URL+"/v1/compute-endpoints/health-local/health",
		env.Keys.Admin, nil)
	require.Equal(t, 200, resp.StatusCode)

	var result map[string]interface{}
	decodeJSON(t, resp, &result)
	assert.Equal(t, "ok", result["status"])
}
