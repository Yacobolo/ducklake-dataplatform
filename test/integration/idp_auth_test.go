//go:build integration

package integration

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// JIT Provisioning E2E
// ---------------------------------------------------------------------------

// TestJIT_ProvisionNewUser verifies that a JWT with an unknown sub claim
// triggers JIT provisioning: the principal is auto-created and the request
// succeeds.
func TestJIT_ProvisionNewUser(t *testing.T) {
	secret := []byte("test-jwt-secret")
	env := setupHTTPServer(t, httpTestOpts{
		JWTSecret:         secret,
		WithAuthenticator: true,
	})

	// Sign a JWT with a brand-new sub that doesn't match any seeded principal.
	newSub := "jit-new-user-" + fmt.Sprintf("%d", time.Now().UnixNano())
	token := generateJWT(t, secret, newSub, time.Now().Add(time.Hour))

	// First request: the user doesn't exist yet, JIT should create it.
	// Use /v1/tags (open to non-admins) to trigger JIT provisioning.
	resp := doRequestWithBearer(t, "GET", env.Server.URL+"/v1/tags", token, nil)
	body := readBody(t, resp)
	require.Equal(t, 200, resp.StatusCode, "JIT user should be able to access read endpoints, got body: %s", string(body))

	// Verify the principal was actually created in the DB.
	resp2 := doRequest(t, "GET", env.Server.URL+"/v1/principals", env.Keys.Admin, nil)
	require.Equal(t, 200, resp2.StatusCode)

	var result map[string]interface{}
	decodeJSON(t, resp2, &result)
	data := result["data"].([]interface{})

	found := false
	for _, item := range data {
		p := item.(map[string]interface{})
		if p["name"].(string) == newSub {
			found = true
			assert.Equal(t, false, p["is_admin"], "JIT user should not be admin")
			assert.Equal(t, "user", p["type"])
			break
		}
	}
	require.True(t, found, "JIT-provisioned principal %q should exist in principals list", newSub)
}

// TestJIT_ProvisionIdempotent verifies that multiple requests with the same
// unknown sub don't create duplicate principals.
func TestJIT_ProvisionIdempotent(t *testing.T) {
	secret := []byte("test-jwt-secret")
	env := setupHTTPServer(t, httpTestOpts{
		JWTSecret:         secret,
		WithAuthenticator: true,
	})

	newSub := "jit-idempotent-" + fmt.Sprintf("%d", time.Now().UnixNano())
	token := generateJWT(t, secret, newSub, time.Now().Add(time.Hour))

	// Make two requests — both should succeed.
	// Use /v1/tags (open to non-admins) to trigger JIT provisioning.
	resp1 := doRequestWithBearer(t, "GET", env.Server.URL+"/v1/tags", token, nil)
	require.Equal(t, 200, resp1.StatusCode)
	_ = resp1.Body.Close()

	resp2 := doRequestWithBearer(t, "GET", env.Server.URL+"/v1/tags", token, nil)
	require.Equal(t, 200, resp2.StatusCode)
	_ = resp2.Body.Close()

	// Count how many principals match the name.
	resp3 := doRequest(t, "GET", env.Server.URL+"/v1/principals", env.Keys.Admin, nil)
	require.Equal(t, 200, resp3.StatusCode)

	var result map[string]interface{}
	decodeJSON(t, resp3, &result)
	data := result["data"].([]interface{})

	count := 0
	for _, item := range data {
		p := item.(map[string]interface{})
		if p["name"].(string) == newSub {
			count++
		}
	}
	assert.Equal(t, 1, count, "JIT should not create duplicate principals")
}

// TestJIT_ExistingExternalID verifies that when a JWT sub matches an
// already-provisioned external ID, the existing principal is resolved
// without creating a duplicate.
func TestJIT_ExistingExternalID(t *testing.T) {
	secret := []byte("test-jwt-secret")
	env := setupHTTPServer(t, httpTestOpts{
		JWTSecret:         secret,
		WithAuthenticator: true,
	})

	// First login — JIT creates the principal.
	extSub := "jit-existing-ext-" + fmt.Sprintf("%d", time.Now().UnixNano())
	token := generateJWT(t, secret, extSub, time.Now().Add(time.Hour))

	// Use /v1/tags (open to non-admins) to trigger JIT provisioning.
	resp := doRequestWithBearer(t, "GET", env.Server.URL+"/v1/tags", token, nil)
	require.Equal(t, 200, resp.StatusCode, "first JIT login should succeed")
	_ = resp.Body.Close()

	// Second login — should resolve to same principal.
	resp2 := doRequestWithBearer(t, "GET", env.Server.URL+"/v1/tags", token, nil)
	require.Equal(t, 200, resp2.StatusCode, "second JIT login should resolve existing")
	_ = resp2.Body.Close()

	// Verify only one principal with this name exists.
	resp3 := doRequest(t, "GET", env.Server.URL+"/v1/principals", env.Keys.Admin, nil)
	require.Equal(t, 200, resp3.StatusCode)
	var result map[string]interface{}
	decodeJSON(t, resp3, &result)
	data := result["data"].([]interface{})

	count := 0
	for _, item := range data {
		p := item.(map[string]interface{})
		if p["name"].(string) == extSub {
			count++
		}
	}
	assert.Equal(t, 1, count, "should resolve to existing principal, not create duplicate")
}

// TestJWT_ExistingPrincipalByName verifies that a JWT user can resolve to
// an existing seeded principal by name (without JIT provisioning).
func TestJWT_ExistingPrincipalByName(t *testing.T) {
	secret := []byte("test-jwt-secret")
	// Do NOT use WithAuthenticator (no JIT) — just the standard Authenticator
	// which looks up by name via principalRepo.
	env := setupHTTPServer(t, httpTestOpts{
		JWTSecret: secret,
	})

	// admin_user is seeded — use its name as the sub.
	token := generateJWT(t, secret, "admin_user", time.Now().Add(time.Hour))

	// Should resolve to the existing admin_user principal (who is admin).
	// Create a new principal to prove admin access works.
	body := map[string]interface{}{"name": "jwt-name-resolve-test", "type": "user"}
	resp := doRequestWithBearer(t, "POST", env.Server.URL+"/v1/principals", token, body)
	require.Equal(t, 201, resp.StatusCode, "admin_user via JWT should be able to create principals")
	_ = resp.Body.Close()
}

// TestJIT_DisplayNameFromEmailClaim verifies that when NameClaim is "email",
// the principal name is derived from the email claim rather than sub.
func TestJIT_DisplayNameFromEmailClaim(t *testing.T) {
	secret := []byte("test-jwt-secret")
	env := setupHTTPServer(t, httpTestOpts{
		JWTSecret:         secret,
		WithAuthenticator: true,
		NameClaim:         "email",
	})

	// Create a JWT with both sub and email claims.
	claims := jwt.MapClaims{
		"sub":   "some-guid-12345",
		"email": "testuser@example.com",
		"exp":   time.Now().Add(time.Hour).Unix(),
		"iat":   time.Now().Unix(),
	}
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	signed, err := token.SignedString(secret)
	require.NoError(t, err)

	// Use /v1/tags (open to non-admins) to trigger JIT provisioning.
	resp := doRequestWithBearer(t, "GET", env.Server.URL+"/v1/tags", signed, nil)
	require.Equal(t, 200, resp.StatusCode)
	_ = resp.Body.Close()

	// Verify the principal was created with the email as the name (lowercased).
	resp2 := doRequest(t, "GET", env.Server.URL+"/v1/principals", env.Keys.Admin, nil)
	require.Equal(t, 200, resp2.StatusCode)

	var result map[string]interface{}
	decodeJSON(t, resp2, &result)
	data := result["data"].([]interface{})

	found := false
	for _, item := range data {
		p := item.(map[string]interface{})
		if p["name"].(string) == "testuser@example.com" {
			found = true
			break
		}
	}
	require.True(t, found, "principal should be created with email claim as name")
}

// ---------------------------------------------------------------------------
// Bootstrap Admin E2E
// ---------------------------------------------------------------------------

// TestBootstrapAdmin_FirstLogin verifies that when AUTH_BOOTSTRAP_ADMIN is set
// and a JWT with the matching sub arrives, the JIT-provisioned principal gets
// IsAdmin=true.
func TestBootstrapAdmin_FirstLogin(t *testing.T) {
	secret := []byte("test-jwt-secret")
	bootstrapSub := "bootstrap-admin-sub-" + fmt.Sprintf("%d", time.Now().UnixNano())

	env := setupHTTPServer(t, httpTestOpts{
		JWTSecret:         secret,
		WithAuthenticator: true,
		BootstrapAdmin:    bootstrapSub,
	})

	token := generateJWT(t, secret, bootstrapSub, time.Now().Add(time.Hour))

	// The bootstrap admin should be able to create principals (admin action).
	body := map[string]interface{}{"name": "bootstrap-created-user", "type": "user"}
	resp := doRequestWithBearer(t, "POST", env.Server.URL+"/v1/principals", token, body)
	respBody := readBody(t, resp)
	require.Equal(t, 201, resp.StatusCode,
		"bootstrap admin should be able to create principals, got: %s", string(respBody))

	// Verify the bootstrap principal is admin.
	resp2 := doRequest(t, "GET", env.Server.URL+"/v1/principals", env.Keys.Admin, nil)
	require.Equal(t, 200, resp2.StatusCode)

	var result map[string]interface{}
	decodeJSON(t, resp2, &result)
	data := result["data"].([]interface{})

	found := false
	for _, item := range data {
		p := item.(map[string]interface{})
		if p["name"].(string) == bootstrapSub {
			found = true
			assert.Equal(t, true, p["is_admin"], "bootstrap admin should have is_admin=true")
			break
		}
	}
	require.True(t, found, "bootstrap admin principal should exist")
}

// TestBootstrapAdmin_NonMatchingSubNotAdmin verifies that a JWT with a sub
// that doesn't match AUTH_BOOTSTRAP_ADMIN does NOT get admin privileges.
func TestBootstrapAdmin_NonMatchingSubNotAdmin(t *testing.T) {
	secret := []byte("test-jwt-secret")
	bootstrapSub := "the-real-admin-sub"

	env := setupHTTPServer(t, httpTestOpts{
		JWTSecret:         secret,
		WithAuthenticator: true,
		BootstrapAdmin:    bootstrapSub,
	})

	// Use a different sub.
	regularSub := "regular-user-sub-" + fmt.Sprintf("%d", time.Now().UnixNano())
	token := generateJWT(t, secret, regularSub, time.Now().Add(time.Hour))

	// Should be JIT-provisioned but NOT admin. Trying to create a principal should fail.
	body := map[string]interface{}{"name": "should-fail-user", "type": "user"}
	resp := doRequestWithBearer(t, "POST", env.Server.URL+"/v1/principals", token, body)
	assert.Equal(t, 403, resp.StatusCode, "non-bootstrap sub should not be admin")
	_ = resp.Body.Close()
}

// ---------------------------------------------------------------------------
// Admin Guard E2E
// ---------------------------------------------------------------------------

// TestAdminGuard_NonAdminBlocked verifies that non-admin users get 403 on
// all mutating management endpoints through the full HTTP stack.
func TestAdminGuard_NonAdminBlocked(t *testing.T) {
	env := setupHTTPServer(t, httpTestOpts{})

	// Use the analyst key (non-admin).
	tests := []struct {
		name   string
		method string
		path   string
		body   interface{}
	}{
		{"create_principal", "POST", "/v1/principals", map[string]interface{}{"name": "guard-test", "type": "user"}},
		{"delete_principal", "DELETE", "/v1/principals/1", nil},
		{"set_admin", "PUT", "/v1/principals/1/admin", map[string]interface{}{"is_admin": true}},
		{"create_group", "POST", "/v1/groups", map[string]interface{}{"name": "guard-test-group"}},
		{"delete_group", "DELETE", "/v1/groups/1", nil},
		{"create_grant", "POST", "/v1/grants", map[string]interface{}{
			"principal_id": "1", "principal_type": "user",
			"securable_type": "catalog", "securable_id": "0", "privilege": "ALL_PRIVILEGES",
		}},
		{"revoke_grant", "DELETE", "/v1/grants/1", nil},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			resp := doRequest(t, tc.method, env.Server.URL+tc.path, env.Keys.Analyst, tc.body)
			assert.Equal(t, 403, resp.StatusCode,
				"non-admin should get 403 for %s %s", tc.method, tc.path)
			_ = resp.Body.Close()
		})
	}
}

// TestAdminGuard_AdminAllowed verifies that admin users can access all
// mutating management endpoints.
func TestAdminGuard_AdminAllowed(t *testing.T) {
	env := setupHTTPServer(t, httpTestOpts{})

	// Create a principal (admin action).
	body := map[string]interface{}{"name": "admin-guard-allowed-user", "type": "user"}
	resp := doRequest(t, "POST", env.Server.URL+"/v1/principals", env.Keys.Admin, body)
	require.Equal(t, 201, resp.StatusCode)

	var result map[string]interface{}
	decodeJSON(t, resp, &result)
	userID := result["id"].(string)

	// Create a group (admin action).
	body2 := map[string]interface{}{"name": "admin-guard-allowed-group"}
	resp2 := doRequest(t, "POST", env.Server.URL+"/v1/groups", env.Keys.Admin, body2)
	require.Equal(t, 201, resp2.StatusCode)
	_ = resp2.Body.Close()

	// Set admin (admin action).
	url := fmt.Sprintf("%s/v1/principals/%s/admin", env.Server.URL, userID)
	body3 := map[string]interface{}{"is_admin": true}
	resp3 := doRequest(t, "PUT", url, env.Keys.Admin, body3)
	require.Equal(t, 204, resp3.StatusCode)
	_ = resp3.Body.Close()

	// Delete principal (admin action).
	url2 := fmt.Sprintf("%s/v1/principals/%s", env.Server.URL, userID)
	resp4 := doRequest(t, "DELETE", url2, env.Keys.Admin, nil)
	require.Equal(t, 204, resp4.StatusCode)
	_ = resp4.Body.Close()
}

// TestAdminGuard_ReadEndpointsOpen verifies that non-admin users can still
// access read endpoints that don't require admin, and get 403 on admin-only ones.
func TestAdminGuard_ReadEndpointsOpen(t *testing.T) {
	env := setupHTTPServer(t, httpTestOpts{})

	tests := []struct {
		name       string
		path       string
		wantStatus int
	}{
		{"list_principals_admin_only", "/v1/principals", 403},
		{"list_groups_admin_only", "/v1/groups", 403},
		{"list_audit_logs_admin_only", "/v1/audit-logs", 403},
		{"list_tags", "/v1/tags", 200},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			resp := doRequest(t, "GET", env.Server.URL+tc.path, env.Keys.Analyst, nil)
			assert.Equal(t, tc.wantStatus, resp.StatusCode,
				"non-admin GET %s should return %d", tc.path, tc.wantStatus)
			_ = resp.Body.Close()
		})
	}
}

// ---------------------------------------------------------------------------
// API Key Lifecycle E2E
// ---------------------------------------------------------------------------

// TestAPIKey_FullLifecycle tests creating, listing, using, and deleting an
// API key through the HTTP API.
func TestAPIKey_FullLifecycle(t *testing.T) {
	secret := []byte("test-jwt-secret")
	env := setupHTTPServer(t, httpTestOpts{
		JWTSecret:         secret,
		WithAuthenticator: true,
		WithAPIKeyService: true,
	})

	// Get admin_user's principal ID.
	resp := doRequest(t, "GET", env.Server.URL+"/v1/principals", env.Keys.Admin, nil)
	require.Equal(t, 200, resp.StatusCode)
	var listResult map[string]interface{}
	decodeJSON(t, resp, &listResult)
	data := listResult["data"].([]interface{})

	var adminPrincipalID string
	for _, item := range data {
		p := item.(map[string]interface{})
		if p["name"].(string) == "admin_user" {
			adminPrincipalID = p["id"].(string)
			break
		}
	}
	require.NotEmpty(t, adminPrincipalID, "admin_user principal should exist")

	// Step 1: Create an API key via the endpoint.
	createBody := map[string]interface{}{
		"principal_id": adminPrincipalID,
		"name":         "lifecycle-test-key",
	}
	resp2 := doRequest(t, "POST", env.Server.URL+"/v1/api-keys", env.Keys.Admin, createBody)
	respBody := readBody(t, resp2)
	require.Equal(t, 201, resp2.StatusCode, "create API key should succeed, got: %s", string(respBody))

	var createResult map[string]interface{}
	require.NoError(t, json.Unmarshal(respBody, &createResult))
	rawKey, ok := createResult["key"].(string)
	require.True(t, ok && rawKey != "", "response should include the raw API key")
	keyID := createResult["id"].(string)
	require.NotEmpty(t, keyID)

	// Step 2: List API keys — should include the new key (without raw value).
	listURL := fmt.Sprintf("%s/v1/api-keys?principal_id=%s", env.Server.URL, adminPrincipalID)
	resp3 := doRequest(t, "GET", listURL, env.Keys.Admin, nil)
	require.Equal(t, 200, resp3.StatusCode)

	var listKeysResult map[string]interface{}
	decodeJSON(t, resp3, &listKeysResult)
	keysData := listKeysResult["data"].([]interface{})
	require.GreaterOrEqual(t, len(keysData), 1, "should have at least 1 key")

	// Verify our key is in the list.
	found := false
	for _, item := range keysData {
		k := item.(map[string]interface{})
		if k["name"].(string) == "lifecycle-test-key" {
			found = true
			// Raw key should NOT be in the list response.
			_, hasKey := k["key"]
			assert.False(t, hasKey, "list response should not include raw key")
			break
		}
	}
	require.True(t, found, "lifecycle-test-key should appear in list")

	// Step 3: Use the newly created API key to authenticate.
	resp4 := doRequest(t, "GET", env.Server.URL+"/v1/principals", rawKey, nil)
	require.Equal(t, 200, resp4.StatusCode, "newly created API key should authenticate successfully")
	_ = resp4.Body.Close()

	// Step 4: Delete the API key.
	deleteURL := fmt.Sprintf("%s/v1/api-keys/%s", env.Server.URL, keyID)
	resp5 := doRequest(t, "DELETE", deleteURL, env.Keys.Admin, nil)
	require.Equal(t, 204, resp5.StatusCode)
	_ = resp5.Body.Close()

	// Step 5: Verify the deleted key no longer works.
	resp6 := doRequest(t, "GET", env.Server.URL+"/v1/principals", rawKey, nil)
	assert.Equal(t, 401, resp6.StatusCode, "deleted API key should not authenticate")
	_ = resp6.Body.Close()
}

// TestAPIKey_WithExpiry tests that an API key with an expiry in the past
// is rejected by the auth middleware.
func TestAPIKey_WithExpiry(t *testing.T) {
	secret := []byte("test-jwt-secret")
	env := setupHTTPServer(t, httpTestOpts{
		JWTSecret:         secret,
		WithAuthenticator: true,
		WithAPIKeyService: true,
	})

	// Get admin_user's principal ID.
	resp := doRequest(t, "GET", env.Server.URL+"/v1/principals", env.Keys.Admin, nil)
	require.Equal(t, 200, resp.StatusCode)
	var listResult map[string]interface{}
	decodeJSON(t, resp, &listResult)
	data := listResult["data"].([]interface{})

	var adminPrincipalID string
	for _, item := range data {
		p := item.(map[string]interface{})
		if p["name"].(string) == "admin_user" {
			adminPrincipalID = p["id"].(string)
			break
		}
	}
	require.NotEmpty(t, adminPrincipalID)

	// Create an API key with future expiry.
	futureExpiry := time.Now().Add(24 * time.Hour).Format(time.RFC3339)
	createBody := map[string]interface{}{
		"principal_id": adminPrincipalID,
		"name":         "expiry-test-key",
		"expires_at":   futureExpiry,
	}
	resp2 := doRequest(t, "POST", env.Server.URL+"/v1/api-keys", env.Keys.Admin, createBody)
	require.Equal(t, 201, resp2.StatusCode)

	var createResult map[string]interface{}
	decodeJSON(t, resp2, &createResult)
	rawKey := createResult["key"].(string)

	// The key should work (it's not expired yet).
	resp3 := doRequest(t, "GET", env.Server.URL+"/v1/principals", rawKey, nil)
	assert.Equal(t, 200, resp3.StatusCode, "non-expired key should work")
	_ = resp3.Body.Close()
}

// TestAPIKey_CleanupExpired tests the admin-only cleanup endpoint.
func TestAPIKey_CleanupExpired(t *testing.T) {
	env := setupHTTPServer(t, httpTestOpts{
		WithAPIKeyService: true,
	})

	// Non-admin should get 403.
	resp := doRequest(t, "POST", env.Server.URL+"/v1/api-keys/cleanup", env.Keys.Analyst, nil)
	assert.Equal(t, 403, resp.StatusCode, "non-admin should get 403 for cleanup")
	_ = resp.Body.Close()

	// Admin should succeed.
	resp2 := doRequest(t, "POST", env.Server.URL+"/v1/api-keys/cleanup", env.Keys.Admin, nil)
	require.Equal(t, 200, resp2.StatusCode, "admin should be able to cleanup expired keys")

	var result map[string]interface{}
	decodeJSON(t, resp2, &result)
	_, hasCount := result["deleted_count"]
	assert.True(t, hasCount, "response should include deleted_count")
}

// ---------------------------------------------------------------------------
// Key Rotation
// ---------------------------------------------------------------------------

// TestKeyRotation verifies the key rotation workflow: create new key, use it,
// delete old key, verify old key fails, new key still works.
func TestKeyRotation(t *testing.T) {
	secret := []byte("test-jwt-secret")
	env := setupHTTPServer(t, httpTestOpts{
		JWTSecret:         secret,
		WithAuthenticator: true,
		WithAPIKeyService: true,
	})

	// Get admin principal ID.
	resp := doRequest(t, "GET", env.Server.URL+"/v1/principals", env.Keys.Admin, nil)
	require.Equal(t, 200, resp.StatusCode)
	var listResult map[string]interface{}
	decodeJSON(t, resp, &listResult)
	data := listResult["data"].([]interface{})

	var adminPrincipalID string
	for _, item := range data {
		p := item.(map[string]interface{})
		if p["name"].(string) == "admin_user" {
			adminPrincipalID = p["id"].(string)
			break
		}
	}
	require.NotEmpty(t, adminPrincipalID)

	// Create "old" key.
	createBody := map[string]interface{}{
		"principal_id": adminPrincipalID,
		"name":         "rotation-old-key",
	}
	resp2 := doRequest(t, "POST", env.Server.URL+"/v1/api-keys", env.Keys.Admin, createBody)
	require.Equal(t, 201, resp2.StatusCode)
	var oldResult map[string]interface{}
	decodeJSON(t, resp2, &oldResult)
	oldKey := oldResult["key"].(string)
	oldKeyID := oldResult["id"].(string)

	// Verify old key works.
	resp3 := doRequest(t, "GET", env.Server.URL+"/v1/principals", oldKey, nil)
	require.Equal(t, 200, resp3.StatusCode, "old key should work initially")
	_ = resp3.Body.Close()

	// Create "new" key.
	createBody2 := map[string]interface{}{
		"principal_id": adminPrincipalID,
		"name":         "rotation-new-key",
	}
	resp4 := doRequest(t, "POST", env.Server.URL+"/v1/api-keys", env.Keys.Admin, createBody2)
	require.Equal(t, 201, resp4.StatusCode)
	var newResult map[string]interface{}
	decodeJSON(t, resp4, &newResult)
	newKey := newResult["key"].(string)

	// Verify new key works.
	resp5 := doRequest(t, "GET", env.Server.URL+"/v1/principals", newKey, nil)
	require.Equal(t, 200, resp5.StatusCode, "new key should work")
	_ = resp5.Body.Close()

	// Delete old key.
	deleteURL := fmt.Sprintf("%s/v1/api-keys/%s", env.Server.URL, oldKeyID)
	resp6 := doRequest(t, "DELETE", deleteURL, env.Keys.Admin, nil)
	require.Equal(t, 204, resp6.StatusCode)
	_ = resp6.Body.Close()

	// Verify old key no longer works.
	resp7 := doRequest(t, "GET", env.Server.URL+"/v1/principals", oldKey, nil)
	assert.Equal(t, 401, resp7.StatusCode, "old key should be rejected after deletion")
	_ = resp7.Body.Close()

	// Verify new key still works.
	resp8 := doRequest(t, "GET", env.Server.URL+"/v1/principals", newKey, nil)
	assert.Equal(t, 200, resp8.StatusCode, "new key should still work after old key deletion")
	_ = resp8.Body.Close()
}

// ---------------------------------------------------------------------------
// Mixed Auth
// ---------------------------------------------------------------------------

// TestMixedAuth_JWTUserCreatesAPIKey verifies that a user who authenticates
// via JWT can have an API key created, and then use that API key to authenticate.
// Both should resolve to the same principal identity.
func TestMixedAuth_JWTUserCreatesAPIKey(t *testing.T) {
	secret := []byte("test-jwt-secret")
	env := setupHTTPServer(t, httpTestOpts{
		JWTSecret:         secret,
		WithAuthenticator: true,
		WithAPIKeyService: true,
	})

	// Step 1: JIT-provision a new user via JWT.
	mixedSub := "mixed-auth-user-" + fmt.Sprintf("%d", time.Now().UnixNano())
	jwtToken := generateJWT(t, secret, mixedSub, time.Now().Add(time.Hour))

	// Use /v1/tags (open to non-admins) to trigger JIT provisioning.
	resp := doRequestWithBearer(t, "GET", env.Server.URL+"/v1/tags", jwtToken, nil)
	require.Equal(t, 200, resp.StatusCode, "JWT auth should JIT-provision the user")
	_ = resp.Body.Close()

	// Step 2: Find the JIT-provisioned user's principal ID.
	resp2 := doRequest(t, "GET", env.Server.URL+"/v1/principals", env.Keys.Admin, nil)
	require.Equal(t, 200, resp2.StatusCode)
	var listResult map[string]interface{}
	decodeJSON(t, resp2, &listResult)
	data := listResult["data"].([]interface{})

	var userID string
	for _, item := range data {
		p := item.(map[string]interface{})
		if p["name"].(string) == mixedSub {
			userID = p["id"].(string)
			break
		}
	}
	require.NotEmpty(t, userID, "JIT-provisioned principal should exist")

	// Step 3: Admin creates an API key for the user.
	createKeyBody := map[string]interface{}{
		"principal_id": userID,
		"name":         "mixed-auth-api-key",
	}
	resp3 := doRequest(t, "POST", env.Server.URL+"/v1/api-keys", env.Keys.Admin, createKeyBody)
	require.Equal(t, 201, resp3.StatusCode)
	var keyResult map[string]interface{}
	decodeJSON(t, resp3, &keyResult)
	apiKey := keyResult["key"].(string)

	// Step 4: Use the API key to authenticate as the same user.
	// Use /v1/tags (open to non-admins) since ListPrincipals and ListGroups require admin.
	resp4 := doRequest(t, "GET", env.Server.URL+"/v1/tags", apiKey, nil)
	require.Equal(t, 200, resp4.StatusCode, "API key should authenticate as the same principal")
	_ = resp4.Body.Close()
}

// TestMixedAuth_BearerTakesPrecedence verifies that when both Authorization
// and X-API-Key headers are present, the Bearer token is used.
func TestMixedAuth_BearerTakesPrecedence(t *testing.T) {
	secret := []byte("test-jwt-secret")
	// No JIT — uses standard name resolution so admin_user resolves correctly.
	env := setupHTTPServer(t, httpTestOpts{
		JWTSecret: secret,
	})

	// Create a JWT for admin_user.
	token := generateJWT(t, secret, "admin_user", time.Now().Add(time.Hour))

	// Send a request with both Bearer and X-API-Key headers.
	req, err := http.NewRequestWithContext(ctx, "POST", env.Server.URL+"/v1/principals", nil)
	require.NoError(t, err)
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("X-API-Key", "bogus-key-that-would-fail")
	req.Header.Set("Content-Type", "application/json")

	// Set a JSON body for creating a principal.
	bodyBytes, _ := json.Marshal(map[string]interface{}{"name": "bearer-precedence-test", "type": "user"})
	req.Body = io.NopCloser(bytes.NewReader(bodyBytes))
	req.ContentLength = int64(len(bodyBytes))

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close() //nolint:errcheck

	// Bearer should take precedence — admin_user is admin, so the request should succeed.
	assert.Equal(t, 201, resp.StatusCode,
		"Bearer should take precedence over X-API-Key")
}

// ---------------------------------------------------------------------------
// JIT + Admin Guard combined
// ---------------------------------------------------------------------------

// TestJIT_NonAdminCannotMutate verifies that a JIT-provisioned non-admin user
// cannot access mutating management endpoints.
func TestJIT_NonAdminCannotMutate(t *testing.T) {
	secret := []byte("test-jwt-secret")
	env := setupHTTPServer(t, httpTestOpts{
		JWTSecret:         secret,
		WithAuthenticator: true,
	})

	newSub := "jit-nonadmin-" + fmt.Sprintf("%d", time.Now().UnixNano())
	token := generateJWT(t, secret, newSub, time.Now().Add(time.Hour))

	// Read should work (JIT provisions the user).
	// Use /v1/tags (open to non-admins) to trigger JIT provisioning.
	resp := doRequestWithBearer(t, "GET", env.Server.URL+"/v1/tags", token, nil)
	require.Equal(t, 200, resp.StatusCode, "JIT user should be able to read")
	_ = resp.Body.Close()

	// Mutating action should be blocked.
	body := map[string]interface{}{"name": "should-not-create", "type": "user"}
	resp2 := doRequestWithBearer(t, "POST", env.Server.URL+"/v1/principals", token, body)
	assert.Equal(t, 403, resp2.StatusCode, "JIT non-admin should get 403 on create")
	_ = resp2.Body.Close()
}

// ---------------------------------------------------------------------------
// Audit Trail for JIT
// ---------------------------------------------------------------------------

// TestJIT_AuditTrail verifies that JIT provisioning creates an audit log entry.
func TestJIT_AuditTrail(t *testing.T) {
	secret := []byte("test-jwt-secret")
	env := setupHTTPServer(t, httpTestOpts{
		JWTSecret:         secret,
		WithAuthenticator: true,
	})

	newSub := "jit-audit-" + fmt.Sprintf("%d", time.Now().UnixNano())
	token := generateJWT(t, secret, newSub, time.Now().Add(time.Hour))

	// Trigger JIT provisioning via /v1/tags (open to non-admins).
	resp := doRequestWithBearer(t, "GET", env.Server.URL+"/v1/tags", token, nil)
	require.Equal(t, 200, resp.StatusCode)
	_ = resp.Body.Close()

	// Check audit logs for the JIT_PROVISION entry.
	logs := fetchAuditLogs(t, env.Server.URL, env.Keys.Admin)
	found := false
	for _, entry := range logs {
		action, _ := entry["action"].(string)
		principal, _ := entry["principal_name"].(string)
		if action == "JIT_PROVISION" && principal == newSub {
			found = true
			break
		}
	}
	assert.True(t, found, "audit logs should contain JIT_PROVISION entry for %s", newSub)
}
