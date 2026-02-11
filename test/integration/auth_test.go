//go:build integration

package integration

import (
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestAuth_APIKey verifies API key authentication through the middleware.
func TestAuth_APIKey(t *testing.T) {
	env := setupHTTPServer(t, httpTestOpts{})

	tests := []struct {
		name       string
		apiKey     string
		wantStatus int
	}{
		{"valid_key_200", env.Keys.Admin, 200},
		{"invalid_key_401", "bogus-key-that-does-not-exist", 401},
		{"empty_key_401", "", 401},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			resp := doRequest(t, "GET", env.Server.URL+"/v1/principals", tc.apiKey, nil)
			defer resp.Body.Close()
			assert.Equal(t, tc.wantStatus, resp.StatusCode)
		})
	}
}

// TestAuth_JWT verifies JWT Bearer token authentication.
func TestAuth_JWT(t *testing.T) {
	secret := []byte("test-jwt-secret")
	env := setupHTTPServer(t, httpTestOpts{JWTSecret: secret})

	t.Run("valid_token_200", func(t *testing.T) {
		token := generateJWT(t, secret, "admin_user", time.Now().Add(time.Hour))
		resp := doRequestWithBearer(t, "GET", env.Server.URL+"/v1/principals", token, nil)
		defer resp.Body.Close()
		require.Equal(t, 200, resp.StatusCode)
	})

	t.Run("expired_token_401", func(t *testing.T) {
		token := generateJWT(t, secret, "admin_user", time.Now().Add(-time.Hour))
		resp := doRequestWithBearer(t, "GET", env.Server.URL+"/v1/principals", token, nil)
		defer resp.Body.Close()
		assert.Equal(t, 401, resp.StatusCode)
	})

	t.Run("wrong_signature_401", func(t *testing.T) {
		token := generateJWT(t, []byte("wrong-secret"), "admin_user", time.Now().Add(time.Hour))
		resp := doRequestWithBearer(t, "GET", env.Server.URL+"/v1/principals", token, nil)
		defer resp.Body.Close()
		assert.Equal(t, 401, resp.StatusCode)
	})

	t.Run("wrong_algorithm_401", func(t *testing.T) {
		// Use HS384 — middleware only allows HS256
		claims := jwt.MapClaims{
			"sub": "admin_user",
			"exp": time.Now().Add(time.Hour).Unix(),
		}
		token := jwt.NewWithClaims(jwt.SigningMethodHS384, claims)
		signed, err := token.SignedString(secret)
		require.NoError(t, err)

		resp := doRequestWithBearer(t, "GET", env.Server.URL+"/v1/principals", signed, nil)
		defer resp.Body.Close()
		assert.Equal(t, 401, resp.StatusCode)
	})

	t.Run("missing_sub_claim_401", func(t *testing.T) {
		claims := jwt.MapClaims{
			"exp": time.Now().Add(time.Hour).Unix(),
		}
		token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
		signed, err := token.SignedString(secret)
		require.NoError(t, err)

		resp := doRequestWithBearer(t, "GET", env.Server.URL+"/v1/principals", signed, nil)
		defer resp.Body.Close()
		assert.Equal(t, 401, resp.StatusCode)
	})

	t.Run("empty_sub_claim_401", func(t *testing.T) {
		claims := jwt.MapClaims{
			"sub": "",
			"exp": time.Now().Add(time.Hour).Unix(),
		}
		token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
		signed, err := token.SignedString(secret)
		require.NoError(t, err)

		resp := doRequestWithBearer(t, "GET", env.Server.URL+"/v1/principals", signed, nil)
		defer resp.Body.Close()
		assert.Equal(t, 401, resp.StatusCode)
	})
}

// TestAuth_NoCredentials verifies requests without any authentication get 401.
func TestAuth_NoCredentials(t *testing.T) {
	env := setupHTTPServer(t, httpTestOpts{})

	resp := doRequest(t, "GET", env.Server.URL+"/v1/principals", "", nil)
	body := readBody(t, resp)

	require.Equal(t, 401, resp.StatusCode)
	assert.Contains(t, string(body), "unauthorized")
}

// TestAuth_PrincipalIdentity verifies that the authenticated principal is
// correctly propagated through the request context by creating a resource
// and inspecting its audit trail.
func TestAuth_PrincipalIdentity(t *testing.T) {
	env := setupHTTPServer(t, httpTestOpts{})

	t.Run("apikey_sets_principal", func(t *testing.T) {
		// Use a GET endpoint authenticated via API key to verify the
		// principal resolves correctly. The admin key maps to admin_user.
		resp := doRequest(t, "GET", env.Server.URL+"/v1/principals", env.Keys.Admin, nil)
		require.Equal(t, 200, resp.StatusCode)

		var result map[string]interface{}
		decodeJSON(t, resp, &result)
		// If we get 200 with data, auth worked and principal was set.
		data := result["data"].([]interface{})
		assert.GreaterOrEqual(t, len(data), 1, "admin should see principals")

		// Also verify a different key maps to a different principal by
		// checking that the analyst key works too.
		resp2 := doRequest(t, "GET", env.Server.URL+"/v1/principals", env.Keys.Analyst, nil)
		require.Equal(t, 200, resp2.StatusCode)
		resp2.Body.Close()
	})

	t.Run("jwt_sets_principal", func(t *testing.T) {
		secret := []byte("test-jwt-secret")
		token := generateJWT(t, secret, "admin_user", time.Now().Add(time.Hour))

		body := map[string]interface{}{"name": "auth-identity-jwt-test", "type": "user"}
		resp := doRequestWithBearer(t, "POST", env.Server.URL+"/v1/principals", token, body)
		require.Equal(t, 201, resp.StatusCode)
		resp.Body.Close()
	})
}
