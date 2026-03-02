//go:build integration

package integration

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func boolPtr(v bool) *bool {
	return &v
}

func TestAuth_BootstrapAndLocalLogin_E2E(t *testing.T) {
	env := setupHTTPServer(t, httpTestOpts{AuthMode: "local_only"})

	bootstrapTokenResp := doRequest(t, "POST", env.Server.URL+"/v1/auth/bootstrap/tokens", env.Keys.Admin, map[string]interface{}{"ttl_seconds": 300})
	require.Equal(t, 201, bootstrapTokenResp.StatusCode)
	var bootstrapTokenPayload map[string]interface{}
	decodeJSON(t, bootstrapTokenResp, &bootstrapTokenPayload)
	bootstrapToken, _ := bootstrapTokenPayload["bootstrap_token"].(string)
	require.NotEmpty(t, bootstrapToken)

	bootstrapResp := doRequest(t, "POST", env.Server.URL+"/v1/auth/bootstrap/complete", "", map[string]interface{}{
		"username":        "localbootstrap",
		"password":        "super-secure-password",
		"principal_name":  "local_bootstrap_admin",
		"bootstrap_token": bootstrapToken,
	})
	require.Equal(t, 201, bootstrapResp.StatusCode)

	var bootstrapPayload map[string]interface{}
	decodeJSON(t, bootstrapResp, &bootstrapPayload)
	bootstrapJWT, _ := bootstrapPayload["token"].(string)
	require.NotEmpty(t, bootstrapJWT)

	loginResp := doRequest(t, "POST", env.Server.URL+"/v1/auth/local/login", "", map[string]interface{}{
		"username": "localbootstrap",
		"password": "super-secure-password",
	})
	require.Equal(t, 200, loginResp.StatusCode)

	var loginPayload map[string]interface{}
	decodeJSON(t, loginResp, &loginPayload)
	loginToken, _ := loginPayload["token"].(string)
	require.NotEmpty(t, loginToken)

	authedResp := doRequestWithBearer(t, "GET", env.Server.URL+"/v1/principals", loginToken, nil)
	defer authedResp.Body.Close() //nolint:errcheck
	assert.Equal(t, 200, authedResp.StatusCode)

	secondBootstrap := doRequest(t, "POST", env.Server.URL+"/v1/auth/bootstrap/complete", "", map[string]interface{}{
		"username":       "another-admin",
		"password":       "super-secure-password",
		"principal_name": "another-admin",
	})
	defer secondBootstrap.Body.Close() //nolint:errcheck
	assert.Equal(t, 403, secondBootstrap.StatusCode)
}

func TestAuth_ModeBehavior_E2E(t *testing.T) {
	t.Run("api_key_only_rejects_bearer_accepts_api_key", func(t *testing.T) {
		secret := []byte("mode-jwt-secret")
		env := setupHTTPServer(t, httpTestOpts{AuthMode: "api_key_only", JWTSecret: secret})

		token := generateJWT(t, secret, "admin_user", time.Now().Add(time.Hour))
		bearerResp := doRequestWithBearer(t, "GET", env.Server.URL+"/v1/principals", token, nil)
		defer bearerResp.Body.Close() //nolint:errcheck
		assert.Equal(t, 401, bearerResp.StatusCode)

		apiKeyResp := doRequest(t, "GET", env.Server.URL+"/v1/principals", env.Keys.Admin, nil)
		defer apiKeyResp.Body.Close() //nolint:errcheck
		assert.Equal(t, 200, apiKeyResp.StatusCode)
	})

	t.Run("oidc_only_accepts_bearer_when_api_keys_disabled", func(t *testing.T) {
		secret := []byte("mode-jwt-secret")
		env := setupHTTPServer(t, httpTestOpts{AuthMode: "oidc_only", JWTSecret: secret, APIKeyEnabled: boolPtr(false)})

		token := generateJWT(t, secret, "admin_user", time.Now().Add(time.Hour))
		bearerResp := doRequestWithBearer(t, "GET", env.Server.URL+"/v1/principals", token, nil)
		defer bearerResp.Body.Close() //nolint:errcheck
		assert.Equal(t, 200, bearerResp.StatusCode)

		apiKeyResp := doRequest(t, "GET", env.Server.URL+"/v1/principals", env.Keys.Admin, nil)
		defer apiKeyResp.Body.Close() //nolint:errcheck
		assert.Equal(t, 401, apiKeyResp.StatusCode)
	})
}
