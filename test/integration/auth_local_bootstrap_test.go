//go:build integration

package integration

import (
	"net/http"
	"net/url"
	"strings"
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

func TestAuth_UIWebSession_E2E(t *testing.T) {
	env := setupHTTPServer(t, httpTestOpts{AuthMode: "local_only"})

	bootstrapTokenResp := doRequest(t, "POST", env.Server.URL+"/v1/auth/bootstrap/tokens", env.Keys.Admin, map[string]interface{}{"ttl_seconds": 300})
	require.Equal(t, 201, bootstrapTokenResp.StatusCode)
	var bootstrapTokenPayload map[string]interface{}
	decodeJSON(t, bootstrapTokenResp, &bootstrapTokenPayload)
	bootstrapToken, _ := bootstrapTokenPayload["bootstrap_token"].(string)
	require.NotEmpty(t, bootstrapToken)

	bootstrapResp := doRequest(t, "POST", env.Server.URL+"/v1/auth/bootstrap/complete", "", map[string]interface{}{
		"username":        "uilocal",
		"password":        "super-secure-password",
		"principal_name":  "ui_local_admin",
		"bootstrap_token": bootstrapToken,
	})
	require.Equal(t, 201, bootstrapResp.StatusCode)
	_ = bootstrapResp.Body.Close()

	loginResp := doFormRequest(t, http.MethodPost, env.Server.URL+"/ui/login", url.Values{
		"username": []string{"uilocal"},
		"password": []string{"super-secure-password"},
	}, nil)
	require.Equal(t, http.StatusSeeOther, loginResp.StatusCode)

	var sessionCookie *http.Cookie
	for _, c := range loginResp.Cookies() {
		if c.Name == "ui_session" {
			sessionCookie = c
			break
		}
	}
	require.NotNil(t, sessionCookie)
	require.NotEmpty(t, sessionCookie.Value)
	_ = loginResp.Body.Close()

	homeResp := doRequestWithCookies(t, http.MethodGet, env.Server.URL+"/ui/", nil, []*http.Cookie{sessionCookie})
	defer homeResp.Body.Close() //nolint:errcheck
	assert.Equal(t, http.StatusOK, homeResp.StatusCode)

	logoutResp := doRequestWithCookies(t, http.MethodPost, env.Server.URL+"/ui/logout", nil, []*http.Cookie{sessionCookie})
	require.Equal(t, http.StatusSeeOther, logoutResp.StatusCode)
	_ = logoutResp.Body.Close()

	afterLogoutResp := doRequestWithCookies(t, http.MethodGet, env.Server.URL+"/ui/", nil, []*http.Cookie{sessionCookie})
	defer afterLogoutResp.Body.Close() //nolint:errcheck
	assert.Equal(t, http.StatusSeeOther, afterLogoutResp.StatusCode)
	assert.Contains(t, afterLogoutResp.Header.Get("Location"), "/ui/login")
}

func TestAuth_UIIgnoresTokenCookie_E2E(t *testing.T) {
	env := setupHTTPServer(t, httpTestOpts{AuthMode: "local_only"})

	resp := doRequestWithCookies(t, http.MethodGet, env.Server.URL+"/ui/", nil, []*http.Cookie{{Name: "ui_bearer", Value: "token"}})
	defer resp.Body.Close() //nolint:errcheck
	require.Equal(t, http.StatusSeeOther, resp.StatusCode)
	assert.Contains(t, resp.Header.Get("Location"), "/ui/login")
}

func TestAuth_AdminRevokeAllSessionsAndStats_E2E(t *testing.T) {
	env := setupHTTPServer(t, httpTestOpts{AuthMode: "local_only"})

	bootstrapTokenResp := doRequest(t, "POST", env.Server.URL+"/v1/auth/bootstrap/tokens", env.Keys.Admin, map[string]interface{}{"ttl_seconds": 300})
	require.Equal(t, 201, bootstrapTokenResp.StatusCode)
	var bootstrapTokenPayload map[string]interface{}
	decodeJSON(t, bootstrapTokenResp, &bootstrapTokenPayload)
	bootstrapToken, _ := bootstrapTokenPayload["bootstrap_token"].(string)
	require.NotEmpty(t, bootstrapToken)

	bootstrapResp := doRequest(t, "POST", env.Server.URL+"/v1/auth/bootstrap/complete", "", map[string]interface{}{
		"username":        "opsadmin",
		"password":        "super-secure-password",
		"principal_name":  "opsadmin",
		"bootstrap_token": bootstrapToken,
	})
	require.Equal(t, 201, bootstrapResp.StatusCode)
	var bootstrapPayload map[string]interface{}
	decodeJSON(t, bootstrapResp, &bootstrapPayload)
	principal, ok := bootstrapPayload["principal"].(map[string]interface{})
	require.True(t, ok)
	principalID, _ := principal["id"].(string)
	require.NotEmpty(t, principalID)

	loginResp := doFormRequest(t, http.MethodPost, env.Server.URL+"/ui/login", url.Values{
		"username": []string{"opsadmin"},
		"password": []string{"super-secure-password"},
	}, nil)
	require.Equal(t, http.StatusSeeOther, loginResp.StatusCode)
	var sessionCookie *http.Cookie
	for _, c := range loginResp.Cookies() {
		if c.Name == "ui_session" {
			sessionCookie = c
			break
		}
	}
	require.NotNil(t, sessionCookie)
	_ = loginResp.Body.Close()

	statsBefore := doRequest(t, http.MethodGet, env.Server.URL+"/v1/auth/sessions/stats", env.Keys.Admin, nil)
	require.Equal(t, 200, statsBefore.StatusCode)
	var beforePayload map[string]interface{}
	decodeJSON(t, statsBefore, &beforePayload)
	activeBefore, _ := beforePayload["active_sessions"].(float64)
	assert.GreaterOrEqual(t, int(activeBefore), 1)

	revokeResp := doRequest(t, http.MethodPost, env.Server.URL+"/v1/auth/sessions/revoke-all", env.Keys.Admin, map[string]interface{}{"principal_id": principalID})
	require.Equal(t, http.StatusNoContent, revokeResp.StatusCode)
	_ = revokeResp.Body.Close()

	uiResp := doRequestWithCookies(t, http.MethodGet, env.Server.URL+"/ui/", nil, []*http.Cookie{sessionCookie})
	defer uiResp.Body.Close() //nolint:errcheck
	assert.Equal(t, http.StatusSeeOther, uiResp.StatusCode)
	assert.Contains(t, uiResp.Header.Get("Location"), "/ui/login")

	statsAfter := doRequest(t, http.MethodGet, env.Server.URL+"/v1/auth/sessions/stats", env.Keys.Admin, nil)
	require.Equal(t, 200, statsAfter.StatusCode)
	var afterPayload map[string]interface{}
	decodeJSON(t, statsAfter, &afterPayload)
	revokedAll, _ := afterPayload["revoked_all_total"].(float64)
	assert.GreaterOrEqual(t, int(revokedAll), 1)
}

func doFormRequest(t *testing.T, method, requestURL string, form url.Values, cookies []*http.Cookie) *http.Response {
	t.Helper()
	req, err := http.NewRequestWithContext(ctx, method, requestURL, strings.NewReader(form.Encode()))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	for _, c := range cookies {
		req.AddCookie(c)
	}
	client := &http.Client{CheckRedirect: func(_ *http.Request, _ []*http.Request) error { return http.ErrUseLastResponse }}
	resp, err := client.Do(req)
	require.NoError(t, err)
	return resp
}

func doRequestWithCookies(t *testing.T, method, requestURL string, body interface{}, cookies []*http.Cookie) *http.Response {
	t.Helper()

	var req *http.Request
	var err error
	if body == nil {
		req, err = http.NewRequestWithContext(ctx, method, requestURL, nil)
		require.NoError(t, err)
	} else {
		resp := doRequest(t, method, requestURL, "", body)
		return resp
	}

	for _, c := range cookies {
		req.AddCookie(c)
	}

	client := &http.Client{CheckRedirect: func(_ *http.Request, _ []*http.Request) error { return http.ErrUseLastResponse }}
	resp, err := client.Do(req)
	require.NoError(t, err)
	return resp
}
