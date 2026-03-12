package cli

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/golang-jwt/jwt/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAuthTokenCmd(t *testing.T) {
	tests := []struct {
		name       string
		args       []string
		wantSub    string
		wantAdmin  bool
		wantErr    bool
		errContain string
	}{
		{
			name:    "basic token",
			args:    []string{"--principal", "alice", "--secret", "test-secret"},
			wantSub: "alice",
		},
		{
			name:      "admin token",
			args:      []string{"--principal", "bob", "--secret", "test-secret", "--admin"},
			wantSub:   "bob",
			wantAdmin: true,
		},
		{
			name:      "custom expiry",
			args:      []string{"--principal", "carol", "--secret", "test-secret", "--expires", "48h"},
			wantSub:   "carol",
			wantAdmin: false,
		},
		{
			name:       "missing principal",
			args:       []string{"--secret", "test-secret"},
			wantErr:    true,
			errContain: "required",
		},
		{
			name:       "missing secret",
			args:       []string{"--principal", "alice"},
			wantErr:    true,
			errContain: "required",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			t.Setenv("HOME", dir)

			cmd := newAuthTokenCmd()
			cmd.SetArgs(tt.args)

			err := cmd.Execute()
			if tt.wantErr {
				require.Error(t, err)
				if tt.errContain != "" {
					assert.Contains(t, err.Error(), tt.errContain)
				}
				return
			}
			require.NoError(t, err)

			// Load the saved config and verify the token was persisted
			cfg, err := LoadUserConfig()
			require.NoError(t, err)

			profileName := cfg.CurrentProfile
			if profileName == "" {
				profileName = "default"
			}
			p, ok := cfg.Profiles[profileName]
			require.True(t, ok, "profile %q should exist", profileName)
			require.NotEmpty(t, p.Token)

			// Parse and verify the saved token
			parsed, err := jwt.Parse(p.Token, func(token *jwt.Token) (interface{}, error) {
				return []byte("test-secret"), nil
			})
			require.NoError(t, err)
			require.True(t, parsed.Valid)

			claims, ok := parsed.Claims.(jwt.MapClaims)
			require.True(t, ok)
			assert.Equal(t, tt.wantSub, claims["sub"])

			if tt.wantAdmin {
				assert.Equal(t, true, claims["admin"])
			} else {
				assert.Nil(t, claims["admin"])
			}

			// Verify standard claims exist
			assert.NotNil(t, claims["iat"])
			assert.NotNil(t, claims["exp"])
		})
	}
}

func TestAuthTokenCmd_SaveToExistingProfile(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("HOME", dir)

	// Create an existing config with a profile
	cfg := &UserConfig{
		CurrentProfile: "dev",
		Profiles: map[string]Profile{
			"dev": {
				Host:   "http://localhost:8080",
				APIKey: "dak_test",
			},
		},
	}
	err := SaveUserConfig(cfg)
	require.NoError(t, err)

	// Generate a token — should save to the "dev" profile
	cmd := newAuthTokenCmd()
	cmd.SetArgs([]string{"--principal", "admin_user", "--secret", "my-secret"})
	err = cmd.Execute()
	require.NoError(t, err)

	// Reload and verify the token was saved without clobbering other fields
	loaded, err := LoadUserConfig()
	require.NoError(t, err)

	p := loaded.Profiles["dev"]
	assert.Equal(t, "http://localhost:8080", p.Host, "host should be preserved")
	assert.Equal(t, "dak_test", p.APIKey, "api-key should be preserved")
	assert.NotEmpty(t, p.Token, "token should be set")

	// Verify the token content
	parsed, err := jwt.Parse(p.Token, func(token *jwt.Token) (interface{}, error) {
		return []byte("my-secret"), nil
	})
	require.NoError(t, err)
	claims, ok := parsed.Claims.(jwt.MapClaims)
	require.True(t, ok)
	assert.Equal(t, "admin_user", claims["sub"])
}

func TestCLI_AuthLocalLoginCommand(t *testing.T) {
	rec := &requestRecorder{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rec.record(r)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"token":"local-token","principal":{"name":"admin"}}`))
	}))
	defer srv.Close()

	rootCmd := newTestRootCmd(t, srv)
	rootCmd.SetArgs([]string{"--host", srv.URL, "auth", "local-login", "--username", "admin", "--password", "super-secure-password"})
	require.NoError(t, rootCmd.Execute())

	captured := rec.last()
	assert.Equal(t, "POST", captured.Method)
	assert.Equal(t, "/v1/auth/local/login", captured.Path)

	var body map[string]interface{}
	require.NoError(t, json.Unmarshal([]byte(captured.Body), &body))
	assert.Equal(t, "admin", body["username"])

	cfg, err := LoadUserConfig()
	require.NoError(t, err)
	assert.Equal(t, "local-token", cfg.Profiles[cfg.CurrentProfile].Token)
}

func TestCLI_AuthLocalLogin_PersistsSelectedProfileAndHost(t *testing.T) {
	rec := &requestRecorder{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rec.record(r)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"token":"local-token","principal":{"name":"admin"}}`))
	}))
	defer srv.Close()

	dir := t.TempDir()
	t.Setenv("HOME", dir)
	require.NoError(t, SaveUserConfig(&UserConfig{
		CurrentProfile: "default",
		Profiles: map[string]Profile{
			"default": {},
			"staging": {},
		},
	}))

	rootCmd := newRootCmd()
	rootCmd.SetArgs([]string{"--host", srv.URL, "--profile", "staging", "auth", "local-login", "--username", "admin", "--password", "super-secure-password"})
	require.NoError(t, rootCmd.Execute())

	cfg, err := LoadUserConfig()
	require.NoError(t, err)
	require.Equal(t, "staging", cfg.CurrentProfile)
	assert.Equal(t, "local-token", cfg.Profiles["staging"].Token)
	assert.Equal(t, srv.URL, cfg.Profiles["staging"].Host)
}

func TestCLI_AuthBootstrapCommands(t *testing.T) {
	t.Run("complete", func(t *testing.T) {
		rec := &requestRecorder{}
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			rec.record(r)
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"token":"bootstrap-token","principal":{"name":"admin_user"}}`))
		}))
		defer srv.Close()

		rootCmd := newTestRootCmd(t, srv)
		rootCmd.SetArgs([]string{"--host", srv.URL, "auth", "bootstrap", "complete", "--username", "admin", "--password", "super-secure-password", "--principal", "admin_user", "--token", "recovery-token"})
		require.NoError(t, rootCmd.Execute())

		captured := rec.last()
		assert.Equal(t, "POST", captured.Method)
		assert.Equal(t, "/v1/auth/bootstrap/complete", captured.Path)

		var body map[string]interface{}
		require.NoError(t, json.Unmarshal([]byte(captured.Body), &body))
		assert.Equal(t, "admin_user", body["principal_name"])
		assert.Equal(t, "recovery-token", body["bootstrap_token"])

		cfg, err := LoadUserConfig()
		require.NoError(t, err)
		assert.Equal(t, "bootstrap-token", cfg.Profiles[cfg.CurrentProfile].Token)
	})

	t.Run("token_create", func(t *testing.T) {
		rec := &requestRecorder{}
		srv := httptest.NewServer(jsonHandler(rec, 201, `{"bootstrap_token":"btok","ttl_seconds":60}`))
		defer srv.Close()

		rootCmd := newTestRootCmd(t, srv)
		rootCmd.SetArgs([]string{"--host", srv.URL, "auth", "bootstrap", "token-create", "--ttl", "1m"})
		require.NoError(t, rootCmd.Execute())

		captured := rec.last()
		assert.Equal(t, "POST", captured.Method)
		assert.Equal(t, "/v1/auth/bootstrap/tokens", captured.Path)

		var body map[string]interface{}
		require.NoError(t, json.Unmarshal([]byte(captured.Body), &body))
		ttlSeconds, ok := body["ttl_seconds"].(float64)
		require.True(t, ok)
		assert.InDelta(t, 60, ttlSeconds, 0.00001)
	})

	t.Run("token_create rejects sub-second ttl", func(t *testing.T) {
		srv := httptest.NewServer(http.NotFoundHandler())
		defer srv.Close()

		rootCmd := newTestRootCmd(t, srv)
		rootCmd.SetArgs([]string{"auth", "bootstrap", "token-create", "--ttl", "500ms"})
		err := rootCmd.Execute()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "at least 1s")
	})
}

func TestCLI_AuthProviderOIDCCommands(t *testing.T) {
	t.Run("get", func(t *testing.T) {
		rec := &requestRecorder{}
		srv := httptest.NewServer(jsonHandler(rec, 200, `{"enabled":false}`))
		defer srv.Close()

		rootCmd := newTestRootCmd(t, srv)
		rootCmd.SetArgs([]string{"--host", srv.URL, "auth", "provider", "oidc", "get"})
		require.NoError(t, rootCmd.Execute())

		captured := rec.last()
		assert.Equal(t, "GET", captured.Method)
		assert.Equal(t, "/v1/auth/provider/oidc", captured.Path)
	})

	t.Run("set", func(t *testing.T) {
		rec := &requestRecorder{}
		srv := httptest.NewServer(jsonHandler(rec, 204, ``))
		defer srv.Close()

		rootCmd := newTestRootCmd(t, srv)
		rootCmd.SetArgs([]string{"--host", srv.URL, "auth", "provider", "oidc", "set", "--issuer", "https://issuer.example.com", "--client-id", "duck-client", "--client-secret", "secret"})
		require.NoError(t, rootCmd.Execute())

		captured := rec.last()
		assert.Equal(t, "PUT", captured.Method)
		assert.Equal(t, "/v1/auth/provider/oidc", captured.Path)

		var body map[string]interface{}
		require.NoError(t, json.Unmarshal([]byte(captured.Body), &body))
		assert.Equal(t, true, body["enabled"])
		assert.Equal(t, "https://issuer.example.com", body["issuer_url"])
	})

	t.Run("disable", func(t *testing.T) {
		rec := &requestRecorder{}
		srv := httptest.NewServer(jsonHandler(rec, 204, ``))
		defer srv.Close()

		rootCmd := newTestRootCmd(t, srv)
		rootCmd.SetArgs([]string{"--host", srv.URL, "auth", "provider", "oidc", "disable"})
		require.NoError(t, rootCmd.Execute())

		captured := rec.last()
		assert.Equal(t, "PUT", captured.Method)
		assert.Equal(t, "/v1/auth/provider/oidc", captured.Path)

		var body map[string]interface{}
		require.NoError(t, json.Unmarshal([]byte(captured.Body), &body))
		assert.Equal(t, false, body["enabled"])
	})
}
