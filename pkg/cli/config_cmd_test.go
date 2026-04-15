package cli

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMaskSecret(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{"empty", "", ""},
		{"short", "abc", "****"},
		{"exactly_10", "1234567890", "****"},
		{"long_token", "eyJhbGciOiJIUzI1NiJ9.payload.sig", "eyJh****.sig"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, maskSecret(tt.input))
		})
	}
}

func TestMaskConfig(t *testing.T) {
	cfg := &UserConfig{
		CurrentProfile: "default",
		Profiles: map[string]Profile{
			"default": {
				Host:   "http://localhost:8080",
				APIKey: "sk-1234567890abcdef",
				Token:  "eyJhbGciOiJIUzI1NiJ9.payload.signature",
			},
		},
	}

	masked := maskConfig(cfg)

	// Non-sensitive fields preserved.
	assert.Equal(t, "http://localhost:8080", masked.Profiles["default"].Host)
	assert.Equal(t, "default", masked.CurrentProfile)

	// Sensitive fields masked.
	assert.NotEqual(t, cfg.Profiles["default"].APIKey, masked.Profiles["default"].APIKey)
	assert.NotEqual(t, cfg.Profiles["default"].Token, masked.Profiles["default"].Token)
	assert.Contains(t, masked.Profiles["default"].APIKey, "****")
	assert.Contains(t, masked.Profiles["default"].Token, "****")

	// Original config not mutated.
	assert.Equal(t, "sk-1234567890abcdef", cfg.Profiles["default"].APIKey)
	assert.Equal(t, "eyJhbGciOiJIUzI1NiJ9.payload.signature", cfg.Profiles["default"].Token)
}

func TestMaskConfig_EmptyProfiles(t *testing.T) {
	cfg := &UserConfig{
		CurrentProfile: "default",
		Profiles:       map[string]Profile{},
	}

	masked := maskConfig(cfg)
	assert.Empty(t, masked.Profiles)
}

func TestAuthProfilesList_TextOutput(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("HOME", dir)

	cfg := &UserConfig{
		CurrentProfile: "default",
		Profiles: map[string]Profile{
			"default": {
				Host:   "http://localhost:8080",
				APIKey: "secret-api-key",
				Output: "json",
			},
		},
	}
	require.NoError(t, SaveUserConfig(cfg))

	rootCmd := newRootCmd()
	rootCmd.SetArgs([]string{"--output", "text", "auth", "profiles", "list"})

	restore := captureStdout(t)
	err := rootCmd.Execute()
	output := restore()

	require.NoError(t, err)
	assert.Contains(t, output, "PROFILE")
	assert.Contains(t, output, "ACTIVE")
	assert.Contains(t, output, "default")
	assert.Contains(t, output, "api-key")
}

func TestAuthProfilesList_TextPrefersTokenOverAPIKey(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("HOME", dir)

	cfg := &UserConfig{
		CurrentProfile: "default",
		Profiles: map[string]Profile{
			"default": {
				Host:   "http://localhost:8080",
				APIKey: "secret-api-key",
				Token:  "secret-token",
			},
		},
	}
	require.NoError(t, SaveUserConfig(cfg))

	rootCmd := newRootCmd()
	rootCmd.SetArgs([]string{"--output", "text", "auth", "profiles", "list"})

	restore := captureStdout(t)
	err := rootCmd.Execute()
	output := restore()

	require.NoError(t, err)
	assert.Contains(t, output, "token")
	assert.NotContains(t, output, "api-key")
}

func TestAuthProfilesSave_GlobalJSONOutputNotShadowed(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("HOME", dir)

	rootCmd := newRootCmd()
	rootCmd.SetArgs([]string{
		"--output", "json",
		"auth", "profiles", "save",
		"--name", "default",
		"--host", "http://localhost:8080",
	})

	restore := captureStdout(t)
	err := rootCmd.Execute()
	output := restore()

	require.NoError(t, err)

	var result map[string]string
	require.NoError(t, json.Unmarshal([]byte(output), &result))
	assert.Equal(t, "ok", result["status"])

	cfg, err := LoadUserConfig()
	require.NoError(t, err)
	assert.Empty(t, cfg.Profiles["default"].Output)
}

func TestAuthProfilesSave_ValidatesDefaultOutputAndHost(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("HOME", dir)

	t.Run("invalid default output", func(t *testing.T) {
		rootCmd := newRootCmd()
		rootCmd.SetArgs([]string{
			"auth", "profiles", "save",
			"--name", "default",
			"--default-output", "yaml",
		})

		err := rootCmd.Execute()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unsupported output format")
	})

	t.Run("invalid host", func(t *testing.T) {
		rootCmd := newRootCmd()
		rootCmd.SetArgs([]string{
			"auth", "profiles", "save",
			"--name", "default",
			"--host", "localhost:8080",
		})

		err := rootCmd.Execute()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "invalid host")
	})
}

func TestAuthProfilesUseAndShow(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("HOME", dir)

	cfg := &UserConfig{
		CurrentProfile: "default",
		Profiles: map[string]Profile{
			"default": {Host: "http://localhost:8080"},
			"staging": {Host: "http://staging.local", Token: "staging-token"},
		},
	}
	require.NoError(t, SaveUserConfig(cfg))

	rootCmd := newRootCmd()
	rootCmd.SetArgs([]string{"--output", "json", "auth", "profiles", "use", "staging"})
	restore := captureStdout(t)
	err := rootCmd.Execute()
	output := restore()
	require.NoError(t, err)

	var result map[string]string
	require.NoError(t, json.Unmarshal([]byte(output), &result))
	assert.Equal(t, "staging", result["active_profile"])

	updatedCfg, err := LoadUserConfig()
	require.NoError(t, err)
	assert.Equal(t, "staging", updatedCfg.CurrentProfile)

	rootCmd = newRootCmd()
	rootCmd.SetArgs([]string{"--output", "json", "auth", "profiles", "show", "staging"})
	restore = captureStdout(t)
	err = rootCmd.Execute()
	output = restore()
	require.NoError(t, err)

	var showPayload map[string]any
	require.NoError(t, json.Unmarshal([]byte(output), &showPayload))
	assert.Equal(t, "staging", showPayload["name"])
	assert.Equal(t, true, showPayload["active"])
}

func TestAuthEnv_UsesResolvedProfile(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("HOME", dir)
	require.NoError(t, SaveUserConfig(&UserConfig{
		CurrentProfile: "default",
		Profiles: map[string]Profile{
			"default": {
				Host:   "http://localhost:8080",
				Token:  "profile-token",
				Output: "json",
			},
		},
	}))

	rootCmd := newRootCmd()
	rootCmd.SetArgs([]string{"--output", "text", "auth", "env"})

	restore := captureStdout(t)
	err := rootCmd.Execute()
	output := restore()
	require.NoError(t, err)

	assert.Contains(t, output, `export QUACK_HOST="http://localhost:8080"`)
	assert.Contains(t, output, `export QUACK_OUTPUT="text"`)
	assert.Contains(t, output, `export QUACK_TOKEN="profile-token"`)
}

func TestAuthDescribe_ShowsSourcesAndAuthStatus(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("HOME", dir)

	rec := &requestRecorder{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rec.record(r)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"data":[]}`))
	}))
	defer srv.Close()

	require.NoError(t, SaveUserConfig(&UserConfig{
		CurrentProfile: "default",
		Profiles: map[string]Profile{
			"default": {
				Host:   srv.URL,
				Token:  "profile-token",
				Output: "json",
			},
		},
	}))

	rootCmd := newRootCmd()
	rootCmd.SetArgs([]string{"--output", "json", "auth", "describe"})

	restore := captureStdout(t)
	err := rootCmd.Execute()
	output := restore()
	require.NoError(t, err)

	var payload map[string]any
	require.NoError(t, json.Unmarshal([]byte(output), &payload))
	assert.Equal(t, "default", payload["profile"])
	assert.Equal(t, srv.URL, payload["host"])
	assert.Equal(t, "profile", payload["host_source"])
	assert.Equal(t, "token", payload["auth_type"])
	assert.Equal(t, "profile", payload["auth_source"])
	assert.Equal(t, "json", payload["output"])
	assert.Equal(t, true, payload["authenticated"])

	captured := rec.last()
	assert.Equal(t, "/v1/catalogs", captured.Path)
	assert.Contains(t, captured.Query, "max_results=1")
}

func TestRemovedConfigCommandIsRejected(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("HOME", dir)

	rootCmd := newRootCmd()
	rootCmd.SetArgs([]string{"config", "show"})

	err := rootCmd.Execute()
	require.Error(t, err)
	assert.Contains(t, err.Error(), `unknown command "config"`)
}
