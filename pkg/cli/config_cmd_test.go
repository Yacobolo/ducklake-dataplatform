package cli

import (
	"encoding/json"
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

func TestConfigShow_TableOutput(t *testing.T) {
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
	rootCmd.SetArgs([]string{"config", "show", "--output", "table"})

	restore := captureStdout(t)
	err := rootCmd.Execute()
	output := restore()

	require.NoError(t, err)
	assert.Contains(t, output, "PROFILE")
	assert.Contains(t, output, "ACTIVE")
	assert.Contains(t, output, "default")
	assert.Contains(t, output, "api-key")
}

func TestConfigSetProfile_GlobalJSONOutputNotShadowed(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("HOME", dir)

	rootCmd := newRootCmd()
	rootCmd.SetArgs([]string{
		"--output", "json",
		"config", "set-profile",
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

func TestConfigSetProfile_ValidatesDefaultOutputAndHost(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("HOME", dir)

	t.Run("invalid default output", func(t *testing.T) {
		rootCmd := newRootCmd()
		rootCmd.SetArgs([]string{
			"config", "set-profile",
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
			"config", "set-profile",
			"--name", "default",
			"--host", "localhost:8080",
		})

		err := rootCmd.Execute()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "invalid host")
	})
}

func TestConfigCommands_RejectUnexpectedArgs(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("HOME", dir)

	cfg := &UserConfig{
		CurrentProfile: "default",
		Profiles: map[string]Profile{
			"default": {Host: "http://localhost:8080"},
		},
	}
	require.NoError(t, SaveUserConfig(cfg))

	tests := []struct {
		name string
		args []string
	}{
		{
			name: "show",
			args: []string{"config", "show", "extra"},
		},
		{
			name: "set-profile",
			args: []string{"config", "set-profile", "--name", "p1", "extra"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rootCmd := newRootCmd()
			rootCmd.SetArgs(tt.args)

			err := rootCmd.Execute()
			require.Error(t, err)
			assert.Contains(t, err.Error(), "unknown command")
		})
	}
}
