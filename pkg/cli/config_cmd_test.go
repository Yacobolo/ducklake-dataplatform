package cli

import (
	"strings"
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

	require.NoError(t, SaveUserConfig(&UserConfig{
		CurrentProfile: "default",
		Profiles: map[string]Profile{
			"default": {
				Host:   "http://localhost:8080",
				APIKey: "dak_default_123456",
				Token:  "tok_default_abcdef",
				Output: "table",
			},
		},
	}))

	rootCmd := newRootCmd()
	rootCmd.SetArgs([]string{"config", "show", "--output", "table"})
	old := captureStdout(t)

	require.NoError(t, rootCmd.Execute())
	output := old()

	assert.Contains(t, output, "PROFILE")
	assert.Contains(t, output, "ACTIVE")
	assert.Contains(t, output, "HOST")
	assert.Contains(t, output, "default")
	assert.Contains(t, output, "http://localhost:8080")
	assert.Contains(t, output, "*")
	assert.NotContains(t, output, "current-profile:")
	assert.False(t, strings.Contains(output, "dak_default_123456"), "api key should be masked in table output")
}
