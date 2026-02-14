package cli

import (
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
