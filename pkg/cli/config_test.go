package cli

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUserConfig_ActiveProfile(t *testing.T) {
	cfg := &UserConfig{
		CurrentProfile: "default",
		Profiles: map[string]Profile{
			"default": {
				Host:   "http://localhost:8080",
				APIKey: "dak_default",
				Output: "table",
			},
			"staging": {
				Host:   "https://staging.example.com",
				APIKey: "dak_staging",
				Output: "json",
			},
		},
	}

	tests := []struct {
		name     string
		override string
		wantHost string
		wantErr  string
	}{
		{
			name:     "uses current profile",
			override: "",
			wantHost: "http://localhost:8080",
		},
		{
			name:     "override to staging",
			override: "staging",
			wantHost: "https://staging.example.com",
		},
		{
			name:     "nonexistent profile returns empty",
			override: "nonexistent",
			wantHost: "",
			wantErr:  `profile "nonexistent" not found`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p, err := cfg.ActiveProfile(tt.override)
			if tt.wantErr != "" {
				require.EqualError(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.wantHost, p.Host)
		})
	}
}

func TestLoadSaveUserConfig(t *testing.T) {
	// Override config path for testing
	dir := t.TempDir()
	t.Setenv("HOME", dir)

	// Save a config
	cfg := &UserConfig{
		CurrentProfile: "test",
		Profiles: map[string]Profile{
			"test": {
				Host:   "http://test:8080",
				APIKey: "dak_test",
			},
		},
	}
	err := SaveUserConfig(cfg)
	require.NoError(t, err)

	// Verify file exists
	configPath := filepath.Join(dir, ".duck", "config.yaml")
	_, err = os.Stat(configPath)
	require.NoError(t, err)

	// Load it back
	loaded, err := LoadUserConfig()
	require.NoError(t, err)
	assert.Equal(t, "test", loaded.CurrentProfile)
	require.Contains(t, loaded.Profiles, "test")
	assert.Equal(t, "http://test:8080", loaded.Profiles["test"].Host)
	assert.Equal(t, "dak_test", loaded.Profiles["test"].APIKey)
}

func TestLoadUserConfig_NotFound(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("HOME", dir)

	_, err := LoadUserConfig()
	require.Error(t, err)
}
