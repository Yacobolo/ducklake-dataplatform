package cli

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"gopkg.in/yaml.v3"
)

// UserConfig represents ~/.quack/config.yaml.
type UserConfig struct {
	CurrentProfile string             `yaml:"current-profile"`
	Profiles       map[string]Profile `yaml:"profiles"`
}

// Profile represents a single named configuration profile.
type Profile struct {
	Host                     string `yaml:"host,omitempty"`
	APIKey                   string `yaml:"api-key,omitempty"`
	Token                    string `yaml:"token,omitempty"`
	Output                   string `yaml:"output,omitempty"`
	QuackAccessExtensionPath string `yaml:"quack-access-extension-path,omitempty"`
}

// ActiveProfile returns the profile to use based on the override or current-profile.
func (c *UserConfig) ActiveProfile(override string) (Profile, error) {
	name := c.CurrentProfile
	if override != "" {
		name = override
	}
	if p, ok := c.Profiles[name]; ok {
		return p, nil
	}
	if override != "" {
		return Profile{}, fmt.Errorf("profile %q not found", override)
	}
	return Profile{}, nil
}

// ConfigDir returns the path to ~/.quack/.
func ConfigDir() string {
	if path := strings.TrimSpace(os.Getenv("QUACK_CONFIG_FILE")); path != "" {
		return filepath.Dir(path)
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return ""
	}
	return filepath.Join(home, ".quack")
}

// ConfigPath returns the path to ~/.quack/config.yaml.
func ConfigPath() string {
	if path := strings.TrimSpace(os.Getenv("QUACK_CONFIG_FILE")); path != "" {
		return path
	}
	return filepath.Join(ConfigDir(), "config.yaml")
}

// LoadUserConfig reads ~/.quack/config.yaml.
func LoadUserConfig() (*UserConfig, error) {
	path := ConfigPath()
	data, err := os.ReadFile(path) //nolint:gosec // path is derived from home dir, not user input
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}
	var cfg UserConfig
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	if cfg.Profiles == nil {
		cfg.Profiles = map[string]Profile{}
	}
	return &cfg, nil
}

// SaveUserConfig writes ~/.quack/config.yaml.
func SaveUserConfig(cfg *UserConfig) error {
	dir := ConfigDir()
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("create config dir: %w", err)
	}
	data, err := yaml.Marshal(cfg)
	if err != nil {
		return fmt.Errorf("marshal config: %w", err)
	}
	return os.WriteFile(ConfigPath(), data, 0o600)
}
