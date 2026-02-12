package cli

import (
	"fmt"
	"os"
	"path/filepath"

	"gopkg.in/yaml.v3"
)

// UserConfig represents ~/.duck/config.yaml.
type UserConfig struct {
	CurrentProfile string             `yaml:"current-profile"`
	Profiles       map[string]Profile `yaml:"profiles"`
}

// Profile represents a single named configuration profile.
type Profile struct {
	Host   string `yaml:"host,omitempty"`
	APIKey string `yaml:"api-key,omitempty"`
	Token  string `yaml:"token,omitempty"`
	Output string `yaml:"output,omitempty"`
}

// ActiveProfile returns the profile to use based on the override or current-profile.
func (c *UserConfig) ActiveProfile(override string) Profile {
	name := c.CurrentProfile
	if override != "" {
		name = override
	}
	if p, ok := c.Profiles[name]; ok {
		return p
	}
	return Profile{}
}

// ConfigDir returns the path to ~/.duck/.
func ConfigDir() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return ""
	}
	return filepath.Join(home, ".duck")
}

// ConfigPath returns the path to ~/.duck/config.yaml.
func ConfigPath() string {
	return filepath.Join(ConfigDir(), "config.yaml")
}

// LoadUserConfig reads ~/.duck/config.yaml.
func LoadUserConfig() (*UserConfig, error) {
	path := ConfigPath()
	data, err := os.ReadFile(path)
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

// SaveUserConfig writes ~/.duck/config.yaml.
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
