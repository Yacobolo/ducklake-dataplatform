package apilint

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// Config holds per-rule severity overrides loaded from an .apilint.yaml file.
type Config struct {
	Rules map[string]string `yaml:"rules"` // ruleID → "off"/"error"/"warning"/"warn"/"info"
}

// LoadConfig reads and parses an .apilint.yaml configuration file.
func LoadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path) //nolint:gosec // path is provided by the caller (CLI flag or test)
	if err != nil {
		return nil, fmt.Errorf("read config %s: %w", path, err)
	}
	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parse config %s: %w", path, err)
	}
	return &cfg, nil
}
