// Package cli implements the CLI code generator that produces Cobra commands
// from an OpenAPI spec and a cli-config.yaml file.
package cli

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// Config is the top-level cli-config.yaml structure.
type Config struct {
	Global         GlobalConfig           `yaml:"global"`
	Groups         map[string]GroupConfig `yaml:"groups"`
	SkipOperations []string               `yaml:"skip_operations"`
}

// GlobalConfig holds global CLI settings.
type GlobalConfig struct {
	DefaultOutput      string `yaml:"default_output"`
	ConfirmDestructive bool   `yaml:"confirm_destructive"`
}

// GroupConfig represents a top-level CLI command group in the config.
type GroupConfig struct {
	Short    string                   `yaml:"short"`
	Commands map[string]CommandConfig `yaml:"commands"`
}

// CommandConfig represents a single CLI command in the config.
type CommandConfig struct {
	OperationID         string                          `yaml:"operation_id"`
	CommandPath         []string                        `yaml:"command_path"`
	Verb                string                          `yaml:"verb"`
	TableColumns        []string                        `yaml:"table_columns"`
	PositionalArgs      []string                        `yaml:"positional_args"`
	Examples            []string                        `yaml:"examples"`
	FlagAliases         map[string]FlagAliasConfig      `yaml:"flag_aliases"`
	Confirm             bool                            `yaml:"confirm"`
	FlattenFields       []string                        `yaml:"flatten_fields"`
	CompoundFlags       map[string]CompoundFlagConfig   `yaml:"compound_flags"`
	ConditionalRequires map[string][]ConditionalRequire `yaml:"conditional_requires"`
}

// FlagAliasConfig holds a short alias for a flag.
type FlagAliasConfig struct {
	Short string `yaml:"short"`
}

// CompoundFlagConfig defines a compound repeatable flag (e.g., --column name:type).
type CompoundFlagConfig struct {
	Fields    []string `yaml:"fields"`
	Separator string   `yaml:"separator"`
}

// ConditionalRequire defines flags required when a discriminator has a specific value.
type ConditionalRequire struct {
	Value    string   `yaml:"value"`
	Required []string `yaml:"required"`
}

// LoadConfig reads and parses a cli-config.yaml file.
func LoadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path) //nolint:gosec // path is from CLI flag, not user input
	if err != nil {
		return nil, fmt.Errorf("read cli config: %w", err)
	}
	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parse cli config: %w", err)
	}
	return &cfg, nil
}
