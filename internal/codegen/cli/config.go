// Package cli implements the CLI code generator that produces Cobra commands
// from an OpenAPI spec and a cli-config.yaml file.
package cli

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// Config is the top-level cli-config.yaml structure.
// It uses a convention-over-configuration approach: most fields are inferred
// from the OpenAPI spec, and only deviations need to be specified.
type Config struct {
	Global           GlobalConfig               `yaml:"global"`
	GroupOverrides   map[string]GroupOverride   `yaml:"group_overrides"`
	CommandOverrides map[string]CommandOverride `yaml:"command_overrides"`
	SkipOperations   []string                   `yaml:"skip_operations"`
}

// GlobalConfig holds global CLI settings.
type GlobalConfig struct {
	DefaultOutput      string   `yaml:"default_output"`
	ConfirmDestructive bool     `yaml:"confirm_destructive"`
	ImplicitParams     []string `yaml:"implicit_params"`
}

// GroupOverride allows overriding the inferred group name or description.
type GroupOverride struct {
	// Name overrides the CLI group name (e.g., "catalog" instead of "catalogs").
	Name string `yaml:"name,omitempty"`
	// Short overrides the group short description.
	Short string `yaml:"short,omitempty"`
}

// CommandOverride specifies per-command deviations from conventions.
// All fields are optional — only set what differs from the convention.
type CommandOverride struct {
	Verb                *string                         `yaml:"verb,omitempty"`
	Group               string                          `yaml:"group,omitempty"`
	CommandPath         *[]string                       `yaml:"command_path,omitempty"`
	TableColumns        *[]string                       `yaml:"table_columns,omitempty"`
	PositionalArgs      *[]string                       `yaml:"positional_args,omitempty"`
	Examples            []string                        `yaml:"examples,omitempty"`
	FlagAliases         map[string]FlagAliasConfig      `yaml:"flag_aliases,omitempty"`
	Confirm             *bool                           `yaml:"confirm,omitempty"`
	FlattenFields       []string                        `yaml:"flatten_fields,omitempty"`
	CompoundFlags       map[string]CompoundFlagConfig   `yaml:"compound_flags,omitempty"`
	ConditionalRequires map[string][]ConditionalRequire `yaml:"conditional_requires,omitempty"`
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

// CommandConfig is the resolved configuration for a single CLI command,
// produced by merging convention-inferred defaults with any overrides.
type CommandConfig struct {
	OperationID         string
	CommandPath         []string
	Verb                string
	TableColumns        []string
	PositionalArgs      []string
	Examples            []string
	FlagAliases         map[string]FlagAliasConfig
	Confirm             bool
	FlattenFields       []string
	CompoundFlags       map[string]CompoundFlagConfig
	ConditionalRequires map[string][]ConditionalRequire
}

// GroupConfig is the resolved configuration for a CLI group.
type GroupConfig struct {
	Short    string
	Commands map[string]CommandConfig
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
