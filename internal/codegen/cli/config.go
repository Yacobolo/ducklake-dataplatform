package cli

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// CLIConfig is the top-level cli-config.yaml structure.
type CLIConfig struct {
	Global         GlobalConfig           `yaml:"global"`
	Groups         map[string]GroupConfig `yaml:"groups"`
	SkipOperations []string               `yaml:"skip_operations"`
}

type GlobalConfig struct {
	DefaultOutput      string `yaml:"default_output"`
	ConfirmDestructive bool   `yaml:"confirm_destructive"`
}

type GroupConfig struct {
	Short    string                   `yaml:"short"`
	Commands map[string]CommandConfig `yaml:"commands"`
}

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

type FlagAliasConfig struct {
	Short string `yaml:"short"`
}

type CompoundFlagConfig struct {
	Fields    []string `yaml:"fields"`
	Separator string   `yaml:"separator"`
}

type ConditionalRequire struct {
	Value    string   `yaml:"value"`
	Required []string `yaml:"required"`
}

// LoadConfig reads and parses a cli-config.yaml file.
func LoadConfig(path string) (*CLIConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read cli config: %w", err)
	}
	var cfg CLIConfig
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parse cli config: %w", err)
	}
	return &cfg, nil
}
