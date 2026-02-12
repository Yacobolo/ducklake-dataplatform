package cli

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoadConfig(t *testing.T) {
	tests := []struct {
		name    string
		yaml    string
		wantErr bool
		check   func(t *testing.T, cfg *Config)
	}{
		{
			name: "minimal config",
			yaml: `
global:
  default_output: table
groups:
  catalog:
    short: "Catalog commands"
    commands:
      list-schemas:
        operation_id: listSchemas
        command_path: [schemas]
        verb: list
        table_columns: [name, owner]
`,
			check: func(t *testing.T, cfg *Config) {
				t.Helper()
				assert.Equal(t, "table", cfg.Global.DefaultOutput)
				require.Contains(t, cfg.Groups, "catalog")
				require.Contains(t, cfg.Groups["catalog"].Commands, "list-schemas")
				cmd := cfg.Groups["catalog"].Commands["list-schemas"]
				assert.Equal(t, "listSchemas", cmd.OperationID)
				assert.Equal(t, []string{"schemas"}, cmd.CommandPath)
				assert.Equal(t, "list", cmd.Verb)
				assert.Equal(t, []string{"name", "owner"}, cmd.TableColumns)
			},
		},
		{
			name: "with positional args and examples",
			yaml: `
global:
  default_output: json
groups:
  catalog:
    short: "Catalog"
    commands:
      create-schema:
        operation_id: createSchema
        command_path: [schemas]
        verb: create
        positional_args: [name]
        examples:
          - "duck catalog schemas create test"
`,
			check: func(t *testing.T, cfg *Config) {
				t.Helper()
				cmd := cfg.Groups["catalog"].Commands["create-schema"]
				assert.Equal(t, []string{"name"}, cmd.PositionalArgs)
				assert.Equal(t, []string{"duck catalog schemas create test"}, cmd.Examples)
			},
		},
		{
			name: "with skip operations",
			yaml: `
global:
  default_output: table
groups: {}
skip_operations:
  - createRowFilterTopLevel
`,
			check: func(t *testing.T, cfg *Config) {
				t.Helper()
				assert.Equal(t, []string{"createRowFilterTopLevel"}, cfg.SkipOperations)
			},
		},
		{
			name: "with flag aliases",
			yaml: `
global:
  default_output: table
groups:
  query:
    short: "Query"
    commands:
      execute:
        operation_id: executeQuery
        command_path: []
        verb: ""
        flag_aliases:
          sql:
            short: s
`,
			check: func(t *testing.T, cfg *Config) {
				t.Helper()
				cmd := cfg.Groups["query"].Commands["execute"]
				require.Contains(t, cmd.FlagAliases, "sql")
				assert.Equal(t, "s", cmd.FlagAliases["sql"].Short)
			},
		},
		{
			name: "with confirm",
			yaml: `
global:
  default_output: table
  confirm_destructive: true
groups:
  catalog:
    short: "Catalog"
    commands:
      delete-schema:
        operation_id: deleteSchema
        command_path: [schemas]
        verb: delete
        positional_args: [schemaName]
        confirm: true
`,
			check: func(t *testing.T, cfg *Config) {
				t.Helper()
				assert.True(t, cfg.Global.ConfirmDestructive)
				cmd := cfg.Groups["catalog"].Commands["delete-schema"]
				assert.True(t, cmd.Confirm)
			},
		},
		{
			name:    "invalid yaml",
			yaml:    `{invalid: yaml: [`,
			wantErr: true,
		},
		{
			name: "with flatten fields",
			yaml: `
global:
  default_output: table
groups:
  ingestion:
    short: "Ingestion"
    commands:
      commit:
        operation_id: commitTableIngestion
        command_path: []
        verb: commit
        positional_args: [schemaName, tableName]
        flatten_fields: [options]
`,
			check: func(t *testing.T, cfg *Config) {
				t.Helper()
				cmd := cfg.Groups["ingestion"].Commands["commit"]
				assert.Equal(t, []string{"options"}, cmd.FlattenFields)
			},
		},
		{
			name: "with compound flags",
			yaml: `
global:
  default_output: table
groups:
  catalog:
    short: "Catalog"
    commands:
      create-table:
        operation_id: createTable
        command_path: [tables]
        verb: create
        positional_args: [schemaName]
        compound_flags:
          columns:
            fields: [name, type]
            separator: ":"
`,
			check: func(t *testing.T, cfg *Config) {
				t.Helper()
				cmd := cfg.Groups["catalog"].Commands["create-table"]
				require.Contains(t, cmd.CompoundFlags, "columns")
				assert.Equal(t, []string{"name", "type"}, cmd.CompoundFlags["columns"].Fields)
				assert.Equal(t, ":", cmd.CompoundFlags["columns"].Separator)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "cli-config.yaml")
			require.NoError(t, os.WriteFile(path, []byte(tt.yaml), 0o644))

			cfg, err := LoadConfig(path)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			if tt.check != nil {
				tt.check(t, cfg)
			}
		})
	}
}

func TestLoadConfig_FileNotFound(t *testing.T) {
	_, err := LoadConfig("/nonexistent/path/config.yaml")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "read cli config")
}
