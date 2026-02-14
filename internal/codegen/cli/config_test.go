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
  implicit_params: [catalogName]
skip_operations: []
`,
			check: func(t *testing.T, cfg *Config) {
				t.Helper()
				assert.Equal(t, "table", cfg.Global.DefaultOutput)
				assert.Equal(t, []string{"catalogName"}, cfg.Global.ImplicitParams)
			},
		},
		{
			name: "with command overrides",
			yaml: `
global:
  default_output: table
command_overrides:
  listSchemas:
    table_columns: [name, owner]
  createSchema:
    positional_args: [name]
`,
			check: func(t *testing.T, cfg *Config) {
				t.Helper()
				require.Contains(t, cfg.CommandOverrides, "listSchemas")
				require.NotNil(t, cfg.CommandOverrides["listSchemas"].TableColumns)
				assert.Equal(t, []string{"name", "owner"}, *cfg.CommandOverrides["listSchemas"].TableColumns)
				require.Contains(t, cfg.CommandOverrides, "createSchema")
				require.NotNil(t, cfg.CommandOverrides["createSchema"].PositionalArgs)
				assert.Equal(t, []string{"name"}, *cfg.CommandOverrides["createSchema"].PositionalArgs)
			},
		},
		{
			name: "with skip operations",
			yaml: `
global:
  default_output: table
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
command_overrides:
  executeQuery:
    verb: ""
    flag_aliases:
      sql:
        short: s
`,
			check: func(t *testing.T, cfg *Config) {
				t.Helper()
				ov := cfg.CommandOverrides["executeQuery"]
				require.NotNil(t, ov.Verb)
				assert.Empty(t, *ov.Verb)
				require.Contains(t, ov.FlagAliases, "sql")
				assert.Equal(t, "s", ov.FlagAliases["sql"].Short)
			},
		},
		{
			name: "with confirm override",
			yaml: `
global:
  default_output: table
  confirm_destructive: true
command_overrides:
  purgeLineage:
    verb: purge
    confirm: true
`,
			check: func(t *testing.T, cfg *Config) {
				t.Helper()
				assert.True(t, cfg.Global.ConfirmDestructive)
				ov := cfg.CommandOverrides["purgeLineage"]
				require.NotNil(t, ov.Confirm)
				assert.True(t, *ov.Confirm)
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
command_overrides:
  commitTableIngestion:
    verb: commit
    flatten_fields: [options]
`,
			check: func(t *testing.T, cfg *Config) {
				t.Helper()
				ov := cfg.CommandOverrides["commitTableIngestion"]
				assert.Equal(t, []string{"options"}, ov.FlattenFields)
			},
		},
		{
			name: "with compound flags",
			yaml: `
global:
  default_output: table
command_overrides:
  createTable:
    compound_flags:
      columns:
        fields: [name, type]
        separator: ":"
`,
			check: func(t *testing.T, cfg *Config) {
				t.Helper()
				ov := cfg.CommandOverrides["createTable"]
				require.Contains(t, ov.CompoundFlags, "columns")
				assert.Equal(t, []string{"name", "type"}, ov.CompoundFlags["columns"].Fields)
				assert.Equal(t, ":", ov.CompoundFlags["columns"].Separator)
			},
		},
		{
			name: "with group overrides",
			yaml: `
global:
  default_output: table
group_overrides:
  Catalogs:
    name: catalog
    short: "Manage the data catalog"
`,
			check: func(t *testing.T, cfg *Config) {
				t.Helper()
				require.Contains(t, cfg.GroupOverrides, "Catalogs")
				assert.Equal(t, "catalog", cfg.GroupOverrides["Catalogs"].Name)
				assert.Equal(t, "Manage the data catalog", cfg.GroupOverrides["Catalogs"].Short)
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
