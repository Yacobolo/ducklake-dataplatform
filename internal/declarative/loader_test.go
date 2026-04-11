package declarative

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoadDirectory_CUEGraph(t *testing.T) {
	dir := newCUEModule(t)

	writeCUEFile(t, filepath.Join(dir, "security", "principals.cue"), `
package duckconfig

platform: security: principals: {
	admin: {
		type: "user"
		is_admin: true
	}
	analyst: {
		type: "user"
	}
}
`)

	writeCUEFile(t, filepath.Join(dir, "catalogs", "main", "catalog.cue"), `
package duckconfig

platform: catalogs: main: {
	metastore_type: "sqlite"
	dsn: "meta.sqlite"
	data_path: "data"
	schemas: analytics: {
		tables: orders: {
			table_type: "MANAGED"
			columns: [{
				name: "id"
				type: "BIGINT"
			}]
		}
		views: summary: {
			view_definition: "SELECT COUNT(*) FROM orders"
		}
	}
}
`)

	writeCUEFile(t, filepath.Join(dir, "workspaces", "personal", "workspace.cue"), `
package duckconfig

platform: workspaces: personal: {
	kind: "personal"
	owner_principal: "admin"
	folders: analysis: {
		default_project_ref: "personal/core"
		notebooks: orders: {
			project_ref: "personal/core"
			environment_ref: "personal/core/dev"
			cells: [{
				type: "sql"
				content: "select 1"
			}]
		}
	}
}
`)

	writeCUEFile(t, filepath.Join(dir, "projects", "core", "project.cue"), `
package duckconfig

platform: projects: core: {
	workspace_ref: "personal"
	kind: "personal"
	environments: dev: {
		kind: "development"
		target_catalog: "main"
		target_schema: "analytics"
	}
	models: daily_orders: {
		materialization: "VIEW"
		sql: "select 1"
	}
	macros: fmt_money: {
		macro_type: "SCALAR"
		body: "amount"
	}
}
`)

	state, err := LoadDirectory(dir)
	require.NoError(t, err)

	require.Len(t, state.Principals, 2)
	require.Len(t, state.Catalogs, 1)
	require.Len(t, state.Tables, 1)
	require.Len(t, state.Workspaces, 1)
	require.Len(t, state.Folders, 1)
	require.Len(t, state.Notebooks, 1)
	require.Len(t, state.Projects, 1)
	require.Len(t, state.Environments, 1)
	require.Len(t, state.Models, 1)
	require.Len(t, state.Macros, 1)
	assert.Equal(t, "personal/analysis", state.Notebooks[0].Spec.FolderRef)
}

func TestLoadDirectory_RejectsYAML(t *testing.T) {
	dir := newCUEModule(t)
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "domains"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "domains", "revenue.yaml"), []byte("kind: Domain\n"), 0o644))

	_, err := LoadDirectory(dir)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "YAML declarative config is no longer supported")
}

func TestLoadDirectory_ConflictingFragmentsFail(t *testing.T) {
	dir := newCUEModule(t)

	writeCUEFile(t, filepath.Join(dir, "workspaces", "one.cue"), `
package duckconfig

platform: workspaces: personal: {
	kind: "personal"
	owner_principal: "admin"
}
`)

	writeCUEFile(t, filepath.Join(dir, "workspaces", "two.cue"), `
package duckconfig

platform: workspaces: personal: {
	kind: "shared"
	owner_team_id: "team-1"
}
`)

	_, err := LoadDirectory(dir)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "conflicting values")
}

func newCUEModule(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "cue.mod"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "cue.mod", "module.cue"), []byte(cueModuleFile), 0o644))
	return dir
}

func writeCUEFile(t *testing.T, path, content string) {
	t.Helper()
	require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
	require.NoError(t, os.WriteFile(path, []byte(content), 0o644))
}
