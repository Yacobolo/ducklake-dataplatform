package declarative

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExportDirectory_CUERoundTrip(t *testing.T) {
	original := &DesiredState{
		Principals: []PrincipalSpec{
			{Name: "admin", Type: "user", IsAdmin: true},
		},
		Domains: []DomainResource{
			{Name: "revenue", Spec: DomainSpec{Description: "Revenue domain"}},
		},
		Teams: []TeamResource{
			{Name: "analytics", Spec: TeamSpec{DomainRef: "revenue", ContactChannel: "#rev"}},
		},
		Catalogs: []CatalogResource{
			{CatalogName: "main", Spec: CatalogSpec{MetastoreType: "sqlite", DSN: "meta.sqlite", DataPath: "data"}},
		},
		Schemas: []SchemaResource{
			{CatalogName: "main", SchemaName: "analytics", Spec: SchemaSpec{Comment: "analytics"}},
		},
		Views: []ViewResource{
			{CatalogName: "main", SchemaName: "analytics", ViewName: "summary", Spec: ViewSpec{ViewDefinition: "select 1"}},
		},
		Workspaces: []WorkspaceResource{
			{Name: "personal", Spec: WorkspaceSpec{Kind: "personal", OwnerPrincipal: "admin"}},
		},
		Folders: []FolderResource{
			{Name: "analysis", Spec: FolderSpec{WorkspaceRef: "personal", DefaultProjectRef: "personal/core", DefaultEnvironmentRef: "personal/core/dev"}},
		},
		Projects: []ProjectResource{
			{Name: "core", Spec: ProjectSpec{WorkspaceRef: "personal", Kind: "personal"}},
		},
		Environments: []EnvironmentResource{
			{Name: "dev", Spec: EnvironmentSpec{ProjectRef: "personal/core", Kind: "development", TargetCatalog: "main", TargetSchema: "analytics"}},
		},
		Notebooks: []NotebookResource{
			{Name: "orders", Spec: NotebookSpec{Owner: "admin", WorkspaceRef: "personal", FolderRef: "personal/analysis", ProjectRef: "personal/core", EnvironmentRef: "personal/core/dev", Cells: []CellSpec{{Type: "sql", Content: "select 1"}}}},
		},
		Macros: []MacroResource{
			{Name: "fmt_money", Spec: MacroSpec{MacroType: "SCALAR", ProjectName: "core", Body: "amount"}},
		},
		Models: []ModelResource{
			{ProjectName: "core", ModelName: "daily_orders", Spec: ModelSpec{Materialization: "VIEW", SQL: "select 1"}},
		},
	}

	dir := t.TempDir()
	require.NoError(t, ExportDirectory(dir, original, false))

	assertFileExists(t, filepath.Join(dir, "cue.mod", "module.cue"))
	assertFileExists(t, filepath.Join(dir, "security", "principals.cue"))
	assertFileExists(t, filepath.Join(dir, "catalogs", "main", "catalog.cue"))
	assertFileExists(t, filepath.Join(dir, "workspaces", "personal", "workspace.cue"))
	assertFileExists(t, filepath.Join(dir, "projects", "core", "project.cue"))

	loaded, err := LoadDirectory(dir)
	require.NoError(t, err)

	plan := Diff(loaded, original)
	assert.False(t, plan.HasChanges(), "exported CUE should round-trip without drift")
}

func TestExportDirectory_EmptyStateWritesModuleOnly(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, ExportDirectory(dir, &DesiredState{}, false))

	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	assert.Equal(t, "cue.mod", entries[0].Name())
}

func TestExportDirectory_OverwriteProtection(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "existing.txt"), []byte("data"), 0o644))

	err := ExportDirectory(dir, &DesiredState{Principals: []PrincipalSpec{{Name: "user1", Type: "user"}}}, false)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not empty")

	require.NoError(t, ExportDirectory(dir, &DesiredState{Principals: []PrincipalSpec{{Name: "user1", Type: "user"}}}, true))
	assertFileExists(t, filepath.Join(dir, "security", "principals.cue"))
}

func assertFileExists(t *testing.T, path string) {
	t.Helper()
	_, err := os.Stat(path)
	require.NoError(t, err, "file should exist: %s", path)
}
