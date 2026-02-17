package model

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoadStarMacroScopes_PrecedenceProjectOverGlobalOverSystem(t *testing.T) {
	tmp := t.TempDir()
	macrosRoot := filepath.Join(tmp, "macros")
	require.NoError(t, os.MkdirAll(filepath.Join(macrosRoot, "system"), 0o755))
	require.NoError(t, os.MkdirAll(filepath.Join(macrosRoot, "catalog_global"), 0o755))
	require.NoError(t, os.MkdirAll(filepath.Join(macrosRoot, "analytics"), 0o755))

	require.NoError(t, os.WriteFile(filepath.Join(macrosRoot, "system", "utils.star"), []byte("def cents_to_dollars(col):\n    return \"system\"\n"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(macrosRoot, "catalog_global", "utils.star"), []byte("def cents_to_dollars(col):\n    return \"global\"\n"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(macrosRoot, "analytics", "utils.star"), []byte("def cents_to_dollars(col):\n    return \"project\"\n"), 0o644))

	wd, err := os.Getwd()
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.Chdir(wd) })
	require.NoError(t, os.Chdir(tmp))

	svc := &Service{}
	defs, runtimes, err := svc.loadStarMacroScopes("analytics")
	require.NoError(t, err)
	require.Contains(t, defs, "utils.cents_to_dollars")
	require.Contains(t, runtimes, "system")
	require.Contains(t, runtimes, "catalog_global")
	require.Contains(t, runtimes, "project")

	def := defs["utils.cents_to_dollars"]
	assert.Equal(t, "project", def.runtimeKey)
}

func TestTopLevelFunctionNames(t *testing.T) {
	src := `
def one(a):
    return a

def two(a, b=1):
    return a + b

def one(a):
    return a
`

	assert.Equal(t, []string{"one", "two"}, topLevelFunctionNames(src))
}

func TestLoadStarModules(t *testing.T) {
	root := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(root, "nested"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(root, "utils.star"), []byte("def cents_to_dollars(col):\n    return col\n"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(root, "nested", "date.star"), []byte("def start_of_day(col):\n    return col\n"), 0o644))

	modules, err := loadStarModules(root)
	require.NoError(t, err)
	require.Len(t, modules, 2)
	assert.Contains(t, modules, "utils")
	assert.Contains(t, modules, "nested.date")
}
