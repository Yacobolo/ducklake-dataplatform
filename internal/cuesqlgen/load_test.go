package cuesqlgen

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	cuesql "duck-demo/pkg/cue-sql"
)

func TestLoadQueries_DuplicateNamesAcrossFilesFail(t *testing.T) {
	dir := t.TempDir()
	writeQuerydefFile(t, dir, "one.cue", `
package querydefs

queries: [{name: "GetThing", kind: "exec", raw: {sql: "DELETE FROM things"}}]
`)
	writeQuerydefFile(t, dir, "two.cue", `
package querydefs

queries: [{name: "GetThing", kind: "exec", raw: {sql: "DELETE FROM other_things"}}]
`)

	_, err := LoadQueries(dir)
	require.Error(t, err)
	assert.Contains(t, err.Error(), `duplicate query name "GetThing"`)
}

func TestValidateQueries_UnknownRawBindFailsClearly(t *testing.T) {
	err := ValidateQueries(Catalog{}, []cuesql.Query{
		{
			Name: "DeleteThing",
			Kind: cuesql.KindExec,
			Params: []cuesql.Param{
				{Name: "ID", Type: "string"},
			},
			Raw: &cuesql.Raw{
				SQL:  "DELETE FROM things WHERE id = ?",
				Bind: []string{"Missing"},
			},
		},
	})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "raw query references unknown param Missing")
}

func TestValidateQueries_UnknownSelectTableFailsClearly(t *testing.T) {
	err := ValidateQueries(Catalog{Tables: map[string]Table{}}, []cuesql.Query{
		{
			Name:   "ListGhosts",
			Kind:   cuesql.KindMany,
			Result: cuesql.Result{Row: "Ghost", Fields: []cuesql.Field{{Name: "ID", Type: "string"}}},
			Select: &cuesql.Select{
				From: "ghosts",
			},
		},
	})

	require.Error(t, err)
	assert.Contains(t, err.Error(), `query ListGhosts: unknown table "ghosts"`)
}

func writeQuerydefFile(t *testing.T, dir, name, contents string) {
	t.Helper()
	err := os.WriteFile(filepath.Join(dir, name), []byte(contents), 0o600)
	require.NoError(t, err)
}
