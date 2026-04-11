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

func TestValidateQueries_UnknownConflictTargetFailsClearly(t *testing.T) {
	err := ValidateQueries(Catalog{
		Tables: map[string]Table{
			"auth_providers": {
				Name: "auth_providers",
				Columns: []Column{
					{Name: "id", DBType: "INTEGER", NotNull: true},
					{Name: "oidc_enabled", DBType: "INTEGER", NotNull: true},
				},
			},
		},
	}, []cuesql.Query{
		{
			Name: "UpsertAuthProviderConfig",
			Kind: cuesql.KindExec,
			Params: []cuesql.Param{
				{Name: "OidcEnabled", Type: "int64"},
			},
			Insert: &cuesql.Insert{
				Into:    "auth_providers",
				Columns: []string{"id", "oidc_enabled"},
				Values: []cuesql.ValueExpr{
					{SQL: "1"},
					{Param: "OidcEnabled"},
				},
				Conflict: &cuesql.Conflict{
					Targets: []string{"missing_id"},
					DoUpdate: []cuesql.Assignment{
						{Column: "oidc_enabled", Value: cuesql.ValueExpr{SQL: "excluded.oidc_enabled"}},
					},
				},
			},
		},
	})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown conflict target missing_id")
}

func TestValidateQueries_LiteralPredicateDoesNotRequireParam(t *testing.T) {
	err := ValidateQueries(Catalog{
		Tables: map[string]Table{
			"setup_state": {
				Name: "setup_state",
				Columns: []Column{
					{Name: "id", DBType: "INTEGER", NotNull: true},
				},
			},
		},
	}, []cuesql.Query{
		{
			Name: "GetSetupState",
			Kind: cuesql.KindOne,
			Result: cuesql.Result{
				Row: "SetupState",
				Fields: []cuesql.Field{
					{Name: "ID", Type: "int64"},
				},
			},
			Select: &cuesql.Select{
				From:    "setup_state",
				Columns: []cuesql.Column{{Expr: "id"}},
				Where: []cuesql.Predicate{
					{Column: "id", Op: "=", ValueSQL: "1"},
				},
			},
		},
	})

	require.NoError(t, err)
}

func TestValidateQueries_PredicateGroupCannotMixScalarFields(t *testing.T) {
	err := ValidateQueries(Catalog{
		Tables: map[string]Table{
			"privilege_grants": {
				Name: "privilege_grants",
				Columns: []Column{
					{Name: "principal_id", DBType: "TEXT", NotNull: true},
				},
			},
		},
	}, []cuesql.Query{
		{
			Name:   "BrokenGroup",
			Kind:   cuesql.KindOne,
			Result: cuesql.Result{Scalar: "int64"},
			Select: &cuesql.Select{
				From:    "privilege_grants",
				Columns: []cuesql.Column{{Expr: "COUNT(*)"}},
				Where: []cuesql.Predicate{
					{
						Column: "principal_id",
						All: []cuesql.Predicate{
							{Column: "principal_id", Op: "=", ValueSQL: "'p1'"},
						},
					},
				},
			},
		},
	})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "predicate groups cannot mix with scalar predicate fields")
}

func TestValidateQueries_SelectCannotDefineBothLimitSQLAndLimitParam(t *testing.T) {
	err := ValidateQueries(Catalog{
		Tables: map[string]Table{
			"setup_state": {
				Name: "setup_state",
				Columns: []Column{
					{Name: "id", DBType: "INTEGER", NotNull: true},
				},
			},
		},
	}, []cuesql.Query{
		{
			Name: "BrokenLimit",
			Kind: cuesql.KindOne,
			Params: []cuesql.Param{
				{Name: "Limit", Type: "int64"},
			},
			Result: cuesql.Result{Scalar: "int64"},
			Select: &cuesql.Select{
				From:       "setup_state",
				Columns:    []cuesql.Column{{Expr: "COUNT(*)"}},
				LimitSQL:   "1",
				LimitParam: "Limit",
			},
		},
	})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "select cannot define both limitSQL and limitParam")
}

func writeQuerydefFile(t *testing.T, dir, name, contents string) {
	t.Helper()
	err := os.WriteFile(filepath.Join(dir, name), []byte(contents), 0o600)
	require.NoError(t, err)
}
