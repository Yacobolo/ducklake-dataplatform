package cuesqlgen

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	cuesql "duck-demo/pkg/cue-sql"
)

func TestGenerateQueries_DynamicPredicatesRenderBuilderWithoutBooleanHacks(t *testing.T) {
	catalog := Catalog{
		Tables: map[string]Table{
			"dashboards": {
				Name: "dashboards",
				Columns: []Column{
					{Name: "id", DBType: "TEXT", NotNull: true},
					{Name: "owner", DBType: "TEXT", NotNull: true},
					{Name: "folder_id", DBType: "TEXT", NotNull: false},
				},
			},
		},
	}
	queries := []cuesql.Query{
		{
			Name: "ListDashboards",
			Kind: cuesql.KindMany,
			Params: []cuesql.Param{
				{Name: "owner", Type: "string"},
				{Name: "folderIDs", Type: "[]string"},
				{Name: "limit", Type: "int64"},
				{Name: "offset", Type: "int64"},
			},
			Result: cuesql.Result{Table: "dashboards"},
			Select: &cuesql.Select{
				From: "dashboards",
				Where: []cuesql.Predicate{
					{Column: "owner", Op: "=", Param: "owner", Optional: true},
					{Column: "folder_id", Param: "folderIDs", Slice: true, Optional: true},
				},
				OrderBy:     []cuesql.OrderBy{{Expr: "id"}},
				LimitParam:  "limit",
				OffsetParam: "offset",
			},
		},
	}

	output := string(generateQueries(catalog, queries))
	require.Contains(t, output, "func buildListDashboardsSQL")
	assert.Contains(t, output, "if arg.Owner != \"\"")
	assert.Contains(t, output, "strings.Join(placeholders, \", \")")
	assert.NotContains(t, output, "(? = '' OR")
	assert.True(t, strings.Contains(output, "folder_id IN (") || strings.Contains(output, "folder_id IN (\""))
}

func TestGenerateQueries_SingleParamCompatibilityUsesDirectArgumentSignature(t *testing.T) {
	catalog := Catalog{
		Tables: map[string]Table{
			"principals": {
				Name: "principals",
				Columns: []Column{
					{Name: "id", DBType: "TEXT", NotNull: true},
					{Name: "name", DBType: "TEXT", NotNull: true},
				},
			},
		},
	}
	queries := []cuesql.Query{
		{
			Name:      "GetPrincipal",
			Kind:      cuesql.KindOne,
			ParamMode: "single",
			Params:    []cuesql.Param{{Name: "id", Type: "string"}},
			Result:    cuesql.Result{Table: "principals"},
			Raw: &cuesql.Raw{
				SQL:  "SELECT id, name FROM principals WHERE id = ?",
				Bind: []string{"id"},
			},
		},
	}

	output := string(generateQueries(catalog, queries))
	assert.Contains(t, output, "func (s *Store) GetPrincipal(ctx context.Context, id string) (Principal, error)")
	assert.NotContains(t, output, "func (s *Store) GetPrincipal(ctx context.Context, arg GetPrincipalParams)")
	assert.Contains(t, output, "row := s.db.QueryRowContext(ctx, getPrincipalSQL, id)")
}

func TestGenerateQueries_RawBindOrderFollowsExplicitBindSequence(t *testing.T) {
	catalog := Catalog{}
	queries := []cuesql.Query{
		{
			Name:      "GetPair",
			Kind:      cuesql.KindExec,
			ParamMode: "struct",
			Params: []cuesql.Param{
				{Name: "First", Type: "string"},
				{Name: "Second", Type: "string"},
			},
			Raw: &cuesql.Raw{
				SQL:  "UPDATE pairs SET left_value = ?, right_value = ?",
				Bind: []string{"Second", "First"},
			},
		},
	}

	output := string(generateQueries(catalog, queries))
	assert.Contains(t, output, "s.db.ExecContext(ctx, getPairSQL, arg.Second, arg.First)")
}

func TestGenerateQueries_DynamicBuilderHandlesStaticLiteralPredicates(t *testing.T) {
	catalog := Catalog{
		Tables: map[string]Table{
			"audit_log": {
				Name: "audit_log",
				Columns: []Column{
					{Name: "id", DBType: "TEXT", NotNull: true},
					{Name: "action", DBType: "TEXT", NotNull: true},
					{Name: "status", DBType: "TEXT", NotNull: true},
					{Name: "created_at", DBType: "TEXT", NotNull: true},
				},
			},
		},
	}
	queries := []cuesql.Query{
		{
			Name: "CountQueryHistory",
			Kind: cuesql.KindOne,
			Params: []cuesql.Param{
				{Name: "Status", Type: "sql.NullString"},
			},
			Result: cuesql.Result{Scalar: "int64"},
			Select: &cuesql.Select{
				From:    "audit_log",
				Columns: []cuesql.Column{{Expr: "COUNT(*)"}},
				Where: []cuesql.Predicate{
					{Column: "action", Op: "=", ValueSQL: "'QUERY'"},
					{Column: "status", Op: "=", Param: "Status", Optional: true},
				},
			},
		},
	}

	output := string(generateQueries(catalog, queries))
	assert.Contains(t, output, `where = append(where, "action = 'QUERY'")`)
	assert.Contains(t, output, `if arg.Status.Valid {`)
	assert.NotContains(t, output, "arg.)")
}

func TestGenerateQueries_DynamicBuilderHandlesPredicateGroups(t *testing.T) {
	catalog := Catalog{
		Tables: map[string]Table{
			"privilege_grants": {
				Name: "privilege_grants",
				Columns: []Column{
					{Name: "id", DBType: "TEXT", NotNull: true},
					{Name: "principal_id", DBType: "TEXT", NotNull: true},
					{Name: "principal_name", DBType: "TEXT", NotNull: true},
					{Name: "privilege", DBType: "TEXT", NotNull: true},
				},
			},
		},
	}
	queries := []cuesql.Query{
		{
			Name: "CheckDirectGrantAny",
			Kind: cuesql.KindOne,
			Params: []cuesql.Param{
				{Name: "PrincipalID", Type: "sql.NullString"},
				{Name: "PrincipalName", Type: "sql.NullString"},
				{Name: "Privilege", Type: "string"},
			},
			Result: cuesql.Result{Scalar: "int64"},
			Select: &cuesql.Select{
				From:    "privilege_grants",
				Columns: []cuesql.Column{{Expr: "COUNT(*)"}},
				Where: []cuesql.Predicate{
					{
						Any: []cuesql.Predicate{
							{Column: "principal_id", Op: "=", Param: "PrincipalID", Optional: true},
							{Column: "principal_name", Op: "=", Param: "PrincipalName", Optional: true},
						},
					},
					{Column: "privilege", Op: "=", Param: "Privilege"},
				},
			},
		},
	}

	output := string(generateQueries(catalog, queries))
	assert.Contains(t, output, "groupWhere0 := make([]string, 0, 2)")
	assert.Contains(t, output, `where = append(where, "("+strings.Join(groupWhere0, " OR ")+")")`)
	assert.Contains(t, output, "args = append(args, groupArgs0...)")
}

func TestGenerateModels_TableResultGeneratesStructFromCatalog(t *testing.T) {
	catalog := Catalog{
		Tables: map[string]Table{
			"tags": {
				Name: "tags",
				Columns: []Column{
					{Name: "id", DBType: "TEXT", NotNull: true},
					{Name: "key", DBType: "TEXT", NotNull: true},
					{Name: "value", DBType: "TEXT", NotNull: false},
					{Name: "created_by", DBType: "TEXT", NotNull: true},
					{Name: "created_at", DBType: "TEXT", NotNull: true},
				},
			},
		},
	}
	queries := []cuesql.Query{
		{
			Name:   "GetTag",
			Kind:   cuesql.KindOne,
			Params: []cuesql.Param{{Name: "id", Type: "string"}},
			Result: cuesql.Result{Table: "tags"},
			Select: &cuesql.Select{
				From: "tags",
				Where: []cuesql.Predicate{
					{Column: "id", Op: "=", Param: "id"},
				},
			},
		},
	}

	output := string(generateModels(catalog, queries))
	assert.Contains(t, output, "type Tag struct {")
	assert.Contains(t, output, "ID string")
	assert.Contains(t, output, "Key string")
	assert.Contains(t, output, "Value sql.NullString")
	assert.Contains(t, output, "CreatedBy string")
	assert.Contains(t, output, "CreatedAt string")
}
