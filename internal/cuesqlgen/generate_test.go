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
