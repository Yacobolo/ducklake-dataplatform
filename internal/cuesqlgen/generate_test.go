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
