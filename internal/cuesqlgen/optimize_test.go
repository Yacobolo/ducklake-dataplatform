package cuesqlgen

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	cuesql "duck-demo/pkg/cue-sql"
)

func TestRenderOptimizedFile_InsertReturningTableKeepsValues(t *testing.T) {
	query := cuesql.Query{
		Name: "CreateTag",
		Kind: cuesql.KindOne,
		Params: []cuesql.Param{
			{Name: "ID", Type: "string"},
			{Name: "Key", Type: "string"},
			{Name: "Value", Type: "sql.NullString"},
			{Name: "CreatedBy", Type: "string"},
		},
		Result: cuesql.Result{Table: "tags"},
		Insert: &cuesql.Insert{
			Into:      "tags",
			Columns:   []string{"id", "key", "value", "created_by"},
			Values:    []cuesql.ValueExpr{{Param: "ID"}, {Param: "Key"}, {Param: "Value"}, {Param: "CreatedBy"}},
			Returning: true,
		},
	}

	body, err := renderOptimizedFile([]cuesql.Query{query})
	require.NoError(t, err)

	text := string(body)
	assert.Contains(t, text, "#InsertReturningTable & {")
	assert.Contains(t, text, "values:")
	assert.Contains(t, text, `{param: "CreatedBy"}`)
}

func TestRenderOptimizedFile_HoistsRepeatedCustomResultDefs(t *testing.T) {
	result := cuesql.Result{
		Row: "AuthIdentity",
		Fields: []cuesql.Field{
			{Name: "ID", Type: "string"},
			{Name: "Provider", Type: "string"},
		},
	}

	queries := []cuesql.Query{
		{
			Name:   "CreateAuthIdentity",
			Kind:   cuesql.KindOne,
			Result: result,
			Insert: &cuesql.Insert{Into: "auth_identities", Columns: []string{"id"}, Values: []cuesql.ValueExpr{{SQL: "'1'"}}},
		},
		{
			Name:   "ListAuthIdentities",
			Kind:   cuesql.KindMany,
			Result: result,
			Select: &cuesql.Select{From: "auth_identities", Columns: []cuesql.Column{{Expr: "id"}, {Expr: "provider"}}},
		},
	}

	body, err := renderOptimizedFile(queries)
	require.NoError(t, err)

	text := string(body)
	assert.Contains(t, text, "#AuthIdentityResult:")
	assert.Equal(t, 1, strings.Count(text, "fields: ["))
	assert.Contains(t, text, "result: #AuthIdentityResult")
}
