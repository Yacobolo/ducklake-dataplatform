package duckdbsql

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDuckDB15_ParseExpr_PythonLambda(t *testing.T) {
	expr, err := ParseExpr(`lambda x: x + 1`)
	require.NoError(t, err)

	lambda, ok := expr.(*LambdaExpr)
	require.True(t, ok, "expected LambdaExpr")
	assert.Equal(t, []string{"x"}, lambda.Params)
}

func TestDuckDB15_ParseExpr_PythonLambdaMultipleParams(t *testing.T) {
	expr, err := ParseExpr(`lambda x, i: x + i`)
	require.NoError(t, err)

	lambda, ok := expr.(*LambdaExpr)
	require.True(t, ok, "expected LambdaExpr")
	assert.Equal(t, []string{"x", "i"}, lambda.Params)
}

func TestDuckDB15_Format_CanonicalizesLambdaSyntax(t *testing.T) {
	stmt, err := Parse(`SELECT list_transform([1, 2, 3], x -> x + 1)`)
	require.NoError(t, err)

	assert.Equal(t, `SELECT list_transform([1, 2, 3], lambda x: "x" + 1)`, Format(stmt))
}

func TestDuckDB15_Parse_CoversNewSyntaxForms(t *testing.T) {
	tests := []string{
		`SELECT list_transform([1, 2, 3], lambda x: x + 1)`,
		`SELECT list_reduce([1, 2, 3], lambda x, i: x + i)`,
		`SELECT COLUMNS(lambda c: c LIKE '%num%') FROM numbers`,
		`SELECT CAST(payload AS VARIANT) FROM events`,
		`SELECT CAST(NULL AS GEOMETRY)`,
		`SELECT CAST(NULL AS GEOMETRY('OGC:CRS84'))`,
		`SELECT variant_extract(payload, 'name') FROM events`,
		`SELECT * FROM read_duckdb('numbers.duckdb')`,
	}

	for _, sql := range tests {
		t.Run(sql, func(t *testing.T) {
			_, err := Parse(sql)
			require.NoError(t, err)
		})
	}
}

func TestDuckDB15_AnalyzeColumnLineage_PythonLambda(t *testing.T) {
	result := analyzeSQL(t, `SELECT list_transform(vals, lambda x: x + delta) AS transformed FROM items`, SchemaInfo{
		"items": {"vals", "delta"},
	})

	require.Len(t, result, 1)
	assert.Equal(t, "transformed", result[0].Name)
	assert.Equal(t, "EXPRESSION", result[0].TransformType)
	assertSources(t, result[0], []ColumnOrigin{
		{Table: "items", Column: "vals"},
		{Table: "items", Column: "delta"},
	})
}
