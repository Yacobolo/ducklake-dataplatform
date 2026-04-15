package sqlrewrite

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDuckDB15_ClassifyStatement_BlocksDangerousFunctionsInsidePythonLambda(t *testing.T) {
	_, err := ClassifyStatement(`SELECT list_transform([1, 2, 3], lambda x: read_csv_auto('secret.csv'))`)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "prohibited function")
}

func TestDuckDB15_ApplyColumnMasks_PreservesPythonLambdaExpressions(t *testing.T) {
	result, err := ApplyColumnMasks(
		`SELECT list_transform(tags, lambda x: x || suffix) AS tags, suffix FROM accounts`,
		"accounts",
		map[string]string{"suffix": "'***'"},
		[]string{"tags", "suffix"},
	)
	require.NoError(t, err)
	assert.Contains(t, result, `lambda x:`)
	assert.Contains(t, result, `||`)
	assert.Contains(t, result, `'***' AS "suffix"`)
}

func TestDuckDB15_InjectMultipleRowFilters_WithPythonLambdaWhereClause(t *testing.T) {
	result, err := InjectMultipleRowFilters(
		`SELECT list_transform(tags, lambda x: x || suffix) AS tags FROM accounts`,
		"accounts",
		[]string{`list_has(list_transform(tags, lambda x: lower(x)), 'vip')`},
	)
	require.NoError(t, err)
	assert.Contains(t, strings.ToLower(result), "where")
	assert.Contains(t, result, `lambda x:`)
	assert.Contains(t, strings.ToLower(result), `lower`)
}
