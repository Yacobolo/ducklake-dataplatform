package notebook

import (
	"testing"

	"duck-demo/internal/domain"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCompileNotebookCellSQL_TreeShakeAndOrder(t *testing.T) {
	base := "base"
	dead := "dead_end"
	output := "out"

	cells := []domain.Cell{
		{ID: "c1", CellType: domain.CellTypeSQL, Role: domain.CellRoleTransform, Name: &base, Content: "SELECT 1 AS id"},
		{ID: "c2", CellType: domain.CellTypeSQL, Role: domain.CellRoleTransform, Name: &dead, Content: "SELECT 999 AS x"},
		{ID: "c3", CellType: domain.CellTypeSQL, Role: domain.CellRoleOutput, Name: &output, Content: "SELECT * FROM base"},
	}

	compiled, err := CompileNotebookCellSQL(cells, "c3", true)
	require.NoError(t, err)
	assert.Contains(t, compiled, `"base" AS (`)
	assert.NotContains(t, compiled, `"dead_end" AS (`)
	assert.Contains(t, compiled, "SELECT * FROM base")
}

func TestCompileNotebookCellSQL_CycleDetected(t *testing.T) {
	a := "a"
	b := "b"
	cells := []domain.Cell{
		{ID: "c1", CellType: domain.CellTypeSQL, Role: domain.CellRoleTransform, Name: &a, Content: "SELECT * FROM b"},
		{ID: "c2", CellType: domain.CellTypeSQL, Role: domain.CellRoleOutput, Name: &b, Content: "SELECT * FROM a"},
	}

	_, err := CompileNotebookCellSQL(cells, "c2", true)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "dependency cycle")
}

func TestCompileNotebookCellSQL_SelfReferenceFails(t *testing.T) {
	output := "out"
	cells := []domain.Cell{
		{ID: "c2", CellType: domain.CellTypeSQL, Role: domain.CellRoleOutput, Name: &output, Content: "SELECT * FROM out"},
	}

	_, err := CompileNotebookCellSQL(cells, "c2", true)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cannot reference itself")
}

func TestCompileNotebookCellSQLWithContext_RefSourceVarParity(t *testing.T) {
	stg := "stg_orders"
	out := "orders_out"
	cells := []domain.Cell{
		{ID: "c1", CellType: domain.CellTypeSQL, Role: domain.CellRoleTransform, Name: &stg, Content: "SELECT 1 AS id"},
		{ID: "c2", CellType: domain.CellTypeSQL, Role: domain.CellRoleOutput, Name: &out, Content: "SELECT * FROM {{ ref('stg_orders') }} WHERE ds = '{{ var('ds', '2026-01-01') }}' AND src = (SELECT MAX(ds) FROM {{ source('raw', 'events') }})"},
	}

	compiled, err := CompileNotebookCellSQLWithContext(cells, "c2", true, CompileContext{
		Vars:          map[string]string{"ds": "2026-03-01"},
		Sources:       map[string]string{"raw.events": `"lake"."raw_events"`},
		StrictSources: true,
	})
	require.NoError(t, err)
	assert.Contains(t, compiled, `"stg_orders" AS (`)
	assert.Contains(t, compiled, `FROM "stg_orders"`)
	assert.Contains(t, compiled, `'2026-03-01'`)
	assert.Contains(t, compiled, `FROM "lake"."raw_events"`)
}

func TestCompileNotebookCellSQLWithContext_SourceStrictUnknownFails(t *testing.T) {
	out := "orders_out"
	cells := []domain.Cell{{
		ID:       "c1",
		CellType: domain.CellTypeSQL,
		Role:     domain.CellRoleOutput,
		Name:     &out,
		Content:  "SELECT * FROM {{ source('raw', 'events') }}",
	}}

	_, err := CompileNotebookCellSQLWithContext(cells, "c1", true, CompileContext{StrictSources: true, Sources: map[string]string{}})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown source")
}

func TestCompileNotebookCellSQLWithContext_VarMissingFails(t *testing.T) {
	out := "orders_out"
	cells := []domain.Cell{{
		ID:       "c1",
		CellType: domain.CellTypeSQL,
		Role:     domain.CellRoleOutput,
		Name:     &out,
		Content:  "SELECT '{{ var('required_ds') }}' AS ds",
	}}

	_, err := CompileNotebookCellSQLWithContext(cells, "c1", true, CompileContext{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), `required var "required_ds" not provided`)
}
