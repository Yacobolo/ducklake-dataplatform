package duckdbsql

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// helper to parse SQL and analyze column lineage.
func analyzeSQL(t *testing.T, sql string, schema SchemaInfo) []ColumnLineage {
	t.Helper()
	stmt, err := Parse(sql)
	require.NoError(t, err, "parse SQL: %s", sql)
	result, err := AnalyzeColumnLineage(stmt, schema)
	require.NoError(t, err, "analyze lineage: %s", sql)
	return result
}

// assertSources checks that the column lineage entry has the expected sources.
func assertSources(t *testing.T, col ColumnLineage, expected []ColumnOrigin) {
	t.Helper()
	assert.ElementsMatch(t, expected, col.Sources, "sources for column %q", col.Name)
}

func TestAnalyzeColumnLineage_BasicReferences(t *testing.T) {
	schema := SchemaInfo{"t": {"a", "b", "c"}}

	tests := []struct {
		name     string
		sql      string
		schema   SchemaInfo
		expected []ColumnLineage
	}{
		{
			name:   "unqualified column",
			sql:    "SELECT a FROM t",
			schema: schema,
			expected: []ColumnLineage{
				{Name: "a", TransformType: "DIRECT", Sources: []ColumnOrigin{{Table: "t", Column: "a"}}},
			},
		},
		{
			name:   "qualified column",
			sql:    "SELECT t.a FROM t",
			schema: schema,
			expected: []ColumnLineage{
				{Name: "a", TransformType: "DIRECT", Sources: []ColumnOrigin{{Table: "t", Column: "a"}}},
			},
		},
		{
			name:   "aliased column",
			sql:    "SELECT a AS alpha FROM t",
			schema: schema,
			expected: []ColumnLineage{
				{Name: "alpha", TransformType: "DIRECT", Sources: []ColumnOrigin{{Table: "t", Column: "a"}}},
			},
		},
		{
			name:   "multiple columns",
			sql:    "SELECT a, b, c FROM t",
			schema: schema,
			expected: []ColumnLineage{
				{Name: "a", TransformType: "DIRECT", Sources: []ColumnOrigin{{Table: "t", Column: "a"}}},
				{Name: "b", TransformType: "DIRECT", Sources: []ColumnOrigin{{Table: "t", Column: "b"}}},
				{Name: "c", TransformType: "DIRECT", Sources: []ColumnOrigin{{Table: "t", Column: "c"}}},
			},
		},
		{
			name:   "schema-qualified table",
			sql:    "SELECT a FROM myschema.t",
			schema: SchemaInfo{"myschema.t": {"a", "b"}},
			expected: []ColumnLineage{
				{Name: "a", TransformType: "DIRECT", Sources: []ColumnOrigin{{Schema: "myschema", Table: "t", Column: "a"}}},
			},
		},
		{
			name:   "table aliased",
			sql:    "SELECT x.a FROM t AS x",
			schema: schema,
			expected: []ColumnLineage{
				{Name: "a", TransformType: "DIRECT", Sources: []ColumnOrigin{{Table: "t", Column: "a"}}},
			},
		},
		{
			name:   "literal only",
			sql:    "SELECT 42 AS answer",
			schema: nil,
			expected: []ColumnLineage{
				{Name: "answer", TransformType: "DIRECT", Sources: nil},
			},
		},
		{
			name:   "string literal",
			sql:    "SELECT 'hello' AS greeting",
			schema: nil,
			expected: []ColumnLineage{
				{Name: "greeting", TransformType: "DIRECT", Sources: nil},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := analyzeSQL(t, tt.sql, tt.schema)
			require.Len(t, result, len(tt.expected))
			for i, exp := range tt.expected {
				assert.Equal(t, exp.Name, result[i].Name, "column name at index %d", i)
				assert.Equal(t, exp.TransformType, result[i].TransformType, "transform type for %s", exp.Name)
				assertSources(t, result[i], exp.Sources)
			}
		})
	}
}

func TestAnalyzeColumnLineage_StarExpansion(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		schema   SchemaInfo
		expected []ColumnLineage
	}{
		{
			name:   "SELECT star",
			sql:    "SELECT * FROM t",
			schema: SchemaInfo{"t": {"a", "b", "c"}},
			expected: []ColumnLineage{
				{Name: "a", TransformType: "DIRECT", Sources: []ColumnOrigin{{Table: "t", Column: "a"}}},
				{Name: "b", TransformType: "DIRECT", Sources: []ColumnOrigin{{Table: "t", Column: "b"}}},
				{Name: "c", TransformType: "DIRECT", Sources: []ColumnOrigin{{Table: "t", Column: "c"}}},
			},
		},
		{
			name:   "SELECT star two tables",
			sql:    "SELECT * FROM t JOIN s ON t.id = s.id",
			schema: SchemaInfo{"t": {"id", "a"}, "s": {"id", "b"}},
			expected: []ColumnLineage{
				{Name: "id", TransformType: "DIRECT", Sources: []ColumnOrigin{{Table: "t", Column: "id"}}},
				{Name: "a", TransformType: "DIRECT", Sources: []ColumnOrigin{{Table: "t", Column: "a"}}},
				{Name: "id", TransformType: "DIRECT", Sources: []ColumnOrigin{{Table: "s", Column: "id"}}},
				{Name: "b", TransformType: "DIRECT", Sources: []ColumnOrigin{{Table: "s", Column: "b"}}},
			},
		},
		{
			name:   "SELECT table star",
			sql:    "SELECT t.* FROM t JOIN s ON t.id = s.id",
			schema: SchemaInfo{"t": {"id", "a"}, "s": {"id", "b"}},
			expected: []ColumnLineage{
				{Name: "id", TransformType: "DIRECT", Sources: []ColumnOrigin{{Table: "t", Column: "id"}}},
				{Name: "a", TransformType: "DIRECT", Sources: []ColumnOrigin{{Table: "t", Column: "a"}}},
			},
		},
		{
			name:   "star EXCLUDE",
			sql:    "SELECT * EXCLUDE (b) FROM t",
			schema: SchemaInfo{"t": {"a", "b", "c"}},
			expected: []ColumnLineage{
				{Name: "a", TransformType: "DIRECT", Sources: []ColumnOrigin{{Table: "t", Column: "a"}}},
				{Name: "c", TransformType: "DIRECT", Sources: []ColumnOrigin{{Table: "t", Column: "c"}}},
			},
		},
		{
			name:   "star REPLACE",
			sql:    "SELECT * REPLACE (UPPER(a) AS a) FROM t",
			schema: SchemaInfo{"t": {"a", "b"}},
			expected: []ColumnLineage{
				{Name: "a", TransformType: "EXPRESSION", Function: "UPPER", Sources: []ColumnOrigin{{Table: "t", Column: "a"}}},
				{Name: "b", TransformType: "DIRECT", Sources: []ColumnOrigin{{Table: "t", Column: "b"}}},
			},
		},
		{
			name:   "star RENAME",
			sql:    "SELECT * RENAME (a AS alpha) FROM t",
			schema: SchemaInfo{"t": {"a", "b"}},
			expected: []ColumnLineage{
				{Name: "alpha", TransformType: "DIRECT", Sources: []ColumnOrigin{{Table: "t", Column: "a"}}},
				{Name: "b", TransformType: "DIRECT", Sources: []ColumnOrigin{{Table: "t", Column: "b"}}},
			},
		},
		{
			name:     "star no schema info",
			sql:      "SELECT * FROM t",
			schema:   SchemaInfo{},
			expected: nil,
		},
		{
			name:   "star EXCLUDE and REPLACE combined",
			sql:    "SELECT * EXCLUDE (c) REPLACE (a + 1 AS a) FROM t",
			schema: SchemaInfo{"t": {"a", "b", "c"}},
			expected: []ColumnLineage{
				{Name: "a", TransformType: "EXPRESSION", Sources: []ColumnOrigin{{Table: "t", Column: "a"}}},
				{Name: "b", TransformType: "DIRECT", Sources: []ColumnOrigin{{Table: "t", Column: "b"}}},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := analyzeSQL(t, tt.sql, tt.schema)
			if tt.expected == nil {
				assert.Empty(t, result)
				return
			}
			require.Len(t, result, len(tt.expected))
			for i, exp := range tt.expected {
				assert.Equal(t, exp.Name, result[i].Name, "column name at index %d", i)
				assert.Equal(t, exp.TransformType, result[i].TransformType, "transform type for %s at index %d", exp.Name, i)
				if exp.Function != "" {
					assert.Equal(t, exp.Function, result[i].Function, "function for %s", exp.Name)
				}
				assertSources(t, result[i], exp.Sources)
			}
		})
	}
}

func TestAnalyzeColumnLineage_Expressions(t *testing.T) {
	schema := SchemaInfo{"t": {"a", "b", "c", "status", "id", "x"}}

	tests := []struct {
		name         string
		sql          string
		expectedName string
		expectedType string
		expectedFunc string
		expectedSrcs []ColumnOrigin
	}{
		{
			name:         "binary arithmetic",
			sql:          "SELECT a + b AS total FROM t",
			expectedName: "total",
			expectedType: "EXPRESSION",
			expectedSrcs: []ColumnOrigin{{Table: "t", Column: "a"}, {Table: "t", Column: "b"}},
		},
		{
			name:         "string concat",
			sql:          "SELECT a || ' ' || b AS full_name FROM t",
			expectedName: "full_name",
			expectedType: "EXPRESSION",
			expectedSrcs: []ColumnOrigin{{Table: "t", Column: "a"}, {Table: "t", Column: "b"}},
		},
		{
			name:         "CASE searched",
			sql:          "SELECT CASE WHEN status = 1 THEN 'active' ELSE 'inactive' END AS label FROM t",
			expectedName: "label",
			expectedType: "EXPRESSION",
			expectedFunc: "CASE",
			expectedSrcs: []ColumnOrigin{{Table: "t", Column: "status"}},
		},
		{
			name:         "CASE simple",
			sql:          "SELECT CASE x WHEN 1 THEN 'a' WHEN 2 THEN 'b' END FROM t",
			expectedName: "",
			expectedType: "EXPRESSION",
			expectedFunc: "CASE",
			expectedSrcs: []ColumnOrigin{{Table: "t", Column: "x"}},
		},
		{
			name:         "CAST",
			sql:          "SELECT CAST(id AS TEXT) AS id_str FROM t",
			expectedName: "id_str",
			expectedType: "EXPRESSION",
			expectedFunc: "CAST",
			expectedSrcs: []ColumnOrigin{{Table: "t", Column: "id"}},
		},
		{
			name:         "DuckDB type cast",
			sql:          "SELECT id::TEXT FROM t",
			expectedName: "",
			expectedType: "EXPRESSION",
			expectedFunc: "CAST",
			expectedSrcs: []ColumnOrigin{{Table: "t", Column: "id"}},
		},
		{
			name:         "COALESCE",
			sql:          "SELECT COALESCE(a, b, 0) FROM t",
			expectedName: "coalesce",
			expectedType: "EXPRESSION",
			expectedFunc: "COALESCE",
			expectedSrcs: []ColumnOrigin{{Table: "t", Column: "a"}, {Table: "t", Column: "b"}},
		},
		{
			name:         "nested expression",
			sql:          "SELECT (a + b) * c AS result FROM t",
			expectedName: "result",
			expectedType: "EXPRESSION",
			expectedSrcs: []ColumnOrigin{{Table: "t", Column: "a"}, {Table: "t", Column: "b"}, {Table: "t", Column: "c"}},
		},
		{
			name:         "IS NULL",
			sql:          "SELECT a IS NOT NULL AS flag FROM t",
			expectedName: "flag",
			expectedType: "EXPRESSION",
			expectedSrcs: []ColumnOrigin{{Table: "t", Column: "a"}},
		},
		{
			name:         "BETWEEN",
			sql:          "SELECT a BETWEEN b AND c AS inrange FROM t",
			expectedName: "inrange",
			expectedType: "EXPRESSION",
			expectedSrcs: []ColumnOrigin{{Table: "t", Column: "a"}, {Table: "t", Column: "b"}, {Table: "t", Column: "c"}},
		},
		{
			name:         "IN list",
			sql:          "SELECT a IN (1, 2, 3) AS matches FROM t",
			expectedName: "matches",
			expectedType: "EXPRESSION",
			expectedSrcs: []ColumnOrigin{{Table: "t", Column: "a"}},
		},
		{
			name:         "LIKE",
			sql:          "SELECT a LIKE '%test%' AS matches FROM t",
			expectedName: "matches",
			expectedType: "EXPRESSION",
			expectedSrcs: []ColumnOrigin{{Table: "t", Column: "a"}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := analyzeSQL(t, tt.sql, schema)
			require.Len(t, result, 1)
			assert.Equal(t, tt.expectedName, result[0].Name)
			assert.Equal(t, tt.expectedType, result[0].TransformType)
			if tt.expectedFunc != "" {
				assert.Equal(t, tt.expectedFunc, result[0].Function)
			}
			assertSources(t, result[0], tt.expectedSrcs)
		})
	}
}

func TestAnalyzeColumnLineage_Functions(t *testing.T) {
	schema := SchemaInfo{"t": {"name", "amount", "id", "cat", "price", "a", "b", "c", "status"}}

	tests := []struct {
		name         string
		sql          string
		expectedName string
		expectedFunc string
		expectedSrcs []ColumnOrigin
	}{
		{
			name:         "scalar function",
			sql:          "SELECT UPPER(name) AS uname FROM t",
			expectedName: "uname",
			expectedFunc: "UPPER",
			expectedSrcs: []ColumnOrigin{{Table: "t", Column: "name"}},
		},
		{
			name:         "aggregate SUM",
			sql:          "SELECT SUM(amount) AS total FROM t",
			expectedName: "total",
			expectedFunc: "SUM",
			expectedSrcs: []ColumnOrigin{{Table: "t", Column: "amount"}},
		},
		{
			name:         "aggregate COUNT star",
			sql:          "SELECT COUNT(*) AS cnt FROM t",
			expectedName: "cnt",
			expectedFunc: "COUNT",
			expectedSrcs: nil,
		},
		{
			name:         "aggregate COUNT column",
			sql:          "SELECT COUNT(id) AS cnt FROM t",
			expectedName: "cnt",
			expectedFunc: "COUNT",
			expectedSrcs: []ColumnOrigin{{Table: "t", Column: "id"}},
		},
		{
			name:         "aggregate COUNT DISTINCT",
			sql:          "SELECT COUNT(DISTINCT cat) FROM t",
			expectedName: "count",
			expectedFunc: "COUNT",
			expectedSrcs: []ColumnOrigin{{Table: "t", Column: "cat"}},
		},
		{
			name:         "nested function",
			sql:          "SELECT ROUND(AVG(price), 2) FROM t",
			expectedName: "round",
			expectedFunc: "ROUND",
			expectedSrcs: []ColumnOrigin{{Table: "t", Column: "price"}},
		},
		{
			name:         "multi-arg function",
			sql:          "SELECT GREATEST(a, b, c) FROM t",
			expectedName: "greatest",
			expectedFunc: "GREATEST",
			expectedSrcs: []ColumnOrigin{{Table: "t", Column: "a"}, {Table: "t", Column: "b"}, {Table: "t", Column: "c"}},
		},
		{
			name:         "function with FILTER",
			sql:          "SELECT SUM(amount) FILTER (WHERE status = 'active') FROM t",
			expectedName: "sum",
			expectedFunc: "SUM",
			expectedSrcs: []ColumnOrigin{{Table: "t", Column: "amount"}, {Table: "t", Column: "status"}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := analyzeSQL(t, tt.sql, schema)
			require.Len(t, result, 1)
			assert.Equal(t, tt.expectedName, result[0].Name)
			assert.Equal(t, "EXPRESSION", result[0].TransformType)
			assert.Equal(t, tt.expectedFunc, result[0].Function)
			assertSources(t, result[0], tt.expectedSrcs)
		})
	}
}

func TestAnalyzeColumnLineage_WindowFunctions(t *testing.T) {
	schema := SchemaInfo{"t": {"dept", "salary", "amount", "customer_id", "price", "date", "a", "b"}}

	tests := []struct {
		name         string
		sql          string
		expectedName string
		expectedFunc string
		expectedSrcs []ColumnOrigin
	}{
		{
			name:         "ROW_NUMBER",
			sql:          "SELECT ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) AS rn FROM t",
			expectedName: "rn",
			expectedFunc: "ROW_NUMBER",
			expectedSrcs: []ColumnOrigin{{Table: "t", Column: "dept"}, {Table: "t", Column: "salary"}},
		},
		{
			name:         "SUM window",
			sql:          "SELECT SUM(amount) OVER (PARTITION BY customer_id) FROM t",
			expectedName: "sum",
			expectedFunc: "SUM",
			expectedSrcs: []ColumnOrigin{{Table: "t", Column: "amount"}, {Table: "t", Column: "customer_id"}},
		},
		{
			name:         "LAG",
			sql:          "SELECT LAG(price, 1) OVER (ORDER BY date) FROM t",
			expectedName: "lag",
			expectedFunc: "LAG",
			expectedSrcs: []ColumnOrigin{{Table: "t", Column: "price"}, {Table: "t", Column: "date"}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := analyzeSQL(t, tt.sql, schema)
			require.Len(t, result, 1)
			assert.Equal(t, tt.expectedName, result[0].Name)
			assert.Equal(t, "EXPRESSION", result[0].TransformType)
			assert.Equal(t, tt.expectedFunc, result[0].Function)
			assertSources(t, result[0], tt.expectedSrcs)
		})
	}
}

func TestAnalyzeColumnLineage_CTEs(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		schema   SchemaInfo
		expected []ColumnLineage
	}{
		{
			name:   "basic CTE",
			sql:    "WITH cte AS (SELECT a, b FROM t) SELECT a FROM cte",
			schema: SchemaInfo{"t": {"a", "b"}},
			expected: []ColumnLineage{
				{Name: "a", TransformType: "DIRECT", Sources: []ColumnOrigin{{Table: "t", Column: "a"}}},
			},
		},
		{
			name:   "CTE with alias",
			sql:    "WITH cte AS (SELECT a AS x FROM t) SELECT x FROM cte",
			schema: SchemaInfo{"t": {"a"}},
			expected: []ColumnLineage{
				{Name: "x", TransformType: "DIRECT", Sources: []ColumnOrigin{{Table: "t", Column: "a"}}},
			},
		},
		{
			name:   "chained CTEs",
			sql:    "WITH c1 AS (SELECT a FROM t), c2 AS (SELECT a AS y FROM c1) SELECT y FROM c2",
			schema: SchemaInfo{"t": {"a"}},
			expected: []ColumnLineage{
				{Name: "y", TransformType: "DIRECT", Sources: []ColumnOrigin{{Table: "t", Column: "a"}}},
			},
		},
		{
			name:   "CTE with expression",
			sql:    "WITH cte AS (SELECT a + b AS total FROM t) SELECT total FROM cte",
			schema: SchemaInfo{"t": {"a", "b"}},
			expected: []ColumnLineage{
				{Name: "total", TransformType: "EXPRESSION", Sources: []ColumnOrigin{{Table: "t", Column: "a"}, {Table: "t", Column: "b"}}},
			},
		},
		{
			name:   "CTE shadows table",
			sql:    "WITH t AS (SELECT x FROM s) SELECT x FROM t",
			schema: SchemaInfo{"s": {"x"}},
			expected: []ColumnLineage{
				{Name: "x", TransformType: "DIRECT", Sources: []ColumnOrigin{{Table: "s", Column: "x"}}},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := analyzeSQL(t, tt.sql, tt.schema)
			require.Len(t, result, len(tt.expected))
			for i, exp := range tt.expected {
				assert.Equal(t, exp.Name, result[i].Name)
				assert.Equal(t, exp.TransformType, result[i].TransformType)
				assertSources(t, result[i], exp.Sources)
			}
		})
	}
}

func TestAnalyzeColumnLineage_DerivedTables(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		schema   SchemaInfo
		expected []ColumnLineage
	}{
		{
			name:   "basic subquery",
			sql:    "SELECT sub.x FROM (SELECT a AS x FROM t) sub",
			schema: SchemaInfo{"t": {"a"}},
			expected: []ColumnLineage{
				{Name: "x", TransformType: "DIRECT", Sources: []ColumnOrigin{{Table: "t", Column: "a"}}},
			},
		},
		{
			name:   "subquery with expression",
			sql:    "SELECT sub.total FROM (SELECT a + b AS total FROM t) sub",
			schema: SchemaInfo{"t": {"a", "b"}},
			expected: []ColumnLineage{
				{Name: "total", TransformType: "EXPRESSION", Sources: []ColumnOrigin{{Table: "t", Column: "a"}, {Table: "t", Column: "b"}}},
			},
		},
		{
			name:   "column aliases on derived table",
			sql:    "SELECT x FROM (SELECT a, b FROM t) sub(x, y)",
			schema: SchemaInfo{"t": {"a", "b"}},
			expected: []ColumnLineage{
				{Name: "x", TransformType: "DIRECT", Sources: []ColumnOrigin{{Table: "t", Column: "a"}}},
			},
		},
		{
			name:   "nested derived tables",
			sql:    "SELECT d2.z FROM (SELECT d1.y AS z FROM (SELECT a AS y FROM t) d1) d2",
			schema: SchemaInfo{"t": {"a"}},
			expected: []ColumnLineage{
				{Name: "z", TransformType: "DIRECT", Sources: []ColumnOrigin{{Table: "t", Column: "a"}}},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := analyzeSQL(t, tt.sql, tt.schema)
			require.Len(t, result, len(tt.expected))
			for i, exp := range tt.expected {
				assert.Equal(t, exp.Name, result[i].Name)
				assert.Equal(t, exp.TransformType, result[i].TransformType)
				assertSources(t, result[i], exp.Sources)
			}
		})
	}
}

func TestAnalyzeColumnLineage_Joins(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		schema   SchemaInfo
		expected []ColumnLineage
	}{
		{
			name:   "JOIN qualified refs",
			sql:    "SELECT t.a, s.b FROM t JOIN s ON t.id = s.id",
			schema: SchemaInfo{"t": {"id", "a"}, "s": {"id", "b"}},
			expected: []ColumnLineage{
				{Name: "a", TransformType: "DIRECT", Sources: []ColumnOrigin{{Table: "t", Column: "a"}}},
				{Name: "b", TransformType: "DIRECT", Sources: []ColumnOrigin{{Table: "s", Column: "b"}}},
			},
		},
		{
			name:   "JOIN unqualified resolved",
			sql:    "SELECT name FROM t JOIN s ON t.id = s.id",
			schema: SchemaInfo{"t": {"id", "name"}, "s": {"id", "val"}},
			expected: []ColumnLineage{
				{Name: "name", TransformType: "DIRECT", Sources: []ColumnOrigin{{Table: "t", Column: "name"}}},
			},
		},
		{
			name:   "LEFT JOIN",
			sql:    "SELECT t.a, s.b FROM t LEFT JOIN s ON t.id = s.id",
			schema: SchemaInfo{"t": {"id", "a"}, "s": {"id", "b"}},
			expected: []ColumnLineage{
				{Name: "a", TransformType: "DIRECT", Sources: []ColumnOrigin{{Table: "t", Column: "a"}}},
				{Name: "b", TransformType: "DIRECT", Sources: []ColumnOrigin{{Table: "s", Column: "b"}}},
			},
		},
		{
			name:   "multi-way JOIN",
			sql:    "SELECT a.x, b.y, c.z FROM a JOIN b ON a.id = b.id JOIN c ON b.id = c.id",
			schema: SchemaInfo{"a": {"id", "x"}, "b": {"id", "y"}, "c": {"id", "z"}},
			expected: []ColumnLineage{
				{Name: "x", TransformType: "DIRECT", Sources: []ColumnOrigin{{Table: "a", Column: "x"}}},
				{Name: "y", TransformType: "DIRECT", Sources: []ColumnOrigin{{Table: "b", Column: "y"}}},
				{Name: "z", TransformType: "DIRECT", Sources: []ColumnOrigin{{Table: "c", Column: "z"}}},
			},
		},
		{
			name:   "self-join with aliases",
			sql:    "SELECT e1.x, e2.x AS bx FROM t e1 JOIN t e2 ON e1.id = e2.id",
			schema: SchemaInfo{"t": {"id", "x"}},
			expected: []ColumnLineage{
				{Name: "x", TransformType: "DIRECT", Sources: []ColumnOrigin{{Table: "t", Column: "x"}}},
				{Name: "bx", TransformType: "DIRECT", Sources: []ColumnOrigin{{Table: "t", Column: "x"}}},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := analyzeSQL(t, tt.sql, tt.schema)
			require.Len(t, result, len(tt.expected))
			for i, exp := range tt.expected {
				assert.Equal(t, exp.Name, result[i].Name)
				assert.Equal(t, exp.TransformType, result[i].TransformType)
				assertSources(t, result[i], exp.Sources)
			}
		})
	}
}

func TestAnalyzeColumnLineage_SetOperations(t *testing.T) {
	schema := SchemaInfo{"t": {"a"}, "s": {"x"}}

	tests := []struct {
		name         string
		sql          string
		expectedName string
		expectedFunc string
		expectedSrcs []ColumnOrigin
	}{
		{
			name:         "UNION ALL",
			sql:          "SELECT a FROM t UNION ALL SELECT x FROM s",
			expectedName: "a",
			expectedFunc: "UNION",
			expectedSrcs: []ColumnOrigin{{Table: "t", Column: "a"}, {Table: "s", Column: "x"}},
		},
		{
			name:         "UNION",
			sql:          "SELECT a FROM t UNION SELECT x FROM s",
			expectedName: "a",
			expectedFunc: "UNION",
			expectedSrcs: []ColumnOrigin{{Table: "t", Column: "a"}, {Table: "s", Column: "x"}},
		},
		{
			name:         "INTERSECT",
			sql:          "SELECT a FROM t INTERSECT SELECT x FROM s",
			expectedName: "a",
			expectedFunc: "INTERSECT",
			expectedSrcs: []ColumnOrigin{{Table: "t", Column: "a"}, {Table: "s", Column: "x"}},
		},
		{
			name:         "EXCEPT",
			sql:          "SELECT a FROM t EXCEPT SELECT x FROM s",
			expectedName: "a",
			expectedFunc: "EXCEPT",
			expectedSrcs: []ColumnOrigin{{Table: "t", Column: "a"}, {Table: "s", Column: "x"}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := analyzeSQL(t, tt.sql, schema)
			require.Len(t, result, 1)
			assert.Equal(t, tt.expectedName, result[0].Name)
			assert.Equal(t, "EXPRESSION", result[0].TransformType)
			assert.Equal(t, tt.expectedFunc, result[0].Function)
			assertSources(t, result[0], tt.expectedSrcs)
		})
	}
}

func TestAnalyzeColumnLineage_Subqueries(t *testing.T) {
	tests := []struct {
		name         string
		sql          string
		schema       SchemaInfo
		expectedName string
		expectedType string
		expectedFunc string
		expectedSrcs []ColumnOrigin
	}{
		{
			name:         "scalar subquery",
			sql:          "SELECT (SELECT MAX(a) FROM t) AS max_a",
			schema:       SchemaInfo{"t": {"a"}},
			expectedName: "max_a",
			expectedType: "EXPRESSION",
			expectedFunc: "SUBQUERY",
			expectedSrcs: []ColumnOrigin{{Table: "t", Column: "a"}},
		},
		{
			name:         "IN subquery",
			sql:          "SELECT a IN (SELECT x FROM s) AS flag FROM t",
			schema:       SchemaInfo{"t": {"a"}, "s": {"x"}},
			expectedName: "flag",
			expectedType: "EXPRESSION",
			expectedSrcs: []ColumnOrigin{{Table: "t", Column: "a"}, {Table: "s", Column: "x"}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := analyzeSQL(t, tt.sql, tt.schema)
			require.Len(t, result, 1)
			assert.Equal(t, tt.expectedName, result[0].Name)
			assert.Equal(t, tt.expectedType, result[0].TransformType)
			if tt.expectedFunc != "" {
				assert.Equal(t, tt.expectedFunc, result[0].Function)
			}
			assertSources(t, result[0], tt.expectedSrcs)
		})
	}
}

func TestAnalyzeColumnLineage_DuckDBSpecific(t *testing.T) {
	tests := []struct {
		name         string
		sql          string
		schema       SchemaInfo
		expectedLen  int
		expectedName string
		expectedType string
		expectedFunc string
		expectedSrcs []ColumnOrigin
	}{
		{
			name:         "QUALIFY does not affect output columns",
			sql:          "SELECT a FROM t QUALIFY ROW_NUMBER() OVER (PARTITION BY a) = 1",
			schema:       SchemaInfo{"t": {"a"}},
			expectedLen:  1,
			expectedName: "a",
			expectedType: "DIRECT",
			expectedSrcs: []ColumnOrigin{{Table: "t", Column: "a"}},
		},
		{
			name:         "double colon type cast",
			sql:          "SELECT id::TEXT AS id_str FROM t",
			schema:       SchemaInfo{"t": {"id"}},
			expectedLen:  1,
			expectedName: "id_str",
			expectedType: "EXPRESSION",
			expectedFunc: "CAST",
			expectedSrcs: []ColumnOrigin{{Table: "t", Column: "id"}},
		},
		{
			name:         "INTERVAL literal",
			sql:          "SELECT INTERVAL '1 day' AS one_day",
			schema:       nil,
			expectedLen:  1,
			expectedName: "one_day",
			expectedType: "DIRECT",
			expectedSrcs: nil,
		},
		{
			name:         "EXTRACT",
			sql:          "SELECT EXTRACT(YEAR FROM created_at) AS yr FROM t",
			schema:       SchemaInfo{"t": {"created_at"}},
			expectedLen:  1,
			expectedName: "yr",
			expectedType: "EXPRESSION",
			expectedFunc: "EXTRACT",
			expectedSrcs: []ColumnOrigin{{Table: "t", Column: "created_at"}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := analyzeSQL(t, tt.sql, tt.schema)
			require.Len(t, result, tt.expectedLen)
			if tt.expectedLen > 0 {
				assert.Equal(t, tt.expectedName, result[0].Name)
				assert.Equal(t, tt.expectedType, result[0].TransformType)
				if tt.expectedFunc != "" {
					assert.Equal(t, tt.expectedFunc, result[0].Function)
				}
				assertSources(t, result[0], tt.expectedSrcs)
			}
		})
	}
}

func TestAnalyzeColumnLineage_EdgeCases(t *testing.T) {
	t.Run("SELECT with no FROM", func(t *testing.T) {
		result := analyzeSQL(t, "SELECT 1, 'hello', NULL", nil)
		require.Len(t, result, 3)
		for _, col := range result {
			assert.Equal(t, "DIRECT", col.TransformType)
			assert.Empty(t, col.Sources)
		}
	})

	t.Run("duplicate sources deduplicated", func(t *testing.T) {
		result := analyzeSQL(t, "SELECT a + a AS doubled FROM t", SchemaInfo{"t": {"a"}})
		require.Len(t, result, 1)
		assert.Equal(t, "EXPRESSION", result[0].TransformType)
		// Should be deduplicated to single source
		require.Len(t, result[0].Sources, 1)
		assert.Equal(t, "a", result[0].Sources[0].Column)
	})

	t.Run("unknown table reference", func(t *testing.T) {
		result := analyzeSQL(t, "SELECT x.a FROM x", SchemaInfo{})
		require.Len(t, result, 1)
		// Graceful: empty sources because x is not in schema
		assert.Empty(t, result[0].Sources)
	})

	t.Run("deeply nested", func(t *testing.T) {
		sql := `
			WITH
				c1 AS (SELECT a FROM t),
				c2 AS (SELECT a AS b FROM c1),
				c3 AS (SELECT b AS c FROM c2),
				c4 AS (SELECT c AS d FROM c3),
				c5 AS (SELECT d AS e FROM c4)
			SELECT e FROM c5
		`
		result := analyzeSQL(t, sql, SchemaInfo{"t": {"a"}})
		require.Len(t, result, 1)
		assert.Equal(t, "e", result[0].Name)
		assert.Equal(t, "DIRECT", result[0].TransformType)
		assertSources(t, result[0], []ColumnOrigin{{Table: "t", Column: "a"}})
	})

	t.Run("non-SELECT returns nil", func(t *testing.T) {
		stmt, err := Parse("INSERT INTO t VALUES (1)")
		require.NoError(t, err)
		result, err := AnalyzeColumnLineage(stmt, nil)
		require.NoError(t, err)
		assert.Nil(t, result)
	})

	t.Run("nil statement", func(t *testing.T) {
		result, err := AnalyzeColumnLineage(nil, nil)
		require.NoError(t, err)
		assert.Nil(t, result)
	})
}

func TestAnalyzeColumnLineage_ColumnNameInference(t *testing.T) {
	schema := SchemaInfo{"t": {"a", "id"}}

	tests := []struct {
		name         string
		sql          string
		expectedName string
	}{
		{
			name:         "alias",
			sql:          "SELECT a AS alpha FROM t",
			expectedName: "alpha",
		},
		{
			name:         "bare column",
			sql:          "SELECT a FROM t",
			expectedName: "a",
		},
		{
			name:         "qualified column",
			sql:          "SELECT t.a FROM t",
			expectedName: "a",
		},
		{
			name:         "function",
			sql:          "SELECT COUNT(*) FROM t",
			expectedName: "count",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := analyzeSQL(t, tt.sql, schema)
			require.Len(t, result, 1)
			assert.Equal(t, tt.expectedName, result[0].Name)
		})
	}
}

func TestAnalyzeColumnLineage_ComplexQuery(t *testing.T) {
	t.Run("real-world analytics query", func(t *testing.T) {
		sql := `
			WITH monthly AS (
				SELECT
					customer_id,
					EXTRACT(MONTH FROM order_date) AS month,
					SUM(amount) AS total
				FROM orders
				GROUP BY 1, 2
			)
			SELECT
				c.name,
				m.month,
				m.total
			FROM monthly m
			JOIN customers c ON m.customer_id = c.id
		`
		schema := SchemaInfo{
			"orders":    {"customer_id", "order_date", "amount"},
			"customers": {"id", "name"},
		}
		result := analyzeSQL(t, sql, schema)
		require.Len(t, result, 3)

		// c.name -> customers.name
		assert.Equal(t, "name", result[0].Name)
		assert.Equal(t, "DIRECT", result[0].TransformType)
		assertSources(t, result[0], []ColumnOrigin{{Table: "customers", Column: "name"}})

		// m.month -> EXPRESSION(EXTRACT) -> orders.order_date
		assert.Equal(t, "month", result[1].Name)
		assert.Equal(t, "EXPRESSION", result[1].TransformType)
		assertSources(t, result[1], []ColumnOrigin{{Table: "orders", Column: "order_date"}})

		// m.total -> EXPRESSION(SUM) -> orders.amount
		assert.Equal(t, "total", result[2].Name)
		assert.Equal(t, "EXPRESSION", result[2].TransformType)
		assertSources(t, result[2], []ColumnOrigin{{Table: "orders", Column: "amount"}})
	})
}
