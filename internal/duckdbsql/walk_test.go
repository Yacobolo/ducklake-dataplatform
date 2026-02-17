package duckdbsql

import (
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// === Classify tests ===

func TestClassify(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want StmtType
	}{
		{"select", "SELECT * FROM t", StmtTypeSelect},
		{"insert", "INSERT INTO t (a) VALUES (1)", StmtTypeInsert},
		{"update", "UPDATE t SET a = 1", StmtTypeUpdate},
		{"delete", "DELETE FROM t WHERE id = 1", StmtTypeDelete},
		{"create_table", "CREATE TABLE foo (id INT)", StmtTypeDDL},
		{"drop_table", "DROP TABLE foo", StmtTypeDDL},
		{"alter", "ALTER TABLE foo ADD COLUMN bar INT", StmtTypeDDL},
		{"set", "SET threads = 4", StmtTypeUtilitySet},
		{"describe", "DESCRIBE t", StmtTypeOther},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			stmt, err := Parse(tc.sql)
			require.NoError(t, err)
			got := Classify(stmt)
			assert.Equal(t, tc.want, got)
		})
	}
}

// === CollectTableNames tests ===

func TestCollectTableNames(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want []string
	}{
		{
			name: "simple_select",
			sql:  "SELECT * FROM titanic",
			want: []string{"titanic"},
		},
		{
			name: "multiple_from",
			sql:  "SELECT * FROM titanic, passengers",
			want: []string{"passengers", "titanic"},
		},
		{
			name: "join",
			sql:  "SELECT * FROM titanic t JOIN cabins c ON t.id = c.tid",
			want: []string{"cabins", "titanic"},
		},
		{
			name: "subquery_from",
			sql:  "SELECT * FROM (SELECT * FROM titanic) sub",
			want: []string{"titanic"},
		},
		{
			name: "subquery_where",
			sql:  "SELECT * FROM titanic WHERE id IN (SELECT tid FROM bookings)",
			want: []string{"bookings", "titanic"},
		},
		{
			name: "union",
			sql:  "SELECT * FROM titanic UNION ALL SELECT * FROM passengers",
			want: []string{"passengers", "titanic"},
		},
		{
			name: "cte",
			sql:  "WITH cte AS (SELECT * FROM titanic) SELECT * FROM cte",
			want: []string{"cte", "titanic"},
		},
		{
			name: "deduplication",
			sql:  "SELECT * FROM titanic t1 JOIN titanic t2 ON t1.id = t2.id",
			want: []string{"titanic"},
		},
		{
			name: "insert",
			sql:  "INSERT INTO orders (id) VALUES (1)",
			want: []string{"orders"},
		},
		{
			name: "insert_select",
			sql:  "INSERT INTO orders SELECT * FROM temp_orders",
			want: []string{"orders", "temp_orders"},
		},
		{
			name: "update",
			sql:  "UPDATE users SET name = 'test'",
			want: []string{"users"},
		},
		{
			name: "update_from",
			sql:  "UPDATE users SET name = s.name FROM source s WHERE users.id = s.id",
			want: []string{"source", "users"},
		},
		{
			name: "delete",
			sql:  "DELETE FROM logs WHERE ts < '2024-01-01'",
			want: []string{"logs"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			stmt, err := Parse(tc.sql)
			require.NoError(t, err)
			got := CollectTableNames(stmt)
			sort.Strings(got)
			sort.Strings(tc.want)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestCollectTableRefs(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want []TableRefName
	}{
		{
			name: "three_part_name",
			sql:  "SELECT * FROM demo.titanic.passengers",
			want: []TableRefName{{Catalog: "demo", Schema: "titanic", Name: "passengers"}},
		},
		{
			name: "mixed_qualified_names",
			sql:  "SELECT * FROM analytics.orders o JOIN demo.titanic.passengers p ON o.id = p.id",
			want: []TableRefName{
				{Catalog: "", Schema: "analytics", Name: "orders"},
				{Catalog: "demo", Schema: "titanic", Name: "passengers"},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			stmt, err := Parse(tc.sql)
			require.NoError(t, err)

			got := CollectTableRefs(stmt)
			sort.Slice(got, func(i, j int) bool {
				left := got[i].Catalog + "." + got[i].Schema + "." + got[i].Name
				right := got[j].Catalog + "." + got[j].Schema + "." + got[j].Name
				return left < right
			})
			sort.Slice(tc.want, func(i, j int) bool {
				left := tc.want[i].Catalog + "." + tc.want[i].Schema + "." + tc.want[i].Name
				right := tc.want[j].Catalog + "." + tc.want[j].Schema + "." + tc.want[j].Name
				return left < right
			})

			assert.Equal(t, tc.want, got)
		})
	}
}

// === TargetTable tests ===

func TestTargetTable(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want string
	}{
		{"insert", "INSERT INTO orders (id) VALUES (1)", "orders"},
		{"update", "UPDATE users SET name = 'test'", "users"},
		{"delete", "DELETE FROM logs WHERE id = 1", "logs"},
		{"select", "SELECT * FROM t", ""},
		{"create_table", "CREATE TABLE foo (id INT)", ""},
		{"insert_schema", "INSERT INTO main.orders (id) VALUES (1)", "orders"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			stmt, err := Parse(tc.sql)
			require.NoError(t, err)
			got := TargetTable(stmt)
			assert.Equal(t, tc.want, got)
		})
	}
}

// === InjectFilter tests ===

func TestInjectFilter_Select(t *testing.T) {
	t.Run("basic", func(t *testing.T) {
		stmt, err := Parse("SELECT * FROM titanic")
		require.NoError(t, err)
		filter := &BinaryExpr{
			Left:  &ColumnRef{Column: "Pclass"},
			Op:    TOKEN_EQ,
			Right: &Literal{Type: LiteralNumber, Value: "1"},
		}
		InjectFilter(stmt, "titanic", filter)

		got := Format(stmt)
		assert.Contains(t, strings.ToLower(got), "where")
		assert.Contains(t, got, "Pclass")
	})

	t.Run("preserves_existing_where", func(t *testing.T) {
		stmt, err := Parse(`SELECT * FROM titanic WHERE "Sex" = 'male'`)
		require.NoError(t, err)
		filter := &BinaryExpr{
			Left:  &ColumnRef{Column: "Pclass"},
			Op:    TOKEN_EQ,
			Right: &Literal{Type: LiteralNumber, Value: "1"},
		}
		InjectFilter(stmt, "titanic", filter)

		got := Format(stmt)
		assert.Contains(t, got, "Sex")
		assert.Contains(t, got, "Pclass")
		assert.Contains(t, strings.ToUpper(got), "AND")
	})

	t.Run("no_matching_table", func(t *testing.T) {
		stmt, err := Parse("SELECT * FROM other_table")
		require.NoError(t, err)
		filter := &BinaryExpr{
			Left:  &ColumnRef{Column: "Pclass"},
			Op:    TOKEN_EQ,
			Right: &Literal{Type: LiteralNumber, Value: "1"},
		}
		InjectFilter(stmt, "titanic", filter)

		got := Format(stmt)
		assert.NotContains(t, strings.ToLower(got), "where")
	})

	t.Run("subquery_in_from", func(t *testing.T) {
		stmt, err := Parse("SELECT * FROM (SELECT * FROM titanic) sub")
		require.NoError(t, err)
		filter := &BinaryExpr{
			Left:  &ColumnRef{Column: "Pclass"},
			Op:    TOKEN_EQ,
			Right: &Literal{Type: LiteralNumber, Value: "1"},
		}
		InjectFilter(stmt, "titanic", filter)

		got := Format(stmt)
		assert.Contains(t, got, "Pclass")
	})

	t.Run("nested_subquery", func(t *testing.T) {
		stmt, err := Parse("SELECT * FROM (SELECT * FROM (SELECT * FROM titanic) a) b")
		require.NoError(t, err)
		filter := &BinaryExpr{
			Left:  &ColumnRef{Column: "Pclass"},
			Op:    TOKEN_EQ,
			Right: &Literal{Type: LiteralNumber, Value: "1"},
		}
		InjectFilter(stmt, "titanic", filter)

		got := Format(stmt)
		assert.Contains(t, got, "Pclass")
	})

	t.Run("union_both_branches", func(t *testing.T) {
		stmt, err := Parse("SELECT * FROM titanic UNION ALL SELECT * FROM titanic")
		require.NoError(t, err)
		filter := &BinaryExpr{
			Left:  &ColumnRef{Column: "Pclass"},
			Op:    TOKEN_EQ,
			Right: &Literal{Type: LiteralNumber, Value: "1"},
		}
		InjectFilter(stmt, "titanic", filter)

		got := Format(stmt)
		count := strings.Count(got, "Pclass")
		assert.GreaterOrEqual(t, count, 2, "filter should be injected in both UNION branches")
	})

	t.Run("self_join", func(t *testing.T) {
		stmt, err := Parse(`SELECT * FROM titanic t1 JOIN titanic t2 ON t1."id" = t2."id"`)
		require.NoError(t, err)
		filter := &BinaryExpr{
			Left:  &ColumnRef{Column: "Pclass"},
			Op:    TOKEN_EQ,
			Right: &Literal{Type: LiteralNumber, Value: "1"},
		}
		InjectFilter(stmt, "titanic", filter)

		got := Format(stmt)
		assert.Contains(t, got, "Pclass")
	})

	t.Run("cte", func(t *testing.T) {
		stmt, err := Parse("WITH cte AS (SELECT * FROM titanic) SELECT * FROM cte")
		require.NoError(t, err)
		filter := &BinaryExpr{
			Left:  &ColumnRef{Column: "Pclass"},
			Op:    TOKEN_EQ,
			Right: &Literal{Type: LiteralNumber, Value: "1"},
		}
		InjectFilter(stmt, "titanic", filter)

		got := Format(stmt)
		assert.Contains(t, got, "Pclass")
	})
}

func TestInjectFilter_Update(t *testing.T) {
	t.Run("basic", func(t *testing.T) {
		stmt, err := Parse(`UPDATE titanic SET "Name" = 'test'`)
		require.NoError(t, err)
		filter := &BinaryExpr{
			Left:  &ColumnRef{Column: "Pclass"},
			Op:    TOKEN_EQ,
			Right: &Literal{Type: LiteralNumber, Value: "1"},
		}
		InjectFilter(stmt, "titanic", filter)

		got := Format(stmt)
		assert.Contains(t, got, "Pclass")
		assert.Contains(t, strings.ToLower(got), "where")
	})

	t.Run("wrong_table", func(t *testing.T) {
		stmt, err := Parse(`UPDATE other SET "Name" = 'test'`)
		require.NoError(t, err)
		filter := &BinaryExpr{
			Left:  &ColumnRef{Column: "Pclass"},
			Op:    TOKEN_EQ,
			Right: &Literal{Type: LiteralNumber, Value: "1"},
		}
		InjectFilter(stmt, "titanic", filter)

		got := Format(stmt)
		assert.NotContains(t, got, "Pclass")
	})
}

func TestInjectFilter_Delete(t *testing.T) {
	stmt, err := Parse("DELETE FROM titanic")
	require.NoError(t, err)
	filter := &BinaryExpr{
		Left:  &ColumnRef{Column: "Pclass"},
		Op:    TOKEN_EQ,
		Right: &Literal{Type: LiteralNumber, Value: "1"},
	}
	InjectFilter(stmt, "titanic", filter)

	got := Format(stmt)
	assert.Contains(t, got, "Pclass")
	assert.Contains(t, strings.ToLower(got), "where")
}

// === ApplyColumnMasks tests ===

func TestApplyColumnMasks_Basic(t *testing.T) {
	stmt, err := Parse(`SELECT "PassengerId", "Name", "Pclass" FROM titanic`)
	require.NoError(t, err)

	err = ApplyColumnMasks(stmt, "titanic", map[string]string{"Name": "'***'"}, nil)
	require.NoError(t, err)

	got := Format(stmt)
	assert.Contains(t, got, "'***'")
	assert.Contains(t, got, "PassengerId")
}

func TestApplyColumnMasks_SelectStar(t *testing.T) {
	stmt, err := Parse("SELECT * FROM titanic")
	require.NoError(t, err)

	allCols := []string{"PassengerId", "Name", "Pclass"}
	err = ApplyColumnMasks(stmt, "titanic", map[string]string{"Name": "'***'"}, allCols)
	require.NoError(t, err)

	got := Format(stmt)
	assert.Contains(t, got, "'***'")
	assert.Contains(t, got, "PassengerId")
	assert.Contains(t, got, "Pclass")
}

func TestApplyColumnMasks_SelectStarNoColumns(t *testing.T) {
	stmt, err := Parse("SELECT * FROM titanic")
	require.NoError(t, err)

	err = ApplyColumnMasks(stmt, "titanic", map[string]string{"Name": "'***'"}, nil)
	require.Error(t, err, "should error when masking SELECT * without column metadata")
}

func TestApplyColumnMasks_NoMasks(t *testing.T) {
	stmt, err := Parse("SELECT * FROM titanic")
	require.NoError(t, err)

	err = ApplyColumnMasks(stmt, "titanic", nil, nil)
	require.NoError(t, err)
}

func TestApplyColumnMasks_NoMatchingTable(t *testing.T) {
	stmt, err := Parse(`SELECT "Name" FROM other_table`)
	require.NoError(t, err)

	err = ApplyColumnMasks(stmt, "titanic", map[string]string{"Name": "'***'"}, nil)
	require.NoError(t, err)

	got := Format(stmt)
	assert.NotContains(t, got, "'***'")
}

func TestApplyColumnMasks_UnparseableMask(t *testing.T) {
	stmt, err := Parse(`SELECT "Name" FROM titanic`)
	require.NoError(t, err)

	err = ApplyColumnMasks(stmt, "titanic", map[string]string{"Name": "INVALID MASK ((("}, nil)
	require.Error(t, err, "should error for unparseable mask expression")
}

func TestApplyColumnMasks_Subquery(t *testing.T) {
	stmt, err := Parse(`SELECT "Name" FROM (SELECT "Name" FROM titanic) sub`)
	require.NoError(t, err)

	err = ApplyColumnMasks(stmt, "titanic", map[string]string{"Name": "'***'"}, nil)
	require.NoError(t, err)

	got := Format(stmt)
	assert.Contains(t, got, "'***'")
}

func TestApplyColumnMasks_CTE(t *testing.T) {
	stmt, err := Parse(`WITH cte AS (SELECT "Name" FROM titanic) SELECT "Name" FROM cte`)
	require.NoError(t, err)

	err = ApplyColumnMasks(stmt, "titanic", map[string]string{"Name": "'***'"}, nil)
	require.NoError(t, err)

	got := Format(stmt)
	assert.Contains(t, got, "'***'")
}

func TestApplyColumnMasks_CaseInsensitive(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		masks    map[string]string
		allCols  []string
		wantMask bool
	}{
		{
			name:     "lowercase_mask_uppercase_query",
			sql:      `SELECT "Email" FROM titanic`,
			masks:    map[string]string{"email": "'***'"},
			wantMask: true,
		},
		{
			name:     "lowercase_mask_mixed_query",
			sql:      `SELECT "eMaIl" FROM titanic`,
			masks:    map[string]string{"email": "'***'"},
			wantMask: true,
		},
		{
			name:     "select_star_case_insensitive",
			sql:      "SELECT * FROM titanic",
			masks:    map[string]string{"name": "'***'"},
			allCols:  []string{"id", "Name", "email"},
			wantMask: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			stmt, err := Parse(tc.sql)
			require.NoError(t, err)

			err = ApplyColumnMasks(stmt, "titanic", tc.masks, tc.allCols)
			require.NoError(t, err)

			got := Format(stmt)
			hasMask := strings.Contains(got, "'***'")
			if tc.wantMask {
				assert.True(t, hasMask, "expected mask in result: %s", got)
			} else {
				assert.False(t, hasMask, "did not expect mask in result: %s", got)
			}
		})
	}
}

// === Integration: full pipeline simulation ===

func TestIntegration_SecurityPipeline(t *testing.T) {
	// Simulate the engine's security pipeline:
	// 1. Parse → Classify
	// 2. CollectTableNames → RBAC check
	// 3. InjectFilter (RLS)
	// 4. ApplyColumnMasks
	// 5. Format → execute

	sql := `SELECT "PassengerId", "Name", "Pclass" FROM titanic WHERE "Sex" = 'male' LIMIT 10`

	// 1. Parse
	stmt, err := Parse(sql)
	require.NoError(t, err)

	// 2. Classify
	assert.Equal(t, StmtTypeSelect, Classify(stmt))

	// 3. Collect tables
	tables := CollectTableNames(stmt)
	assert.Contains(t, tables, "titanic")

	// 4. Inject RLS filter
	filter, err := ParseExpr(`"Pclass" = 1`)
	require.NoError(t, err)
	InjectFilter(stmt, "titanic", filter)

	// 5. Apply column masks
	err = ApplyColumnMasks(stmt, "titanic", map[string]string{"Name": "'***'"}, nil)
	require.NoError(t, err)

	// 6. Format
	got := Format(stmt)
	t.Logf("result: %s", got)

	// Verify all pieces are present
	assert.Contains(t, got, "PassengerId")
	assert.Contains(t, got, "'***'")    // mask applied
	assert.Contains(t, got, "Pclass")   // RLS filter injected
	assert.Contains(t, got, "Sex")      // original WHERE preserved
	assert.Contains(t, got, "LIMIT 10") // LIMIT preserved
	assert.Contains(t, got, "AND")      // filter ANDed with existing WHERE

	// Verify it's valid SQL (re-parseable)
	_, err = Parse(got)
	assert.NoError(t, err, "output should be valid SQL")
}
