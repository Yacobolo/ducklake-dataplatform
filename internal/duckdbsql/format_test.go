package duckdbsql

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestFormat_RoundTrip tests that format(parse(sql)) produces functionally
// equivalent SQL. The output uses quoted identifiers and SQL-standard forms.
func TestFormat_RoundTrip(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want string // expected output; empty means re-parse to check it doesn't error
	}{
		// === Basic SELECT ===
		{
			name: "select_star",
			sql:  "SELECT * FROM t",
			want: `SELECT * FROM "t"`,
		},
		{
			name: "select_columns",
			sql:  `SELECT "a", "b" FROM "t"`,
			want: `SELECT "a", "b" FROM "t"`,
		},
		{
			name: "select_alias",
			sql:  "SELECT x AS y FROM t",
			want: `SELECT "x" AS "y" FROM "t"`,
		},
		{
			name: "select_distinct",
			sql:  "SELECT DISTINCT x FROM t",
			want: `SELECT DISTINCT "x" FROM "t"`,
		},

		// === WHERE ===
		{
			name: "where_eq",
			sql:  `SELECT * FROM t WHERE "id" = 1`,
			want: `SELECT * FROM "t" WHERE "id" = 1`,
		},
		{
			name: "where_and",
			sql:  `SELECT * FROM t WHERE a = 1 AND b = 2`,
			want: `SELECT * FROM "t" WHERE "a" = 1 AND "b" = 2`,
		},
		{
			name: "where_string",
			sql:  `SELECT * FROM t WHERE name = 'hello'`,
			want: `SELECT * FROM "t" WHERE "name" = 'hello'`,
		},
		{
			name: "where_ne",
			sql:  `SELECT * FROM t WHERE x != 1`,
			want: `SELECT * FROM "t" WHERE "x" <> 1`,
		},

		// === ORDER BY / LIMIT / OFFSET ===
		{
			name: "order_by",
			sql:  "SELECT * FROM t ORDER BY x ASC, y DESC",
			want: `SELECT * FROM "t" ORDER BY "x", "y" DESC`,
		},
		{
			name: "limit",
			sql:  "SELECT * FROM t LIMIT 10",
			want: `SELECT * FROM "t" LIMIT 10`,
		},
		{
			name: "limit_offset",
			sql:  "SELECT * FROM t LIMIT 10 OFFSET 5",
			want: `SELECT * FROM "t" LIMIT 10 OFFSET 5`,
		},

		// === GROUP BY / HAVING ===
		{
			name: "group_by",
			sql:  "SELECT x, count(*) FROM t GROUP BY x",
			want: `SELECT "x", count(*) FROM "t" GROUP BY "x"`,
		},
		{
			name: "group_by_all",
			sql:  "SELECT x, count(*) FROM t GROUP BY ALL",
			want: `SELECT "x", count(*) FROM "t" GROUP BY ALL`,
		},
		{
			name: "having",
			sql:  "SELECT x, count(*) FROM t GROUP BY x HAVING count(*) > 1",
			want: `SELECT "x", count(*) FROM "t" GROUP BY "x" HAVING count(*) > 1`,
		},

		// === JOINs ===
		{
			name: "inner_join",
			sql:  `SELECT * FROM a JOIN b ON a.id = b.id`,
			want: `SELECT * FROM "a" JOIN "b" ON "a"."id" = "b"."id"`,
		},
		{
			name: "left_join",
			sql:  `SELECT * FROM a LEFT JOIN b ON a.id = b.id`,
			want: `SELECT * FROM "a" LEFT JOIN "b" ON "a"."id" = "b"."id"`,
		},
		{
			name: "cross_join",
			sql:  "SELECT * FROM a CROSS JOIN b",
			want: `SELECT * FROM "a" CROSS JOIN "b"`,
		},
		{
			name: "comma_join",
			sql:  "SELECT * FROM a, b",
			want: `SELECT * FROM "a", "b"`,
		},
		{
			name: "join_using",
			sql:  "SELECT * FROM a JOIN b USING (id)",
			want: `SELECT * FROM "a" JOIN "b" USING ("id")`,
		},
		{
			name: "table_alias",
			sql:  "SELECT * FROM titanic t JOIN cabins c ON t.id = c.tid",
			want: `SELECT * FROM "titanic" "t" JOIN "cabins" "c" ON "t"."id" = "c"."tid"`,
		},

		// === Subqueries ===
		{
			name: "subquery_from",
			sql:  "SELECT * FROM (SELECT * FROM t) sub",
			want: `SELECT * FROM (SELECT * FROM "t") "sub"`,
		},
		{
			name: "subquery_where",
			sql:  "SELECT * FROM t WHERE id IN (SELECT id FROM s)",
			want: `SELECT * FROM "t" WHERE "id" IN (SELECT "id" FROM "s")`,
		},

		// === UNION ===
		{
			name: "union_all",
			sql:  "SELECT * FROM a UNION ALL SELECT * FROM b",
			want: `SELECT * FROM "a" UNION ALL SELECT * FROM "b"`,
		},
		{
			name: "union",
			sql:  "SELECT * FROM a UNION SELECT * FROM b",
			want: `SELECT * FROM "a" UNION SELECT * FROM "b"`,
		},

		// === CTE ===
		{
			name: "cte",
			sql:  "WITH cte AS (SELECT * FROM t) SELECT * FROM cte",
			want: `WITH "cte" AS (SELECT * FROM "t") SELECT * FROM "cte"`,
		},

		// === Expressions ===
		{
			name: "case_expr",
			sql:  "SELECT CASE WHEN x = 1 THEN 'a' ELSE 'b' END FROM t",
			want: `SELECT CASE WHEN "x" = 1 THEN 'a' ELSE 'b' END FROM "t"`,
		},
		{
			name: "cast_expr",
			sql:  "SELECT CAST(x AS INTEGER) FROM t",
			want: `SELECT CAST("x" AS INTEGER) FROM "t"`,
		},
		{
			name: "type_cast",
			sql:  "SELECT x::INT FROM t",
			want: `SELECT "x"::INT FROM "t"`,
		},
		{
			name: "between",
			sql:  "SELECT * FROM t WHERE x BETWEEN 1 AND 10",
			want: `SELECT * FROM "t" WHERE "x" BETWEEN 1 AND 10`,
		},
		{
			name: "is_null",
			sql:  "SELECT * FROM t WHERE x IS NULL",
			want: `SELECT * FROM "t" WHERE "x" IS NULL`,
		},
		{
			name: "is_not_null",
			sql:  "SELECT * FROM t WHERE x IS NOT NULL",
			want: `SELECT * FROM "t" WHERE "x" IS NOT NULL`,
		},
		{
			name: "like",
			sql:  "SELECT * FROM t WHERE name LIKE '%test%'",
			want: `SELECT * FROM "t" WHERE "name" LIKE '%test%'`,
		},
		{
			name: "ilike",
			sql:  "SELECT * FROM t WHERE name ILIKE '%test%'",
			want: `SELECT * FROM "t" WHERE "name" ILIKE '%test%'`,
		},
		{
			name: "exists",
			sql:  "SELECT * FROM t WHERE EXISTS (SELECT 1 FROM s)",
			want: `SELECT * FROM "t" WHERE EXISTS (SELECT 1 FROM "s")`,
		},
		{
			name: "not_expr",
			sql:  "SELECT NOT true",
			want: `SELECT NOT TRUE`,
		},
		{
			name: "paren_expr",
			sql:  "SELECT (1 + 2) * 3",
			want: `SELECT (1 + 2) * 3`,
		},
		{
			name: "func_count_star",
			sql:  "SELECT count(*) FROM t",
			want: `SELECT count(*) FROM "t"`,
		},
		{
			name: "func_with_args",
			sql:  "SELECT coalesce(a, 0) FROM t",
			want: `SELECT coalesce("a", 0) FROM "t"`,
		},
		{
			name: "concat_op",
			sql:  "SELECT 'a' || 'b'",
			want: `SELECT 'a' || 'b'`,
		},

		// === INSERT ===
		{
			name: "insert_values",
			sql:  "INSERT INTO t (a, b) VALUES (1, 'x')",
			want: `INSERT INTO "t" ("a", "b") VALUES (1, 'x')`,
		},
		{
			name: "insert_select",
			sql:  "INSERT INTO t SELECT * FROM s",
			want: `INSERT INTO "t" SELECT * FROM "s"`,
		},

		// === UPDATE ===
		{
			name: "update",
			sql:  `UPDATE t SET name = 'test' WHERE id = 1`,
			want: `UPDATE "t" SET "name" = 'test' WHERE "id" = 1`,
		},

		// === DELETE ===
		{
			name: "delete",
			sql:  "DELETE FROM t WHERE id = 1",
			want: `DELETE FROM "t" WHERE "id" = 1`,
		},

		// === Schema-qualified ===
		{
			name: "schema_qualified_table",
			sql:  "SELECT * FROM main.t",
			want: `SELECT * FROM "main"."t"`,
		},
		{
			name: "catalog_schema_table",
			sql:  "SELECT * FROM lake.main.t",
			want: `SELECT * FROM "lake"."main"."t"`,
		},

		// === String escaping ===
		{
			name: "string_with_quote",
			sql:  "SELECT 'it''s'",
			want: `SELECT 'it''s'`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			stmt, err := Parse(tc.sql)
			require.NoError(t, err, "parse failed for: %s", tc.sql)

			got := Format(stmt)

			if tc.want != "" {
				assert.Equal(t, tc.want, got)
			}

			// Also verify the formatted output can be re-parsed
			_, err = Parse(got)
			assert.NoError(t, err, "re-parse failed for formatted output: %s", got)
		})
	}
}

// TestFormatExpr tests standalone expression formatting.
func TestFormatExpr_RoundTrip(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want string
	}{
		{"simple_eq", `"Pclass" = 1`, `"Pclass" = 1`},
		{"and_expr", `"a" = 1 AND "b" = 2`, `"a" = 1 AND "b" = 2`},
		{"or_expr", `("a" = 1) OR ("b" = 2)`, `("a" = 1) OR ("b" = 2)`},
		{"string_literal", `"name" = 'test'`, `"name" = 'test'`},
		{"not_equal", `"x" <> 1`, `"x" <> 1`},
		{"comparison", `"age" >= 18`, `"age" >= 18`},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			expr, err := ParseExpr(tc.sql)
			require.NoError(t, err)
			got := FormatExpr(expr)
			assert.Equal(t, tc.want, got)
		})
	}
}

// TestFormat_DDLPassthrough verifies DDL statements are emitted as-is.
func TestFormat_DDLPassthrough(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		{"create_table", "CREATE TABLE foo (id INT, name VARCHAR)"},
		{"drop_table", "DROP TABLE foo"},
		{"create_view", "CREATE VIEW v AS SELECT * FROM t"},
		{"create_schema", "CREATE SCHEMA myschema"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			stmt, err := Parse(tc.sql)
			require.NoError(t, err)
			got := Format(stmt)
			assert.Equal(t, tc.sql, got)
		})
	}
}

// TestFormat_UtilityPassthrough verifies utility statements are emitted as-is.
func TestFormat_UtilityPassthrough(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		{"set", "SET threads = 4"},
		{"describe", "DESCRIBE titanic"},
		{"show", "SHOW TABLES"},
		{"explain", "EXPLAIN SELECT 1"},
		{"begin", "BEGIN TRANSACTION"},
		{"commit", "COMMIT"},
		{"rollback", "ROLLBACK"},
		{"vacuum", "VACUUM"},
		{"checkpoint", "CHECKPOINT"},
		{"grant", "GRANT SELECT ON t TO user1"},
		{"revoke", "REVOKE SELECT ON t FROM user1"},
		{"reset", "RESET ALL"},
		{"import", "IMPORT DATABASE 'path'"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			stmt, err := Parse(tc.sql)
			require.NoError(t, err)
			got := Format(stmt)
			assert.Equal(t, tc.sql, got)
		})
	}
}

// TestFormat_TryCast verifies TRY_CAST round-trip.
func TestFormat_TryCast(t *testing.T) {
	sql := "SELECT TRY_CAST(x AS VARCHAR) FROM t"
	stmt, err := Parse(sql)
	require.NoError(t, err)
	got := Format(stmt)
	assert.Contains(t, got, "TRY_CAST")
	// Verify re-parseable
	_, err = Parse(got)
	assert.NoError(t, err)
}

// TestFormat_Extract verifies EXTRACT round-trip.
func TestFormat_Extract(t *testing.T) {
	sql := "SELECT EXTRACT(MONTH FROM d) FROM t"
	stmt, err := Parse(sql)
	require.NoError(t, err)
	got := Format(stmt)
	assert.Contains(t, got, "EXTRACT(MONTH FROM")
	// Verify re-parseable
	_, err = Parse(got)
	assert.NoError(t, err)
}

// TestFormat_Glob verifies GLOB round-trip.
func TestFormat_Glob(t *testing.T) {
	sql := "SELECT * FROM t WHERE x GLOB '*.txt'"
	stmt, err := Parse(sql)
	require.NoError(t, err)
	got := Format(stmt)
	assert.Contains(t, got, "GLOB")
	// Verify re-parseable
	_, err = Parse(got)
	assert.NoError(t, err)
}

// TestFormat_SimilarTo verifies SIMILAR TO round-trip.
func TestFormat_SimilarTo(t *testing.T) {
	sql := "SELECT * FROM t WHERE x SIMILAR TO '%test%'"
	stmt, err := Parse(sql)
	require.NoError(t, err)
	got := Format(stmt)
	assert.Contains(t, got, "SIMILAR TO")
	// Verify re-parseable
	_, err = Parse(got)
	assert.NoError(t, err)
}

// TestFormat_NullLiteral verifies NULL is formatted correctly.
func TestFormat_NullLiteral(t *testing.T) {
	stmt, err := Parse("SELECT NULL")
	require.NoError(t, err)
	got := Format(stmt)
	assert.Equal(t, "SELECT NULL", got)
}

// TestFormat_BoolLiterals verifies TRUE/FALSE formatting.
func TestFormat_BoolLiterals(t *testing.T) {
	stmt, err := Parse("SELECT TRUE, FALSE")
	require.NoError(t, err)
	got := Format(stmt)
	assert.Equal(t, "SELECT TRUE, FALSE", got)
}

// TestFormat_WindowFunction verifies window function formatting.
func TestFormat_WindowFunction(t *testing.T) {
	sql := `SELECT row_number() OVER (PARTITION BY x ORDER BY y) FROM t`
	stmt, err := Parse(sql)
	require.NoError(t, err)
	got := Format(stmt)
	assert.Contains(t, got, "OVER")
	assert.Contains(t, got, "PARTITION BY")
	assert.Contains(t, got, "ORDER BY")
}

// TestFormat_InsertMultipleRows verifies multi-row INSERT formatting.
func TestFormat_InsertMultipleRows(t *testing.T) {
	sql := "INSERT INTO t (a, b) VALUES (1, 'x'), (2, 'y'), (3, 'z')"
	stmt, err := Parse(sql)
	require.NoError(t, err)
	got := Format(stmt)
	assert.Contains(t, got, "VALUES")
	// Verify re-parseable
	_, err = Parse(got)
	assert.NoError(t, err)
}

// TestFormat_NestedSubquery verifies deeply nested subquery formatting.
func TestFormat_NestedSubquery(t *testing.T) {
	sql := "SELECT * FROM (SELECT * FROM (SELECT * FROM t) a) b"
	stmt, err := Parse(sql)
	require.NoError(t, err)
	got := Format(stmt)
	_, err = Parse(got)
	assert.NoError(t, err, "re-parse of nested subquery failed: %s", got)
}
