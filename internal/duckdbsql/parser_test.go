package duckdbsql

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// === Parse entry point tests ===

func TestParse_EmptySQL(t *testing.T) {
	_, err := Parse("")
	require.Error(t, err)
}

func TestParse_MultiStatement(t *testing.T) {
	_, err := Parse("SELECT 1; SELECT 2")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "multi-statement")
}

func TestParse_InvalidSQL(t *testing.T) {
	_, err := Parse("SELEKT * FORM titanic")
	require.Error(t, err)
}

// === ParseExpr tests ===

func TestParseExpr_Simple(t *testing.T) {
	expr, err := ParseExpr(`"Pclass" = 1`)
	require.NoError(t, err)
	require.IsType(t, &BinaryExpr{}, expr)

	bin := expr.(*BinaryExpr)
	assert.Equal(t, TOKEN_EQ, bin.Op)
	assert.IsType(t, &ColumnRef{}, bin.Left)
	assert.IsType(t, &Literal{}, bin.Right)
}

func TestParseExpr_TrailingGarbage(t *testing.T) {
	_, err := ParseExpr("1 + 2 GARBAGE")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unexpected token")
}

func TestParseExpr_Empty(t *testing.T) {
	_, err := ParseExpr("")
	require.Error(t, err)
}

func TestParseExpr_UnparseableMask(t *testing.T) {
	_, err := ParseExpr("INVALID MASK (((")
	require.Error(t, err)
}

func TestParseExpr_MalformedDollar(t *testing.T) {
	_, err := ParseExpr("INVALID SQL $$")
	require.Error(t, err)
}

// === Expression parsing ===

func TestParse_BinaryOperators(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		op   TokenType
	}{
		{"eq", "SELECT 1 = 2", TOKEN_EQ},
		{"ne", "SELECT 1 != 2", TOKEN_NE},
		{"lt", "SELECT 1 < 2", TOKEN_LT},
		{"gt", "SELECT 1 > 2", TOKEN_GT},
		{"le", "SELECT 1 <= 2", TOKEN_LE},
		{"ge", "SELECT 1 >= 2", TOKEN_GE},
		{"add", "SELECT 1 + 2", TOKEN_PLUS},
		{"sub", "SELECT 1 - 2", TOKEN_MINUS},
		{"mul", "SELECT 1 * 2", TOKEN_STAR},
		{"div", "SELECT 1 / 2", TOKEN_SLASH},
		{"mod", "SELECT 1 % 2", TOKEN_MOD},
		{"concat", "SELECT 'a' || 'b'", TOKEN_DPIPE},
		{"and", "SELECT 1 AND 2", TOKEN_AND},
		{"or", "SELECT 1 OR 2", TOKEN_OR},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			stmt, err := Parse(tc.sql)
			require.NoError(t, err)
			sel := stmt.(*SelectStmt)
			item := sel.Body.Left.Columns[0]
			bin, ok := item.Expr.(*BinaryExpr)
			require.True(t, ok, "expected BinaryExpr")
			assert.Equal(t, tc.op, bin.Op)
		})
	}
}

func TestParse_UnaryExpr(t *testing.T) {
	t.Run("not", func(t *testing.T) {
		stmt, err := Parse("SELECT NOT true")
		require.NoError(t, err)
		sel := stmt.(*SelectStmt)
		unary, ok := sel.Body.Left.Columns[0].Expr.(*UnaryExpr)
		require.True(t, ok)
		assert.Equal(t, TOKEN_NOT, unary.Op)
	})

	t.Run("negative", func(t *testing.T) {
		stmt, err := Parse("SELECT -42")
		require.NoError(t, err)
		sel := stmt.(*SelectStmt)
		unary, ok := sel.Body.Left.Columns[0].Expr.(*UnaryExpr)
		require.True(t, ok)
		assert.Equal(t, TOKEN_MINUS, unary.Op)
	})
}

func TestParse_Literals(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		litType LiteralType
		litVal  string
	}{
		{"integer", "SELECT 42", LiteralNumber, "42"},
		{"float", "SELECT 3.14", LiteralNumber, "3.14"},
		{"string", "SELECT 'hello'", LiteralString, "hello"},
		{"true", "SELECT TRUE", LiteralBool, "true"},
		{"false", "SELECT FALSE", LiteralBool, "false"},
		{"null", "SELECT NULL", LiteralNull, "NULL"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			stmt, err := Parse(tc.sql)
			require.NoError(t, err)
			sel := stmt.(*SelectStmt)
			lit, ok := sel.Body.Left.Columns[0].Expr.(*Literal)
			require.True(t, ok)
			assert.Equal(t, tc.litType, lit.Type)
			assert.Equal(t, tc.litVal, lit.Value)
		})
	}
}

func TestParse_ColumnRef(t *testing.T) {
	t.Run("simple", func(t *testing.T) {
		stmt, err := Parse("SELECT name FROM t")
		require.NoError(t, err)
		sel := stmt.(*SelectStmt)
		col, ok := sel.Body.Left.Columns[0].Expr.(*ColumnRef)
		require.True(t, ok)
		assert.Equal(t, "name", col.Column)
		assert.Empty(t, col.Table)
	})

	t.Run("qualified", func(t *testing.T) {
		stmt, err := Parse("SELECT t.name FROM t")
		require.NoError(t, err)
		sel := stmt.(*SelectStmt)
		col, ok := sel.Body.Left.Columns[0].Expr.(*ColumnRef)
		require.True(t, ok)
		assert.Equal(t, "t", col.Table)
		assert.Equal(t, "name", col.Column)
	})

	t.Run("quoted", func(t *testing.T) {
		stmt, err := Parse(`SELECT "PassengerId" FROM t`)
		require.NoError(t, err)
		sel := stmt.(*SelectStmt)
		col, ok := sel.Body.Left.Columns[0].Expr.(*ColumnRef)
		require.True(t, ok)
		assert.Equal(t, "PassengerId", col.Column)
	})
}

func TestParse_Star(t *testing.T) {
	stmt, err := Parse("SELECT * FROM t")
	require.NoError(t, err)
	sel := stmt.(*SelectStmt)
	assert.True(t, sel.Body.Left.Columns[0].Star)
}

func TestParse_Alias(t *testing.T) {
	stmt, err := Parse("SELECT name AS n FROM t")
	require.NoError(t, err)
	sel := stmt.(*SelectStmt)
	assert.Equal(t, "n", sel.Body.Left.Columns[0].Alias)
}

func TestParse_FuncCall(t *testing.T) {
	t.Run("simple", func(t *testing.T) {
		stmt, err := Parse("SELECT count(*) FROM t")
		require.NoError(t, err)
		sel := stmt.(*SelectStmt)
		fn, ok := sel.Body.Left.Columns[0].Expr.(*FuncCall)
		require.True(t, ok)
		assert.Equal(t, "count", fn.Name)
		assert.True(t, fn.Star)
	})

	t.Run("with_args", func(t *testing.T) {
		stmt, err := Parse("SELECT coalesce(a, b, 0) FROM t")
		require.NoError(t, err)
		sel := stmt.(*SelectStmt)
		fn, ok := sel.Body.Left.Columns[0].Expr.(*FuncCall)
		require.True(t, ok)
		assert.Equal(t, "coalesce", fn.Name)
		assert.Len(t, fn.Args, 3)
	})

	t.Run("distinct", func(t *testing.T) {
		stmt, err := Parse("SELECT count(DISTINCT x) FROM t")
		require.NoError(t, err)
		sel := stmt.(*SelectStmt)
		fn, ok := sel.Body.Left.Columns[0].Expr.(*FuncCall)
		require.True(t, ok)
		assert.True(t, fn.Distinct)
	})
}

func TestParse_CaseExpr(t *testing.T) {
	stmt, err := Parse("SELECT CASE WHEN x = 1 THEN 'a' WHEN x = 2 THEN 'b' ELSE 'c' END FROM t")
	require.NoError(t, err)
	sel := stmt.(*SelectStmt)
	ce, ok := sel.Body.Left.Columns[0].Expr.(*CaseExpr)
	require.True(t, ok)
	assert.Len(t, ce.Whens, 2)
	assert.NotNil(t, ce.Else)
}

func TestParse_CastExpr(t *testing.T) {
	stmt, err := Parse("SELECT CAST(x AS INTEGER) FROM t")
	require.NoError(t, err)
	sel := stmt.(*SelectStmt)
	cast, ok := sel.Body.Left.Columns[0].Expr.(*CastExpr)
	require.True(t, ok)
	assert.Equal(t, "INTEGER", cast.TypeName)
}

func TestParse_InExpr(t *testing.T) {
	t.Run("values", func(t *testing.T) {
		stmt, err := Parse("SELECT * FROM t WHERE x IN (1, 2, 3)")
		require.NoError(t, err)
		sel := stmt.(*SelectStmt)
		in, ok := sel.Body.Left.Where.(*InExpr)
		require.True(t, ok)
		assert.Len(t, in.Values, 3)
		assert.False(t, in.Not)
	})

	t.Run("not_in", func(t *testing.T) {
		stmt, err := Parse("SELECT * FROM t WHERE x NOT IN (1)")
		require.NoError(t, err)
		sel := stmt.(*SelectStmt)
		in, ok := sel.Body.Left.Where.(*InExpr)
		require.True(t, ok)
		assert.True(t, in.Not)
	})

	t.Run("subquery", func(t *testing.T) {
		stmt, err := Parse("SELECT * FROM t WHERE x IN (SELECT id FROM s)")
		require.NoError(t, err)
		sel := stmt.(*SelectStmt)
		in, ok := sel.Body.Left.Where.(*InExpr)
		require.True(t, ok)
		assert.NotNil(t, in.Query)
	})
}

func TestParse_BetweenExpr(t *testing.T) {
	stmt, err := Parse("SELECT * FROM t WHERE x BETWEEN 1 AND 10")
	require.NoError(t, err)
	sel := stmt.(*SelectStmt)
	bet, ok := sel.Body.Left.Where.(*BetweenExpr)
	require.True(t, ok)
	assert.False(t, bet.Not)
}

func TestParse_IsNullExpr(t *testing.T) {
	t.Run("is_null", func(t *testing.T) {
		stmt, err := Parse("SELECT * FROM t WHERE x IS NULL")
		require.NoError(t, err)
		sel := stmt.(*SelectStmt)
		isn, ok := sel.Body.Left.Where.(*IsNullExpr)
		require.True(t, ok)
		assert.False(t, isn.Not)
	})

	t.Run("is_not_null", func(t *testing.T) {
		stmt, err := Parse("SELECT * FROM t WHERE x IS NOT NULL")
		require.NoError(t, err)
		sel := stmt.(*SelectStmt)
		isn, ok := sel.Body.Left.Where.(*IsNullExpr)
		require.True(t, ok)
		assert.True(t, isn.Not)
	})
}

func TestParse_LikeExpr(t *testing.T) {
	t.Run("like", func(t *testing.T) {
		stmt, err := Parse("SELECT * FROM t WHERE name LIKE '%test%'")
		require.NoError(t, err)
		sel := stmt.(*SelectStmt)
		like, ok := sel.Body.Left.Where.(*LikeExpr)
		require.True(t, ok)
		assert.False(t, like.ILike)
	})

	t.Run("ilike", func(t *testing.T) {
		stmt, err := Parse("SELECT * FROM t WHERE name ILIKE '%test%'")
		require.NoError(t, err)
		sel := stmt.(*SelectStmt)
		like, ok := sel.Body.Left.Where.(*LikeExpr)
		require.True(t, ok)
		assert.True(t, like.ILike)
	})
}

func TestParse_ExistsExpr(t *testing.T) {
	stmt, err := Parse("SELECT * FROM t WHERE EXISTS (SELECT 1 FROM s)")
	require.NoError(t, err)
	sel := stmt.(*SelectStmt)
	ex, ok := sel.Body.Left.Where.(*ExistsExpr)
	require.True(t, ok)
	assert.NotNil(t, ex.Select)
	assert.False(t, ex.Not)
}

// === DuckDB Expression Extensions ===

func TestParse_TypeCast(t *testing.T) {
	stmt, err := Parse("SELECT x::INT FROM t")
	require.NoError(t, err)
	sel := stmt.(*SelectStmt)
	tc, ok := sel.Body.Left.Columns[0].Expr.(*TypeCastExpr)
	require.True(t, ok)
	assert.Equal(t, "INT", tc.TypeName)
}

func TestParse_IntervalExpr(t *testing.T) {
	stmt, err := Parse("SELECT INTERVAL '1' DAY")
	require.NoError(t, err)
	sel := stmt.(*SelectStmt)
	iv, ok := sel.Body.Left.Columns[0].Expr.(*IntervalExpr)
	require.True(t, ok)
	assert.Equal(t, "DAY", iv.Unit)
}

// === SELECT statement tests ===

func TestParse_SimpleSelect(t *testing.T) {
	stmt, err := Parse("SELECT * FROM titanic")
	require.NoError(t, err)
	sel, ok := stmt.(*SelectStmt)
	require.True(t, ok)
	assert.NotNil(t, sel.Body)
	assert.NotNil(t, sel.Body.Left)
	assert.NotNil(t, sel.Body.Left.From)
}

func TestParse_SelectDistinct(t *testing.T) {
	stmt, err := Parse("SELECT DISTINCT x FROM t")
	require.NoError(t, err)
	sel := stmt.(*SelectStmt)
	assert.True(t, sel.Body.Left.Distinct)
}

func TestParse_SelectWhere(t *testing.T) {
	stmt, err := Parse("SELECT * FROM t WHERE x = 1")
	require.NoError(t, err)
	sel := stmt.(*SelectStmt)
	assert.NotNil(t, sel.Body.Left.Where)
}

func TestParse_SelectGroupBy(t *testing.T) {
	stmt, err := Parse("SELECT x, count(*) FROM t GROUP BY x")
	require.NoError(t, err)
	sel := stmt.(*SelectStmt)
	assert.Len(t, sel.Body.Left.GroupBy, 1)
}

func TestParse_SelectGroupByAll(t *testing.T) {
	stmt, err := Parse("SELECT x, count(*) FROM t GROUP BY ALL")
	require.NoError(t, err)
	sel := stmt.(*SelectStmt)
	assert.True(t, sel.Body.Left.GroupByAll)
}

func TestParse_SelectHaving(t *testing.T) {
	stmt, err := Parse("SELECT x, count(*) FROM t GROUP BY x HAVING count(*) > 1")
	require.NoError(t, err)
	sel := stmt.(*SelectStmt)
	assert.NotNil(t, sel.Body.Left.Having)
}

func TestParse_SelectOrderBy(t *testing.T) {
	stmt, err := Parse("SELECT * FROM t ORDER BY x ASC, y DESC")
	require.NoError(t, err)
	sel := stmt.(*SelectStmt)
	assert.Len(t, sel.Body.Left.OrderBy, 2)
	assert.False(t, sel.Body.Left.OrderBy[0].Desc)
	assert.True(t, sel.Body.Left.OrderBy[1].Desc)
}

func TestParse_SelectOrderByAll(t *testing.T) {
	stmt, err := Parse("SELECT * FROM t ORDER BY ALL DESC")
	require.NoError(t, err)
	sel := stmt.(*SelectStmt)
	assert.True(t, sel.Body.Left.OrderByAll)
	assert.True(t, sel.Body.Left.OrderByAllDesc)
}

func TestParse_SelectLimit(t *testing.T) {
	stmt, err := Parse("SELECT * FROM t LIMIT 10")
	require.NoError(t, err)
	sel := stmt.(*SelectStmt)
	assert.NotNil(t, sel.Body.Left.Limit)
}

func TestParse_SelectOffset(t *testing.T) {
	stmt, err := Parse("SELECT * FROM t LIMIT 10 OFFSET 5")
	require.NoError(t, err)
	sel := stmt.(*SelectStmt)
	assert.NotNil(t, sel.Body.Left.Limit)
	assert.NotNil(t, sel.Body.Left.Offset)
}

func TestParse_SelectQualify(t *testing.T) {
	stmt, err := Parse("SELECT *, row_number() OVER (PARTITION BY x ORDER BY y) AS rn FROM t QUALIFY rn = 1")
	require.NoError(t, err)
	sel := stmt.(*SelectStmt)
	assert.NotNil(t, sel.Body.Left.Qualify)
}

// === FROM clause tests ===

func TestParse_FromSimple(t *testing.T) {
	stmt, err := Parse("SELECT * FROM titanic")
	require.NoError(t, err)
	sel := stmt.(*SelectStmt)
	tn, ok := sel.Body.Left.From.Source.(*TableName)
	require.True(t, ok)
	assert.Equal(t, "titanic", tn.Name)
}

func TestParse_FromAlias(t *testing.T) {
	stmt, err := Parse("SELECT * FROM titanic t")
	require.NoError(t, err)
	sel := stmt.(*SelectStmt)
	tn, ok := sel.Body.Left.From.Source.(*TableName)
	require.True(t, ok)
	assert.Equal(t, "titanic", tn.Name)
	assert.Equal(t, "t", tn.Alias)
}

func TestParse_FromSchema(t *testing.T) {
	stmt, err := Parse("SELECT * FROM main.titanic")
	require.NoError(t, err)
	sel := stmt.(*SelectStmt)
	tn, ok := sel.Body.Left.From.Source.(*TableName)
	require.True(t, ok)
	assert.Equal(t, "main", tn.Schema)
	assert.Equal(t, "titanic", tn.Name)
}

func TestParse_FromCatalog(t *testing.T) {
	stmt, err := Parse("SELECT * FROM lake.main.titanic")
	require.NoError(t, err)
	sel := stmt.(*SelectStmt)
	tn, ok := sel.Body.Left.From.Source.(*TableName)
	require.True(t, ok)
	assert.Equal(t, "lake", tn.Catalog)
	assert.Equal(t, "main", tn.Schema)
	assert.Equal(t, "titanic", tn.Name)
}

func TestParse_FromSubquery(t *testing.T) {
	stmt, err := Parse("SELECT * FROM (SELECT * FROM t) sub")
	require.NoError(t, err)
	sel := stmt.(*SelectStmt)
	dt, ok := sel.Body.Left.From.Source.(*DerivedTable)
	require.True(t, ok)
	assert.Equal(t, "sub", dt.Alias)
	assert.NotNil(t, dt.Select)
}

// === JOIN tests ===

func TestParse_JoinTypes(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		joinType JoinType
	}{
		{"inner", "SELECT * FROM a JOIN b ON a.id = b.id", JoinInner},
		{"inner_explicit", "SELECT * FROM a INNER JOIN b ON a.id = b.id", JoinInner},
		{"left", "SELECT * FROM a LEFT JOIN b ON a.id = b.id", JoinLeft},
		{"right", "SELECT * FROM a RIGHT JOIN b ON a.id = b.id", JoinRight},
		{"full", "SELECT * FROM a FULL JOIN b ON a.id = b.id", JoinFull},
		{"cross", "SELECT * FROM a CROSS JOIN b", JoinCross},
		{"comma", "SELECT * FROM a, b", JoinComma},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			stmt, err := Parse(tc.sql)
			require.NoError(t, err)
			sel := stmt.(*SelectStmt)
			require.NotEmpty(t, sel.Body.Left.From.Joins)
			assert.Equal(t, tc.joinType, sel.Body.Left.From.Joins[0].Type)
		})
	}
}

func TestParse_JoinUsing(t *testing.T) {
	stmt, err := Parse("SELECT * FROM a JOIN b USING (id)")
	require.NoError(t, err)
	sel := stmt.(*SelectStmt)
	assert.Equal(t, []string{"id"}, sel.Body.Left.From.Joins[0].Using)
}

func TestParse_NaturalJoin(t *testing.T) {
	stmt, err := Parse("SELECT * FROM a NATURAL JOIN b")
	require.NoError(t, err)
	sel := stmt.(*SelectStmt)
	assert.True(t, sel.Body.Left.From.Joins[0].Natural)
}

// === Set operations ===

func TestParse_Union(t *testing.T) {
	stmt, err := Parse("SELECT * FROM a UNION SELECT * FROM b")
	require.NoError(t, err)
	sel := stmt.(*SelectStmt)
	assert.Equal(t, SetOpUnion, sel.Body.Op)
	assert.NotNil(t, sel.Body.Right)
}

func TestParse_UnionAll(t *testing.T) {
	stmt, err := Parse("SELECT * FROM a UNION ALL SELECT * FROM b")
	require.NoError(t, err)
	sel := stmt.(*SelectStmt)
	assert.Equal(t, SetOpUnionAll, sel.Body.Op)
}

func TestParse_Intersect(t *testing.T) {
	stmt, err := Parse("SELECT * FROM a INTERSECT SELECT * FROM b")
	require.NoError(t, err)
	sel := stmt.(*SelectStmt)
	assert.Equal(t, SetOpIntersect, sel.Body.Op)
}

func TestParse_Except(t *testing.T) {
	stmt, err := Parse("SELECT * FROM a EXCEPT SELECT * FROM b")
	require.NoError(t, err)
	sel := stmt.(*SelectStmt)
	assert.Equal(t, SetOpExcept, sel.Body.Op)
}

// === CTE ===

func TestParse_CTE(t *testing.T) {
	stmt, err := Parse("WITH cte AS (SELECT * FROM t) SELECT * FROM cte")
	require.NoError(t, err)
	sel := stmt.(*SelectStmt)
	require.NotNil(t, sel.With)
	assert.Len(t, sel.With.CTEs, 1)
	assert.Equal(t, "cte", sel.With.CTEs[0].Name)
}

func TestParse_CTERecursive(t *testing.T) {
	stmt, err := Parse("WITH RECURSIVE cte AS (SELECT 1 UNION ALL SELECT n+1 FROM cte WHERE n < 10) SELECT * FROM cte")
	require.NoError(t, err)
	sel := stmt.(*SelectStmt)
	assert.True(t, sel.With.Recursive)
}

// === DML statements ===

func TestParse_InsertValues(t *testing.T) {
	stmt, err := Parse("INSERT INTO t (a, b) VALUES (1, 'x'), (2, 'y')")
	require.NoError(t, err)
	ins, ok := stmt.(*InsertStmt)
	require.True(t, ok)
	assert.Equal(t, "t", ins.Table.Name)
	assert.Equal(t, []string{"a", "b"}, ins.Columns)
	assert.Len(t, ins.Values, 2)
}

func TestParse_InsertSelect(t *testing.T) {
	stmt, err := Parse("INSERT INTO t SELECT * FROM s")
	require.NoError(t, err)
	ins, ok := stmt.(*InsertStmt)
	require.True(t, ok)
	assert.NotNil(t, ins.Query)
}

func TestParse_Update(t *testing.T) {
	stmt, err := Parse(`UPDATE t SET name = 'test' WHERE id = 1`)
	require.NoError(t, err)
	upd, ok := stmt.(*UpdateStmt)
	require.True(t, ok)
	assert.Equal(t, "t", upd.Table.Name)
	assert.Len(t, upd.Sets, 1)
	assert.NotNil(t, upd.Where)
}

func TestParse_Delete(t *testing.T) {
	stmt, err := Parse("DELETE FROM t WHERE id = 1")
	require.NoError(t, err)
	del, ok := stmt.(*DeleteStmt)
	require.True(t, ok)
	assert.Equal(t, "t", del.Table.Name)
	assert.NotNil(t, del.Where)
}

// === DDL / Utility statements ===

func TestParse_CreateTable(t *testing.T) {
	stmt, err := Parse("CREATE TABLE foo (id INT, name VARCHAR)")
	require.NoError(t, err)
	ddl, ok := stmt.(*DDLStmt)
	require.True(t, ok)
	assert.Equal(t, DDLCreateTable, ddl.Type)
}

func TestParse_DropTable(t *testing.T) {
	stmt, err := Parse("DROP TABLE foo")
	require.NoError(t, err)
	ddl, ok := stmt.(*DDLStmt)
	require.True(t, ok)
	assert.Equal(t, DDLDrop, ddl.Type)
}

func TestParse_CreateView(t *testing.T) {
	stmt, err := Parse("CREATE VIEW v AS SELECT * FROM t")
	require.NoError(t, err)
	ddl, ok := stmt.(*DDLStmt)
	require.True(t, ok)
	assert.Equal(t, DDLCreateView, ddl.Type)
}

func TestParse_CreateSchema(t *testing.T) {
	stmt, err := Parse("CREATE SCHEMA myschema")
	require.NoError(t, err)
	ddl, ok := stmt.(*DDLStmt)
	require.True(t, ok)
	assert.Equal(t, DDLCreateSchema, ddl.Type)
}

func TestParse_UtilitySet(t *testing.T) {
	stmt, err := Parse("SET threads = 4")
	require.NoError(t, err)
	util, ok := stmt.(*UtilityStmt)
	require.True(t, ok)
	assert.Equal(t, UtilitySet, util.Type)
}

func TestParse_UtilityDescribe(t *testing.T) {
	stmt, err := Parse("DESCRIBE titanic")
	require.NoError(t, err)
	util, ok := stmt.(*UtilityStmt)
	require.True(t, ok)
	assert.Equal(t, UtilityDescribe, util.Type)
}

func TestParse_UtilityAttach(t *testing.T) {
	stmt, err := Parse("ATTACH 'test.db' AS testdb")
	require.NoError(t, err)
	util, ok := stmt.(*UtilityStmt)
	require.True(t, ok)
	assert.Equal(t, UtilityAttach, util.Type)
}

// === Missing statement starters ===

func TestParse_UtilityStatementStarters(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		utilType UtilityType
	}{
		{"show_tables", "SHOW TABLES", UtilityShow},
		{"show_databases", "SHOW databases", UtilityShow},
		{"explain", "EXPLAIN SELECT 1", UtilityExplain},
		{"explain_analyze", "EXPLAIN ANALYZE SELECT * FROM t", UtilityExplain},
		{"summarize", "SUMMARIZE titanic", UtilitySummarize},
		{"begin", "BEGIN TRANSACTION", UtilityBegin},
		{"begin_simple", "BEGIN", UtilityBegin},
		{"commit", "COMMIT", UtilityCommit},
		{"rollback", "ROLLBACK", UtilityRollback},
		{"prepare", "PREPARE q AS SELECT 1", UtilityPrepare},
		{"execute", "EXECUTE q", UtilityExecute},
		{"deallocate", "DEALLOCATE q", UtilityDeallocate},
		{"vacuum", "VACUUM", UtilityVacuum},
		{"vacuum_analyze", "VACUUM ANALYZE", UtilityVacuum},
		{"checkpoint", "CHECKPOINT", UtilityCheckpoint},
		{"reindex", "REINDEX", UtilityReindex},
		{"grant", "GRANT SELECT ON t TO user1", UtilityGrant},
		{"revoke", "REVOKE SELECT ON t FROM user1", UtilityRevoke},
		{"reset", "RESET ALL", UtilityReset},
		{"reset_setting", "RESET threads", UtilityReset},
		{"import", "IMPORT DATABASE 'path'", UtilityImport},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			stmt, err := Parse(tc.sql)
			require.NoError(t, err)
			util, ok := stmt.(*UtilityStmt)
			require.True(t, ok, "expected UtilityStmt, got %T", stmt)
			assert.Equal(t, tc.utilType, util.Type)
		})
	}
}

// === TRY_CAST ===

func TestParse_TryCast(t *testing.T) {
	stmt, err := Parse("SELECT TRY_CAST(x AS INTEGER) FROM t")
	require.NoError(t, err)
	sel := stmt.(*SelectStmt)
	cast, ok := sel.Body.Left.Columns[0].Expr.(*CastExpr)
	require.True(t, ok)
	assert.True(t, cast.TryCast)
	assert.Equal(t, "INTEGER", cast.TypeName)
}

func TestParse_TryCast_Nested(t *testing.T) {
	stmt, err := Parse("SELECT TRY_CAST(TRY_CAST(x AS VARCHAR) AS INTEGER) FROM t")
	require.NoError(t, err)
	sel := stmt.(*SelectStmt)
	cast, ok := sel.Body.Left.Columns[0].Expr.(*CastExpr)
	require.True(t, ok)
	assert.True(t, cast.TryCast)
	assert.Equal(t, "INTEGER", cast.TypeName)
	inner, ok := cast.Expr.(*CastExpr)
	require.True(t, ok)
	assert.True(t, inner.TryCast)
	assert.Equal(t, "VARCHAR", inner.TypeName)
}

// === EXTRACT ===

func TestParse_Extract(t *testing.T) {
	tests := []struct {
		name  string
		sql   string
		field string
	}{
		{"year", "SELECT EXTRACT(YEAR FROM created_at) FROM t", "YEAR"},
		{"month", "SELECT EXTRACT(MONTH FROM d) FROM t", "MONTH"},
		{"day", "SELECT EXTRACT(DAY FROM d) FROM t", "DAY"},
		{"hour", "SELECT EXTRACT(HOUR FROM ts) FROM t", "HOUR"},
		{"epoch", "SELECT EXTRACT(EPOCH FROM ts) FROM t", "EPOCH"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			stmt, err := Parse(tc.sql)
			require.NoError(t, err)
			sel := stmt.(*SelectStmt)
			ext, ok := sel.Body.Left.Columns[0].Expr.(*ExtractExpr)
			require.True(t, ok)
			assert.Equal(t, tc.field, ext.Field)
		})
	}
}

// === GLOB ===

func TestParse_GlobExpr(t *testing.T) {
	t.Run("glob", func(t *testing.T) {
		stmt, err := Parse("SELECT * FROM t WHERE name GLOB '*test*'")
		require.NoError(t, err)
		sel := stmt.(*SelectStmt)
		g, ok := sel.Body.Left.Where.(*GlobExpr)
		require.True(t, ok)
		assert.False(t, g.Not)
	})

	t.Run("not_glob", func(t *testing.T) {
		stmt, err := Parse("SELECT * FROM t WHERE name NOT GLOB '*test*'")
		require.NoError(t, err)
		sel := stmt.(*SelectStmt)
		g, ok := sel.Body.Left.Where.(*GlobExpr)
		require.True(t, ok)
		assert.True(t, g.Not)
	})
}

// === SIMILAR TO ===

func TestParse_SimilarToExpr(t *testing.T) {
	t.Run("similar_to", func(t *testing.T) {
		stmt, err := Parse("SELECT * FROM t WHERE name SIMILAR TO '%test%'")
		require.NoError(t, err)
		sel := stmt.(*SelectStmt)
		s, ok := sel.Body.Left.Where.(*SimilarToExpr)
		require.True(t, ok)
		assert.False(t, s.Not)
	})

	t.Run("not_similar_to", func(t *testing.T) {
		stmt, err := Parse("SELECT * FROM t WHERE name NOT SIMILAR TO '%test%'")
		require.NoError(t, err)
		sel := stmt.(*SelectStmt)
		s, ok := sel.Body.Left.Where.(*SimilarToExpr)
		require.True(t, ok)
		assert.True(t, s.Not)
	})
}

// === Window functions ===

func TestParse_WindowFunction(t *testing.T) {
	stmt, err := Parse("SELECT row_number() OVER (PARTITION BY x ORDER BY y) FROM t")
	require.NoError(t, err)
	sel := stmt.(*SelectStmt)
	fn, ok := sel.Body.Left.Columns[0].Expr.(*FuncCall)
	require.True(t, ok)
	assert.NotNil(t, fn.Window)
	assert.Len(t, fn.Window.PartitionBy, 1)
	assert.Len(t, fn.Window.OrderBy, 1)
}
