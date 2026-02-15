package duckdbsql

// Comprehensive DuckDB SQL syntax test suite derived from the official DuckDB
// documentation at docs/stable/sql/. Each test case exercises a specific syntax
// form from the docs. Tests are organized by documentation section.
//
// The primary test (TestDuckDBSpec_Parse) verifies that Parse() succeeds for
// every documented syntax form. The round-trip test (TestDuckDBSpec_RoundTrip)
// additionally checks that Format(Parse(sql)) produces valid SQL that can be
// re-parsed.
//
// Tests that fail indicate missing parser support — they are the implementation
// backlog.

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// specCase represents a single syntax test from the DuckDB docs.
type specCase struct {
	name string // descriptive test name
	sql  string // SQL to parse (must be a single statement)
}

// === Expressions ===

var specExprComparison = []specCase{
	// docs/stable/sql/expressions/comparison_operators.md
	{name: "eq", sql: `SELECT 2 = 3`},
	{name: "double_eq", sql: `SELECT 2 == 3`},
	{name: "ne_angle", sql: `SELECT 2 <> 2`},
	{name: "ne_bang", sql: `SELECT 2 != 2`},
	{name: "lt", sql: `SELECT 2 < 3`},
	{name: "gt", sql: `SELECT 2 > 3`},
	{name: "le", sql: `SELECT 2 <= 3`},
	{name: "ge", sql: `SELECT 4 >= 1`},
	{name: "is_null", sql: `SELECT x IS NULL FROM t`},
	{name: "is_not_null", sql: `SELECT x IS NOT NULL FROM t`},
	{name: "isnull_alias", sql: `SELECT x ISNULL FROM t`},
	{name: "notnull_alias", sql: `SELECT x NOTNULL FROM t`},
	{name: "between", sql: `SELECT x FROM t WHERE a BETWEEN 1 AND 10`},
	{name: "not_between", sql: `SELECT x FROM t WHERE a NOT BETWEEN 1 AND 10`},
	{name: "is_distinct_from", sql: `SELECT 2 IS DISTINCT FROM NULL`},
	{name: "is_not_distinct_from", sql: `SELECT NULL IS NOT DISTINCT FROM NULL`},
}

var specExprLogical = []specCase{
	// docs/stable/sql/expressions/logical_operators.md
	{name: "and", sql: `SELECT a AND b FROM t`},
	{name: "or", sql: `SELECT a OR b FROM t`},
	{name: "not", sql: `SELECT NOT a FROM t`},
	{name: "null_and_false", sql: `SELECT NULL AND false`},
	{name: "null_or_true", sql: `SELECT NULL OR true`},
	{name: "compound", sql: `SELECT a AND b OR NOT c FROM t`},
}

var specExprArithmetic = []specCase{
	// Standard arithmetic operators
	{name: "add", sql: `SELECT 1 + 2`},
	{name: "subtract", sql: `SELECT 3 - 1`},
	{name: "multiply", sql: `SELECT 2 * 3`},
	{name: "divide", sql: `SELECT 10 / 3`},
	{name: "modulo", sql: `SELECT 10 % 3`},
	{name: "integer_division", sql: `SELECT 10 // 3`},
	{name: "string_concat", sql: `SELECT 'hello' || ' ' || 'world'`},
	{name: "unary_minus", sql: `SELECT -x FROM t`},
	{name: "unary_plus", sql: `SELECT +x FROM t`},
}

var specExprBitwise = []specCase{
	// Bitwise operators (DuckDB supports these on INTEGER types)
	{name: "bitwise_and", sql: `SELECT 5 & 3`},
	{name: "bitwise_or", sql: `SELECT 5 | 3`},
	{name: "bitwise_xor", sql: `SELECT 5 ^ 3`},
	{name: "bitwise_not", sql: `SELECT ~5`},
	{name: "shift_left", sql: `SELECT 1 << 4`},
	{name: "shift_right", sql: `SELECT 16 >> 2`},
	{name: "bitwise_compound", sql: `SELECT (a & b) | (c ^ d) FROM t`},
}

var specExprCase = []specCase{
	// docs/stable/sql/expressions/case.md
	{name: "searched_case", sql: `SELECT CASE WHEN i > 2 THEN 1 ELSE 0 END FROM t`},
	{name: "simple_case", sql: `SELECT CASE i WHEN 1 THEN 10 WHEN 2 THEN 20 WHEN 3 THEN 30 END FROM t`},
	{name: "case_no_else", sql: `SELECT CASE WHEN i = 1 THEN 10 END FROM t`},
	{name: "chained_when", sql: `SELECT CASE WHEN i = 1 THEN 10 WHEN i = 2 THEN 20 ELSE 0 END FROM t`},
	{name: "if_function", sql: `SELECT IF(i > 2, 1, 0) FROM t`},
}

var specExprCast = []specCase{
	// docs/stable/sql/expressions/cast.md
	{name: "cast_basic", sql: `SELECT CAST(i AS VARCHAR) FROM t`},
	{name: "cast_shorthand", sql: `SELECT i::DOUBLE FROM t`},
	{name: "try_cast", sql: `SELECT TRY_CAST('hello' AS INTEGER)`},
	{name: "cast_to_decimal", sql: `SELECT CAST(1.5 AS DECIMAL(10, 2))`},
	{name: "cast_to_varchar_array", sql: `SELECT CAST(x AS VARCHAR[]) FROM t`},
	{name: "double_colon_chain", sql: `SELECT '42'::INTEGER::DOUBLE`},
}

var specExprIn = []specCase{
	// docs/stable/sql/expressions/in.md
	{name: "in_tuple", sql: `SELECT 'Math' IN ('CS', 'Math')`},
	{name: "in_list", sql: `SELECT 'Math' IN ['CS', 'Math', NULL]`},
	{name: "in_subquery", sql: `SELECT 42 IN (SELECT unnest([32, 42, 52]) AS x)`},
	{name: "not_in", sql: `SELECT x FROM t WHERE x NOT IN (1, 2, 3)`},
	{name: "in_string", sql: `SELECT 'Hello' IN 'Hello World'`},
}

var specExprStar = []specCase{
	// docs/stable/sql/expressions/star.md
	{name: "star", sql: `SELECT * FROM tbl`},
	{name: "table_star", sql: `SELECT tbl.* FROM tbl JOIN other USING (id)`},
	{name: "star_exclude", sql: `SELECT * EXCLUDE (col) FROM tbl`},
	{name: "star_exclude_multi", sql: `SELECT * EXCLUDE (col1, col2) FROM tbl`},
	{name: "star_replace", sql: `SELECT * REPLACE (col1 / 1000 AS col1) FROM tbl`},
	{name: "star_rename", sql: `SELECT * RENAME (col1 AS height, col2 AS width) FROM tbl`},
	{name: "columns_star", sql: `SELECT min(COLUMNS(*)), count(COLUMNS(*)) FROM numbers`},
	{name: "columns_regex", sql: `SELECT COLUMNS('(id|numbers?)') FROM numbers`},
	{name: "columns_lambda", sql: `SELECT COLUMNS(c -> c LIKE '%num%') FROM numbers`},
	{name: "columns_list", sql: `SELECT COLUMNS(['id', 'num']) FROM numbers`},
	{name: "columns_exclude_inside", sql: `SELECT count(COLUMNS(* EXCLUDE (number))) FROM numbers`},
	{name: "columns_replace_inside", sql: `SELECT min(COLUMNS(* REPLACE (number + id AS number))) FROM numbers`},
}

var specExprSubquery = []specCase{
	// docs/stable/sql/expressions/subqueries.md
	{name: "scalar_subquery", sql: `SELECT (SELECT min(grade) FROM grades) FROM t`},
	{name: "scalar_subquery_where", sql: `SELECT course FROM grades WHERE grade = (SELECT min(grade) FROM grades)`},
	{name: "exists", sql: `SELECT EXISTS (SELECT * FROM grades WHERE course = 'Math')`},
	{name: "not_exists", sql: `SELECT * FROM person WHERE NOT EXISTS (SELECT * FROM interest WHERE interest.person_id = person.id)`},
	{name: "any_subquery", sql: `SELECT 5 >= ANY (SELECT grade FROM grades)`},
	{name: "all_subquery", sql: `SELECT 6 <= ALL (SELECT grade FROM grades)`},
	{name: "array_subquery", sql: `SELECT ARRAY(SELECT grade FROM grades)`},
	{name: "correlated_subquery", sql: `SELECT * FROM grades g1 WHERE grade = (SELECT min(grade) FROM grades WHERE course = g1.course)`},
}

var specExprCollation = []specCase{
	// docs/stable/sql/expressions/collations.md
	{name: "collate_nocase", sql: `SELECT 'hello' COLLATE NOCASE = 'HELLO'`},
	{name: "collate_noaccent", sql: "SELECT 'hello' COLLATE NOACCENT = 'h\u00ebllo'"},
	{name: "collate_chained", sql: "SELECT 'hello' COLLATE NOCASE.NOACCENT = 'H\u00cbLLO'"},
}

var specExprTry = []specCase{
	// docs/stable/sql/expressions/try.md
	{name: "try_cast_expr", sql: `SELECT TRY('abc'::INTEGER)`},
	{name: "try_function", sql: `SELECT TRY(ln(0))`},
}

var specExprLike = []specCase{
	// LIKE / ILIKE / GLOB / SIMILAR TO
	{name: "like", sql: `SELECT * FROM t WHERE name LIKE '%mark%'`},
	{name: "ilike", sql: `SELECT * FROM t WHERE name ILIKE '%mark%'`},
	{name: "not_like", sql: `SELECT * FROM t WHERE name NOT LIKE '%mark%'`},
	{name: "not_ilike", sql: `SELECT * FROM t WHERE name NOT ILIKE '%mark%'`},
	{name: "glob", sql: `SELECT * FROM t WHERE name GLOB 'mar*'`},
	{name: "not_glob", sql: `SELECT * FROM t WHERE name NOT GLOB 'mar*'`},
	{name: "similar_to", sql: `SELECT * FROM t WHERE name SIMILAR TO 'mar.'`},
	{name: "not_similar_to", sql: `SELECT * FROM t WHERE name NOT SIMILAR TO 'mar.'`},
	{name: "like_escape", sql: `SELECT * FROM t WHERE name LIKE '%\%%' ESCAPE '\'`},
}

var specExprMisc = []specCase{
	// Miscellaneous expression forms
	{name: "is_true", sql: `SELECT x IS TRUE FROM t`},
	{name: "is_not_true", sql: `SELECT x IS NOT TRUE FROM t`},
	{name: "is_false", sql: `SELECT x IS FALSE FROM t`},
	{name: "is_not_false", sql: `SELECT x IS NOT FALSE FROM t`},
	{name: "extract_year", sql: `SELECT EXTRACT(YEAR FROM d) FROM t`},
	{name: "extract_epoch", sql: `SELECT EXTRACT(EPOCH FROM ts) FROM t`},
	{name: "interval", sql: `SELECT INTERVAL '1' YEAR`},
	{name: "interval_compound", sql: `SELECT ts + INTERVAL '3' DAY FROM t`},
	{name: "coalesce", sql: `SELECT COALESCE(a, b, c) FROM t`},
	{name: "nullif", sql: `SELECT NULLIF(a, 0) FROM t`},
}

// === DuckDB-Specific Expressions ===

var specExprDuckDB = []specCase{
	// DuckDB-specific expression syntax
	{name: "lambda_simple", sql: `SELECT list_transform([1, 2, 3], x -> x + 1)`},
	{name: "lambda_multi_param", sql: `SELECT list_reduce([1, 2, 3], (x, y) -> x + y)`},
	{name: "struct_literal", sql: `SELECT {'x': 1, 'y': 2, 'z': 3}`},
	{name: "struct_nested", sql: `SELECT {'birds': ['duck', 'goose'], 'aliens': NULL}`},
	{name: "list_literal", sql: `SELECT [1, 2, 3]`},
	{name: "list_empty", sql: `SELECT []`},
	{name: "array_index", sql: `SELECT arr[1] FROM t`},
	{name: "array_slice", sql: `SELECT arr[1:3] FROM t`},
	{name: "array_slice_open_start", sql: `SELECT arr[:3] FROM t`},
	{name: "array_slice_open_end", sql: `SELECT arr[1:] FROM t`},
	{name: "map_literal", sql: `SELECT MAP {'key1': 50, 'key2': 75}`},
	{name: "map_access_bracket", sql: `SELECT m['key1'] FROM t`},
	{name: "struct_dot_access", sql: `SELECT s.field FROM t`},
	{name: "struct_dot_chain", sql: `SELECT s.a.b.c FROM t`},
	{name: "list_comprehension", sql: `SELECT [x + 1 FOR x IN [1, 2, 3]]`},
	{name: "list_comprehension_if", sql: `SELECT [x FOR x IN [1, 2, 3, 4, 5] IF x > 2]`},
	{name: "union_value", sql: `SELECT union_value(num := 2)`},
	{name: "underscore_numeric", sql: `SELECT 100_000_000`},
}

// === SELECT Clause ===

var specSelect = []specCase{
	// docs/stable/sql/query_syntax/select.md
	{name: "select_star", sql: `SELECT * FROM tbl`},
	{name: "select_expression", sql: `SELECT col1 + col2 AS res, sqrt(col1) AS root FROM tbl`},
	{name: "select_distinct", sql: `SELECT DISTINCT city FROM addresses`},
	{name: "select_distinct_on", sql: `SELECT DISTINCT ON(country) city, population FROM cities ORDER BY population DESC`},
	{name: "count_star", sql: `SELECT count(*) FROM addresses`},
	{name: "window_row_number", sql: `SELECT row_number() OVER () FROM sales`},
	{name: "window_lag", sql: `SELECT amount - lag(amount) OVER (ORDER BY time) FROM sales`},
	{name: "select_unnest_list", sql: `SELECT unnest([1, 2, 3])`},
	{name: "select_unnest_struct", sql: `SELECT unnest({'a': 42, 'b': 84})`},
	{name: "prefix_alias", sql: `SELECT res: col1 + col2 FROM tbl`},
}

// === FROM and JOIN ===

var specFrom = []specCase{
	// docs/stable/sql/query_syntax/from.md
	{name: "basic_from", sql: `SELECT * FROM tbl`},
	{name: "from_first", sql: `FROM tbl SELECT *`},
	{name: "from_first_implicit", sql: `FROM tbl`},
	{name: "from_first_with_where", sql: `FROM range(100) AS t(i) SELECT sum(t.i) WHERE i % 2 = 0`},
	{name: "table_alias", sql: `SELECT tn.* FROM tbl tn`},
	{name: "schema_qualified", sql: `SELECT * FROM schema_name.tbl`},
	{name: "catalog_schema_table", sql: `SELECT * FROM catalog.schema_name.tbl`},
	{name: "table_function", sql: `SELECT t.i FROM range(100) AS t(i)`},
	{name: "subquery_from", sql: `SELECT * FROM (SELECT * FROM tbl) sub`},
	{name: "csv_string_from", sql: `SELECT * FROM 'test.csv'`},
	{name: "read_csv_func", sql: `SELECT * FROM read_csv('test.csv')`},
	{name: "with_ordinality", sql: `SELECT * FROM read_csv('test.csv') WITH ORDINALITY`},
	{name: "cross_join", sql: `SELECT a.*, b.* FROM a CROSS JOIN b`},
	{name: "comma_join", sql: `SELECT a.*, b.* FROM a, b`},
	{name: "inner_join_on", sql: `SELECT * FROM t1 JOIN t2 ON t1.key = t2.key`},
	{name: "inner_join_using", sql: `SELECT * FROM t1 JOIN t2 USING (key)`},
	{name: "left_join", sql: `SELECT * FROM t1 LEFT JOIN t2 ON t1.id = t2.id`},
	{name: "right_join", sql: `SELECT * FROM t1 RIGHT JOIN t2 ON t1.id = t2.id`},
	{name: "full_outer_join", sql: `SELECT * FROM t1 FULL OUTER JOIN t2 ON t1.id = t2.id`},
	{name: "natural_join", sql: `SELECT * FROM t1 NATURAL JOIN t2`},
	{name: "semi_join", sql: `SELECT * FROM t1 SEMI JOIN t2 USING (id)`},
	{name: "anti_join", sql: `SELECT * FROM t1 ANTI JOIN t2 USING (id)`},
	{name: "positional_join", sql: `SELECT * FROM t1 POSITIONAL JOIN t2`},
	{name: "asof_join", sql: `SELECT t.*, p.price FROM trades t ASOF JOIN prices p ON t.symbol = p.symbol AND t.when >= p.when`},
	{name: "asof_left_join", sql: `SELECT * FROM trades t ASOF LEFT JOIN prices p ON t.symbol = p.symbol AND t.when >= p.when`},
	{name: "asof_join_using", sql: `SELECT * FROM trades t ASOF JOIN prices p USING (symbol, "when")`},
	{name: "lateral_subquery", sql: `SELECT * FROM range(3) t(i), LATERAL (SELECT i + 1) t2(j)`},
	{name: "lateral_union", sql: `SELECT * FROM generate_series(0, 1) t(i), LATERAL (SELECT i + 10 UNION ALL SELECT i + 100) t2(j)`},
	{name: "values_in_from", sql: `SELECT * FROM (VALUES ('Amsterdam', 1), ('London', 2)) cities(name, id)`},
	{name: "values_in_join", sql: `SELECT * FROM t1 NATURAL JOIN (VALUES (2), (4)) _(x)`},
	{name: "self_join_alias", sql: `SELECT * FROM t AS t1 JOIN t AS t2 USING (x)`},
	{name: "tablesample_percent", sql: `SELECT * FROM tbl TABLESAMPLE 10%`},
	{name: "tablesample_rows", sql: `SELECT * FROM tbl TABLESAMPLE 10 ROWS`},
	{name: "row_as_struct", sql: `SELECT t FROM (SELECT 41 AS x, 'hello' AS y) t`},
}

// === WHERE ===

var specWhere = []specCase{
	// docs/stable/sql/query_syntax/where.md
	{name: "where_eq", sql: `SELECT * FROM tbl WHERE id = 3`},
	{name: "where_like", sql: `SELECT * FROM tbl WHERE name LIKE '%mark%'`},
	{name: "where_ilike", sql: `SELECT * FROM tbl WHERE name ILIKE '%mark%'`},
	{name: "where_or", sql: `SELECT * FROM tbl WHERE id = 3 OR id = 7`},
}

// === GROUP BY ===

var specGroupBy = []specCase{
	// docs/stable/sql/query_syntax/groupby.md
	{name: "group_by_single", sql: `SELECT city, count(*) FROM addresses GROUP BY city`},
	{name: "group_by_multi", sql: `SELECT city, street, avg(income) FROM addresses GROUP BY city, street`},
	{name: "group_by_all", sql: `SELECT city, street, avg(income) FROM addresses GROUP BY ALL`},
}

// === GROUPING SETS / CUBE / ROLLUP ===

var specGroupingSets = []specCase{
	// docs/stable/sql/query_syntax/grouping_sets.md
	{name: "grouping_sets", sql: `SELECT city, street, avg(income) FROM addresses GROUP BY GROUPING SETS ((city, street), (city), (street), ())`},
	{name: "cube", sql: `SELECT city, street, avg(income) FROM addresses GROUP BY CUBE (city, street)`},
	{name: "rollup", sql: `SELECT city, street, avg(income) FROM addresses GROUP BY ROLLUP (city, street)`},
	{name: "grouping_id", sql: `SELECT y, q, m, GROUPING_ID(y, q, m) FROM days GROUP BY GROUPING SETS ((y, q, m), (y, q), (y), ())`},
}

// === HAVING ===

var specHaving = []specCase{
	// docs/stable/sql/query_syntax/having.md
	{name: "having_count", sql: `SELECT city, count(*) FROM addresses GROUP BY city HAVING count(*) >= 50`},
	{name: "having_compound", sql: `SELECT city, street, avg(income) FROM addresses GROUP BY city, street HAVING avg(income) > 2 * median(income)`},
}

// === ORDER BY ===

var specOrderBy = []specCase{
	// docs/stable/sql/query_syntax/orderby.md
	{name: "order_by", sql: `SELECT * FROM addresses ORDER BY city`},
	{name: "order_by_desc", sql: `SELECT * FROM addresses ORDER BY city DESC`},
	{name: "order_by_nulls_last", sql: `SELECT * FROM addresses ORDER BY city DESC NULLS LAST`},
	{name: "order_by_nulls_first", sql: `SELECT * FROM addresses ORDER BY city ASC NULLS FIRST`},
	{name: "order_by_multi", sql: `SELECT * FROM addresses ORDER BY city, zip`},
	{name: "order_by_collate", sql: `SELECT * FROM addresses ORDER BY city COLLATE DE`},
	{name: "order_by_all", sql: `SELECT * FROM addresses ORDER BY ALL`},
	{name: "order_by_all_desc", sql: `SELECT * FROM addresses ORDER BY ALL DESC`},
}

// === LIMIT / OFFSET ===

var specLimit = []specCase{
	// docs/stable/sql/query_syntax/limit.md
	{name: "limit", sql: `SELECT * FROM addresses LIMIT 5`},
	{name: "limit_offset", sql: `SELECT * FROM addresses LIMIT 5 OFFSET 5`},
	{name: "limit_percent", sql: `SELECT * FROM addresses LIMIT 10%`},
}

// === SAMPLE ===

var specSample = []specCase{
	// docs/stable/sql/query_syntax/sample.md
	{name: "using_sample_percent", sql: `SELECT * FROM addresses USING SAMPLE 1%`},
	{name: "using_sample_bernoulli", sql: `SELECT * FROM addresses USING SAMPLE 1% (bernoulli)`},
	{name: "using_sample_rows", sql: `SELECT * FROM (SELECT * FROM addresses) USING SAMPLE 10 ROWS`},
}

// === QUALIFY ===

var specQualify = []specCase{
	// docs/stable/sql/query_syntax/qualify.md
	{name: "qualify_inline", sql: `SELECT schema_name, function_name, row_number() OVER (PARTITION BY schema_name ORDER BY function_name) AS function_rank FROM duckdb_functions() QUALIFY row_number() OVER (PARTITION BY schema_name ORDER BY function_name) < 3`},
	{name: "qualify_alias", sql: `SELECT schema_name, function_name, row_number() OVER (PARTITION BY schema_name ORDER BY function_name) AS function_rank FROM duckdb_functions() QUALIFY function_rank < 3`},
}

// === WINDOW ===

var specWindow = []specCase{
	// Window function syntax from various docs
	{name: "window_partition_order", sql: `SELECT row_number() OVER (PARTITION BY dept ORDER BY salary DESC) FROM employees`},
	{name: "window_named", sql: `SELECT row_number() OVER my_window AS rn FROM t WINDOW my_window AS (PARTITION BY x ORDER BY y)`},
	{name: "window_frame_rows", sql: `SELECT sum(x) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM t`},
	{name: "window_frame_range", sql: `SELECT sum(x) OVER (ORDER BY id RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM t`},
	{name: "window_frame_groups", sql: `SELECT sum(x) OVER (ORDER BY id GROUPS BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM t`},
	{name: "window_frame_unbounded", sql: `SELECT sum(x) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FROM t`},
	{name: "window_filter", sql: `SELECT count(*) FILTER (WHERE x > 0) OVER (PARTITION BY g) FROM t`},
	{name: "aggregate_order_by", sql: `SELECT array_agg(x ORDER BY y DESC) FROM t`},
	{name: "aggregate_distinct", sql: `SELECT count(DISTINCT x) FROM t`},
	{name: "within_group", sql: `SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY x) FROM t`},
}

// === WITH / CTE ===

var specWith = []specCase{
	// docs/stable/sql/query_syntax/with.md
	{name: "basic_cte", sql: `WITH cte AS (SELECT 42 AS x) SELECT * FROM cte`},
	{name: "chained_cte", sql: `WITH cte1 AS (SELECT 42 AS i), cte2 AS (SELECT i * 100 AS x FROM cte1) SELECT * FROM cte2`},
	{name: "cte_column_list", sql: `WITH cte(j) AS (SELECT 42 AS i) SELECT * FROM cte`},
	{name: "cte_materialized", sql: `WITH t(x) AS MATERIALIZED (SELECT 42) SELECT * FROM t`},
	{name: "cte_not_materialized", sql: `WITH t(x) AS NOT MATERIALIZED (SELECT 42) SELECT * FROM t`},
	{name: "recursive_cte", sql: `WITH RECURSIVE cnt(x) AS (SELECT 0 UNION ALL SELECT x + 1 FROM cnt WHERE x < 10) SELECT x FROM cnt`},
}

// === Set Operations ===

var specSetOps = []specCase{
	// docs/stable/sql/query_syntax/setops.md
	{name: "union", sql: `SELECT * FROM range(2) t1(x) UNION SELECT * FROM range(3) t2(x)`},
	{name: "union_all", sql: `SELECT * FROM range(2) t1(x) UNION ALL SELECT * FROM range(3) t2(x)`},
	{name: "union_by_name", sql: `SELECT * FROM capitals UNION BY NAME SELECT * FROM weather`},
	{name: "intersect", sql: `SELECT * FROM range(2) t1(x) INTERSECT SELECT * FROM range(6) t2(x)`},
	{name: "intersect_all", sql: `SELECT unnest([5, 5, 6, 6, 6, 6, 7, 8]) AS x INTERSECT ALL SELECT unnest([5, 6, 6, 7, 7, 9])`},
	{name: "except", sql: `SELECT * FROM range(5) t1(x) EXCEPT SELECT * FROM range(2) t2(x)`},
	{name: "except_all", sql: `SELECT unnest([5, 5, 6, 6, 6, 6, 7, 8]) AS x EXCEPT ALL SELECT unnest([5, 6, 6, 7, 7, 9])`},
}

// === VALUES ===

var specValues = []specCase{
	// docs/stable/sql/query_syntax/values.md
	{name: "standalone_values", sql: `VALUES ('Amsterdam', 1), ('London', 2)`},
	{name: "values_in_from", sql: `SELECT * FROM (VALUES ('Amsterdam', 1), ('London', 2)) cities(name, id)`},
}

// === FILTER ===

var specFilter = []specCase{
	// docs/stable/sql/query_syntax/filter.md
	{name: "filter_count", sql: `SELECT count() FILTER (i <= 5) FROM generate_series(1, 10) tbl(i)`},
	{name: "filter_sum", sql: `SELECT sum(i) FILTER (i <= 5) FROM generate_series(1, 10) tbl(i)`},
	{name: "filter_multiple", sql: `SELECT count() FILTER (i <= 5) AS lte_five, count() FILTER (i % 2 = 1) AS odds FROM generate_series(1, 10) tbl(i)`},
}

// === Prepared Statements ===

var specPrepared = []specCase{
	// docs/stable/sql/query_syntax/prepared_statements.md
	{name: "param_question_mark", sql: `SELECT * FROM person WHERE name = ? AND age >= ?`},
	{name: "param_positional", sql: `SELECT * FROM person WHERE starts_with(name, $2) AND age >= $1`},
	{name: "param_named", sql: `SELECT * FROM person WHERE starts_with(name, $name_start) AND age >= $min_age`},
}

// === INSERT ===

var specInsert = []specCase{
	// docs/stable/sql/statements/insert.md
	{name: "insert_values", sql: `INSERT INTO tbl VALUES (1), (2), (3)`},
	{name: "insert_select", sql: `INSERT INTO tbl SELECT * FROM other_tbl`},
	{name: "insert_columns", sql: `INSERT INTO tbl (i) VALUES (1), (2), (3)`},
	{name: "insert_default", sql: `INSERT INTO tbl (i) VALUES (1), (DEFAULT), (3)`},
	{name: "insert_or_ignore", sql: `INSERT OR IGNORE INTO tbl (i) VALUES (1)`},
	{name: "insert_or_replace", sql: `INSERT OR REPLACE INTO tbl (i) VALUES (1)`},
	{name: "insert_by_name", sql: `INSERT INTO tbl BY NAME (SELECT 42 AS b, 32 AS a)`},
	{name: "insert_by_position", sql: `INSERT INTO tbl BY POSITION VALUES (5, 42)`},
	{name: "insert_returning", sql: `INSERT INTO t1 SELECT 42 RETURNING *`},
	{name: "insert_returning_expr", sql: `INSERT INTO t2 SELECT 2 AS i, 3 AS j RETURNING *, i * j AS product`},
	{name: "on_conflict_nothing", sql: `INSERT INTO tbl VALUES (1, 42) ON CONFLICT DO NOTHING`},
	{name: "on_conflict_update", sql: `INSERT INTO tbl VALUES (1, 84) ON CONFLICT DO UPDATE SET j = EXCLUDED.j`},
	{name: "on_conflict_column", sql: `INSERT INTO tbl VALUES (1, 40, 700) ON CONFLICT (i) DO UPDATE SET k = 2 * EXCLUDED.k`},
	{name: "on_conflict_where", sql: `INSERT INTO tbl VALUES (1, 40, 700) ON CONFLICT (i) DO UPDATE SET k = 2 * EXCLUDED.k WHERE k < 100`},
}

// === UPDATE ===

var specUpdate = []specCase{
	// docs/stable/sql/statements/update.md
	{name: "update_basic", sql: `UPDATE tbl SET i = 0 WHERE i IS NULL`},
	{name: "update_multi_column", sql: `UPDATE tbl SET i = 1, j = 2`},
	{name: "update_from", sql: `UPDATE original SET value = new.value FROM new WHERE original.key = new.key`},
	{name: "update_correlated", sql: `UPDATE original SET value = (SELECT new.value FROM new WHERE original.key = new.key)`},
	{name: "update_from_join", sql: `UPDATE city SET revenue = revenue + 100 FROM country WHERE city.country_code = country.code AND country.name = 'France'`},
}

// === DELETE ===

var specDelete = []specCase{
	// docs/stable/sql/statements/delete.md
	{name: "delete_where", sql: `DELETE FROM tbl WHERE i = 2`},
	{name: "delete_all", sql: `DELETE FROM tbl`},
	{name: "delete_returning", sql: `DELETE FROM employees RETURNING name, 2025 - age AS approx_birthyear`},
	{name: "delete_using", sql: `DELETE FROM tbl USING other WHERE tbl.id = other.id`},
}

// === MERGE ===

var specMerge = []specCase{
	// docs/stable/sql/statements/merge_into.md
	{name: "merge_upsert", sql: `MERGE INTO people USING (SELECT 3 AS id, 'Sarah' AS name, 95000.0 AS salary) AS src ON (src.id = people.id) WHEN MATCHED THEN UPDATE SET name = src.name, salary = src.salary WHEN NOT MATCHED THEN INSERT VALUES (src.id, src.name, src.salary)`},
	{name: "merge_delete", sql: `MERGE INTO people USING (SELECT 1 AS id) AS deletes ON (deletes.id = people.id) WHEN MATCHED THEN DELETE`},
	{name: "merge_conditional", sql: `MERGE INTO people USING (SELECT 2 AS id) AS src ON (src.id = people.id) WHEN MATCHED AND people.salary >= 100000 THEN DELETE WHEN MATCHED THEN UPDATE SET salary = salary + 1000`},
}

// === DDL (classification + raw pass-through) ===

var specDDL = []specCase{
	// docs/stable/sql/statements/create_table.md + create_view.md + etc.
	{name: "create_table", sql: `CREATE TABLE t1 (i INTEGER, j INTEGER)`},
	{name: "create_table_pk", sql: `CREATE TABLE t1 (id INTEGER PRIMARY KEY, j VARCHAR)`},
	{name: "create_table_composite_pk", sql: `CREATE TABLE t1 (id INTEGER, j VARCHAR, PRIMARY KEY (id, j))`},
	{name: "create_table_constraints", sql: `CREATE TABLE t1 (i INTEGER NOT NULL DEFAULT 0, d DOUBLE CHECK (d < 10), dt DATE UNIQUE, ts TIMESTAMP)`},
	{name: "create_table_as", sql: `CREATE TABLE t1 AS SELECT 42 AS i, 84 AS j`},
	{name: "create_table_as_from", sql: `CREATE TABLE t1 AS FROM read_csv('path/file.csv')`},
	{name: "create_temp_table", sql: `CREATE TEMP TABLE t1 AS SELECT * FROM read_csv('path/file.csv')`},
	{name: "create_or_replace_table", sql: `CREATE OR REPLACE TABLE t1 (i INTEGER, j INTEGER)`},
	{name: "create_if_not_exists", sql: `CREATE TABLE IF NOT EXISTS t1 (i INTEGER, j INTEGER)`},
	{name: "create_table_foreign_key", sql: `CREATE TABLE t2 (id INTEGER PRIMARY KEY, t1_id INTEGER, FOREIGN KEY (t1_id) REFERENCES t1 (id))`},
	{name: "create_table_generated", sql: `CREATE TABLE t1 (x FLOAT, two_x AS (2 * x))`},
	{name: "create_table_generated_full", sql: `CREATE TABLE t1 (x FLOAT, two_x FLOAT GENERATED ALWAYS AS (2 * x) VIRTUAL)`},
	{name: "create_view", sql: `CREATE VIEW v1 AS SELECT * FROM tbl`},
	{name: "create_or_replace_view", sql: `CREATE OR REPLACE VIEW v1 AS SELECT 42`},
	{name: "create_view_columns", sql: `CREATE VIEW v1(a) AS SELECT 42`},
	{name: "create_schema", sql: `CREATE SCHEMA s1`},
	{name: "create_schema_if_not_exists", sql: `CREATE SCHEMA IF NOT EXISTS s2`},
	{name: "create_unique_index", sql: `CREATE UNIQUE INDEX films_id_idx ON films (id)`},
	{name: "create_index", sql: `CREATE INDEX s_idx ON films (revenue)`},
	{name: "create_index_compound", sql: `CREATE INDEX gy_idx ON films (genre, year)`},
	{name: "create_index_expr", sql: `CREATE INDEX i_idx ON integers ((j + k))`},
	{name: "drop_table", sql: `DROP TABLE tbl`},
	{name: "drop_view_if_exists", sql: `DROP VIEW IF EXISTS v1`},
	{name: "drop_schema_cascade", sql: `DROP SCHEMA myschema CASCADE`},
	{name: "alter_add_column", sql: `ALTER TABLE integers ADD COLUMN k INTEGER`},
	{name: "alter_drop_column", sql: `ALTER TABLE integers DROP COLUMN k`},
	{name: "alter_type", sql: `ALTER TABLE integers ALTER i TYPE VARCHAR`},
	{name: "alter_set_default", sql: `ALTER TABLE integers ALTER COLUMN i SET DEFAULT 10`},
	{name: "alter_drop_default", sql: `ALTER TABLE integers ALTER COLUMN i DROP DEFAULT`},
	{name: "alter_set_not_null", sql: `ALTER TABLE integers ALTER COLUMN i SET NOT NULL`},
	{name: "alter_rename_table", sql: `ALTER TABLE integers RENAME TO integers_old`},
	{name: "alter_rename_column", sql: `ALTER TABLE integers RENAME i TO ii`},
	{name: "truncate", sql: `TRUNCATE tbl`},
}

// === CREATE MACRO ===

var specMacro = []specCase{
	// docs/stable/sql/statements/create_macro.md
	{name: "scalar_macro", sql: `CREATE MACRO add(a, b) AS a + b`},
	{name: "macro_or_replace", sql: `CREATE OR REPLACE MACRO add(a, b) AS a + b`},
	{name: "macro_if_not_exists", sql: `CREATE MACRO IF NOT EXISTS add(a, b) AS a + b`},
	{name: "macro_default_param", sql: `CREATE MACRO add_default(a, b := 5) AS a + b`},
	{name: "macro_case", sql: `CREATE MACRO ifelse(a, b, c) AS CASE WHEN a THEN b ELSE c END`},
	{name: "macro_subquery", sql: `CREATE MACRO one() AS (SELECT 1)`},
	{name: "function_alias", sql: `CREATE FUNCTION main.my_avg(x) AS sum(x) / count(x)`},
	{name: "table_macro", sql: `CREATE MACRO static_table() AS TABLE SELECT 'Hello' AS column1, 'World' AS column2`},
	{name: "table_macro_params", sql: `CREATE MACRO dynamic_table(col1_value, col2_value) AS TABLE SELECT col1_value AS column1, col2_value AS column2`},
}

// === CREATE SEQUENCE ===

var specSequence = []specCase{
	// docs/stable/sql/statements/create_sequence.md
	{name: "create_sequence", sql: `CREATE SEQUENCE serial`},
	{name: "sequence_start", sql: `CREATE SEQUENCE serial START 101`},
	{name: "sequence_start_increment", sql: `CREATE SEQUENCE serial START WITH 1 INCREMENT BY 2`},
	{name: "sequence_desc", sql: `CREATE SEQUENCE serial START WITH 99 INCREMENT BY -1 MAXVALUE 99`},
	{name: "sequence_cycle", sql: `CREATE SEQUENCE serial START WITH 1 MAXVALUE 10 CYCLE`},
	{name: "drop_sequence", sql: `DROP SEQUENCE serial`},
}

// === CREATE TYPE ===

var specType = []specCase{
	// docs/stable/sql/statements/create_type.md
	{name: "enum_type", sql: `CREATE TYPE mood AS ENUM ('happy', 'sad', 'curious')`},
	{name: "struct_type", sql: `CREATE TYPE many_things AS STRUCT(k INTEGER, l VARCHAR)`},
	{name: "union_type", sql: `CREATE TYPE one_thing AS UNION(number INTEGER, string VARCHAR)`},
	{name: "type_alias", sql: `CREATE TYPE x_index AS INTEGER`},
}

// === PIVOT ===

var specPivot = []specCase{
	// docs/stable/sql/statements/pivot.md
	{name: "pivot_basic", sql: `PIVOT cities ON year USING sum(population)`},
	{name: "pivot_group_by", sql: `PIVOT cities ON year USING sum(population) GROUP BY country`},
	{name: "pivot_in_filter", sql: `PIVOT cities ON year IN (2000, 2010) USING sum(population) GROUP BY country`},
	{name: "pivot_multi_on", sql: `PIVOT cities ON country, name USING sum(population)`},
	{name: "pivot_multi_agg", sql: `PIVOT cities ON year USING sum(population) AS total, max(population) AS max GROUP BY country`},
	{name: "pivot_sql_standard", sql: `SELECT * FROM cities PIVOT (sum(population) FOR year IN (2000, 2010, 2020) GROUP BY country)`},
}

// === UNPIVOT ===

var specUnpivot = []specCase{
	// docs/stable/sql/statements/unpivot.md
	{name: "unpivot_basic", sql: `UNPIVOT monthly_sales ON jan, feb, mar, apr, may, jun INTO NAME month VALUE sales`},
	{name: "unpivot_columns", sql: `UNPIVOT monthly_sales ON COLUMNS(* EXCLUDE (empid, dept)) INTO NAME month VALUE sales`},
	{name: "unpivot_multi_value", sql: `UNPIVOT monthly_sales ON (jan, feb, mar) AS q1, (apr, may, jun) AS q2 INTO NAME quarter VALUE month_1_sales, month_2_sales, month_3_sales`},
	{name: "unpivot_sql_standard", sql: `FROM monthly_sales UNPIVOT (sales FOR month IN (jan, feb, mar, apr, may, jun))`},
}

// === COPY ===

var specCopy = []specCase{
	// docs/stable/sql/statements/copy.md
	{name: "copy_from_csv", sql: `COPY lineitem FROM 'lineitem.csv'`},
	{name: "copy_from_delimiter", sql: `COPY lineitem FROM 'lineitem.csv' (DELIMITER '|')`},
	{name: "copy_from_parquet", sql: `COPY lineitem FROM 'lineitem.pq' (FORMAT parquet)`},
	{name: "copy_to_csv", sql: `COPY lineitem TO 'lineitem.csv' (FORMAT csv, DELIMITER '|', HEADER)`},
	{name: "copy_query_to", sql: `COPY (SELECT l_orderkey, l_partkey FROM lineitem) TO 'lineitem.parquet' (COMPRESSION zstd)`},
	{name: "copy_from_database", sql: `COPY FROM DATABASE db1 TO db2`},
}

// === ATTACH / DETACH ===

var specAttach = []specCase{
	// docs/stable/sql/statements/attach.md
	{name: "attach_basic", sql: `ATTACH 'file.db'`},
	{name: "attach_alias", sql: `ATTACH 'file.db' AS file_db`},
	{name: "attach_readonly", sql: `ATTACH 'file.db' (READ_ONLY)`},
	{name: "attach_type", sql: `ATTACH 'sqlite_file.db' AS sqlite_db (TYPE sqlite)`},
	{name: "attach_if_not_exists", sql: `ATTACH IF NOT EXISTS 'file.db'`},
	{name: "detach", sql: `DETACH file`},
}

// === COMMENT ON ===

var specCommentOn = []specCase{
	// docs/stable/sql/statements/comment_on.md
	{name: "comment_table", sql: `COMMENT ON TABLE test_table IS 'very nice table'`},
	{name: "comment_column", sql: `COMMENT ON COLUMN test_table.test_column IS 'very nice column'`},
	{name: "comment_view", sql: `COMMENT ON VIEW test_view IS 'very nice view'`},
	{name: "comment_index", sql: `COMMENT ON INDEX test_index IS 'very nice index'`},
	{name: "comment_null", sql: `COMMENT ON TABLE test_table IS NULL`},
}

// === EXPORT / IMPORT DATABASE ===

var specExport = []specCase{
	// docs/stable/sql/statements/export.md
	{name: "export_database", sql: `EXPORT DATABASE 'target_directory'`},
	{name: "export_parquet", sql: `EXPORT DATABASE 'target_directory' (FORMAT parquet)`},
	{name: "import_database", sql: `IMPORT DATABASE 'source_directory'`},
}

// === Utility Statements ===

var specUtility = []specCase{
	{name: "set_variable", sql: `SET memory_limit = '10GB'`},
	{name: "reset_variable", sql: `RESET memory_limit`},
	{name: "describe_table", sql: `DESCRIBE tbl`},
	{name: "show_tables", sql: `SHOW TABLES`},
	{name: "show_databases", sql: `SHOW DATABASES`},
	{name: "explain", sql: `EXPLAIN SELECT * FROM tbl`},
	{name: "explain_analyze", sql: `EXPLAIN ANALYZE SELECT * FROM tbl`},
	{name: "summarize", sql: `SUMMARIZE tbl`},
	{name: "begin_transaction", sql: `BEGIN TRANSACTION`},
	{name: "commit", sql: `COMMIT`},
	{name: "rollback", sql: `ROLLBACK`},
	{name: "vacuum", sql: `VACUUM`},
	{name: "checkpoint", sql: `CHECKPOINT`},
	{name: "call", sql: `CALL pragma_version()`},
	{name: "use_database", sql: `USE file`},
	{name: "use_schema", sql: `USE new_db.my_schema`},
	{name: "install", sql: `INSTALL httpfs`},
	{name: "load", sql: `LOAD httpfs`},
	{name: "prepare", sql: `PREPARE query_person AS SELECT * FROM person WHERE name = ? AND age >= ?`},
	{name: "execute", sql: `EXECUTE query_person('B', 40)`},
	{name: "deallocate", sql: `DEALLOCATE query_person`},
}

// === Data Types (in CAST expressions) ===

var specDataTypes = []specCase{
	// docs/stable/sql/data_types/
	{name: "cast_integer", sql: `SELECT CAST(x AS INTEGER) FROM t`},
	{name: "cast_bigint", sql: `SELECT CAST(x AS BIGINT) FROM t`},
	{name: "cast_hugeint", sql: `SELECT CAST(x AS HUGEINT) FROM t`},
	{name: "cast_double", sql: `SELECT CAST(x AS DOUBLE) FROM t`},
	{name: "cast_float", sql: `SELECT CAST(x AS FLOAT) FROM t`},
	{name: "cast_decimal", sql: `SELECT CAST(x AS DECIMAL(18, 3)) FROM t`},
	{name: "cast_varchar", sql: `SELECT CAST(x AS VARCHAR) FROM t`},
	{name: "cast_varchar_n", sql: `SELECT CAST(x AS VARCHAR(255)) FROM t`},
	{name: "cast_boolean", sql: `SELECT CAST(x AS BOOLEAN) FROM t`},
	{name: "cast_date", sql: `SELECT CAST(x AS DATE) FROM t`},
	{name: "cast_timestamp", sql: `SELECT CAST(x AS TIMESTAMP) FROM t`},
	{name: "cast_timestamp_tz", sql: `SELECT CAST(x AS TIMESTAMP WITH TIME ZONE) FROM t`},
	{name: "cast_time", sql: `SELECT CAST(x AS TIME) FROM t`},
	{name: "cast_interval", sql: `SELECT CAST(x AS INTERVAL) FROM t`},
	{name: "cast_blob", sql: `SELECT CAST(x AS BLOB) FROM t`},
	{name: "cast_uuid", sql: `SELECT CAST(x AS UUID) FROM t`},
	{name: "cast_json", sql: `SELECT CAST(x AS JSON) FROM t`},
	{name: "cast_struct", sql: `SELECT CAST(x AS STRUCT(a INTEGER, b VARCHAR)) FROM t`},
	{name: "cast_list", sql: `SELECT CAST(x AS INTEGER[]) FROM t`},
	{name: "cast_list_alt", sql: `SELECT CAST(x AS LIST(INTEGER)) FROM t`},
	{name: "cast_map", sql: `SELECT CAST(x AS MAP(VARCHAR, INTEGER)) FROM t`},
	{name: "cast_union", sql: `SELECT CAST(x AS UNION(num INTEGER, str VARCHAR)) FROM t`},
	{name: "cast_enum", sql: `SELECT CAST(x AS mood) FROM t`},
	{name: "cast_array_fixed", sql: `SELECT CAST(x AS INTEGER[3]) FROM t`},
	{name: "cast_tinyint", sql: `SELECT CAST(x AS TINYINT) FROM t`},
	{name: "cast_smallint", sql: `SELECT CAST(x AS SMALLINT) FROM t`},
	{name: "cast_uinteger", sql: `SELECT CAST(x AS UINTEGER) FROM t`},
	{name: "cast_ubigint", sql: `SELECT CAST(x AS UBIGINT) FROM t`},
	{name: "double_precision", sql: `SELECT CAST(x AS DOUBLE PRECISION) FROM t`},
}

// === Friendly SQL / Dialect ===

var specFriendlySQL = []specCase{
	// docs/stable/sql/dialect/friendly_sql.md
	{name: "from_first_select", sql: `FROM tbl SELECT *`},
	{name: "from_first_implicit_star", sql: `FROM tbl`},
	{name: "select_struct_star", sql: `SELECT st.* FROM (SELECT {'x': 1, 'y': 2} AS st)`},
	{name: "string_as_table", sql: `SELECT * FROM 'test.csv'`},
}

// ===========================================================================
// Test runners
// ===========================================================================

// allGroups returns the complete list of test groups for both test functions.
func allGroups() []struct {
	group string
	cases []specCase
} {
	return []struct {
		group string
		cases []specCase
	}{
		// Expressions
		{"expr/comparison", specExprComparison},
		{"expr/logical", specExprLogical},
		{"expr/arithmetic", specExprArithmetic},
		{"expr/bitwise", specExprBitwise},
		{"expr/case", specExprCase},
		{"expr/cast", specExprCast},
		{"expr/in", specExprIn},
		{"expr/star", specExprStar},
		{"expr/subquery", specExprSubquery},
		{"expr/collation", specExprCollation},
		{"expr/try", specExprTry},
		{"expr/like", specExprLike},
		{"expr/misc", specExprMisc},
		{"expr/duckdb", specExprDuckDB},
		// Query syntax
		{"select", specSelect},
		{"from", specFrom},
		{"where", specWhere},
		{"group_by", specGroupBy},
		{"grouping_sets", specGroupingSets},
		{"having", specHaving},
		{"order_by", specOrderBy},
		{"limit", specLimit},
		{"sample", specSample},
		{"qualify", specQualify},
		{"window", specWindow},
		{"with", specWith},
		{"set_ops", specSetOps},
		{"values", specValues},
		{"filter", specFilter},
		{"prepared", specPrepared},
		// DML
		{"insert", specInsert},
		{"update", specUpdate},
		{"delete", specDelete},
		{"merge", specMerge},
		// DDL
		{"ddl", specDDL},
		{"macro", specMacro},
		{"sequence", specSequence},
		{"type", specType},
		// Statements
		{"pivot", specPivot},
		{"unpivot", specUnpivot},
		{"copy", specCopy},
		{"attach", specAttach},
		{"comment_on", specCommentOn},
		{"export", specExport},
		{"utility", specUtility},
		// Data types
		{"data_types", specDataTypes},
		// Friendly SQL
		{"friendly_sql", specFriendlySQL},
	}
}

func TestDuckDBSpec_Parse(t *testing.T) {
	for _, g := range allGroups() {
		t.Run(g.group, func(t *testing.T) {
			for _, tc := range g.cases {
				t.Run(tc.name, func(t *testing.T) {
					_, err := Parse(tc.sql)
					require.NoError(t, err, "Parse(%q) failed", tc.sql)
				})
			}
		})
	}
}

func TestDuckDBSpec_RoundTrip(t *testing.T) {
	for _, g := range allGroups() {
		t.Run(g.group, func(t *testing.T) {
			for _, tc := range g.cases {
				t.Run(tc.name, func(t *testing.T) {
					stmt, err := Parse(tc.sql)
					if err != nil {
						t.Skipf("Parse failed (covered by Parse test): %v", err)
						return
					}

					formatted := Format(stmt)
					assert.NotEmpty(t, formatted, "Format produced empty string for %q", tc.sql)

					// Re-parse the formatted output
					_, err = Parse(formatted)
					require.NoError(t, err, "Re-parse of formatted SQL failed.\nOriginal: %s\nFormatted: %s", tc.sql, formatted)
				})
			}
		})
	}
}
