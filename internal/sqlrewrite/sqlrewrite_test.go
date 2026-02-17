package sqlrewrite

import (
	"sort"
	"strings"
	"testing"
)

// --- ExtractTableNames tests ---

func TestExtractTableNames_SimpleSelect(t *testing.T) {
	tables, err := ExtractTableNames("SELECT * FROM titanic")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertTables(t, tables, []string{"titanic"})
}

func TestExtractTableNames_MultipleFrom(t *testing.T) {
	tables, err := ExtractTableNames("SELECT * FROM titanic, passengers")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertTables(t, tables, []string{"titanic", "passengers"})
}

func TestExtractTableNames_Join(t *testing.T) {
	tables, err := ExtractTableNames("SELECT * FROM titanic t JOIN cabins c ON t.id = c.passenger_id")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertTables(t, tables, []string{"titanic", "cabins"})
}

func TestExtractTableNames_Subquery(t *testing.T) {
	tables, err := ExtractTableNames("SELECT * FROM (SELECT * FROM titanic) sub")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertTables(t, tables, []string{"titanic"})
}

func TestExtractTableNames_SubqueryInWhere(t *testing.T) {
	tables, err := ExtractTableNames("SELECT * FROM titanic WHERE id IN (SELECT passenger_id FROM bookings)")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertTables(t, tables, []string{"titanic", "bookings"})
}

func TestExtractTableNames_Union(t *testing.T) {
	tables, err := ExtractTableNames("SELECT * FROM titanic UNION ALL SELECT * FROM passengers")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertTables(t, tables, []string{"titanic", "passengers"})
}

func TestExtractTableNames_CTE(t *testing.T) {
	tables, err := ExtractTableNames("WITH cte AS (SELECT * FROM titanic) SELECT * FROM cte")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertContains(t, tables, "titanic")
}

func TestExtractTableNames_Deduplication(t *testing.T) {
	tables, err := ExtractTableNames("SELECT * FROM titanic t1 JOIN titanic t2 ON t1.id = t2.id")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertTables(t, tables, []string{"titanic"})
}

func TestExtractTableNames_QuotedIdentifiers(t *testing.T) {
	tables, err := ExtractTableNames(`SELECT "PassengerId" FROM "titanic"`)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertTables(t, tables, []string{"titanic"})
}

func TestExtractTableRefs_QualifiedNames(t *testing.T) {
	refs, err := ExtractTableRefs("SELECT * FROM analytics.events e JOIN main.users u ON e.user_id = u.id")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	got := make([]string, 0, len(refs))
	for _, ref := range refs {
		got = append(got, ref.Identifier())
	}
	assertTables(t, got, []string{"analytics.events", "main.users"})
}

func TestExtractTableNames_InvalidSQL(t *testing.T) {
	_, err := ExtractTableNames("SELEKT * FORM titanic")
	if err == nil {
		t.Error("expected error for invalid SQL")
	}
}

func TestExtractTableRefs_QualifiedThreePart(t *testing.T) {
	refs, err := ExtractTableRefs("SELECT * FROM demo.titanic.passengers")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(refs) != 1 {
		t.Fatalf("expected 1 ref, got %d", len(refs))
	}
	if refs[0].Catalog != "demo" || refs[0].Schema != "titanic" || refs[0].Name != "passengers" {
		t.Fatalf("unexpected ref: %+v", refs[0])
	}
}

func TestExtractTableRefs_SchemaQualified(t *testing.T) {
	refs, err := ExtractTableRefs("SELECT * FROM analytics.orders")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(refs) != 1 {
		t.Fatalf("expected 1 ref, got %d", len(refs))
	}
	if refs[0].Catalog != "" || refs[0].Schema != "analytics" || refs[0].Name != "orders" {
		t.Fatalf("unexpected ref: %+v", refs[0])
	}
}

// --- RewriteQuery tests (backward-compatible RLSRule API) ---

func TestRewriteQuery_NoRules(t *testing.T) {
	sql := "SELECT * FROM titanic"
	result, err := RewriteQuery(sql, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != sql {
		t.Errorf("expected unchanged query, got: %s", result)
	}
}

func TestRewriteQuery_SingleRule(t *testing.T) {
	sql := "SELECT * FROM titanic"
	rules := map[string][]RLSRule{
		"titanic": {
			{Table: "titanic", Column: "Pclass", Operator: OpEqual, Value: int64(1)},
		},
	}

	result, err := RewriteQuery(sql, rules)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	t.Logf("rewritten: %s", result)

	lower := strings.ToLower(result)
	if !strings.Contains(lower, "where") {
		t.Error("expected WHERE clause in rewritten query")
	}
	if !strings.Contains(result, "Pclass") {
		t.Error("expected Pclass in rewritten query")
	}
}

func TestRewriteQuery_MultipleRules(t *testing.T) {
	sql := "SELECT * FROM titanic"
	rules := map[string][]RLSRule{
		"titanic": {
			{Table: "titanic", Column: "Pclass", Operator: OpEqual, Value: int64(1)},
			{Table: "titanic", Column: "Survived", Operator: OpEqual, Value: int64(1)},
		},
	}

	result, err := RewriteQuery(sql, rules)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	t.Logf("rewritten: %s", result)

	if !strings.Contains(result, "Pclass") {
		t.Error("expected Pclass in rewritten query")
	}
	if !strings.Contains(result, "Survived") {
		t.Error("expected Survived in rewritten query")
	}
}

func TestRewriteQuery_PreservesExistingWhere(t *testing.T) {
	sql := `SELECT * FROM titanic WHERE "Sex" = 'male'`
	rules := map[string][]RLSRule{
		"titanic": {
			{Table: "titanic", Column: "Pclass", Operator: OpEqual, Value: int64(1)},
		},
	}

	result, err := RewriteQuery(sql, rules)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	t.Logf("rewritten: %s", result)

	if !strings.Contains(result, "Pclass") {
		t.Error("expected Pclass in rewritten query")
	}
	lower := strings.ToLower(result)
	if !strings.Contains(lower, "sex") {
		t.Error("expected original Sex condition to be preserved")
	}
}

func TestRewriteQuery_WithLimit(t *testing.T) {
	sql := "SELECT * FROM titanic LIMIT 10"
	rules := map[string][]RLSRule{
		"titanic": {
			{Table: "titanic", Column: "Pclass", Operator: OpEqual, Value: int64(1)},
		},
	}

	result, err := RewriteQuery(sql, rules)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	t.Logf("rewritten: %s", result)

	lower := strings.ToLower(result)
	if !strings.Contains(lower, "limit") {
		t.Error("expected LIMIT to be preserved")
	}
	if !strings.Contains(result, "Pclass") {
		t.Error("expected Pclass filter")
	}
}

func TestRewriteQuery_WithOrderBy(t *testing.T) {
	sql := `SELECT * FROM titanic ORDER BY "Name" LIMIT 10`
	rules := map[string][]RLSRule{
		"titanic": {
			{Table: "titanic", Column: "Survived", Operator: OpEqual, Value: int64(1)},
		},
	}

	result, err := RewriteQuery(sql, rules)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	t.Logf("rewritten: %s", result)

	lower := strings.ToLower(result)
	if !strings.Contains(lower, "order by") {
		t.Error("expected ORDER BY to be preserved")
	}
}

func TestRewriteQuery_JoinWithRules(t *testing.T) {
	sql := "SELECT * FROM titanic t JOIN cabins c ON t.id = c.passenger_id"
	rules := map[string][]RLSRule{
		"titanic": {
			{Table: "titanic", Column: "Pclass", Operator: OpEqual, Value: int64(1)},
		},
	}

	result, err := RewriteQuery(sql, rules)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	t.Logf("rewritten: %s", result)

	if !strings.Contains(result, "Pclass") {
		t.Error("expected Pclass filter")
	}
}

func TestRewriteQuery_AllOperators(t *testing.T) {
	ops := []struct {
		op    string
		sqlOp string
	}{
		{OpEqual, "="},
		{OpNotEqual, "<>"},
		{OpLessThan, "<"},
		{OpLessEqual, "<="},
		{OpGreaterThan, ">"},
		{OpGreaterEqual, ">="},
	}

	for _, tc := range ops {
		t.Run(tc.op, func(t *testing.T) {
			sql := "SELECT * FROM titanic"
			rules := map[string][]RLSRule{
				"titanic": {
					{Table: "titanic", Column: "Age", Operator: tc.op, Value: int64(30)},
				},
			}

			result, err := RewriteQuery(sql, rules)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			t.Logf("rewritten (%s): %s", tc.op, result)

			if !strings.Contains(result, tc.sqlOp) {
				t.Errorf("expected operator %s in rewritten query", tc.sqlOp)
			}
		})
	}
}

func TestRewriteQuery_StringValue(t *testing.T) {
	sql := "SELECT * FROM titanic"
	rules := map[string][]RLSRule{
		"titanic": {
			{Table: "titanic", Column: "Sex", Operator: OpEqual, Value: "male"},
		},
	}

	result, err := RewriteQuery(sql, rules)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	t.Logf("rewritten: %s", result)

	if !strings.Contains(result, "'male'") {
		t.Error("expected string literal 'male' in rewritten query")
	}
}

func TestRewriteQuery_FloatValue(t *testing.T) {
	sql := "SELECT * FROM titanic"
	rules := map[string][]RLSRule{
		"titanic": {
			{Table: "titanic", Column: "Fare", Operator: OpGreaterThan, Value: float64(50.5)},
		},
	}

	result, err := RewriteQuery(sql, rules)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	t.Logf("rewritten: %s", result)

	if !strings.Contains(result, "50.5") {
		t.Error("expected float value 50.5 in rewritten query")
	}
}

func TestRewriteQuery_UnsupportedOperator(t *testing.T) {
	sql := "SELECT * FROM titanic"
	rules := map[string][]RLSRule{
		"titanic": {
			{Table: "titanic", Column: "Pclass", Operator: "invalid_op", Value: int64(1)},
		},
	}

	_, err := RewriteQuery(sql, rules)
	if err == nil {
		t.Error("expected error for unsupported operator")
	}
}

func TestRewriteQuery_UnsupportedValueType(t *testing.T) {
	sql := "SELECT * FROM titanic"
	rules := map[string][]RLSRule{
		"titanic": {
			{Table: "titanic", Column: "Pclass", Operator: OpEqual, Value: []int{1, 2}},
		},
	}

	_, err := RewriteQuery(sql, rules)
	if err == nil {
		t.Error("expected error for unsupported value type")
	}
}

func TestRewriteQuery_NoMatchingTable(t *testing.T) {
	sql := "SELECT * FROM titanic"
	rules := map[string][]RLSRule{
		"other_table": {
			{Table: "other_table", Column: "id", Operator: OpEqual, Value: int64(1)},
		},
	}

	result, err := RewriteQuery(sql, rules)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	t.Logf("result: %s", result)
}

func TestRewriteQuery_SelectedColumns(t *testing.T) {
	sql := `SELECT "PassengerId", "Name", "Pclass" FROM titanic LIMIT 5`
	rules := map[string][]RLSRule{
		"titanic": {
			{Table: "titanic", Column: "Pclass", Operator: OpEqual, Value: int64(1)},
		},
	}

	result, err := RewriteQuery(sql, rules)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	t.Logf("rewritten: %s", result)

	if !strings.Contains(result, "Pclass") {
		t.Error("expected Pclass filter")
	}
}

func TestRewriteQuery_InvalidSQL(t *testing.T) {
	_, err := RewriteQuery("SELEKT * FORM titanic", map[string][]RLSRule{
		"titanic": {
			{Table: "titanic", Column: "id", Operator: OpEqual, Value: int64(1)},
		},
	})
	if err == nil {
		t.Error("expected error for invalid SQL")
	}
}

func TestRewriteQuery_Union(t *testing.T) {
	sql := "SELECT * FROM titanic UNION ALL SELECT * FROM titanic"
	rules := map[string][]RLSRule{
		"titanic": {
			{Table: "titanic", Column: "Pclass", Operator: OpEqual, Value: int64(1)},
		},
	}

	result, err := RewriteQuery(sql, rules)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	t.Logf("rewritten: %s", result)

	count := strings.Count(result, "Pclass")
	if count < 2 {
		t.Errorf("expected Pclass filter in both UNION branches, found %d occurrences", count)
	}
}

// --- ClassifyStatement tests ---

func TestClassifyStatement_Select(t *testing.T) {
	typ, err := ClassifyStatement("SELECT * FROM titanic")
	if err != nil {
		t.Fatal(err)
	}
	if typ != StmtSelect {
		t.Errorf("expected SELECT, got %s", typ)
	}
}

func TestClassifyStatement_Insert(t *testing.T) {
	typ, err := ClassifyStatement("INSERT INTO titanic (id) VALUES (1)")
	if err != nil {
		t.Fatal(err)
	}
	if typ != StmtInsert {
		t.Errorf("expected INSERT, got %s", typ)
	}
}

func TestClassifyStatement_Update(t *testing.T) {
	typ, err := ClassifyStatement("UPDATE titanic SET name = 'test' WHERE id = 1")
	if err != nil {
		t.Fatal(err)
	}
	if typ != StmtUpdate {
		t.Errorf("expected UPDATE, got %s", typ)
	}
}

func TestClassifyStatement_Delete(t *testing.T) {
	typ, err := ClassifyStatement("DELETE FROM titanic WHERE id = 1")
	if err != nil {
		t.Fatal(err)
	}
	if typ != StmtDelete {
		t.Errorf("expected DELETE, got %s", typ)
	}
}

func TestClassifyStatement_DDL_Create(t *testing.T) {
	typ, err := ClassifyStatement("CREATE TABLE foo (id INT)")
	if err != nil {
		t.Fatal(err)
	}
	if typ != StmtDDL {
		t.Errorf("expected DDL, got %s", typ)
	}
}

func TestClassifyStatement_DDL_Drop(t *testing.T) {
	typ, err := ClassifyStatement("DROP TABLE titanic")
	if err != nil {
		t.Fatal(err)
	}
	if typ != StmtDDL {
		t.Errorf("expected DDL, got %s", typ)
	}
}

func TestClassifyStatement_MultiStatement(t *testing.T) {
	_, err := ClassifyStatement("SELECT 1; DROP TABLE titanic")
	if err == nil {
		t.Error("expected error for multi-statement SQL")
	}
	if err != nil && !strings.Contains(err.Error(), "multi-statement") {
		t.Errorf("expected multi-statement error, got: %v", err)
	}
}

func TestClassifyStatement_Invalid(t *testing.T) {
	_, err := ClassifyStatement("SELEKT * FORM titanic")
	if err == nil {
		t.Error("expected error for invalid SQL")
	}
}

func TestClassifyStatement_MultiStatementRejected(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		{"select_then_drop", "SELECT 1; DROP TABLE titanic"},
		{"select_then_insert", "SELECT 1; INSERT INTO titanic (id) VALUES (1)"},
		{"two_selects", "SELECT 1; SELECT 2"},
		{"select_then_delete", "SELECT * FROM titanic; DELETE FROM titanic WHERE id = 1"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ClassifyStatement(tc.sql)
			if err == nil {
				t.Error("expected error for multi-statement SQL")
			}
		})
	}
}

// --- InjectRowFilterSQL tests ---

func TestInjectRowFilterSQL_Basic(t *testing.T) {
	result, err := InjectRowFilterSQL(
		`SELECT * FROM titanic`,
		"titanic",
		`"Pclass" = 1`,
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("result: %s", result)

	lower := strings.ToLower(result)
	if !strings.Contains(lower, "where") {
		t.Error("expected WHERE clause")
	}
}

func TestInjectRowFilterSQL_PreservesExistingWhere(t *testing.T) {
	result, err := InjectRowFilterSQL(
		`SELECT * FROM titanic WHERE "Sex" = 'male'`,
		"titanic",
		`"Pclass" = 1`,
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("result: %s", result)

	lower := strings.ToLower(result)
	if !strings.Contains(lower, "sex") {
		t.Error("expected original WHERE to be preserved")
	}
	if !strings.Contains(lower, "pclass") {
		t.Error("expected injected filter")
	}
}

func TestInjectRowFilterSQL_NoMatchingTable(t *testing.T) {
	result, err := InjectRowFilterSQL(
		`SELECT * FROM other_table`,
		"titanic",
		`"Pclass" = 1`,
	)
	if err != nil {
		t.Fatal(err)
	}
	// Should not inject anything
	lower := strings.ToLower(result)
	if strings.Contains(lower, "where") {
		t.Error("should not inject filter for non-matching table")
	}
}

func TestInjectRowFilterSQL_EmptyFilter(t *testing.T) {
	sql := "SELECT * FROM titanic"
	result, err := InjectRowFilterSQL(sql, "titanic", "")
	if err != nil {
		t.Fatal(err)
	}
	if result != sql {
		t.Error("empty filter should return original SQL")
	}
}

func TestInjectRowFilterSQL_SelfJoin(t *testing.T) {
	result, err := InjectRowFilterSQL(
		`SELECT * FROM titanic t1 JOIN titanic t2 ON t1."PassengerId" = t2."PassengerId"`,
		"titanic",
		`"Pclass" = 1`,
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("result: %s", result)

	lower := strings.ToLower(result)
	if !strings.Contains(lower, "where") {
		t.Error("expected WHERE clause to be injected for self-join")
	}
	if !strings.Contains(result, "Pclass") {
		t.Error("expected Pclass filter in self-join query")
	}
}

func TestInjectRowFilterSQL_QualifiedTableName(t *testing.T) {
	result, err := InjectRowFilterSQL(
		`SELECT * FROM analytics.titanic`,
		"analytics.titanic",
		`"Pclass" = 1`,
	)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(strings.ToLower(result), "where") {
		t.Error("expected WHERE clause for qualified table filter")
	}
}

func TestInjectMultipleRowFilters_ANDComposition(t *testing.T) {
	result, err := InjectMultipleRowFilters(
		`SELECT * FROM titanic`,
		"titanic",
		[]string{`"Pclass" = 1`, `"Survived" = 1`},
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("result: %s", result)

	lower := strings.ToLower(result)
	if !strings.Contains(lower, "pclass") {
		t.Error("expected Pclass in AND-composed filter")
	}
	if !strings.Contains(lower, "survived") {
		t.Error("expected Survived in AND-composed filter")
	}
	// Filters should be combined with AND, so both conditions should appear.
	if !strings.Contains(lower, "and") {
		t.Error("expected AND in combined filter (two filters compose with AND)")
	}
}

// --- ApplyColumnMasks tests ---

func TestApplyColumnMasks_Basic(t *testing.T) {
	allCols := []string{"PassengerId", "Name", "Pclass"}
	result, err := ApplyColumnMasks(
		`SELECT "PassengerId", "Name", "Pclass" FROM titanic`,
		"titanic",
		map[string]string{"Name": "'***'"},
		allCols,
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("result: %s", result)

	if !strings.Contains(result, "'***'") {
		t.Error("expected mask expression in result")
	}
	// The original "Name" column reference should be replaced
	if !strings.Contains(result, "PassengerId") {
		t.Error("expected non-masked columns to be preserved")
	}
}

func TestApplyColumnMasks_SelectStar(t *testing.T) {
	allCols := []string{"PassengerId", "Name", "Pclass"}
	result, err := ApplyColumnMasks(
		`SELECT * FROM titanic`,
		"titanic",
		map[string]string{"Name": "'***'"},
		allCols,
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("result: %s", result)

	if !strings.Contains(result, "'***'") {
		t.Error("expected mask expression in SELECT * result")
	}
	if !strings.Contains(result, "PassengerId") {
		t.Error("expected non-masked columns to be expanded from *")
	}
	if !strings.Contains(result, "Pclass") {
		t.Error("expected non-masked columns to be expanded from *")
	}
}

func TestApplyColumnMasks_SelectStarNoColumns(t *testing.T) {
	// SELECT * with masks but no column metadata should return an error
	_, err := ApplyColumnMasks(
		`SELECT * FROM titanic`,
		"titanic",
		map[string]string{"Name": "'***'"},
		nil,
	)
	if err == nil {
		t.Error("expected error when masking SELECT * without column metadata")
	}
}

func TestApplyColumnMasks_UnparseableMask(t *testing.T) {
	allCols := []string{"PassengerId", "Name"}
	_, err := ApplyColumnMasks(
		`SELECT "PassengerId", "Name" FROM titanic`,
		"titanic",
		map[string]string{"Name": "INVALID MASK ((("},
		allCols,
	)
	if err == nil {
		t.Error("expected error for unparseable mask expression")
	}
}

func TestApplyColumnMasks_NoMasks(t *testing.T) {
	sql := `SELECT "Name" FROM titanic`
	result, err := ApplyColumnMasks(sql, "titanic", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if result != sql {
		t.Error("nil masks should return original SQL")
	}
}

func TestApplyColumnMasks_NoMatchingTable(t *testing.T) {
	sql := `SELECT "Name" FROM other_table`
	result, err := ApplyColumnMasks(sql, "titanic", map[string]string{"Name": "'***'"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	// Should not modify
	if strings.Contains(result, "'***'") {
		t.Error("should not mask columns for non-matching table")
	}
}

func TestApplyColumnMasks_MalformedExpressionErrors(t *testing.T) {
	_, err := ApplyColumnMasks(
		`SELECT "Name" FROM titanic`,
		"titanic",
		map[string]string{"Name": "INVALID SQL $$"},
		nil,
	)
	if err == nil {
		t.Error("expected error for malformed mask expression")
	}
}

func TestApplyColumnMasks_ValidExpressionSucceeds(t *testing.T) {
	result, err := ApplyColumnMasks(
		`SELECT "Name" FROM titanic`,
		"titanic",
		map[string]string{"Name": "'***'"},
		nil,
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(result, "'***'") {
		t.Error("expected mask expression in result")
	}
}

func TestApplyColumnMasks_Subquery(t *testing.T) {
	result, err := ApplyColumnMasks(
		`SELECT "Name" FROM (SELECT "Name" FROM titanic) sub`,
		"titanic",
		map[string]string{"Name": "'***'"},
		nil,
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	t.Logf("result: %s", result)
	if !strings.Contains(result, "'***'") {
		t.Error("expected mask to be applied inside subquery")
	}
}

func TestApplyColumnMasks_CTE(t *testing.T) {
	result, err := ApplyColumnMasks(
		`WITH cte AS (SELECT "Name" FROM titanic) SELECT "Name" FROM cte`,
		"titanic",
		map[string]string{"Name": "'***'"},
		nil,
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	t.Logf("result: %s", result)
	if !strings.Contains(result, "'***'") {
		t.Error("expected mask to be applied inside CTE")
	}
}

// --- Case-insensitive column mask lookup tests (Issue #48) ---

func TestApplyColumnMasks_CaseInsensitive(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		masks    map[string]string
		allCols  []string
		wantMask bool
	}{
		{
			name:     "mask on lowercase, query uses uppercase",
			sql:      `SELECT "Email" FROM titanic`,
			masks:    map[string]string{"email": "'***'"},
			wantMask: true,
		},
		{
			name:     "mask on lowercase, query uses mixed case",
			sql:      `SELECT "eMaIl" FROM titanic`,
			masks:    map[string]string{"email": "'***'"},
			wantMask: true,
		},
		{
			name:     "mask on lowercase, query uses lowercase",
			sql:      `SELECT email FROM titanic`,
			masks:    map[string]string{"email": "'***'"},
			wantMask: true,
		},
		{
			name:     "select star with case-insensitive mask",
			sql:      `SELECT * FROM titanic`,
			masks:    map[string]string{"name": "'***'"},
			allCols:  []string{"id", "Name", "email"},
			wantMask: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result, err := ApplyColumnMasks(tc.sql, "titanic", tc.masks, tc.allCols)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			t.Logf("result: %s", result)
			hasMask := strings.Contains(result, "'***'")
			if tc.wantMask && !hasMask {
				t.Error("expected mask expression in result, got none")
			}
			if !tc.wantMask && hasMask {
				t.Error("did not expect mask expression in result")
			}
		})
	}
}

// --- QuoteIdentifier tests ---

func TestQuoteIdentifier(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		// All identifiers are now unconditionally quoted
		{"simple", `"simple"`},
		{"with_underscore", `"with_underscore"`},
		{"MixedCase", `"MixedCase"`},
		{"has space", `"has space"`},
		{`has"quote`, `"has""quote"`},
		// SQL reserved words are now properly quoted
		{"select", `"select"`},
		{"table", `"table"`},
		{"order", `"order"`},
		{"group", `"group"`},
		{"user", `"user"`},
		{"from", `"from"`},
		{"where", `"where"`},
		// Empty string
		{"", `""`},
	}

	for _, tc := range tests {
		t.Run(tc.input, func(t *testing.T) {
			got := QuoteIdentifier(tc.input)
			if got != tc.expected {
				t.Errorf("QuoteIdentifier(%q) = %q, want %q", tc.input, got, tc.expected)
			}
		})
	}
}

// --- int32 overflow fix test ---

func TestMakeIntegerConst_LargeValue(t *testing.T) {
	sql := "SELECT * FROM titanic"
	rules := map[string][]RLSRule{
		"titanic": {
			{Table: "titanic", Column: "id", Operator: OpEqual, Value: int64(3000000000)},
		},
	}

	result, err := RewriteQuery(sql, rules)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	t.Logf("rewritten: %s", result)

	// Should contain the large value without overflow
	if !strings.Contains(result, "3000000000") {
		t.Error("expected large integer value to be preserved")
	}
}

// --- Helpers ---

func assertTables(t *testing.T, got, want []string) {
	t.Helper()
	sort.Strings(got)
	sort.Strings(want)
	if len(got) != len(want) {
		t.Errorf("tables: got %v, want %v", got, want)
		return
	}
	for i := range got {
		if got[i] != want[i] {
			t.Errorf("tables[%d]: got %q, want %q", i, got[i], want[i])
		}
	}
}

func assertContains(t *testing.T, tables []string, want string) {
	t.Helper()
	for _, tbl := range tables {
		if tbl == want {
			return
		}
	}
	t.Errorf("expected tables to contain %q, got: %v", want, tables)
}

// --- InjectRowFilterSQL for UPDATE/DELETE (Issue #40) ---

func TestInjectRowFilterSQL_Update(t *testing.T) {
	tests := []struct {
		name      string
		sql       string
		table     string
		filter    string
		wantWhere bool
	}{
		{
			name:      "update_no_existing_where",
			sql:       `UPDATE titanic SET "Name" = 'test'`,
			table:     "titanic",
			filter:    `"Pclass" = 1`,
			wantWhere: true,
		},
		{
			name:      "update_with_existing_where",
			sql:       `UPDATE titanic SET "Name" = 'test' WHERE "PassengerId" = 1`,
			table:     "titanic",
			filter:    `"Pclass" = 1`,
			wantWhere: true,
		},
		{
			name:      "update_wrong_table",
			sql:       `UPDATE other_table SET "Name" = 'test'`,
			table:     "titanic",
			filter:    `"Pclass" = 1`,
			wantWhere: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result, err := InjectRowFilterSQL(tc.sql, tc.table, tc.filter)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			t.Logf("result: %s", result)

			lower := strings.ToLower(result)
			hasPclass := strings.Contains(lower, "pclass")
			if tc.wantWhere && !hasPclass {
				t.Error("expected filter to be injected")
			}
			if !tc.wantWhere && hasPclass {
				t.Error("expected filter NOT to be injected for wrong table")
			}
		})
	}
}

func TestInjectRowFilterSQL_Delete(t *testing.T) {
	tests := []struct {
		name      string
		sql       string
		table     string
		filter    string
		wantWhere bool
	}{
		{
			name:      "delete_no_existing_where",
			sql:       `DELETE FROM titanic`,
			table:     "titanic",
			filter:    `"Pclass" = 1`,
			wantWhere: true,
		},
		{
			name:      "delete_with_existing_where",
			sql:       `DELETE FROM titanic WHERE "PassengerId" = 1`,
			table:     "titanic",
			filter:    `"Pclass" = 1`,
			wantWhere: true,
		},
		{
			name:      "delete_wrong_table",
			sql:       `DELETE FROM other_table WHERE id = 1`,
			table:     "titanic",
			filter:    `"Pclass" = 1`,
			wantWhere: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result, err := InjectRowFilterSQL(tc.sql, tc.table, tc.filter)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			t.Logf("result: %s", result)

			lower := strings.ToLower(result)
			hasPclass := strings.Contains(lower, "pclass")
			if tc.wantWhere && !hasPclass {
				t.Error("expected filter to be injected")
			}
			if !tc.wantWhere && hasPclass {
				t.Error("expected filter NOT to be injected for wrong table")
			}
		})
	}
}

func TestInjectRowFilterSQL_UpdatePreservesExistingWhere(t *testing.T) {
	result, err := InjectRowFilterSQL(
		`UPDATE titanic SET "Name" = 'test' WHERE "PassengerId" = 1`,
		"titanic",
		`"Pclass" = 1`,
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("result: %s", result)

	lower := strings.ToLower(result)
	if !strings.Contains(lower, "pclass") {
		t.Error("expected injected filter")
	}
	if !strings.Contains(lower, "passengerid") {
		t.Error("expected original WHERE to be preserved")
	}
}

func TestInjectMultipleRowFilters_Update(t *testing.T) {
	result, err := InjectMultipleRowFilters(
		`UPDATE titanic SET "Name" = 'test'`,
		"titanic",
		[]string{`"Pclass" = 1`, `"Survived" = 1`},
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("result: %s", result)

	lower := strings.ToLower(result)
	if !strings.Contains(lower, "pclass") {
		t.Error("expected Pclass in OR-composed filter")
	}
	if !strings.Contains(lower, "survived") {
		t.Error("expected Survived in OR-composed filter")
	}
}

// --- InjectRowFilterSQL for subqueries in FROM clause (Issue #47) ---

func TestInjectRowFilterSQL_SubqueryInFrom(t *testing.T) {
	result, err := InjectRowFilterSQL(
		`SELECT * FROM (SELECT * FROM titanic) sub`,
		"titanic",
		`"Pclass" = 1`,
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("result: %s", result)

	lower := strings.ToLower(result)
	if !strings.Contains(lower, "pclass") {
		t.Error("expected filter to be injected into subquery in FROM clause")
	}
}

func TestInjectRowFilterSQL_SubqueryInJoin(t *testing.T) {
	result, err := InjectRowFilterSQL(
		`SELECT * FROM orders o JOIN (SELECT * FROM titanic) sub ON o.id = sub."PassengerId"`,
		"titanic",
		`"Pclass" = 1`,
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("result: %s", result)

	lower := strings.ToLower(result)
	if !strings.Contains(lower, "pclass") {
		t.Error("expected filter to be injected into subquery inside JOIN")
	}
}

func TestInjectRowFilterSQL_NestedSubqueryInFrom(t *testing.T) {
	result, err := InjectRowFilterSQL(
		`SELECT * FROM (SELECT * FROM (SELECT * FROM titanic) inner_sub) outer_sub`,
		"titanic",
		`"Pclass" = 1`,
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("result: %s", result)

	lower := strings.ToLower(result)
	if !strings.Contains(lower, "pclass") {
		t.Error("expected filter to be injected into deeply nested subquery")
	}
}

// --- ExtractTargetTable tests ---

func TestExtractTargetTable(t *testing.T) {
	tests := []struct {
		name      string
		sql       string
		want      string
		wantError bool
	}{
		{
			name: "insert",
			sql:  "INSERT INTO orders (id, amount) VALUES (1, 100)",
			want: "orders",
		},
		{
			name: "update",
			sql:  "UPDATE users SET name = 'Alice' WHERE id = 1",
			want: "users",
		},
		{
			name: "delete",
			sql:  "DELETE FROM logs WHERE ts < '2024-01-01'",
			want: "logs",
		},
		{
			name: "select_returns_empty",
			sql:  "SELECT * FROM titanic",
			want: "",
		},
		{
			name: "create_table_returns_empty",
			sql:  "CREATE TABLE foo (id INT)",
			want: "",
		},
		{
			name: "empty_sql",
			sql:  "",
			want: "",
		},
		{
			name: "insert_with_schema_prefix",
			sql:  "INSERT INTO main.orders (id) VALUES (1)",
			want: "orders",
		},
		{
			name: "update_with_subquery",
			sql:  "UPDATE orders SET total = (SELECT SUM(amount) FROM items)",
			want: "orders",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ExtractTargetTable(tc.sql)
			if tc.wantError {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tc.want {
				t.Errorf("ExtractTargetTable(%q) = %q, want %q", tc.sql, got, tc.want)
			}
		})
	}
}

// --- Security: block dangerous DuckDB functions ---

func TestClassifyStatement_BlocksDangerousFunctions(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		{"read_csv_auto", "SELECT * FROM read_csv_auto('/etc/passwd')"},
		{"read_parquet", "SELECT * FROM read_parquet('/etc/shadow')"},
		{"read_json", "SELECT * FROM read_json('/etc/hosts')"},
		{"read_text", "SELECT read_text('/etc/passwd')"},
		{"glob", "SELECT * FROM glob('/tmp/*')"},
		{"sqlite_scan", "SELECT * FROM sqlite_scan('/etc/passwd', 'sqlite_master')"},
		{"read_blob", "SELECT * FROM read_blob('/etc/passwd')"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ClassifyStatement(tc.sql)
			if err == nil {
				t.Errorf("ClassifyStatement(%q): expected error for dangerous function, got nil", tc.sql)
			}
		})
	}
}

func TestClassifyStatement_BlocksMetadataLeaks(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		{"duckdb_extensions", "SELECT * FROM duckdb_extensions()"},
		{"duckdb_settings", "SELECT * FROM duckdb_settings()"},
		{"duckdb_databases", "SELECT * FROM duckdb_databases()"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ClassifyStatement(tc.sql)
			if err == nil {
				t.Errorf("ClassifyStatement(%q): expected error for metadata leak function, got nil", tc.sql)
			}
		})
	}
}

func TestExtractTableNames_DetectsFunctionCalls(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		{"read_csv_auto", "SELECT * FROM read_csv_auto('/etc/passwd')"},
		{"glob", "SELECT * FROM glob('/tmp/*')"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tables, err := ExtractTableNames(tc.sql)
			if err != nil {
				return // error is acceptable — means the function was detected
			}
			// If no error, the table list must NOT be empty, otherwise the
			// engine will treat this as a "table-less SELECT" and allow it
			// for all authenticated users, bypassing RBAC entirely.
			if len(tables) == 0 {
				t.Errorf("ExtractTableNames(%q): got empty table list — function-based FROM clause will bypass RBAC as table-less SELECT", tc.sql)
			}
		})
	}
}
