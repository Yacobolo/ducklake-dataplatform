package sqlrewrite

import (
	"sort"
	"strings"
	"testing"

	"duck-demo/policy"
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
	// CTEs: "cte" is in FROM but is virtual; "titanic" is the real table
	assertContains(t, tables, "titanic")
}

func TestExtractTableNames_Deduplication(t *testing.T) {
	tables, err := ExtractTableNames("SELECT * FROM titanic t1 JOIN titanic t2 ON t1.id = t2.id")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Should deduplicate
	assertTables(t, tables, []string{"titanic"})
}

func TestExtractTableNames_QuotedIdentifiers(t *testing.T) {
	tables, err := ExtractTableNames(`SELECT "PassengerId" FROM "titanic"`)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertTables(t, tables, []string{"titanic"})
}

func TestExtractTableNames_InvalidSQL(t *testing.T) {
	_, err := ExtractTableNames("SELEKT * FORM titanic")
	if err == nil {
		t.Error("expected error for invalid SQL")
	}
}

// --- RewriteQuery tests ---

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
	rules := map[string][]policy.RLSRule{
		"titanic": {
			{Table: "titanic", Column: "Pclass", Operator: policy.OpEqual, Value: int64(1)},
		},
	}

	result, err := RewriteQuery(sql, rules)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	t.Logf("rewritten: %s", result)

	// Should contain the WHERE clause with the filter
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
	rules := map[string][]policy.RLSRule{
		"titanic": {
			{Table: "titanic", Column: "Pclass", Operator: policy.OpEqual, Value: int64(1)},
			{Table: "titanic", Column: "Survived", Operator: policy.OpEqual, Value: int64(1)},
		},
	}

	result, err := RewriteQuery(sql, rules)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	t.Logf("rewritten: %s", result)

	// Should contain AND combining both conditions
	if !strings.Contains(result, "Pclass") {
		t.Error("expected Pclass in rewritten query")
	}
	if !strings.Contains(result, "Survived") {
		t.Error("expected Survived in rewritten query")
	}
}

func TestRewriteQuery_PreservesExistingWhere(t *testing.T) {
	sql := `SELECT * FROM titanic WHERE "Sex" = 'male'`
	rules := map[string][]policy.RLSRule{
		"titanic": {
			{Table: "titanic", Column: "Pclass", Operator: policy.OpEqual, Value: int64(1)},
		},
	}

	result, err := RewriteQuery(sql, rules)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	t.Logf("rewritten: %s", result)

	// Should preserve the original WHERE and add the RLS filter
	if !strings.Contains(result, "Pclass") {
		t.Error("expected Pclass in rewritten query")
	}
	// Original condition should still be present
	lower := strings.ToLower(result)
	if !strings.Contains(lower, "sex") {
		t.Error("expected original Sex condition to be preserved")
	}
}

func TestRewriteQuery_WithLimit(t *testing.T) {
	sql := "SELECT * FROM titanic LIMIT 10"
	rules := map[string][]policy.RLSRule{
		"titanic": {
			{Table: "titanic", Column: "Pclass", Operator: policy.OpEqual, Value: int64(1)},
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
	rules := map[string][]policy.RLSRule{
		"titanic": {
			{Table: "titanic", Column: "Survived", Operator: policy.OpEqual, Value: int64(1)},
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
	rules := map[string][]policy.RLSRule{
		"titanic": {
			{Table: "titanic", Column: "Pclass", Operator: policy.OpEqual, Value: int64(1)},
		},
	}

	result, err := RewriteQuery(sql, rules)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	t.Logf("rewritten: %s", result)

	// Should qualify the column with the table alias
	if !strings.Contains(result, "Pclass") {
		t.Error("expected Pclass filter")
	}
}

func TestRewriteQuery_AllOperators(t *testing.T) {
	ops := []struct {
		op    string
		sqlOp string
	}{
		{policy.OpEqual, "="},
		{policy.OpNotEqual, "<>"},
		{policy.OpLessThan, "<"},
		{policy.OpLessEqual, "<="},
		{policy.OpGreaterThan, ">"},
		{policy.OpGreaterEqual, ">="},
	}

	for _, tc := range ops {
		t.Run(tc.op, func(t *testing.T) {
			sql := "SELECT * FROM titanic"
			rules := map[string][]policy.RLSRule{
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
	rules := map[string][]policy.RLSRule{
		"titanic": {
			{Table: "titanic", Column: "Sex", Operator: policy.OpEqual, Value: "male"},
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
	rules := map[string][]policy.RLSRule{
		"titanic": {
			{Table: "titanic", Column: "Fare", Operator: policy.OpGreaterThan, Value: float64(50.5)},
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
	rules := map[string][]policy.RLSRule{
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
	rules := map[string][]policy.RLSRule{
		"titanic": {
			{Table: "titanic", Column: "Pclass", Operator: policy.OpEqual, Value: []int{1, 2}},
		},
	}

	_, err := RewriteQuery(sql, rules)
	if err == nil {
		t.Error("expected error for unsupported value type")
	}
}

func TestRewriteQuery_NoMatchingTable(t *testing.T) {
	sql := "SELECT * FROM titanic"
	rules := map[string][]policy.RLSRule{
		"other_table": {
			{Table: "other_table", Column: "id", Operator: policy.OpEqual, Value: int64(1)},
		},
	}

	result, err := RewriteQuery(sql, rules)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// No matching table means no filter injected
	t.Logf("result: %s", result)
}

func TestRewriteQuery_SelectedColumns(t *testing.T) {
	sql := `SELECT "PassengerId", "Name", "Pclass" FROM titanic LIMIT 5`
	rules := map[string][]policy.RLSRule{
		"titanic": {
			{Table: "titanic", Column: "Pclass", Operator: policy.OpEqual, Value: int64(1)},
		},
	}

	result, err := RewriteQuery(sql, rules)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	t.Logf("rewritten: %s", result)

	// Should have WHERE clause
	if !strings.Contains(result, "Pclass") {
		t.Error("expected Pclass filter")
	}
}

func TestRewriteQuery_InvalidSQL(t *testing.T) {
	_, err := RewriteQuery("SELEKT * FORM titanic", map[string][]policy.RLSRule{
		"titanic": {
			{Table: "titanic", Column: "id", Operator: policy.OpEqual, Value: int64(1)},
		},
	})
	if err == nil {
		t.Error("expected error for invalid SQL")
	}
}

func TestRewriteQuery_Union(t *testing.T) {
	sql := "SELECT * FROM titanic UNION ALL SELECT * FROM titanic"
	rules := map[string][]policy.RLSRule{
		"titanic": {
			{Table: "titanic", Column: "Pclass", Operator: policy.OpEqual, Value: int64(1)},
		},
	}

	result, err := RewriteQuery(sql, rules)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	t.Logf("rewritten: %s", result)

	// Both sides of UNION should have the filter
	// Count occurrences of "Pclass"
	count := strings.Count(result, "Pclass")
	if count < 2 {
		t.Errorf("expected Pclass filter in both UNION branches, found %d occurrences", count)
	}
}

// --- QuoteIdentifier tests ---

func TestQuoteIdentifier(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"simple", "simple"},
		{"with_underscore", "with_underscore"},
		{"MixedCase", `"MixedCase"`},
		{"has space", `"has space"`},
		{`has"quote`, `"has""quote"`},
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
