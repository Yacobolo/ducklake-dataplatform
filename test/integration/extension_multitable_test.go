//go:build integration

package integration

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestExtension_MultiTable tests complex SQL, cross-table queries, and security
// edge cases for the duck_access extension against a multi-table local server.
func TestExtension_MultiTable(t *testing.T) {
	checkExtensionBinaries(t)
	env := setupMultiTableLocalServer(t)

	// =======================================================================
	// Complex SQL Tests (single titanic table)
	// =======================================================================

	t.Run("GroupBy_With_RLS", func(t *testing.T) {
		t.Parallel()
		result, stderr, err := runDuckDBQuery(t, env.Server.URL, env.Keys.Analyst,
			`SELECT "Pclass", COUNT(*) as cnt FROM titanic GROUP BY "Pclass"`)
		require.NoError(t, err, "query failed: %v\nstderr: %s", err, stderr)

		// Analyst only sees Pclass=1 due to RLS, so GROUP BY should return 1 row.
		require.Len(t, result, 1, "expected 1 group (analyst only sees Pclass=1)")
		pclass := getScalarInt(t, result[:1], "Pclass")
		assert.Equal(t, 1, pclass, "expected Pclass=1")
	})

	t.Run("OrderBy_With_RLS", func(t *testing.T) {
		t.Parallel()
		result, stderr, err := runDuckDBQuery(t, env.Server.URL, env.Keys.Analyst,
			`SELECT "Fare" FROM titanic ORDER BY "Fare" DESC LIMIT 5`)
		require.NoError(t, err, "query failed: %v\nstderr: %s", err, stderr)

		// Should return 5 rows, all within Pclass=1 fare range.
		require.Len(t, result, 5, "expected 5 rows")
	})

	t.Run("CTE_With_RLS", func(t *testing.T) {
		t.Parallel()
		result, stderr, err := runDuckDBQuery(t, env.Server.URL, env.Keys.Analyst,
			`WITH cte AS (SELECT * FROM titanic) SELECT count(*) as cnt FROM cte WHERE "Fare" > 50`)
		require.NoError(t, err, "query failed: %v\nstderr: %s", err, stderr)

		cnt := getScalarInt(t, result, "cnt")
		assert.Greater(t, cnt, 0, "expected cnt > 0 (some Pclass=1 fares exceed 50)")
		assert.Less(t, cnt, 216, "expected cnt < 216 (RLS + user WHERE)")
	})

	t.Run("Subquery_With_RLS", func(t *testing.T) {
		t.Parallel()
		result, stderr, err := runDuckDBQuery(t, env.Server.URL, env.Keys.Analyst,
			`SELECT count(*) as cnt FROM titanic WHERE "Fare" > (SELECT AVG("Fare") FROM titanic)`)
		require.NoError(t, err, "query failed: %v\nstderr: %s", err, stderr)

		cnt := getScalarInt(t, result, "cnt")
		assert.Greater(t, cnt, 0, "expected cnt > 0")
		assert.Less(t, cnt, 216, "expected cnt < 216")
	})

	t.Run("Aggregate_Functions", func(t *testing.T) {
		t.Parallel()
		result, stderr, err := runDuckDBQuery(t, env.Server.URL, env.Keys.Analyst,
			`SELECT MIN("Fare") as min_fare, MAX("Fare") as max_fare, COUNT(*) as cnt FROM titanic`)
		require.NoError(t, err, "query failed: %v\nstderr: %s", err, stderr)

		cnt := getScalarInt(t, result, "cnt")
		assert.Equal(t, 216, cnt, "expected 216 rows (Pclass=1 only)")
	})

	t.Run("Having_With_RLS", func(t *testing.T) {
		t.Parallel()
		result, stderr, err := runDuckDBQuery(t, env.Server.URL, env.Keys.Analyst,
			`SELECT "Embarked", COUNT(*) as cnt FROM titanic GROUP BY "Embarked" HAVING COUNT(*) > 5`)
		require.NoError(t, err, "query failed: %v\nstderr: %s", err, stderr)

		require.GreaterOrEqual(t, len(result), 1, "expected at least 1 row with count > 5")
	})

	// =======================================================================
	// Multi-Table Tests
	// =======================================================================

	t.Run("Admin_CrossJoin", func(t *testing.T) {
		t.Parallel()
		result, stderr, err := runDuckDBQuery(t, env.Server.URL, env.Keys.Admin,
			`SELECT t."Name", d.dept_name FROM titanic t, departments d LIMIT 10`)
		require.NoError(t, err, "query failed: %v\nstderr: %s", err, stderr)

		require.Len(t, result, 10, "expected 10 rows from cross join")
		for i, row := range result {
			_, hasName := row["Name"]
			_, hasDept := row["dept_name"]
			assert.True(t, hasName, "row %d missing Name key", i)
			assert.True(t, hasDept, "row %d missing dept_name key", i)
		}
	})

	t.Run("DeptViewer_OnlyDepartments", func(t *testing.T) {
		t.Parallel()
		result, stderr, err := runDuckDBQuery(t, env.Server.URL, env.Keys.DeptViewer,
			`SELECT count(*) as cnt FROM departments`)
		require.NoError(t, err, "query failed: %v\nstderr: %s", err, stderr)

		cnt := getScalarInt(t, result, "cnt")
		assert.Equal(t, 10, cnt, "expected 10 departments")
	})

	t.Run("DeptViewer_DeniedTitanic", func(t *testing.T) {
		t.Parallel()
		_, stderr, err := runDuckDBQuery(t, env.Server.URL, env.Keys.DeptViewer,
			`SELECT * FROM titanic LIMIT 1`)
		if err == nil {
			t.Fatal("expected error, got success")
		}
		assert.True(t, containsAny(stderr, "access denied", "403", "lacks SELECT"),
			"expected access denied error, got stderr: %s", stderr)
	})

	t.Run("USOnly_DeptFilter", func(t *testing.T) {
		t.Parallel()
		result, stderr, err := runDuckDBQuery(t, env.Server.URL, env.Keys.USOnlyViewer,
			`SELECT count(*) as cnt FROM departments`)
		require.NoError(t, err, "query failed: %v\nstderr: %s", err, stderr)

		cnt := getScalarInt(t, result, "cnt")
		assert.Equal(t, 5, cnt, "expected 5 US-region departments")
	})

	t.Run("USOnly_TitanicFilter", func(t *testing.T) {
		t.Parallel()
		result, stderr, err := runDuckDBQuery(t, env.Server.URL, env.Keys.USOnlyViewer,
			`SELECT DISTINCT "Embarked" FROM titanic`)
		require.NoError(t, err, "query failed: %v\nstderr: %s", err, stderr)

		require.Len(t, result, 1, "expected exactly 1 distinct Embarked value")
		embarked, ok := result[0]["Embarked"].(string)
		require.True(t, ok, "Embarked not a string: %v (%T)", result[0]["Embarked"], result[0]["Embarked"])
		assert.Equal(t, "S", embarked, "expected Embarked='S' for US-only viewer")
	})

	t.Run("CrossTable_SalaryMask", func(t *testing.T) {
		t.Parallel()
		result, stderr, err := runDuckDBQuery(t, env.Server.URL, env.Keys.MaskedViewer,
			`SELECT "avg_salary" FROM departments LIMIT 5`)
		require.NoError(t, err, "query failed: %v\nstderr: %s", err, stderr)

		require.Len(t, result, 5, "expected 5 rows")
		for i, row := range result {
			v, ok := row["avg_salary"]
			require.True(t, ok, "row %d missing avg_salary", i)
			salary, ok := v.(float64)
			require.True(t, ok, "row %d avg_salary not a number: %v (%T)", i, v, v)
			assert.Equal(t, float64(0), salary, "row %d: expected avg_salary=0 (masked), got %v", i, salary)
		}
	})

	// =======================================================================
	// Security Edge Cases
	// =======================================================================

	t.Run("MultipleRLSFilters", func(t *testing.T) {
		t.Parallel()
		result, stderr, err := runDuckDBQuery(t, env.Server.URL, env.Keys.MultiFilterUser,
			`SELECT DISTINCT "Pclass" as pclass, "Survived" as survived FROM titanic`)
		require.NoError(t, err, "query failed: %v\nstderr: %s", err, stderr)

		for i, row := range result {
			pclass, ok := row["pclass"].(float64)
			require.True(t, ok, "row %d: pclass not a number: %v", i, row["pclass"])
			assert.Equal(t, float64(1), pclass, "row %d: expected pclass=1", i)

			survived, ok := row["survived"].(float64)
			require.True(t, ok, "row %d: survived not a number: %v", i, row["survived"])
			assert.Equal(t, float64(1), survived, "row %d: expected survived=1", i)
		}
	})

	t.Run("MultipleColumnMasks", func(t *testing.T) {
		t.Parallel()
		result, stderr, err := runDuckDBQuery(t, env.Server.URL, env.Keys.MaskedViewer,
			`SELECT "Name", "Fare" FROM titanic LIMIT 5`)
		require.NoError(t, err, "query failed: %v\nstderr: %s", err, stderr)

		require.Len(t, result, 5, "expected 5 rows")
		for i, row := range result {
			name, ok := row["Name"].(string)
			require.True(t, ok, "row %d: Name not a string: %v (%T)", i, row["Name"], row["Name"])
			assert.Equal(t, "***", name, "row %d: expected Name='***' (masked)", i)

			fareVal, ok := row["Fare"].(float64)
			require.True(t, ok, "row %d: Fare not a number: %v (%T)", i, row["Fare"], row["Fare"])
			assert.Equal(t, 0.0, math.Mod(fareVal, 10.0),
				"row %d: expected Fare divisible by 10 (bucketed), got %v", i, fareVal)
		}
	})

	t.Run("SelectStar_WithMasks", func(t *testing.T) {
		t.Parallel()
		result, stderr, err := runDuckDBQuery(t, env.Server.URL, env.Keys.MaskedViewer,
			`SELECT * FROM titanic LIMIT 1`)
		require.NoError(t, err, "query failed: %v\nstderr: %s", err, stderr)

		require.Len(t, result, 1, "expected 1 row")
		name, ok := result[0]["Name"].(string)
		require.True(t, ok, "Name not a string: %v (%T)", result[0]["Name"], result[0]["Name"])
		assert.Equal(t, "***", name, "expected Name='***' in SELECT * with masks")
	})

	t.Run("InsertDenied", func(t *testing.T) {
		t.Parallel()
		_, stderr, err := runDuckDBQuery(t, env.Server.URL, env.Keys.Analyst,
			`INSERT INTO titanic ("PassengerId") VALUES (9999)`)
		if err == nil {
			t.Fatal("expected error, got success")
		}
		assert.True(t, containsAny(stderr, "error", "denied", "not allowed", "permission", "INSERT"),
			"expected error indicator in stderr, got: %s", stderr)
	})

	t.Run("DropDenied", func(t *testing.T) {
		t.Parallel()
		_, stderr, err := runDuckDBQuery(t, env.Server.URL, env.Keys.Analyst,
			`DROP TABLE titanic`)
		if err == nil {
			t.Fatal("expected error, got success")
		}
		assert.True(t, containsAny(stderr, "error", "denied", "not allowed", "permission", "DROP"),
			"expected error indicator in stderr, got: %s", stderr)
	})

	t.Run("CreateDenied", func(t *testing.T) {
		t.Parallel()
		_, stderr, err := runDuckDBQuery(t, env.Server.URL, env.Keys.Analyst,
			`CREATE TABLE evil (id INT)`)
		if err == nil {
			t.Fatal("expected error, got success")
		}
		assert.True(t, containsAny(stderr, "error", "denied", "not allowed", "permission", "CREATE"),
			"expected error indicator in stderr, got: %s", stderr)
	})
}
