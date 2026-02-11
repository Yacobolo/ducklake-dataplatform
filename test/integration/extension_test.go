//go:build integration

package integration

import (
	"strings"
	"testing"
)

func TestExtension(t *testing.T) {
	checkPrerequisites(t)

	env := setupIntegrationServer(t)

	t.Run("Admin_FullAccess", func(t *testing.T) {
		t.Parallel()
		result, stderr, err := runDuckDBQuery(t, env.Server.URL, env.Keys.Admin,
			`SELECT * FROM titanic LIMIT 5;`)
		if err != nil {
			t.Fatalf("query failed: %v\nstderr: %s", err, stderr)
		}
		if len(result) != 5 {
			t.Errorf("expected 5 rows, got %d", len(result))
		}
		// Admin should see real Name values (not masked)
		for _, row := range result {
			if name, ok := row["Name"].(string); ok {
				if name == "***" {
					t.Errorf("admin should see real names, got '***'")
					break
				}
			}
		}
	})

	t.Run("Admin_RowCount_891", func(t *testing.T) {
		t.Parallel()
		result, stderr, err := runDuckDBQuery(t, env.Server.URL, env.Keys.Admin,
			`SELECT count(*) as cnt FROM titanic;`)
		if err != nil {
			t.Fatalf("query failed: %v\nstderr: %s", err, stderr)
		}
		cnt := getScalarInt(t, result, "cnt")
		if cnt != 891 {
			t.Errorf("expected 891 rows, got %d", cnt)
		}
	})

	t.Run("Analyst_RLS_Pclass1", func(t *testing.T) {
		t.Parallel()
		result, stderr, err := runDuckDBQuery(t, env.Server.URL, env.Keys.Analyst,
			`SELECT DISTINCT "Pclass" FROM titanic;`)
		if err != nil {
			t.Fatalf("query failed: %v\nstderr: %s", err, stderr)
		}
		// Should only contain Pclass=1 due to RLS filter
		if len(result) != 1 {
			t.Errorf("expected 1 distinct Pclass value, got %d: %v", len(result), result)
		}
		if len(result) > 0 {
			pclass := getScalarInt(t, result[:1], "Pclass")
			if pclass != 1 {
				t.Errorf("expected Pclass=1, got %d", pclass)
			}
		}
	})

	t.Run("Analyst_RowCount_216", func(t *testing.T) {
		t.Parallel()
		result, stderr, err := runDuckDBQuery(t, env.Server.URL, env.Keys.Analyst,
			`SELECT count(*) as cnt FROM titanic;`)
		if err != nil {
			t.Fatalf("query failed: %v\nstderr: %s", err, stderr)
		}
		cnt := getScalarInt(t, result, "cnt")
		if cnt != 216 {
			t.Errorf("expected 216 rows (Pclass=1 only), got %d", cnt)
		}
	})

	t.Run("Analyst_NameMasked", func(t *testing.T) {
		t.Parallel()
		result, stderr, err := runDuckDBQuery(t, env.Server.URL, env.Keys.Analyst,
			`SELECT DISTINCT "Name" FROM titanic LIMIT 1;`)
		if err != nil {
			t.Fatalf("query failed: %v\nstderr: %s", err, stderr)
		}
		if len(result) == 0 {
			t.Fatal("expected at least 1 row")
		}
		name, ok := result[0]["Name"].(string)
		if !ok {
			t.Fatalf("Name column not a string: %v (%T)", result[0]["Name"], result[0]["Name"])
		}
		if name != "***" {
			t.Errorf("expected masked name '***', got %q", name)
		}
	})

	t.Run("Researcher_AllRows", func(t *testing.T) {
		t.Parallel()
		result, stderr, err := runDuckDBQuery(t, env.Server.URL, env.Keys.Researcher,
			`SELECT count(*) as cnt FROM titanic;`)
		if err != nil {
			t.Fatalf("query failed: %v\nstderr: %s", err, stderr)
		}
		cnt := getScalarInt(t, result, "cnt")
		if cnt != 891 {
			t.Errorf("expected 891 rows (no RLS), got %d", cnt)
		}
	})

	t.Run("Researcher_NameVisible", func(t *testing.T) {
		t.Parallel()
		result, stderr, err := runDuckDBQuery(t, env.Server.URL, env.Keys.Researcher,
			`SELECT "Name" FROM titanic LIMIT 1;`)
		if err != nil {
			t.Fatalf("query failed: %v\nstderr: %s", err, stderr)
		}
		if len(result) == 0 {
			t.Fatal("expected at least 1 row")
		}
		name, ok := result[0]["Name"].(string)
		if !ok {
			t.Fatalf("Name column not a string: %v (%T)", result[0]["Name"], result[0]["Name"])
		}
		if name == "***" {
			t.Error("researcher with see_original=1 should see real names, got '***'")
		}
	})

	t.Run("InvalidAPIKey_Error", func(t *testing.T) {
		t.Parallel()
		_, stderr, err := runDuckDBQuery(t, env.Server.URL, "invalid-key-12345",
			`SELECT * FROM titanic LIMIT 1;`)
		if err == nil {
			t.Fatal("expected error for invalid API key, got success")
		}
		// The extension surfaces the 401 error from the server
		combined := strings.ToLower(stderr)
		if !strings.Contains(combined, "401") &&
			!strings.Contains(combined, "unauthorized") &&
			!strings.Contains(combined, "authentication") {
			t.Errorf("expected auth error in stderr, got: %s", stderr)
		}
	})

	t.Run("NonexistentTable_Error", func(t *testing.T) {
		t.Parallel()
		_, stderr, err := runDuckDBQuery(t, env.Server.URL, env.Keys.Admin,
			`SELECT * FROM nonexistent_table LIMIT 1;`)
		if err == nil {
			t.Fatal("expected error for nonexistent table, got success")
		}
		combined := strings.ToLower(stderr)
		if !strings.Contains(combined, "not found") &&
			!strings.Contains(combined, "404") &&
			!strings.Contains(combined, "does not exist") &&
			!strings.Contains(combined, "not exist") {
			t.Errorf("expected 'not found' error in stderr, got: %s", stderr)
		}
	})
}
