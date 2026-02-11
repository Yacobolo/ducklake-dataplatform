//go:build integration

package integration

import (
	"testing"
)

func TestExtension(t *testing.T) {
	checkPrerequisites(t)

	env := setupIntegrationServer(t)

	cases := []struct {
		name    string
		apiKey  string
		sql     string
		wantErr bool
		check   func(t *testing.T, result duckDBResult, stderr string)
	}{
		{
			name:   "Admin_FullAccess",
			apiKey: env.Keys.Admin,
			sql:    `SELECT * FROM titanic LIMIT 5;`,
			check: func(t *testing.T, result duckDBResult, _ string) {
				t.Helper()
				if len(result) != 5 {
					t.Errorf("expected 5 rows, got %d", len(result))
				}
				// Verify all 12 columns are present
				if len(result) > 0 {
					for _, col := range titanicColumns {
						if _, ok := result[0][col]; !ok {
							t.Errorf("missing expected column %q", col)
						}
					}
				}
				// Admin should see real Name values (not masked)
				for _, row := range result {
					if name, ok := row["Name"].(string); ok && name == "***" {
						t.Errorf("admin should see real names, got '***'")
						break
					}
				}
			},
		},
		{
			name:   "Admin_RowCount_891",
			apiKey: env.Keys.Admin,
			sql:    `SELECT count(*) as cnt FROM titanic;`,
			check: func(t *testing.T, result duckDBResult, _ string) {
				t.Helper()
				if cnt := getScalarInt(t, result, "cnt"); cnt != 891 {
					t.Errorf("expected 891 rows, got %d", cnt)
				}
			},
		},
		{
			name:   "Analyst_RLS_Pclass1",
			apiKey: env.Keys.Analyst,
			sql:    `SELECT DISTINCT "Pclass" FROM titanic;`,
			check: func(t *testing.T, result duckDBResult, _ string) {
				t.Helper()
				if len(result) != 1 {
					t.Errorf("expected 1 distinct Pclass value, got %d: %v", len(result), result)
				}
				if len(result) > 0 {
					if pclass := getScalarInt(t, result[:1], "Pclass"); pclass != 1 {
						t.Errorf("expected Pclass=1, got %d", pclass)
					}
				}
			},
		},
		{
			name:   "Analyst_RowCount_216",
			apiKey: env.Keys.Analyst,
			sql:    `SELECT count(*) as cnt FROM titanic;`,
			check: func(t *testing.T, result duckDBResult, _ string) {
				t.Helper()
				if cnt := getScalarInt(t, result, "cnt"); cnt != 216 {
					t.Errorf("expected 216 rows (Pclass=1 only), got %d", cnt)
				}
			},
		},
		{
			name:   "Analyst_NameMasked",
			apiKey: env.Keys.Analyst,
			sql:    `SELECT DISTINCT "Name" FROM titanic;`,
			check: func(t *testing.T, result duckDBResult, _ string) {
				t.Helper()
				// All names are masked to '***', so DISTINCT should return exactly 1 row
				if len(result) != 1 {
					t.Errorf("expected exactly 1 distinct masked name, got %d rows", len(result))
				}
				if len(result) > 0 {
					name, ok := result[0]["Name"].(string)
					if !ok {
						t.Fatalf("Name column not a string: %v (%T)", result[0]["Name"], result[0]["Name"])
					}
					if name != "***" {
						t.Errorf("expected masked name '***', got %q", name)
					}
				}
			},
		},
		{
			name:   "Analyst_RLS_WithUserWhere",
			apiKey: env.Keys.Analyst,
			sql:    `SELECT count(*) as cnt FROM titanic WHERE "Age" > 30;`,
			check: func(t *testing.T, result duckDBResult, _ string) {
				t.Helper()
				// RLS enforces Pclass=1; user adds Age>30 on top.
				// Result must be >0 (some Pclass=1 passengers are >30)
				// and <216 (not all Pclass=1 passengers are >30).
				cnt := getScalarInt(t, result, "cnt")
				if cnt <= 0 {
					t.Errorf("expected cnt > 0, got %d", cnt)
				}
				if cnt >= 216 {
					t.Errorf("expected cnt < 216 (user WHERE should filter further), got %d", cnt)
				}
			},
		},
		{
			name:   "Researcher_AllRows",
			apiKey: env.Keys.Researcher,
			sql:    `SELECT count(*) as cnt FROM titanic;`,
			check: func(t *testing.T, result duckDBResult, _ string) {
				t.Helper()
				if cnt := getScalarInt(t, result, "cnt"); cnt != 891 {
					t.Errorf("expected 891 rows (no RLS), got %d", cnt)
				}
			},
		},
		{
			name:   "Researcher_NameVisible",
			apiKey: env.Keys.Researcher,
			sql:    `SELECT "Name" FROM titanic LIMIT 1;`,
			check: func(t *testing.T, result duckDBResult, _ string) {
				t.Helper()
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
			},
		},
		{
			name:    "NoAccess_403",
			apiKey:  env.Keys.NoAccess,
			sql:     `SELECT * FROM titanic LIMIT 1;`,
			wantErr: true,
			check: func(t *testing.T, _ duckDBResult, stderr string) {
				t.Helper()
				if !containsAny(stderr, "access denied", "403", "permission", "lacks SELECT") {
					t.Errorf("expected access denied error, got stderr: %s", stderr)
				}
			},
		},
		{
			name:    "InvalidAPIKey_401",
			apiKey:  "invalid-key-12345",
			sql:     `SELECT * FROM titanic LIMIT 1;`,
			wantErr: true,
			check: func(t *testing.T, _ duckDBResult, stderr string) {
				t.Helper()
				if !containsAny(stderr, "401", "unauthorized", "authentication") {
					t.Errorf("expected auth error in stderr, got: %s", stderr)
				}
			},
		},
		{
			name:    "NonexistentTable_404",
			apiKey:  env.Keys.Admin,
			sql:     `SELECT * FROM nonexistent_table LIMIT 1;`,
			wantErr: true,
			check: func(t *testing.T, _ duckDBResult, stderr string) {
				t.Helper()
				if !containsAny(stderr, "not found", "404", "does not exist") {
					t.Errorf("expected 'not found' error in stderr, got: %s", stderr)
				}
			},
		},
		{
			name:   "Admin_AuditLogWritten",
			apiKey: env.Keys.Admin,
			sql:    `SELECT count(*) as cnt FROM titanic;`,
			check: func(t *testing.T, result duckDBResult, _ string) {
				t.Helper()
				// First verify the query itself succeeded
				if cnt := getScalarInt(t, result, "cnt"); cnt != 891 {
					t.Errorf("expected 891 rows, got %d", cnt)
				}
				// Now verify the manifest endpoint wrote an audit entry
				entries := fetchAuditLogs(t, env.Server.URL, env.Keys.Admin)
				found := false
				for _, e := range entries {
					action, _ := e["action"].(string)
					status, _ := e["status"].(string)
					if action == "MANIFEST" && status == "ALLOWED" {
						found = true
						break
					}
				}
				if !found {
					t.Errorf("no MANIFEST audit entry found; got %d entries", len(entries))
				}
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			result, stderr, err := runDuckDBQuery(t, env.Server.URL, tc.apiKey, tc.sql)
			if tc.wantErr {
				if err == nil {
					t.Fatal("expected error, got success")
				}
			} else {
				if err != nil {
					t.Fatalf("query failed: %v\nstderr: %s", err, stderr)
				}
			}
			tc.check(t, result, stderr)
		})
	}
}
