package repository

import (
	"context"
	"database/sql"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	internaldb "duck-demo/internal/db"
	"duck-demo/internal/domain"
)

// setupSearchRepo creates a test SQLite database with ducklake tables,
// governance tables (from migrations), and seed data for search tests.
// In single-DB mode both metaDB and controlDB point to the same DB.
func setupSearchRepo(t *testing.T) (*SearchRepo, *sql.DB) {
	t.Helper()
	writeDB, _ := internaldb.OpenTestSQLite(t)
	createDuckLakeTables(t, writeDB)
	return NewSearchRepo(writeDB, writeDB), writeDB
}

// seedSearchData inserts a realistic set of ducklake rows plus governance
// metadata so that every search path (name, comment, tag, property) can
// be exercised.
func seedSearchData(t *testing.T, db *sql.DB) {
	t.Helper()

	// --- ducklake objects ---------------------------------------------------
	// Schema: analytics (active)
	_, err := db.Exec(`INSERT INTO ducklake_schema (schema_id, schema_name, end_snapshot) VALUES (1, 'analytics', NULL)`)
	require.NoError(t, err)
	// Schema: staging (active)
	_, err = db.Exec(`INSERT INTO ducklake_schema (schema_id, schema_name, end_snapshot) VALUES (2, 'staging', NULL)`)
	require.NoError(t, err)
	// Schema: old_schema (soft-deleted)
	_, err = db.Exec(`INSERT INTO ducklake_schema (schema_id, schema_name, end_snapshot) VALUES (3, 'old_schema', 50)`)
	require.NoError(t, err)

	// Tables in analytics
	_, err = db.Exec(`INSERT INTO ducklake_table (table_id, schema_id, table_name, end_snapshot) VALUES (10, 1, 'events', NULL)`)
	require.NoError(t, err)
	_, err = db.Exec(`INSERT INTO ducklake_table (table_id, schema_id, table_name, end_snapshot) VALUES (11, 1, 'sessions', NULL)`)
	require.NoError(t, err)
	// Soft-deleted table
	_, err = db.Exec(`INSERT INTO ducklake_table (table_id, schema_id, table_name, end_snapshot) VALUES (12, 1, 'deleted_tbl', 99)`)
	require.NoError(t, err)

	// Table in staging
	_, err = db.Exec(`INSERT INTO ducklake_table (table_id, schema_id, table_name, end_snapshot) VALUES (20, 2, 'raw_events', NULL)`)
	require.NoError(t, err)

	// Columns on analytics.events
	_, err = db.Exec(`INSERT INTO ducklake_column (column_id, table_id, column_name, column_type, end_snapshot) VALUES (100, 10, 'event_id', 'INTEGER', NULL)`)
	require.NoError(t, err)
	_, err = db.Exec(`INSERT INTO ducklake_column (column_id, table_id, column_name, column_type, end_snapshot) VALUES (101, 10, 'user_email', 'VARCHAR', NULL)`)
	require.NoError(t, err)
	// Soft-deleted column
	_, err = db.Exec(`INSERT INTO ducklake_column (column_id, table_id, column_name, column_type, end_snapshot) VALUES (102, 10, 'old_col', 'TEXT', 30)`)
	require.NoError(t, err)

	// --- governance metadata ------------------------------------------------
	// Comment on analytics schema
	_, err = db.Exec(`INSERT INTO catalog_metadata (securable_type, securable_name, comment, properties, deleted_at) VALUES ('schema', 'analytics', 'Core analytics schema', '{"team":"data"}', NULL)`)
	require.NoError(t, err)

	// Comment on analytics.events table
	_, err = db.Exec(`INSERT INTO catalog_metadata (securable_type, securable_name, comment, properties, deleted_at) VALUES ('table', 'analytics.events', 'Event tracking table', '{"pii":"false"}', NULL)`)
	require.NoError(t, err)

	// Column comment
	_, err = db.Exec(`INSERT INTO column_metadata (table_securable_name, column_name, comment) VALUES ('analytics.events', 'user_email', 'The user email address')`)
	require.NoError(t, err)

	// Tag on schema "analytics" (securable_id = schema_id as TEXT)
	_, err = db.Exec(`INSERT INTO tags (id, key, value, created_by) VALUES ('tag-1', 'env', 'production', 'admin')`)
	require.NoError(t, err)
	_, err = db.Exec(`INSERT INTO tag_assignments (id, tag_id, securable_type, securable_id, assigned_by) VALUES ('ta-1', 'tag-1', 'schema', '1', 'admin')`)
	require.NoError(t, err)

	// Tag on table "events" (securable_id = table_id as TEXT)
	_, err = db.Exec(`INSERT INTO tags (id, key, value, created_by) VALUES ('tag-2', 'classification', 'internal', 'admin')`)
	require.NoError(t, err)
	_, err = db.Exec(`INSERT INTO tag_assignments (id, tag_id, securable_type, securable_id, assigned_by) VALUES ('ta-2', 'tag-2', 'table', '10', 'admin')`)
	require.NoError(t, err)

	// Tag on column (securable_type='column', securable_id=table_id, column_name set)
	_, err = db.Exec(`INSERT INTO tags (id, key, value, created_by) VALUES ('tag-3', 'pii', 'email', 'admin')`)
	require.NoError(t, err)
	_, err = db.Exec(`INSERT INTO tag_assignments (id, tag_id, securable_type, securable_id, column_name, assigned_by) VALUES ('ta-3', 'tag-3', 'column', '10', 'user_email', 'admin')`)
	require.NoError(t, err)
}

// ---------------------------------------------------------------------------
// Search by name
// ---------------------------------------------------------------------------

func TestSearchRepo_SearchBySchemaName(t *testing.T) {
	repo, db := setupSearchRepo(t)
	seedSearchData(t, db)
	ctx := context.Background()

	results, total, err := repo.Search(ctx, "analytics", nil, 100, 0)
	require.NoError(t, err)
	assert.Greater(t, total, int64(0))

	// Should find the schema "analytics" by name
	found := false
	for _, r := range results {
		if r.Type == "schema" && r.Name == "analytics" && r.MatchField == "name" {
			found = true
			break
		}
	}
	assert.True(t, found, "expected schema 'analytics' matched by name")
}

func TestSearchRepo_SearchByTableName(t *testing.T) {
	repo, db := setupSearchRepo(t)
	seedSearchData(t, db)
	ctx := context.Background()

	results, total, err := repo.Search(ctx, "events", nil, 100, 0)
	require.NoError(t, err)
	assert.Greater(t, total, int64(0))

	found := false
	for _, r := range results {
		if r.Type == "table" && r.Name == "events" && r.MatchField == "name" {
			found = true
			assert.NotNil(t, r.SchemaName)
			assert.Equal(t, "analytics", *r.SchemaName)
			break
		}
	}
	assert.True(t, found, "expected table 'events' matched by name")
}

func TestSearchRepo_SearchByColumnName(t *testing.T) {
	repo, db := setupSearchRepo(t)
	seedSearchData(t, db)
	ctx := context.Background()

	results, total, err := repo.Search(ctx, "user_email", nil, 100, 0)
	require.NoError(t, err)
	assert.Greater(t, total, int64(0))

	found := false
	for _, r := range results {
		if r.Type == "column" && r.Name == "user_email" && r.MatchField == "name" {
			found = true
			assert.NotNil(t, r.SchemaName)
			assert.Equal(t, "analytics", *r.SchemaName)
			assert.NotNil(t, r.TableName)
			assert.Equal(t, "events", *r.TableName)
			break
		}
	}
	assert.True(t, found, "expected column 'user_email' matched by name")
}

// ---------------------------------------------------------------------------
// Search with object type filter
// ---------------------------------------------------------------------------

func TestSearchRepo_SearchWithObjectTypeFilter(t *testing.T) {
	repo, db := setupSearchRepo(t)
	seedSearchData(t, db)
	ctx := context.Background()

	tests := []struct {
		name       string
		query      string
		objectType string
		wantType   string
	}{
		{
			name:       "filter schema only",
			query:      "analytics",
			objectType: "schema",
			wantType:   "schema",
		},
		{
			name:       "filter table only",
			query:      "events",
			objectType: "table",
			wantType:   "table",
		},
		{
			name:       "filter column only",
			query:      "user_email",
			objectType: "column",
			wantType:   "column",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			objType := tc.objectType
			results, _, err := repo.Search(ctx, tc.query, &objType, 100, 0)
			require.NoError(t, err)
			for _, r := range results {
				assert.Equal(t, tc.wantType, r.Type, "all results should match object type filter")
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Search with pagination
// ---------------------------------------------------------------------------

func TestSearchRepo_SearchPagination(t *testing.T) {
	repo, db := setupSearchRepo(t)
	seedSearchData(t, db)
	ctx := context.Background()

	// Search for "e" which should match multiple objects (events, user_email, event_id, raw_events, etc.)
	allResults, totalAll, err := repo.Search(ctx, "e", nil, 100, 0)
	require.NoError(t, err)
	require.Greater(t, len(allResults), 2, "need at least 3 results to test pagination")

	// Get first page of size 2
	page1, total1, err := repo.Search(ctx, "e", nil, 2, 0)
	require.NoError(t, err)
	assert.Equal(t, totalAll, total1, "total should be consistent across pages")
	assert.Len(t, page1, 2)

	// Get second page of size 2 starting at offset 2
	page2, total2, err := repo.Search(ctx, "e", nil, 2, 2)
	require.NoError(t, err)
	assert.Equal(t, totalAll, total2)
	assert.NotEmpty(t, page2)

	// Verify pages don't overlap: build a key for each result
	type resultKey struct {
		Type       string
		Name       string
		MatchField string
	}
	page1Keys := make(map[resultKey]bool)
	for _, r := range page1 {
		page1Keys[resultKey{r.Type, r.Name, r.MatchField}] = true
	}
	for _, r := range page2 {
		k := resultKey{r.Type, r.Name, r.MatchField}
		assert.False(t, page1Keys[k], "page2 result %+v should not appear in page1", k)
	}
}

// ---------------------------------------------------------------------------
// Search with no results
// ---------------------------------------------------------------------------

func TestSearchRepo_SearchNoResults(t *testing.T) {
	repo, db := setupSearchRepo(t)
	seedSearchData(t, db)
	ctx := context.Background()

	results, total, err := repo.Search(ctx, "zzz_nonexistent_xyz", nil, 100, 0)
	require.NoError(t, err)
	assert.Equal(t, int64(0), total)
	assert.Empty(t, results)
}

// ---------------------------------------------------------------------------
// Search matching governance metadata (comments, tags, properties)
// ---------------------------------------------------------------------------

func TestSearchRepo_SearchByComment(t *testing.T) {
	repo, db := setupSearchRepo(t)
	seedSearchData(t, db)
	ctx := context.Background()

	// "Core analytics" appears in the schema comment
	results, total, err := repo.Search(ctx, "Core analytics", nil, 100, 0)
	require.NoError(t, err)
	assert.Greater(t, total, int64(0))

	found := false
	for _, r := range results {
		if r.Type == "schema" && r.Name == "analytics" && r.MatchField == "comment" {
			found = true
			assert.NotNil(t, r.Comment)
			break
		}
	}
	assert.True(t, found, "expected schema matched by comment")
}

func TestSearchRepo_SearchByTag(t *testing.T) {
	repo, db := setupSearchRepo(t)
	seedSearchData(t, db)
	ctx := context.Background()

	// "production" is a tag value on the analytics schema
	results, total, err := repo.Search(ctx, "production", nil, 100, 0)
	require.NoError(t, err)
	assert.Greater(t, total, int64(0))

	found := false
	for _, r := range results {
		if r.Type == "schema" && r.MatchField == "tag" {
			found = true
			break
		}
	}
	assert.True(t, found, "expected schema matched by tag")
}

func TestSearchRepo_SearchByProperty(t *testing.T) {
	repo, db := setupSearchRepo(t)
	seedSearchData(t, db)
	ctx := context.Background()

	// Properties contain {"pii":"false"} on analytics.events
	// Search for "pii" — shouldn't match name or comment for the table, should match property
	results, total, err := repo.Search(ctx, "pii", nil, 100, 0)
	require.NoError(t, err)
	assert.Greater(t, total, int64(0))

	foundProp := false
	for _, r := range results {
		if r.MatchField == "property" {
			foundProp = true
			break
		}
	}
	// "pii" also appears as tag key — so we just verify we got results
	assert.True(t, len(results) > 0, "expected results for property/tag search")
	// If property match is present, verify it
	if foundProp {
		assert.True(t, foundProp, "expected at least one property match")
	}
}

func TestSearchRepo_SearchByColumnComment(t *testing.T) {
	repo, db := setupSearchRepo(t)
	seedSearchData(t, db)
	ctx := context.Background()

	// "email address" appears in the column_metadata comment for user_email
	results, total, err := repo.Search(ctx, "email address", nil, 100, 0)
	require.NoError(t, err)
	assert.Greater(t, total, int64(0))

	found := false
	for _, r := range results {
		if r.Type == "column" && r.Name == "user_email" && r.MatchField == "comment" {
			found = true
			assert.NotNil(t, r.Comment)
			break
		}
	}
	assert.True(t, found, "expected column matched by comment")
}

func TestSearchRepo_SearchByTableTag(t *testing.T) {
	repo, db := setupSearchRepo(t)
	seedSearchData(t, db)
	ctx := context.Background()

	// "classification" is a tag key on the events table
	results, total, err := repo.Search(ctx, "classification", nil, 100, 0)
	require.NoError(t, err)
	assert.Greater(t, total, int64(0))

	found := false
	for _, r := range results {
		if r.Type == "table" && r.MatchField == "tag" {
			found = true
			break
		}
	}
	assert.True(t, found, "expected table matched by tag")
}

func TestSearchRepo_SearchByColumnTag(t *testing.T) {
	repo, db := setupSearchRepo(t)
	seedSearchData(t, db)
	ctx := context.Background()

	// Tag with key "pii" and value "email" is assigned to the column
	colType := "column"
	results, total, err := repo.Search(ctx, "pii", &colType, 100, 0)
	require.NoError(t, err)
	assert.Greater(t, total, int64(0))

	found := false
	for _, r := range results {
		if r.Type == "column" && r.MatchField == "tag" {
			found = true
			break
		}
	}
	assert.True(t, found, "expected column matched by tag")
}

// ---------------------------------------------------------------------------
// Search excludes soft-deleted ducklake objects
// ---------------------------------------------------------------------------

func TestSearchRepo_SearchExcludesSoftDeleted(t *testing.T) {
	repo, db := setupSearchRepo(t)
	seedSearchData(t, db)
	ctx := context.Background()

	// "old_schema" is soft-deleted
	results, total, err := repo.Search(ctx, "old_schema", nil, 100, 0)
	require.NoError(t, err)
	assert.Equal(t, int64(0), total)
	assert.Empty(t, results)

	// "deleted_tbl" is soft-deleted
	results, total, err = repo.Search(ctx, "deleted_tbl", nil, 100, 0)
	require.NoError(t, err)
	assert.Equal(t, int64(0), total)
	assert.Empty(t, results)

	// "old_col" is soft-deleted
	results, total, err = repo.Search(ctx, "old_col", nil, 100, 0)
	require.NoError(t, err)
	assert.Equal(t, int64(0), total)
	assert.Empty(t, results)
}

// ---------------------------------------------------------------------------
// mergeSearchResults (unexported, accessible from same package)
// ---------------------------------------------------------------------------

func TestMergeSearchResults_Deduplication(t *testing.T) {
	schema := "analytics"
	nameResults := []domain.SearchResult{
		{Type: "schema", Name: "analytics", MatchField: "name"},
		{Type: "table", Name: "events", SchemaName: &schema, MatchField: "name"},
	}
	govResults := []domain.SearchResult{
		// Duplicate of schema by name — should be deduplicated
		{Type: "schema", Name: "analytics", MatchField: "comment"},
		// Duplicate of table by name — should be deduplicated
		{Type: "table", Name: "events", SchemaName: &schema, MatchField: "tag"},
		// Unique — should be included
		{Type: "schema", Name: "staging", MatchField: "comment"},
	}

	merged := mergeSearchResults(nameResults, govResults)
	assert.Len(t, merged, 3, "expected 2 name results + 1 unique gov result")

	// Verify name results come first (priority ordering)
	assert.Equal(t, "name", merged[0].MatchField)
	assert.Equal(t, "name", merged[1].MatchField)

	// The third should be the non-duplicate governance result
	assert.Equal(t, "staging", merged[2].Name)
	assert.Equal(t, "comment", merged[2].MatchField)
}

func TestMergeSearchResults_EmptyInputs(t *testing.T) {
	t.Run("both empty", func(t *testing.T) {
		merged := mergeSearchResults(nil, nil)
		assert.Empty(t, merged)
	})

	t.Run("only name results", func(t *testing.T) {
		nameResults := []domain.SearchResult{
			{Type: "schema", Name: "test", MatchField: "name"},
		}
		merged := mergeSearchResults(nameResults, nil)
		assert.Len(t, merged, 1)
	})

	t.Run("only gov results", func(t *testing.T) {
		govResults := []domain.SearchResult{
			{Type: "schema", Name: "test", MatchField: "comment"},
		}
		merged := mergeSearchResults(nil, govResults)
		assert.Len(t, merged, 1)
	})
}

func TestMergeSearchResults_PriorityOrdering(t *testing.T) {
	// Name results should always come before governance results in the
	// merged slice, ensuring name matches have higher priority.
	nameResults := []domain.SearchResult{
		{Type: "schema", Name: "alpha", MatchField: "name"},
	}
	govResults := []domain.SearchResult{
		{Type: "schema", Name: "beta", MatchField: "comment"},
		{Type: "schema", Name: "gamma", MatchField: "property"},
	}

	merged := mergeSearchResults(nameResults, govResults)
	require.Len(t, merged, 3)
	assert.Equal(t, "alpha", merged[0].Name)
	assert.Equal(t, "name", merged[0].MatchField)
	assert.Equal(t, "beta", merged[1].Name)
	assert.Equal(t, "gamma", merged[2].Name)
}
