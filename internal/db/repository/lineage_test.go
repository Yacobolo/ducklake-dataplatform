package repository

import (
	"context"
	"database/sql"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	internaldb "duck-demo/internal/db"
	"duck-demo/internal/domain"
)

func setupLineageRepo(t *testing.T) (*LineageRepo, *sql.DB) {
	t.Helper()
	writeDB, _ := internaldb.OpenTestSQLite(t)
	return NewLineageRepo(writeDB), writeDB
}

func lineagePtrStr(s string) *string { return &s }

// getLineageEdgeID retrieves the ID of a lineage edge by querying the DB directly,
// since the GetUpstream/GetDownstream queries use SELECT DISTINCT without the id column.
func getLineageEdgeID(t *testing.T, db *sql.DB, sourceTable, targetTable string) string {
	t.Helper()
	var id string
	err := db.QueryRow(
		"SELECT id FROM lineage_edges WHERE source_table = ? AND target_table = ? LIMIT 1",
		sourceTable, targetTable,
	).Scan(&id)
	require.NoError(t, err)
	return id
}

func TestLineageRepo_InsertAndGetUpstream(t *testing.T) {
	repo, _ := setupLineageRepo(t)
	ctx := context.Background()

	edge := &domain.LineageEdge{
		SourceTable:   "orders",
		TargetTable:   lineagePtrStr("revenue_report"),
		SourceSchema:  "default",
		TargetSchema:  "default",
		EdgeType:      "READ",
		PrincipalName: "alice",
		QueryHash:     nil,
	}

	err := repo.InsertEdge(ctx, edge)
	require.NoError(t, err)

	edges, total, err := repo.GetUpstream(ctx, "revenue_report", domain.PageRequest{})
	require.NoError(t, err)
	assert.Equal(t, int64(1), total)
	require.Len(t, edges, 1)

	got := edges[0]
	assert.Equal(t, "orders", got.SourceTable)
	require.NotNil(t, got.TargetTable)
	assert.Equal(t, "revenue_report", *got.TargetTable)
	assert.Equal(t, "READ", got.EdgeType)
	assert.Equal(t, "alice", got.PrincipalName)
	assert.Equal(t, "default", got.SourceSchema)
	assert.Equal(t, "default", got.TargetSchema)
}

func TestLineageRepo_GetDownstream(t *testing.T) {
	repo, _ := setupLineageRepo(t)
	ctx := context.Background()

	edge := &domain.LineageEdge{
		SourceTable:   "orders",
		TargetTable:   lineagePtrStr("summary"),
		SourceSchema:  "public",
		TargetSchema:  "public",
		EdgeType:      "WRITE",
		PrincipalName: "bob",
		QueryHash:     nil,
	}

	err := repo.InsertEdge(ctx, edge)
	require.NoError(t, err)

	edges, total, err := repo.GetDownstream(ctx, "orders", domain.PageRequest{})
	require.NoError(t, err)
	assert.Equal(t, int64(1), total)
	require.Len(t, edges, 1)

	got := edges[0]
	assert.Equal(t, "orders", got.SourceTable)
	require.NotNil(t, got.TargetTable)
	assert.Equal(t, "summary", *got.TargetTable)
	assert.Equal(t, "WRITE", got.EdgeType)
	assert.Equal(t, "bob", got.PrincipalName)
}

func TestLineageRepo_DeleteEdge(t *testing.T) {
	repo, db := setupLineageRepo(t)
	ctx := context.Background()

	edge := &domain.LineageEdge{
		SourceTable:   "raw_events",
		TargetTable:   lineagePtrStr("cleaned_events"),
		SourceSchema:  "default",
		TargetSchema:  "default",
		EdgeType:      "READ_WRITE",
		PrincipalName: "etl_user",
		QueryHash:     nil,
	}

	err := repo.InsertEdge(ctx, edge)
	require.NoError(t, err)

	// Verify the edge exists.
	edges, total, err := repo.GetUpstream(ctx, "cleaned_events", domain.PageRequest{})
	require.NoError(t, err)
	assert.Equal(t, int64(1), total)
	require.Len(t, edges, 1)

	// Get the ID directly from the DB since GetUpstream doesn't return it.
	edgeID := getLineageEdgeID(t, db, "raw_events", "cleaned_events")
	require.NotEmpty(t, edgeID)

	// Delete the edge.
	err = repo.DeleteEdge(ctx, edgeID)
	require.NoError(t, err)

	// Verify it's gone.
	edges, total, err = repo.GetUpstream(ctx, "cleaned_events", domain.PageRequest{})
	require.NoError(t, err)
	assert.Equal(t, int64(0), total)
	assert.Empty(t, edges)
}

func TestLineageRepo_PurgeOlderThan(t *testing.T) {
	repo, _ := setupLineageRepo(t)
	ctx := context.Background()

	edge1 := &domain.LineageEdge{
		SourceTable:   "transactions",
		TargetTable:   lineagePtrStr("report_a"),
		SourceSchema:  "default",
		TargetSchema:  "default",
		EdgeType:      "READ",
		PrincipalName: "analyst",
		QueryHash:     nil,
	}
	edge2 := &domain.LineageEdge{
		SourceTable:   "customers",
		TargetTable:   lineagePtrStr("report_a"),
		SourceSchema:  "default",
		TargetSchema:  "default",
		EdgeType:      "READ",
		PrincipalName: "analyst",
		QueryHash:     nil,
	}

	err := repo.InsertEdge(ctx, edge1)
	require.NoError(t, err)
	err = repo.InsertEdge(ctx, edge2)
	require.NoError(t, err)

	// Use a future cutoff so all existing edges are "older than" the cutoff.
	cutoff := time.Now().Add(time.Hour)
	count, err := repo.PurgeOlderThan(ctx, cutoff)
	require.NoError(t, err)
	assert.Equal(t, int64(2), count)

	// Verify all edges are gone.
	edges, total, err := repo.GetUpstream(ctx, "report_a", domain.PageRequest{})
	require.NoError(t, err)
	assert.Equal(t, int64(0), total)
	assert.Empty(t, edges)
}

func TestLineageRepo_EmptyResults(t *testing.T) {
	repo, _ := setupLineageRepo(t)
	ctx := context.Background()

	edges, total, err := repo.GetUpstream(ctx, "nonexistent_table", domain.PageRequest{})
	require.NoError(t, err)
	assert.Equal(t, int64(0), total)
	assert.Empty(t, edges)
}
