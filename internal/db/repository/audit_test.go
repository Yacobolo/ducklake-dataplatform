package repository

import (
	"context"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	internaldb "duck-demo/internal/db"
	"duck-demo/internal/domain"
)

func setupAuditRepo(t *testing.T) *AuditRepo {
	t.Helper()
	writeDB, _ := internaldb.OpenTestSQLite(t)
	return NewAuditRepo(writeDB)
}

func auditPtrStr(s string) *string { return &s }
func auditPtrInt64(i int64) *int64 { return &i }

func makeAuditEntry(principal, action, status string) *domain.AuditEntry {
	return &domain.AuditEntry{
		PrincipalName:  principal,
		Action:         action,
		StatementType:  auditPtrStr("SELECT"),
		OriginalSQL:    auditPtrStr("SELECT * FROM t"),
		RewrittenSQL:   auditPtrStr("SELECT * FROM t WHERE 1=1"),
		TablesAccessed: []string{"t"},
		Status:         status,
		DurationMs:     auditPtrInt64(42),
		RowsReturned:   auditPtrInt64(10),
		CreatedAt:      time.Now(),
	}
}

func TestAuditRepo_InsertAndList(t *testing.T) {
	repo := setupAuditRepo(t)
	ctx := context.Background()

	// Insert two entries with different principals.
	err := repo.Insert(ctx, makeAuditEntry("alice", "QUERY", "ALLOWED"))
	require.NoError(t, err)

	err = repo.Insert(ctx, makeAuditEntry("bob", "QUERY", "ALLOWED"))
	require.NoError(t, err)

	// List all (no filters).
	entries, total, err := repo.List(ctx, domain.AuditFilter{
		Page: domain.PageRequest{},
	})
	require.NoError(t, err)
	assert.Equal(t, int64(2), total)
	assert.Len(t, entries, 2)
}

func TestAuditRepo_FilterByPrincipal(t *testing.T) {
	repo := setupAuditRepo(t)
	ctx := context.Background()

	require.NoError(t, repo.Insert(ctx, makeAuditEntry("alice", "QUERY", "ALLOWED")))
	require.NoError(t, repo.Insert(ctx, makeAuditEntry("alice", "QUERY", "ALLOWED")))
	require.NoError(t, repo.Insert(ctx, makeAuditEntry("bob", "QUERY", "ALLOWED")))

	entries, total, err := repo.List(ctx, domain.AuditFilter{
		PrincipalName: auditPtrStr("alice"),
		Page:          domain.PageRequest{},
	})
	require.NoError(t, err)
	assert.Equal(t, int64(2), total)
	assert.Len(t, entries, 2)

	for _, e := range entries {
		assert.Equal(t, "alice", e.PrincipalName)
	}
}

func TestAuditRepo_FilterByAction(t *testing.T) {
	repo := setupAuditRepo(t)
	ctx := context.Background()

	require.NoError(t, repo.Insert(ctx, makeAuditEntry("alice", "QUERY", "ALLOWED")))
	require.NoError(t, repo.Insert(ctx, makeAuditEntry("alice", "GRANT", "ALLOWED")))
	require.NoError(t, repo.Insert(ctx, makeAuditEntry("bob", "QUERY", "ALLOWED")))

	entries, total, err := repo.List(ctx, domain.AuditFilter{
		Action: auditPtrStr("QUERY"),
		Page:   domain.PageRequest{},
	})
	require.NoError(t, err)
	assert.Equal(t, int64(2), total)
	assert.Len(t, entries, 2)

	for _, e := range entries {
		assert.Equal(t, "QUERY", e.Action)
	}
}

func TestAuditRepo_FilterByStatus(t *testing.T) {
	repo := setupAuditRepo(t)
	ctx := context.Background()

	require.NoError(t, repo.Insert(ctx, makeAuditEntry("alice", "QUERY", "ALLOWED")))
	require.NoError(t, repo.Insert(ctx, makeAuditEntry("bob", "QUERY", "DENIED")))
	require.NoError(t, repo.Insert(ctx, makeAuditEntry("carol", "QUERY", "ALLOWED")))

	entries, total, err := repo.List(ctx, domain.AuditFilter{
		Status: auditPtrStr("ALLOWED"),
		Page:   domain.PageRequest{},
	})
	require.NoError(t, err)
	assert.Equal(t, int64(2), total)
	assert.Len(t, entries, 2)

	for _, e := range entries {
		assert.Equal(t, "ALLOWED", e.Status)
	}
}

func TestAuditRepo_Pagination(t *testing.T) {
	repo := setupAuditRepo(t)
	ctx := context.Background()

	// Insert 3 entries.
	require.NoError(t, repo.Insert(ctx, makeAuditEntry("alice", "QUERY", "ALLOWED")))
	require.NoError(t, repo.Insert(ctx, makeAuditEntry("bob", "QUERY", "ALLOWED")))
	require.NoError(t, repo.Insert(ctx, makeAuditEntry("carol", "QUERY", "ALLOWED")))

	// Request page with MaxResults=2.
	entries, total, err := repo.List(ctx, domain.AuditFilter{
		Page: domain.PageRequest{MaxResults: 2},
	})
	require.NoError(t, err)
	assert.Equal(t, int64(3), total)
	assert.Len(t, entries, 2)
}

func TestAuditRepo_EmptyList(t *testing.T) {
	repo := setupAuditRepo(t)
	ctx := context.Background()

	entries, total, err := repo.List(ctx, domain.AuditFilter{
		Page: domain.PageRequest{},
	})
	require.NoError(t, err)
	assert.Equal(t, int64(0), total)
	assert.Empty(t, entries)
}
