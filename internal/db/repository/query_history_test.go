package repository

import (
	"context"
	"testing"

	"github.com/google/uuid"
	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	internaldb "duck-demo/internal/db"
	"duck-demo/internal/domain"
)

func setupQueryHistoryRepo(t *testing.T) (*QueryHistoryRepo, *AuditRepo) {
	t.Helper()
	writeDB, _ := internaldb.OpenTestSQLite(t)
	return NewQueryHistoryRepo(writeDB), NewAuditRepo(writeDB)
}

func qhPtrStr(s string) *string { return &s }
func qhPtrInt64(i int64) *int64 { return &i }

func TestQueryHistoryRepo_ListAll(t *testing.T) {
	qhRepo, auditRepo := setupQueryHistoryRepo(t)
	ctx := context.Background()

	// Insert 2 QUERY audit entries.
	require.NoError(t, auditRepo.Insert(ctx, &domain.AuditEntry{
		ID:            uuid.New().String(),
		PrincipalName: "alice",
		Action:        "QUERY",
		StatementType: qhPtrStr("SELECT"),
		OriginalSQL:   qhPtrStr("SELECT * FROM t"),
		Status:        "ALLOWED",
		DurationMs:    qhPtrInt64(100),
	}))
	require.NoError(t, auditRepo.Insert(ctx, &domain.AuditEntry{
		ID:            uuid.New().String(),
		PrincipalName: "bob",
		Action:        "QUERY",
		StatementType: qhPtrStr("SELECT"),
		OriginalSQL:   qhPtrStr("SELECT 1"),
		Status:        "ALLOWED",
		DurationMs:    qhPtrInt64(50),
	}))

	entries, total, err := qhRepo.List(ctx, domain.QueryHistoryFilter{
		Page: domain.PageRequest{},
	})
	require.NoError(t, err)
	assert.Equal(t, int64(2), total)
	assert.Len(t, entries, 2)
}

func TestQueryHistoryRepo_FilterByPrincipal(t *testing.T) {
	qhRepo, auditRepo := setupQueryHistoryRepo(t)
	ctx := context.Background()

	require.NoError(t, auditRepo.Insert(ctx, &domain.AuditEntry{
		ID:            uuid.New().String(),
		PrincipalName: "alice",
		Action:        "QUERY",
		StatementType: qhPtrStr("SELECT"),
		OriginalSQL:   qhPtrStr("SELECT * FROM t"),
		Status:        "ALLOWED",
		DurationMs:    qhPtrInt64(100),
	}))
	require.NoError(t, auditRepo.Insert(ctx, &domain.AuditEntry{
		ID:            uuid.New().String(),
		PrincipalName: "bob",
		Action:        "QUERY",
		StatementType: qhPtrStr("SELECT"),
		OriginalSQL:   qhPtrStr("SELECT 1"),
		Status:        "ALLOWED",
		DurationMs:    qhPtrInt64(50),
	}))

	entries, total, err := qhRepo.List(ctx, domain.QueryHistoryFilter{
		PrincipalName: qhPtrStr("alice"),
		Page:          domain.PageRequest{},
	})
	require.NoError(t, err)
	assert.Equal(t, int64(1), total)
	assert.Len(t, entries, 1)
	assert.Equal(t, "alice", entries[0].PrincipalName)
}

func TestQueryHistoryRepo_FilterByStatus(t *testing.T) {
	qhRepo, auditRepo := setupQueryHistoryRepo(t)
	ctx := context.Background()

	require.NoError(t, auditRepo.Insert(ctx, &domain.AuditEntry{
		ID:            uuid.New().String(),
		PrincipalName: "alice",
		Action:        "QUERY",
		StatementType: qhPtrStr("SELECT"),
		OriginalSQL:   qhPtrStr("SELECT * FROM t"),
		Status:        "ALLOWED",
		DurationMs:    qhPtrInt64(100),
	}))
	require.NoError(t, auditRepo.Insert(ctx, &domain.AuditEntry{
		ID:            uuid.New().String(),
		PrincipalName: "bob",
		Action:        "QUERY",
		StatementType: qhPtrStr("SELECT"),
		OriginalSQL:   qhPtrStr("SELECT * FROM secret"),
		Status:        "DENIED",
		DurationMs:    qhPtrInt64(10),
	}))

	entries, total, err := qhRepo.List(ctx, domain.QueryHistoryFilter{
		Status: qhPtrStr("ALLOWED"),
		Page:   domain.PageRequest{},
	})
	require.NoError(t, err)
	assert.Equal(t, int64(1), total)
	assert.Len(t, entries, 1)
	assert.Equal(t, "ALLOWED", entries[0].Status)
}

func TestQueryHistoryRepo_OnlyQUERYAction(t *testing.T) {
	qhRepo, auditRepo := setupQueryHistoryRepo(t)
	ctx := context.Background()

	// Insert a GRANT entry — should NOT appear in query history.
	require.NoError(t, auditRepo.Insert(ctx, &domain.AuditEntry{
		ID:            uuid.New().String(),
		PrincipalName: "admin",
		Action:        "GRANT",
		Status:        "ALLOWED",
	}))
	// Insert a QUERY entry — should appear.
	require.NoError(t, auditRepo.Insert(ctx, &domain.AuditEntry{
		ID:            uuid.New().String(),
		PrincipalName: "alice",
		Action:        "QUERY",
		StatementType: qhPtrStr("SELECT"),
		OriginalSQL:   qhPtrStr("SELECT 1"),
		Status:        "ALLOWED",
		DurationMs:    qhPtrInt64(5),
	}))

	entries, total, err := qhRepo.List(ctx, domain.QueryHistoryFilter{
		Page: domain.PageRequest{},
	})
	require.NoError(t, err)
	assert.Equal(t, int64(1), total)
	assert.Len(t, entries, 1)
	assert.Equal(t, "alice", entries[0].PrincipalName)
}

func TestQueryHistoryRepo_EmptyList(t *testing.T) {
	qhRepo, _ := setupQueryHistoryRepo(t)
	ctx := context.Background()

	entries, total, err := qhRepo.List(ctx, domain.QueryHistoryFilter{
		Page: domain.PageRequest{},
	})
	require.NoError(t, err)
	assert.Equal(t, int64(0), total)
	assert.Empty(t, entries)
}
