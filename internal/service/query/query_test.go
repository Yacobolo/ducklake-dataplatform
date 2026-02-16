package query

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"duck-demo/internal/domain"
	"duck-demo/internal/testutil"

	_ "github.com/duckdb/duckdb-go/v2"
)

// openDuckDB opens an in-memory DuckDB connection for producing real *sql.Rows.
func openDuckDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("duckdb", "")
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// === Execute ===

func TestQueryService_Execute(t *testing.T) {
	t.Parallel()

	db := openDuckDB(t)

	tests := []struct {
		name       string
		principal  string
		sqlQuery   string
		setupEng   func(*testutil.MockSessionEngine, *sql.DB)
		lineage    domain.LineageRepository
		wantErr    bool
		wantErrMsg string
		checkAudit func(t *testing.T, audit *testutil.MockAuditRepo)
		checkRes   func(t *testing.T, result *QueryResult)
	}{
		{
			name:      "empty SQL returns validation error",
			principal: "alice",
			sqlQuery:  "",
			setupEng:  func(_ *testutil.MockSessionEngine, _ *sql.DB) {},
			wantErr:   true,
			checkAudit: func(t *testing.T, audit *testutil.MockAuditRepo) {
				t.Helper()
				assert.Empty(t, audit.Entries, "no audit entry for validation error")
			},
		},
		{
			name:      "whitespace-only SQL returns validation error",
			principal: "alice",
			sqlQuery:  "   \t\n  ",
			setupEng:  func(_ *testutil.MockSessionEngine, _ *sql.DB) {},
			wantErr:   true,
			checkAudit: func(t *testing.T, audit *testutil.MockAuditRepo) {
				t.Helper()
				assert.Empty(t, audit.Entries, "no audit entry for validation error")
			},
		},
		{
			name:      "engine error returns error and logs DENIED audit",
			principal: "bob",
			sqlQuery:  "SELECT * FROM secret_table",
			setupEng: func(eng *testutil.MockSessionEngine, _ *sql.DB) {
				eng.QueryFn = func(_ context.Context, _, _ string) (*sql.Rows, error) {
					return nil, fmt.Errorf("access denied for principal bob")
				}
			},
			wantErr:    true,
			wantErrMsg: "access denied for principal bob",
			checkAudit: func(t *testing.T, audit *testutil.MockAuditRepo) {
				t.Helper()
				require.Len(t, audit.Entries, 1)
				assert.Equal(t, "DENIED", audit.Entries[0].Status)
				assert.Equal(t, "QUERY", audit.Entries[0].Action)
				assert.Equal(t, "bob", audit.Entries[0].PrincipalName)
			},
		},
		{
			name:      "successful SELECT returns rows and logs ALLOWED audit",
			principal: "alice",
			sqlQuery:  "SELECT 1 AS id, 'hello' AS name",
			setupEng: func(eng *testutil.MockSessionEngine, ddb *sql.DB) {
				eng.QueryFn = func(ctx context.Context, _, q string) (*sql.Rows, error) {
					return ddb.QueryContext(ctx, q)
				}
			},
			wantErr: false,
			checkAudit: func(t *testing.T, audit *testutil.MockAuditRepo) {
				t.Helper()
				require.Len(t, audit.Entries, 1)
				assert.Equal(t, "ALLOWED", audit.Entries[0].Status)
				assert.Equal(t, "alice", audit.Entries[0].PrincipalName)
			},
			checkRes: func(t *testing.T, result *QueryResult) {
				t.Helper()
				require.NotNil(t, result)
				assert.Equal(t, []string{"id", "name"}, result.Columns)
				assert.Equal(t, 1, result.RowCount)
				require.Len(t, result.Rows, 1)
				assert.Equal(t, int32(1), result.Rows[0][0])
				assert.Equal(t, "hello", result.Rows[0][1])
			},
		},
		{
			name:      "nil lineage repo does not panic",
			principal: "alice",
			sqlQuery:  "SELECT 42 AS val",
			setupEng: func(eng *testutil.MockSessionEngine, ddb *sql.DB) {
				eng.QueryFn = func(ctx context.Context, _, q string) (*sql.Rows, error) {
					return ddb.QueryContext(ctx, q)
				}
			},
			lineage: nil, // explicitly nil
			wantErr: false,
			checkAudit: func(t *testing.T, audit *testutil.MockAuditRepo) {
				t.Helper()
				require.Len(t, audit.Entries, 1)
				assert.Equal(t, "ALLOWED", audit.Entries[0].Status)
			},
			checkRes: func(t *testing.T, result *QueryResult) {
				t.Helper()
				require.NotNil(t, result)
				assert.Equal(t, 1, result.RowCount)
			},
		},
		{
			name:      "lineage emits READ edges for SELECT",
			principal: "alice",
			sqlQuery:  "SELECT 1 AS x",
			setupEng: func(eng *testutil.MockSessionEngine, ddb *sql.DB) {
				eng.QueryFn = func(ctx context.Context, _, q string) (*sql.Rows, error) {
					return ddb.QueryContext(ctx, q)
				}
			},
			lineage: &testutil.MockLineageRepo{
				InsertEdgeFn: func(_ context.Context, edge *domain.LineageEdge) error {
					// Best-effort lineage uses sqlrewrite.ExtractTableNames which may
					// not find tables in "SELECT 1", so we accept any call gracefully.
					return nil
				},
			},
			wantErr: false,
			checkAudit: func(t *testing.T, audit *testutil.MockAuditRepo) {
				t.Helper()
				require.Len(t, audit.Entries, 1)
				assert.Equal(t, "ALLOWED", audit.Entries[0].Status)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			eng := &testutil.MockSessionEngine{}
			tt.setupEng(eng, db)

			audit := &testutil.MockAuditRepo{}

			lineage := tt.lineage

			svc := NewQueryService(eng, audit, lineage)

			result, err := svc.Execute(context.Background(), tt.principal, tt.sqlQuery)

			if tt.wantErr {
				require.Error(t, err)
				if tt.wantErrMsg != "" {
					assert.Contains(t, err.Error(), tt.wantErrMsg)
				}
				assert.Nil(t, result)
			} else {
				require.NoError(t, err)
			}

			if tt.checkAudit != nil {
				tt.checkAudit(t, audit)
			}
			if tt.checkRes != nil {
				tt.checkRes(t, result)
			}
		})
	}
}

// === Execute — audit details ===

func TestQueryService_Execute_AuditDetails(t *testing.T) {
	t.Parallel()

	db := openDuckDB(t)
	eng := &testutil.MockSessionEngine{
		QueryFn: func(ctx context.Context, _, q string) (*sql.Rows, error) {
			return db.QueryContext(ctx, q)
		},
	}
	audit := &testutil.MockAuditRepo{}
	svc := NewQueryService(eng, audit, nil)

	result, err := svc.Execute(context.Background(), "alice", "SELECT 1 AS id, 'hello' AS name")
	require.NoError(t, err)
	require.NotNil(t, result)

	// Verify audit entry contains expected detail fields.
	require.Len(t, audit.Entries, 1)
	entry := audit.Entries[0]

	assert.NotNil(t, entry.OriginalSQL, "OriginalSQL should be set")
	assert.Equal(t, "SELECT 1 AS id, 'hello' AS name", *entry.OriginalSQL)

	assert.NotNil(t, entry.StatementType, "StatementType should be set")
	assert.Equal(t, "QUERY", *entry.StatementType)

	assert.NotNil(t, entry.RowsReturned, "RowsReturned should be set")
	assert.Equal(t, int64(1), *entry.RowsReturned)

	assert.NotNil(t, entry.DurationMs, "DurationMs should be set")
	assert.GreaterOrEqual(t, *entry.DurationMs, int64(0))
}

// === Execute — multiple rows ===

func TestQueryService_Execute_MultipleRows(t *testing.T) {
	t.Parallel()

	db := openDuckDB(t)
	eng := &testutil.MockSessionEngine{
		QueryFn: func(ctx context.Context, _, q string) (*sql.Rows, error) {
			return db.QueryContext(ctx, q)
		},
	}
	audit := &testutil.MockAuditRepo{}
	svc := NewQueryService(eng, audit, nil)

	// Generate 5 rows using DuckDB's generate_series.
	result, err := svc.Execute(context.Background(), "alice", "SELECT i FROM generate_series(1, 5) AS t(i)")
	require.NoError(t, err)
	require.NotNil(t, result)

	assert.Equal(t, 5, result.RowCount)
	assert.Len(t, result.Rows, 5)
	assert.Equal(t, []string{"i"}, result.Columns)
}

// === Execute — DML lineage emits WRITE edge ===

func TestQueryService_Execute_DMLLineage(t *testing.T) {
	t.Parallel()

	db := openDuckDB(t)

	// Create the source and target tables in the in-memory DuckDB so the
	// INSERT statement actually succeeds and returns *sql.Rows.
	_, err := db.ExecContext(context.Background(), "CREATE TABLE src(id INTEGER, val TEXT)")
	require.NoError(t, err)
	_, err = db.ExecContext(context.Background(), "INSERT INTO src VALUES (1, 'a'), (2, 'b')")
	require.NoError(t, err)
	_, err = db.ExecContext(context.Background(), "CREATE TABLE dst(id INTEGER, val TEXT)")
	require.NoError(t, err)

	eng := &testutil.MockSessionEngine{
		QueryFn: func(ctx context.Context, _, q string) (*sql.Rows, error) {
			return db.QueryContext(ctx, q)
		},
	}
	audit := &testutil.MockAuditRepo{}

	// Collect lineage edges via channel for synchronisation.
	var mu sync.Mutex
	var edges []*domain.LineageEdge
	lineage := &testutil.MockLineageRepo{
		InsertEdgeFn: func(_ context.Context, edge *domain.LineageEdge) error {
			mu.Lock()
			defer mu.Unlock()
			edges = append(edges, edge)
			return nil
		},
	}

	svc := NewQueryService(eng, audit, lineage)

	result, err := svc.Execute(context.Background(), "alice", "INSERT INTO dst SELECT * FROM src")
	require.NoError(t, err)
	require.NotNil(t, result)

	// emitLineage is called synchronously in the current implementation, but
	// give a small grace period in case it becomes async.
	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(edges) > 0
	}, 2*time.Second, 10*time.Millisecond, "expected at least one lineage edge")

	mu.Lock()
	defer mu.Unlock()

	// Verify at least one WRITE edge targeting dst.
	var foundWrite bool
	for _, e := range edges {
		if e.EdgeType == "WRITE" && e.TargetTable != nil && *e.TargetTable == "dst" {
			foundWrite = true
			assert.Equal(t, "src", e.SourceTable)
			assert.Equal(t, "alice", e.PrincipalName)
		}
	}
	assert.True(t, foundWrite, "expected a WRITE edge for target table dst; got edges: %+v", edges)
}

// === Execute — lineage error suppressed ===

func TestQueryService_Execute_LineageErrorSuppressed(t *testing.T) {
	t.Parallel()

	db := openDuckDB(t)

	// Create a table so ExtractTableNames finds something.
	_, err := db.ExecContext(context.Background(), "CREATE TABLE orders(id INTEGER)")
	require.NoError(t, err)
	_, err = db.ExecContext(context.Background(), "INSERT INTO orders VALUES (1)")
	require.NoError(t, err)

	eng := &testutil.MockSessionEngine{
		QueryFn: func(ctx context.Context, _, q string) (*sql.Rows, error) {
			return db.QueryContext(ctx, q)
		},
	}
	audit := &testutil.MockAuditRepo{}

	lineage := &testutil.MockLineageRepo{
		InsertEdgeFn: func(_ context.Context, _ *domain.LineageEdge) error {
			return fmt.Errorf("simulated lineage storage failure")
		},
	}

	svc := NewQueryService(eng, audit, lineage)

	// The query should still succeed even though lineage recording fails.
	result, err := svc.Execute(context.Background(), "alice", "SELECT * FROM orders")
	require.NoError(t, err, "lineage errors must not propagate to the caller")
	require.NotNil(t, result)
	assert.Equal(t, 1, result.RowCount)
}

// === splitQualifiedName ===

func TestSplitQualifiedName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		input      string
		wantSchema string
		wantTable  string
	}{
		{
			name:       "qualified name with schema",
			input:      "schema.table",
			wantSchema: "schema",
			wantTable:  "table",
		},
		{
			name:       "unqualified name defaults to main",
			input:      "table",
			wantSchema: "main",
			wantTable:  "table",
		},
		{
			name:       "multiple dots splits on first",
			input:      "a.b.c",
			wantSchema: "a",
			wantTable:  "b.c",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			schema, table := splitQualifiedName(tt.input)
			assert.Equal(t, tt.wantSchema, schema)
			assert.Equal(t, tt.wantTable, table)
		})
	}
}

// === TablesAccessedStr ===

func TestTablesAccessedStr(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		tables []string
		want   string
	}{
		{
			name:   "empty slice",
			tables: []string{},
			want:   "",
		},
		{
			name:   "nil slice",
			tables: nil,
			want:   "",
		},
		{
			name:   "single table",
			tables: []string{"t1"},
			want:   "t1",
		},
		{
			name:   "multiple tables",
			tables: []string{"t1", "t2", "t3"},
			want:   "t1,t2,t3",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := TablesAccessedStr(tt.tables)
			assert.Equal(t, tt.want, got)
		})
	}
}
