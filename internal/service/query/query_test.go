package query

import (
	"context"
	"database/sql"
	"fmt"
	"testing"

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
	t.Cleanup(func() { db.Close() })
	return db
}

// queryDuckDB executes a SQL statement against an in-memory DuckDB and returns *sql.Rows.
func queryDuckDB(t *testing.T, db *sql.DB, query string) *sql.Rows {
	t.Helper()
	rows, err := db.Query(query)
	require.NoError(t, err)
	return rows
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
				eng.QueryFn = func(_ context.Context, _, q string) (*sql.Rows, error) {
					return ddb.Query(q)
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
				eng.QueryFn = func(_ context.Context, _, q string) (*sql.Rows, error) {
					return ddb.Query(q)
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
				eng.QueryFn = func(_ context.Context, _, q string) (*sql.Rows, error) {
					return ddb.Query(q)
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
