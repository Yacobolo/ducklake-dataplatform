package model

import (
	"context"
	"database/sql"
	"io"
	"log/slog"
	"sort"
	"testing"

	"duck-demo/internal/domain"
	"duck-demo/internal/sqlrewrite"

	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type passthroughSessionEngine struct{}

func (passthroughSessionEngine) Query(ctx context.Context, _ string, sqlQuery string) (*sql.Rows, error) {
	panic("unexpected Query call in executor tests")
}

func (passthroughSessionEngine) QueryOnConn(ctx context.Context, conn *sql.Conn, _ string, sqlQuery string) (*sql.Rows, error) {
	return conn.QueryContext(ctx, sqlQuery)
}

func newDuckDBServiceForTest(t *testing.T) (*Service, *sql.DB) {
	t.Helper()
	db, err := sql.Open("duckdb", "")
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	_, err = db.Exec("CREATE SCHEMA analytics")
	require.NoError(t, err)

	return &Service{
		engine: passthroughSessionEngine{},
		duckDB: db,
		logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}, db
}

func TestCanDirectExecOnConn(t *testing.T) {
	tests := []struct {
		name     string
		stmtType sqlrewrite.StatementType
		query    string
		want     bool
	}{
		{name: "allow create view", stmtType: sqlrewrite.StmtDDL, query: "CREATE OR REPLACE VIEW main.v AS SELECT 1", want: true},
		{name: "allow create table", stmtType: sqlrewrite.StmtDDL, query: "CREATE OR REPLACE TABLE main.t AS SELECT 1", want: true},
		{name: "allow temp table", stmtType: sqlrewrite.StmtDDL, query: "CREATE TEMP TABLE _tmp AS SELECT 1", want: true},
		{name: "allow drop table", stmtType: sqlrewrite.StmtDDL, query: "DROP TABLE IF EXISTS _tmp", want: true},
		{name: "allow create macro", stmtType: sqlrewrite.StmtDDL, query: "CREATE OR REPLACE MACRO m(x) AS x + 1", want: true},
		{name: "allow set variable", stmtType: sqlrewrite.StmtOther, query: "SET VARIABLE load_window_days='7'", want: true},
		{name: "deny create schema", stmtType: sqlrewrite.StmtDDL, query: "CREATE SCHEMA analytics", want: false},
		{name: "deny drop schema", stmtType: sqlrewrite.StmtDDL, query: "DROP SCHEMA analytics", want: false},
		{name: "deny copy", stmtType: sqlrewrite.StmtOther, query: "COPY t TO 'x.parquet'", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := canDirectExecOnConn(tt.stmtType, tt.query)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestResolveIncrementalStrategy(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{name: "default merge", in: "", want: "merge"},
		{name: "normalized merge", in: " MERGE ", want: "merge"},
		{name: "delete insert alias", in: "delete+insert", want: "delete_insert"},
		{name: "delete insert canonical", in: "delete_insert", want: "delete_insert"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, resolveIncrementalStrategy(tt.in))
		})
	}
}

func TestResolveSchemaChangePolicy(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{name: "default ignore", in: "", want: "ignore"},
		{name: "normalize ignore", in: " IGNORE ", want: "ignore"},
		{name: "pass through fail", in: "fail", want: "fail"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, resolveSchemaChangePolicy(tt.in))
		})
	}
}

func TestSameColumns(t *testing.T) {
	assert.True(t, sameColumns([]string{"id", "name"}, []string{"id", "name"}))
	assert.False(t, sameColumns([]string{"id", "name"}, []string{"name", "id"}))
	assert.False(t, sameColumns([]string{"id"}, []string{"id", "name"}))
}

func TestEnforceIncrementalSchemaPolicy(t *testing.T) {
	t.Run("fail policy rejects schema drift", func(t *testing.T) {
		svc, db := newDuckDBServiceForTest(t)
		_, err := db.Exec(`CREATE TABLE analytics.orders (id INTEGER, amount INTEGER)`)
		require.NoError(t, err)

		conn, err := db.Conn(context.Background())
		require.NoError(t, err)
		defer func() { _ = conn.Close() }()

		err = svc.enforceIncrementalSchemaPolicy(
			context.Background(),
			conn,
			&domain.Model{
				ProjectName: "analytics",
				Name:        "orders",
				SQL:         "SELECT 1 AS id, 2 AS amount, 'x' AS extra_col",
				Config:      domain.ModelConfig{OnSchemaChange: "fail"},
			},
			ExecutionConfig{TargetSchema: "analytics"},
			"admin",
		)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "schema change detected for incremental model")
	})

	t.Run("fail policy allows stable schema", func(t *testing.T) {
		svc, db := newDuckDBServiceForTest(t)
		_, err := db.Exec(`CREATE TABLE analytics.orders (id INTEGER, amount INTEGER)`)
		require.NoError(t, err)

		conn, err := db.Conn(context.Background())
		require.NoError(t, err)
		defer func() { _ = conn.Close() }()

		err = svc.enforceIncrementalSchemaPolicy(
			context.Background(),
			conn,
			&domain.Model{
				ProjectName: "analytics",
				Name:        "orders",
				SQL:         "SELECT 1 AS id, 2 AS amount",
				Config:      domain.ModelConfig{OnSchemaChange: "fail"},
			},
			ExecutionConfig{TargetSchema: "analytics"},
			"admin",
		)
		require.NoError(t, err)
	})

	t.Run("unsupported policy returns validation error", func(t *testing.T) {
		svc, db := newDuckDBServiceForTest(t)
		_, err := db.Exec(`CREATE TABLE analytics.orders (id INTEGER, amount INTEGER)`)
		require.NoError(t, err)

		conn, err := db.Conn(context.Background())
		require.NoError(t, err)
		defer func() { _ = conn.Close() }()

		err = svc.enforceIncrementalSchemaPolicy(
			context.Background(),
			conn,
			&domain.Model{
				ProjectName: "analytics",
				Name:        "orders",
				SQL:         "SELECT 1 AS id, 2 AS amount",
				Config:      domain.ModelConfig{OnSchemaChange: "append_new_columns"},
			},
			ExecutionConfig{TargetSchema: "analytics"},
			"admin",
		)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unsupported on_schema_change")
	})
}

func TestMaterializeIncremental(t *testing.T) {
	t.Run("full refresh replaces target table contents", func(t *testing.T) {
		svc, db := newDuckDBServiceForTest(t)
		_, err := db.Exec(`CREATE TABLE analytics.orders (id INTEGER, amount INTEGER)`)
		require.NoError(t, err)
		_, err = db.Exec(`INSERT INTO analytics.orders VALUES (1, 10), (2, 20)`)
		require.NoError(t, err)

		conn, err := db.Conn(context.Background())
		require.NoError(t, err)
		defer func() { _ = conn.Close() }()

		rows, err := svc.materializeIncremental(
			context.Background(),
			conn,
			&domain.Model{
				ProjectName:     "analytics",
				Name:            "orders",
				SQL:             "SELECT * FROM (VALUES (3, 30)) AS src(id, amount)",
				Materialization: domain.MaterializationIncremental,
			},
			ExecutionConfig{TargetSchema: "analytics", FullRefresh: true},
			"admin",
		)
		require.NoError(t, err)
		assert.EqualValues(t, 1, rows)

		actual := queryOrderRows(t, db)
		require.Len(t, actual, 1)
		assert.Equal(t, [2]int64{3, 30}, actual[0])
	})

	t.Run("merge strategy updates and inserts", func(t *testing.T) {
		svc, db := newDuckDBServiceForTest(t)
		_, err := db.Exec(`CREATE TABLE analytics.orders (id INTEGER, amount INTEGER)`)
		require.NoError(t, err)
		_, err = db.Exec(`INSERT INTO analytics.orders VALUES (1, 10), (2, 20)`)
		require.NoError(t, err)

		conn, err := db.Conn(context.Background())
		require.NoError(t, err)
		defer func() { _ = conn.Close() }()

		rows, err := svc.materializeIncremental(
			context.Background(),
			conn,
			&domain.Model{
				ProjectName:     "analytics",
				Name:            "orders",
				SQL:             "SELECT * FROM (VALUES (2, 200), (3, 30)) AS src(id, amount)",
				Materialization: domain.MaterializationIncremental,
				Config: domain.ModelConfig{
					UniqueKey: []string{"id"},
				},
			},
			ExecutionConfig{TargetSchema: "analytics"},
			"admin",
		)
		require.NoError(t, err)
		assert.EqualValues(t, 3, rows)

		actual := queryOrderRows(t, db)
		require.Equal(t, [][2]int64{{1, 10}, {2, 200}, {3, 30}}, actual)

		rows, err = svc.materializeIncremental(
			context.Background(),
			conn,
			&domain.Model{
				ProjectName:     "analytics",
				Name:            "orders",
				SQL:             "SELECT * FROM (VALUES (2, 200), (3, 30)) AS src(id, amount)",
				Materialization: domain.MaterializationIncremental,
				Config: domain.ModelConfig{
					UniqueKey: []string{"id"},
				},
			},
			ExecutionConfig{TargetSchema: "analytics"},
			"admin",
		)
		require.NoError(t, err)
		assert.EqualValues(t, 3, rows)
		assert.Equal(t, [][2]int64{{1, 10}, {2, 200}, {3, 30}}, queryOrderRows(t, db))
	})

	t.Run("delete_insert strategy updates and inserts", func(t *testing.T) {
		svc, db := newDuckDBServiceForTest(t)
		_, err := db.Exec(`CREATE TABLE analytics.orders (id INTEGER, amount INTEGER)`)
		require.NoError(t, err)
		_, err = db.Exec(`INSERT INTO analytics.orders VALUES (1, 10), (2, 20)`)
		require.NoError(t, err)

		conn, err := db.Conn(context.Background())
		require.NoError(t, err)
		defer func() { _ = conn.Close() }()

		rows, err := svc.materializeIncremental(
			context.Background(),
			conn,
			&domain.Model{
				ProjectName:     "analytics",
				Name:            "orders",
				SQL:             "SELECT * FROM (VALUES (2, 200), (3, 30)) AS src(id, amount)",
				Materialization: domain.MaterializationIncremental,
				Config: domain.ModelConfig{
					UniqueKey:           []string{"id"},
					IncrementalStrategy: "delete_insert",
				},
			},
			ExecutionConfig{TargetSchema: "analytics"},
			"admin",
		)
		require.NoError(t, err)
		assert.EqualValues(t, 3, rows)
		assert.Equal(t, [][2]int64{{1, 10}, {2, 200}, {3, 30}}, queryOrderRows(t, db))
	})
}

func queryOrderRows(t *testing.T, db *sql.DB) [][2]int64 {
	t.Helper()

	rows, err := db.Query(`SELECT id, amount FROM analytics.orders`)
	require.NoError(t, err)
	defer func() { _ = rows.Close() }()

	out := make([][2]int64, 0)
	for rows.Next() {
		var id int64
		var amount int64
		require.NoError(t, rows.Scan(&id, &amount))
		out = append(out, [2]int64{id, amount})
	}
	require.NoError(t, rows.Err())

	sort.Slice(out, func(i, j int) bool {
		return out[i][0] < out[j][0]
	})
	return out
}
