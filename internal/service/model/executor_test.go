package model

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"sort"
	"testing"

	"github.com/Yacobolo/quackstack/internal/domain"
	"github.com/Yacobolo/quackstack/internal/sqlrewrite"

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

type ddlRejectingSessionEngine struct {
	queries []string
}

func (*ddlRejectingSessionEngine) Query(context.Context, string, string) (*sql.Rows, error) {
	panic("unexpected Query call in executor tests")
}

func (e *ddlRejectingSessionEngine) QueryOnConn(ctx context.Context, conn *sql.Conn, _ string, sqlQuery string) (*sql.Rows, error) {
	e.queries = append(e.queries, sqlQuery)
	stmtType, err := sqlrewrite.ClassifyStatement(sqlQuery)
	if err != nil {
		return nil, err
	}
	if stmtType == sqlrewrite.StmtDDL {
		return nil, fmt.Errorf("DDL statements are not allowed through the query engine")
	}
	return conn.QueryContext(ctx, sqlQuery)
}

func newDuckDBServiceForTest(t *testing.T) (*Service, *sql.DB) {
	t.Helper()
	db, err := sql.Open("duckdb", "")
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	_, err = db.ExecContext(context.Background(), "CREATE SCHEMA analytics")
	require.NoError(t, err)

	return &Service{
		engine: passthroughSessionEngine{},
		duckDB: db,
		logger: slog.New(slog.DiscardHandler),
	}, db
}

type executorMacroRepoStub struct {
	all []domain.Macro
}

func (s executorMacroRepoStub) Create(context.Context, *domain.Macro) (*domain.Macro, error) {
	panic("unexpected call")
}

func (s executorMacroRepoStub) GetByName(context.Context, string) (*domain.Macro, error) {
	panic("unexpected call")
}

func (s executorMacroRepoStub) List(context.Context, domain.PageRequest) ([]domain.Macro, int64, error) {
	panic("unexpected call")
}

func (s executorMacroRepoStub) Update(context.Context, string, domain.UpdateMacroRequest) (*domain.Macro, error) {
	panic("unexpected call")
}

func (s executorMacroRepoStub) Delete(context.Context, string) error {
	panic("unexpected call")
}

func (s executorMacroRepoStub) ListAll(context.Context) ([]domain.Macro, error) {
	return append([]domain.Macro(nil), s.all...), nil
}

func (s executorMacroRepoStub) ListRevisions(context.Context, string) ([]domain.MacroRevision, error) {
	panic("unexpected call")
}

func (s executorMacroRepoStub) GetRevisionByVersion(context.Context, string, int) (*domain.MacroRevision, error) {
	panic("unexpected call")
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

func TestExecuteSingleModel_DDLBypassesQueryEngine(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name            string
		materialization string
		wantQueryCount  int
	}{
		{name: "view", materialization: domain.MaterializationView, wantQueryCount: 0},
		{name: "table", materialization: domain.MaterializationTable, wantQueryCount: 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			engine := &ddlRejectingSessionEngine{}
			db, err := sql.Open("duckdb", "")
			require.NoError(t, err)
			t.Cleanup(func() { _ = db.Close() })

			_, err = db.ExecContext(context.Background(), "CREATE SCHEMA analytics")
			require.NoError(t, err)

			svc := &Service{
				engine: engine,
				duckDB: db,
				logger: slog.New(slog.DiscardHandler),
			}

			rowsAffected, err := svc.executeSingleModel(
				context.Background(),
				&domain.Model{
					ProjectName:     "analytics",
					Name:            "orders",
					SQL:             "SELECT 1 AS id",
					Materialization: tc.materialization,
				},
				ExecutionConfig{TargetSchema: "analytics"},
				"admin",
				slog.New(slog.DiscardHandler),
			)
			require.NoError(t, err)

			if tc.materialization == domain.MaterializationTable {
				require.NotNil(t, rowsAffected)
				assert.EqualValues(t, 1, *rowsAffected)
			} else {
				assert.Nil(t, rowsAffected)
			}

			require.Len(t, engine.queries, tc.wantQueryCount)
			for _, query := range engine.queries {
				assert.NotContains(t, query, "CREATE OR REPLACE")
			}
		})
	}
}

func TestVerifyMacrosLoadable_SkipsCompileTimeNamespacedMacros(t *testing.T) {
	t.Parallel()

	db, err := sql.Open("duckdb", "")
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	svc := &Service{
		engine: passthroughSessionEngine{},
		duckDB: db,
		logger: slog.New(slog.DiscardHandler),
		macros: executorMacroRepoStub{
			all: []domain.Macro{
				{
					Name:       "utils.cents_to_dollars",
					MacroType:  domain.MacroTypeScalar,
					Parameters: []string{"col"},
					Body:       "\"(\" + col + \" / 100.0)\"",
				},
				{
					Name:       "double_val",
					MacroType:  domain.MacroTypeScalar,
					Parameters: []string{"x"},
					Body:       "x * 2",
				},
			},
		},
	}

	require.NoError(t, svc.verifyMacrosLoadable(context.Background(), "admin"))

	row := db.QueryRowContext(context.Background(), "SELECT double_val(3)")
	var got int64
	require.NoError(t, row.Scan(&got))
	assert.EqualValues(t, 6, got)

	_, err = db.ExecContext(context.Background(), "SELECT utils.cents_to_dollars(100)")
	require.Error(t, err)
}

func TestResolveIncrementalStrategy(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{name: "default merge", in: "", want: "merge"},
		{name: "normalized merge", in: " MERGE ", want: "merge"},
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

func TestResolveEphemeralModels_RewritesRefsToCTEs(t *testing.T) {
	t.Parallel()

	models := []domain.Model{
		{
			ProjectName:     "analytics",
			Name:            "stg_orders_star",
			Materialization: domain.MaterializationEphemeral,
			SQL:             `select * from "e2e"."raw"."orders"`,
		},
		{
			ProjectName:     "analytics",
			Name:            "stg_orders",
			Materialization: domain.MaterializationEphemeral,
			DependsOn:       []string{"analytics.stg_orders_star"},
			SQL:             `select order_id from "e2e"."app_dev"."stg_orders_star"`,
		},
		{
			ProjectName:     "analytics",
			Name:            "fct_orders",
			Materialization: domain.MaterializationTable,
			DependsOn:       []string{"analytics.stg_orders"},
			SQL:             `select * from "e2e"."app_dev"."stg_orders"`,
		},
	}

	resolved := resolveEphemeralModels(models, "e2e", "app_dev")
	require.Len(t, resolved, 1)
	assert.Equal(t, "fct_orders", resolved[0].Name)
	assert.Equal(t, `WITH "stg_orders_star" AS (select * from "e2e"."raw"."orders"), "stg_orders" AS (select order_id from "stg_orders_star") select * from "stg_orders"`, resolved[0].SQL)
}

func TestSameColumns(t *testing.T) {
	assert.True(t, sameColumns([]string{"id", "name"}, []string{"id", "name"}))
	assert.False(t, sameColumns([]string{"id", "name"}, []string{"name", "id"}))
	assert.False(t, sameColumns([]string{"id"}, []string{"id", "name"}))
}

func TestEnforceIncrementalSchemaPolicy(t *testing.T) {
	t.Run("fail policy rejects schema drift", func(t *testing.T) {
		svc, db := newDuckDBServiceForTest(t)
		_, err := db.ExecContext(context.Background(), `CREATE TABLE analytics.orders (id INTEGER, amount INTEGER)`)
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
		_, err := db.ExecContext(context.Background(), `CREATE TABLE analytics.orders (id INTEGER, amount INTEGER)`)
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
		_, err := db.ExecContext(context.Background(), `CREATE TABLE analytics.orders (id INTEGER, amount INTEGER)`)
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
		_, err := db.ExecContext(context.Background(), `CREATE TABLE analytics.orders (id INTEGER, amount INTEGER)`)
		require.NoError(t, err)
		_, err = db.ExecContext(context.Background(), `INSERT INTO analytics.orders VALUES (1, 10), (2, 20)`)
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
		_, err := db.ExecContext(context.Background(), `CREATE TABLE analytics.orders (id INTEGER, amount INTEGER)`)
		require.NoError(t, err)
		_, err = db.ExecContext(context.Background(), `INSERT INTO analytics.orders VALUES (1, 10), (2, 20)`)
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
		_, err := db.ExecContext(context.Background(), `CREATE TABLE analytics.orders (id INTEGER, amount INTEGER)`)
		require.NoError(t, err)
		_, err = db.ExecContext(context.Background(), `INSERT INTO analytics.orders VALUES (1, 10), (2, 20)`)
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

func TestMaterializeSeed_UsesTableSemantics(t *testing.T) {
	svc, db := newDuckDBServiceForTest(t)
	conn, err := db.Conn(context.Background())
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

	rows, err := svc.materializeSeed(
		context.Background(),
		conn,
		&domain.Model{
			ProjectName:     "analytics",
			Name:            "seed_orders",
			SQL:             "SELECT * FROM (VALUES (1, 'new'), (2, 'paid')) AS src(id, status)",
			Materialization: domain.MaterializationSeed,
		},
		ExecutionConfig{TargetSchema: "analytics"},
		"admin",
	)
	require.NoError(t, err)
	assert.EqualValues(t, 2, rows)

	var cnt int
	err = db.QueryRowContext(context.Background(), `SELECT COUNT(*) FROM analytics.seed_orders`).Scan(&cnt)
	require.NoError(t, err)
	assert.Equal(t, 2, cnt)
}

func TestMaterializeSnapshot_SCD2Baseline(t *testing.T) {
	t.Run("first run creates current rows", func(t *testing.T) {
		svc, db := newDuckDBServiceForTest(t)
		conn, err := db.Conn(context.Background())
		require.NoError(t, err)
		defer func() { _ = conn.Close() }()

		rows, err := svc.materializeSnapshot(
			context.Background(),
			conn,
			&domain.Model{
				ProjectName:     "analytics",
				Name:            "snap_orders",
				SQL:             "SELECT * FROM (VALUES (1, 'new'), (2, 'paid')) AS src(id, status)",
				Materialization: domain.MaterializationSnapshot,
				Config:          domain.ModelConfig{UniqueKey: []string{"id"}},
			},
			ExecutionConfig{TargetSchema: "analytics"},
			"admin",
		)
		require.NoError(t, err)
		assert.EqualValues(t, 2, rows)

		var currentCount int
		err = db.QueryRowContext(context.Background(), `SELECT COUNT(*) FROM analytics.snap_orders WHERE dbt_is_current = TRUE`).Scan(&currentCount)
		require.NoError(t, err)
		assert.Equal(t, 2, currentCount)
	})

	t.Run("second run expires changed rows and inserts new version", func(t *testing.T) {
		svc, db := newDuckDBServiceForTest(t)
		conn, err := db.Conn(context.Background())
		require.NoError(t, err)
		defer func() { _ = conn.Close() }()

		_, err = svc.materializeSnapshot(
			context.Background(),
			conn,
			&domain.Model{
				ProjectName:     "analytics",
				Name:            "snap_orders",
				SQL:             "SELECT * FROM (VALUES (1, 'new'), (2, 'paid')) AS src(id, status)",
				Materialization: domain.MaterializationSnapshot,
				Config:          domain.ModelConfig{UniqueKey: []string{"id"}},
			},
			ExecutionConfig{TargetSchema: "analytics"},
			"admin",
		)
		require.NoError(t, err)

		rows, err := svc.materializeSnapshot(
			context.Background(),
			conn,
			&domain.Model{
				ProjectName:     "analytics",
				Name:            "snap_orders",
				SQL:             "SELECT * FROM (VALUES (1, 'new'), (2, 'refunded'), (3, 'paid')) AS src(id, status)",
				Materialization: domain.MaterializationSnapshot,
				Config:          domain.ModelConfig{UniqueKey: []string{"id"}},
			},
			ExecutionConfig{TargetSchema: "analytics"},
			"admin",
		)
		require.NoError(t, err)
		assert.EqualValues(t, 4, rows)

		var total, current int
		err = db.QueryRowContext(context.Background(), `SELECT COUNT(*), SUM(CASE WHEN dbt_is_current THEN 1 ELSE 0 END) FROM analytics.snap_orders`).Scan(&total, &current)
		require.NoError(t, err)
		assert.Equal(t, 4, total)
		assert.Equal(t, 3, current)

		var historical int
		err = db.QueryRowContext(context.Background(), `SELECT COUNT(*) FROM analytics.snap_orders WHERE id = 2 AND dbt_is_current = FALSE AND dbt_valid_to IS NOT NULL`).Scan(&historical)
		require.NoError(t, err)
		assert.Equal(t, 1, historical)
	})
}

func queryOrderRows(t *testing.T, db *sql.DB) [][2]int64 {
	t.Helper()

	rows, err := db.QueryContext(context.Background(), `SELECT id, amount FROM analytics.orders`)
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
