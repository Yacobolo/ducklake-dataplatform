package flightsql

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/array"
	arrowflight "github.com/apache/arrow-go/v18/arrow/flight"
	arrowflightsql "github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	grpcHealthV1 "google.golang.org/grpc/health/grpc_health_v1"
)

func TestServer_StartAndShutdown(t *testing.T) {
	srv := NewServer("127.0.0.1:0", nil, nil)
	require.NoError(t, srv.Start())
	t.Cleanup(func() {
		_ = srv.Shutdown(context.Background())
	})

	addr := srv.Addr()
	require.NotEmpty(t, addr)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	conn, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	healthClient := grpcHealthV1.NewHealthClient(conn)
	resp, err := healthClient.Check(ctx, &grpcHealthV1.HealthCheckRequest{})
	require.NoError(t, err)
	require.Equal(t, grpcHealthV1.HealthCheckResponse_SERVING, resp.GetStatus())

	require.NoError(t, srv.Shutdown(ctx))
}

func TestServer_ExecuteStatementQuery(t *testing.T) {
	srv := NewServer("127.0.0.1:0", nil, func(_ context.Context, principal string, sqlQuery string) (*QueryResult, error) {
		require.Equal(t, "anonymous", principal)
		require.Equal(t, "SELECT 1", sqlQuery)
		return &QueryResult{
			Columns: []string{"value"},
			Rows:    [][]interface{}{{"1"}},
		}, nil
	})
	require.NoError(t, srv.Start())
	t.Cleanup(func() {
		_ = srv.Shutdown(context.Background())
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	client, err := arrowflightsql.NewClient(
		srv.Addr(),
		nil,
		nil,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = client.Close()
	})

	info, err := client.Execute(ctx, "SELECT 1")
	require.NoError(t, err)
	require.Len(t, info.Endpoint, 1)
	require.NotNil(t, info.Endpoint[0].Ticket)

	rdr, err := client.DoGet(ctx, info.Endpoint[0].Ticket)
	require.NoError(t, err)
	t.Cleanup(func() {
		rdr.Release()
	})

	require.True(t, rdr.Next())
	rec := rdr.Record()
	require.Equal(t, int64(1), rec.NumRows())
	require.Equal(t, int64(1), rec.NumCols())

	col, ok := rec.Column(0).(*array.String)
	require.True(t, ok)
	require.Equal(t, "1", col.Value(0))
	require.False(t, rdr.Next())
}

func TestServer_GetSqlInfo(t *testing.T) {
	srv := NewServer("127.0.0.1:0", nil, func(_ context.Context, _ string, _ string) (*QueryResult, error) {
		return &QueryResult{}, nil
	})
	require.NoError(t, srv.Start())
	t.Cleanup(func() {
		_ = srv.Shutdown(context.Background())
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	client, err := arrowflightsql.NewClient(
		srv.Addr(),
		nil,
		nil,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = client.Close()
	})

	info, err := client.GetSqlInfo(ctx, []arrowflightsql.SqlInfo{arrowflightsql.SqlInfoFlightSqlServerName, arrowflightsql.SqlInfoFlightSqlServerSql})
	require.NoError(t, err)
	require.NotEmpty(t, info.Endpoint)

	rdr, err := client.DoGet(ctx, info.Endpoint[0].Ticket)
	require.NoError(t, err)
	t.Cleanup(func() {
		rdr.Release()
	})

	require.True(t, rdr.Next())
	require.GreaterOrEqual(t, rdr.Record().NumRows(), int64(1))
}

func TestServer_CancelStatementQuery(t *testing.T) {
	srv := NewServer("127.0.0.1:0", nil, func(_ context.Context, _ string, _ string) (*QueryResult, error) {
		return &QueryResult{
			Columns: []string{"value"},
			Rows:    [][]interface{}{{"1"}},
		}, nil
	})
	require.NoError(t, srv.Start())
	t.Cleanup(func() {
		_ = srv.Shutdown(context.Background())
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	client, err := arrowflightsql.NewClient(
		srv.Addr(),
		nil,
		nil,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = client.Close()
	})

	info, err := client.Execute(ctx, "SELECT 1")
	require.NoError(t, err)

	status, err := client.CancelQuery(ctx, info)
	require.NoError(t, err)
	require.Equal(t, arrowflightsql.CancelResultCancelled, status)

	_, err = client.DoGet(ctx, info.Endpoint[0].Ticket)
	require.Error(t, err)
	require.Contains(t, err.Error(), "query canceled")
}

func TestServer_MetadataDiscovery(t *testing.T) {
	srv := NewServer("127.0.0.1:0", nil, func(_ context.Context, _ string, sqlQuery string) (*QueryResult, error) {
		switch {
		case strings.Contains(sqlQuery, "DISTINCT table_catalog"):
			return &QueryResult{Columns: []string{"table_catalog"}, Rows: [][]interface{}{{"main"}}}, nil
		case strings.Contains(sqlQuery, "DISTINCT table_catalog, table_schema"):
			return &QueryResult{Columns: []string{"table_catalog", "table_schema"}, Rows: [][]interface{}{{"main", "public"}}}, nil
		case strings.Contains(sqlQuery, "DISTINCT table_type"):
			return &QueryResult{Columns: []string{"table_type"}, Rows: [][]interface{}{{"BASE TABLE"}}}, nil
		default:
			return &QueryResult{}, nil
		}
	})
	require.NoError(t, srv.Start())
	t.Cleanup(func() {
		_ = srv.Shutdown(context.Background())
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	client, err := arrowflightsql.NewClient(
		srv.Addr(),
		nil,
		nil,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = client.Close()
	})

	catalogs, err := client.GetCatalogs(ctx)
	require.NoError(t, err)
	require.NotEmpty(t, catalogs.Endpoint)

	catalogReader, err := client.DoGet(ctx, catalogs.Endpoint[0].Ticket)
	require.NoError(t, err)
	t.Cleanup(func() { catalogReader.Release() })
	require.True(t, catalogReader.Next())
	require.Equal(t, int64(1), catalogReader.Record().NumRows())

	schemas, err := client.GetDBSchemas(ctx, nil)
	require.NoError(t, err)
	require.NotEmpty(t, schemas.Endpoint)

	schemaReader, err := client.DoGet(ctx, schemas.Endpoint[0].Ticket)
	require.NoError(t, err)
	t.Cleanup(func() { schemaReader.Release() })
	require.True(t, schemaReader.Next())
	require.Equal(t, int64(1), schemaReader.Record().NumRows())

	tableTypes, err := client.GetTableTypes(ctx)
	require.NoError(t, err)
	require.NotEmpty(t, tableTypes.Endpoint)

	typesReader, err := client.DoGet(ctx, tableTypes.Endpoint[0].Ticket)
	require.NoError(t, err)
	t.Cleanup(func() { typesReader.Release() })
	require.True(t, typesReader.Next())
	require.Equal(t, int64(1), typesReader.Record().NumRows())
}

func TestServer_GetTables(t *testing.T) {
	srv := NewServer("127.0.0.1:0", nil, func(_ context.Context, principal string, sqlQuery string) (*QueryResult, error) {
		require.Equal(t, "anonymous", principal)
		if strings.Contains(sqlQuery, "information_schema.tables") {
			return &QueryResult{
				Columns: []string{"table_catalog", "table_schema", "table_name", "table_type"},
				Rows:    [][]interface{}{{"main", "public", "orders", "BASE TABLE"}},
			}, nil
		}
		if strings.Contains(sqlQuery, "information_schema.columns") {
			return &QueryResult{
				Columns: []string{"column_name", "data_type", "is_nullable", "numeric_precision", "numeric_scale", "column_default", "remarks"},
				Rows: [][]interface{}{
					{"id", "INTEGER", "NO", int64(32), int64(0), "nextval('orders_id_seq')", "primary key"},
					{"total", "DECIMAL(10,2)", "YES", int64(10), int64(2), nil, "order total"},
					{"created_at", "TIMESTAMP", "NO", nil, nil, nil, "creation time"},
				},
			}, nil
		}
		return &QueryResult{
			Columns: nil,
			Rows:    nil,
		}, nil
	})
	require.NoError(t, srv.Start())
	t.Cleanup(func() {
		_ = srv.Shutdown(context.Background())
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	client, err := arrowflightsql.NewClient(
		srv.Addr(),
		nil,
		nil,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = client.Close()
	})

	info, err := client.GetTables(ctx, &arrowflightsql.GetTablesOpts{IncludeSchema: true})
	require.NoError(t, err)
	require.NotEmpty(t, info.Endpoint)

	rdr, err := client.DoGet(ctx, info.Endpoint[0].Ticket)
	require.NoError(t, err)
	t.Cleanup(func() {
		rdr.Release()
	})

	require.True(t, rdr.Next())
	rec := rdr.Record()
	require.Equal(t, int64(1), rec.NumRows())
	require.Equal(t, int64(5), rec.NumCols())

	tableNameCol, ok := rec.Column(2).(*array.String)
	require.True(t, ok)
	require.Equal(t, "orders", tableNameCol.Value(0))

	tableSchemaCol, ok := rec.Column(4).(*array.Binary)
	require.True(t, ok)
	require.NotEmpty(t, tableSchemaCol.Value(0))

	tableSchema, err := arrowflight.DeserializeSchema(tableSchemaCol.Value(0), memory.DefaultAllocator)
	require.NoError(t, err)
	require.Len(t, tableSchema.Fields(), 3)

	idField := tableSchema.Field(0)
	require.Equal(t, "id", idField.Name)
	typeName, ok := idField.Metadata.GetValue(arrowflightsql.TypeNameKey)
	require.True(t, ok)
	require.Equal(t, "INTEGER", typeName)
	autoIncrement, ok := idField.Metadata.GetValue(arrowflightsql.IsAutoIncrementKey)
	require.True(t, ok)
	require.Equal(t, "1", autoIncrement)
	tableName, ok := idField.Metadata.GetValue(arrowflightsql.TableNameKey)
	require.True(t, ok)
	require.Equal(t, "orders", tableName)
	remarks, ok := idField.Metadata.GetValue(arrowflightsql.RemarksKey)
	require.True(t, ok)
	require.Equal(t, "primary key", remarks)

	totalField := tableSchema.Field(1)
	require.Equal(t, "total", totalField.Name)
	totalTypeName, ok := totalField.Metadata.GetValue(arrowflightsql.TypeNameKey)
	require.True(t, ok)
	require.Equal(t, "DECIMAL(10,2)", totalTypeName)
	precision, ok := totalField.Metadata.GetValue(arrowflightsql.PrecisionKey)
	require.True(t, ok)
	require.Equal(t, "10", precision)
	scale, ok := totalField.Metadata.GetValue(arrowflightsql.ScaleKey)
	require.True(t, ok)
	require.Equal(t, "2", scale)

	createdAtField := tableSchema.Field(2)
	require.Equal(t, "created_at", createdAtField.Name)
	createdAtPrecision, ok := createdAtField.Metadata.GetValue(arrowflightsql.PrecisionKey)
	require.True(t, ok)
	require.Equal(t, "6", createdAtPrecision)
	createdAtScale, ok := createdAtField.Metadata.GetValue(arrowflightsql.ScaleKey)
	require.True(t, ok)
	require.Equal(t, "6", createdAtScale)
	createdAtRemarks, ok := createdAtField.Metadata.GetValue(arrowflightsql.RemarksKey)
	require.True(t, ok)
	require.Equal(t, "creation time", createdAtRemarks)
}

func TestServer_GetSchemaTables(t *testing.T) {
	srv := NewServer("127.0.0.1:0", nil, func(_ context.Context, _ string, _ string) (*QueryResult, error) {
		return &QueryResult{}, nil
	})
	require.NoError(t, srv.Start())
	t.Cleanup(func() {
		_ = srv.Shutdown(context.Background())
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	client, err := arrowflightsql.NewClient(
		srv.Addr(),
		nil,
		nil,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = client.Close()
	})

	schemaResp, err := client.GetTablesSchema(ctx, &arrowflightsql.GetTablesOpts{IncludeSchema: true})
	require.NoError(t, err)
	schema, err := arrowflight.DeserializeSchema(schemaResp.Schema, memory.DefaultAllocator)
	require.NoError(t, err)
	require.Len(t, schema.Fields(), 5)
	require.Equal(t, "table_schema", schema.Field(4).Name)

	noSchemaResp, err := client.GetTablesSchema(ctx, &arrowflightsql.GetTablesOpts{IncludeSchema: false})
	require.NoError(t, err)
	noSchema, err := arrowflight.DeserializeSchema(noSchemaResp.Schema, memory.DefaultAllocator)
	require.NoError(t, err)
	require.Len(t, noSchema.Fields(), 4)
}

func TestServer_GetTables_RemarksFallbackQuery(t *testing.T) {
	remarksQueryAttempts := 0
	srv := NewServer("127.0.0.1:0", nil, func(_ context.Context, _ string, sqlQuery string) (*QueryResult, error) {
		if strings.Contains(sqlQuery, "information_schema.tables") {
			return &QueryResult{
				Columns: []string{"table_catalog", "table_schema", "table_name", "table_type"},
				Rows:    [][]interface{}{{"main", "public", "orders", "BASE TABLE"}},
			}, nil
		}
		if strings.Contains(sqlQuery, "COALESCE(comment, '')") {
			remarksQueryAttempts++
			return nil, fmt.Errorf("binder: column comment does not exist")
		}
		if strings.Contains(sqlQuery, "information_schema.columns") {
			return &QueryResult{
				Columns: []string{"column_name", "data_type", "is_nullable", "numeric_precision", "numeric_scale", "column_default", "remarks"},
				Rows: [][]interface{}{
					{"id", "INTEGER", "NO", int64(32), int64(0), nil, ""},
				},
			}, nil
		}
		return &QueryResult{}, nil
	})
	require.NoError(t, srv.Start())
	t.Cleanup(func() {
		_ = srv.Shutdown(context.Background())
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	client, err := arrowflightsql.NewClient(
		srv.Addr(),
		nil,
		nil,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = client.Close()
	})

	info, err := client.GetTables(ctx, &arrowflightsql.GetTablesOpts{IncludeSchema: true})
	require.NoError(t, err)
	require.NotEmpty(t, info.Endpoint)

	rdr, err := client.DoGet(ctx, info.Endpoint[0].Ticket)
	require.NoError(t, err)
	t.Cleanup(func() {
		rdr.Release()
	})
	require.True(t, rdr.Next())
	require.Equal(t, 1, remarksQueryAttempts)
}
