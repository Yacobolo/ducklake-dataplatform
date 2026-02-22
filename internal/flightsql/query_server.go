package flightsql

import (
	"context"
	"fmt"
	"log/slog"
	"regexp"
	"strconv"
	"strings"
	"sync"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	arrowflight "github.com/apache/arrow-go/v18/arrow/flight"
	arrowflightsql "github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/google/uuid"
	"google.golang.org/grpc/metadata"
)

type QueryResult struct {
	Columns []string
	Rows    [][]interface{}
}

type QueryExecutor func(ctx context.Context, principal string, sqlQuery string) (*QueryResult, error)

type queryServer struct {
	arrowflightsql.BaseServer

	query QueryExecutor

	mu      sync.Mutex
	tickets map[string]*QueryResult
}

func newQueryServer(_ string, _ *slog.Logger, query QueryExecutor) *queryServer {
	srv := &queryServer{query: query, tickets: make(map[string]*QueryResult)}
	_ = srv.RegisterSqlInfo(arrowflightsql.SqlInfoFlightSqlServerName, "duck-demo")
	_ = srv.RegisterSqlInfo(arrowflightsql.SqlInfoFlightSqlServerVersion, "dev")
	_ = srv.RegisterSqlInfo(arrowflightsql.SqlInfoFlightSqlServerArrowVersion, "18")
	_ = srv.RegisterSqlInfo(arrowflightsql.SqlInfoFlightSqlServerSql, true)
	_ = srv.RegisterSqlInfo(arrowflightsql.SqlInfoFlightSqlServerReadOnly, false)
	_ = srv.RegisterSqlInfo(arrowflightsql.SqlInfoFlightSqlServerCancel, true)
	return srv
}

func (s *queryServer) GetFlightInfoStatement(ctx context.Context, stmt arrowflightsql.StatementQuery, desc *arrowflight.FlightDescriptor) (*arrowflight.FlightInfo, error) {
	principal := principalFromContext(ctx)
	result, err := s.query(ctx, principal, stmt.GetQuery())
	if err != nil {
		return nil, err
	}

	handle := uuid.NewString()
	s.mu.Lock()
	s.tickets[handle] = result
	s.mu.Unlock()

	ticket, err := arrowflightsql.CreateStatementQueryTicket([]byte(handle))
	if err != nil {
		return nil, fmt.Errorf("create statement query ticket: %w", err)
	}

	schema := schemaFromColumns(result.Columns)
	return &arrowflight.FlightInfo{
		Schema:           arrowflight.SerializeSchema(schema, memory.DefaultAllocator),
		FlightDescriptor: desc,
		Endpoint: []*arrowflight.FlightEndpoint{{
			Ticket: &arrowflight.Ticket{Ticket: ticket},
			Location: []*arrowflight.Location{{
				Uri: arrowflight.LocationReuseConnection,
			}},
		}},
		TotalRecords: int64(len(result.Rows)),
		TotalBytes:   -1,
		Ordered:      true,
	}, nil
}

func (s *queryServer) GetFlightInfoTables(_ context.Context, req arrowflightsql.GetTables, desc *arrowflight.FlightDescriptor) (*arrowflight.FlightInfo, error) {
	schema := tablesSchema(req.GetIncludeSchema())
	return &arrowflight.FlightInfo{
		Schema:           arrowflight.SerializeSchema(schema, memory.DefaultAllocator),
		FlightDescriptor: desc,
		Endpoint: []*arrowflight.FlightEndpoint{{
			Ticket: &arrowflight.Ticket{Ticket: desc.Cmd},
			Location: []*arrowflight.Location{{
				Uri: arrowflight.LocationReuseConnection,
			}},
		}},
		TotalRecords: -1,
		TotalBytes:   -1,
		Ordered:      true,
	}, nil
}

func (s *queryServer) GetSchemaTables(_ context.Context, req arrowflightsql.GetTables, _ *arrowflight.FlightDescriptor) (*arrowflight.SchemaResult, error) {
	return &arrowflight.SchemaResult{Schema: arrowflight.SerializeSchema(tablesSchema(req.GetIncludeSchema()), memory.DefaultAllocator)}, nil
}

func (s *queryServer) DoGetTables(ctx context.Context, req arrowflightsql.GetTables) (*arrow.Schema, <-chan arrowflight.StreamChunk, error) {
	principal := principalFromContext(ctx)
	result, err := s.query(ctx, principal, buildTablesQuery(req))
	if err != nil {
		return nil, nil, err
	}

	schema := tablesSchema(req.GetIncludeSchema())
	tableSchemas := map[string][]byte{}
	if req.GetIncludeSchema() {
		tableSchemas, err = s.loadTableSchemas(ctx, principal, result)
		if err != nil {
			return nil, nil, err
		}
	}
	record, err := recordFromTablesResult(schema, result, req.GetIncludeSchema(), tableSchemas)
	if err != nil {
		return nil, nil, err
	}
	return streamSingleRecord(ctx, schema, record)
}

func (s *queryServer) GetSchemaStatement(ctx context.Context, stmt arrowflightsql.StatementQuery, _ *arrowflight.FlightDescriptor) (*arrowflight.SchemaResult, error) {
	principal := principalFromContext(ctx)
	result, err := s.query(ctx, principal, stmt.GetQuery())
	if err != nil {
		return nil, err
	}

	schema := schemaFromColumns(result.Columns)
	return &arrowflight.SchemaResult{Schema: arrowflight.SerializeSchema(schema, memory.DefaultAllocator)}, nil
}

func (s *queryServer) DoGetStatement(ctx context.Context, queryTicket arrowflightsql.StatementQueryTicket) (*arrow.Schema, <-chan arrowflight.StreamChunk, error) {
	handle := string(queryTicket.GetStatementHandle())

	s.mu.Lock()
	result, ok := s.tickets[handle]
	if ok {
		delete(s.tickets, handle)
	}
	s.mu.Unlock()
	if !ok {
		return nil, nil, fmt.Errorf("unknown statement handle")
	}

	schema := schemaFromColumns(result.Columns)
	record, err := recordFromResult(schema, result)
	if err != nil {
		return nil, nil, err
	}
	return streamSingleRecord(ctx, schema, record)
}

func schemaFromColumns(columns []string) *arrow.Schema {
	fields := make([]arrow.Field, len(columns))
	for i, column := range columns {
		name := strings.TrimSpace(column)
		if name == "" {
			name = fmt.Sprintf("column_%d", i+1)
		}
		fields[i] = arrow.Field{Name: name, Type: arrow.BinaryTypes.String, Nullable: true}
	}
	return arrow.NewSchema(fields, nil)
}

func recordFromResult(schema *arrow.Schema, result *QueryResult) (arrow.Record, error) {
	builders := make([]*array.StringBuilder, len(result.Columns))
	for i := range result.Columns {
		builders[i] = array.NewStringBuilder(memory.DefaultAllocator)
	}

	for _, row := range result.Rows {
		for i := range result.Columns {
			if i >= len(row) || row[i] == nil {
				builders[i].AppendNull()
				continue
			}
			builders[i].Append(fmt.Sprintf("%v", row[i]))
		}
	}

	cols := make([]arrow.Array, len(result.Columns))
	for i := range result.Columns {
		cols[i] = builders[i].NewArray()
		builders[i].Release()
	}

	record := array.NewRecord(schema, cols, int64(len(result.Rows)))
	for i := range cols {
		cols[i].Release()
	}

	return record, nil
}

func streamSingleRecord(ctx context.Context, schema *arrow.Schema, record arrow.Record) (*arrow.Schema, <-chan arrowflight.StreamChunk, error) {
	rdr, err := array.NewRecordReader(schema, []arrow.RecordBatch{record})
	if err != nil {
		record.Release()
		return nil, nil, err
	}
	record.Release()
	ch := make(chan arrowflight.StreamChunk)
	go arrowflight.StreamChunksFromReader(ctx, rdr, ch)
	return schema, ch, nil
}

func tablesSchema(includeSchema bool) *arrow.Schema {
	fields := []arrow.Field{
		{Name: "catalog_name", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "db_schema_name", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "table_name", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "table_type", Type: arrow.BinaryTypes.String, Nullable: false},
	}
	if includeSchema {
		fields = append(fields, arrow.Field{Name: "table_schema", Type: arrow.BinaryTypes.Binary, Nullable: false})
	}
	return arrow.NewSchema(fields, nil)
}

func recordFromTablesResult(schema *arrow.Schema, result *QueryResult, includeSchema bool, tableSchemas map[string][]byte) (arrow.Record, error) {
	catalogBuilder := array.NewStringBuilder(memory.DefaultAllocator)
	schemaBuilder := array.NewStringBuilder(memory.DefaultAllocator)
	tableBuilder := array.NewStringBuilder(memory.DefaultAllocator)
	typeBuilder := array.NewStringBuilder(memory.DefaultAllocator)
	var tableSchemaBuilder *array.BinaryBuilder
	if includeSchema {
		tableSchemaBuilder = array.NewBinaryBuilder(memory.DefaultAllocator, arrow.BinaryTypes.Binary)
	}

	for _, row := range result.Rows {
		catalog := rowValue(row, 0)
		dbSchema := rowValue(row, 1)
		tableName := rowValue(row, 2)

		appendNullableString(catalogBuilder, catalog)
		appendNullableString(schemaBuilder, dbSchema)
		appendRequiredString(tableBuilder, tableName)
		appendRequiredString(typeBuilder, rowValue(row, 3))
		if includeSchema {
			tableSchemaBuilder.Append(tableSchemas[tableKey(catalog, dbSchema, tableName)])
		}
	}

	columns := []arrow.Array{
		catalogBuilder.NewArray(),
		schemaBuilder.NewArray(),
		tableBuilder.NewArray(),
		typeBuilder.NewArray(),
	}
	catalogBuilder.Release()
	schemaBuilder.Release()
	tableBuilder.Release()
	typeBuilder.Release()
	if includeSchema {
		columns = append(columns, tableSchemaBuilder.NewArray())
		tableSchemaBuilder.Release()
	}

	record := array.NewRecord(schema, columns, int64(len(result.Rows)))
	for _, column := range columns {
		column.Release()
	}
	return record, nil
}

func rowValue(row []interface{}, idx int) interface{} {
	if idx < 0 || idx >= len(row) {
		return nil
	}
	return row[idx]
}

func appendNullableString(builder *array.StringBuilder, value interface{}) {
	if value == nil {
		builder.AppendNull()
		return
	}
	builder.Append(fmt.Sprintf("%v", value))
}

func appendRequiredString(builder *array.StringBuilder, value interface{}) {
	if value == nil {
		builder.Append("")
		return
	}
	builder.Append(fmt.Sprintf("%v", value))
}

func buildTablesQuery(req arrowflightsql.GetTables) string {
	query := "SELECT table_catalog, table_schema, table_name, table_type FROM information_schema.tables"
	filters := make([]string, 0, 4)

	if catalog := req.GetCatalog(); catalog != nil {
		filters = append(filters, fmt.Sprintf("table_catalog = %s", quoteSQLLiteral(*catalog)))
	}
	if pattern := req.GetDBSchemaFilterPattern(); pattern != nil {
		filters = append(filters, fmt.Sprintf("table_schema LIKE %s", quoteSQLLiteral(*pattern)))
	}
	if pattern := req.GetTableNameFilterPattern(); pattern != nil {
		filters = append(filters, fmt.Sprintf("table_name LIKE %s", quoteSQLLiteral(*pattern)))
	}

	if tableTypes := req.GetTableTypes(); len(tableTypes) > 0 {
		typeLiterals := make([]string, 0, len(tableTypes)*2)
		for _, tableType := range tableTypes {
			normalized := strings.ToUpper(strings.TrimSpace(tableType))
			if normalized == "" {
				continue
			}
			typeLiterals = append(typeLiterals, quoteSQLLiteral(normalized))
			if normalized == "TABLE" {
				typeLiterals = append(typeLiterals, quoteSQLLiteral("BASE TABLE"))
			}
		}
		if len(typeLiterals) > 0 {
			filters = append(filters, fmt.Sprintf("UPPER(table_type) IN (%s)", strings.Join(typeLiterals, ",")))
		}
	}

	if len(filters) > 0 {
		query += " WHERE " + strings.Join(filters, " AND ")
	}
	query += " ORDER BY table_catalog, table_schema, table_name, table_type"
	return query
}

func (s *queryServer) loadTableSchemas(ctx context.Context, principal string, tables *QueryResult) (map[string][]byte, error) {
	out := make(map[string][]byte, len(tables.Rows))
	for _, row := range tables.Rows {
		catalog := fmt.Sprintf("%v", rowValue(row, 0))
		dbSchema := fmt.Sprintf("%v", rowValue(row, 1))
		tableName := fmt.Sprintf("%v", rowValue(row, 2))
		if tableName == "" || tableName == "<nil>" {
			continue
		}

		columns, err := s.query(ctx, principal, buildTableColumnsQueryWithRemarks(catalog, dbSchema, tableName))
		if err != nil {
			columns, err = s.query(ctx, principal, buildTableColumnsQueryFallback(catalog, dbSchema, tableName))
		}
		if err != nil {
			return nil, err
		}

		tableSchema := buildArrowSchemaFromColumns(catalog, dbSchema, tableName, columns)
		out[tableKey(rowValue(row, 0), rowValue(row, 1), rowValue(row, 2))] = arrowflight.SerializeSchema(tableSchema, memory.DefaultAllocator)
	}
	return out, nil
}

func buildTableColumnsQueryWithRemarks(catalog, dbSchema, tableName string) string {
	parts := []string{"table_name = " + quoteSQLLiteral(tableName)}
	if catalog != "" && catalog != "<nil>" {
		parts = append(parts, "table_catalog = "+quoteSQLLiteral(catalog))
	}
	if dbSchema != "" && dbSchema != "<nil>" {
		parts = append(parts, "table_schema = "+quoteSQLLiteral(dbSchema))
	}
	return "SELECT column_name, data_type, is_nullable, numeric_precision, numeric_scale, column_default, COALESCE(comment, '') AS remarks FROM information_schema.columns WHERE " + strings.Join(parts, " AND ") + " ORDER BY ordinal_position"
}

func buildTableColumnsQueryFallback(catalog, dbSchema, tableName string) string {
	parts := []string{"table_name = " + quoteSQLLiteral(tableName)}
	if catalog != "" && catalog != "<nil>" {
		parts = append(parts, "table_catalog = "+quoteSQLLiteral(catalog))
	}
	if dbSchema != "" && dbSchema != "<nil>" {
		parts = append(parts, "table_schema = "+quoteSQLLiteral(dbSchema))
	}
	return "SELECT column_name, data_type, is_nullable, numeric_precision, numeric_scale, column_default, '' AS remarks FROM information_schema.columns WHERE " + strings.Join(parts, " AND ") + " ORDER BY ordinal_position"
}

func buildArrowSchemaFromColumns(catalog, dbSchema, tableName string, columns *QueryResult) *arrow.Schema {
	fields := make([]arrow.Field, 0, len(columns.Rows))
	for _, row := range columns.Rows {
		columnName := strings.TrimSpace(fmt.Sprintf("%v", rowValue(row, 0)))
		if columnName == "" || columnName == "<nil>" {
			continue
		}
		rawTypeName := strings.TrimSpace(fmt.Sprintf("%v", rowValue(row, 1)))
		dataType := strings.ToUpper(rawTypeName)
		nullable := strings.EqualFold(strings.TrimSpace(fmt.Sprintf("%v", rowValue(row, 2))), "YES")
		precision, scale := sqlPrecisionScale(rawTypeName, rowValue(row, 3), rowValue(row, 4))
		defaultExpr := strings.ToLower(strings.TrimSpace(fmt.Sprintf("%v", rowValue(row, 5))))
		remarks := strings.TrimSpace(fmt.Sprintf("%v", rowValue(row, 6)))
		if strings.EqualFold(remarks, "<nil>") {
			remarks = ""
		}
		isAutoIncrement := strings.Contains(defaultExpr, "nextval") || strings.Contains(defaultExpr, "identity")
		isCaseSensitive := isCaseSensitiveType(dataType)

		metadata := arrow.NewMetadata(
			[]string{
				arrowflightsql.CatalogNameKey,
				arrowflightsql.SchemaNameKey,
				arrowflightsql.TableNameKey,
				arrowflightsql.TypeNameKey,
				arrowflightsql.PrecisionKey,
				arrowflightsql.ScaleKey,
				arrowflightsql.IsAutoIncrementKey,
				arrowflightsql.IsCaseSensitiveKey,
				arrowflightsql.IsReadOnlyKey,
				arrowflightsql.IsSearchableKey,
				arrowflightsql.RemarksKey,
			},
			[]string{
				catalog,
				dbSchema,
				tableName,
				rawTypeName,
				strconv.Itoa(precision),
				strconv.Itoa(scale),
				boolAsFlag(isAutoIncrement),
				boolAsFlag(isCaseSensitive),
				"0",
				"1",
				remarks,
			},
		)

		fields = append(fields, arrow.Field{
			Name:     columnName,
			Type:     arrowTypeForSQL(dataType),
			Nullable: nullable,
			Metadata: metadata,
		})
	}
	return arrow.NewSchema(fields, nil)
}

func arrowTypeForSQL(dataType string) arrow.DataType {
	switch {
	case strings.Contains(dataType, "BIGINT"):
		return arrow.PrimitiveTypes.Int64
	case strings.Contains(dataType, "INT"):
		return arrow.PrimitiveTypes.Int32
	case strings.Contains(dataType, "DOUBLE") || strings.Contains(dataType, "FLOAT"):
		return arrow.PrimitiveTypes.Float64
	case strings.Contains(dataType, "DECIMAL") || strings.Contains(dataType, "NUMERIC"):
		return &arrow.Decimal128Type{Precision: 38, Scale: 9}
	case strings.Contains(dataType, "BOOL"):
		return arrow.FixedWidthTypes.Boolean
	case strings.Contains(dataType, "DATE"):
		return arrow.FixedWidthTypes.Date32
	case strings.Contains(dataType, "TIMESTAMP"):
		return &arrow.TimestampType{Unit: arrow.Microsecond}
	default:
		return arrow.BinaryTypes.String
	}
}

func sqlPrecisionScale(typeName string, precisionRaw, scaleRaw interface{}) (int, int) {
	precision := parseSQLInt(precisionRaw)
	scale := parseSQLInt(scaleRaw)
	if precision > 0 {
		return precision, scale
	}

	decimalPattern := regexp.MustCompile(`(?i)(?:decimal|numeric)\s*\((\d+)\s*,\s*(\d+)\)`)
	matches := decimalPattern.FindStringSubmatch(typeName)
	if len(matches) == 3 {
		p, _ := strconv.Atoi(matches[1])
		s, _ := strconv.Atoi(matches[2])
		return p, s
	}

	return defaultPrecisionScaleForType(strings.ToUpper(typeName))
}

func defaultPrecisionScaleForType(typeName string) (int, int) {
	switch {
	case strings.Contains(typeName, "BIGINT"):
		return 64, 0
	case strings.Contains(typeName, "SMALLINT"):
		return 16, 0
	case strings.Contains(typeName, "TINYINT"):
		return 8, 0
	case strings.Contains(typeName, "INT"):
		return 32, 0
	case strings.Contains(typeName, "DOUBLE"):
		return 53, 0
	case strings.Contains(typeName, "FLOAT") || strings.Contains(typeName, "REAL"):
		return 24, 0
	case strings.Contains(typeName, "TIMESTAMP"):
		return 6, 6
	case strings.Contains(typeName, "TIME"):
		return 6, 6
	case strings.Contains(typeName, "DATE"):
		return 10, 0
	case strings.Contains(typeName, "BOOL"):
		return 1, 0
	default:
		return 0, 0
	}
}

func parseSQLInt(value interface{}) int {
	if value == nil {
		return 0
	}
	text := strings.TrimSpace(fmt.Sprintf("%v", value))
	if text == "" || strings.EqualFold(text, "<nil>") {
		return 0
	}
	i, err := strconv.Atoi(text)
	if err != nil {
		return 0
	}
	return i
}

func boolAsFlag(v bool) string {
	if v {
		return "1"
	}
	return "0"
}

func isCaseSensitiveType(dataType string) bool {
	return strings.Contains(dataType, "CHAR") || strings.Contains(dataType, "TEXT") || strings.Contains(dataType, "STRING") || strings.Contains(dataType, "VARCHAR")
}

func tableKey(catalog, dbSchema, tableName interface{}) string {
	return fmt.Sprintf("%v|%v|%v", catalog, dbSchema, tableName)
}

func quoteSQLLiteral(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "''") + "'"
}

func principalFromContext(ctx context.Context) string {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return "anonymous"
	}
	for _, key := range []string{"x-duck-principal", "x-principal", "user"} {
		values := md.Get(key)
		if len(values) > 0 && values[0] != "" {
			return values[0]
		}
	}
	return "anonymous"
}
