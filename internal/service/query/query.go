package query

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"time"

	"duck-demo/internal/domain"
	"duck-demo/internal/service/auditutil"
	"duck-demo/internal/sqlrewrite"
)

// QueryResult holds the structured output of a SQL query.
//
//nolint:revive // Name chosen for clarity across package boundaries
type QueryResult struct {
	Columns  []string
	Rows     [][]interface{}
	RowCount int
}

// QueryService wraps the QueryEngine and records audit entries.
//
//nolint:revive // Name chosen for clarity across package boundaries
type QueryService struct {
	engine        domain.QueryEngine
	audit         domain.AuditRepository
	lineage       domain.LineageRepository
	colLineage    domain.ColumnLineageRepository
	catalog       sqlrewrite.CatalogResolver
	defaultSchema string
	jobRepo       domain.QueryJobRepository
	jobCancels    sync.Map
	asyncEnabled  bool
}

// NewQueryService creates a new QueryService.
func NewQueryService(eng domain.QueryEngine, audit domain.AuditRepository, lineage domain.LineageRepository) *QueryService {
	return &QueryService{engine: eng, audit: audit, lineage: lineage, defaultSchema: "main", asyncEnabled: true}
}

// SetColumnLineage configures column-level lineage capture.
// This is optional — if not called, column lineage is silently skipped.
func (s *QueryService) SetColumnLineage(colRepo domain.ColumnLineageRepository, catalog sqlrewrite.CatalogResolver) {
	s.colLineage = colRepo
	s.catalog = catalog
}

// SetJobRepository configures durable async query lifecycle storage.
func (s *QueryService) SetJobRepository(repo domain.QueryJobRepository) {
	s.jobRepo = repo
}

// SetAsyncEnabled toggles async query lifecycle endpoints.
func (s *QueryService) SetAsyncEnabled(enabled bool) {
	s.asyncEnabled = enabled
}

// Execute runs a SQL query as the given principal and returns structured results.
func (s *QueryService) Execute(ctx context.Context, principalName, sqlQuery string) (*QueryResult, error) {
	if strings.TrimSpace(sqlQuery) == "" {
		return nil, domain.ErrValidation("sql query is required")
	}

	start := time.Now()

	rows, err := s.engine.Query(ctx, principalName, sqlQuery)
	duration := time.Since(start).Milliseconds()

	if err != nil {
		// Log failed query
		s.logAudit(ctx, principalName, "QUERY", &sqlQuery, nil, nil, "DENIED", err.Error(), duration, nil)
		return nil, err
	}
	defer rows.Close() //nolint:errcheck

	result, err := scanRows(rows)
	if err != nil {
		s.logAudit(ctx, principalName, "QUERY", &sqlQuery, nil, nil, "ERROR", err.Error(), duration, nil)
		return nil, fmt.Errorf("scan results: %w", err)
	}

	rowCount := int64(result.RowCount)
	s.logAudit(ctx, principalName, "QUERY", &sqlQuery, nil, nil, "ALLOWED", "", duration, &rowCount)

	// Best-effort lineage emission
	s.emitLineage(ctx, principalName, sqlQuery)

	return result, nil
}

// emitLineage extracts table names and target table from the SQL to record lineage edges.
func (s *QueryService) emitLineage(ctx context.Context, principalName, sqlQuery string) {
	if s.lineage == nil {
		return
	}

	tables, err := sqlrewrite.ExtractTableNames(sqlQuery)
	if err != nil || len(tables) == 0 {
		return
	}

	targetTable, _ := sqlrewrite.ExtractTargetTable(sqlQuery)

	if targetTable != "" {
		// DML statement (INSERT/UPDATE/DELETE) — source tables write into target
		targetSchema, targetName := splitQualifiedName(targetTable)
		for _, src := range tables {
			if src == targetTable {
				continue
			}
			srcSchema, srcName := splitQualifiedName(src)
			_ = s.lineage.InsertEdge(ctx, &domain.LineageEdge{
				SourceTable:   srcName,
				TargetTable:   &targetName,
				SourceSchema:  srcSchema,
				TargetSchema:  targetSchema,
				EdgeType:      "WRITE",
				PrincipalName: principalName,
			})
		}
	} else {
		// SELECT — all tables are read sources
		for _, src := range tables {
			srcSchema, srcName := splitQualifiedName(src)
			edge := &domain.LineageEdge{
				SourceTable:   srcName,
				SourceSchema:  srcSchema,
				EdgeType:      "READ",
				PrincipalName: principalName,
			}
			_ = s.lineage.InsertEdge(ctx, edge)

			// Best-effort column lineage for SELECT statements
			if edge.ID != "" {
				s.emitColumnLineage(ctx, edge.ID, sqlQuery)
			}
		}
	}
}

// emitColumnLineage extracts column-level lineage from a SELECT query and
// stores it alongside the table-level lineage edge. Best-effort: failures
// are silently discarded.
func (s *QueryService) emitColumnLineage(ctx context.Context, edgeID, sqlQuery string) {
	if s.colLineage == nil || s.catalog == nil {
		return
	}

	entries, err := sqlrewrite.ExtractColumnLineage(ctx, sqlQuery, s.defaultSchema, s.catalog)
	if err != nil || len(entries) == 0 {
		return
	}

	// Convert ColumnLineageEntry to ColumnLineageEdge for storage
	var edges []domain.ColumnLineageEdge
	for _, entry := range entries {
		for _, src := range entry.Sources {
			edges = append(edges, domain.ColumnLineageEdge{
				LineageEdgeID: edgeID,
				TargetColumn:  entry.TargetColumn,
				SourceSchema:  src.Schema,
				SourceTable:   src.Table,
				SourceColumn:  src.Column,
				TransformType: entry.TransformType,
				Function:      entry.Function,
			})
		}
	}

	if len(edges) > 0 {
		_ = s.colLineage.InsertBatch(ctx, edgeID, edges)
	}
}

// splitQualifiedName splits "schema.table" into ("schema", "table").
// If there is no dot, it returns ("main", name).
func splitQualifiedName(name string) (schema, table string) {
	parts := strings.SplitN(name, ".", 2)
	if len(parts) == 2 {
		return parts[0], parts[1]
	}
	return "main", name
}

func scanRows(rows *sql.Rows) (*QueryResult, error) {
	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	var resultRows [][]interface{}
	for rows.Next() {
		vals := make([]interface{}, len(cols))
		ptrs := make([]interface{}, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, err
		}
		// Convert byte slices to strings for JSON serialization
		row := make([]interface{}, len(vals))
		for i, v := range vals {
			if b, ok := v.([]byte); ok {
				row[i] = string(b)
			} else {
				row[i] = v
			}
		}
		resultRows = append(resultRows, row)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return &QueryResult{
		Columns:  cols,
		Rows:     resultRows,
		RowCount: len(resultRows),
	}, nil
}

func (s *QueryService) logAudit(ctx context.Context, principal, action string, originalSQL, rewrittenSQL *string, tables []string, status, errMsg string, durationMs int64, rowsReturned *int64) {
	entry := &domain.AuditEntry{
		PrincipalName:  principal,
		Action:         action,
		OriginalSQL:    originalSQL,
		RewrittenSQL:   rewrittenSQL,
		TablesAccessed: tables,
		Status:         status,
		DurationMs:     &durationMs,
		RowsReturned:   rowsReturned,
	}
	if errMsg != "" {
		entry.ErrorMessage = &errMsg
	}
	stmtType := "QUERY"
	entry.StatementType = &stmtType

	_ = auditutil.Insert(ctx, s.audit, entry)
}

// TablesAccessedStr returns a comma-separated list of tables for display.
func TablesAccessedStr(tables []string) string {
	return strings.Join(tables, ",")
}
