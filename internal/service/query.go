package service

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"duck-demo/internal/domain"
	"duck-demo/internal/engine"
	"duck-demo/internal/sqlrewrite"
)

// QueryResult holds the structured output of a SQL query.
type QueryResult struct {
	Columns  []string
	Rows     [][]interface{}
	RowCount int
}

// QueryService wraps the SecureEngine and records audit entries.
type QueryService struct {
	engine  *engine.SecureEngine
	audit   domain.AuditRepository
	lineage domain.LineageRepository
}

func NewQueryService(eng *engine.SecureEngine, audit domain.AuditRepository, lineage domain.LineageRepository) *QueryService {
	return &QueryService{engine: eng, audit: audit, lineage: lineage}
}

// Execute runs a SQL query as the given principal and returns structured results.
func (s *QueryService) Execute(ctx context.Context, principalName, sqlQuery string) (*QueryResult, error) {
	start := time.Now()

	rows, err := s.engine.Query(ctx, principalName, sqlQuery)
	duration := time.Since(start).Milliseconds()

	if err != nil {
		// Log failed query
		s.logAudit(ctx, principalName, "QUERY", &sqlQuery, nil, nil, "DENIED", err.Error(), duration, nil)
		return nil, err
	}
	defer rows.Close()

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
		for _, src := range tables {
			if src == targetTable {
				continue
			}
			_ = s.lineage.InsertEdge(ctx, &domain.LineageEdge{
				SourceTable:   src,
				TargetTable:   &targetTable,
				EdgeType:      "WRITE",
				PrincipalName: principalName,
			})
		}
	} else {
		// SELECT — all tables are read sources
		for _, src := range tables {
			_ = s.lineage.InsertEdge(ctx, &domain.LineageEdge{
				SourceTable:   src,
				EdgeType:      "READ",
				PrincipalName: principalName,
			})
		}
	}
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

	// Best-effort audit logging — don't fail the query if audit fails
	_ = s.audit.Insert(ctx, entry)
}

// TablesAccessedStr returns a comma-separated list of tables for display.
func TablesAccessedStr(tables []string) string {
	return strings.Join(tables, ",")
}
