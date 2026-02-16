package model

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"duck-demo/internal/domain"
)

// validateContract checks that a materialized model's output matches its contract.
// Returns nil if no contract is defined or validation passes.
func (s *Service) validateContract(ctx context.Context, conn *sql.Conn,
	model *domain.Model, config ExecutionConfig, principal string) error {

	if model.Contract == nil || !model.Contract.Enforce || len(model.Contract.Columns) == 0 {
		return nil
	}

	fqn := quoteIdent(config.TargetSchema) + "." + quoteIdent(model.Name)

	// Query information_schema to get actual columns
	infoSQL := fmt.Sprintf(
		"SELECT column_name, data_type, is_nullable FROM information_schema.columns WHERE table_schema = '%s' AND table_name = '%s' ORDER BY ordinal_position",
		strings.ReplaceAll(config.TargetSchema, "'", "''"),
		strings.ReplaceAll(model.Name, "'", "''"),
	)

	rows, err := s.engine.QueryOnConn(ctx, conn, principal, infoSQL)
	if err != nil {
		return fmt.Errorf("query information_schema for %s: %w", fqn, err)
	}
	defer func() { _ = rows.Close() }()

	actualCols := make(map[string]struct{ dataType, isNullable string })
	for rows.Next() {
		var colName, dataType, isNullable string
		if err := rows.Scan(&colName, &dataType, &isNullable); err != nil {
			return fmt.Errorf("scan column info: %w", err)
		}
		actualCols[strings.ToLower(colName)] = struct{ dataType, isNullable string }{
			dataType:   strings.ToUpper(dataType),
			isNullable: isNullable,
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate columns: %w", err)
	}

	var violations []string
	for _, expected := range model.Contract.Columns {
		actual, ok := actualCols[strings.ToLower(expected.Name)]
		if !ok {
			violations = append(violations, fmt.Sprintf("column %q not found in output", expected.Name))
			continue
		}
		if expected.Type != "" && !strings.EqualFold(actual.dataType, expected.Type) {
			violations = append(violations, fmt.Sprintf("column %q: expected type %s, got %s",
				expected.Name, expected.Type, actual.dataType))
		}
		if !expected.Nullable && strings.EqualFold(actual.isNullable, "YES") {
			violations = append(violations, fmt.Sprintf("column %q: expected NOT NULL but is nullable",
				expected.Name))
		}
	}

	if len(violations) > 0 {
		return domain.ErrValidation("contract violation for %s: %s", model.QualifiedName(), strings.Join(violations, "; "))
	}
	return nil
}
