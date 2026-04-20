package repository

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	dbstore "github.com/Yacobolo/quackstack/internal/db/dbstore"
	"github.com/Yacobolo/quackstack/internal/domain"
)

// ColumnLineageRepo implements domain.ColumnLineageRepository using SQLite.
type ColumnLineageRepo struct {
	db *sql.DB
	q  *dbstore.Queries
}

// NewColumnLineageRepo creates a new ColumnLineageRepo.
func NewColumnLineageRepo(db *sql.DB) *ColumnLineageRepo {
	return &ColumnLineageRepo{db: db, q: dbstore.New(db)}
}

// InsertBatch inserts all column lineage edges for a given table-level edge.
func (r *ColumnLineageRepo) InsertBatch(ctx context.Context, edgeID string, edges []domain.ColumnLineageEdge) error {
	if len(edges) == 0 {
		return nil
	}

	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck

	qtx := r.q.WithTx(tx)
	for _, edge := range edges {
		if err := qtx.InsertColumnLineageEdge(ctx, dbstore.InsertColumnLineageEdgeParams{
			LineageEdgeID: edgeID,
			TargetColumn:  edge.TargetColumn,
			SourceSchema:  edge.SourceSchema,
			SourceTable:   edge.SourceTable,
			SourceColumn:  edge.SourceColumn,
			TransformType: string(edge.TransformType),
			FunctionName:  edge.Function,
		}); err != nil {
			return err
		}
	}

	return tx.Commit()
}

// GetByEdgeID returns all column lineage edges for a table-level lineage edge.
func (r *ColumnLineageRepo) GetByEdgeID(ctx context.Context, edgeID string) ([]domain.ColumnLineageEdge, error) {
	rows, err := r.q.GetColumnLineageByEdgeID(ctx, edgeID)
	if err != nil {
		return nil, err
	}
	return mapColumnLineageEdges(rows), nil
}

// GetForTable returns all column lineage for a target table.
func (r *ColumnLineageRepo) GetForTable(ctx context.Context, schema, table string) ([]domain.ColumnLineageEdge, error) {
	rows, err := r.q.GetColumnLineageForTable(ctx, dbstore.GetColumnLineageForTableParams{
		TargetSchema: sql.NullString{String: schema, Valid: true},
		TargetTable:  sql.NullString{String: table, Valid: true},
	})
	if err != nil {
		return nil, err
	}
	return mapColumnLineageEdges(rows), nil
}

// GetForSourceColumn returns all column lineage edges sourced from a specific column.
func (r *ColumnLineageRepo) GetForSourceColumn(ctx context.Context, schema, table, column string) ([]domain.ColumnLineageEdge, error) {
	rows, err := r.q.GetColumnLineageForSourceColumn(ctx, dbstore.GetColumnLineageForSourceColumnParams{
		SourceSchema: schema,
		SourceTable:  table,
		SourceColumn: column,
	})
	if err != nil {
		return nil, err
	}
	return mapColumnLineageEdges(rows), nil
}

// DeleteByEdgeID removes all column lineage for a table-level edge.
func (r *ColumnLineageRepo) DeleteByEdgeID(ctx context.Context, edgeID string) error {
	return r.q.DeleteColumnLineageByEdgeID(ctx, edgeID)
}

// ReplaceBuildLineage replaces compile-time build lineage rows for a build.
func (r *ColumnLineageRepo) ReplaceBuildLineage(ctx context.Context, buildID string, items []domain.CompiledColumnLineage) error {
	return r.replaceLineage(ctx, "build_id", buildID, items)
}

// ReplaceCompilationLineage replaces compile-time lineage rows for a compilation.
func (r *ColumnLineageRepo) ReplaceCompilationLineage(ctx context.Context, compilationID string, items []domain.CompiledColumnLineage) error {
	return r.replaceLineage(ctx, "compilation_id", compilationID, items)
}

func (r *ColumnLineageRepo) replaceLineage(ctx context.Context, keyColumn, keyID string, items []domain.CompiledColumnLineage) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	deleteSQL := compiledColumnLineageDeleteSQL(keyColumn)
	if _, err := tx.ExecContext(ctx, deleteSQL, keyID); err != nil {
		return err
	}
	for _, item := range items {
		reasonsJSON, err := json.Marshal(nilSafeStrings(itemSensitivityReasons(item)))
		if err != nil {
			return fmt.Errorf("marshal sensitivity reasons: %w", err)
		}
		if len(item.Sources) == 0 {
			if _, err := tx.ExecContext(ctx, `
					INSERT INTO compiled_column_lineage (
					build_id, compilation_id, project_name, model_name, target_catalog, target_schema, target_table, target_column,
					transform_type, function_name, partial, source_kind, sensitivity_status, sensitivity_partial,
					sensitivity_reasons_json
				) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
				lineageIDForColumn(keyColumn, keyID, "build_id"), lineageIDForColumn(keyColumn, keyID, "compilation_id"), item.ProjectName, item.ModelName, item.TargetCatalog, item.TargetSchema, item.TargetTable, item.TargetColumn,
				string(item.TransformType), item.Function, boolIntCompile(item.Partial), "", itemSensitivityStatus(item),
				boolIntCompile(itemSensitivityPartial(item)), string(reasonsJSON),
			); err != nil {
				return err
			}
			continue
		}
		for _, src := range item.Sources {
			if _, err := tx.ExecContext(ctx, `
				INSERT INTO compiled_column_lineage (
					build_id, compilation_id, project_name, model_name, target_catalog, target_schema, target_table, target_column,
					transform_type, function_name, partial, source_catalog, source_schema, source_table, source_column,
					source_kind, source_model_name, sensitivity_status, sensitivity_partial, sensitivity_reasons_json
				) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
				lineageIDForColumn(keyColumn, keyID, "build_id"), lineageIDForColumn(keyColumn, keyID, "compilation_id"), item.ProjectName, item.ModelName, item.TargetCatalog, item.TargetSchema, item.TargetTable, item.TargetColumn,
				string(item.TransformType), item.Function, boolIntCompile(item.Partial), nullableCompileString(src.Catalog), nullableCompileString(src.Schema),
				nullableCompileString(src.Table), nullableCompileString(src.Column), src.Kind, nullableCompileString(src.ModelName),
				itemSensitivityStatus(item), boolIntCompile(itemSensitivityPartial(item)), string(reasonsJSON),
			); err != nil {
				return err
			}
		}
	}

	return tx.Commit()
}

func compiledColumnLineageDeleteSQL(keyColumn string) string {
	switch keyColumn {
	case "build_id":
		return `DELETE FROM compiled_column_lineage WHERE build_id = ?`
	case "compilation_id":
		return `DELETE FROM compiled_column_lineage WHERE compilation_id = ?`
	default:
		return `DELETE FROM compiled_column_lineage WHERE build_id = ?`
	}
}

// ListBuildLineage returns grouped compile-time lineage rows for a build.
func (r *ColumnLineageRepo) ListBuildLineage(ctx context.Context, buildID string) ([]domain.CompiledColumnLineage, error) {
	rows, err := r.db.QueryContext(ctx, compiledColumnLineageSelectSQL+` WHERE build_id = ? ORDER BY model_name, target_column, id`, buildID)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	return scanCompiledColumnLineage(rows)
}

// ListBuildLineageByModel returns grouped compile-time lineage rows for one build/model.
func (r *ColumnLineageRepo) ListBuildLineageByModel(ctx context.Context, buildID, modelName string) ([]domain.CompiledColumnLineage, error) {
	rows, err := r.db.QueryContext(ctx, compiledColumnLineageSelectSQL+` WHERE build_id = ? AND model_name = ? ORDER BY target_column, id`, buildID, modelName)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	return scanCompiledColumnLineage(rows)
}

// ListBuildImpactsForSourceColumn returns impacted compiled columns for a specific source column.
func (r *ColumnLineageRepo) ListBuildImpactsForSourceColumn(ctx context.Context, buildID, schema, table, column string) ([]domain.CompiledColumnLineage, error) {
	rows, err := r.db.QueryContext(ctx, compiledColumnLineageSelectSQL+`
		WHERE build_id = ?
		  AND COALESCE(source_schema, '') = ?
		  AND COALESCE(source_table, '') = ?
		  AND COALESCE(source_column, '') = ?
		ORDER BY model_name, target_column, id`, buildID, schema, table, column)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	return scanCompiledColumnLineage(rows)
}

// ListCompilationLineage returns grouped compile-time lineage rows for a compilation.
func (r *ColumnLineageRepo) ListCompilationLineage(ctx context.Context, compilationID string) ([]domain.CompiledColumnLineage, error) {
	rows, err := r.db.QueryContext(ctx, compiledColumnLineageSelectSQL+` WHERE compilation_id = ? ORDER BY model_name, target_column, id`, compilationID)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	return scanCompiledColumnLineage(rows)
}

// ListCompilationLineageByModel returns grouped compile-time lineage rows for one compilation/model.
func (r *ColumnLineageRepo) ListCompilationLineageByModel(ctx context.Context, compilationID, modelName string) ([]domain.CompiledColumnLineage, error) {
	rows, err := r.db.QueryContext(ctx, compiledColumnLineageSelectSQL+` WHERE compilation_id = ? AND model_name = ? ORDER BY target_column, id`, compilationID, modelName)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	return scanCompiledColumnLineage(rows)
}

// ListCompilationImpactsForSourceColumn returns impacted compiled columns for a specific source column.
func (r *ColumnLineageRepo) ListCompilationImpactsForSourceColumn(ctx context.Context, compilationID, schema, table, column string) ([]domain.CompiledColumnLineage, error) {
	rows, err := r.db.QueryContext(ctx, compiledColumnLineageSelectSQL+`
		WHERE compilation_id = ?
		  AND COALESCE(source_schema, '') = ?
		  AND COALESCE(source_table, '') = ?
		  AND COALESCE(source_column, '') = ?
		ORDER BY model_name, target_column, id`, compilationID, schema, table, column)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	return scanCompiledColumnLineage(rows)
}

// mapColumnLineageEdges converts dbstore rows to domain types.
func mapColumnLineageEdges(rows []dbstore.ColumnLineageEdge) []domain.ColumnLineageEdge {
	edges := make([]domain.ColumnLineageEdge, len(rows))
	for i, row := range rows {
		edges[i] = domain.ColumnLineageEdge{
			ID:            row.ID,
			LineageEdgeID: row.LineageEdgeID,
			TargetColumn:  row.TargetColumn,
			SourceSchema:  row.SourceSchema,
			SourceTable:   row.SourceTable,
			SourceColumn:  row.SourceColumn,
			TransformType: domain.TransformType(row.TransformType),
			Function:      row.FunctionName,
		}
	}
	return edges
}

const compiledColumnLineageSelectSQL = `
	SELECT
		id, build_id, compilation_id, project_name, model_name, target_catalog, target_schema, target_table, target_column,
		transform_type, function_name, partial, source_catalog, source_schema, source_table, source_column,
		source_kind, source_model_name, sensitivity_status, sensitivity_partial, sensitivity_reasons_json
	FROM compiled_column_lineage`

func scanCompiledColumnLineage(rows *sql.Rows) ([]domain.CompiledColumnLineage, error) {
	type key struct {
		modelName     string
		targetSchema  string
		targetTable   string
		targetColumn  string
		transformType string
		functionName  string
	}
	grouped := make(map[key]*domain.CompiledColumnLineage)
	order := make([]key, 0)
	for rows.Next() {
		var (
			buildID            sql.NullString
			compilationID      sql.NullString
			projectName        string
			modelName          string
			targetCatalog      sql.NullString
			targetSchema       string
			targetTable        string
			targetColumn       string
			transformType      string
			functionName       string
			partial            int64
			sourceCatalog      sql.NullString
			sourceSchema       sql.NullString
			sourceTable        sql.NullString
			sourceColumn       sql.NullString
			sourceKind         string
			sourceModelName    sql.NullString
			sensitivityStatus  string
			sensitivityPartial int64
			sensitivityReasons string
		)
		if err := rows.Scan(
			new(int64), &buildID, &compilationID, &projectName, &modelName, &targetCatalog, &targetSchema, &targetTable, &targetColumn,
			&transformType, &functionName, &partial, &sourceCatalog, &sourceSchema, &sourceTable, &sourceColumn,
			&sourceKind, &sourceModelName, &sensitivityStatus, &sensitivityPartial, &sensitivityReasons,
		); err != nil {
			return nil, err
		}
		k := key{modelName: modelName, targetSchema: targetSchema, targetTable: targetTable, targetColumn: targetColumn, transformType: transformType, functionName: functionName}
		item, ok := grouped[k]
		if !ok {
			compilationIDValue := ""
			if compilationID.Valid {
				compilationIDValue = compilationID.String
			}
			buildIDValue := ""
			if buildID.Valid {
				buildIDValue = buildID.String
			}
			item = &domain.CompiledColumnLineage{
				BuildID:       buildIDValue,
				CompilationID: compilationIDValue,
				ProjectName:   projectName,
				ModelName:     modelName,
				TargetCatalog: targetCatalog.String,
				TargetSchema:  targetSchema,
				TargetTable:   targetTable,
				TargetColumn:  targetColumn,
				TransformType: domain.TransformType(transformType),
				Function:      functionName,
				Partial:       partial != 0,
			}
			if sensitivityStatus != "" {
				item.Sensitivity = &domain.ColumnSensitivityInfo{
					Status:  sensitivityStatus,
					Partial: sensitivityPartial != 0,
				}
				_ = json.Unmarshal([]byte(sensitivityReasons), &item.Sensitivity.Reasons)
			}
			grouped[k] = item
			order = append(order, k)
		}
		if sourceTable.Valid || sourceColumn.Valid || sourceKind != "" {
			item.Sources = append(item.Sources, domain.ColumnLineageSourceRef{
				Catalog:   sourceCatalog.String,
				Schema:    sourceSchema.String,
				Table:     sourceTable.String,
				Column:    sourceColumn.String,
				Kind:      sourceKind,
				ModelName: sourceModelName.String,
			})
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	out := make([]domain.CompiledColumnLineage, 0, len(order))
	for _, k := range order {
		out = append(out, *grouped[k])
	}
	return out, nil
}

func lineageIDForColumn(keyColumn, keyID, target string) any {
	if keyColumn == target {
		return keyID
	}
	return nil
}

func boolIntCompile(v bool) int {
	if v {
		return 1
	}
	return 0
}

func nullableCompileString(v string) any {
	if v == "" {
		return nil
	}
	return v
}

func nilSafeStrings(items []string) []string {
	if items == nil {
		return []string{}
	}
	return items
}

func itemSensitivityStatus(item domain.CompiledColumnLineage) string {
	if item.Sensitivity == nil {
		return ""
	}
	return item.Sensitivity.Status
}

func itemSensitivityPartial(item domain.CompiledColumnLineage) bool {
	return item.Sensitivity != nil && item.Sensitivity.Partial
}

func itemSensitivityReasons(item domain.CompiledColumnLineage) []string {
	if item.Sensitivity == nil {
		return nil
	}
	return item.Sensitivity.Reasons
}
