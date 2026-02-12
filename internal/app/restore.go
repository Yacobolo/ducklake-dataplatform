package app

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"

	"duck-demo/internal/db/repository"
	"duck-demo/internal/ddl"
)

// restoreExternalTableViews recreates DuckDB VIEWs for all non-deleted external
// tables. VIEWs are in-memory and lost on DuckDB restart, so this must run at startup.
// Errors are logged but not fatal (best-effort).
func restoreExternalTableViews(ctx context.Context, duckDB *sql.DB, repo *repository.ExternalTableRepo, logger *slog.Logger) error {
	tables, err := repo.ListAll(ctx)
	if err != nil {
		return fmt.Errorf("list external tables: %w", err)
	}
	if len(tables) == 0 {
		return nil
	}

	restored := 0
	for _, et := range tables {
		// TODO: pass actual catalog name when multi-catalog is wired
		viewSQL, err := ddl.CreateExternalTableView("lake", et.SchemaName, et.TableName, et.SourcePath, et.FileFormat)
		if err != nil {
			logger.Warn("build external table view DDL failed", "schema", et.SchemaName, "table", et.TableName, "error", err)
			continue
		}
		if _, err := duckDB.ExecContext(ctx, viewSQL); err != nil {
			logger.Warn("restore external table view failed", "schema", et.SchemaName, "table", et.TableName, "error", err)
			continue
		}
		restored++
	}
	if restored > 0 {
		logger.Info("restored external table VIEWs", "count", restored)
	}
	return nil
}
