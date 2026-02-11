package app

import (
	"context"
	"database/sql"
	"fmt"
	"log"

	"duck-demo/internal/db/repository"
	"duck-demo/internal/ddl"
)

// restoreExternalTableViews recreates DuckDB VIEWs for all non-deleted external
// tables. VIEWs are in-memory and lost on DuckDB restart, so this must run at startup.
// Errors are logged but not fatal (best-effort).
func restoreExternalTableViews(ctx context.Context, duckDB *sql.DB, repo *repository.ExternalTableRepo) error {
	tables, err := repo.ListAll(ctx)
	if err != nil {
		return fmt.Errorf("list external tables: %w", err)
	}
	if len(tables) == 0 {
		return nil
	}

	restored := 0
	for _, et := range tables {
		viewSQL, err := ddl.CreateExternalTableView(et.SchemaName, et.TableName, et.SourcePath, et.FileFormat)
		if err != nil {
			log.Printf("warning: build external table view DDL for %s.%s: %v", et.SchemaName, et.TableName, err)
			continue
		}
		if _, err := duckDB.ExecContext(ctx, viewSQL); err != nil {
			log.Printf("warning: restore external table view %s.%s: %v", et.SchemaName, et.TableName, err)
			continue
		}
		restored++
	}
	if restored > 0 {
		log.Printf("Restored %d external table VIEW(s)", restored)
	}
	return nil
}
