package repository

import (
	"context"
	"database/sql"
	"log/slog"
	"time"

	dbstore "duck-demo/internal/db/dbstore"
	"duck-demo/internal/domain"
)

// CatalogRepo implements domain.CatalogRepository using the DuckLake SQLite
// metastore (metaDB) for reads and the DuckDB connection (duckDB) for DDL.
type CatalogRepo struct {
	metaDB      *sql.DB
	controlDB   *sql.DB // control-plane DB (for transactions on catalog_metadata, tags, etc.)
	duckDB      *sql.DB
	q           *dbstore.Queries // sqlc queries for application-owned tables
	extRepo     *ExternalTableRepo
	catalogName string // DuckDB catalog alias (e.g., "lake")
	logger      *slog.Logger
}

// NewCatalogRepo creates a new CatalogRepo.
// metaDB is the per-catalog DuckLake metastore (for ducklake_* table queries).
// controlDB is the control-plane database (for transactions).
// controlQ is dbstore.Queries backed by controlDB (for catalog_metadata, tag_assignments, etc.).
func NewCatalogRepo(metaDB *sql.DB, controlDB *sql.DB, controlQ *dbstore.Queries, duckDB *sql.DB, catalogName string, extRepo *ExternalTableRepo, logger *slog.Logger) *CatalogRepo {
	return &CatalogRepo{metaDB: metaDB, controlDB: controlDB, duckDB: duckDB, catalogName: catalogName, q: controlQ, extRepo: extRepo, logger: logger}
}

// refreshMetaDB forces metaDB to see the latest WAL changes written by
// DuckLake's internal SQLite handle after DDL operations (CREATE/DROP
// SCHEMA/TABLE). Without this, metaDB may read a stale WAL snapshot and
// miss rows that DuckLake just inserted or updated.
//
// The approach: cycle the pool's idle connections so the next query opens a
// fresh SQLite read snapshot that includes the DuckLake WAL entries.
func (r *CatalogRepo) refreshMetaDB(_ context.Context) {
	cur := r.metaDB.Stats().MaxOpenConnections
	r.metaDB.SetMaxIdleConns(0)
	if cur > 0 {
		r.metaDB.SetMaxIdleConns(cur)
	} else {
		r.metaDB.SetMaxIdleConns(2) // Go default
	}
}

// GetCatalogInfo returns information about the single "lake" catalog.
func (r *CatalogRepo) GetCatalogInfo(ctx context.Context) (*domain.CatalogInfo, error) {
	info := &domain.CatalogInfo{
		Name: r.catalogName,
	}

	// Try to read comment from catalog_metadata via sqlc
	row, err := r.q.GetCatalogMetadata(ctx, dbstore.GetCatalogMetadataParams{
		SecurableType: "catalog",
		SecurableName: r.catalogName,
	})
	if err == nil {
		if row.Comment.Valid {
			info.Comment = row.Comment.String
		}
		info.CreatedAt, _ = time.Parse("2006-01-02 15:04:05", row.CreatedAt)
		info.UpdatedAt, _ = time.Parse("2006-01-02 15:04:05", row.UpdatedAt)
	}

	return info, nil
}

// GetMetastoreSummary returns high-level info about the DuckLake metastore.
// NOTE: These queries hit ducklake_* tables (not managed by sqlc).
func (r *CatalogRepo) GetMetastoreSummary(ctx context.Context) (*domain.MetastoreSummary, error) {
	summary := &domain.MetastoreSummary{
		CatalogName:    r.catalogName,
		MetastoreType:  "DuckLake (SQLite)",
		StorageBackend: "S3",
	}

	// Read data_path (ducklake_metadata — not managed by sqlc)
	var dataPath sql.NullString
	_ = r.metaDB.QueryRowContext(ctx,
		`SELECT value FROM ducklake_metadata WHERE key = 'data_path'`).Scan(&dataPath)
	if dataPath.Valid {
		summary.DataPath = dataPath.String
	}

	// Count schemas (ducklake_schema — not managed by sqlc)
	_ = r.metaDB.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM ducklake_schema WHERE end_snapshot IS NULL`).Scan(&summary.SchemaCount)

	// Count tables (ducklake_table — not managed by sqlc)
	_ = r.metaDB.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM ducklake_table WHERE end_snapshot IS NULL`).Scan(&summary.TableCount)

	return summary, nil
}
