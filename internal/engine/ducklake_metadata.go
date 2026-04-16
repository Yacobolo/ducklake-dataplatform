package engine

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/Yacobolo/quackstack/internal/ddl"
	"github.com/Yacobolo/quackstack/internal/domain"
)

func duckLakeMetadataCatalogAlias(catalogName string) string {
	return "__ducklake_meta_" + strings.TrimSpace(catalogName)
}

// AttachDuckLakeMetadataSchema exposes a DuckLake catalog's metastore tables as
// read-only views inside the attached catalog so they can be queried via
// catalog-qualified SQL such as lake.__ducklake_metadata_lake.ducklake_schema.
func AttachDuckLakeMetadataSchema(ctx context.Context, db *sql.DB, reg domain.CatalogRegistration) error {
	if strings.TrimSpace(reg.Name) == "" {
		return fmt.Errorf("ducklake metadata schema requires a catalog name")
	}

	sourceCatalog, sourceSchema, err := attachDuckLakeMetadataSource(ctx, db, reg)
	if err != nil {
		return err
	}
	if sourceCatalog == "" || sourceSchema == "" {
		return nil
	}

	metadataSchema := domain.DuckLakeMetadataSchemaName(reg.Name)
	createSchemaSQL := fmt.Sprintf(
		"CREATE SCHEMA IF NOT EXISTS %s.%s",
		ddl.QuoteIdentifier(reg.Name),
		ddl.QuoteIdentifier(metadataSchema),
	)
	if _, err := db.ExecContext(ctx, createSchemaSQL); err != nil {
		return fmt.Errorf("create ducklake metadata schema %q: %w", metadataSchema, err)
	}

	tableRows, err := db.QueryContext(ctx, fmt.Sprintf(
		`SELECT table_name
		 FROM information_schema.tables
		 WHERE table_catalog = %s AND table_schema = %s AND table_name LIKE 'ducklake_%%'
		 ORDER BY table_name`,
		ddl.QuoteLiteral(sourceCatalog),
		ddl.QuoteLiteral(sourceSchema),
	))
	if err != nil {
		return fmt.Errorf("list ducklake metadata tables for %q: %w", reg.Name, err)
	}
	defer tableRows.Close() //nolint:errcheck

	for tableRows.Next() {
		var tableName string
		if err := tableRows.Scan(&tableName); err != nil {
			return fmt.Errorf("scan ducklake metadata table for %q: %w", reg.Name, err)
		}
		createViewSQL := fmt.Sprintf(
			"CREATE OR REPLACE VIEW %s.%s.%s AS SELECT * FROM %s.%s.%s",
			ddl.QuoteIdentifier(reg.Name),
			ddl.QuoteIdentifier(metadataSchema),
			ddl.QuoteIdentifier(tableName),
			ddl.QuoteIdentifier(sourceCatalog),
			ddl.QuoteIdentifier(sourceSchema),
			ddl.QuoteIdentifier(tableName),
		)
		if _, err := db.ExecContext(ctx, createViewSQL); err != nil {
			return fmt.Errorf("create ducklake metadata view %q.%q.%q: %w", reg.Name, metadataSchema, tableName, err)
		}
	}
	if err := tableRows.Err(); err != nil {
		return fmt.Errorf("iterate ducklake metadata tables for %q: %w", reg.Name, err)
	}

	return nil
}

func attachDuckLakeMetadataSource(ctx context.Context, db *sql.DB, reg domain.CatalogRegistration) (catalogName string, schemaName string, err error) {
	switch reg.MetastoreType {
	case domain.MetastoreTypeSQLite:
		alias := duckLakeMetadataCatalogAlias(reg.Name)
		if !IsCatalogAttached(ctx, db, alias) {
			attachSQL := fmt.Sprintf(
				"ATTACH %s AS %s (TYPE sqlite, READ_ONLY)",
				ddl.QuoteLiteral(reg.DSN),
				ddl.QuoteIdentifier(alias),
			)
			if _, err := db.ExecContext(ctx, attachSQL); err != nil {
				return "", "", fmt.Errorf("attach ducklake sqlite metastore for %q: %w", reg.Name, err)
			}
		}
		return lookupDuckLakeMetadataSourceSchema(ctx, db, alias)
	case domain.MetastoreTypePostgres:
		// PostgreSQL-backed DuckLake metadata is not surfaced yet because it
		// requires a second attachment over the postgres extension with a stable
		// schema discovery path. SQLite-backed catalogs cover local/dev flows.
		return "", "", nil
	default:
		return "", "", fmt.Errorf("unsupported metastore type for ducklake metadata schema: %q", reg.MetastoreType)
	}
}

func lookupDuckLakeMetadataSourceSchema(ctx context.Context, db *sql.DB, sourceCatalog string) (catalogName string, schemaName string, err error) {
	var discoveredSchema string
	err = db.QueryRowContext(ctx,
		`SELECT table_schema
		 FROM information_schema.tables
		 WHERE table_catalog = ? AND table_name = 'ducklake_schema'
		 ORDER BY table_schema
		 LIMIT 1`,
		sourceCatalog,
	).Scan(&discoveredSchema)
	if err == sql.ErrNoRows {
		return "", "", nil
	}
	if err != nil {
		return "", "", fmt.Errorf("discover ducklake metadata schema in %q: %w", sourceCatalog, err)
	}
	return sourceCatalog, discoveredSchema, nil
}

// DetachDuckLakeMetadataCatalog detaches the auxiliary metastore catalog used
// to back queryable DuckLake metadata views.
func DetachDuckLakeMetadataCatalog(ctx context.Context, db *sql.DB, catalogName string) error {
	alias := duckLakeMetadataCatalogAlias(catalogName)
	if !IsCatalogAttached(ctx, db, alias) {
		return nil
	}
	return DetachCatalog(ctx, db, alias)
}
