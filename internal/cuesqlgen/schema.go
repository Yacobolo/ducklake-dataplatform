// Package cuesqlgen loads SQLite schema metadata from migrations for cue-sql generation.
package cuesqlgen

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	// Register the SQLite driver used for migration-backed schema introspection.
	_ "github.com/mattn/go-sqlite3"
)

// Catalog describes the available SQLite tables discovered from migrations.
type Catalog struct {
	Tables map[string]Table
}

// Table describes a SQLite table and its columns.
type Table struct {
	Name    string
	Columns []Column
}

// Column describes a single SQLite table column.
type Column struct {
	Name    string
	DBType  string
	NotNull bool
	PK      bool
}

// MustTable returns the named table or an error if it does not exist.
func (c Catalog) MustTable(name string) (Table, error) {
	table, ok := c.Tables[name]
	if !ok {
		return Table{}, fmt.Errorf("unknown table %q", name)
	}
	return table, nil
}

// ColumnsForTable returns the column names for the named table in storage order.
func (c Catalog) ColumnsForTable(name string) ([]string, error) {
	table, err := c.MustTable(name)
	if err != nil {
		return nil, err
	}
	columns := make([]string, 0, len(table.Columns))
	for _, column := range table.Columns {
		columns = append(columns, column.Name)
	}
	return columns, nil
}

// LoadCatalog applies migrations into a temporary SQLite database and introspects the resulting schema.
func LoadCatalog(migrationsDir string) (Catalog, error) {
	ctx := context.Background()
	tempFile, err := os.CreateTemp("", "cue-sql-*.sqlite")
	if err != nil {
		return Catalog{}, fmt.Errorf("create temp sqlite file: %w", err)
	}
	path := tempFile.Name()
	if err := tempFile.Close(); err != nil {
		return Catalog{}, fmt.Errorf("close temp sqlite file: %w", err)
	}
	defer func() {
		_ = os.Remove(path)
	}()

	db, err := sql.Open("sqlite3", path)
	if err != nil {
		return Catalog{}, fmt.Errorf("open temp sqlite database: %w", err)
	}
	defer db.Close() //nolint:errcheck

	files, err := filepath.Glob(filepath.Join(migrationsDir, "*.sql"))
	if err != nil {
		return Catalog{}, fmt.Errorf("glob migrations: %w", err)
	}
	sort.Strings(files)
	for _, file := range files {
		//nolint:gosec // The generator controls the migration directory contents.
		contents, err := os.ReadFile(file)
		if err != nil {
			return Catalog{}, fmt.Errorf("read migration %s: %w", file, err)
		}
		up := gooseUp(string(contents))
		if strings.TrimSpace(up) == "" {
			continue
		}
		if _, err := db.ExecContext(ctx, up); err != nil {
			return Catalog{}, fmt.Errorf("apply migration %s: %w", filepath.Base(file), err)
		}
	}

	rows, err := db.QueryContext(ctx, `SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%' ORDER BY name`)
	if err != nil {
		return Catalog{}, fmt.Errorf("list sqlite tables: %w", err)
	}
	defer rows.Close() //nolint:errcheck

	catalog := Catalog{Tables: make(map[string]Table)}
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return Catalog{}, fmt.Errorf("scan sqlite table name: %w", err)
		}
		table, err := introspectTable(ctx, db, tableName)
		if err != nil {
			return Catalog{}, err
		}
		catalog.Tables[tableName] = table
	}
	if err := rows.Err(); err != nil {
		return Catalog{}, fmt.Errorf("iterate sqlite tables: %w", err)
	}
	return catalog, nil
}

func gooseUp(contents string) string {
	upMarker := "-- +goose Up"
	downMarker := "-- +goose Down"
	start := strings.Index(contents, upMarker)
	if start == -1 {
		return contents
	}
	section := contents[start+len(upMarker):]
	if down := strings.Index(section, downMarker); down >= 0 {
		section = section[:down]
	}
	return section
}

func introspectTable(ctx context.Context, db *sql.DB, tableName string) (Table, error) {
	query := fmt.Sprintf("PRAGMA table_info(%q)", tableName)
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return Table{}, fmt.Errorf("introspect table %s: %w", tableName, err)
	}
	defer rows.Close() //nolint:errcheck

	table := Table{Name: tableName}
	for rows.Next() {
		var (
			cid        int
			name       string
			dbType     string
			notNull    int
			defaultVal sql.NullString
			pk         int
		)
		if err := rows.Scan(&cid, &name, &dbType, &notNull, &defaultVal, &pk); err != nil {
			return Table{}, fmt.Errorf("scan table_info for %s: %w", tableName, err)
		}
		table.Columns = append(table.Columns, Column{
			Name:    name,
			DBType:  dbType,
			NotNull: notNull == 1 || pk == 1,
			PK:      pk == 1,
		})
	}
	if err := rows.Err(); err != nil {
		return Table{}, fmt.Errorf("iterate table_info for %s: %w", tableName, err)
	}
	return table, nil
}
