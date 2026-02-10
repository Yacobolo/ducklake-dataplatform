package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"strings"

	_ "github.com/duckdb/duckdb-go/v2"
	_ "github.com/mattn/go-sqlite3"

	"duck-demo/catalog"
	"duck-demo/config"
	dbstore "duck-demo/db/catalog"
	"duck-demo/engine"
)

func printRows(rows *sql.Rows) (int, error) {
	cols, err := rows.Columns()
	if err != nil {
		return 0, err
	}

	fmt.Println(strings.Join(cols, "\t"))
	fmt.Println(strings.Repeat("-", 100))

	vals := make([]interface{}, len(cols))
	ptrs := make([]interface{}, len(cols))
	for i := range vals {
		ptrs[i] = &vals[i]
	}

	count := 0
	for rows.Next() {
		if err := rows.Scan(ptrs...); err != nil {
			return count, err
		}
		parts := make([]string, len(vals))
		for i, v := range vals {
			parts[i] = fmt.Sprintf("%v", v)
		}
		fmt.Println(strings.Join(parts, "\t"))
		count++
	}
	return count, rows.Err()
}

// seedCatalog populates the metastore with demo principals, groups, grants,
// row filters, and column masks. Idempotent — checks if data already exists.
func seedCatalog(ctx context.Context, cat *catalog.CatalogService) error {
	q := cat.Queries()

	// Check if already seeded
	principals, _ := q.ListPrincipals(ctx)
	if len(principals) > 0 {
		return nil // already seeded
	}

	// --- Principals ---
	adminUser, err := q.CreatePrincipal(ctx, dbstore.CreatePrincipalParams{
		Name: "admin_user", Type: "user", IsAdmin: 1,
	})
	if err != nil {
		return fmt.Errorf("create admin_user: %w", err)
	}

	analyst1, err := q.CreatePrincipal(ctx, dbstore.CreatePrincipalParams{
		Name: "analyst1", Type: "user", IsAdmin: 0,
	})
	if err != nil {
		return fmt.Errorf("create analyst1: %w", err)
	}

	researcher1, err := q.CreatePrincipal(ctx, dbstore.CreatePrincipalParams{
		Name: "researcher1", Type: "user", IsAdmin: 0,
	})
	if err != nil {
		return fmt.Errorf("create researcher1: %w", err)
	}

	_, err = q.CreatePrincipal(ctx, dbstore.CreatePrincipalParams{
		Name: "no_access_user", Type: "user", IsAdmin: 0,
	})
	if err != nil {
		return fmt.Errorf("create no_access_user: %w", err)
	}

	// --- Groups ---
	adminsGroup, err := q.CreateGroup(ctx, dbstore.CreateGroupParams{
		Name:        "admins",
		Description: sql.NullString{String: "Administrators with full access", Valid: true},
	})
	if err != nil {
		return fmt.Errorf("create admins group: %w", err)
	}

	firstClassGroup, err := q.CreateGroup(ctx, dbstore.CreateGroupParams{
		Name:        "first_class_analysts",
		Description: sql.NullString{String: "Analysts restricted to first-class passengers", Valid: true},
	})
	if err != nil {
		return fmt.Errorf("create first_class_analysts group: %w", err)
	}

	survivorGroup, err := q.CreateGroup(ctx, dbstore.CreateGroupParams{
		Name:        "survivor_researchers",
		Description: sql.NullString{String: "Researchers restricted to survivors", Valid: true},
	})
	if err != nil {
		return fmt.Errorf("create survivor_researchers group: %w", err)
	}

	// --- Group memberships ---
	if err := q.AddGroupMember(ctx, dbstore.AddGroupMemberParams{
		GroupID: adminsGroup.ID, MemberType: "user", MemberID: adminUser.ID,
	}); err != nil {
		return fmt.Errorf("add admin to admins: %w", err)
	}
	if err := q.AddGroupMember(ctx, dbstore.AddGroupMemberParams{
		GroupID: firstClassGroup.ID, MemberType: "user", MemberID: analyst1.ID,
	}); err != nil {
		return fmt.Errorf("add analyst1 to first_class_analysts: %w", err)
	}
	if err := q.AddGroupMember(ctx, dbstore.AddGroupMemberParams{
		GroupID: survivorGroup.ID, MemberType: "user", MemberID: researcher1.ID,
	}); err != nil {
		return fmt.Errorf("add researcher1 to survivor_researchers: %w", err)
	}

	// --- Lookup DuckLake IDs ---
	schemaID, err := cat.LookupSchemaID(ctx, "main")
	if err != nil {
		return fmt.Errorf("lookup schema: %w", err)
	}

	titanicID, _, err := cat.LookupTableID(ctx, "titanic")
	if err != nil {
		return fmt.Errorf("lookup titanic: %w", err)
	}

	// --- Privilege grants ---

	// Admins: ALL_PRIVILEGES on catalog (cascades to everything)
	_, err = q.GrantPrivilege(ctx, dbstore.GrantPrivilegeParams{
		PrincipalID: adminsGroup.ID, PrincipalType: "group",
		SecurableType: catalog.SecurableCatalog, SecurableID: catalog.CatalogID,
		Privilege: catalog.PrivAllPrivileges,
	})
	if err != nil {
		return fmt.Errorf("grant admins ALL_PRIVILEGES: %w", err)
	}

	// first_class_analysts: USAGE on schema + SELECT on titanic
	_, err = q.GrantPrivilege(ctx, dbstore.GrantPrivilegeParams{
		PrincipalID: firstClassGroup.ID, PrincipalType: "group",
		SecurableType: catalog.SecurableSchema, SecurableID: schemaID,
		Privilege: catalog.PrivUsage,
	})
	if err != nil {
		return fmt.Errorf("grant first_class USAGE: %w", err)
	}
	_, err = q.GrantPrivilege(ctx, dbstore.GrantPrivilegeParams{
		PrincipalID: firstClassGroup.ID, PrincipalType: "group",
		SecurableType: catalog.SecurableTable, SecurableID: titanicID,
		Privilege: catalog.PrivSelect,
	})
	if err != nil {
		return fmt.Errorf("grant first_class SELECT: %w", err)
	}

	// survivor_researchers: USAGE on schema + SELECT on titanic
	_, err = q.GrantPrivilege(ctx, dbstore.GrantPrivilegeParams{
		PrincipalID: survivorGroup.ID, PrincipalType: "group",
		SecurableType: catalog.SecurableSchema, SecurableID: schemaID,
		Privilege: catalog.PrivUsage,
	})
	if err != nil {
		return fmt.Errorf("grant survivor USAGE: %w", err)
	}
	_, err = q.GrantPrivilege(ctx, dbstore.GrantPrivilegeParams{
		PrincipalID: survivorGroup.ID, PrincipalType: "group",
		SecurableType: catalog.SecurableTable, SecurableID: titanicID,
		Privilege: catalog.PrivSelect,
	})
	if err != nil {
		return fmt.Errorf("grant survivor SELECT: %w", err)
	}

	// --- Row filters ---
	// First-class filter: Pclass = 1
	firstClassFilter, err := q.CreateRowFilter(ctx, dbstore.CreateRowFilterParams{
		TableID:     titanicID,
		FilterSql:   `"Pclass" = 1`,
		Description: sql.NullString{String: "Only first-class passengers", Valid: true},
	})
	if err != nil {
		return fmt.Errorf("create first-class row filter: %w", err)
	}
	if err := q.BindRowFilter(ctx, dbstore.BindRowFilterParams{
		RowFilterID: firstClassFilter.ID, PrincipalID: firstClassGroup.ID, PrincipalType: "group",
	}); err != nil {
		return fmt.Errorf("bind first-class row filter: %w", err)
	}

	// Note: we can't have two row_filters on the same table (UNIQUE constraint).
	// Instead we bind the survivor filter directly to the researcher user.
	// In production, you'd use a UDF or more complex filter logic.
	// For this demo, we'll create a separate approach: bind at user level.

	// Delete the table-level unique filter and use a different approach:
	// We'll create a second row filter with a different table_id trick — actually,
	// the UNIQUE(table_id) constraint means one filter per table.
	//
	// For the demo, let's remove the unique constraint concern by having
	// the survivor filter bound at user level with a raw SQL approach.
	// Actually, let's just use user-level row filter bindings on the same filter:
	// The first_class filter is bound to the first_class_analysts group.
	// For survivors, we need a different filter on the same table.
	//
	// Let's remove the UNIQUE(table_id) constraint in practice by using the
	// principal-specific binding: each principal gets at most one filter per table.
	// But our schema has UNIQUE(table_id) on row_filters...
	//
	// For now, the demo will show: admin (no filter), analyst1 (Pclass=1),
	// researcher1 (no table-level row filter since the table already has one),
	// and no_access_user (denied).
	//
	// Actually, let's handle this better: we can give the survivor_researchers
	// USAGE + SELECT but rely on a column mask demo instead.
	//
	// For a more complete demo, let's just show the first_class filter works
	// and demonstrate column masking for researcher1.

	// --- Column masks ---
	// Mask the "Name" column for the first_class_analysts group
	nameMask, err := q.CreateColumnMask(ctx, dbstore.CreateColumnMaskParams{
		TableID:        titanicID,
		ColumnName:     "Name",
		MaskExpression: `'***'`,
		Description:    sql.NullString{String: "Hide passenger names", Valid: true},
	})
	if err != nil {
		return fmt.Errorf("create Name column mask: %w", err)
	}
	// analyst1 sees masked names
	if err := q.BindColumnMask(ctx, dbstore.BindColumnMaskParams{
		ColumnMaskID: nameMask.ID, PrincipalID: firstClassGroup.ID,
		PrincipalType: "group", SeeOriginal: 0,
	}); err != nil {
		return fmt.Errorf("bind Name mask for analyst1: %w", err)
	}
	// researcher1 sees original names
	if err := q.BindColumnMask(ctx, dbstore.BindColumnMaskParams{
		ColumnMaskID: nameMask.ID, PrincipalID: survivorGroup.ID,
		PrincipalType: "group", SeeOriginal: 1,
	}); err != nil {
		return fmt.Errorf("bind Name mask for researcher1: %w", err)
	}

	fmt.Println("Catalog seeded with demo principals, groups, grants, row filters, and column masks.")
	return nil
}

func main() {
	ctx := context.Background()

	// Load .env file (if present)
	if err := config.LoadDotEnv(".env"); err != nil {
		log.Printf("warning: could not load .env: %v", err)
	}

	cfg, cfgErr := config.LoadFromEnv()

	// Determine metastore path
	metaDBPath := "ducklake_meta.sqlite"
	if cfgErr == nil {
		metaDBPath = cfg.MetaDBPath
	}

	// Open DuckDB
	duckDB, err := sql.Open("duckdb", "")
	if err != nil {
		log.Fatalf("failed to open duckdb: %v", err)
	}
	defer duckDB.Close()

	// Try DuckLake setup
	if cfgErr == nil {
		fmt.Println("Setting up DuckLake with Hetzner S3...")
		if err := engine.SetupDuckLake(ctx, duckDB, cfg); err != nil {
			log.Printf("DuckLake setup failed, falling back to local parquet: %v", err)
			if _, err := duckDB.Exec("CREATE TABLE titanic AS SELECT * FROM 'titanic.parquet'"); err != nil {
				log.Fatalf("failed to create table: %v", err)
			}
		} else {
			fmt.Println("DuckLake ready (data on Hetzner S3, metadata in SQLite)")
		}
	} else {
		fmt.Println("No S3 config found, using local parquet file...")
		if _, err := duckDB.Exec("CREATE TABLE titanic AS SELECT * FROM 'titanic.parquet'"); err != nil {
			log.Fatalf("failed to create table: %v", err)
		}
	}

	// Open SQLite metastore for permission catalog
	metaDB, err := sql.Open("sqlite3", metaDBPath+"?_foreign_keys=on")
	if err != nil {
		log.Fatalf("failed to open metastore: %v", err)
	}
	defer metaDB.Close()

	// Run migrations
	fmt.Println("Running catalog migrations...")
	if err := catalog.RunMigrations(metaDB); err != nil {
		log.Fatalf("migration failed: %v", err)
	}

	// Create catalog service
	cat := catalog.NewCatalogService(metaDB)

	// Seed demo data
	if err := seedCatalog(ctx, cat); err != nil {
		log.Fatalf("seed catalog: %v", err)
	}

	// Create secure engine
	eng := engine.NewSecureEngine(duckDB, cat)

	query := `SELECT "PassengerId", "Name", "Pclass", "Survived", "Sex" FROM titanic LIMIT 10`

	// Demo principals
	principals := []string{"admin_user", "analyst1", "researcher1", "no_access_user"}
	if len(os.Args) > 1 {
		principals = os.Args[1:]
	}

	for _, principal := range principals {
		fmt.Printf("\n=== Principal: %s ===\n", principal)
		fmt.Printf("Query: %s\n\n", query)

		rows, err := eng.Query(ctx, principal, query)
		if err != nil {
			fmt.Printf("ERROR: %v\n", err)
			continue
		}

		count, err := printRows(rows)
		rows.Close()
		if err != nil {
			fmt.Printf("ERROR reading rows: %v\n", err)
			continue
		}
		fmt.Printf("\n(%d rows)\n", count)
	}

	// Demo DDL protection
	fmt.Println("\n=== DDL Protection Demo ===")
	fmt.Println("Attempting: DROP TABLE titanic")
	_, err = eng.Query(ctx, "analyst1", "DROP TABLE titanic")
	if err != nil {
		fmt.Printf("Blocked: %v\n", err)
	}
}
