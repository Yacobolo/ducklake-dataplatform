package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"strings"

	_ "github.com/duckdb/duckdb-go/v2"

	"duck-demo/config"
	"duck-demo/engine"
	"duck-demo/policy"
)

// Hardcoded RBAC + RLS policy.
func buildPolicyStore() *policy.PolicyStore {
	store := policy.NewPolicyStore()

	store.UpdateRole(&policy.Role{
		Name:          "admin",
		AllowedTables: []string{"*"},
	})

	store.UpdateRole(&policy.Role{
		Name:          "first_class_analyst",
		AllowedTables: []string{"titanic"},
		RLSRules: []policy.RLSRule{
			{Table: "titanic", Column: "Pclass", Operator: policy.OpEqual, Value: int64(1)},
		},
	})

	store.UpdateRole(&policy.Role{
		Name:          "survivor_researcher",
		AllowedTables: []string{"titanic"},
		RLSRules: []policy.RLSRule{
			{Table: "titanic", Column: "Survived", Operator: policy.OpEqual, Value: int64(1)},
		},
	})

	store.UpdateRole(&policy.Role{
		Name:          "no_access",
		AllowedTables: []string{},
	})

	return store
}

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

func main() {
	ctx := context.Background()

	// Load .env file (if present)
	if err := config.LoadDotEnv(".env"); err != nil {
		log.Printf("warning: could not load .env: %v", err)
	}

	db, err := sql.Open("duckdb", "")
	if err != nil {
		log.Fatalf("failed to open duckdb: %v", err)
	}
	defer db.Close()

	// Try to set up DuckLake with Hetzner S3
	cfg, cfgErr := config.LoadFromEnv()
	if cfgErr == nil {
		fmt.Println("Setting up DuckLake with Hetzner S3...")
		if err := engine.SetupDuckLake(ctx, db, cfg); err != nil {
			log.Printf("DuckLake setup failed, falling back to local parquet: %v", err)
			// Fallback: load from local parquet
			if _, err := db.Exec("CREATE TABLE titanic AS SELECT * FROM 'titanic.parquet'"); err != nil {
				log.Fatalf("failed to create table: %v", err)
			}
		} else {
			fmt.Println("DuckLake ready (data on Hetzner S3, metadata in SQLite)")
		}
	} else {
		fmt.Println("No S3 config found, using local parquet file...")
		// Fallback: load from local parquet
		if _, err := db.Exec("CREATE TABLE titanic AS SELECT * FROM 'titanic.parquet'"); err != nil {
			log.Fatalf("failed to create table: %v", err)
		}
	}

	eng := engine.NewSecureEngine(db, buildPolicyStore())

	query := `SELECT "PassengerId", "Name", "Pclass", "Survived", "Sex" FROM titanic LIMIT 10`

	// Allow overriding role via CLI arg
	roles := []string{"admin", "first_class_analyst", "survivor_researcher", "no_access"}
	if len(os.Args) > 1 {
		roles = os.Args[1:]
	}

	for _, role := range roles {
		fmt.Printf("\n=== Role: %s ===\n", role)
		fmt.Printf("Query: %s\n\n", query)

		rows, err := eng.Query(ctx, role, query)
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
}
