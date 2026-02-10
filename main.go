package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"strings"

	_ "github.com/marcboeker/go-duckdb"

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
	db, err := sql.Open("duckdb", "")
	if err != nil {
		log.Fatalf("failed to open duckdb: %v", err)
	}
	defer db.Close()

	// Install substrait extension
	if _, err := db.Exec("INSTALL substrait; LOAD substrait;"); err != nil {
		log.Fatalf("failed to load substrait: %v", err)
	}

	// Register parquet as a table
	if _, err := db.Exec("CREATE TABLE titanic AS SELECT * FROM 'titanic.parquet'"); err != nil {
		log.Fatalf("failed to create table: %v", err)
	}

	eng := engine.NewSecureEngine(db, buildPolicyStore())

	query := `SELECT "PassengerId", "Name", "Pclass", "Survived", "Sex" FROM titanic LIMIT 10`

	// Allow overriding role via CLI arg
	roles := []string{"admin", "first_class_analyst", "survivor_researcher", "no_access"}
	if len(os.Args) > 1 {
		roles = os.Args[1:]
	}

	ctx := context.Background()
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
