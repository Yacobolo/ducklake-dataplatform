package engine

import (
	"database/sql"
	"fmt"

	"duck-demo/policy"

	pb "github.com/substrait-io/substrait-protobuf/go/substraitpb"
	"google.golang.org/protobuf/proto"
)

// SecureEngine wraps a DuckDB connection and enforces RBAC + RLS
// by intercepting queries through the substrait plan.
type SecureEngine struct {
	db    *sql.DB
	store *policy.PolicyStore
}

// NewSecureEngine creates a SecureEngine with the given database and policy store.
func NewSecureEngine(db *sql.DB, store *policy.PolicyStore) *SecureEngine {
	return &SecureEngine{db: db, store: store}
}

// Query executes a SQL query as the given role, enforcing RBAC and RLS.
//
// The flow:
//  1. Look up the role in the policy store
//  2. Get the substrait plan from DuckDB via get_substrait()
//  3. Extract table names from the plan (walker)
//  4. Check RBAC: does the role have access to all tables?
//  5. Apply RLS: inject FilterRel for each table with rules
//  6. Execute the modified plan via from_substrait()
func (e *SecureEngine) Query(roleName, sqlQuery string) (*sql.Rows, error) {
	// 1. Look up role
	role, err := e.store.GetRole(roleName)
	if err != nil {
		return nil, fmt.Errorf("policy error: %w", err)
	}

	// 2. Get substrait plan
	var blob []byte
	err = e.db.QueryRow("CALL get_substrait($1)", sqlQuery).Scan(&blob)
	if err != nil {
		return nil, fmt.Errorf("get_substrait: %w", err)
	}

	// 3. Unmarshal the plan
	plan := &pb.Plan{}
	if err := proto.Unmarshal(blob, plan); err != nil {
		return nil, fmt.Errorf("unmarshal plan: %w", err)
	}

	// 4. Extract tables and check RBAC
	tables := ExtractTableNames(plan)
	if err := role.CheckAccess(tables); err != nil {
		return nil, err
	}

	// 5. Build RLS rules map for tables in this query
	rulesByTable := make(map[string][]policy.RLSRule)
	for _, table := range tables {
		rules := role.RLSRulesForTable(table)
		if len(rules) > 0 {
			rulesByTable[table] = rules
		}
	}

	// 6. Rewrite the plan with RLS filters
	if err := RewritePlan(plan, rulesByTable); err != nil {
		return nil, fmt.Errorf("rewrite plan: %w", err)
	}

	// 7. Marshal the modified plan
	modifiedBlob, err := proto.Marshal(plan)
	if err != nil {
		return nil, fmt.Errorf("marshal plan: %w", err)
	}

	// 8. Execute via from_substrait
	rows, err := e.db.Query("CALL from_substrait($1::BLOB)", modifiedBlob)
	if err != nil {
		return nil, fmt.Errorf("from_substrait: %w", err)
	}

	return rows, nil
}
