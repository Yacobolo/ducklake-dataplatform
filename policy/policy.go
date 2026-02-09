package policy

import "fmt"

// Operator constants for RLS rule conditions.
const (
	OpEqual        = "eq"
	OpNotEqual     = "neq"
	OpLessThan     = "lt"
	OpLessEqual    = "lte"
	OpGreaterThan  = "gt"
	OpGreaterEqual = "gte"
)

// RLSRule defines a row-level security filter applied to a specific table.
type RLSRule struct {
	Table    string      // table name this rule applies to
	Column   string      // column name to filter on
	Operator string      // comparison operator (use Op* constants)
	Value    interface{} // literal value to compare against
}

// Role defines a set of permissions: which tables can be accessed
// and what row-level filters are enforced.
type Role struct {
	Name          string
	AllowedTables []string  // table names, or ["*"] for wildcard
	DeniedTables  []string  // explicit deny (overrides allow)
	RLSRules      []RLSRule // row-level filters per table
}

// CanAccess returns true if this role is allowed to access the given table.
// Deny list takes precedence over allow list.
func (r *Role) CanAccess(table string) bool {
	// Check deny list first
	for _, d := range r.DeniedTables {
		if d == table {
			return false
		}
	}

	// Check allow list
	for _, a := range r.AllowedTables {
		if a == "*" || a == table {
			return true
		}
	}

	return false
}

// CheckAccess validates that this role can access all the given tables.
// Returns an error describing the first table that is denied.
func (r *Role) CheckAccess(tables []string) error {
	for _, t := range tables {
		if !r.CanAccess(t) {
			return fmt.Errorf("access denied: role %q cannot access table %q", r.Name, t)
		}
	}
	return nil
}

// RLSRulesForTable returns the RLS rules that apply to the given table.
func (r *Role) RLSRulesForTable(table string) []RLSRule {
	var rules []RLSRule
	for _, rule := range r.RLSRules {
		if rule.Table == table {
			rules = append(rules, rule)
		}
	}
	return rules
}

// PolicyStore holds all defined roles and provides lookup.
type PolicyStore struct {
	roles map[string]*Role
}

// NewPolicyStore creates an empty policy store.
func NewPolicyStore() *PolicyStore {
	return &PolicyStore{roles: make(map[string]*Role)}
}

// AddRole registers a role in the store.
func (s *PolicyStore) AddRole(role *Role) {
	s.roles[role.Name] = role
}

// GetRole returns the role with the given name, or an error if not found.
func (s *PolicyStore) GetRole(name string) (*Role, error) {
	role, ok := s.roles[name]
	if !ok {
		return nil, fmt.Errorf("unknown role: %q", name)
	}
	return role, nil
}
