// Package sqlrewrite provides SQL-level RBAC and RLS enforcement.
//
// It parses SQL queries using the PostgreSQL parser (pg_query_go), extracts
// table names, checks access control, and injects WHERE clause filters for
// row-level security. This replaces the previous Substrait-based approach,
// removing the dependency on the DuckDB substrait extension.
package sqlrewrite

import (
	"fmt"
	"strings"

	"duck-demo/policy"

	pg_query "github.com/pganalyze/pg_query_go/v6"
)

// ExtractTableNames parses a SQL query and returns the deduplicated list
// of table names referenced in FROM clauses and JOINs.
// Handles compound names (e.g., "lake.main.titanic") by using the last element.
func ExtractTableNames(sql string) ([]string, error) {
	result, err := pg_query.Parse(sql)
	if err != nil {
		return nil, fmt.Errorf("parse SQL: %w", err)
	}

	seen := make(map[string]bool)
	var tables []string

	for _, stmt := range result.Stmts {
		collectTablesFromNode(stmt.Stmt, seen, &tables)
	}

	return tables, nil
}

// RewriteQuery parses the SQL query, injects WHERE clause conditions based on
// the RLS rules, and returns the rewritten SQL string.
// If rulesByTable is empty, the original query is returned unchanged.
func RewriteQuery(sql string, rulesByTable map[string][]policy.RLSRule) (string, error) {
	if len(rulesByTable) == 0 {
		return sql, nil
	}

	result, err := pg_query.Parse(sql)
	if err != nil {
		return "", fmt.Errorf("parse SQL: %w", err)
	}

	for _, stmt := range result.Stmts {
		if err := injectFiltersIntoNode(stmt.Stmt, rulesByTable); err != nil {
			return "", err
		}
	}

	output, err := pg_query.Deparse(result)
	if err != nil {
		return "", fmt.Errorf("deparse SQL: %w", err)
	}

	return output, nil
}

// resolveTableName extracts the table name from a compound identifier.
// For "lake.main.titanic", returns "titanic". For "titanic", returns "titanic".
func resolveTableName(schemaname, relname string) string {
	// relname is always the table name in PostgreSQL's RangeVar
	return relname
}

// collectTablesFromNode recursively walks a parse tree node, collecting
// table names from RangeVar references.
func collectTablesFromNode(node *pg_query.Node, seen map[string]bool, tables *[]string) {
	if node == nil {
		return
	}

	switch n := node.Node.(type) {
	case *pg_query.Node_SelectStmt:
		collectTablesFromSelectStmt(n.SelectStmt, seen, tables)
	case *pg_query.Node_InsertStmt:
		if n.InsertStmt.Relation != nil {
			addTable(n.InsertStmt.Relation.Relname, seen, tables)
		}
		if n.InsertStmt.SelectStmt != nil {
			collectTablesFromNode(n.InsertStmt.SelectStmt, seen, tables)
		}
	case *pg_query.Node_UpdateStmt:
		if n.UpdateStmt.Relation != nil {
			addTable(n.UpdateStmt.Relation.Relname, seen, tables)
		}
		for _, from := range n.UpdateStmt.FromClause {
			collectTablesFromNode(from, seen, tables)
		}
	case *pg_query.Node_DeleteStmt:
		if n.DeleteStmt.Relation != nil {
			addTable(n.DeleteStmt.Relation.Relname, seen, tables)
		}
	}
}

// collectTablesFromSelectStmt handles SELECT statements (including set operations).
func collectTablesFromSelectStmt(sel *pg_query.SelectStmt, seen map[string]bool, tables *[]string) {
	if sel == nil {
		return
	}

	// Handle UNION/INTERSECT/EXCEPT
	if sel.Larg != nil {
		collectTablesFromSelectStmt(sel.Larg, seen, tables)
	}
	if sel.Rarg != nil {
		collectTablesFromSelectStmt(sel.Rarg, seen, tables)
	}

	// FROM clause
	for _, from := range sel.FromClause {
		collectTablesFromFromNode(from, seen, tables)
	}

	// WHERE clause subqueries
	collectTablesFromExpr(sel.WhereClause, seen, tables)

	// HAVING clause subqueries
	collectTablesFromExpr(sel.HavingClause, seen, tables)

	// Target list subqueries
	for _, target := range sel.TargetList {
		collectTablesFromExpr(target, seen, tables)
	}

	// WITH (CTEs)
	if sel.WithClause != nil {
		for _, cte := range sel.WithClause.Ctes {
			if c, ok := cte.Node.(*pg_query.Node_CommonTableExpr); ok {
				collectTablesFromNode(c.CommonTableExpr.Ctequery, seen, tables)
			}
		}
	}
}

// collectTablesFromFromNode handles nodes in FROM clauses.
func collectTablesFromFromNode(node *pg_query.Node, seen map[string]bool, tables *[]string) {
	if node == nil {
		return
	}

	switch n := node.Node.(type) {
	case *pg_query.Node_RangeVar:
		addTable(n.RangeVar.Relname, seen, tables)
	case *pg_query.Node_JoinExpr:
		collectTablesFromFromNode(n.JoinExpr.Larg, seen, tables)
		collectTablesFromFromNode(n.JoinExpr.Rarg, seen, tables)
	case *pg_query.Node_RangeSubselect:
		collectTablesFromNode(n.RangeSubselect.Subquery, seen, tables)
	case *pg_query.Node_RangeFunction:
		// Table-valued functions — skip, not a real table
	}
}

// collectTablesFromExpr walks expression nodes looking for subqueries.
func collectTablesFromExpr(node *pg_query.Node, seen map[string]bool, tables *[]string) {
	if node == nil {
		return
	}

	switch n := node.Node.(type) {
	case *pg_query.Node_SubLink:
		collectTablesFromNode(n.SubLink.Subselect, seen, tables)
	case *pg_query.Node_BoolExpr:
		for _, arg := range n.BoolExpr.Args {
			collectTablesFromExpr(arg, seen, tables)
		}
	case *pg_query.Node_AExpr:
		collectTablesFromExpr(n.AExpr.Lexpr, seen, tables)
		collectTablesFromExpr(n.AExpr.Rexpr, seen, tables)
	case *pg_query.Node_ResTarget:
		collectTablesFromExpr(n.ResTarget.Val, seen, tables)
	}
}

func addTable(name string, seen map[string]bool, tables *[]string) {
	if name == "" || seen[name] {
		return
	}
	seen[name] = true
	*tables = append(*tables, name)
}

// injectFiltersIntoNode finds SELECT statements and injects WHERE conditions.
func injectFiltersIntoNode(node *pg_query.Node, rulesByTable map[string][]policy.RLSRule) error {
	if node == nil {
		return nil
	}

	switch n := node.Node.(type) {
	case *pg_query.Node_SelectStmt:
		return injectFiltersIntoSelectStmt(n.SelectStmt, rulesByTable)
	}
	return nil
}

// injectFiltersIntoSelectStmt injects WHERE conditions into a SELECT statement
// based on the tables referenced in its FROM clause.
func injectFiltersIntoSelectStmt(sel *pg_query.SelectStmt, rulesByTable map[string][]policy.RLSRule) error {
	if sel == nil {
		return nil
	}

	// Handle UNION/INTERSECT/EXCEPT — recurse into both sides
	if sel.Larg != nil {
		if err := injectFiltersIntoSelectStmt(sel.Larg, rulesByTable); err != nil {
			return err
		}
	}
	if sel.Rarg != nil {
		if err := injectFiltersIntoSelectStmt(sel.Rarg, rulesByTable); err != nil {
			return err
		}
	}

	// Handle CTEs
	if sel.WithClause != nil {
		for _, cte := range sel.WithClause.Ctes {
			if c, ok := cte.Node.(*pg_query.Node_CommonTableExpr); ok {
				if err := injectFiltersIntoNode(c.CommonTableExpr.Ctequery, rulesByTable); err != nil {
					return err
				}
			}
		}
	}

	// Collect tables from FROM clause (with their aliases)
	tableRefs := collectTableRefs(sel.FromClause)

	// Build filter expressions for each table that has RLS rules
	var filterNodes []*pg_query.Node
	for _, ref := range tableRefs {
		rules, ok := rulesByTable[ref.tableName]
		if !ok || len(rules) == 0 {
			continue
		}

		for _, rule := range rules {
			expr, err := buildRuleExpr(rule, ref.alias)
			if err != nil {
				return err
			}
			filterNodes = append(filterNodes, expr)
		}
	}

	if len(filterNodes) == 0 {
		return nil
	}

	// Combine all filter nodes with AND
	combinedFilter := combineWithAnd(filterNodes)

	// Inject into WHERE clause
	if sel.WhereClause == nil {
		sel.WhereClause = combinedFilter
	} else {
		// Combine existing WHERE with new filters using AND
		sel.WhereClause = makeAndExpr(sel.WhereClause, combinedFilter)
	}

	// Also recurse into subqueries in FROM clause
	for _, from := range sel.FromClause {
		if err := injectFiltersIntoFromNode(from, rulesByTable); err != nil {
			return err
		}
	}

	// Recurse into subqueries in WHERE clause expressions
	if err := injectFiltersIntoExpr(sel.WhereClause, rulesByTable); err != nil {
		return err
	}

	return nil
}

// tableRef holds a table name and its alias (if any) for column qualification.
type tableRef struct {
	tableName string
	alias     string // empty if no alias; use tableName for column refs
}

// collectTableRefs extracts table references from FROM clause nodes.
func collectTableRefs(fromClause []*pg_query.Node) []tableRef {
	var refs []tableRef
	for _, node := range fromClause {
		collectTableRefsFromNode(node, &refs)
	}
	return refs
}

func collectTableRefsFromNode(node *pg_query.Node, refs *[]tableRef) {
	if node == nil {
		return
	}

	switch n := node.Node.(type) {
	case *pg_query.Node_RangeVar:
		ref := tableRef{tableName: n.RangeVar.Relname}
		if n.RangeVar.Alias != nil && n.RangeVar.Alias.Aliasname != "" {
			ref.alias = n.RangeVar.Alias.Aliasname
		}
		*refs = append(*refs, ref)
	case *pg_query.Node_JoinExpr:
		collectTableRefsFromNode(n.JoinExpr.Larg, refs)
		collectTableRefsFromNode(n.JoinExpr.Rarg, refs)
	}
}

// injectFiltersIntoFromNode recurses into subqueries in FROM clause.
func injectFiltersIntoFromNode(node *pg_query.Node, rulesByTable map[string][]policy.RLSRule) error {
	if node == nil {
		return nil
	}

	switch n := node.Node.(type) {
	case *pg_query.Node_RangeSubselect:
		return injectFiltersIntoNode(n.RangeSubselect.Subquery, rulesByTable)
	case *pg_query.Node_JoinExpr:
		if err := injectFiltersIntoFromNode(n.JoinExpr.Larg, rulesByTable); err != nil {
			return err
		}
		return injectFiltersIntoFromNode(n.JoinExpr.Rarg, rulesByTable)
	}
	return nil
}

// injectFiltersIntoExpr recurses into subqueries in expressions.
func injectFiltersIntoExpr(node *pg_query.Node, rulesByTable map[string][]policy.RLSRule) error {
	if node == nil {
		return nil
	}

	switch n := node.Node.(type) {
	case *pg_query.Node_SubLink:
		return injectFiltersIntoNode(n.SubLink.Subselect, rulesByTable)
	case *pg_query.Node_BoolExpr:
		for _, arg := range n.BoolExpr.Args {
			if err := injectFiltersIntoExpr(arg, rulesByTable); err != nil {
				return err
			}
		}
	case *pg_query.Node_AExpr:
		if err := injectFiltersIntoExpr(n.AExpr.Lexpr, rulesByTable); err != nil {
			return err
		}
		return injectFiltersIntoExpr(n.AExpr.Rexpr, rulesByTable)
	}
	return nil
}

// buildRuleExpr creates an A_Expr node representing a single RLS rule condition.
// For example: "Pclass" = 1 or t."Survived" = 1
func buildRuleExpr(rule policy.RLSRule, tableAlias string) (*pg_query.Node, error) {
	// Left side: column reference (optionally qualified with table alias)
	colRef := makeColumnRef(rule.Column, tableAlias)

	// Right side: literal value
	literal, err := makeLiteral(rule.Value)
	if err != nil {
		return nil, fmt.Errorf("RLS rule for %s.%s: %w", rule.Table, rule.Column, err)
	}

	// Operator
	opName, err := operatorToSQL(rule.Operator)
	if err != nil {
		return nil, err
	}

	return &pg_query.Node{
		Node: &pg_query.Node_AExpr{
			AExpr: &pg_query.A_Expr{
				Kind:  pg_query.A_Expr_Kind_AEXPR_OP,
				Name:  []*pg_query.Node{makeStringNode(opName)},
				Lexpr: colRef,
				Rexpr: literal,
			},
		},
	}, nil
}

// operatorToSQL converts a policy operator constant to a SQL operator string.
func operatorToSQL(op string) (string, error) {
	switch op {
	case policy.OpEqual:
		return "=", nil
	case policy.OpNotEqual:
		return "<>", nil
	case policy.OpLessThan:
		return "<", nil
	case policy.OpLessEqual:
		return "<=", nil
	case policy.OpGreaterThan:
		return ">", nil
	case policy.OpGreaterEqual:
		return ">=", nil
	default:
		return "", fmt.Errorf("unsupported operator: %q", op)
	}
}

// makeColumnRef creates a ColumnRef node. If tableAlias is non-empty,
// it creates a qualified reference (alias."column"), otherwise just ("column").
func makeColumnRef(column, tableAlias string) *pg_query.Node {
	var fields []*pg_query.Node
	if tableAlias != "" {
		fields = append(fields, makeStringNode(tableAlias))
	}
	fields = append(fields, makeStringNode(column))

	return &pg_query.Node{
		Node: &pg_query.Node_ColumnRef{
			ColumnRef: &pg_query.ColumnRef{
				Fields: fields,
			},
		},
	}
}

// makeLiteral creates an A_Const node for the given Go value.
func makeLiteral(v interface{}) (*pg_query.Node, error) {
	switch val := v.(type) {
	case int:
		return makeIntegerConst(int64(val)), nil
	case int8:
		return makeIntegerConst(int64(val)), nil
	case int16:
		return makeIntegerConst(int64(val)), nil
	case int32:
		return makeIntegerConst(int64(val)), nil
	case int64:
		return makeIntegerConst(val), nil
	case float32:
		return makeFloatConst(fmt.Sprintf("%g", val)), nil
	case float64:
		return makeFloatConst(fmt.Sprintf("%g", val)), nil
	case string:
		return makeStringConst(val), nil
	case bool:
		// PostgreSQL represents bools as string constants 'true'/'false' in some contexts,
		// but for WHERE clauses we use the keyword form via TypeCast.
		if val {
			return makeStringConst("true"), nil
		}
		return makeStringConst("false"), nil
	default:
		return nil, fmt.Errorf("unsupported literal type: %T", v)
	}
}

func makeIntegerConst(v int64) *pg_query.Node {
	return &pg_query.Node{
		Node: &pg_query.Node_AConst{
			AConst: &pg_query.A_Const{
				Val: &pg_query.A_Const_Ival{
					Ival: &pg_query.Integer{Ival: int32(v)},
				},
			},
		},
	}
}

func makeFloatConst(v string) *pg_query.Node {
	return &pg_query.Node{
		Node: &pg_query.Node_AConst{
			AConst: &pg_query.A_Const{
				Val: &pg_query.A_Const_Fval{
					Fval: &pg_query.Float{Fval: v},
				},
			},
		},
	}
}

func makeStringConst(v string) *pg_query.Node {
	return &pg_query.Node{
		Node: &pg_query.Node_AConst{
			AConst: &pg_query.A_Const{
				Val: &pg_query.A_Const_Sval{
					Sval: &pg_query.String{Sval: v},
				},
			},
		},
	}
}

func makeStringNode(s string) *pg_query.Node {
	return &pg_query.Node{
		Node: &pg_query.Node_String_{
			String_: &pg_query.String{Sval: s},
		},
	}
}

// combineWithAnd combines multiple expressions into a single BoolExpr AND.
// If there's only one expression, returns it directly.
func combineWithAnd(exprs []*pg_query.Node) *pg_query.Node {
	if len(exprs) == 1 {
		return exprs[0]
	}
	return &pg_query.Node{
		Node: &pg_query.Node_BoolExpr{
			BoolExpr: &pg_query.BoolExpr{
				Boolop: pg_query.BoolExprType_AND_EXPR,
				Args:   exprs,
			},
		},
	}
}

// makeAndExpr creates a BoolExpr AND combining two expressions.
func makeAndExpr(left, right *pg_query.Node) *pg_query.Node {
	// If either side is already an AND, flatten it
	var args []*pg_query.Node

	if be, ok := left.Node.(*pg_query.Node_BoolExpr); ok && be.BoolExpr.Boolop == pg_query.BoolExprType_AND_EXPR {
		args = append(args, be.BoolExpr.Args...)
	} else {
		args = append(args, left)
	}

	if be, ok := right.Node.(*pg_query.Node_BoolExpr); ok && be.BoolExpr.Boolop == pg_query.BoolExprType_AND_EXPR {
		args = append(args, be.BoolExpr.Args...)
	} else {
		args = append(args, right)
	}

	return &pg_query.Node{
		Node: &pg_query.Node_BoolExpr{
			BoolExpr: &pg_query.BoolExpr{
				Boolop: pg_query.BoolExprType_AND_EXPR,
				Args:   args,
			},
		},
	}
}

// QuoteIdentifier quotes a SQL identifier if it contains special characters
// or is a reserved word. Uses double quotes.
func QuoteIdentifier(s string) string {
	// Simple check: if it's all lowercase alphanumeric + underscore, no quoting needed
	for _, c := range s {
		if !((c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '_') {
			return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
		}
	}
	return s
}
