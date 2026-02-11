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

	pg_query "github.com/pganalyze/pg_query_go/v6"
)

// Operator constants for RLS rule conditions (migrated from policy package).
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
func RewriteQuery(sql string, rulesByTable map[string][]RLSRule) (string, error) {
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

// StatementType represents the kind of SQL statement.
type StatementType int

const (
	StmtSelect StatementType = iota
	StmtInsert
	StmtUpdate
	StmtDelete
	StmtDDL
	StmtOther
)

func (t StatementType) String() string {
	switch t {
	case StmtSelect:
		return "SELECT"
	case StmtInsert:
		return "INSERT"
	case StmtUpdate:
		return "UPDATE"
	case StmtDelete:
		return "DELETE"
	case StmtDDL:
		return "DDL"
	default:
		return "OTHER"
	}
}

// ClassifyStatement parses the SQL and returns the statement type.
func ClassifyStatement(sql string) (StatementType, error) {
	result, err := pg_query.Parse(sql)
	if err != nil {
		return StmtOther, fmt.Errorf("parse SQL: %w", err)
	}

	if len(result.Stmts) == 0 {
		return StmtOther, nil
	}

	switch result.Stmts[0].Stmt.Node.(type) {
	case *pg_query.Node_SelectStmt:
		return StmtSelect, nil
	case *pg_query.Node_InsertStmt:
		return StmtInsert, nil
	case *pg_query.Node_UpdateStmt:
		return StmtUpdate, nil
	case *pg_query.Node_DeleteStmt:
		return StmtDelete, nil
	case *pg_query.Node_CreateStmt, *pg_query.Node_AlterTableStmt, *pg_query.Node_DropStmt,
		*pg_query.Node_IndexStmt, *pg_query.Node_ViewStmt, *pg_query.Node_CreateSchemaStmt,
		*pg_query.Node_TruncateStmt, *pg_query.Node_RenameStmt:
		return StmtDDL, nil
	default:
		return StmtOther, nil
	}
}

// ExtractTargetTable parses a SQL DML statement and returns the target table name
// for INSERT, UPDATE, and DELETE statements. Returns empty string for SELECT/DDL/other.
func ExtractTargetTable(sqlStr string) (string, error) {
	result, err := pg_query.Parse(sqlStr)
	if err != nil {
		return "", fmt.Errorf("parse SQL: %w", err)
	}

	if len(result.Stmts) == 0 {
		return "", nil
	}

	switch n := result.Stmts[0].Stmt.Node.(type) {
	case *pg_query.Node_InsertStmt:
		if n.InsertStmt.Relation != nil {
			return n.InsertStmt.Relation.Relname, nil
		}
	case *pg_query.Node_UpdateStmt:
		if n.UpdateStmt.Relation != nil {
			return n.UpdateStmt.Relation.Relname, nil
		}
	case *pg_query.Node_DeleteStmt:
		if n.DeleteStmt.Relation != nil {
			return n.DeleteStmt.Relation.Relname, nil
		}
	}

	return "", nil
}

// InjectRowFilterSQL injects a raw SQL WHERE clause expression into all SELECT
// statements that reference the given table. The filterSQL is a raw expression
// string like `"Pclass" = 1`.
func InjectRowFilterSQL(sqlStr string, tableName string, filterSQL string) (string, error) {
	if filterSQL == "" {
		return sqlStr, nil
	}

	// Parse the filter expression into an AST node
	filterResult, err := pg_query.Parse("SELECT 1 WHERE " + filterSQL)
	if err != nil {
		return "", fmt.Errorf("parse row filter %q: %w", filterSQL, err)
	}
	sel := filterResult.Stmts[0].Stmt.GetSelectStmt()
	if sel == nil || sel.WhereClause == nil {
		return "", fmt.Errorf("row filter %q did not produce a WHERE clause", filterSQL)
	}
	filterNode := sel.WhereClause

	// Parse the original query
	result, err := pg_query.Parse(sqlStr)
	if err != nil {
		return "", fmt.Errorf("parse SQL: %w", err)
	}

	for _, stmt := range result.Stmts {
		injectRawFilterIntoNode(stmt.Stmt, tableName, filterNode)
	}

	output, err := pg_query.Deparse(result)
	if err != nil {
		return "", fmt.Errorf("deparse SQL: %w", err)
	}
	return output, nil
}

// InjectMultipleRowFilters injects multiple row filter expressions into a SQL
// query for a given table. Multiple filters are combined with OR (each filter
// represents a separate visibility window), then ANDed with any existing WHERE.
func InjectMultipleRowFilters(sqlStr string, tableName string, filters []string) (string, error) {
	if len(filters) == 0 {
		return sqlStr, nil
	}
	if len(filters) == 1 {
		return InjectRowFilterSQL(sqlStr, tableName, filters[0])
	}

	// Combine filters with OR: (filter1) OR (filter2) OR ...
	parts := make([]string, len(filters))
	for i, f := range filters {
		parts[i] = "(" + f + ")"
	}
	combined := strings.Join(parts, " OR ")
	return InjectRowFilterSQL(sqlStr, tableName, combined)
}

// injectRawFilterIntoNode recurses into statement nodes to find SELECTs.
func injectRawFilterIntoNode(node *pg_query.Node, tableName string, filterNode *pg_query.Node) {
	if node == nil {
		return
	}
	if n, ok := node.Node.(*pg_query.Node_SelectStmt); ok {
		injectRawFilterIntoSelectStmt(n.SelectStmt, tableName, filterNode)
	}
}

func injectRawFilterIntoSelectStmt(sel *pg_query.SelectStmt, tableName string, filterNode *pg_query.Node) {
	if sel == nil {
		return
	}
	// Recurse into UNION/INTERSECT/EXCEPT
	if sel.Larg != nil {
		injectRawFilterIntoSelectStmt(sel.Larg, tableName, filterNode)
	}
	if sel.Rarg != nil {
		injectRawFilterIntoSelectStmt(sel.Rarg, tableName, filterNode)
	}

	// Check if this SELECT references the target table
	refs := collectTableRefs(sel.FromClause)
	found := false
	for _, ref := range refs {
		if ref.tableName == tableName {
			found = true
			break
		}
	}
	if !found {
		return
	}

	// Inject filter
	if sel.WhereClause == nil {
		sel.WhereClause = filterNode
	} else {
		sel.WhereClause = makeAndExpr(sel.WhereClause, filterNode)
	}
}

// ApplyColumnMasks rewrites SELECT target columns to apply mask expressions.
// masks is a map of column_name → mask_expression (e.g., {"Name": "'***'"}).
// Only top-level SELECT * or explicit column references are rewritten.
func ApplyColumnMasks(sqlStr string, tableName string, masks map[string]string) (string, error) {
	if len(masks) == 0 {
		return sqlStr, nil
	}

	result, err := pg_query.Parse(sqlStr)
	if err != nil {
		return "", fmt.Errorf("parse SQL: %w", err)
	}

	for _, stmt := range result.Stmts {
		if n, ok := stmt.Stmt.Node.(*pg_query.Node_SelectStmt); ok {
			applyMasksToSelectStmt(n.SelectStmt, tableName, masks)
		}
	}

	output, err := pg_query.Deparse(result)
	if err != nil {
		return "", fmt.Errorf("deparse SQL: %w", err)
	}
	return output, nil
}

// applyMasksToSelectStmt modifies the target list of a SELECT to replace
// masked columns with their mask expressions.
func applyMasksToSelectStmt(sel *pg_query.SelectStmt, tableName string, masks map[string]string) {
	if sel == nil {
		return
	}

	// Recurse into UNION
	if sel.Larg != nil {
		applyMasksToSelectStmt(sel.Larg, tableName, masks)
	}
	if sel.Rarg != nil {
		applyMasksToSelectStmt(sel.Rarg, tableName, masks)
	}

	// Check if this SELECT references the target table
	refs := collectTableRefs(sel.FromClause)
	found := false
	for _, ref := range refs {
		if ref.tableName == tableName {
			found = true
			break
		}
	}
	if !found {
		return
	}

	// Rewrite target list: replace column refs that match masked columns
	for i, target := range sel.TargetList {
		rt, ok := target.Node.(*pg_query.Node_ResTarget)
		if !ok {
			continue
		}

		colName := extractColumnName(rt.ResTarget.Val)
		if colName == "" {
			continue
		}

		maskExpr, shouldMask := masks[colName]
		if !shouldMask {
			continue
		}

		// Parse the mask expression and replace the target value
		maskResult, err := pg_query.Parse("SELECT " + maskExpr + " AS " + QuoteIdentifier(colName))
		if err != nil {
			continue // skip columns with unparseable mask expressions
		}
		maskSel := maskResult.Stmts[0].Stmt.GetSelectStmt()
		if maskSel != nil && len(maskSel.TargetList) > 0 {
			sel.TargetList[i] = maskSel.TargetList[0]
		}
	}
}

// extractColumnName gets the column name from a node, handling ColumnRef.
func extractColumnName(node *pg_query.Node) string {
	if node == nil {
		return ""
	}
	cr, ok := node.Node.(*pg_query.Node_ColumnRef)
	if !ok {
		return ""
	}
	// The last field in a column ref is the column name
	fields := cr.ColumnRef.Fields
	if len(fields) == 0 {
		return ""
	}
	last := fields[len(fields)-1]
	if s, ok := last.Node.(*pg_query.Node_String_); ok {
		return s.String_.Sval
	}
	return ""
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
func injectFiltersIntoNode(node *pg_query.Node, rulesByTable map[string][]RLSRule) error {
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
func injectFiltersIntoSelectStmt(sel *pg_query.SelectStmt, rulesByTable map[string][]RLSRule) error {
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
func injectFiltersIntoFromNode(node *pg_query.Node, rulesByTable map[string][]RLSRule) error {
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
func injectFiltersIntoExpr(node *pg_query.Node, rulesByTable map[string][]RLSRule) error {
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
func buildRuleExpr(rule RLSRule, tableAlias string) (*pg_query.Node, error) {
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
	case OpEqual:
		return "=", nil
	case OpNotEqual:
		return "<>", nil
	case OpLessThan:
		return "<", nil
	case OpLessEqual:
		return "<=", nil
	case OpGreaterThan:
		return ">", nil
	case OpGreaterEqual:
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
	// If value fits in int32, use Ival; otherwise use Fval (string representation)
	// to avoid silent overflow.
	if v >= -2147483648 && v <= 2147483647 {
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
	// Large values: represent as numeric string
	return makeFloatConst(fmt.Sprintf("%d", v))
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
