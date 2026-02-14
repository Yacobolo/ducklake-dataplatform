package sqlrewrite

import (
	pg_query "github.com/pganalyze/pg_query_go/v6"
)

// tableRef holds a table name and its alias (if any) for column qualification.
type tableRef struct {
	tableName string
	alias     string // empty if no alias; use tableName for column refs
}

// selectReferencesTable reports whether the given SELECT's FROM clause
// references the named table.
func selectReferencesTable(sel *pg_query.SelectStmt, tableName string) bool {
	for _, ref := range collectTableRefs(sel.FromClause) {
		if ref.tableName == tableName {
			return true
		}
	}
	return false
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

// injectRawFilterIntoNode recurses into statement nodes to find SELECTs,
// UPDATEs, and DELETEs, injecting WHERE clause filters for RLS.
func injectRawFilterIntoNode(node *pg_query.Node, tableName string, filterNode *pg_query.Node) {
	if node == nil {
		return
	}
	switch n := node.Node.(type) {
	case *pg_query.Node_SelectStmt:
		injectRawFilterIntoSelectStmt(n.SelectStmt, tableName, filterNode)
	case *pg_query.Node_UpdateStmt:
		injectRawFilterIntoUpdateStmt(n.UpdateStmt, tableName, filterNode)
	case *pg_query.Node_DeleteStmt:
		injectRawFilterIntoDeleteStmt(n.DeleteStmt, tableName, filterNode)
	}
}

// injectRawFilterIntoUpdateStmt injects a WHERE filter into an UPDATE statement
// when it targets the specified table.
func injectRawFilterIntoUpdateStmt(upd *pg_query.UpdateStmt, tableName string, filterNode *pg_query.Node) {
	if upd == nil || upd.Relation == nil {
		return
	}
	if upd.Relation.Relname != tableName {
		return
	}
	if upd.WhereClause == nil {
		upd.WhereClause = filterNode
	} else {
		upd.WhereClause = makeAndExpr(upd.WhereClause, filterNode)
	}
}

// injectRawFilterIntoDeleteStmt injects a WHERE filter into a DELETE statement
// when it targets the specified table.
func injectRawFilterIntoDeleteStmt(del *pg_query.DeleteStmt, tableName string, filterNode *pg_query.Node) {
	if del == nil || del.Relation == nil {
		return
	}
	if del.Relation.Relname != tableName {
		return
	}
	if del.WhereClause == nil {
		del.WhereClause = filterNode
	} else {
		del.WhereClause = makeAndExpr(del.WhereClause, filterNode)
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

	// Recurse into subqueries in FROM clause (RangeSubselect and JoinExpr nodes)
	for _, from := range sel.FromClause {
		injectRawFilterIntoFromNode(from, tableName, filterNode)
	}

	// Check if this SELECT references the target table
	if !selectReferencesTable(sel, tableName) {
		return
	}

	// Inject filter
	if sel.WhereClause == nil {
		sel.WhereClause = filterNode
	} else {
		sel.WhereClause = makeAndExpr(sel.WhereClause, filterNode)
	}
}

// injectRawFilterIntoFromNode recurses into subqueries within FROM clause nodes
// to apply raw RLS filters to nested SELECTs (e.g. SELECT * FROM (SELECT * FROM secret_table) sub).
func injectRawFilterIntoFromNode(node *pg_query.Node, tableName string, filterNode *pg_query.Node) {
	if node == nil {
		return
	}
	switch n := node.Node.(type) {
	case *pg_query.Node_RangeSubselect:
		if n.RangeSubselect.Subquery != nil {
			injectRawFilterIntoNode(n.RangeSubselect.Subquery, tableName, filterNode)
		}
	case *pg_query.Node_JoinExpr:
		injectRawFilterIntoFromNode(n.JoinExpr.Larg, tableName, filterNode)
		injectRawFilterIntoFromNode(n.JoinExpr.Rarg, tableName, filterNode)
	}
}

// injectFiltersIntoNode finds SELECT statements and injects WHERE conditions.
func injectFiltersIntoNode(node *pg_query.Node, rulesByTable map[string][]RLSRule) error {
	if node == nil {
		return nil
	}

	if n, ok := node.Node.(*pg_query.Node_SelectStmt); ok {
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
