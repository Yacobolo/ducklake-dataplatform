// ast_collect.go — AST walkers that collect table names from parse trees.
package sqlrewrite

import (
	pg_query "github.com/pganalyze/pg_query_go/v6"
)

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
