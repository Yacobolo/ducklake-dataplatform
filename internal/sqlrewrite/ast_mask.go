package sqlrewrite

import (
	"fmt"
	"strings"

	pg_query "github.com/pganalyze/pg_query_go/v6"
)

// ApplyColumnMasks rewrites SELECT target columns to apply mask expressions.
// masks is a map of column_name -> mask_expression (e.g., {"Name": "'***'"}).
// allColumns is the full list of column names for the table, used to expand
// SELECT * into explicit column references so masks can be applied.
// Returns an error if any mask expression cannot be parsed (to prevent
// silent security degradation where a column is served unmasked).
func ApplyColumnMasks(sqlStr string, tableName string, masks map[string]string, allColumns []string) (string, error) {
	if len(masks) == 0 {
		return sqlStr, nil
	}

	// Normalize mask keys to lowercase for case-insensitive matching.
	normalized := make(map[string]string, len(masks))
	for k, v := range masks {
		normalized[strings.ToLower(k)] = v
	}
	masks = normalized

	result, err := pg_query.Parse(sqlStr)
	if err != nil {
		return "", fmt.Errorf("parse SQL: %w", err)
	}

	for _, stmt := range result.Stmts {
		if n, ok := stmt.Stmt.Node.(*pg_query.Node_SelectStmt); ok {
			if err := applyMasksToSelectStmt(n.SelectStmt, tableName, masks, allColumns); err != nil {
				return "", err
			}
		}
	}

	output, err := pg_query.Deparse(result)
	if err != nil {
		return "", fmt.Errorf("deparse SQL: %w", err)
	}
	return output, nil
}

// applyMasksToSelectStmt modifies the target list of a SELECT to replace
// masked columns with their mask expressions. Returns an error if a mask
// expression cannot be parsed (to prevent silent security degradation).
func applyMasksToSelectStmt(sel *pg_query.SelectStmt, tableName string, masks map[string]string, allColumns []string) error {
	if sel == nil {
		return nil
	}

	// Recurse into UNION
	if sel.Larg != nil {
		if err := applyMasksToSelectStmt(sel.Larg, tableName, masks, allColumns); err != nil {
			return err
		}
	}
	if sel.Rarg != nil {
		if err := applyMasksToSelectStmt(sel.Rarg, tableName, masks, allColumns); err != nil {
			return err
		}
	}

	// Recurse into CTEs
	if sel.WithClause != nil {
		for _, cte := range sel.WithClause.Ctes {
			if c, ok := cte.Node.(*pg_query.Node_CommonTableExpr); ok {
				if err := applyMasksToNode(c.CommonTableExpr.Ctequery, tableName, masks); err != nil {
					return err
				}
			}
		}
	}

	// Recurse into FROM subqueries
	for _, from := range sel.FromClause {
		if err := applyMasksToFromNode(from, tableName, masks); err != nil {
			return err
		}
	}

	// Check if this SELECT references the target table
	if !selectReferencesTable(sel, tableName) {
		return nil
	}

	// Expand SELECT * into explicit column references before applying masks.
	if err := expandStarTargets(sel, allColumns); err != nil {
		return err
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

		maskExpr, shouldMask := masks[strings.ToLower(colName)]
		if !shouldMask {
			continue
		}

		// Parse the mask expression and replace the target value.
		// We return an error rather than silently skipping, because a failed
		// mask parse means the column would be served unmasked — a security risk.
		maskResult, err := pg_query.Parse("SELECT " + maskExpr + " AS " + QuoteIdentifier(colName))
		if err != nil {
			return fmt.Errorf("parse column mask for %q: %w", colName, err)
		}
		maskSel := maskResult.Stmts[0].Stmt.GetSelectStmt()
		if maskSel != nil && len(maskSel.TargetList) > 0 {
			sel.TargetList[i] = maskSel.TargetList[0]
		}
	}
	return nil
}

// applyMasksToNode recurses into statement nodes to find SELECTs for masking.
func applyMasksToNode(node *pg_query.Node, tableName string, masks map[string]string) error {
	if node == nil {
		return nil
	}
	if n, ok := node.Node.(*pg_query.Node_SelectStmt); ok {
		return applyMasksToSelectStmt(n.SelectStmt, tableName, masks, nil)
	}
	return nil
}

// applyMasksToFromNode recurses into subqueries in FROM clause nodes for masking.
func applyMasksToFromNode(node *pg_query.Node, tableName string, masks map[string]string) error {
	if node == nil {
		return nil
	}
	switch n := node.Node.(type) {
	case *pg_query.Node_RangeSubselect:
		return applyMasksToNode(n.RangeSubselect.Subquery, tableName, masks)
	case *pg_query.Node_JoinExpr:
		if err := applyMasksToFromNode(n.JoinExpr.Larg, tableName, masks); err != nil {
			return err
		}
		return applyMasksToFromNode(n.JoinExpr.Rarg, tableName, masks)
	}
	return nil
}

// expandStarTargets replaces SELECT * (A_Star nodes) in the target list with
// explicit column references for all columns. This allows column masks to be
// applied correctly even when the user writes SELECT *.
func expandStarTargets(sel *pg_query.SelectStmt, allColumns []string) error {
	if len(allColumns) == 0 {
		// No column metadata available; cannot expand star.
		// Check if there's a star that needs expansion.
		for _, target := range sel.TargetList {
			if rt, ok := target.Node.(*pg_query.Node_ResTarget); ok {
				if rt.ResTarget.Val != nil {
					if cr, ok := rt.ResTarget.Val.Node.(*pg_query.Node_ColumnRef); ok {
						for _, f := range cr.ColumnRef.Fields {
							if _, isStar := f.Node.(*pg_query.Node_AStar); isStar {
								return fmt.Errorf("cannot apply column masks to SELECT * without column metadata")
							}
						}
					}
				}
			}
		}
		return nil
	}

	var expanded []*pg_query.Node
	needsExpansion := false
	for _, target := range sel.TargetList {
		isStar := false
		if rt, ok := target.Node.(*pg_query.Node_ResTarget); ok {
			if rt.ResTarget.Val != nil {
				if cr, ok := rt.ResTarget.Val.Node.(*pg_query.Node_ColumnRef); ok {
					for _, f := range cr.ColumnRef.Fields {
						if _, ok := f.Node.(*pg_query.Node_AStar); ok {
							isStar = true
							break
						}
					}
				}
			}
		}

		if isStar {
			needsExpansion = true
			for _, colName := range allColumns {
				expanded = append(expanded, &pg_query.Node{
					Node: &pg_query.Node_ResTarget{
						ResTarget: &pg_query.ResTarget{
							Val: makeColumnRef(colName, ""),
						},
					},
				})
			}
		} else {
			expanded = append(expanded, target)
		}
	}

	if needsExpansion {
		sel.TargetList = expanded
	}
	return nil
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
