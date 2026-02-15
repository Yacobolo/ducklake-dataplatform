package duckdbsql

import (
	"fmt"
	"strings"
)

// === Statement Classification ===

// StmtType represents the kind of SQL statement.
type StmtType int

// StmtTypeSelect and friends classify statement types.
const (
	StmtTypeSelect StmtType = iota
	StmtTypeInsert
	StmtTypeUpdate
	StmtTypeDelete
	StmtTypeDDL
	StmtTypeOther
)

// Classify returns the statement type for a parsed statement.
func Classify(stmt Stmt) StmtType {
	switch stmt.(type) {
	case *SelectStmt:
		return StmtTypeSelect
	case *InsertStmt:
		return StmtTypeInsert
	case *UpdateStmt:
		return StmtTypeUpdate
	case *DeleteStmt:
		return StmtTypeDelete
	case *DDLStmt:
		return StmtTypeDDL
	case *UtilityStmt:
		return StmtTypeOther
	default:
		return StmtTypeOther
	}
}

// === Table Name Collection ===

// CollectTableNames returns a deduplicated list of table names referenced in
// the statement (FROM, JOIN, subqueries, CTEs, INSERT/UPDATE/DELETE targets).
func CollectTableNames(stmt Stmt) []string {
	seen := make(map[string]bool)
	var tables []string

	switch s := stmt.(type) {
	case *SelectStmt:
		collectTablesFromSelect(s, seen, &tables)
	case *InsertStmt:
		if s.Table != nil {
			addTable(s.Table.Name, seen, &tables)
		}
		if s.Query != nil {
			collectTablesFromSelect(s.Query, seen, &tables)
		}
	case *UpdateStmt:
		if s.Table != nil {
			addTable(s.Table.Name, seen, &tables)
		}
		if s.From != nil {
			collectTablesFromFrom(s.From, seen, &tables)
		}
	case *DeleteStmt:
		if s.Table != nil {
			addTable(s.Table.Name, seen, &tables)
		}
	}

	return tables
}

func collectTablesFromSelect(sel *SelectStmt, seen map[string]bool, tables *[]string) {
	if sel == nil {
		return
	}

	// WITH clause (CTEs)
	if sel.With != nil {
		for _, cte := range sel.With.CTEs {
			collectTablesFromSelect(cte.Select, seen, tables)
		}
	}

	if sel.Body != nil {
		collectTablesFromBody(sel.Body, seen, tables)
	}
}

func collectTablesFromBody(body *SelectBody, seen map[string]bool, tables *[]string) {
	if body == nil {
		return
	}
	if body.Left != nil {
		collectTablesFromCore(body.Left, seen, tables)
	}
	if body.Right != nil {
		collectTablesFromBody(body.Right, seen, tables)
	}
}

func collectTablesFromCore(sc *SelectCore, seen map[string]bool, tables *[]string) {
	if sc == nil {
		return
	}

	// FROM clause
	if sc.From != nil {
		collectTablesFromFrom(sc.From, seen, tables)
	}

	// WHERE clause subqueries
	collectTablesFromExpr(sc.Where, seen, tables)

	// HAVING clause subqueries
	collectTablesFromExpr(sc.Having, seen, tables)

	// SELECT list subqueries
	for _, col := range sc.Columns {
		collectTablesFromExpr(col.Expr, seen, tables)
	}

	// VALUES rows
	for _, row := range sc.ValuesRows {
		for _, expr := range row {
			collectTablesFromExpr(expr, seen, tables)
		}
	}
}

func collectTablesFromFrom(from *FromClause, seen map[string]bool, tables *[]string) {
	if from == nil {
		return
	}
	collectTablesFromTableRef(from.Source, seen, tables)
	for _, join := range from.Joins {
		collectTablesFromTableRef(join.Right, seen, tables)
	}
}

func collectTablesFromTableRef(ref TableRef, seen map[string]bool, tables *[]string) {
	if ref == nil {
		return
	}

	switch t := ref.(type) {
	case *TableName:
		addTable(t.Name, seen, tables)
	case *DerivedTable:
		collectTablesFromSelect(t.Select, seen, tables)
	case *LateralTable:
		collectTablesFromSelect(t.Select, seen, tables)
	case *FuncTable:
		// Table-valued functions in FROM clauses (e.g., read_csv_auto(), range()).
		// Add a "__func__<name>" sentinel entry so the engine does not treat
		// the query as a "table-less SELECT" and bypass RBAC. The engine skips
		// these sentinel entries during table-level privilege checks.
		if t.Func != nil && t.Func.Name != "" {
			addTable("__func__"+strings.ToLower(t.Func.Name), seen, tables)
		}
	case *PivotTable:
		collectTablesFromTableRef(t.Source, seen, tables)
	case *UnpivotTable:
		collectTablesFromTableRef(t.Source, seen, tables)
	case *StringTable:
		addTable(t.Path, seen, tables)
	}
}

func collectTablesFromExpr(e Expr, seen map[string]bool, tables *[]string) {
	if e == nil {
		return
	}

	switch expr := e.(type) {
	case *SubqueryExpr:
		collectTablesFromSelect(expr.Select, seen, tables)
	case *ExistsExpr:
		collectTablesFromSelect(expr.Select, seen, tables)
	case *InExpr:
		collectTablesFromExpr(expr.Expr, seen, tables)
		if expr.Query != nil {
			collectTablesFromSelect(expr.Query, seen, tables)
		}
		for _, v := range expr.Values {
			collectTablesFromExpr(v, seen, tables)
		}
	case *BinaryExpr:
		collectTablesFromExpr(expr.Left, seen, tables)
		collectTablesFromExpr(expr.Right, seen, tables)
	case *UnaryExpr:
		collectTablesFromExpr(expr.Expr, seen, tables)
	case *ParenExpr:
		collectTablesFromExpr(expr.Expr, seen, tables)
	case *FuncCall:
		for _, arg := range expr.Args {
			collectTablesFromExpr(arg, seen, tables)
		}
	case *CaseExpr:
		collectTablesFromExpr(expr.Operand, seen, tables)
		for _, w := range expr.Whens {
			collectTablesFromExpr(w.Condition, seen, tables)
			collectTablesFromExpr(w.Result, seen, tables)
		}
		collectTablesFromExpr(expr.Else, seen, tables)
	case *CastExpr:
		collectTablesFromExpr(expr.Expr, seen, tables)
	case *TypeCastExpr:
		collectTablesFromExpr(expr.Expr, seen, tables)
	case *BetweenExpr:
		collectTablesFromExpr(expr.Expr, seen, tables)
		collectTablesFromExpr(expr.Low, seen, tables)
		collectTablesFromExpr(expr.High, seen, tables)
	case *IsNullExpr:
		collectTablesFromExpr(expr.Expr, seen, tables)
	case *IsBoolExpr:
		collectTablesFromExpr(expr.Expr, seen, tables)
	case *LikeExpr:
		collectTablesFromExpr(expr.Expr, seen, tables)
		collectTablesFromExpr(expr.Pattern, seen, tables)
	case *IsDistinctExpr:
		collectTablesFromExpr(expr.Left, seen, tables)
		collectTablesFromExpr(expr.Right, seen, tables)
	case *CollateExpr:
		collectTablesFromExpr(expr.Expr, seen, tables)
	case *MapLiteral:
		for _, e := range expr.Entries {
			collectTablesFromExpr(e.Value, seen, tables)
		}
	case *ListComprehension:
		collectTablesFromExpr(expr.Expr, seen, tables)
		collectTablesFromExpr(expr.List, seen, tables)
		collectTablesFromExpr(expr.Cond, seen, tables)
	case *NamedArgExpr:
		collectTablesFromExpr(expr.Value, seen, tables)
	case *GroupingExpr:
		for _, group := range expr.Groups {
			for _, e := range group {
				collectTablesFromExpr(e, seen, tables)
			}
		}
	case *ParamExpr, *DefaultExpr:
		// Leaf nodes, no sub-expressions
	}
}

func addTable(name string, seen map[string]bool, tables *[]string) {
	if name == "" || seen[name] {
		return
	}
	seen[name] = true
	*tables = append(*tables, name)
}

// === Target Table Extraction ===

// TargetTable returns the target table name for INSERT, UPDATE, or DELETE.
// Returns empty string for SELECT, DDL, and other statement types.
func TargetTable(stmt Stmt) string {
	switch s := stmt.(type) {
	case *InsertStmt:
		if s.Table != nil {
			return s.Table.Name
		}
	case *UpdateStmt:
		if s.Table != nil {
			return s.Table.Name
		}
	case *DeleteStmt:
		if s.Table != nil {
			return s.Table.Name
		}
	}
	return ""
}

// === Filter Injection (RLS) ===

// InjectFilter injects a WHERE clause filter into all SELECT/UPDATE/DELETE
// nodes that reference the given table. The filter is ANDed with any
// existing WHERE clause.
func InjectFilter(stmt Stmt, tableName string, filter Expr) {
	switch s := stmt.(type) {
	case *SelectStmt:
		injectFilterIntoSelect(s, tableName, filter)
	case *UpdateStmt:
		injectFilterIntoUpdate(s, tableName, filter)
	case *DeleteStmt:
		injectFilterIntoDelete(s, tableName, filter)
	}
}

func injectFilterIntoSelect(sel *SelectStmt, tableName string, filter Expr) {
	if sel == nil {
		return
	}

	// Recurse into CTEs
	if sel.With != nil {
		for _, cte := range sel.With.CTEs {
			injectFilterIntoSelect(cte.Select, tableName, filter)
		}
	}

	if sel.Body != nil {
		injectFilterIntoBody(sel.Body, tableName, filter)
	}
}

func injectFilterIntoBody(body *SelectBody, tableName string, filter Expr) {
	if body == nil {
		return
	}

	if body.Left != nil {
		injectFilterIntoCore(body.Left, tableName, filter)
	}
	if body.Right != nil {
		injectFilterIntoBody(body.Right, tableName, filter)
	}
}

func injectFilterIntoCore(sc *SelectCore, tableName string, filter Expr) {
	if sc == nil {
		return
	}

	// Recurse into FROM subqueries
	if sc.From != nil {
		injectFilterIntoFrom(sc.From, tableName, filter)
	}

	// Check if this SELECT references the target table
	if sc.From != nil && fromReferencesTable(sc.From, tableName) {
		sc.Where = andExpr(sc.Where, filter)
	}
}

func injectFilterIntoFrom(from *FromClause, tableName string, filter Expr) {
	if from == nil {
		return
	}
	injectFilterIntoTableRef(from.Source, tableName, filter)
	for _, join := range from.Joins {
		injectFilterIntoTableRef(join.Right, tableName, filter)
	}
}

func injectFilterIntoTableRef(ref TableRef, tableName string, filter Expr) {
	if ref == nil {
		return
	}

	switch t := ref.(type) {
	case *DerivedTable:
		injectFilterIntoSelect(t.Select, tableName, filter)
	case *LateralTable:
		injectFilterIntoSelect(t.Select, tableName, filter)
	case *PivotTable:
		injectFilterIntoTableRef(t.Source, tableName, filter)
	case *UnpivotTable:
		injectFilterIntoTableRef(t.Source, tableName, filter)
	case *StringTable:
		// No subqueries to recurse into
	}
}

func injectFilterIntoUpdate(upd *UpdateStmt, tableName string, filter Expr) {
	if upd == nil || upd.Table == nil {
		return
	}
	if upd.Table.Name != tableName {
		return
	}
	upd.Where = andExpr(upd.Where, filter)
}

func injectFilterIntoDelete(del *DeleteStmt, tableName string, filter Expr) {
	if del == nil || del.Table == nil {
		return
	}
	if del.Table.Name != tableName {
		return
	}
	del.Where = andExpr(del.Where, filter)
}

// fromReferencesTable checks if a FROM clause directly references the given table.
func fromReferencesTable(from *FromClause, tableName string) bool {
	if from == nil {
		return false
	}
	if tableRefReferencesTable(from.Source, tableName) {
		return true
	}
	for _, join := range from.Joins {
		if tableRefReferencesTable(join.Right, tableName) {
			return true
		}
	}
	return false
}

func tableRefReferencesTable(ref TableRef, tableName string) bool {
	if ref == nil {
		return false
	}
	switch t := ref.(type) {
	case *TableName:
		return t.Name == tableName
	case *PivotTable:
		return tableRefReferencesTable(t.Source, tableName)
	case *UnpivotTable:
		return tableRefReferencesTable(t.Source, tableName)
	case *StringTable:
		return t.Path == tableName
	}
	return false
}

// andExpr combines two expressions with AND. If existing is nil, returns the filter.
func andExpr(existing, filter Expr) Expr {
	if existing == nil {
		return filter
	}
	return &BinaryExpr{
		Left:  existing,
		Op:    TOKEN_AND,
		Right: filter,
	}
}

// === Column Masking ===

// ApplyColumnMasks rewrites SELECT columns to apply mask expressions for the
// given table. masks maps column_name -> mask_expression_sql. allColumns is
// used to expand SELECT * into explicit column references.
// Mask keys and column lookups are case-insensitive.
// Returns an error if a mask expression cannot be parsed.
func ApplyColumnMasks(stmt Stmt, tableName string, masks map[string]string, allColumns []string) error {
	if len(masks) == 0 {
		return nil
	}

	// Normalize mask keys to lowercase
	normalized := make(map[string]string, len(masks))
	for k, v := range masks {
		normalized[strings.ToLower(k)] = v
	}

	if s, ok := stmt.(*SelectStmt); ok {
		return applyMasksToSelect(s, tableName, normalized, allColumns)
	}
	return nil
}

func applyMasksToSelect(sel *SelectStmt, tableName string, masks map[string]string, allColumns []string) error {
	if sel == nil {
		return nil
	}

	// Recurse into CTEs
	if sel.With != nil {
		for _, cte := range sel.With.CTEs {
			if err := applyMasksToSelect(cte.Select, tableName, masks, nil); err != nil {
				return err
			}
		}
	}

	if sel.Body != nil {
		return applyMasksToBody(sel.Body, tableName, masks, allColumns)
	}
	return nil
}

func applyMasksToBody(body *SelectBody, tableName string, masks map[string]string, allColumns []string) error {
	if body == nil {
		return nil
	}

	if body.Left != nil {
		if err := applyMasksToCore(body.Left, tableName, masks, allColumns); err != nil {
			return err
		}
	}
	if body.Right != nil {
		if err := applyMasksToBody(body.Right, tableName, masks, allColumns); err != nil {
			return err
		}
	}
	return nil
}

func applyMasksToCore(sc *SelectCore, tableName string, masks map[string]string, allColumns []string) error {
	if sc == nil {
		return nil
	}

	// Recurse into FROM subqueries
	if sc.From != nil {
		if err := applyMasksToFrom(sc.From, tableName, masks); err != nil {
			return err
		}
	}

	// Check if this SELECT references the target table
	if sc.From == nil || !fromReferencesTable(sc.From, tableName) {
		return nil
	}

	// Expand SELECT * into explicit columns
	if err := expandStarColumns(sc, allColumns); err != nil {
		return err
	}

	// Rewrite column references that match masked columns
	for i, item := range sc.Columns {
		colName := extractColumnNameFromExpr(item.Expr)
		if colName == "" {
			continue
		}

		maskExpr, shouldMask := masks[strings.ToLower(colName)]
		if !shouldMask {
			continue
		}

		// Parse the mask expression
		maskParsed, err := ParseExpr(maskExpr)
		if err != nil {
			return fmt.Errorf("parse column mask for %q: %w", colName, err)
		}

		sc.Columns[i] = SelectItem{
			Expr:  maskParsed,
			Alias: colName,
		}
	}
	return nil
}

func applyMasksToFrom(from *FromClause, tableName string, masks map[string]string) error {
	if from == nil {
		return nil
	}
	if err := applyMasksToTableRef(from.Source, tableName, masks); err != nil {
		return err
	}
	for _, join := range from.Joins {
		if err := applyMasksToTableRef(join.Right, tableName, masks); err != nil {
			return err
		}
	}
	return nil
}

func applyMasksToTableRef(ref TableRef, tableName string, masks map[string]string) error {
	if ref == nil {
		return nil
	}

	switch t := ref.(type) {
	case *DerivedTable:
		return applyMasksToSelect(t.Select, tableName, masks, nil)
	case *LateralTable:
		return applyMasksToSelect(t.Select, tableName, masks, nil)
	case *StringTable:
		// No subqueries
	}
	return nil
}

// expandStarColumns replaces SELECT * items with explicit column references.
func expandStarColumns(sc *SelectCore, allColumns []string) error {
	if len(allColumns) == 0 {
		// Check if there is a * that needs expansion
		for _, item := range sc.Columns {
			if item.Star {
				return fmt.Errorf("cannot apply column masks to SELECT * without column metadata")
			}
		}
		return nil
	}

	var expanded []SelectItem
	needsExpansion := false

	for _, item := range sc.Columns {
		if item.Star {
			needsExpansion = true
			for _, colName := range allColumns {
				expanded = append(expanded, SelectItem{
					Expr: &ColumnRef{Column: colName},
				})
			}
		} else {
			expanded = append(expanded, item)
		}
	}

	if needsExpansion {
		sc.Columns = expanded
	}
	return nil
}

// extractColumnNameFromExpr extracts the column name from a ColumnRef expression.
func extractColumnNameFromExpr(e Expr) string {
	if e == nil {
		return ""
	}
	if expr, ok := e.(*ColumnRef); ok {
		return expr.Column
	}
	return ""
}

// === Dangerous Function Detection ===

// ContainsDangerousFunction walks the AST looking for function calls whose
// names appear in the blocklist. Returns the function name and true if found.
func ContainsDangerousFunction(stmt Stmt, blocklist map[string]bool) (string, bool) {
	switch s := stmt.(type) {
	case *SelectStmt:
		return dangerousFuncInSelect(s, blocklist)
	case *InsertStmt:
		if s.Query != nil {
			return dangerousFuncInSelect(s.Query, blocklist)
		}
	}
	return "", false
}

func dangerousFuncInSelect(sel *SelectStmt, blocklist map[string]bool) (string, bool) {
	if sel == nil {
		return "", false
	}
	// CTEs
	if sel.With != nil {
		for _, cte := range sel.With.CTEs {
			if name, found := dangerousFuncInSelect(cte.Select, blocklist); found {
				return name, true
			}
		}
	}
	if sel.Body != nil {
		return dangerousFuncInBody(sel.Body, blocklist)
	}
	return "", false
}

func dangerousFuncInBody(body *SelectBody, blocklist map[string]bool) (string, bool) {
	if body == nil {
		return "", false
	}
	if body.Left != nil {
		if name, found := dangerousFuncInCore(body.Left, blocklist); found {
			return name, true
		}
	}
	if body.Right != nil {
		return dangerousFuncInBody(body.Right, blocklist)
	}
	return "", false
}

func dangerousFuncInCore(sc *SelectCore, blocklist map[string]bool) (string, bool) {
	if sc == nil {
		return "", false
	}
	// SELECT list
	for _, col := range sc.Columns {
		if name, found := dangerousFuncInExpr(col.Expr, blocklist); found {
			return name, true
		}
	}
	// FROM clause (table-valued functions)
	if sc.From != nil {
		if name, found := dangerousFuncInFrom(sc.From, blocklist); found {
			return name, true
		}
	}
	// WHERE
	if name, found := dangerousFuncInExpr(sc.Where, blocklist); found {
		return name, true
	}
	// HAVING
	if name, found := dangerousFuncInExpr(sc.Having, blocklist); found {
		return name, true
	}
	return "", false
}

func dangerousFuncInFrom(from *FromClause, blocklist map[string]bool) (string, bool) {
	if from == nil {
		return "", false
	}
	if name, found := dangerousFuncInTableRef(from.Source, blocklist); found {
		return name, true
	}
	for _, join := range from.Joins {
		if name, found := dangerousFuncInTableRef(join.Right, blocklist); found {
			return name, true
		}
	}
	return "", false
}

func dangerousFuncInTableRef(ref TableRef, blocklist map[string]bool) (string, bool) {
	if ref == nil {
		return "", false
	}
	switch t := ref.(type) {
	case *FuncTable:
		if t.Func != nil && blocklist[strings.ToLower(t.Func.Name)] {
			return t.Func.Name, true
		}
		// Check function arguments
		if t.Func != nil {
			for _, arg := range t.Func.Args {
				if name, found := dangerousFuncInExpr(arg, blocklist); found {
					return name, true
				}
			}
		}
	case *DerivedTable:
		return dangerousFuncInSelect(t.Select, blocklist)
	case *LateralTable:
		return dangerousFuncInSelect(t.Select, blocklist)
	case *PivotTable:
		return dangerousFuncInTableRef(t.Source, blocklist)
	case *UnpivotTable:
		return dangerousFuncInTableRef(t.Source, blocklist)
	case *StringTable:
		// No functions to check
	}
	return "", false
}

func dangerousFuncInExpr(e Expr, blocklist map[string]bool) (string, bool) {
	if e == nil {
		return "", false
	}
	switch expr := e.(type) {
	case *FuncCall:
		if blocklist[strings.ToLower(expr.Name)] {
			return expr.Name, true
		}
		for _, arg := range expr.Args {
			if name, found := dangerousFuncInExpr(arg, blocklist); found {
				return name, true
			}
		}
	case *SubqueryExpr:
		return dangerousFuncInSelect(expr.Select, blocklist)
	case *ExistsExpr:
		return dangerousFuncInSelect(expr.Select, blocklist)
	case *InExpr:
		if name, found := dangerousFuncInExpr(expr.Expr, blocklist); found {
			return name, true
		}
		if expr.Query != nil {
			return dangerousFuncInSelect(expr.Query, blocklist)
		}
		for _, v := range expr.Values {
			if name, found := dangerousFuncInExpr(v, blocklist); found {
				return name, true
			}
		}
	case *BinaryExpr:
		if name, found := dangerousFuncInExpr(expr.Left, blocklist); found {
			return name, true
		}
		return dangerousFuncInExpr(expr.Right, blocklist)
	case *UnaryExpr:
		return dangerousFuncInExpr(expr.Expr, blocklist)
	case *ParenExpr:
		return dangerousFuncInExpr(expr.Expr, blocklist)
	case *CaseExpr:
		if name, found := dangerousFuncInExpr(expr.Operand, blocklist); found {
			return name, true
		}
		for _, w := range expr.Whens {
			if name, found := dangerousFuncInExpr(w.Condition, blocklist); found {
				return name, true
			}
			if name, found := dangerousFuncInExpr(w.Result, blocklist); found {
				return name, true
			}
		}
		return dangerousFuncInExpr(expr.Else, blocklist)
	case *CastExpr:
		return dangerousFuncInExpr(expr.Expr, blocklist)
	case *TypeCastExpr:
		return dangerousFuncInExpr(expr.Expr, blocklist)
	case *BetweenExpr:
		if name, found := dangerousFuncInExpr(expr.Expr, blocklist); found {
			return name, true
		}
		if name, found := dangerousFuncInExpr(expr.Low, blocklist); found {
			return name, true
		}
		return dangerousFuncInExpr(expr.High, blocklist)
	case *IsNullExpr:
		return dangerousFuncInExpr(expr.Expr, blocklist)
	case *IsBoolExpr:
		return dangerousFuncInExpr(expr.Expr, blocklist)
	case *LikeExpr:
		if name, found := dangerousFuncInExpr(expr.Expr, blocklist); found {
			return name, true
		}
		return dangerousFuncInExpr(expr.Pattern, blocklist)
	case *IsDistinctExpr:
		if name, found := dangerousFuncInExpr(expr.Left, blocklist); found {
			return name, true
		}
		return dangerousFuncInExpr(expr.Right, blocklist)
	case *CollateExpr:
		return dangerousFuncInExpr(expr.Expr, blocklist)
	case *MapLiteral:
		for _, e := range expr.Entries {
			if name, found := dangerousFuncInExpr(e.Value, blocklist); found {
				return name, true
			}
		}
	case *ListComprehension:
		if name, found := dangerousFuncInExpr(expr.Expr, blocklist); found {
			return name, true
		}
		if name, found := dangerousFuncInExpr(expr.List, blocklist); found {
			return name, true
		}
		return dangerousFuncInExpr(expr.Cond, blocklist)
	case *NamedArgExpr:
		return dangerousFuncInExpr(expr.Value, blocklist)
	case *GroupingExpr:
		for _, group := range expr.Groups {
			for _, e := range group {
				if name, found := dangerousFuncInExpr(e, blocklist); found {
					return name, true
				}
			}
		}
	case *ParamExpr, *DefaultExpr:
		// Leaf nodes
	}
	return "", false
}
