package duckdbsql

import (
	"strings"
)

// === Column Lineage Analysis ===

// ColumnOrigin traces a column back to a physical table.
type ColumnOrigin struct {
	Schema string
	Table  string
	Column string
}

// ColumnLineage describes one output column and its source(s).
type ColumnLineage struct {
	Name          string
	Index         int
	TransformType string // "DIRECT" or "EXPRESSION"
	Function      string
	Sources       []ColumnOrigin
}

// SchemaInfo provides column lists for base tables (for star expansion and
// unqualified column resolution). Keys can be "table" or "schema.table".
type SchemaInfo map[string][]string

// sourceKind distinguishes different FROM-clause source types.
type sourceKind int

const (
	sourceBaseTable sourceKind = iota
	sourceDerivedTable
	sourceCTE
	sourceFuncTable
)

// scopeCol represents a column provided by a source within a scope.
type scopeCol struct {
	name          string
	origins       []ColumnOrigin
	transformType string // "DIRECT" or "EXPRESSION" — propagated from inner query
	function      string // function name if EXPRESSION
}

// sourceDef represents one table-like source in a FROM clause.
type sourceDef struct {
	alias   string     // the name used to qualify columns
	table   string     // physical table name (empty for derived/CTE)
	schema  string     // physical table schema
	columns []scopeCol // nil = unresolved
	kind    sourceKind
}

// cteDef holds a CTE's analyzed output schema.
type cteDef struct {
	name    string
	columns []scopeCol
}

// scope represents the column namespace visible at a given point in the query.
type scope struct {
	sources []sourceDef
	ctes    map[string]*cteDef
	parent  *scope
}

// findSource returns the source matching the given alias.
func (s *scope) findSource(alias string) (*sourceDef, bool) {
	lower := strings.ToLower(alias)
	for i := range s.sources {
		if strings.ToLower(s.sources[i].alias) == lower {
			return &s.sources[i], true
		}
	}
	return nil, false
}

// findColumn searches for an unqualified column across all sources in scope.
// Returns the scopeCol and true if found unambiguously.
func (s *scope) findColumn(name string) (*scopeCol, bool) {
	lower := strings.ToLower(name)
	var found *scopeCol
	count := 0
	for _, src := range s.sources {
		if src.columns == nil {
			continue
		}
		for i := range src.columns {
			if strings.ToLower(src.columns[i].name) == lower {
				found = &src.columns[i]
				count++
				break
			}
		}
	}
	if count == 1 {
		return found, true
	}
	// Ambiguous or not found — try parent scope for correlated subqueries
	if count == 0 && s.parent != nil {
		return s.parent.findColumn(name)
	}
	return nil, false
}

// findColumnInSource searches for a column within a specific source.
func findColumnInSource(src *sourceDef, name string) (*scopeCol, bool) {
	if src.columns == nil {
		return nil, false
	}
	lower := strings.ToLower(name)
	for i := range src.columns {
		if strings.ToLower(src.columns[i].name) == lower {
			return &src.columns[i], true
		}
	}
	return nil, false
}

// AnalyzeColumnLineage performs pure AST-level column lineage analysis on
// a parsed statement. It traces each output column back to its source
// table(s) and column(s). The schema parameter provides column lists for
// base tables, enabling star expansion and unqualified column resolution.
// Only SELECT statements are analyzed; other types return nil.
func AnalyzeColumnLineage(stmt Stmt, schema SchemaInfo) ([]ColumnLineage, error) {
	sel, ok := stmt.(*SelectStmt)
	if !ok || sel == nil {
		return nil, nil
	}
	if schema == nil {
		schema = SchemaInfo{}
	}
	return analyzeSelectStmt(sel, schema, nil)
}

func analyzeSelectStmt(sel *SelectStmt, schema SchemaInfo, parentScope *scope) ([]ColumnLineage, error) {
	if sel == nil {
		return nil, nil
	}

	// Phase 1: analyze CTEs in order — each CTE can see prior CTEs
	ctes := make(map[string]*cteDef)
	if sel.With != nil {
		for _, cte := range sel.With.CTEs {
			// Build a temporary parent scope that includes prior CTEs
			// so each CTE can reference earlier ones
			cteParent := &scope{
				ctes:   copyCTEs(ctes),
				parent: parentScope,
			}
			cols, err := analyzeSelectStmt(cte.Select, schema, cteParent)
			if err != nil {
				return nil, err
			}
			cd := &cteDef{name: cte.Name}
			if len(cte.Columns) > 0 && len(cols) > 0 {
				for i, col := range cols {
					name := col.Name
					if i < len(cte.Columns) {
						name = cte.Columns[i]
					}
					cd.columns = append(cd.columns, scopeCol{
						name:          name,
						origins:       col.Sources,
						transformType: col.TransformType,
						function:      col.Function,
					})
				}
			} else {
				for _, col := range cols {
					cd.columns = append(cd.columns, scopeCol{
						name:          col.Name,
						origins:       col.Sources,
						transformType: col.TransformType,
						function:      col.Function,
					})
				}
			}
			ctes[strings.ToLower(cte.Name)] = cd
		}
	}

	// Phase 2: analyze body
	if sel.Body == nil {
		return nil, nil
	}
	return analyzeBody(sel.Body, schema, ctes, parentScope)
}

func copyCTEs(ctes map[string]*cteDef) map[string]*cteDef {
	if len(ctes) == 0 {
		return nil
	}
	cp := make(map[string]*cteDef, len(ctes))
	for k, v := range ctes {
		cp[k] = v
	}
	return cp
}

// findCTE searches for a CTE definition by name, first in the given map,
// then walking up parent scopes. This allows chained CTEs where a later CTE
// references an earlier one visible through parent scope.
func findCTE(ctes map[string]*cteDef, parentScope *scope, name string) *cteDef {
	lower := strings.ToLower(name)
	if cd, ok := ctes[lower]; ok {
		return cd
	}
	// Walk parent scopes
	for s := parentScope; s != nil; s = s.parent {
		if s.ctes != nil {
			if cd, ok := s.ctes[lower]; ok {
				return cd
			}
		}
	}
	return nil
}

func analyzeBody(body *SelectBody, schema SchemaInfo, ctes map[string]*cteDef, parentScope *scope) ([]ColumnLineage, error) {
	if body == nil {
		return nil, nil
	}

	leftCols, err := analyzeCore(body.Left, schema, ctes, parentScope)
	if err != nil {
		return nil, err
	}

	if body.Op == SetOpNone || body.Right == nil {
		return leftCols, nil
	}

	// Set operations: analyze both branches, merge
	rightCols, err := analyzeBody(body.Right, schema, ctes, parentScope)
	if err != nil {
		return nil, err
	}

	return mergeSetOp(leftCols, rightCols, body.Op), nil
}

func mergeSetOp(left, right []ColumnLineage, op SetOpType) []ColumnLineage {
	n := len(left)
	if len(right) < n {
		n = len(right)
	}
	if n == 0 {
		if len(left) > 0 {
			return left
		}
		return right
	}

	opName := strings.TrimSuffix(string(op), " ALL")

	merged := make([]ColumnLineage, n)
	for i := 0; i < n; i++ {
		merged[i] = ColumnLineage{
			Name:          left[i].Name, // output names come from left branch
			Index:         i,
			TransformType: "EXPRESSION",
			Function:      opName,
			Sources:       dedupOrigins(append(left[i].Sources, right[i].Sources...)),
		}
	}
	return merged
}

func analyzeCore(sc *SelectCore, schema SchemaInfo, ctes map[string]*cteDef, parentScope *scope) ([]ColumnLineage, error) {
	if sc == nil {
		return nil, nil
	}

	// Handle VALUES
	if len(sc.ValuesRows) > 0 {
		return nil, nil
	}

	// Build scope from FROM clause
	s := buildScope(sc.From, schema, ctes, parentScope)

	// Trace each SELECT item
	var result []ColumnLineage
	for i, item := range sc.Columns {
		cols, err := traceSelectItem(item, s, schema, ctes, i)
		if err != nil {
			return nil, err
		}
		result = append(result, cols...)
	}

	return result, nil
}

func buildScope(from *FromClause, schema SchemaInfo, ctes map[string]*cteDef, parentScope *scope) *scope {
	s := &scope{
		ctes:   ctes,
		parent: parentScope,
	}

	if from == nil {
		return s
	}

	src := resolveTableRef(from.Source, schema, ctes, parentScope)
	if src != nil {
		s.sources = append(s.sources, *src)
	}

	for _, join := range from.Joins {
		src := resolveTableRef(join.Right, schema, ctes, parentScope)
		if src != nil {
			s.sources = append(s.sources, *src)
		}
	}

	return s
}

func resolveTableRef(ref TableRef, schema SchemaInfo, ctes map[string]*cteDef, parentScope *scope) *sourceDef {
	if ref == nil {
		return nil
	}

	switch t := ref.(type) {
	case *TableName:
		alias := t.Alias
		if alias == "" {
			alias = t.Name
		}

		// Check if this is a CTE reference — search current ctes map and parent scopes
		if cd := findCTE(ctes, parentScope, t.Name); cd != nil {
			src := &sourceDef{
				alias:   alias,
				kind:    sourceCTE,
				columns: cd.columns,
			}
			if len(t.ColumnAliases) > 0 {
				src.columns = applyColumnAliases(src.columns, t.ColumnAliases)
			}
			return src
		}

		// Base table — look up in schema info
		src := &sourceDef{
			alias:  alias,
			table:  t.Name,
			schema: t.Schema,
			kind:   sourceBaseTable,
		}

		for _, colName := range lookupSchemaColumns(schema, t.Schema, t.Name) {
			origin := ColumnOrigin{
				Schema: t.Schema,
				Table:  t.Name,
				Column: colName,
			}
			src.columns = append(src.columns, scopeCol{
				name:    colName,
				origins: []ColumnOrigin{origin},
			})
		}
		if len(t.ColumnAliases) > 0 {
			src.columns = applyColumnAliases(src.columns, t.ColumnAliases)
		}
		return src

	case *DerivedTable:
		alias := t.Alias
		if alias == "" {
			alias = "subquery"
		}
		cols, _ := analyzeSelectStmt(t.Select, schema, parentScope)
		src := &sourceDef{
			alias: alias,
			kind:  sourceDerivedTable,
		}
		for _, col := range cols {
			src.columns = append(src.columns, scopeCol{
				name:          col.Name,
				origins:       col.Sources,
				transformType: col.TransformType,
				function:      col.Function,
			})
		}
		if len(t.ColumnAliases) > 0 {
			src.columns = applyColumnAliases(src.columns, t.ColumnAliases)
		}
		return src

	case *LateralTable:
		alias := t.Alias
		if alias == "" {
			alias = "lateral"
		}
		cols, _ := analyzeSelectStmt(t.Select, schema, parentScope)
		src := &sourceDef{
			alias: alias,
			kind:  sourceDerivedTable,
		}
		for _, col := range cols {
			src.columns = append(src.columns, scopeCol{
				name:          col.Name,
				origins:       col.Sources,
				transformType: col.TransformType,
				function:      col.Function,
			})
		}
		if len(t.ColumnAliases) > 0 {
			src.columns = applyColumnAliases(src.columns, t.ColumnAliases)
		}
		return src

	case *FuncTable:
		alias := t.Alias
		if alias == "" && t.Func != nil {
			alias = t.Func.Name
		}
		src := &sourceDef{
			alias: alias,
			kind:  sourceFuncTable,
		}
		if len(t.ColumnAliases) > 0 {
			for _, ca := range t.ColumnAliases {
				src.columns = append(src.columns, scopeCol{name: ca})
			}
		}
		return src

	case *PivotTable:
		alias := t.Alias
		if alias == "" {
			alias = "pivot"
		}
		return &sourceDef{alias: alias, kind: sourceFuncTable}

	case *UnpivotTable:
		alias := t.Alias
		if alias == "" {
			alias = "unpivot"
		}
		return &sourceDef{alias: alias, kind: sourceFuncTable}

	case *StringTable:
		alias := t.Alias
		if alias == "" {
			alias = t.Path
		}
		return &sourceDef{alias: alias, kind: sourceBaseTable}
	}

	return nil
}

func lookupSchemaColumns(schema SchemaInfo, schemaName, tableName string) []string {
	// Try schema.table first, then just table
	if schemaName != "" {
		key := strings.ToLower(schemaName) + "." + strings.ToLower(tableName)
		if cols, ok := schema[key]; ok {
			return cols
		}
	}
	if cols, ok := schema[strings.ToLower(tableName)]; ok {
		return cols
	}
	return nil
}

func applyColumnAliases(cols []scopeCol, aliases []string) []scopeCol {
	result := make([]scopeCol, len(cols))
	copy(result, cols)
	for i := range result {
		if i < len(aliases) {
			result[i].name = aliases[i]
		}
	}
	return result
}

// === Select Item Tracing ===

func traceSelectItem(item SelectItem, s *scope, schema SchemaInfo, ctes map[string]*cteDef, idx int) ([]ColumnLineage, error) {
	// Case 1: SELECT *
	if item.Star {
		return expandStar(s, item.Modifiers, schema, idx)
	}

	// Case 2: SELECT t.*
	if item.TableStar != "" {
		return expandTableStar(item.TableStar, s, item.Modifiers, schema, idx)
	}

	// Case 3: expression
	sources, transformType, funcName := traceExpr(item.Expr, s, schema, ctes)
	name := inferColumnName(item)
	return []ColumnLineage{{
		Name:          name,
		Index:         idx,
		TransformType: transformType,
		Function:      funcName,
		Sources:       dedupOrigins(sources),
	}}, nil
}

func expandStar(s *scope, modifiers []StarModifier, schema SchemaInfo, startIdx int) ([]ColumnLineage, error) {
	var result []ColumnLineage
	idx := startIdx

	excludeSet := buildExcludeSet(modifiers)
	replaceMap := buildReplaceMap(modifiers)
	renameMap := buildRenameMap(modifiers)

	for _, src := range s.sources {
		if src.columns == nil {
			continue
		}
		for _, col := range src.columns {
			if excludeSet[strings.ToLower(col.name)] {
				continue
			}

			name := col.name
			if newName, ok := renameMap[strings.ToLower(col.name)]; ok {
				name = newName
			}

			if replExpr, ok := replaceMap[strings.ToLower(col.name)]; ok {
				replSources, replTransform, replFunc := traceExpr(replExpr, s, schema, nil)
				result = append(result, ColumnLineage{
					Name:          name,
					Index:         idx,
					TransformType: replTransform,
					Function:      replFunc,
					Sources:       dedupOrigins(replSources),
				})
			} else {
				result = append(result, ColumnLineage{
					Name:          name,
					Index:         idx,
					TransformType: "DIRECT",
					Sources:       col.origins,
				})
			}
			idx++
		}
	}
	return result, nil
}

func expandTableStar(tableName string, s *scope, modifiers []StarModifier, schema SchemaInfo, startIdx int) ([]ColumnLineage, error) {
	src, ok := s.findSource(tableName)
	if !ok || src.columns == nil {
		return nil, nil
	}

	var result []ColumnLineage
	idx := startIdx

	excludeSet := buildExcludeSet(modifiers)
	replaceMap := buildReplaceMap(modifiers)
	renameMap := buildRenameMap(modifiers)

	for _, col := range src.columns {
		if excludeSet[strings.ToLower(col.name)] {
			continue
		}

		name := col.name
		if newName, ok := renameMap[strings.ToLower(col.name)]; ok {
			name = newName
		}

		if replExpr, ok := replaceMap[strings.ToLower(col.name)]; ok {
			replSources, replTransform, replFunc := traceExpr(replExpr, s, schema, nil)
			result = append(result, ColumnLineage{
				Name:          name,
				Index:         idx,
				TransformType: replTransform,
				Function:      replFunc,
				Sources:       dedupOrigins(replSources),
			})
		} else {
			result = append(result, ColumnLineage{
				Name:          name,
				Index:         idx,
				TransformType: "DIRECT",
				Sources:       col.origins,
			})
		}
		idx++
	}
	return result, nil
}

// === Expression Tracing ===

// traceExpr recursively traces an expression to its source columns.
// Returns (sources, transformType, functionName).
func traceExpr(expr Expr, s *scope, schema SchemaInfo, ctes map[string]*cteDef) ([]ColumnOrigin, string, string) {
	if expr == nil {
		return nil, "DIRECT", ""
	}

	switch e := expr.(type) {
	case *ColumnRef:
		sc := resolveColumnRef(e, s)
		if sc == nil {
			return nil, "DIRECT", ""
		}
		// Propagate transform type from inner query (CTE/derived table)
		transformType := sc.transformType
		if transformType == "" {
			transformType = "DIRECT"
		}
		return sc.origins, transformType, sc.function

	case *Literal:
		return nil, "DIRECT", ""

	case *ParenExpr:
		return traceExpr(e.Expr, s, schema, ctes)

	case *FuncCall:
		return traceFuncCall(e, s, schema, ctes)

	case *BinaryExpr:
		leftSrc, _, _ := traceExpr(e.Left, s, schema, ctes)
		rightSrc, _, _ := traceExpr(e.Right, s, schema, ctes)
		leftSrc = append(leftSrc, rightSrc...)
		return dedupOrigins(leftSrc), "EXPRESSION", ""

	case *UnaryExpr:
		src, _, _ := traceExpr(e.Expr, s, schema, ctes)
		return src, "EXPRESSION", ""

	case *CaseExpr:
		return traceCaseExpr(e, s, schema, ctes)

	case *CastExpr:
		src, _, _ := traceExpr(e.Expr, s, schema, ctes)
		return src, "EXPRESSION", "CAST"

	case *TypeCastExpr:
		src, _, _ := traceExpr(e.Expr, s, schema, ctes)
		return src, "EXPRESSION", "CAST"

	case *InExpr:
		return traceInExpr(e, s, schema, ctes)

	case *BetweenExpr:
		src1, _, _ := traceExpr(e.Expr, s, schema, ctes)
		src2, _, _ := traceExpr(e.Low, s, schema, ctes)
		src3, _, _ := traceExpr(e.High, s, schema, ctes)
		src1 = append(src1, src2...)
		src1 = append(src1, src3...)
		return dedupOrigins(src1), "EXPRESSION", ""

	case *IsNullExpr:
		src, _, _ := traceExpr(e.Expr, s, schema, ctes)
		return src, "EXPRESSION", ""

	case *IsBoolExpr:
		src, _, _ := traceExpr(e.Expr, s, schema, ctes)
		return src, "EXPRESSION", ""

	case *LikeExpr:
		src, _, _ := traceExpr(e.Expr, s, schema, ctes)
		patSrc, _, _ := traceExpr(e.Pattern, s, schema, ctes)
		return dedupOrigins(append(src, patSrc...)), "EXPRESSION", ""

	case *GlobExpr:
		src, _, _ := traceExpr(e.Expr, s, schema, ctes)
		patSrc, _, _ := traceExpr(e.Pattern, s, schema, ctes)
		return dedupOrigins(append(src, patSrc...)), "EXPRESSION", ""

	case *SimilarToExpr:
		src, _, _ := traceExpr(e.Expr, s, schema, ctes)
		patSrc, _, _ := traceExpr(e.Pattern, s, schema, ctes)
		return dedupOrigins(append(src, patSrc...)), "EXPRESSION", ""

	case *ExistsExpr:
		return traceExistsExpr(e, s, schema, ctes)

	case *SubqueryExpr:
		return traceSubqueryExpr(e, s, schema, ctes)

	case *ExtractExpr:
		src, _, _ := traceExpr(e.Expr, s, schema, ctes)
		return src, "EXPRESSION", "EXTRACT"

	case *IntervalExpr:
		return nil, "DIRECT", ""

	case *StarExpr:
		// Handled at SelectItem level
		return nil, "DIRECT", ""

	case *RawExpr:
		// Opaque — no sources
		return nil, "EXPRESSION", ""

	case *IndexExpr:
		src, _, _ := traceExpr(e.Expr, s, schema, ctes)
		idxSrc, _, _ := traceExpr(e.Index, s, schema, ctes)
		return dedupOrigins(append(src, idxSrc...)), "EXPRESSION", ""

	case *StructLiteral:
		var all []ColumnOrigin
		for _, f := range e.Fields {
			src, _, _ := traceExpr(f.Value, s, schema, ctes)
			all = append(all, src...)
		}
		return dedupOrigins(all), "EXPRESSION", ""

	case *ListLiteral:
		var all []ColumnOrigin
		for _, elem := range e.Elements {
			src, _, _ := traceExpr(elem, s, schema, ctes)
			all = append(all, src...)
		}
		return dedupOrigins(all), "EXPRESSION", ""

	case *LambdaExpr:
		src, _, _ := traceExpr(e.Body, s, schema, ctes)
		return src, "EXPRESSION", ""

	case *ColumnsExpr:
		return nil, "EXPRESSION", ""

	case *IsDistinctExpr:
		leftSrc, _, _ := traceExpr(e.Left, s, schema, ctes)
		rightSrc, _, _ := traceExpr(e.Right, s, schema, ctes)
		return dedupOrigins(append(leftSrc, rightSrc...)), "EXPRESSION", ""

	case *CollateExpr:
		return traceExpr(e.Expr, s, schema, ctes)

	case *MapLiteral:
		var all []ColumnOrigin
		for _, entry := range e.Entries {
			src, _, _ := traceExpr(entry.Value, s, schema, ctes)
			all = append(all, src...)
		}
		return dedupOrigins(all), "EXPRESSION", ""

	case *ListComprehension:
		var all []ColumnOrigin
		src, _, _ := traceExpr(e.Expr, s, schema, ctes)
		all = append(all, src...)
		src, _, _ = traceExpr(e.List, s, schema, ctes)
		all = append(all, src...)
		src, _, _ = traceExpr(e.Cond, s, schema, ctes)
		all = append(all, src...)
		return dedupOrigins(all), "EXPRESSION", ""

	case *NamedArgExpr:
		return traceExpr(e.Value, s, schema, ctes)

	case *ParamExpr, *DefaultExpr:
		return nil, "DIRECT", ""

	default:
		return nil, "EXPRESSION", ""
	}
}

func traceFuncCall(fc *FuncCall, s *scope, schema SchemaInfo, ctes map[string]*cteDef) ([]ColumnOrigin, string, string) {
	var all []ColumnOrigin

	for _, arg := range fc.Args {
		src, _, _ := traceExpr(arg, s, schema, ctes)
		all = append(all, src...)
	}

	if fc.Filter != nil {
		src, _, _ := traceExpr(fc.Filter, s, schema, ctes)
		all = append(all, src...)
	}

	if fc.Window != nil {
		for _, p := range fc.Window.PartitionBy {
			src, _, _ := traceExpr(p, s, schema, ctes)
			all = append(all, src...)
		}
		for _, o := range fc.Window.OrderBy {
			src, _, _ := traceExpr(o.Expr, s, schema, ctes)
			all = append(all, src...)
		}
	}

	// Named window references: full resolution would need the SelectCore's
	// Windows list. Currently a no-op simplification.

	return dedupOrigins(all), "EXPRESSION", strings.ToUpper(fc.Name)
}

func traceCaseExpr(ce *CaseExpr, s *scope, schema SchemaInfo, ctes map[string]*cteDef) ([]ColumnOrigin, string, string) {
	var all []ColumnOrigin

	if ce.Operand != nil {
		src, _, _ := traceExpr(ce.Operand, s, schema, ctes)
		all = append(all, src...)
	}

	for _, w := range ce.Whens {
		src, _, _ := traceExpr(w.Condition, s, schema, ctes)
		all = append(all, src...)
		src, _, _ = traceExpr(w.Result, s, schema, ctes)
		all = append(all, src...)
	}

	if ce.Else != nil {
		src, _, _ := traceExpr(ce.Else, s, schema, ctes)
		all = append(all, src...)
	}

	return dedupOrigins(all), "EXPRESSION", "CASE"
}

func traceInExpr(ie *InExpr, s *scope, schema SchemaInfo, ctes map[string]*cteDef) ([]ColumnOrigin, string, string) {
	var all []ColumnOrigin

	src, _, _ := traceExpr(ie.Expr, s, schema, ctes)
	all = append(all, src...)

	if ie.Query != nil {
		cols, _ := analyzeSelectStmt(ie.Query, schema, s)
		if len(cols) > 0 {
			all = append(all, cols[0].Sources...)
		}
	}

	for _, v := range ie.Values {
		src, _, _ := traceExpr(v, s, schema, ctes)
		all = append(all, src...)
	}

	return dedupOrigins(all), "EXPRESSION", ""
}

func traceExistsExpr(ee *ExistsExpr, s *scope, schema SchemaInfo, ctes map[string]*cteDef) ([]ColumnOrigin, string, string) {
	// EXISTS checks row existence — the select list doesn't contribute
	// to column lineage, but WHERE conditions may reference parent scope.
	// We trace the subquery's WHERE to find correlated column references.
	if ee.Select != nil && ee.Select.Body != nil && ee.Select.Body.Left != nil {
		core := ee.Select.Body.Left
		var all []ColumnOrigin
		if core.Where != nil {
			src, _, _ := traceExpr(core.Where, s, schema, ctes)
			all = append(all, src...)
		}
		return dedupOrigins(all), "EXPRESSION", ""
	}
	return nil, "EXPRESSION", ""
}

func traceSubqueryExpr(se *SubqueryExpr, s *scope, schema SchemaInfo, _ map[string]*cteDef) ([]ColumnOrigin, string, string) {
	cols, _ := analyzeSelectStmt(se.Select, schema, s)
	if len(cols) > 0 {
		return cols[0].Sources, "EXPRESSION", "SUBQUERY"
	}
	return nil, "EXPRESSION", "SUBQUERY"
}

// === Column Reference Resolution ===

// resolveColumnRef resolves a column reference to its scopeCol (which carries
// origins, transformType, and function from inner queries like CTEs/derived tables).
// Returns nil if the column cannot be resolved.
func resolveColumnRef(ref *ColumnRef, s *scope) *scopeCol {
	if s == nil {
		return nil
	}

	if ref.Table != "" {
		// Qualified: look up source by alias
		src, ok := s.findSource(ref.Table)
		if !ok {
			// Try parent scope
			if s.parent != nil {
				return resolveColumnRef(ref, s.parent)
			}
			return nil
		}
		sc, ok := findColumnInSource(src, ref.Column)
		if ok {
			return sc
		}
		return nil
	}

	// Unqualified: search all sources
	sc, ok := s.findColumn(ref.Column)
	if ok {
		return sc
	}

	return nil
}

// === Helpers ===

func inferColumnName(item SelectItem) string {
	if item.Alias != "" {
		return item.Alias
	}
	if item.Expr != nil {
		switch e := item.Expr.(type) {
		case *ColumnRef:
			return e.Column
		case *FuncCall:
			return strings.ToLower(e.Name)
		}
	}
	return ""
}

func buildExcludeSet(modifiers []StarModifier) map[string]bool {
	set := make(map[string]bool)
	for _, mod := range modifiers {
		if exc, ok := mod.(*ExcludeModifier); ok {
			for _, col := range exc.Columns {
				set[strings.ToLower(col)] = true
			}
		}
	}
	return set
}

func buildReplaceMap(modifiers []StarModifier) map[string]Expr {
	m := make(map[string]Expr)
	for _, mod := range modifiers {
		if rep, ok := mod.(*ReplaceModifier); ok {
			for _, item := range rep.Items {
				m[strings.ToLower(item.Alias)] = item.Expr
			}
		}
	}
	return m
}

func buildRenameMap(modifiers []StarModifier) map[string]string {
	m := make(map[string]string)
	for _, mod := range modifiers {
		if ren, ok := mod.(*RenameModifier); ok {
			for _, item := range ren.Items {
				m[strings.ToLower(item.OldName)] = item.NewName
			}
		}
	}
	return m
}

func dedupOrigins(origins []ColumnOrigin) []ColumnOrigin {
	if len(origins) == 0 {
		return nil
	}
	seen := make(map[string]bool)
	var result []ColumnOrigin
	for _, o := range origins {
		key := strings.ToLower(o.Schema) + "." + strings.ToLower(o.Table) + "." + strings.ToLower(o.Column)
		if !seen[key] {
			seen[key] = true
			result = append(result, o)
		}
	}
	return result
}
