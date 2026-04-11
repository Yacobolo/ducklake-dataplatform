package cuesqlgen

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"cuelang.org/go/cue/format"

	cuesql "duck-demo/pkg/cue-sql"
)

// OptimizeStats summarizes a querydef optimization pass.
type OptimizeStats struct {
	FilesChanged   int
	QueriesChanged int
}

// OptimizeQuerydefFiles rewrites querydef files into a compact canonical form.
func OptimizeQuerydefFiles(srcDir string) (OptimizeStats, error) {
	catalog, err := LoadCatalog(filepath.Join(srcDir, "..", "migrations"))
	if err != nil {
		return OptimizeStats{}, fmt.Errorf("load catalog: %w", err)
	}

	files, err := LoadQueryFiles(srcDir)
	if err != nil {
		return OptimizeStats{}, err
	}

	stats := OptimizeStats{}
	for _, file := range files {
		original := make([]cuesql.Query, len(file.Queries))
		copy(original, file.Queries)

		optimized := make([]cuesql.Query, len(file.Queries))
		changedQueries := 0
		for i, query := range file.Queries {
			next := canonicalizeQuery(query, catalog)
			if !queriesEqual(query, next) {
				changedQueries++
			}
			optimized[i] = next
		}

		body, err := renderOptimizedFile(optimized)
		if err != nil {
			return stats, fmt.Errorf("render %s: %w", filepath.Base(file.Path), err)
		}
		formatted, err := format.Source(body)
		if err != nil {
			return stats, fmt.Errorf("format %s: %w\n%s", filepath.Base(file.Path), err, string(body))
		}

		current, err := os.ReadFile(file.Path)
		if err != nil {
			return stats, fmt.Errorf("read %s: %w", file.Path, err)
		}
		if bytes.Equal(bytes.TrimSpace(current), bytes.TrimSpace(formatted)) {
			continue
		}
		if err := os.WriteFile(file.Path, formatted, 0o600); err != nil {
			return stats, fmt.Errorf("write %s: %w", file.Path, err)
		}
		stats.FilesChanged++
		stats.QueriesChanged += changedQueries
	}

	return stats, nil
}

func queriesEqual(a, b cuesql.Query) bool {
	left, _ := json.Marshal(a)
	right, _ := json.Marshal(b)
	return bytes.Equal(left, right)
}

func canonicalizeQuery(query cuesql.Query, catalog Catalog) cuesql.Query {
	if next, ok := StructurizeQuery(query); ok {
		query = next
	}

	if table, ok := collapseToTableResult(query, catalog); ok {
		query.Result = cuesql.Result{Table: table}
	}

	if query.Select != nil && query.Result.Table != "" && selectColumnsMatchTable(query, catalog) {
		query.Select.Columns = nil
	}
	if query.Insert != nil && query.Result.Table != "" && returningColumnsMatchTable(query.Insert.ReturningColumns, query.Result.Table, catalog) {
		query.Insert.Returning = true
		query.Insert.ReturningColumns = nil
	}
	if query.Update != nil && query.Result.Table != "" && returningColumnsMatchTable(query.Update.ReturningColumns, query.Result.Table, catalog) {
		query.Update.Returning = true
		query.Update.ReturningColumns = nil
	}

	return query
}

func collapseToTableResult(query cuesql.Query, catalog Catalog) (string, bool) {
	if query.Result.Table != "" {
		return query.Result.Table, true
	}
	if len(query.Result.Fields) == 0 {
		return "", false
	}

	tableName := ""
	switch {
	case query.Select != nil:
		tableName = query.Select.From
	case query.Insert != nil:
		tableName = query.Insert.Into
	case query.Update != nil:
		tableName = query.Update.Table
	default:
		return "", false
	}

	table, err := catalog.MustTable(tableName)
	if err != nil {
		return "", false
	}
	if query.Result.Row != "" && query.Result.Row != structNameForTable(tableName) {
		return "", false
	}
	if len(query.Result.Fields) != len(table.Columns) {
		return "", false
	}
	for i, column := range table.Columns {
		field := query.Result.Fields[i]
		if fieldName(field.Name) != camel(column.Name) {
			return "", false
		}
		if field.Type != goTypeForColumn(column) {
			return "", false
		}
		if field.Column != "" && field.Column != column.Name {
			return "", false
		}
	}
	return tableName, true
}

func selectColumnsMatchTable(query cuesql.Query, catalog Catalog) bool {
	stmt := query.Select
	if stmt == nil || len(stmt.Columns) == 0 || query.Result.Table == "" {
		return false
	}
	tableColumns, err := catalog.ColumnsForTable(query.Result.Table)
	if err != nil || len(stmt.Columns) != len(tableColumns) {
		return false
	}
	qualifier := stmt.From
	if stmt.Alias != "" {
		qualifier = stmt.Alias
	}
	for i, name := range tableColumns {
		expr := stmt.Columns[i].Expr
		if stmt.Columns[i].Alias != "" {
			return false
		}
		if expr != name && expr != qualifier+"."+name {
			return false
		}
	}
	return true
}

func returningColumnsMatchTable(columns []cuesql.Column, table string, catalog Catalog) bool {
	if len(columns) == 0 {
		return false
	}
	tableColumns, err := catalog.ColumnsForTable(table)
	if err != nil || len(columns) != len(tableColumns) {
		return false
	}
	for i, name := range tableColumns {
		if columns[i].Alias != "" || columns[i].Expr != name {
			return false
		}
	}
	return true
}

type localResultDef struct {
	Name   string
	Result cuesql.Result
}

func renderOptimizedFile(queries []cuesql.Query) ([]byte, error) {
	defs, resultRefs := collectLocalResultDefs(queries)

	var buf bytes.Buffer
	buf.WriteString("package querydefs\n\n")
	for _, def := range defs {
		buf.WriteString(def.Name)
		buf.WriteString(": ")
		renderResult(&buf, def.Result, "")
		buf.WriteString("\n\n")
	}
	buf.WriteString("queries: [\n")
	for _, query := range queries {
		renderQuery(&buf, query, resultRefs[query.Name])
		buf.WriteString(",\n")
	}
	buf.WriteString("]\n")
	return buf.Bytes(), nil
}

func collectLocalResultDefs(queries []cuesql.Query) ([]localResultDef, map[string]string) {
	type group struct {
		result cuesql.Result
		names  []string
	}
	groups := map[string]*group{}
	for _, query := range queries {
		if query.Result.Table != "" || query.Result.Scalar != "" || len(query.Result.Fields) == 0 {
			continue
		}
		keyBytes, _ := json.Marshal(query.Result)
		key := string(keyBytes)
		if groups[key] == nil {
			groups[key] = &group{result: query.Result}
		}
		groups[key].names = append(groups[key].names, query.Name)
	}

	usedNames := map[string]int{}
	resultRefs := map[string]string{}
	defs := make([]localResultDef, 0, len(groups))
	for _, group := range groups {
		if len(group.names) < 2 || group.result.Row == "" {
			continue
		}
		base := "#" + group.result.Row + "Result"
		name := base
		if usedNames[base] > 0 {
			name = fmt.Sprintf("%s%d", base, usedNames[base]+1)
		}
		usedNames[base]++
		defs = append(defs, localResultDef{Name: name, Result: group.result})
		for _, queryName := range group.names {
			resultRefs[queryName] = name
		}
	}
	sort.Slice(defs, func(i, j int) bool { return defs[i].Name < defs[j].Name })
	return defs, resultRefs
}

func renderQuery(buf *bytes.Buffer, query cuesql.Query, resultRef string) {
	if renderSharedQuery(buf, query, resultRef) {
		return
	}
	buf.WriteString("\t{\n")
	renderBaseQueryFields(buf, query, resultRef, "\t\t")
	buf.WriteString("\t}")
}

func renderBaseQueryFields(buf *bytes.Buffer, query cuesql.Query, resultRef string, indent string) {
	writeLine(buf, indent, "name: "+quote(query.Name))
	writeLine(buf, indent, "kind: "+quote(string(query.Kind)))
	if query.ParamMode != "" && !paramModeIsDefault(query) {
		writeLine(buf, indent, "paramMode: "+quote(query.ParamMode))
	}
	if len(query.Params) > 0 {
		writeLabel(buf, indent, "params", func() {
			renderParams(buf, query.Params, indent+"\t")
		})
	}
	if query.Kind != cuesql.KindExec && query.Kind != cuesql.KindExecResult && query.Kind != cuesql.KindExecRows {
		if resultRef != "" {
			writeLine(buf, indent, "result: "+resultRef)
		} else {
			writeLabel(buf, indent, "result", func() {
				renderResult(buf, query.Result, indent+"\t")
			})
		}
	}
	switch {
	case query.Select != nil:
		writeLabel(buf, indent, "select", func() { renderSelect(buf, *query.Select, indent+"\t") })
	case query.Insert != nil:
		writeLabel(buf, indent, "insert", func() { renderInsert(buf, *query.Insert, indent+"\t") })
	case query.Update != nil:
		writeLabel(buf, indent, "update", func() { renderUpdate(buf, *query.Update, indent+"\t") })
	case query.Delete != nil:
		writeLabel(buf, indent, "delete", func() { renderDelete(buf, *query.Delete, indent+"\t") })
	case query.Raw != nil:
		writeLabel(buf, indent, "raw", func() { renderRaw(buf, *query.Raw, indent+"\t") })
	}
}

func renderSharedQuery(buf *bytes.Buffer, query cuesql.Query, resultRef string) bool {
	if resultRef != "" {
		return false
	}
	if spec, ok := matchCountAll(query); ok {
		renderSharedBlock(buf, "#CountAll", func() {
			writeLine(buf, "\t\t", "name: "+quote(query.Name))
			writeLine(buf, "\t\t", "_table: "+quote(spec.table))
		})
		return true
	}
	if spec, ok := matchGetByID(query); ok {
		renderSharedBlock(buf, "#GetByID", func() {
			writeLine(buf, "\t\t", "name: "+quote(query.Name))
			writeLine(buf, "\t\t", "_table: "+quote(spec.table))
		})
		return true
	}
	if spec, ok := matchDeleteByID(query); ok {
		renderSharedBlock(buf, "#DeleteByID", func() {
			writeLine(buf, "\t\t", "name: "+quote(query.Name))
			writeLine(buf, "\t\t", "_table: "+quote(spec.table))
		})
		return true
	}
	if spec, ok := matchGetByStringField(query); ok {
		renderSharedBlock(buf, "#GetByStringField", func() {
			writeLine(buf, "\t\t", "name: "+quote(query.Name))
			writeLine(buf, "\t\t", "_table: "+quote(spec.table))
			writeLine(buf, "\t\t", "_field: "+quote(spec.field))
			writeLine(buf, "\t\t", "_param: "+quote(spec.param))
		})
		return true
	}
	if spec, ok := matchGetByTwoStringFields(query); ok {
		renderSharedBlock(buf, "#GetByTwoStringFields", func() {
			writeLine(buf, "\t\t", "name: "+quote(query.Name))
			writeLine(buf, "\t\t", "_table: "+quote(spec.table))
			writeLine(buf, "\t\t", "_field1: "+quote(spec.field1))
			writeLine(buf, "\t\t", "_param1: "+quote(spec.param1))
			writeLine(buf, "\t\t", "_field2: "+quote(spec.field2))
			writeLine(buf, "\t\t", "_param2: "+quote(spec.param2))
		})
		return true
	}
	if spec, ok := matchListAllOrdered(query); ok {
		renderSharedBlock(buf, "#ListAllOrdered", func() {
			writeLine(buf, "\t\t", "name: "+quote(query.Name))
			writeLine(buf, "\t\t", "_table: "+quote(spec.table))
			writeLabel(buf, "\t\t", "_order", func() { renderOrderBy(buf, spec.order, "\t\t\t") })
			if len(spec.where) > 0 {
				writeStructLabel(buf, "\t\t", "select", func() {
					writeLabel(buf, "\t\t\t", "where", func() { renderPredicates(buf, spec.where, "\t\t\t\t") })
				})
			}
		})
		return true
	}
	if spec, ok := matchListPaginatedOrdered(query); ok {
		renderSharedBlock(buf, "#ListPaginatedOrdered", func() {
			writeLine(buf, "\t\t", "name: "+quote(query.Name))
			writeLine(buf, "\t\t", "_table: "+quote(spec.table))
			writeLabel(buf, "\t\t", "_order", func() { renderOrderBy(buf, spec.order, "\t\t\t") })
		})
		return true
	}
	if spec, ok := matchCountFiltered(query); ok {
		renderSharedBlock(buf, "#CountFiltered", func() {
			writeLine(buf, "\t\t", "name: "+quote(query.Name))
			writeLine(buf, "\t\t", "_table: "+quote(spec.table))
			writeLabel(buf, "\t\t", "_params", func() { renderParams(buf, spec.params, "\t\t\t") })
			writeLabel(buf, "\t\t", "_where", func() { renderPredicates(buf, spec.where, "\t\t\t") })
		})
		return true
	}
	if spec, ok := matchListFilteredPaginatedOrdered(query); ok {
		renderSharedBlock(buf, "#ListFilteredPaginatedOrdered", func() {
			writeLine(buf, "\t\t", "name: "+quote(query.Name))
			writeLine(buf, "\t\t", "_table: "+quote(spec.table))
			writeLabel(buf, "\t\t", "_params", func() { renderParams(buf, spec.params, "\t\t\t") })
			writeLabel(buf, "\t\t", "_where", func() { renderPredicates(buf, spec.where, "\t\t\t") })
			writeLabel(buf, "\t\t", "_order", func() { renderOrderBy(buf, spec.order, "\t\t\t") })
		})
		return true
	}
	if spec, ok := matchInsertReturningTable(query); ok {
		renderSharedBlock(buf, "#InsertReturningTable", func() {
			writeLine(buf, "\t\t", "name: "+quote(query.Name))
			writeLine(buf, "\t\t", "_table: "+quote(spec.table))
			if len(query.Params) > 0 {
				writeLabel(buf, "\t\t", "params", func() { renderParams(buf, query.Params, "\t\t\t") })
			}
			writeStructLabel(buf, "\t\t", "insert", func() {
				if spec.stmt.Modifier != "" {
					writeLine(buf, "\t\t\t", "modifier: "+quote(spec.stmt.Modifier))
				}
				writeStringArrayLabel(buf, "\t\t\t", "columns", spec.stmt.Columns)
				writeLabel(buf, "\t\t\t", "values", func() { renderValueExprs(buf, spec.stmt.Values, "\t\t\t\t") })
				if spec.stmt.Conflict != nil {
					writeLabel(buf, "\t\t\t", "conflict", func() { renderConflict(buf, *spec.stmt.Conflict, "\t\t\t\t") })
				}
			})
		})
		return true
	}
	if spec, ok := matchUpdateByIDTouch(query); ok {
		renderSharedBlock(buf, "#UpdateByIDTouch", func() {
			writeLine(buf, "\t\t", "name: "+quote(query.Name))
			writeLine(buf, "\t\t", "_table: "+quote(spec.table))
			if query.Kind != cuesql.KindExec {
				writeLine(buf, "\t\t", "_kind: "+quote(string(query.Kind)))
			}
			writeLabel(buf, "\t\t", "params", func() { renderParams(buf, query.Params, "\t\t\t") })
			writeLabel(buf, "\t\t", "_set", func() { renderAssignments(buf, spec.set, "\t\t\t") })
		})
		return true
	}
	return false
}

func renderSharedBlock(buf *bytes.Buffer, shared string, body func()) {
	buf.WriteString("\t")
	buf.WriteString(shared)
	buf.WriteString(" & {\n")
	body()
	buf.WriteString("\t}")
}

type simpleTableSpec struct{ table string }
type fieldSpec struct {
	table string
	field string
	param string
}
type twoFieldSpec struct {
	table         string
	field1, param1 string
	field2, param2 string
}
type orderSpec struct {
	table string
	order []cuesql.OrderBy
	where []cuesql.Predicate
}
type filteredSpec struct {
	table  string
	params []cuesql.Param
	where  []cuesql.Predicate
	order  []cuesql.OrderBy
}
type insertSpec struct {
	table string
	stmt  *cuesql.Insert
}
type updateTouchSpec struct {
	table string
	set   []cuesql.Assignment
}

func matchCountAll(query cuesql.Query) (simpleTableSpec, bool) {
	if query.Kind != cuesql.KindOne || query.Select == nil || len(query.Params) != 0 || query.Result.Scalar != "int64" {
		return simpleTableSpec{}, false
	}
	stmt := query.Select
	if len(stmt.Where) != 0 || len(stmt.OrderBy) != 0 || stmt.LimitParam != "" || stmt.OffsetParam != "" || stmt.LimitSQL != "" {
		return simpleTableSpec{}, false
	}
	if len(stmt.Columns) != 1 || strings.ToUpper(stmt.Columns[0].Expr) != "COUNT(*)" {
		return simpleTableSpec{}, false
	}
	return simpleTableSpec{table: stmt.From}, true
}

func matchGetByID(query cuesql.Query) (simpleTableSpec, bool) {
	if query.Kind != cuesql.KindOne || query.Select == nil || query.Result.Table == "" || query.Result.Table != query.Select.From {
		return simpleTableSpec{}, false
	}
	if len(query.Params) != 1 || query.Params[0].Name != "id" || query.Params[0].Type != "string" {
		return simpleTableSpec{}, false
	}
	stmt := query.Select
	if stmt.Alias != "" || len(stmt.Joins) != 0 || len(stmt.OrderBy) != 0 || stmt.LimitParam != "" || stmt.OffsetParam != "" || stmt.LimitSQL != "" {
		return simpleTableSpec{}, false
	}
	if len(stmt.Where) != 1 {
		return simpleTableSpec{}, false
	}
	p := stmt.Where[0]
	if p.Column != "id" || p.Op != "=" || p.Param != "id" || p.Optional || p.Slice || p.ValueSQL != "" || len(p.Any) > 0 || len(p.All) > 0 || p.RawSQL != "" {
		return simpleTableSpec{}, false
	}
	return simpleTableSpec{table: stmt.From}, true
}

func matchDeleteByID(query cuesql.Query) (simpleTableSpec, bool) {
	if query.Kind != cuesql.KindExec || query.Delete == nil || len(query.Params) != 1 || query.Params[0].Name != "id" || query.Params[0].Type != "string" {
		return simpleTableSpec{}, false
	}
	stmt := query.Delete
	if len(stmt.Where) != 1 {
		return simpleTableSpec{}, false
	}
	p := stmt.Where[0]
	if p.Column != "id" || p.Op != "=" || p.Param != "id" || p.Optional || p.Slice || p.ValueSQL != "" || len(p.Any) > 0 || len(p.All) > 0 || p.RawSQL != "" {
		return simpleTableSpec{}, false
	}
	return simpleTableSpec{table: stmt.From}, true
}

func matchGetByStringField(query cuesql.Query) (fieldSpec, bool) {
	if query.Kind != cuesql.KindOne || query.Select == nil || query.Result.Table == "" || query.Result.Table != query.Select.From {
		return fieldSpec{}, false
	}
	if len(query.Params) != 1 || query.Params[0].Type != "string" {
		return fieldSpec{}, false
	}
	stmt := query.Select
	if stmt.Alias != "" || len(stmt.Joins) != 0 || len(stmt.OrderBy) != 0 || stmt.LimitParam != "" || stmt.OffsetParam != "" || stmt.LimitSQL != "" || len(stmt.Where) != 1 {
		return fieldSpec{}, false
	}
	p := stmt.Where[0]
	if p.Op != "=" || p.Param != query.Params[0].Name || p.Optional || p.Slice || p.ValueSQL != "" || len(p.Any) > 0 || len(p.All) > 0 || p.RawSQL != "" {
		return fieldSpec{}, false
	}
	return fieldSpec{table: stmt.From, field: p.Column, param: p.Param}, true
}

func matchGetByTwoStringFields(query cuesql.Query) (twoFieldSpec, bool) {
	if query.Kind != cuesql.KindOne || query.Select == nil || query.Result.Table == "" || query.Result.Table != query.Select.From || len(query.Params) != 2 {
		return twoFieldSpec{}, false
	}
	for _, param := range query.Params {
		if param.Type != "string" {
			return twoFieldSpec{}, false
		}
	}
	stmt := query.Select
	if stmt.Alias != "" || len(stmt.Joins) != 0 || len(stmt.OrderBy) != 0 || stmt.LimitParam != "" || stmt.OffsetParam != "" || stmt.LimitSQL != "" || len(stmt.Where) != 2 {
		return twoFieldSpec{}, false
	}
	first, second := stmt.Where[0], stmt.Where[1]
	if !simpleEquality(first) || !simpleEquality(second) {
		return twoFieldSpec{}, false
	}
	return twoFieldSpec{table: stmt.From, field1: first.Column, param1: first.Param, field2: second.Column, param2: second.Param}, true
}

func matchListAllOrdered(query cuesql.Query) (orderSpec, bool) {
	if query.Kind != cuesql.KindMany || query.Select == nil || query.Result.Table == "" || query.Result.Table != query.Select.From || len(query.Params) != 0 {
		return orderSpec{}, false
	}
	stmt := query.Select
	if stmt.Alias != "" || len(stmt.Joins) != 0 || stmt.LimitParam != "" || stmt.OffsetParam != "" || stmt.LimitSQL != "" {
		return orderSpec{}, false
	}
	return orderSpec{table: stmt.From, order: stmt.OrderBy, where: stmt.Where}, true
}

func matchListPaginatedOrdered(query cuesql.Query) (orderSpec, bool) {
	if query.Kind != cuesql.KindMany || query.Select == nil || query.Result.Table == "" || query.Result.Table != query.Select.From {
		return orderSpec{}, false
	}
	stmt := query.Select
	if stmt.Alias != "" || len(stmt.Joins) != 0 || len(stmt.Where) != 0 || stmt.LimitParam != "Limit" || stmt.OffsetParam != "Offset" || stmt.LimitSQL != "" {
		return orderSpec{}, false
	}
	if len(query.Params) != 2 || query.Params[0].Name != "Limit" || query.Params[0].Type != "int64" || query.Params[1].Name != "Offset" || query.Params[1].Type != "int64" {
		return orderSpec{}, false
	}
	return orderSpec{table: stmt.From, order: stmt.OrderBy}, true
}

func matchCountFiltered(query cuesql.Query) (filteredSpec, bool) {
	if query.Kind != cuesql.KindOne || query.Select == nil || query.Result.Scalar != "int64" {
		return filteredSpec{}, false
	}
	stmt := query.Select
	if len(stmt.Columns) != 1 || strings.ToUpper(stmt.Columns[0].Expr) != "COUNT(*)" || len(stmt.OrderBy) != 0 || stmt.LimitParam != "" || stmt.OffsetParam != "" || stmt.LimitSQL != "" || stmt.Alias != "" || len(stmt.Joins) != 0 {
		return filteredSpec{}, false
	}
	return filteredSpec{table: stmt.From, params: query.Params, where: stmt.Where}, true
}

func matchListFilteredPaginatedOrdered(query cuesql.Query) (filteredSpec, bool) {
	if query.Kind != cuesql.KindMany || query.Select == nil || query.Result.Table == "" || query.Result.Table != query.Select.From {
		return filteredSpec{}, false
	}
	stmt := query.Select
	if stmt.Alias != "" || len(stmt.Joins) != 0 || stmt.LimitParam != "Limit" || stmt.OffsetParam != "Offset" || stmt.LimitSQL != "" || len(query.Params) < 2 {
		return filteredSpec{}, false
	}
	if query.Params[len(query.Params)-2].Name != "Limit" || query.Params[len(query.Params)-2].Type != "int64" || query.Params[len(query.Params)-1].Name != "Offset" || query.Params[len(query.Params)-1].Type != "int64" {
		return filteredSpec{}, false
	}
	return filteredSpec{table: stmt.From, params: query.Params[:len(query.Params)-2], where: stmt.Where, order: stmt.OrderBy}, true
}

func matchInsertReturningTable(query cuesql.Query) (insertSpec, bool) {
	if query.Kind != cuesql.KindOne || query.Insert == nil || query.Result.Table == "" || query.Result.Table != query.Insert.Into || !query.Insert.Returning || len(query.Insert.ReturningColumns) != 0 {
		return insertSpec{}, false
	}
	return insertSpec{table: query.Insert.Into, stmt: query.Insert}, true
}

func matchUpdateByIDTouch(query cuesql.Query) (updateTouchSpec, bool) {
	if query.Update == nil {
		return updateTouchSpec{}, false
	}
	stmt := query.Update
	if len(stmt.Where) != 1 || !simpleEquality(stmt.Where[0]) || stmt.Where[0].Column != "id" || stmt.Where[0].Param != "ID" {
		return updateTouchSpec{}, false
	}
	if len(stmt.Set) == 0 {
		return updateTouchSpec{}, false
	}
	last := stmt.Set[len(stmt.Set)-1]
	if last.Column != "updated_at" || last.Value.SQL != "datetime('now')" || last.CoalesceWith {
		return updateTouchSpec{}, false
	}
	if query.Kind == cuesql.KindOne {
		if query.Result.Table != stmt.Table || !stmt.Returning || len(stmt.ReturningColumns) != 0 {
			return updateTouchSpec{}, false
		}
	} else if query.Kind != cuesql.KindExec {
		return updateTouchSpec{}, false
	}
	return updateTouchSpec{table: stmt.Table, set: stmt.Set[:len(stmt.Set)-1]}, true
}

func simpleEquality(predicate cuesql.Predicate) bool {
	return predicate.Op == "=" && predicate.Param != "" && !predicate.Optional && !predicate.Slice && predicate.ValueSQL == "" && predicate.RawSQL == "" && len(predicate.All) == 0 && len(predicate.Any) == 0
}

func paramModeIsDefault(query cuesql.Query) bool {
	if len(query.Params) == 1 {
		return query.ParamMode == "single" || query.ParamMode == ""
	}
	return query.ParamMode == "struct" || query.ParamMode == ""
}

func renderParams(buf *bytes.Buffer, params []cuesql.Param, indent string) {
	buf.WriteString("[\n")
	for _, param := range params {
		writeLine(buf, indent, "{name: "+quote(param.Name)+", type: "+quote(param.Type)+"},")
	}
	writeLine(buf, indent[:len(indent)-1], "]")
}

func renderResult(buf *bytes.Buffer, result cuesql.Result, indent string) {
	if result.Table != "" && result.Row == "" && len(result.Fields) == 0 && result.Scalar == "" {
		buf.WriteString("{table: ")
		buf.WriteString(quote(result.Table))
		buf.WriteString("}")
		return
	}
	if result.Scalar != "" && result.Table == "" && result.Row == "" && len(result.Fields) == 0 {
		buf.WriteString("{scalar: ")
		buf.WriteString(quote(result.Scalar))
		buf.WriteString("}")
		return
	}
	buf.WriteString("{\n")
	if result.Table != "" {
		writeLine(buf, indent+"\t", "table: "+quote(result.Table))
	}
	if result.Row != "" {
		writeLine(buf, indent+"\t", "row: "+quote(result.Row))
	}
	if result.Scalar != "" {
		writeLine(buf, indent+"\t", "scalar: "+quote(result.Scalar))
	}
	if len(result.Fields) > 0 {
		writeLabel(buf, indent+"\t", "fields", func() { renderFields(buf, result.Fields, indent+"\t\t") })
	}
	writeLine(buf, indent, "}")
}

func renderFields(buf *bytes.Buffer, fields []cuesql.Field, indent string) {
	buf.WriteString("[\n")
	for _, field := range fields {
		line := "{name: " + quote(field.Name) + ", type: " + quote(field.Type)
		if field.Column != "" {
			line += ", column: " + quote(field.Column)
		}
		line += "},"
		writeLine(buf, indent, line)
	}
	writeLine(buf, indent[:len(indent)-1], "]")
}

func renderSelect(buf *bytes.Buffer, stmt cuesql.Select, indent string) {
	buf.WriteString("{\n")
	writeLine(buf, indent+"\t", "from: "+quote(stmt.From))
	if stmt.Alias != "" {
		writeLine(buf, indent+"\t", "alias: "+quote(stmt.Alias))
	}
	if stmt.Distinct {
		writeLine(buf, indent+"\t", "distinct: true")
	}
	if len(stmt.Columns) > 0 {
		writeLabel(buf, indent+"\t", "columns", func() { renderColumns(buf, stmt.Columns, indent+"\t\t") })
	}
	if len(stmt.Joins) > 0 {
		writeLabel(buf, indent+"\t", "joins", func() { renderJoins(buf, stmt.Joins, indent+"\t\t") })
	}
	if len(stmt.Where) > 0 {
		writeLabel(buf, indent+"\t", "where", func() { renderPredicates(buf, stmt.Where, indent+"\t\t") })
	}
	if len(stmt.OrderBy) > 0 {
		writeLabel(buf, indent+"\t", "orderBy", func() { renderOrderBy(buf, stmt.OrderBy, indent+"\t\t") })
	}
	if stmt.LimitSQL != "" {
		writeLine(buf, indent+"\t", "limitSQL: "+quote(stmt.LimitSQL))
	}
	if stmt.LimitParam != "" {
		writeLine(buf, indent+"\t", "limitParam: "+quote(stmt.LimitParam))
	}
	if stmt.OffsetParam != "" {
		writeLine(buf, indent+"\t", "offsetParam: "+quote(stmt.OffsetParam))
	}
	writeLine(buf, indent, "}")
}

func renderInsert(buf *bytes.Buffer, stmt cuesql.Insert, indent string) {
	buf.WriteString("{\n")
	if stmt.Modifier != "" {
		writeLine(buf, indent+"\t", "modifier: "+quote(stmt.Modifier))
	}
	writeLine(buf, indent+"\t", "into: "+quote(stmt.Into))
	writeStringArrayLabel(buf, indent+"\t", "columns", stmt.Columns)
	writeLabel(buf, indent+"\t", "values", func() { renderValueExprs(buf, stmt.Values, indent+"\t\t") })
	if stmt.Conflict != nil {
		writeLabel(buf, indent+"\t", "conflict", func() { renderConflict(buf, *stmt.Conflict, indent+"\t\t") })
	}
	if stmt.Returning {
		writeLine(buf, indent+"\t", "returning: true")
	}
	if len(stmt.ReturningColumns) > 0 {
		writeLabel(buf, indent+"\t", "returningColumns", func() { renderColumns(buf, stmt.ReturningColumns, indent+"\t\t") })
	}
	writeLine(buf, indent, "}")
}

func renderConflict(buf *bytes.Buffer, conflict cuesql.Conflict, indent string) {
	buf.WriteString("{\n")
	writeStringArrayLabel(buf, indent+"\t", "targets", conflict.Targets)
	writeLabel(buf, indent+"\t", "doUpdate", func() { renderAssignments(buf, conflict.DoUpdate, indent+"\t\t") })
	writeLine(buf, indent, "}")
}

func renderUpdate(buf *bytes.Buffer, stmt cuesql.Update, indent string) {
	buf.WriteString("{\n")
	writeLine(buf, indent+"\t", "table: "+quote(stmt.Table))
	writeLabel(buf, indent+"\t", "set", func() { renderAssignments(buf, stmt.Set, indent+"\t\t") })
	if len(stmt.Where) > 0 {
		writeLabel(buf, indent+"\t", "where", func() { renderPredicates(buf, stmt.Where, indent+"\t\t") })
	}
	if stmt.Returning {
		writeLine(buf, indent+"\t", "returning: true")
	}
	if len(stmt.ReturningColumns) > 0 {
		writeLabel(buf, indent+"\t", "returningColumns", func() { renderColumns(buf, stmt.ReturningColumns, indent+"\t\t") })
	}
	writeLine(buf, indent, "}")
}

func renderDelete(buf *bytes.Buffer, stmt cuesql.Delete, indent string) {
	buf.WriteString("{\n")
	writeLine(buf, indent+"\t", "from: "+quote(stmt.From))
	if len(stmt.Where) > 0 {
		writeLabel(buf, indent+"\t", "where", func() { renderPredicates(buf, stmt.Where, indent+"\t\t") })
	}
	writeLine(buf, indent, "}")
}

func renderRaw(buf *bytes.Buffer, raw cuesql.Raw, indent string) {
	buf.WriteString("{\n")
	writeLine(buf, indent+"\t", "sql: "+quote(raw.SQL))
	if len(raw.Bind) > 0 {
		writeStringArrayLabel(buf, indent+"\t", "bind", raw.Bind)
	}
	writeLine(buf, indent, "}")
}

func renderColumns(buf *bytes.Buffer, columns []cuesql.Column, indent string) {
	buf.WriteString("[\n")
	for _, column := range columns {
		line := "{expr: " + quote(column.Expr)
		if column.Alias != "" {
			line += ", alias: " + quote(column.Alias)
		}
		line += "},"
		writeLine(buf, indent, line)
	}
	writeLine(buf, indent[:len(indent)-1], "]")
}

func renderJoins(buf *bytes.Buffer, joins []cuesql.Join, indent string) {
	buf.WriteString("[\n")
	for _, join := range joins {
		line := "{"
		parts := []string{}
		if join.Type != "" {
			parts = append(parts, "type: "+quote(join.Type))
		}
		parts = append(parts, "table: "+quote(join.Table))
		if join.Alias != "" {
			parts = append(parts, "alias: "+quote(join.Alias))
		}
		parts = append(parts, "on: "+quote(join.On))
		line += strings.Join(parts, ", ") + "},"
		writeLine(buf, indent, line)
	}
	writeLine(buf, indent[:len(indent)-1], "]")
}

func renderPredicates(buf *bytes.Buffer, predicates []cuesql.Predicate, indent string) {
	buf.WriteString("[\n")
	for _, predicate := range predicates {
		renderPredicate(buf, predicate, indent)
	}
	writeLine(buf, indent[:len(indent)-1], "]")
}

func renderPredicate(buf *bytes.Buffer, predicate cuesql.Predicate, indent string) {
	if len(predicate.All) > 0 {
		writeLabel(buf, indent, "", func() {
			writeLabel(buf, indent+"\t", "all", func() { renderPredicates(buf, predicate.All, indent+"\t\t") })
		})
		return
	}
	if len(predicate.Any) > 0 {
		writeLabel(buf, indent, "", func() {
			writeLabel(buf, indent+"\t", "any", func() { renderPredicates(buf, predicate.Any, indent+"\t\t") })
		})
		return
	}
	parts := make([]string, 0, 8)
	if predicate.Column != "" {
		parts = append(parts, "column: "+quote(predicate.Column))
	}
	if predicate.Expr != "" {
		parts = append(parts, "expr: "+quote(predicate.Expr))
	}
	if predicate.Op != "" {
		parts = append(parts, "op: "+quote(predicate.Op))
	}
	if predicate.Param != "" {
		parts = append(parts, "param: "+quote(predicate.Param))
	}
	if predicate.ValueSQL != "" {
		parts = append(parts, "valueSQL: "+quote(predicate.ValueSQL))
	}
	if predicate.Optional {
		parts = append(parts, "optional: true")
	}
	if predicate.Slice {
		parts = append(parts, "slice: true")
	}
	if predicate.RawSQL != "" {
		parts = append(parts, "rawSQL: "+quote(predicate.RawSQL))
	}
	writeLine(buf, indent, "{"+strings.Join(parts, ", ")+"},")
}

func renderOrderBy(buf *bytes.Buffer, items []cuesql.OrderBy, indent string) {
	buf.WriteString("[\n")
	for _, item := range items {
		line := "{expr: " + quote(item.Expr)
		if item.Desc {
			line += ", desc: true"
		}
		line += "},"
		writeLine(buf, indent, line)
	}
	writeLine(buf, indent[:len(indent)-1], "]")
}

func renderValueExprs(buf *bytes.Buffer, values []cuesql.ValueExpr, indent string) {
	buf.WriteString("[\n")
	for _, value := range values {
		line := "{"
		if value.Param != "" {
			line += "param: " + quote(value.Param)
		} else {
			line += "sql: " + quote(value.SQL)
		}
		line += "},"
		writeLine(buf, indent, line)
	}
	writeLine(buf, indent[:len(indent)-1], "]")
}

func renderAssignments(buf *bytes.Buffer, set []cuesql.Assignment, indent string) {
	buf.WriteString("[\n")
	for _, assignment := range set {
		buf.WriteString(indent)
		buf.WriteString("{column: ")
		buf.WriteString(quote(assignment.Column))
		buf.WriteString(", value: {")
		if assignment.Value.Param != "" {
			buf.WriteString("param: ")
			buf.WriteString(quote(assignment.Value.Param))
		} else {
			buf.WriteString("sql: ")
			buf.WriteString(quote(assignment.Value.SQL))
		}
		buf.WriteString("}")
		if assignment.CoalesceWith {
			buf.WriteString(", coalesceWith: true")
		}
		buf.WriteString("},\n")
	}
	writeLine(buf, indent[:len(indent)-1], "]")
}

func writeLabel(buf *bytes.Buffer, indent, label string, body func()) {
	if label == "" {
		buf.WriteString(indent)
		buf.WriteString("{\n")
		body()
		writeLine(buf, indent, "},")
		return
	}
	buf.WriteString(indent)
	buf.WriteString(label)
	buf.WriteString(": ")
	body()
	if !strings.HasSuffix(buf.String(), "\n") {
		buf.WriteString("\n")
	}
}

func writeStructLabel(buf *bytes.Buffer, indent, label string, body func()) {
	buf.WriteString(indent)
	buf.WriteString(label)
	buf.WriteString(": {\n")
	body()
	writeLine(buf, indent, "}")
}

func writeStringArrayLabel(buf *bytes.Buffer, indent, label string, values []string) {
	if len(values) == 0 {
		return
	}
	writeLabel(buf, indent, label, func() {
		buf.WriteString("[\n")
		for _, value := range values {
			writeLine(buf, indent+"\t", quote(value)+",")
		}
		writeLine(buf, indent, "]")
	})
}

func writeLine(buf *bytes.Buffer, indent, line string) {
	buf.WriteString(indent)
	buf.WriteString(line)
	buf.WriteString("\n")
}

func quote(value string) string {
	return strconv.Quote(value)
}
