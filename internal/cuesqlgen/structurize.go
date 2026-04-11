package cuesqlgen

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"

	"cuelang.org/go/cue"
	"cuelang.org/go/cue/cuecontext"

	cuesql "duck-demo/pkg/cue-sql"
)

var joinKeywordRe = regexp.MustCompile(`(?i)^(LEFT OUTER JOIN|LEFT JOIN|INNER JOIN|JOIN)\s+([A-Za-z0-9_]+)(?:\s+([A-Za-z0-9_]+))?\s+ON\s+(.+)$`)

// StructurizeStats summarizes a structurize pass.
type StructurizeStats struct {
	FilesChanged   int
	QueriesChanged int
}

// StructurizeLegacyFiles rewrites straightforward raw legacy querydefs into structured CUE-SQL statements.
func StructurizeLegacyFiles(dir string) (StructurizeStats, error) {
	files, err := filepath.Glob(filepath.Join(dir, "legacy_*.cue"))
	if err != nil {
		return StructurizeStats{}, fmt.Errorf("glob legacy querydefs: %w", err)
	}
	sort.Strings(files)

	stats := StructurizeStats{}
	for _, file := range files {
		queries, err := loadQueryFile(file)
		if err != nil {
			return stats, err
		}

		changed := false
		for i := range queries {
			next, ok := StructurizeQuery(queries[i])
			if !ok {
				continue
			}
			queries[i] = next
			stats.QueriesChanged++
			changed = true
		}
		if !changed {
			continue
		}

		payload, err := json.MarshalIndent(queries, "", "\t")
		if err != nil {
			return stats, fmt.Errorf("marshal %s: %w", filepath.Base(file), err)
		}
		body := "package querydefs\n\nqueries: " + string(payload) + "\n"
		if err := os.WriteFile(file, []byte(body), 0o600); err != nil {
			return stats, fmt.Errorf("write %s: %w", file, err)
		}
		stats.FilesChanged++
	}

	return stats, nil
}

// StructurizeQuery converts a raw query into a structured statement when the SQL shape is simple enough.
func StructurizeQuery(query cuesql.Query) (cuesql.Query, bool) {
	if query.Raw == nil {
		return query, false
	}

	sql := normalizeSQL(query.Raw.SQL)
	binds := append([]string(nil), query.Raw.Bind...)

	if insert, ok := parseInsert(sql, binds); ok {
		query.Insert = insert
		query.Raw = nil
		return query, true
	}
	if selectStmt, ok := parseSelect(sql, binds); ok {
		query.Select = selectStmt
		query.Raw = nil
		return query, true
	}
	if update, ok := parseUpdate(sql, binds); ok {
		query.Update = update
		query.Raw = nil
		return query, true
	}
	if deleteStmt, ok := parseDelete(sql, binds); ok {
		query.Delete = deleteStmt
		query.Raw = nil
		return query, true
	}

	return query, false
}

func loadQueryFile(path string) ([]cuesql.Query, error) {
	//nolint:gosec // The structurize command only rewrites repo-local querydef files discovered under the configured source dir.
	contents, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", path, err)
	}
	ctx := cuecontext.New()
	value := ctx.CompileBytes(contents, cue.Filename(path))
	if err := value.Err(); err != nil {
		return nil, fmt.Errorf("compile %s: %w", filepath.Base(path), err)
	}
	field := value.LookupPath(cue.ParsePath("queries"))
	if err := field.Err(); err != nil {
		return nil, fmt.Errorf("%s: missing queries field: %w", filepath.Base(path), err)
	}
	var queries []cuesql.Query
	if err := field.Decode(&queries); err != nil {
		return nil, fmt.Errorf("decode %s queries: %w", filepath.Base(path), err)
	}
	return queries, nil
}

func parseSelect(sql string, binds []string) (*cuesql.Select, bool) {
	if !strings.HasPrefix(strings.ToUpper(sql), "SELECT ") {
		return nil, false
	}
	fromIdx := indexKeyword(sql, " FROM ")
	if fromIdx < 0 {
		return nil, false
	}

	columnsPart := strings.TrimSpace(sql[len("SELECT "):fromIdx])
	rest := strings.TrimSpace(sql[fromIdx+len(" FROM "):])

	fromPart, wherePart, orderPart, limitPart, offsetPart := splitSelectClauses(rest)
	fromName, alias, joins, ok := parseFromAndJoins(fromPart)
	if !ok {
		return nil, false
	}

	cursor := &bindCursor{binds: binds}
	where, ok := parsePredicates(wherePart, cursor)
	if !ok {
		return nil, false
	}

	stmt := &cuesql.Select{
		From:    fromName,
		Alias:   alias,
		Columns: parseColumns(columnsPart),
		Joins:   joins,
		Where:   where,
	}

	if orderPart != "" {
		stmt.OrderBy = parseOrderBy(orderPart)
	}
	if limitPart != "" {
		if limitPart != "?" {
			return nil, false
		}
		param, ok := cursor.Next()
		if !ok {
			return nil, false
		}
		stmt.LimitParam = param
	}
	if offsetPart != "" {
		if offsetPart != "?" {
			return nil, false
		}
		param, ok := cursor.Next()
		if !ok {
			return nil, false
		}
		stmt.OffsetParam = param
	}
	if !cursor.Done() {
		return nil, false
	}
	return stmt, true
}

func parseInsert(sql string, binds []string) (*cuesql.Insert, bool) {
	upper := strings.ToUpper(sql)
	if !strings.HasPrefix(upper, "INSERT ") || strings.Contains(upper, " ON CONFLICT(") {
		return nil, false
	}

	rest := strings.TrimSpace(sql[len("INSERT "):])
	modifier := ""
	if strings.HasPrefix(strings.ToUpper(rest), "OR IGNORE ") {
		modifier = "OR IGNORE"
		rest = strings.TrimSpace(rest[len("OR IGNORE "):])
	} else if strings.HasPrefix(strings.ToUpper(rest), "OR REPLACE ") {
		modifier = "OR REPLACE"
		rest = strings.TrimSpace(rest[len("OR REPLACE "):])
	}
	if !strings.HasPrefix(strings.ToUpper(rest), "INTO ") {
		return nil, false
	}
	rest = strings.TrimSpace(rest[len("INTO "):])

	open := strings.Index(rest, "(")
	closeIdx := findMatchingParen(rest, open)
	if open <= 0 || closeIdx <= open {
		return nil, false
	}
	into := strings.TrimSpace(rest[:open])
	columns := splitCSV(rest[open+1 : closeIdx])
	remaining := strings.TrimSpace(rest[closeIdx+1:])
	if !strings.HasPrefix(strings.ToUpper(remaining), "VALUES ") {
		return nil, false
	}
	remaining = strings.TrimSpace(remaining[len("VALUES "):])
	valueOpen := strings.Index(remaining, "(")
	valueClose := findMatchingParen(remaining, valueOpen)
	if valueOpen != 0 || valueClose <= valueOpen {
		return nil, false
	}
	valuesPart := remaining[valueOpen+1 : valueClose]
	remaining = strings.TrimSpace(remaining[valueClose+1:])

	cursor := &bindCursor{binds: binds}
	values := make([]cuesql.ValueExpr, 0, len(columns))
	for _, value := range splitCSV(valuesPart) {
		expr, ok := parseValueExpr(value, cursor)
		if !ok {
			return nil, false
		}
		values = append(values, expr)
	}

	stmt := &cuesql.Insert{
		Modifier: modifier,
		Into:     into,
		Columns:  columns,
		Values:   values,
	}
	if remaining != "" {
		if !strings.HasPrefix(strings.ToUpper(remaining), "RETURNING ") {
			return nil, false
		}
		stmt.ReturningColumns = parseColumns(strings.TrimSpace(remaining[len("RETURNING "):]))
	}
	if !cursor.Done() {
		return nil, false
	}
	return stmt, true
}

func parseUpdate(sql string, binds []string) (*cuesql.Update, bool) {
	if !strings.HasPrefix(strings.ToUpper(sql), "UPDATE ") {
		return nil, false
	}
	rest := strings.TrimSpace(sql[len("UPDATE "):])
	setIdx := indexKeyword(rest, " SET ")
	if setIdx < 0 {
		return nil, false
	}

	table := strings.TrimSpace(rest[:setIdx])
	rest = strings.TrimSpace(rest[setIdx+len(" SET "):])

	setPart, wherePart, returningPart := splitUpdateClauses(rest)
	cursor := &bindCursor{binds: binds}

	assignments := make([]cuesql.Assignment, 0, len(splitCSV(setPart)))
	for _, assignment := range splitCSV(setPart) {
		column, value, ok := strings.Cut(assignment, "=")
		if !ok {
			return nil, false
		}
		expr, ok := parseValueExpr(strings.TrimSpace(value), cursor)
		if !ok {
			return nil, false
		}
		assignments = append(assignments, cuesql.Assignment{
			Column: strings.TrimSpace(column),
			Value:  expr,
		})
	}

	where, ok := parsePredicates(wherePart, cursor)
	if !ok {
		return nil, false
	}

	stmt := &cuesql.Update{
		Table: table,
		Set:   assignments,
		Where: where,
	}
	if returningPart != "" {
		stmt.ReturningColumns = parseColumns(returningPart)
	}
	if !cursor.Done() {
		return nil, false
	}
	return stmt, true
}

func parseDelete(sql string, binds []string) (*cuesql.Delete, bool) {
	if !strings.HasPrefix(strings.ToUpper(sql), "DELETE FROM ") {
		return nil, false
	}
	rest := strings.TrimSpace(sql[len("DELETE FROM "):])
	whereIdx := indexKeyword(rest, " WHERE ")
	from := rest
	wherePart := ""
	if whereIdx >= 0 {
		from = strings.TrimSpace(rest[:whereIdx])
		wherePart = strings.TrimSpace(rest[whereIdx+len(" WHERE "):])
	}
	cursor := &bindCursor{binds: binds}
	where, ok := parsePredicates(wherePart, cursor)
	if !ok || !cursor.Done() {
		return nil, false
	}
	return &cuesql.Delete{From: from, Where: where}, true
}

func parseValueExpr(raw string, cursor *bindCursor) (cuesql.ValueExpr, bool) {
	value := strings.TrimSpace(raw)
	if strings.Count(value, "?") > 1 {
		return cuesql.ValueExpr{}, false
	}
	if value == "?" {
		param, ok := cursor.Next()
		if !ok {
			return cuesql.ValueExpr{}, false
		}
		return cuesql.ValueExpr{Param: param}, true
	}
	if strings.Contains(value, "?") {
		return cuesql.ValueExpr{}, false
	}
	return cuesql.ValueExpr{SQL: value}, true
}

func parsePredicates(wherePart string, cursor *bindCursor) ([]cuesql.Predicate, bool) {
	if strings.TrimSpace(wherePart) == "" {
		return nil, true
	}
	parts := splitByKeyword(wherePart, " AND ")
	predicates := make([]cuesql.Predicate, 0, len(parts))
	for _, part := range parts {
		predicate, ok := parsePredicate(part, cursor)
		if !ok {
			return nil, false
		}
		predicates = append(predicates, predicate)
	}
	return predicates, true
}

func parsePredicate(raw string, cursor *bindCursor) (cuesql.Predicate, bool) {
	part := strings.TrimSpace(raw)
	if part == "" {
		return cuesql.Predicate{}, false
	}
	if strings.Contains(strings.ToUpper(part), " OR ") {
		if strings.Contains(part, "?") {
			return cuesql.Predicate{}, false
		}
		return cuesql.Predicate{RawSQL: part}, true
	}

	if matches := regexp.MustCompile(`(?i)^(.+?)\s+IN\s+\((.+)\)$`).FindStringSubmatch(part); len(matches) == 3 {
		inner := strings.TrimSpace(matches[2])
		if inner != "?" {
			if strings.Contains(inner, "?") {
				return cuesql.Predicate{}, false
			}
			return cuesql.Predicate{RawSQL: part}, true
		}
		param, ok := cursor.Next()
		if !ok {
			return cuesql.Predicate{}, false
		}
		return cuesql.Predicate{
			Column: strings.TrimSpace(matches[1]),
			Op:     "IN",
			Param:  param,
			Slice:  true,
		}, true
	}

	for _, op := range []string{" IS NOT ", " IS ", " >= ", " <= ", " <> ", " != ", " = ", " > ", " < ", " LIKE "} {
		idx := indexKeyword(part, op)
		if idx < 0 {
			continue
		}
		lhs := strings.TrimSpace(part[:idx])
		rhs := strings.TrimSpace(part[idx+len(op):])
		operator := strings.TrimSpace(op)
		if rhs == "?" {
			param, ok := cursor.Next()
			if !ok {
				return cuesql.Predicate{}, false
			}
			return cuesql.Predicate{Column: lhs, Op: operator, Param: param}, true
		}
		if strings.Contains(rhs, "?") {
			return cuesql.Predicate{}, false
		}
		return cuesql.Predicate{Column: lhs, Op: operator, ValueSQL: rhs}, true
	}

	if strings.Contains(part, "?") {
		return cuesql.Predicate{}, false
	}
	return cuesql.Predicate{RawSQL: part}, true
}

func parseFromAndJoins(raw string) (string, string, []cuesql.Join, bool) {
	part := strings.TrimSpace(raw)
	segments := splitJoinSegments(part)
	if len(segments) == 0 {
		return "", "", nil, false
	}
	baseTokens := strings.Fields(segments[0])
	if len(baseTokens) == 0 || len(baseTokens) > 2 {
		return "", "", nil, false
	}
	from := baseTokens[0]
	alias := ""
	if len(baseTokens) == 2 {
		alias = baseTokens[1]
	}
	joins := make([]cuesql.Join, 0, len(segments)-1)
	for _, segment := range segments[1:] {
		matches := joinKeywordRe.FindStringSubmatch(strings.TrimSpace(segment))
		if len(matches) != 5 {
			return "", "", nil, false
		}
		joins = append(joins, cuesql.Join{
			Type:  strings.ToUpper(strings.TrimSpace(matches[1])),
			Table: matches[2],
			Alias: matches[3],
			On:    strings.TrimSpace(matches[4]),
		})
	}
	return from, alias, joins, true
}

func splitJoinSegments(raw string) []string {
	var segments []string
	start := 0
	for {
		idx, _ := nextJoinKeyword(raw, start)
		if idx < 0 {
			segments = append(segments, strings.TrimSpace(raw[start:]))
			break
		}
		if idx > start {
			segments = append(segments, strings.TrimSpace(raw[start:idx]))
		}
		start = idx
		nextIdx, _ := nextJoinKeyword(raw, start+1)
		if nextIdx < 0 {
			segments = append(segments, strings.TrimSpace(raw[start:]))
			break
		}
		segments = append(segments, strings.TrimSpace(raw[start:nextIdx]))
		start = nextIdx
	}
	cleaned := segments[:0]
	for _, segment := range segments {
		if segment != "" {
			cleaned = append(cleaned, segment)
		}
	}
	return cleaned
}

func nextJoinKeyword(sql string, start int) (int, string) {
	keywords := []string{" LEFT OUTER JOIN ", " LEFT JOIN ", " INNER JOIN ", " JOIN "}
	bestIdx := -1
	bestKeyword := ""
	for _, keyword := range keywords {
		idx := indexKeywordFrom(sql, keyword, start)
		if idx >= 0 && (bestIdx < 0 || idx < bestIdx) {
			bestIdx = idx
			bestKeyword = keyword
		}
	}
	return bestIdx, bestKeyword
}

func splitSelectClauses(rest string) (string, string, string, string, string) {
	fromPart := rest
	wherePart := ""
	orderPart := ""
	limitPart := ""
	offsetPart := ""

	cutAt := len(rest)
	for _, keyword := range []string{" WHERE ", " ORDER BY ", " LIMIT ", " OFFSET "} {
		if idx := indexKeyword(rest, keyword); idx >= 0 && idx < cutAt {
			cutAt = idx
		}
	}
	fromPart = strings.TrimSpace(rest[:cutAt])
	remaining := strings.TrimSpace(rest[cutAt:])

	if strings.HasPrefix(strings.ToUpper(remaining), "WHERE ") {
		remaining = strings.TrimSpace(remaining[len("WHERE "):])
		next := len(remaining)
		for _, keyword := range []string{" ORDER BY ", " LIMIT ", " OFFSET "} {
			if idx := indexKeyword(remaining, keyword); idx >= 0 && idx < next {
				next = idx
			}
		}
		wherePart = strings.TrimSpace(remaining[:next])
		remaining = strings.TrimSpace(remaining[next:])
	}
	if strings.HasPrefix(strings.ToUpper(remaining), "ORDER BY ") {
		remaining = strings.TrimSpace(remaining[len("ORDER BY "):])
		next := len(remaining)
		for _, keyword := range []string{" LIMIT ", " OFFSET "} {
			if idx := indexKeyword(remaining, keyword); idx >= 0 && idx < next {
				next = idx
			}
		}
		orderPart = strings.TrimSpace(remaining[:next])
		remaining = strings.TrimSpace(remaining[next:])
	}
	if strings.HasPrefix(strings.ToUpper(remaining), "LIMIT ") {
		remaining = strings.TrimSpace(remaining[len("LIMIT "):])
		next := len(remaining)
		if idx := indexKeyword(remaining, " OFFSET "); idx >= 0 {
			next = idx
		}
		limitPart = strings.TrimSpace(remaining[:next])
		remaining = strings.TrimSpace(remaining[next:])
	}
	if strings.HasPrefix(strings.ToUpper(remaining), "OFFSET ") {
		offsetPart = strings.TrimSpace(remaining[len("OFFSET "):])
	}

	return fromPart, wherePart, orderPart, limitPart, offsetPart
}

func splitUpdateClauses(rest string) (string, string, string) {
	setPart := rest
	wherePart := ""
	returningPart := ""

	cutAt := len(rest)
	for _, keyword := range []string{" WHERE ", " RETURNING "} {
		if idx := indexKeyword(rest, keyword); idx >= 0 && idx < cutAt {
			cutAt = idx
		}
	}
	setPart = strings.TrimSpace(rest[:cutAt])
	remaining := strings.TrimSpace(rest[cutAt:])

	if strings.HasPrefix(strings.ToUpper(remaining), "WHERE ") {
		remaining = strings.TrimSpace(remaining[len("WHERE "):])
		next := len(remaining)
		if idx := indexKeyword(remaining, " RETURNING "); idx >= 0 {
			next = idx
		}
		wherePart = strings.TrimSpace(remaining[:next])
		remaining = strings.TrimSpace(remaining[next:])
	}
	if strings.HasPrefix(strings.ToUpper(remaining), "RETURNING ") {
		returningPart = strings.TrimSpace(remaining[len("RETURNING "):])
	}

	return setPart, wherePart, returningPart
}

func parseOrderBy(raw string) []cuesql.OrderBy {
	items := splitCSV(raw)
	orderBy := make([]cuesql.OrderBy, 0, len(items))
	for _, item := range items {
		part := strings.TrimSpace(item)
		desc := false
		if strings.HasSuffix(strings.ToUpper(part), " DESC") {
			desc = true
			part = strings.TrimSpace(part[:len(part)-len(" DESC")])
		} else if strings.HasSuffix(strings.ToUpper(part), " ASC") {
			part = strings.TrimSpace(part[:len(part)-len(" ASC")])
		}
		orderBy = append(orderBy, cuesql.OrderBy{Expr: part, Desc: desc})
	}
	return orderBy
}

func parseColumns(raw string) []cuesql.Column {
	parts := splitCSV(raw)
	columns := make([]cuesql.Column, 0, len(parts))
	for _, part := range parts {
		item := strings.TrimSpace(part)
		alias := ""
		if idx := indexKeyword(item, " AS "); idx >= 0 {
			alias = strings.TrimSpace(item[idx+len(" AS "):])
			item = strings.TrimSpace(item[:idx])
		}
		columns = append(columns, cuesql.Column{Expr: item, Alias: alias})
	}
	return columns
}

func splitCSV(raw string) []string {
	return splitByDelimiter(raw, ',')
}

func splitByKeyword(raw, keyword string) []string {
	var parts []string
	start := 0
	for {
		idx := indexKeywordFrom(raw, keyword, start)
		if idx < 0 {
			parts = append(parts, strings.TrimSpace(raw[start:]))
			break
		}
		parts = append(parts, strings.TrimSpace(raw[start:idx]))
		start = idx + len(keyword)
	}
	cleaned := parts[:0]
	for _, part := range parts {
		if part != "" {
			cleaned = append(cleaned, part)
		}
	}
	return cleaned
}

func splitByDelimiter(raw string, delimiter rune) []string {
	parts := make([]string, 0, 4)
	start := 0
	depth := 0
	inQuote := false
	for i, r := range raw {
		switch {
		case r == '\'':
			inQuote = !inQuote
		case !inQuote && r == '(':
			depth++
		case !inQuote && r == ')' && depth > 0:
			depth--
		case !inQuote && depth == 0 && r == delimiter:
			parts = append(parts, strings.TrimSpace(raw[start:i]))
			start = i + 1
		}
	}
	parts = append(parts, strings.TrimSpace(raw[start:]))
	return parts
}

func indexKeyword(sql, keyword string) int {
	return indexKeywordFrom(sql, keyword, 0)
}

func indexKeywordFrom(sql, keyword string, start int) int {
	depth := 0
	inQuote := false
	upperSQL := strings.ToUpper(sql)
	upperKeyword := strings.ToUpper(keyword)
	for i := start; i <= len(sql)-len(keyword); i++ {
		switch sql[i] {
		case '\'':
			inQuote = !inQuote
		case '(':
			if !inQuote {
				depth++
			}
		case ')':
			if !inQuote && depth > 0 {
				depth--
			}
		}
		if inQuote || depth > 0 {
			continue
		}
		if strings.HasPrefix(upperSQL[i:], upperKeyword) {
			return i
		}
	}
	return -1
}

func findMatchingParen(sql string, open int) int {
	if open < 0 || open >= len(sql) || sql[open] != '(' {
		return -1
	}
	depth := 0
	inQuote := false
	for i := open; i < len(sql); i++ {
		switch sql[i] {
		case '\'':
			inQuote = !inQuote
		case '(':
			if !inQuote {
				depth++
			}
		case ')':
			if !inQuote {
				depth--
				if depth == 0 {
					return i
				}
			}
		}
	}
	return -1
}

func normalizeSQL(raw string) string {
	lines := strings.Split(raw, "\n")
	kept := make([]string, 0, len(lines))
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || strings.HasPrefix(trimmed, "--") {
			continue
		}
		kept = append(kept, trimmed)
	}
	return compactWhitespace(strings.Join(kept, " "))
}

func compactWhitespace(raw string) string {
	var b strings.Builder
	inQuote := false
	spacePending := false
	for _, r := range raw {
		switch {
		case r == '\'':
			if spacePending && b.Len() > 0 {
				b.WriteByte(' ')
				spacePending = false
			}
			inQuote = !inQuote
			b.WriteRune(r)
		case inQuote:
			b.WriteRune(r)
		case r == ' ' || r == '\t' || r == '\n' || r == '\r':
			spacePending = true
		default:
			if spacePending && b.Len() > 0 {
				b.WriteByte(' ')
			}
			spacePending = false
			b.WriteRune(r)
		}
	}
	return strings.TrimSpace(strings.TrimSuffix(b.String(), ";"))
}

type bindCursor struct {
	binds []string
	index int
}

func (b *bindCursor) Next() (string, bool) {
	if b.index >= len(b.binds) {
		return "", false
	}
	value := b.binds[b.index]
	b.index++
	return value, true
}

func (b *bindCursor) Done() bool {
	return b.index == len(b.binds)
}
