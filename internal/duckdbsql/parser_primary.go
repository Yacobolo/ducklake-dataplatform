package duckdbsql

import (
	"fmt"
	"strings"

	"duck-demo/internal/duckdbsql/catalog"
)

// Primary expression parsing: literals, column refs, function calls, CASE, CAST,
// DuckDB extensions (list, struct, COLUMNS, INTERVAL, EXISTS).

// parsePrimary parses a primary expression.
func (p *Parser) parsePrimary() Expr {
	switch p.token.Type {
	case TOKEN_NUMBER:
		lit := &Literal{Type: LiteralNumber, Value: p.token.Literal}
		p.nextToken()
		return lit

	case TOKEN_STRING:
		lit := &Literal{Type: LiteralString, Value: p.token.Literal}
		p.nextToken()
		return lit

	case TOKEN_TRUE:
		p.nextToken()
		return &Literal{Type: LiteralBool, Value: "true"}

	case TOKEN_FALSE:
		p.nextToken()
		return &Literal{Type: LiteralBool, Value: "false"}

	case TOKEN_NULL:
		p.nextToken()
		return &Literal{Type: LiteralNull, Value: "NULL"}

	case TOKEN_CASE:
		return p.parseCaseExpr()

	case TOKEN_CAST:
		return p.parseCastExpr()

	case TOKEN_TRY_CAST:
		return p.parseTryCastExpr()

	case TOKEN_EXTRACT:
		return p.parseExtractExpr()

	case TOKEN_NOT:
		if p.checkPeek(TOKEN_EXISTS) {
			p.nextToken() // consume NOT
			return p.parseExistsExpr(true)
		}
		p.nextToken()
		return &UnaryExpr{Op: TOKEN_NOT, Expr: p.parsePrimary()}

	case TOKEN_EXISTS:
		return p.parseExistsExpr(false)

	case TOKEN_IDENT:
		return p.parseIdentifierExpr()

	case TOKEN_LPAREN:
		return p.parseParenExpr()

	case TOKEN_STAR:
		p.nextToken()
		star := &StarExpr{}
		star.Modifiers = p.parseStarModifiers()
		return star

	// DuckDB extensions
	case TOKEN_LBRACKET:
		return p.parseListLiteral()

	case TOKEN_LBRACE:
		return p.parseStructLiteral()

	case TOKEN_INTERVAL:
		return p.parseIntervalExpr()

	case TOKEN_COLUMNS:
		return p.parseColumnsExpr()

	default:
		// Try to handle keywords that can be used as identifiers in some contexts
		// (e.g., function names like REPLACE, RENAME)
		if p.token.Type > TOKEN_STRING && p.token.Type < TOKEN_ANTI {
			// This is a keyword, but it might be used as a function name
			if p.checkPeek(TOKEN_LPAREN) {
				return p.parseIdentifierExpr()
			}
		}
		p.addError(fmt.Sprintf("unexpected token in expression: %s (%q)", p.token.Type, p.token.Literal))
		p.nextToken()
		return nil
	}
}

// parseIdentifierExpr parses an identifier (column ref or function call).
func (p *Parser) parseIdentifierExpr() Expr {
	name := p.token.Literal
	p.nextToken()

	// Function call: name(...)
	if p.check(TOKEN_LPAREN) {
		return p.parseFuncCall(name, "")
	}

	// Qualified name: schema.table.column or table.column
	if p.check(TOKEN_DOT) {
		return p.parseQualifiedRef(name)
	}

	// Simple column reference
	return &ColumnRef{Column: name}
}

// parseQualifiedRef parses a qualified name (table.column, schema.table.column, or table.*).
func (p *Parser) parseQualifiedRef(firstPart string) Expr {
	parts := []string{firstPart}

	for p.match(TOKEN_DOT) {
		// table.* or table.column.*
		if p.check(TOKEN_STAR) {
			p.nextToken()
			star := &StarExpr{Table: firstPart}
			star.Modifiers = p.parseStarModifiers()
			return star
		}

		if p.check(TOKEN_IDENT) {
			parts = append(parts, p.token.Literal)
			p.nextToken()
		} else {
			break
		}
	}

	// Check for function call on qualified name: schema.func(...)
	if p.check(TOKEN_LPAREN) && len(parts) == 2 {
		return p.parseFuncCall(parts[1], parts[0])
	}

	ref := &ColumnRef{}
	switch len(parts) {
	case 2:
		ref.Table = parts[0]
		ref.Column = parts[1]
	case 3:
		// schema.table.column → use table.column
		ref.Table = parts[1]
		ref.Column = parts[2]
	default:
		ref.Column = parts[len(parts)-1]
	}
	return ref
}

// parseFuncCall parses a function call: name([DISTINCT] args [ORDER BY ...]) [FILTER ...] [OVER ...]
func (p *Parser) parseFuncCall(name string, schema string) Expr {
	fn := &FuncCall{Name: name, Schema: schema}

	p.expect(TOKEN_LPAREN)

	// COUNT(*)
	if p.check(TOKEN_STAR) {
		fn.Star = true
		p.nextToken()
	} else if !p.check(TOKEN_RPAREN) {
		if p.match(TOKEN_DISTINCT) {
			fn.Distinct = true
		}

		// Parse arguments
		for {
			arg := p.parseExpression()
			if arg != nil {
				fn.Args = append(fn.Args, arg)
			}
			if !p.match(TOKEN_COMMA) {
				break
			}
		}

		// ORDER BY within aggregate (e.g., array_agg(x ORDER BY y))
		if p.check(TOKEN_ORDER) {
			p.nextToken() // consume ORDER
			p.expect(TOKEN_BY)
			fn.OrderBy = p.parseOrderByList()
		}
	}

	p.expect(TOKEN_RPAREN)

	// FILTER clause
	if p.match(TOKEN_FILTER) {
		p.expect(TOKEN_LPAREN)
		p.expect(TOKEN_WHERE)
		fn.Filter = p.parseExpression()
		p.expect(TOKEN_RPAREN)
	}

	// OVER clause (window function)
	if p.match(TOKEN_OVER) {
		fn.Window = p.parseWindowSpec()
	}

	return fn
}

// parseWindowSpec parses a window specification.
func (p *Parser) parseWindowSpec() *WindowSpec {
	spec := &WindowSpec{}

	// Named window reference
	if p.check(TOKEN_IDENT) && !p.checkPeek(TOKEN_DOT) {
		spec.Name = p.token.Literal
		p.nextToken()
		return spec
	}

	p.expect(TOKEN_LPAREN)

	if p.match(TOKEN_PARTITION) {
		p.expect(TOKEN_BY)
		spec.PartitionBy = p.parseExpressionList()
	}

	if p.check(TOKEN_ORDER) {
		p.nextToken()
		p.expect(TOKEN_BY)
		spec.OrderBy = p.parseOrderByList()
	}

	if p.check(TOKEN_ROWS) || p.check(TOKEN_RANGE) || p.check(TOKEN_GROUPS) {
		spec.Frame = p.parseFrameSpec()
	}

	p.expect(TOKEN_RPAREN)
	return spec
}

// parseFrameSpec parses a window frame specification.
func (p *Parser) parseFrameSpec() *FrameSpec {
	frame := &FrameSpec{}

	switch {
	case p.match(TOKEN_ROWS):
		frame.Type = FrameRows
	case p.match(TOKEN_RANGE):
		frame.Type = FrameRange
	case p.match(TOKEN_GROUPS):
		frame.Type = FrameGroups
	}

	if p.match(TOKEN_BETWEEN) {
		frame.Start = p.parseFrameBound()
		p.expect(TOKEN_AND)
		frame.End = p.parseFrameBound()
	} else {
		frame.Start = p.parseFrameBound()
	}

	return frame
}

// parseFrameBound parses a frame bound.
func (p *Parser) parseFrameBound() *FrameBound {
	bound := &FrameBound{}

	switch {
	case p.match(TOKEN_UNBOUNDED):
		if p.match(TOKEN_PRECEDING) {
			bound.Type = FrameUnboundedPreceding
		} else if p.match(TOKEN_FOLLOWING) {
			bound.Type = FrameUnboundedFollowing
		}
	case p.match(TOKEN_CURRENT):
		p.expect(TOKEN_ROW)
		bound.Type = FrameCurrentRow
	default:
		bound.Offset = p.parseExpression()
		if p.match(TOKEN_PRECEDING) {
			bound.Type = FrameExprPreceding
		} else if p.match(TOKEN_FOLLOWING) {
			bound.Type = FrameExprFollowing
		}
	}

	return bound
}

// parseCaseExpr parses a CASE expression.
func (p *Parser) parseCaseExpr() Expr {
	p.expect(TOKEN_CASE)
	caseExpr := &CaseExpr{}

	if !p.check(TOKEN_WHEN) {
		caseExpr.Operand = p.parseExpression()
	}

	for p.match(TOKEN_WHEN) {
		when := WhenClause{}
		when.Condition = p.parseExpression()
		p.expect(TOKEN_THEN)
		when.Result = p.parseExpression()
		caseExpr.Whens = append(caseExpr.Whens, when)
	}

	if p.match(TOKEN_ELSE) {
		caseExpr.Else = p.parseExpression()
	}

	p.expect(TOKEN_END)
	return caseExpr
}

// parseCastExpr parses CAST(expr AS type).
func (p *Parser) parseCastExpr() Expr {
	p.expect(TOKEN_CAST)
	p.expect(TOKEN_LPAREN)

	cast := &CastExpr{}
	cast.Expr = p.parseExpression()
	p.expect(TOKEN_AS)
	cast.TypeName = p.parseTypeName()

	p.expect(TOKEN_RPAREN)
	return cast
}

// parseTryCastExpr parses TRY_CAST(expr AS type).
func (p *Parser) parseTryCastExpr() Expr {
	p.expect(TOKEN_TRY_CAST)
	p.expect(TOKEN_LPAREN)

	cast := &CastExpr{TryCast: true}
	cast.Expr = p.parseExpression()
	p.expect(TOKEN_AS)
	cast.TypeName = p.parseTypeName()

	p.expect(TOKEN_RPAREN)
	return cast
}

// parseExtractExpr parses EXTRACT(field FROM expr).
func (p *Parser) parseExtractExpr() Expr {
	p.nextToken() // consume EXTRACT
	p.expect(TOKEN_LPAREN)

	// The field name (YEAR, MONTH, DAY, HOUR, MINUTE, SECOND, EPOCH, etc.)
	field := strings.ToUpper(p.token.Literal)
	p.nextToken() // consume field name

	p.expect(TOKEN_FROM)
	expr := p.parseExpression()
	p.expect(TOKEN_RPAREN)

	return &ExtractExpr{Field: field, Expr: expr}
}

// parseTypeName parses a type name with optional parameters.
func (p *Parser) parseTypeName() string {
	// Accept both identifiers and some keywords as type names
	// (e.g., INTEGER, VARCHAR, DOUBLE, BOOLEAN, DATE, TIME, TIMESTAMP, etc.)
	var typeName string
	if p.check(TOKEN_IDENT) || p.isTypeLikeKeyword() {
		typeName = strings.ToUpper(p.token.Literal)
		p.nextToken()
	} else {
		p.addError("expected type name")
		return ""
	}

	// Handle compound type names like DOUBLE PRECISION, TIMESTAMP WITH TIME ZONE
	for p.check(TOKEN_IDENT) || p.isTypeLikeKeyword() {
		// Only continue for known compound type patterns
		upper := strings.ToUpper(p.token.Literal)
		if upper == "PRECISION" || upper == "VARYING" || upper == "ZONE" ||
			upper == "WITHOUT" || upper == "WITH" || upper == "TIME" {
			typeName += " " + upper
			p.nextToken()
		} else {
			break
		}
	}

	// Type parameters like VARCHAR(255) or DECIMAL(10, 2) or STRUCT(...)
	if p.match(TOKEN_LPAREN) {
		typeName += "("
		depth := 1
		for depth > 0 && p.token.Type != TOKEN_EOF {
			if p.token.Type == TOKEN_LPAREN {
				depth++
			} else if p.token.Type == TOKEN_RPAREN {
				depth--
				if depth == 0 {
					p.nextToken()
					break
				}
			}
			typeName += p.token.Literal
			p.nextToken()
		}
		typeName += ")"
	}

	// Handle array type: INTEGER[]
	if p.check(TOKEN_LBRACKET) && p.checkPeek(TOKEN_RBRACKET) {
		typeName += "[]"
		p.nextToken() // [
		p.nextToken() // ]
	}

	return typeName
}

// isTypeLikeKeyword returns true if the current token is a keyword that could be a type name.
func (p *Parser) isTypeLikeKeyword() bool {
	// Many SQL type names are also keywords
	switch p.token.Type {
	case TOKEN_DEFAULT, TOKEN_SET, TOKEN_ROW, TOKEN_CURRENT, TOKEN_TRUE, TOKEN_FALSE,
		TOKEN_NULL, TOKEN_ALL, TOKEN_DISTINCT:
		return false // These are not type names
	}
	// Accept most other keywords as potential type names if followed by type-like context
	if p.token.Type > TOKEN_STRING && p.token.Type < TOKEN_ANTI {
		return true
	}
	// Also accept identifiers that are recognized DuckDB type names (e.g., HUGEINT, UINTEGER)
	if p.token.Type == TOKEN_IDENT {
		return catalog.TypeNames[strings.ToUpper(p.token.Literal)]
	}
	return false
}

// parseExistsExpr parses [NOT] EXISTS (subquery).
func (p *Parser) parseExistsExpr(not bool) Expr {
	p.nextToken() // consume EXISTS
	p.expect(TOKEN_LPAREN)
	exists := &ExistsExpr{Not: not, Select: p.parseSelectStatement()}
	p.expect(TOKEN_RPAREN)
	return exists
}

// parseParenExpr parses a parenthesized expression or subquery.
func (p *Parser) parseParenExpr() Expr {
	p.expect(TOKEN_LPAREN)

	// Check if this is a subquery
	if p.check(TOKEN_SELECT) || p.check(TOKEN_WITH) {
		subquery := &SubqueryExpr{Select: p.parseSelectStatement()}
		p.expect(TOKEN_RPAREN)
		return subquery
	}

	expr := p.parseExpression()

	// Comma-separated list (for lambda parameters like (x, y) -> ...)
	if p.check(TOKEN_COMMA) {
		for p.match(TOKEN_COMMA) {
			right := p.parseExpression()
			expr = &BinaryExpr{Left: expr, Op: TOKEN_COMMA, Right: right}
		}
	}

	p.expect(TOKEN_RPAREN)
	return &ParenExpr{Expr: expr}
}

// parseOrderByList parses a list of ORDER BY items.
func (p *Parser) parseOrderByList() []OrderByItem {
	var items []OrderByItem
	for {
		item := p.parseOrderByItem()
		items = append(items, item)
		if !p.match(TOKEN_COMMA) {
			break
		}
	}
	return items
}

// parseOrderByItem parses a single ORDER BY item.
func (p *Parser) parseOrderByItem() OrderByItem {
	item := OrderByItem{}
	item.Expr = p.parseExpression()

	if p.match(TOKEN_ASC) {
		item.Desc = false
	} else if p.match(TOKEN_DESC) {
		item.Desc = true
	}

	if p.match(TOKEN_NULLS) {
		if p.match(TOKEN_FIRST) {
			b := true
			item.NullsFirst = &b
		} else if p.match(TOKEN_LAST) {
			b := false
			item.NullsFirst = &b
		}
	}

	return item
}

// === DuckDB Extension Primary Expressions ===

// parseListLiteral parses [expr, expr, ...].
func (p *Parser) parseListLiteral() Expr {
	p.nextToken() // consume [
	list := &ListLiteral{}

	if !p.check(TOKEN_RBRACKET) {
		for {
			elem := p.parseExpression()
			if elem != nil {
				list.Elements = append(list.Elements, elem)
			}
			if !p.match(TOKEN_COMMA) {
				break
			}
		}
	}

	p.expect(TOKEN_RBRACKET)
	return list
}

// parseStructLiteral parses {'key': value, ...}.
func (p *Parser) parseStructLiteral() Expr {
	p.nextToken() // consume {
	s := &StructLiteral{}

	if !p.check(TOKEN_RBRACE) {
		for {
			var key string
			switch p.token.Type {
			case TOKEN_IDENT:
				key = p.token.Literal
				p.nextToken()
			case TOKEN_STRING:
				key = p.token.Literal
				p.nextToken()
			default:
				p.addError(fmt.Sprintf("struct literal: expected key, got %s", p.token.Type))
				p.nextToken()
				return s
			}

			p.expect(TOKEN_COLON)
			value := p.parseExpression()

			s.Fields = append(s.Fields, StructField{Key: key, Value: value})

			if !p.match(TOKEN_COMMA) {
				break
			}
		}
	}

	p.expect(TOKEN_RBRACE)
	return s
}

// parseIntervalExpr parses INTERVAL 'value' [unit].
// DuckDB supports: INTERVAL 'value' unit, INTERVAL 'value', INTERVAL expr.
func (p *Parser) parseIntervalExpr() Expr {
	p.nextToken() // consume INTERVAL

	iv := &IntervalExpr{}
	iv.Value = p.parsePrimary()

	// Optional unit: DAY, HOUR, MINUTE, SECOND, MONTH, YEAR, WEEK, MILLISECOND, MICROSECOND
	if p.check(TOKEN_IDENT) {
		upper := strings.ToUpper(p.token.Literal)
		switch upper {
		case "YEAR", "YEARS", "MONTH", "MONTHS", "DAY", "DAYS",
			"HOUR", "HOURS", "MINUTE", "MINUTES", "SECOND", "SECONDS",
			"WEEK", "WEEKS", "MILLISECOND", "MILLISECONDS",
			"MICROSECOND", "MICROSECONDS":
			iv.Unit = upper
			p.nextToken()
		}
	}

	return iv
}

// parseColumnsExpr parses DuckDB COLUMNS(pattern).
func (p *Parser) parseColumnsExpr() Expr {
	p.nextToken() // consume COLUMNS
	p.expect(TOKEN_LPAREN)
	pattern := p.parseExpression()
	p.expect(TOKEN_RPAREN)
	return &ColumnsExpr{Pattern: pattern}
}

// parseStarModifiers parses optional EXCLUDE/REPLACE/RENAME modifiers after *.
func (p *Parser) parseStarModifiers() []StarModifier {
	var modifiers []StarModifier

	for {
		switch p.token.Type {
		case TOKEN_EXCLUDE:
			p.nextToken()
			mod := p.parseExcludeModifier()
			if mod != nil {
				modifiers = append(modifiers, mod)
			}
		case TOKEN_REPLACE:
			p.nextToken()
			mod := p.parseReplaceModifier()
			if mod != nil {
				modifiers = append(modifiers, mod)
			}
		case TOKEN_RENAME:
			p.nextToken()
			mod := p.parseRenameModifier()
			if mod != nil {
				modifiers = append(modifiers, mod)
			}
		default:
			return modifiers
		}
	}
}

// parseExcludeModifier parses EXCLUDE (col1, col2, ...).
func (p *Parser) parseExcludeModifier() StarModifier {
	p.expect(TOKEN_LPAREN)
	var cols []string
	for {
		if !p.check(TOKEN_IDENT) {
			p.addError("expected column name in EXCLUDE")
			break
		}
		cols = append(cols, p.token.Literal)
		p.nextToken()
		if !p.match(TOKEN_COMMA) {
			break
		}
	}
	p.expect(TOKEN_RPAREN)
	return &ExcludeModifier{Columns: cols}
}

// parseReplaceModifier parses REPLACE (expr AS col, ...).
func (p *Parser) parseReplaceModifier() StarModifier {
	p.expect(TOKEN_LPAREN)
	var items []ReplaceItem
	for {
		expr := p.parseExpression()
		p.expect(TOKEN_AS)
		var name string
		if p.check(TOKEN_IDENT) {
			name = p.token.Literal
			p.nextToken()
		} else {
			p.addError("expected column name in REPLACE")
		}
		items = append(items, ReplaceItem{Expr: expr, Alias: name})
		if !p.match(TOKEN_COMMA) {
			break
		}
	}
	p.expect(TOKEN_RPAREN)
	return &ReplaceModifier{Items: items}
}

// parseRenameModifier parses RENAME (old AS new, ...).
func (p *Parser) parseRenameModifier() StarModifier {
	p.expect(TOKEN_LPAREN)
	var items []RenameItem
	for {
		var oldName string
		if p.check(TOKEN_IDENT) {
			oldName = p.token.Literal
			p.nextToken()
		} else {
			p.addError("expected column name in RENAME")
			break
		}
		p.expect(TOKEN_AS)
		var newName string
		if p.check(TOKEN_IDENT) {
			newName = p.token.Literal
			p.nextToken()
		} else {
			p.addError("expected new column name in RENAME")
		}
		items = append(items, RenameItem{OldName: oldName, NewName: newName})
		if !p.match(TOKEN_COMMA) {
			break
		}
	}
	p.expect(TOKEN_RPAREN)
	return &RenameModifier{Items: items}
}
