package duckdbsql

import (
	"fmt"
	"strings"
)

// FROM clause parsing: table references, derived tables, function tables,
// JOINs (all types including DuckDB-specific), PIVOT/UNPIVOT.

// parseFromClause parses the FROM clause.
func (p *Parser) parseFromClause() *FromClause {
	from := &FromClause{}
	from.Source = p.parseTableRef()

	// Check for PIVOT/UNPIVOT after source
	from.Source = p.parseFromItemExtensions(from.Source)

	// Parse JOINs
	for {
		join := p.parseJoin()
		if join == nil {
			break
		}
		from.Joins = append(from.Joins, join)
	}

	return from
}

// parseTableRef parses a table reference in FROM.
func (p *Parser) parseTableRef() TableRef {
	// LATERAL subquery
	if p.match(TOKEN_LATERAL) {
		return p.parseLateralTable()
	}

	// Derived table (subquery)
	if p.check(TOKEN_LPAREN) {
		return p.parseDerivedTable()
	}

	// String literal as table source (e.g., 'file.csv')
	if p.check(TOKEN_STRING) {
		return p.parseStringTable()
	}

	// Table name or function call
	return p.parseTableNameOrFunc()
}

// parseTableNameOrFunc parses a table name or table-valued function.
func (p *Parser) parseTableNameOrFunc() TableRef {
	// Accept identifiers and keywords that could be function/table names.
	// Many DuckDB built-in functions (range, generate_series, unnest, etc.)
	// are also keywords, so we accept any keyword token here too.
	if !p.check(TOKEN_IDENT) && !p.isTableNameKeyword() {
		p.addError(fmt.Sprintf("expected table name, got %s", p.token.Type))
		return &TableName{}
	}

	// Parse potentially qualified name: catalog.schema.table or schema.func(...)
	parts := []string{p.token.Literal}
	p.nextToken()

	for p.match(TOKEN_DOT) {
		if p.check(TOKEN_IDENT) {
			parts = append(parts, p.token.Literal)
			p.nextToken()
		}
	}

	// Check if it's a function call (table-valued function)
	if p.check(TOKEN_LPAREN) {
		fn := p.parseFuncCall(parts[len(parts)-1], "")
		funcCall, ok := fn.(*FuncCall)
		if !ok {
			p.addError("expected function call")
			return &TableName{Name: parts[len(parts)-1]}
		}
		if len(parts) > 1 {
			funcCall.Schema = parts[0]
		}

		ft := &FuncTable{Func: funcCall}

		// Optional alias with optional column alias list: AS t(i, j) or t(i, j)
		if p.match(TOKEN_AS) {
			if p.check(TOKEN_IDENT) {
				ft.Alias = p.token.Literal
				p.nextToken()
			}
		} else if p.check(TOKEN_IDENT) && !p.isJoinKeyword(p.token) && !p.isClauseKeyword(p.token) {
			ft.Alias = p.token.Literal
			p.nextToken()
		}
		// Column alias list: t(col1, col2, ...)
		if ft.Alias != "" && p.match(TOKEN_LPAREN) {
			ft.ColumnAliases = p.parseColumnAliasList()
			p.expect(TOKEN_RPAREN)
		}

		// WITH ORDINALITY
		if p.check(TOKEN_WITH) && !p.isClauseKeyword(p.peek) {
			if p.peek.Type == TOKEN_IDENT && strings.EqualFold(p.peek.Literal, "ORDINALITY") {
				p.nextToken() // consume WITH
				p.nextToken() // consume ORDINALITY
				ft.WithOrdinality = true
			}
		}

		return ft
	}

	// Build TableName
	table := &TableName{}
	switch len(parts) {
	case 1:
		table.Name = parts[0]
	case 2:
		table.Schema = parts[0]
		table.Name = parts[1]
	case 3:
		table.Catalog = parts[0]
		table.Schema = parts[1]
		table.Name = parts[2]
	}

	// Optional alias with optional column alias list
	if p.match(TOKEN_AS) {
		if p.check(TOKEN_IDENT) {
			table.Alias = p.token.Literal
			p.nextToken()
		}
	} else if p.check(TOKEN_IDENT) && !p.isJoinKeyword(p.token) && !p.isClauseKeyword(p.token) && !strings.EqualFold(p.token.Literal, "TABLESAMPLE") {
		table.Alias = p.token.Literal
		p.nextToken()
	}
	// Column alias list: t(col1, col2, ...)
	if table.Alias != "" && p.match(TOKEN_LPAREN) {
		table.ColumnAliases = p.parseColumnAliasList()
		p.expect(TOKEN_RPAREN)
	}

	return table
}

// parseDerivedTable parses a derived table (subquery in FROM).
func (p *Parser) parseDerivedTable() *DerivedTable {
	p.expect(TOKEN_LPAREN)
	derived := &DerivedTable{}
	derived.Select = p.parseSelectStatement()
	p.expect(TOKEN_RPAREN)

	// Alias
	if p.match(TOKEN_AS) {
		if p.check(TOKEN_IDENT) {
			derived.Alias = p.token.Literal
			p.nextToken()
		}
	} else if p.check(TOKEN_IDENT) && !p.isJoinKeyword(p.token) && !p.isClauseKeyword(p.token) {
		derived.Alias = p.token.Literal
		p.nextToken()
	}

	// Column alias list
	if derived.Alias != "" && p.match(TOKEN_LPAREN) {
		derived.ColumnAliases = p.parseColumnAliasList()
		p.expect(TOKEN_RPAREN)
	}

	return derived
}

// parseLateralTable parses a LATERAL subquery.
func (p *Parser) parseLateralTable() *LateralTable {
	p.expect(TOKEN_LPAREN)
	lateral := &LateralTable{}
	lateral.Select = p.parseSelectStatement()
	p.expect(TOKEN_RPAREN)

	if p.match(TOKEN_AS) {
		if p.check(TOKEN_IDENT) {
			lateral.Alias = p.token.Literal
			p.nextToken()
		}
	} else if p.check(TOKEN_IDENT) {
		lateral.Alias = p.token.Literal
		p.nextToken()
	}

	// Column alias list
	if lateral.Alias != "" && p.match(TOKEN_LPAREN) {
		lateral.ColumnAliases = p.parseColumnAliasList()
		p.expect(TOKEN_RPAREN)
	}

	return lateral
}

// parseFromItemExtensions checks for PIVOT/UNPIVOT after a table reference.
func (p *Parser) parseFromItemExtensions(source TableRef) TableRef {
	for {
		switch p.token.Type {
		case TOKEN_PIVOT:
			p.nextToken()
			source = p.parsePivot(source)
		case TOKEN_UNPIVOT:
			p.nextToken()
			source = p.parseUnpivot(source)
		default:
			// TABLESAMPLE soft keyword
			if p.check(TOKEN_IDENT) && strings.EqualFold(p.token.Literal, "TABLESAMPLE") {
				p.nextToken()                                           // consume TABLESAMPLE
				p.parseExpressionWithPrecedence(PrecedenceMultiply + 1) // consume size expr, stop before %
				if !p.match(TOKEN_MOD) {                                // % sign
					p.match(TOKEN_PERCENT)
				}
				p.match(TOKEN_ROWS)
				continue
			}
			return source
		}
	}
}

// parsePivot parses PIVOT (aggregates FOR column IN (values)).
func (p *Parser) parsePivot(source TableRef) TableRef {
	pivot := &PivotTable{Source: source}

	p.expect(TOKEN_LPAREN)

	// Parse aggregates
	for {
		agg := PivotAggregate{}
		expr := p.parseExpression()
		fn, ok := expr.(*FuncCall)
		if !ok {
			p.addError("PIVOT: expected aggregate function")
			break
		}
		agg.Func = fn

		if p.match(TOKEN_AS) {
			if p.check(TOKEN_IDENT) {
				agg.Alias = p.token.Literal
				p.nextToken()
			}
		}

		pivot.Aggregates = append(pivot.Aggregates, agg)

		if p.check(TOKEN_FOR) {
			break
		}
		if !p.match(TOKEN_COMMA) {
			break
		}
	}

	// FOR column
	p.expect(TOKEN_FOR)
	if p.check(TOKEN_IDENT) {
		pivot.ForColumn = p.token.Literal
		p.nextToken()
	}

	// IN (values) or IN *
	p.expect(TOKEN_IN)
	if p.match(TOKEN_STAR) {
		pivot.InStar = true
	} else {
		p.expect(TOKEN_LPAREN)
		for {
			val := PivotInValue{}
			val.Value = p.parseExpression()
			if p.match(TOKEN_AS) {
				if p.check(TOKEN_IDENT) {
					val.Alias = p.token.Literal
					p.nextToken()
				}
			}
			pivot.InValues = append(pivot.InValues, val)
			if !p.match(TOKEN_COMMA) {
				break
			}
		}
		p.expect(TOKEN_RPAREN)
	}

	// GROUP BY inside PIVOT (SQL standard syntax)
	if p.check(TOKEN_GROUP) {
		p.nextToken()
		p.expect(TOKEN_BY)
		pivot.GroupBy = p.parseExpressionList()
	}

	p.expect(TOKEN_RPAREN)

	// Optional alias
	if p.match(TOKEN_AS) {
		if p.check(TOKEN_IDENT) {
			pivot.Alias = p.token.Literal
			p.nextToken()
		}
	} else if p.check(TOKEN_IDENT) && !p.isJoinKeyword(p.token) && !p.isClauseKeyword(p.token) {
		pivot.Alias = p.token.Literal
		p.nextToken()
	}

	return pivot
}

// parseUnpivot parses UNPIVOT (value FOR name IN (columns)).
func (p *Parser) parseUnpivot(source TableRef) TableRef {
	unpivot := &UnpivotTable{Source: source}

	p.expect(TOKEN_LPAREN)

	// Value column(s)
	if p.match(TOKEN_LPAREN) {
		for {
			if p.check(TOKEN_IDENT) {
				unpivot.ValueColumns = append(unpivot.ValueColumns, p.token.Literal)
				p.nextToken()
			}
			if !p.match(TOKEN_COMMA) {
				break
			}
		}
		p.expect(TOKEN_RPAREN)
	} else if p.check(TOKEN_IDENT) {
		unpivot.ValueColumns = []string{p.token.Literal}
		p.nextToken()
	}

	// FOR name_column
	p.expect(TOKEN_FOR)
	if p.check(TOKEN_IDENT) {
		unpivot.NameColumn = p.token.Literal
		p.nextToken()
	}

	// IN (columns)
	p.expect(TOKEN_IN)
	p.expect(TOKEN_LPAREN)

	expectedCols := len(unpivot.ValueColumns)
	for {
		group := UnpivotInGroup{}
		if expectedCols > 1 && p.check(TOKEN_LPAREN) {
			p.nextToken()
			for {
				if p.check(TOKEN_IDENT) {
					group.Columns = append(group.Columns, p.token.Literal)
					p.nextToken()
				}
				if !p.match(TOKEN_COMMA) {
					break
				}
			}
			p.expect(TOKEN_RPAREN)
		} else if p.check(TOKEN_IDENT) {
			group.Columns = []string{p.token.Literal}
			p.nextToken()
		}
		if p.match(TOKEN_AS) {
			if p.check(TOKEN_STRING) {
				group.Alias = p.token.Literal
				p.nextToken()
			} else if p.check(TOKEN_IDENT) {
				group.Alias = p.token.Literal
				p.nextToken()
			}
		}
		unpivot.InColumns = append(unpivot.InColumns, group)
		if !p.match(TOKEN_COMMA) {
			break
		}
	}

	p.expect(TOKEN_RPAREN) // close IN list
	p.expect(TOKEN_RPAREN) // close UNPIVOT

	// Optional alias
	if p.match(TOKEN_AS) {
		if p.check(TOKEN_IDENT) {
			unpivot.Alias = p.token.Literal
			p.nextToken()
		}
	} else if p.check(TOKEN_IDENT) && !p.isJoinKeyword(p.token) && !p.isClauseKeyword(p.token) {
		unpivot.Alias = p.token.Literal
		p.nextToken()
	}

	return unpivot
}

// parseJoin parses a JOIN clause.
func (p *Parser) parseJoin() *Join {
	join := &Join{}

	// Comma join
	if p.match(TOKEN_COMMA) {
		join.Type = JoinComma
		join.Right = p.parseTableRef()
		join.Right = p.parseFromItemExtensions(join.Right)
		return join
	}

	// NATURAL modifier
	if p.match(TOKEN_NATURAL) {
		join.Natural = true
	}

	// Determine join type
	gotJoinType := false
	switch p.token.Type {
	case TOKEN_INNER:
		join.Type = JoinInner
		p.nextToken()
		p.match(TOKEN_OUTER) // optional, usually not used with INNER
		gotJoinType = true
	case TOKEN_LEFT:
		p.nextToken()
		switch {
		case p.match(TOKEN_SEMI):
			join.Type = JoinLeftSemi
		case p.match(TOKEN_ANTI):
			join.Type = JoinLeftAnti
		default:
			join.Type = JoinLeft
			p.match(TOKEN_OUTER)
		}
		gotJoinType = true
	case TOKEN_RIGHT:
		p.nextToken()
		switch {
		case p.match(TOKEN_SEMI):
			join.Type = JoinRightSemi
		case p.match(TOKEN_ANTI):
			join.Type = JoinRightAnti
		default:
			join.Type = JoinRight
			p.match(TOKEN_OUTER)
		}
		gotJoinType = true
	case TOKEN_FULL:
		join.Type = JoinFull
		p.nextToken()
		p.match(TOKEN_OUTER)
		gotJoinType = true
	case TOKEN_CROSS:
		join.Type = JoinCross
		p.nextToken()
		gotJoinType = true
	case TOKEN_SEMI:
		join.Type = JoinSemi
		p.nextToken()
		gotJoinType = true
	case TOKEN_ANTI:
		join.Type = JoinAnti
		p.nextToken()
		gotJoinType = true
	case TOKEN_ASOF:
		p.nextToken() // consume ASOF
		switch p.token.Type {
		case TOKEN_LEFT:
			join.Type = JoinAsOfLeft
			p.nextToken()
			p.match(TOKEN_OUTER)
		case TOKEN_RIGHT:
			join.Type = JoinAsOfRight
			p.nextToken()
			p.match(TOKEN_OUTER)
		default:
			join.Type = JoinAsOf
		}
		gotJoinType = true
	case TOKEN_POSITIONAL:
		join.Type = JoinPositional
		p.nextToken()
		gotJoinType = true
	case TOKEN_JOIN:
		join.Type = JoinInner
		gotJoinType = true
	}

	if !gotJoinType && !join.Natural {
		return nil // no join
	}

	if !p.expect(TOKEN_JOIN) {
		return nil
	}

	join.Right = p.parseTableRef()
	join.Right = p.parseFromItemExtensions(join.Right)
	p.parseJoinCondition(join)
	return join
}

// parseJoinCondition handles ON/USING/NATURAL validation.
func (p *Parser) parseJoinCondition(join *Join) {
	switch {
	case join.Natural:
		// NATURAL JOIN has no condition
	case join.Type == JoinCross || join.Type == JoinComma || join.Type == JoinPositional:
		// No condition needed
	case p.match(TOKEN_ON):
		join.Condition = p.parseExpression()
	case p.match(TOKEN_USING):
		join.Using = p.parseUsingColumns()
	}
}

// parseUsingColumns parses USING (col1, col2, ...).
func (p *Parser) parseUsingColumns() []string {
	p.expect(TOKEN_LPAREN)
	var cols []string
	for {
		if p.check(TOKEN_IDENT) || p.check(TOKEN_STRING) {
			cols = append(cols, p.token.Literal)
			p.nextToken()
		} else {
			p.addError("expected column name in USING clause")
			break
		}
		if !p.match(TOKEN_COMMA) {
			break
		}
	}
	p.expect(TOKEN_RPAREN)
	return cols
}

// parseColumnAliasList parses a parenthesized list of column aliases: (col1, col2, ...).
// The opening paren has already been consumed.
func (p *Parser) parseColumnAliasList() []string {
	var aliases []string
	for p.check(TOKEN_IDENT) {
		aliases = append(aliases, p.token.Literal)
		p.nextToken()
		if !p.match(TOKEN_COMMA) {
			break
		}
	}
	return aliases
}

func (p *Parser) parseStringTable() *StringTable {
	st := &StringTable{Path: p.token.Literal}
	p.nextToken()
	if p.match(TOKEN_AS) {
		if p.check(TOKEN_IDENT) {
			st.Alias = p.token.Literal
			p.nextToken()
		}
	} else if p.check(TOKEN_IDENT) && !p.isJoinKeyword(p.token) && !p.isClauseKeyword(p.token) {
		st.Alias = p.token.Literal
		p.nextToken()
	}
	return st
}
