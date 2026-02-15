package duckdbsql

import "strings"

// Statement parsing: SELECT, INSERT, UPDATE, DELETE, DDL, utility statements.

// parseSelectStatement parses a complete SELECT statement (WITH ... SELECT ...).
func (p *Parser) parseSelectStatement() *SelectStmt {
	stmt := &SelectStmt{}

	if p.check(TOKEN_WITH) {
		stmt.With = p.parseWithClause()
	}

	stmt.Body = p.parseSelectBody()
	return stmt
}

// parseWithClause parses a WITH clause with CTEs.
func (p *Parser) parseWithClause() *WithClause {
	p.expect(TOKEN_WITH)
	with := &WithClause{}

	if p.match(TOKEN_RECURSIVE) {
		with.Recursive = true
	}

	for {
		cte := p.parseCTE()
		with.CTEs = append(with.CTEs, cte)
		if !p.match(TOKEN_COMMA) {
			break
		}
	}

	return with
}

// parseCTE parses a single CTE.
func (p *Parser) parseCTE() *CTE {
	cte := &CTE{}

	if !p.check(TOKEN_IDENT) {
		p.addError("expected CTE name")
		return cte
	}
	cte.Name = p.token.Literal
	p.nextToken()

	p.expect(TOKEN_AS)
	p.expect(TOKEN_LPAREN)
	cte.Select = p.parseSelectStatement()
	p.expect(TOKEN_RPAREN)

	return cte
}

// parseSelectBody parses a SELECT body with possible set operations.
func (p *Parser) parseSelectBody() *SelectBody {
	body := &SelectBody{}
	body.Left = p.parseSelectCore()

	if p.check(TOKEN_UNION) || p.check(TOKEN_INTERSECT) || p.check(TOKEN_EXCEPT) {
		switch p.token.Type {
		case TOKEN_UNION:
			p.nextToken()
			if p.match(TOKEN_ALL) {
				body.Op = SetOpUnionAll
				body.All = true
			} else {
				body.Op = SetOpUnion
				p.match(TOKEN_DISTINCT)
			}
		case TOKEN_INTERSECT:
			p.nextToken()
			body.Op = SetOpIntersect
			p.match(TOKEN_ALL)
		case TOKEN_EXCEPT:
			p.nextToken()
			body.Op = SetOpExcept
			p.match(TOKEN_ALL)
		}

		// DuckDB: BY NAME
		if p.check(TOKEN_BY) {
			p.nextToken()
			if p.matchSoftKeyword("NAME") {
				body.ByName = true
			} else {
				p.addError("expected NAME after BY in set operation")
			}
		}

		body.Right = p.parseSelectBody()
	}

	return body
}

// parseSelectCore parses a single SELECT clause with all optional clauses.
func (p *Parser) parseSelectCore() *SelectCore {
	p.expect(TOKEN_SELECT)
	sc := &SelectCore{}

	if p.match(TOKEN_DISTINCT) {
		sc.Distinct = true
	} else {
		p.match(TOKEN_ALL)
	}

	sc.Columns = p.parseSelectList()

	if p.match(TOKEN_FROM) {
		sc.From = p.parseFromClause()
	}

	// Parse clauses in DuckDB order
	p.parseDuckDBClauses(sc)

	return sc
}

// parseDuckDBClauses parses optional clauses in DuckDB order.
func (p *Parser) parseDuckDBClauses(sc *SelectCore) {
	// WHERE
	if p.match(TOKEN_WHERE) {
		sc.Where = p.parseExpression()
	}

	// GROUP BY [ALL]
	if p.check(TOKEN_GROUP) {
		p.nextToken()
		p.expect(TOKEN_BY)
		if p.match(TOKEN_ALL) {
			sc.GroupByAll = true
		} else {
			sc.GroupBy = p.parseExpressionList()
		}
	}

	// HAVING
	if p.match(TOKEN_HAVING) {
		sc.Having = p.parseExpression()
	}

	// QUALIFY (DuckDB)
	if p.match(TOKEN_QUALIFY) {
		sc.Qualify = p.parseExpression()
	}

	// WINDOW
	if p.check(TOKEN_WINDOW) {
		p.nextToken()
		sc.Windows = p.parseWindowDefs()
	}

	// ORDER BY [ALL]
	if p.check(TOKEN_ORDER) {
		p.nextToken()
		p.expect(TOKEN_BY)
		if p.match(TOKEN_ALL) {
			sc.OrderByAll = true
			if p.match(TOKEN_DESC) {
				sc.OrderByAllDesc = true
			} else {
				p.match(TOKEN_ASC)
			}
		} else {
			sc.OrderBy = p.parseOrderByList()
		}
	}

	// LIMIT
	if p.match(TOKEN_LIMIT) {
		sc.Limit = p.parseExpression()
	}

	// OFFSET
	if p.match(TOKEN_OFFSET) {
		sc.Offset = p.parseExpression()
	}

	// FETCH FIRST/NEXT
	if p.check(TOKEN_FETCH) {
		sc.Fetch = p.parseFetchClause()
	}
}

// parseWindowDefs parses named window definitions.
func (p *Parser) parseWindowDefs() []WindowDef {
	var defs []WindowDef
	for {
		def := WindowDef{}
		if p.check(TOKEN_IDENT) {
			def.Name = p.token.Literal
			p.nextToken()
		}
		p.expect(TOKEN_AS)
		p.expect(TOKEN_LPAREN)
		def.Spec = &WindowSpec{}
		if p.match(TOKEN_PARTITION) {
			p.expect(TOKEN_BY)
			def.Spec.PartitionBy = p.parseExpressionList()
		}
		if p.check(TOKEN_ORDER) {
			p.nextToken()
			p.expect(TOKEN_BY)
			def.Spec.OrderBy = p.parseOrderByList()
		}
		if p.check(TOKEN_ROWS) || p.check(TOKEN_RANGE) || p.check(TOKEN_GROUPS) {
			def.Spec.Frame = p.parseFrameSpec()
		}
		p.expect(TOKEN_RPAREN)
		defs = append(defs, def)
		if !p.match(TOKEN_COMMA) {
			break
		}
	}
	return defs
}

// parseFetchClause parses FETCH FIRST/NEXT n ROWS ONLY/WITH TIES.
func (p *Parser) parseFetchClause() *FetchClause {
	p.expect(TOKEN_FETCH)
	fetch := &FetchClause{}

	if p.match(TOKEN_FIRST) {
		fetch.First = true
	} else {
		p.expect(TOKEN_NEXT)
	}

	if p.check(TOKEN_NUMBER) || p.check(TOKEN_IDENT) {
		fetch.Count = p.parseExpression()
	}

	if p.matchSoftKeyword("PERCENT") {
		fetch.Percent = true
	}

	// ROWS or ROW
	if !p.match(TOKEN_ROWS) {
		p.match(TOKEN_ROW)
	}

	if p.match(TOKEN_WITH) {
		p.expect(TOKEN_TIES)
		fetch.WithTies = true
	} else {
		p.match(TOKEN_ONLY)
	}

	return fetch
}

// parseSelectList parses the list of SELECT items.
func (p *Parser) parseSelectList() []SelectItem {
	var items []SelectItem
	for {
		item := p.parseSelectItem()
		items = append(items, item)
		if !p.match(TOKEN_COMMA) {
			break
		}
	}
	return items
}

// parseSelectItem parses a single SELECT item.
func (p *Parser) parseSelectItem() SelectItem {
	item := SelectItem{}

	// SELECT *
	if p.check(TOKEN_STAR) {
		item.Star = true
		p.nextToken()
		item.Modifiers = p.parseStarModifiers()
		return item
	}

	// table.* pattern using 3-token lookahead
	if p.check(TOKEN_IDENT) && p.checkPeek(TOKEN_DOT) && p.checkPeek2(TOKEN_STAR) {
		tableName := p.token.Literal
		p.nextToken() // consume ident
		p.nextToken() // consume DOT
		p.nextToken() // consume STAR
		item.TableStar = tableName
		item.Modifiers = p.parseStarModifiers()
		return item
	}

	// Regular expression
	item.Expr = p.parseExpression()

	// Optional alias
	if p.match(TOKEN_AS) {
		if p.check(TOKEN_IDENT) || p.check(TOKEN_STRING) {
			item.Alias = p.token.Literal
			p.nextToken()
		} else {
			p.addError("expected alias after AS")
		}
	} else if p.check(TOKEN_IDENT) && !p.isKeyword(p.token) {
		item.Alias = p.token.Literal
		p.nextToken()
	}

	return item
}

// === INSERT Statement ===

func (p *Parser) parseInsertStatement() *InsertStmt {
	p.expect(TOKEN_INSERT)
	p.expect(TOKEN_INTO)

	stmt := &InsertStmt{}
	stmt.Table = p.parseTableNameRef()

	// Optional column list
	if p.match(TOKEN_LPAREN) {
		for {
			if p.check(TOKEN_IDENT) {
				stmt.Columns = append(stmt.Columns, p.token.Literal)
				p.nextToken()
			}
			if !p.match(TOKEN_COMMA) {
				break
			}
		}
		p.expect(TOKEN_RPAREN)
	}

	// VALUES or SELECT
	if p.check(TOKEN_VALUES) {
		p.nextToken()
		for {
			p.expect(TOKEN_LPAREN)
			row := p.parseExpressionList()
			stmt.Values = append(stmt.Values, row)
			p.expect(TOKEN_RPAREN)
			if !p.match(TOKEN_COMMA) {
				break
			}
		}
	} else if p.check(TOKEN_SELECT) || p.check(TOKEN_WITH) {
		stmt.Query = p.parseSelectStatement()
	} else if p.match(TOKEN_DEFAULT) {
		p.expect(TOKEN_VALUES)
		// INSERT INTO t DEFAULT VALUES - no explicit values
	}

	// ON CONFLICT
	if p.check(TOKEN_ON) && p.checkPeek(TOKEN_CONFLICT) {
		stmt.OnConflict = p.parseOnConflict()
	}

	// RETURNING
	if p.match(TOKEN_RETURNING) {
		stmt.Returning = p.parseSelectList()
	}

	// Consume trailing semicolon
	p.match(TOKEN_SEMICOLON)
	return stmt
}

// parseOnConflict parses ON CONFLICT clause.
func (p *Parser) parseOnConflict() *OnConflictClause {
	p.expect(TOKEN_ON)
	p.expect(TOKEN_CONFLICT)

	oc := &OnConflictClause{}

	// Optional conflict target
	if p.match(TOKEN_LPAREN) {
		for {
			if p.check(TOKEN_IDENT) {
				oc.Columns = append(oc.Columns, p.token.Literal)
				p.nextToken()
			}
			if !p.match(TOKEN_COMMA) {
				break
			}
		}
		p.expect(TOKEN_RPAREN)
	}

	p.expect(TOKEN_DO)

	if p.match(TOKEN_NOTHING) {
		oc.DoNothing = true
	} else if p.match(TOKEN_UPDATE) {
		p.expect(TOKEN_SET)
		for {
			set := SetClause{}
			if p.check(TOKEN_IDENT) {
				set.Column = p.token.Literal
				p.nextToken()
			}
			p.expect(TOKEN_EQ)
			set.Value = p.parseExpression()
			oc.DoUpdate = append(oc.DoUpdate, set)
			if !p.match(TOKEN_COMMA) {
				break
			}
		}
	}

	return oc
}

// === UPDATE Statement ===

func (p *Parser) parseUpdateStatement() *UpdateStmt {
	p.expect(TOKEN_UPDATE)

	stmt := &UpdateStmt{}
	stmt.Table = p.parseTableNameRef()

	p.expect(TOKEN_SET)
	for {
		set := SetClause{}
		if p.check(TOKEN_IDENT) {
			set.Column = p.token.Literal
			p.nextToken()
		}
		p.expect(TOKEN_EQ)
		set.Value = p.parseExpression()
		stmt.Sets = append(stmt.Sets, set)
		if !p.match(TOKEN_COMMA) {
			break
		}
	}

	// FROM clause (DuckDB/PostgreSQL extension)
	if p.match(TOKEN_FROM) {
		stmt.From = p.parseFromClause()
	}

	// WHERE
	if p.match(TOKEN_WHERE) {
		stmt.Where = p.parseExpression()
	}

	// RETURNING
	if p.match(TOKEN_RETURNING) {
		stmt.Returning = p.parseSelectList()
	}

	p.match(TOKEN_SEMICOLON)
	return stmt
}

// === DELETE Statement ===

func (p *Parser) parseDeleteStatement() *DeleteStmt {
	p.expect(TOKEN_DELETE)
	p.expect(TOKEN_FROM)

	stmt := &DeleteStmt{}
	stmt.Table = p.parseTableNameRef()

	// USING clause
	if p.match(TOKEN_USING) {
		stmt.Using = p.parseFromClause()
	}

	// WHERE
	if p.match(TOKEN_WHERE) {
		stmt.Where = p.parseExpression()
	}

	// RETURNING
	if p.match(TOKEN_RETURNING) {
		stmt.Returning = p.parseSelectList()
	}

	p.match(TOKEN_SEMICOLON)
	return stmt
}

// parseTableNameRef parses a simple table name reference (for INSERT/UPDATE/DELETE target).
func (p *Parser) parseTableNameRef() *TableName {
	table := &TableName{}

	if !p.check(TOKEN_IDENT) {
		p.addError("expected table name")
		return table
	}

	parts := []string{p.token.Literal}
	p.nextToken()

	for p.match(TOKEN_DOT) {
		if p.check(TOKEN_IDENT) {
			parts = append(parts, p.token.Literal)
			p.nextToken()
		}
	}

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

	// Optional alias (not common for DML targets, but supported)
	if p.match(TOKEN_AS) {
		if p.check(TOKEN_IDENT) {
			table.Alias = p.token.Literal
			p.nextToken()
		}
	} else if p.check(TOKEN_IDENT) && !p.isKeyword(p.token) && !p.isClauseKeyword(p.token) {
		// Peek ahead - only treat as alias if it's not SET (for UPDATE t SET ...)
		if !p.check(TOKEN_SET) {
			table.Alias = p.token.Literal
			p.nextToken()
		}
	}

	return table
}

// === DDL Statements ===

func (p *Parser) parseDDLStatement() Stmt {
	// We're at CREATE - classify what kind
	p.nextToken() // consume CREATE

	// Handle CREATE OR REPLACE
	if p.matchSoftKeyword("OR") {
		p.expect(TOKEN_REPLACE)
	}

	// Handle CREATE TEMPORARY/TEMP
	p.match(TOKEN_TEMPORARY)

	switch {
	case p.check(TOKEN_TABLE):
		stmt := &DDLStmt{Type: DDLCreateTable, Raw: p.input}
		p.consumeUntilEOF()
		return stmt
	case p.check(TOKEN_VIEW):
		stmt := &DDLStmt{Type: DDLCreateView, Raw: p.input}
		p.consumeUntilEOF()
		return stmt
	case p.check(TOKEN_SCHEMA):
		stmt := &DDLStmt{Type: DDLCreateSchema, Raw: p.input}
		p.consumeUntilEOF()
		return stmt
	case p.check(TOKEN_INDEX):
		stmt := &DDLStmt{Type: DDLCreateIndex, Raw: p.input}
		p.consumeUntilEOF()
		return stmt
	case p.check(TOKEN_MACRO):
		stmt := &DDLStmt{Type: DDLCreateMacro, Raw: p.input}
		p.consumeUntilEOF()
		return stmt
	case p.check(TOKEN_TYPE):
		stmt := &DDLStmt{Type: DDLCreateType, Raw: p.input}
		p.consumeUntilEOF()
		return stmt
	case p.check(TOKEN_SECRET):
		stmt := &DDLStmt{Type: DDLCreateSecret, Raw: p.input}
		p.consumeUntilEOF()
		return stmt
	case p.check(TOKEN_FUNCTION):
		stmt := &DDLStmt{Type: DDLCreateFunction, Raw: p.input}
		p.consumeUntilEOF()
		return stmt
	default:
		// Unknown CREATE ... - classify as DDLOther
		stmt := &DDLStmt{Type: DDLOther, Raw: p.input}
		p.consumeUntilEOF()
		return stmt
	}
}

func (p *Parser) parseDDLDrop() Stmt {
	stmt := &DDLStmt{Type: DDLDrop, Raw: p.input}
	p.consumeUntilEOF()
	return stmt
}

func (p *Parser) parseDDLAlter() Stmt {
	stmt := &DDLStmt{Type: DDLAlter, Raw: p.input}
	p.consumeUntilEOF()
	return stmt
}

func (p *Parser) parseDDLSimple(ddlType DDLType) Stmt {
	stmt := &DDLStmt{Type: ddlType, Raw: p.input}
	p.consumeUntilEOF()
	return stmt
}

// === Utility Statements ===

func (p *Parser) parseUtility(utilType UtilityType) Stmt {
	// For CREATE SECRET, we need to detect it came through CREATE
	stmt := &UtilityStmt{Type: utilType, Raw: p.input}
	p.consumeUntilEOF()
	return stmt
}

// soft keyword helper used by parseDDLStatement
func init() {
	// Ensure "or" is also matched as soft keyword in DDL context
	_ = strings.EqualFold("or", "OR")
}
