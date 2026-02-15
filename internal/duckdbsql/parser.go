package duckdbsql

import (
	"fmt"
	"strings"
)

// Parser parses DuckDB SQL into an AST.
type Parser struct {
	lexer  *Lexer
	input  string // original input for raw extraction
	token  Token  // current token
	peek   Token  // lookahead token
	peek2  Token  // second lookahead token
	errors []error
}

// NewParser creates a new parser for the given SQL input.
func NewParser(sql string) *Parser {
	p := &Parser{
		lexer: NewLexer(sql),
		input: sql,
	}
	// Initialize three-token lookahead
	p.nextToken()
	p.nextToken()
	p.nextToken()
	return p
}

// Parse parses the SQL and returns the top-level statement.
// Returns an error if parsing fails or if multi-statement input is detected.
func Parse(sql string) (Stmt, error) {
	sql = strings.TrimSpace(sql)
	if sql == "" {
		return nil, fmt.Errorf("empty SQL")
	}

	p := NewParser(sql)
	stmt := p.parseTopLevel()
	if len(p.errors) > 0 {
		return nil, p.errors[0]
	}

	// Multi-statement rejection: after parsing one statement,
	// ensure we're at EOF (semicolons are consumed by parser).
	if p.token.Type != TOKEN_EOF {
		return nil, fmt.Errorf("multi-statement queries are not allowed")
	}

	return stmt, nil
}

// ParseExpr parses a standalone expression from SQL text.
// Used for parsing RLS filter expressions without wrapping in SELECT.
func ParseExpr(sql string) (Expr, error) {
	sql = strings.TrimSpace(sql)
	if sql == "" {
		return nil, fmt.Errorf("empty expression")
	}

	p := NewParser(sql)
	expr := p.parseExpression()
	if len(p.errors) > 0 {
		return nil, p.errors[0]
	}

	// Ensure we consumed all tokens
	if p.token.Type != TOKEN_EOF {
		return nil, fmt.Errorf("unexpected token after expression: %s", p.token.Literal)
	}

	return expr, nil
}

// parseTopLevel dispatches to the appropriate statement parser based on the first token.
func (p *Parser) parseTopLevel() Stmt {
	switch p.token.Type {
	case TOKEN_SELECT, TOKEN_WITH:
		return p.parseSelectStatement()

	case TOKEN_INSERT:
		return p.parseInsertStatement()

	case TOKEN_UPDATE:
		return p.parseUpdateStatement()

	case TOKEN_DELETE:
		return p.parseDeleteStatement()

	case TOKEN_CREATE:
		return p.parseDDLStatement()

	case TOKEN_DROP:
		return p.parseDDLDrop()

	case TOKEN_ALTER:
		return p.parseDDLAlter()

	case TOKEN_TRUNCATE:
		return p.parseDDLSimple(DDLTruncate)

	// Utility statements
	case TOKEN_CALL:
		return p.parseUtility(UtilityCall)
	case TOKEN_COPY:
		return p.parseUtility(UtilityCopy)
	case TOKEN_EXPORT:
		return p.parseUtility(UtilityExport)
	case TOKEN_PRAGMA:
		return p.parseUtility(UtilityPragma)
	case TOKEN_SET:
		return p.parseUtility(UtilitySet)
	case TOKEN_INSTALL:
		return p.parseUtility(UtilityInstall)
	case TOKEN_LOAD:
		return p.parseUtility(UtilityLoad)
	case TOKEN_ATTACH:
		return p.parseUtility(UtilityAttach)
	case TOKEN_DETACH:
		return p.parseUtility(UtilityDetach)
	case TOKEN_USE:
		return p.parseUtility(UtilityUse)
	case TOKEN_DESCRIBE:
		return p.parseUtility(UtilityDescribe)

	default:
		p.addError(fmt.Sprintf("unexpected token at start of statement: %s", p.token.Type))
		return nil
	}
}

// === Token Helpers ===

// nextToken advances to the next token.
func (p *Parser) nextToken() {
	p.token = p.peek
	p.peek = p.peek2
	p.peek2 = p.lexer.NextToken()
}

// check returns true if the current token is of the given type.
func (p *Parser) check(t TokenType) bool {
	return p.token.Type == t
}

// checkPeek returns true if the peek token is of the given type.
func (p *Parser) checkPeek(t TokenType) bool {
	return p.peek.Type == t
}

// checkPeek2 returns true if the peek2 token is of the given type.
func (p *Parser) checkPeek2(t TokenType) bool {
	return p.peek2.Type == t
}

// match consumes the current token if it matches and returns true.
func (p *Parser) match(t TokenType) bool {
	if p.check(t) {
		p.nextToken()
		return true
	}
	return false
}

// matchSoftKeyword consumes the current token if it's an identifier matching
// the given soft keyword (case-insensitive).
func (p *Parser) matchSoftKeyword(keyword string) bool {
	if p.check(TOKEN_IDENT) && strings.EqualFold(p.token.Literal, keyword) {
		p.nextToken()
		return true
	}
	return false
}

// expect consumes the current token if it matches, otherwise adds an error.
func (p *Parser) expect(t TokenType) bool {
	if p.check(t) {
		p.nextToken()
		return true
	}
	p.addError(fmt.Sprintf("unexpected token %s, expected %s", p.token.Type, t))
	return false
}

// addError adds a parse error.
func (p *Parser) addError(msg string) {
	p.errors = append(p.errors, fmt.Errorf("parse error: %s", msg))
}

// === Keyword Classification ===

// isKeyword returns true if the token is a reserved keyword that cannot be used as an alias.
func (p *Parser) isKeyword(tok Token) bool {
	switch tok.Type {
	case TOKEN_FROM, TOKEN_UNION, TOKEN_INTERSECT, TOKEN_EXCEPT,
		TOKEN_LEFT, TOKEN_RIGHT, TOKEN_INNER, TOKEN_OUTER, TOKEN_FULL,
		TOKEN_CROSS, TOKEN_JOIN, TOKEN_ON, TOKEN_LATERAL,
		TOKEN_WHERE, TOKEN_GROUP, TOKEN_HAVING, TOKEN_ORDER, TOKEN_LIMIT,
		TOKEN_OFFSET, TOKEN_QUALIFY, TOKEN_WINDOW, TOKEN_FETCH,
		TOKEN_SELECT, TOKEN_INSERT, TOKEN_UPDATE, TOKEN_DELETE,
		TOKEN_CREATE, TOKEN_DROP, TOKEN_ALTER, TOKEN_SET,
		TOKEN_INTO, TOKEN_VALUES, TOKEN_RETURNING, TOKEN_USING:
		return true
	}
	return false
}

// isJoinKeyword returns true if token is a JOIN-related keyword.
func (p *Parser) isJoinKeyword(tok Token) bool {
	switch tok.Type {
	case TOKEN_JOIN, TOKEN_ON, TOKEN_USING, TOKEN_NATURAL, TOKEN_LATERAL, TOKEN_OUTER,
		TOKEN_INNER, TOKEN_LEFT, TOKEN_RIGHT, TOKEN_FULL, TOKEN_CROSS,
		TOKEN_SEMI, TOKEN_ANTI, TOKEN_ASOF, TOKEN_POSITIONAL:
		return true
	}
	return false
}

// isClauseKeyword returns true if token starts a new clause.
func (p *Parser) isClauseKeyword(tok Token) bool {
	switch tok.Type {
	case TOKEN_UNION, TOKEN_INTERSECT, TOKEN_EXCEPT,
		TOKEN_WHERE, TOKEN_GROUP, TOKEN_HAVING, TOKEN_ORDER,
		TOKEN_LIMIT, TOKEN_OFFSET, TOKEN_QUALIFY, TOKEN_WINDOW, TOKEN_FETCH:
		return true
	}
	return false
}

// consumeUntilEOF consumes all remaining tokens.
// Used for DDL/utility statements that don't need deep parsing.
func (p *Parser) consumeUntilEOF() {
	for p.token.Type != TOKEN_EOF && p.token.Type != TOKEN_SEMICOLON {
		p.nextToken()
	}
	// Consume trailing semicolon if present
	p.match(TOKEN_SEMICOLON)
}
