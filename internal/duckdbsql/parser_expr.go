package duckdbsql

import (
	"fmt"
	"strings"
)

// Expression parsing using Pratt parser (precedence climbing) for DuckDB SQL.

// parseExpression parses an expression using precedence climbing.
func (p *Parser) parseExpression() Expr {
	return p.parseExpressionWithPrecedence(PrecedenceNone + 1)
}

// parseExpressionWithPrecedence implements Pratt parsing.
func (p *Parser) parseExpressionWithPrecedence(minPrecedence int) Expr {
	left := p.parsePrefixExpr()
	if left == nil {
		return nil
	}

	for {
		prec := p.getInfixPrecedence()
		if prec < minPrecedence {
			break
		}
		left = p.parseInfixExpr(left, prec)
		if left == nil {
			break
		}
	}

	return left
}

// parsePrefixExpr parses prefix expressions (unary operators and primary expressions).
func (p *Parser) parsePrefixExpr() Expr {
	switch p.token.Type {
	case TOKEN_NOT:
		p.nextToken()
		expr := p.parseExpressionWithPrecedence(PrecedenceNot)
		return &UnaryExpr{Op: TOKEN_NOT, Expr: expr}

	case TOKEN_MINUS:
		p.nextToken()
		expr := p.parseExpressionWithPrecedence(PrecedenceUnary)
		return &UnaryExpr{Op: TOKEN_MINUS, Expr: expr}

	case TOKEN_PLUS:
		p.nextToken()
		expr := p.parseExpressionWithPrecedence(PrecedenceUnary)
		return &UnaryExpr{Op: TOKEN_PLUS, Expr: expr}

	case TOKEN_TILDE:
		p.nextToken()
		expr := p.parseExpressionWithPrecedence(PrecedenceUnary)
		return &UnaryExpr{Op: TOKEN_TILDE, Expr: expr}

	default:
		return p.parsePrimary()
	}
}

// getInfixPrecedence returns the precedence of the current token as an infix operator.
func (p *Parser) getInfixPrecedence() int {
	switch p.token.Type {
	case TOKEN_OR:
		return PrecedenceOr
	case TOKEN_AND:
		return PrecedenceAnd
	case TOKEN_EQ, TOKEN_NE, TOKEN_LT, TOKEN_GT, TOKEN_LE, TOKEN_GE, TOKEN_DBLEQ:
		return PrecedenceComparison
	case TOKEN_IS, TOKEN_IN, TOKEN_BETWEEN, TOKEN_LIKE, TOKEN_ILIKE, TOKEN_GLOB, TOKEN_SIMILAR:
		return PrecedenceComparison
	case TOKEN_NOT:
		return PrecedenceComparison
	case TOKEN_PLUS, TOKEN_MINUS, TOKEN_DPIPE, TOKEN_AMP, TOKEN_PIPE, TOKEN_CARET, TOKEN_LSHIFT, TOKEN_RSHIFT:
		return PrecedenceAddition
	case TOKEN_STAR, TOKEN_SLASH, TOKEN_MOD, TOKEN_DSLASH:
		return PrecedenceMultiply
	case TOKEN_DCOLON:
		return PrecedencePostfix
	case TOKEN_LBRACKET:
		return PrecedencePostfix
	case TOKEN_ARROW:
		return PrecedenceOr
	case TOKEN_COLONEQ:
		return PrecedenceOr
	case TOKEN_IDENT:
		if strings.EqualFold(p.token.Literal, "COLLATE") {
			return PrecedencePostfix
		}
		return PrecedenceNone
	default:
		return PrecedenceNone
	}
}

// parseInfixExpr parses an infix expression given the left operand.
func (p *Parser) parseInfixExpr(left Expr, prec int) Expr {
	switch p.token.Type {
	case TOKEN_NOT:
		return p.parseNotInfixExpr(left)
	case TOKEN_IS:
		return p.parseIsExpr(left)
	case TOKEN_IN:
		p.nextToken()
		return p.parseInExpr(left, false)
	case TOKEN_BETWEEN:
		p.nextToken()
		return p.parseBetweenExpr(left, false)
	case TOKEN_LIKE:
		p.nextToken()
		return p.parseLikeExpr(left, false, false)
	case TOKEN_ILIKE:
		p.nextToken()
		return p.parseLikeExpr(left, false, true)
	case TOKEN_GLOB:
		p.nextToken()
		return p.parseGlobExpr(left, false)
	case TOKEN_SIMILAR:
		p.nextToken()
		return p.parseSimilarToExpr(left, false)
	case TOKEN_DCOLON:
		return p.parseTypeCastExpr(left)
	case TOKEN_LBRACKET:
		return p.parseIndexOrSliceExpr(left)
	case TOKEN_ARROW:
		return p.parseLambdaExpr(left)
	case TOKEN_COLONEQ:
		return p.parseNamedArgExpr(left)
	case TOKEN_IDENT:
		if strings.EqualFold(p.token.Literal, "COLLATE") {
			return p.parseCollateExpr(left)
		}
		op := p.token.Type
		p.nextToken()
		right := p.parseExpressionWithPrecedence(prec + 1)
		return &BinaryExpr{Left: left, Op: op, Right: right}
	default:
		op := p.token.Type
		p.nextToken()
		right := p.parseExpressionWithPrecedence(prec + 1)
		return &BinaryExpr{Left: left, Op: op, Right: right}
	}
}

// parseNotInfixExpr handles NOT as an infix modifier (NOT IN, NOT BETWEEN, NOT LIKE, NOT ILIKE).
func (p *Parser) parseNotInfixExpr(left Expr) Expr {
	p.nextToken() // consume NOT

	switch p.token.Type {
	case TOKEN_IN:
		p.nextToken()
		return p.parseInExpr(left, true)
	case TOKEN_BETWEEN:
		p.nextToken()
		return p.parseBetweenExpr(left, true)
	case TOKEN_LIKE:
		p.nextToken()
		return p.parseLikeExpr(left, true, false)
	case TOKEN_ILIKE:
		p.nextToken()
		return p.parseLikeExpr(left, true, true)
	case TOKEN_GLOB:
		p.nextToken()
		return p.parseGlobExpr(left, true)
	case TOKEN_SIMILAR:
		p.nextToken()
		return p.parseSimilarToExpr(left, true)
	default:
		p.addError("expected IN, BETWEEN, LIKE, ILIKE, GLOB, or SIMILAR after NOT")
		return left
	}
}

// parseIsExpr parses IS [NOT] NULL / IS [NOT] TRUE / IS [NOT] FALSE.
func (p *Parser) parseIsExpr(left Expr) Expr {
	p.nextToken() // consume IS
	isNot := p.match(TOKEN_NOT)

	switch p.token.Type {
	case TOKEN_NULL:
		p.nextToken()
		return &IsNullExpr{Expr: left, Not: isNot}
	case TOKEN_TRUE:
		p.nextToken()
		return &IsBoolExpr{Expr: left, Not: isNot, Value: true}
	case TOKEN_FALSE:
		p.nextToken()
		return &IsBoolExpr{Expr: left, Not: isNot, Value: false}
	case TOKEN_DISTINCT:
		p.nextToken() // consume DISTINCT
		p.expect(TOKEN_FROM)
		right := p.parseExpressionWithPrecedence(PrecedenceComparison + 1)
		return &IsDistinctExpr{Left: left, Not: isNot, Right: right}
	default:
		p.addError("expected NULL, TRUE, FALSE, or DISTINCT after IS")
		return left
	}
}

// parseInExpr parses IN (values) or IN (subquery).
func (p *Parser) parseInExpr(left Expr, not bool) Expr {
	in := &InExpr{Expr: left, Not: not}

	if p.check(TOKEN_LPAREN) {
		p.nextToken() // consume (
		if p.check(TOKEN_SELECT) || p.check(TOKEN_WITH) {
			in.Query = p.parseSelectStatement()
		} else {
			in.Values = p.parseExpressionList()
		}
		p.expect(TOKEN_RPAREN)
	} else if p.check(TOKEN_LBRACKET) {
		listExpr := p.parseListLiteral()
		in.Values = []Expr{listExpr}
	} else {
		// IN 'string' or IN expr
		in.Values = []Expr{p.parsePrimary()}
	}

	return in
}

// parseBetweenExpr parses BETWEEN low AND high.
func (p *Parser) parseBetweenExpr(left Expr, not bool) Expr {
	between := &BetweenExpr{Expr: left, Not: not}
	between.Low = p.parseExpressionWithPrecedence(PrecedenceAddition)
	p.expect(TOKEN_AND)
	between.High = p.parseExpressionWithPrecedence(PrecedenceAddition)
	return between
}

// parseLikeExpr parses LIKE/ILIKE pattern.
func (p *Parser) parseLikeExpr(left Expr, not bool, ilike bool) Expr {
	like := &LikeExpr{Expr: left, Not: not, ILike: ilike}
	like.Pattern = p.parseExpressionWithPrecedence(PrecedenceAddition)
	if p.matchSoftKeyword("ESCAPE") {
		like.Escape = p.parseExpressionWithPrecedence(PrecedenceAddition)
	}
	return like
}

// parseTypeCastExpr parses expr::type.
func (p *Parser) parseTypeCastExpr(left Expr) Expr {
	p.nextToken() // consume ::
	typeName := p.parseTypeName()
	return &TypeCastExpr{Expr: left, TypeName: typeName}
}

// parseIndexOrSliceExpr parses expr[index] or expr[start:stop].
func (p *Parser) parseIndexOrSliceExpr(left Expr) Expr {
	p.nextToken() // consume [
	idx := &IndexExpr{Expr: left}

	if p.check(TOKEN_COLON) {
		// [:end]
		idx.IsSlice = true
		p.nextToken()
		if !p.check(TOKEN_RBRACKET) {
			idx.Stop = p.parseExpression()
		}
	} else if !p.check(TOKEN_RBRACKET) {
		start := p.parseExpression()
		if p.match(TOKEN_COLON) {
			// [start:end]
			idx.IsSlice = true
			idx.Start = start
			if !p.check(TOKEN_RBRACKET) {
				idx.Stop = p.parseExpression()
			}
		} else {
			// [index]
			idx.Index = start
		}
	}

	p.expect(TOKEN_RBRACKET)
	return idx
}

// parseLambdaExpr parses x -> expr or (x, y) -> expr.
func (p *Parser) parseLambdaExpr(left Expr) Expr {
	p.nextToken() // consume ->
	lambda := &LambdaExpr{}

	params, err := extractLambdaParams(left)
	if err != nil {
		p.addError(err.Error())
		return left
	}
	lambda.Params = params
	lambda.Body = p.parseExpression()

	return lambda
}

// extractLambdaParams extracts parameter names from a lambda parameter expression.
func extractLambdaParams(expr Expr) ([]string, error) {
	switch e := expr.(type) {
	case *ColumnRef:
		if e.Table != "" {
			return nil, fmt.Errorf("invalid lambda parameter: qualified name not allowed")
		}
		return []string{e.Column}, nil
	case *ParenExpr:
		return extractLambdaParams(e.Expr)
	case *BinaryExpr:
		if e.Op == TOKEN_COMMA {
			leftParams, err := extractLambdaParams(e.Left)
			if err != nil {
				return nil, err
			}
			rightParams, err := extractLambdaParams(e.Right)
			if err != nil {
				return nil, err
			}
			return append(leftParams, rightParams...), nil
		}
		return nil, fmt.Errorf("invalid lambda parameter: unexpected binary expression")
	default:
		return nil, fmt.Errorf("invalid lambda parameter: expected identifier, got %T", expr)
	}
}

// parseGlobExpr parses [NOT] GLOB pattern.
func (p *Parser) parseGlobExpr(left Expr, not bool) Expr {
	return &GlobExpr{
		Expr:    left,
		Not:     not,
		Pattern: p.parseExpressionWithPrecedence(PrecedenceAddition),
	}
}

// parseSimilarToExpr parses [NOT] SIMILAR TO pattern.
func (p *Parser) parseSimilarToExpr(left Expr, not bool) Expr {
	// Expect TO after SIMILAR
	if !p.matchSoftKeyword("TO") {
		p.expect(TOKEN_IDENT) // will produce an error message
	}
	return &SimilarToExpr{
		Expr:    left,
		Not:     not,
		Pattern: p.parseExpressionWithPrecedence(PrecedenceAddition),
	}
}

// parseExpressionList parses a comma-separated list of expressions.
func (p *Parser) parseExpressionList() []Expr {
	var exprs []Expr
	for {
		expr := p.parseExpression()
		if expr != nil {
			exprs = append(exprs, expr)
		}
		if !p.match(TOKEN_COMMA) {
			break
		}
	}
	return exprs
}

// parseCollateExpr parses COLLATE collation_name.
func (p *Parser) parseCollateExpr(left Expr) Expr {
	p.nextToken() // consume COLLATE
	var parts []string
	parts = append(parts, p.token.Literal)
	p.nextToken()
	for p.match(TOKEN_DOT) {
		parts = append(parts, p.token.Literal)
		p.nextToken()
	}
	return &CollateExpr{Expr: left, Collation: strings.Join(parts, ".")}
}

// parseNamedArgExpr parses name := value.
func (p *Parser) parseNamedArgExpr(left Expr) Expr {
	p.nextToken() // consume :=
	name := ""
	if col, ok := left.(*ColumnRef); ok {
		name = col.Column
	}
	right := p.parseExpressionWithPrecedence(PrecedenceOr + 1)
	return &NamedArgExpr{Name: name, Value: right}
}
