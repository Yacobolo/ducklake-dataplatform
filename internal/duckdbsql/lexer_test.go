package duckdbsql

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLexer_Punctuation(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		wantType TokenType
		wantLit  string
	}{
		{"plus", "+", TOKEN_PLUS, "+"},
		{"minus", "-", TOKEN_MINUS, "-"},
		{"star", "*", TOKEN_STAR, "*"},
		{"slash", "/", TOKEN_SLASH, "/"},
		{"double_slash", "//", TOKEN_DSLASH, "//"},
		{"mod", "%", TOKEN_MOD, "%"},
		{"eq", "=", TOKEN_EQ, "="},
		{"ne_bang", "!=", TOKEN_NE, "!="},
		{"ne_diamond", "<>", TOKEN_NE, "<>"},
		{"lt", "<", TOKEN_LT, "<"},
		{"gt", ">", TOKEN_GT, ">"},
		{"le", "<=", TOKEN_LE, "<="},
		{"ge", ">=", TOKEN_GE, ">="},
		{"dot", ".", TOKEN_DOT, "."},
		{"comma", ",", TOKEN_COMMA, ","},
		{"semicolon", ";", TOKEN_SEMICOLON, ";"},
		{"lparen", "(", TOKEN_LPAREN, "("},
		{"rparen", ")", TOKEN_RPAREN, ")"},
		{"lbracket", "[", TOKEN_LBRACKET, "["},
		{"rbracket", "]", TOKEN_RBRACKET, "]"},
		{"lbrace", "{", TOKEN_LBRACE, "{"},
		{"rbrace", "}", TOKEN_RBRACE, "}"},
		{"colon", ":", TOKEN_COLON, ":"},
		{"dcolon", "::", TOKEN_DCOLON, "::"},
		{"arrow", "->", TOKEN_ARROW, "->"},
		{"dpipe", "||", TOKEN_DPIPE, "||"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			l := NewLexer(tc.input)
			tok := l.NextToken()
			assert.Equal(t, tc.wantType, tok.Type, "token type")
			assert.Equal(t, tc.wantLit, tok.Literal, "token literal")
		})
	}
}

func TestLexer_Numbers(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantLit string
	}{
		{"integer", "42", "42"},
		{"decimal", "3.14", "3.14"},
		{"scientific", "1e10", "1e10"},
		{"scientific_upper", "2E5", "2E5"},
		{"scientific_negative", "1e-3", "1e-3"},
		{"scientific_positive", "1e+3", "1e+3"},
		{"large_integer", "3000000000", "3000000000"},
		{"zero", "0", "0"},
		{"decimal_start", "0.5", "0.5"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			l := NewLexer(tc.input)
			tok := l.NextToken()
			assert.Equal(t, TOKEN_NUMBER, tok.Type)
			assert.Equal(t, tc.wantLit, tok.Literal)
		})
	}
}

func TestLexer_Strings(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantLit string
	}{
		{"simple", "'hello'", "hello"},
		{"empty", "''", ""},
		{"with_spaces", "'hello world'", "hello world"},
		{"escaped_quote", "'it''s'", "it's"},
		{"double_escape", "'a''''b'", "a''b"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			l := NewLexer(tc.input)
			tok := l.NextToken()
			assert.Equal(t, TOKEN_STRING, tok.Type)
			assert.Equal(t, tc.wantLit, tok.Literal)
		})
	}
}

func TestLexer_QuotedIdentifiers(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantLit string
	}{
		{"simple", `"foo"`, "foo"},
		{"mixed_case", `"FooBar"`, "FooBar"},
		{"with_spaces", `"foo bar"`, "foo bar"},
		{"escaped_quote", `"foo""bar"`, `foo"bar`},
		{"reserved_word", `"select"`, "select"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			l := NewLexer(tc.input)
			tok := l.NextToken()
			assert.Equal(t, TOKEN_IDENT, tok.Type)
			assert.Equal(t, tc.wantLit, tok.Literal)
		})
	}
}

func TestLexer_Keywords(t *testing.T) {
	tests := []struct {
		input    string
		wantType TokenType
	}{
		{"SELECT", TOKEN_SELECT},
		{"select", TOKEN_SELECT},
		{"Select", TOKEN_SELECT},
		{"FROM", TOKEN_FROM},
		{"WHERE", TOKEN_WHERE},
		{"AND", TOKEN_AND},
		{"OR", TOKEN_OR},
		{"NOT", TOKEN_NOT},
		{"JOIN", TOKEN_JOIN},
		{"LEFT", TOKEN_LEFT},
		{"RIGHT", TOKEN_RIGHT},
		{"INNER", TOKEN_INNER},
		{"FULL", TOKEN_FULL},
		{"CROSS", TOKEN_CROSS},
		{"ON", TOKEN_ON},
		{"AS", TOKEN_AS},
		{"IN", TOKEN_IN},
		{"IS", TOKEN_IS},
		{"NULL", TOKEN_NULL},
		{"TRUE", TOKEN_TRUE},
		{"FALSE", TOKEN_FALSE},
		{"BETWEEN", TOKEN_BETWEEN},
		{"LIKE", TOKEN_LIKE},
		{"EXISTS", TOKEN_EXISTS},
		{"CASE", TOKEN_CASE},
		{"WHEN", TOKEN_WHEN},
		{"THEN", TOKEN_THEN},
		{"ELSE", TOKEN_ELSE},
		{"END", TOKEN_END},
		{"CAST", TOKEN_CAST},
		{"ORDER", TOKEN_ORDER},
		{"BY", TOKEN_BY},
		{"GROUP", TOKEN_GROUP},
		{"HAVING", TOKEN_HAVING},
		{"LIMIT", TOKEN_LIMIT},
		{"OFFSET", TOKEN_OFFSET},
		{"UNION", TOKEN_UNION},
		{"ALL", TOKEN_ALL},
		{"DISTINCT", TOKEN_DISTINCT},
		{"INSERT", TOKEN_INSERT},
		{"INTO", TOKEN_INTO},
		{"VALUES", TOKEN_VALUES},
		{"UPDATE", TOKEN_UPDATE},
		{"SET", TOKEN_SET},
		{"DELETE", TOKEN_DELETE},
		{"CREATE", TOKEN_CREATE},
		{"DROP", TOKEN_DROP},
		{"ALTER", TOKEN_ALTER},
		{"TABLE", TOKEN_TABLE},
		{"VIEW", TOKEN_VIEW},
		{"SCHEMA", TOKEN_SCHEMA},
		{"INDEX", TOKEN_INDEX},
		{"WITH", TOKEN_WITH},
		{"RECURSIVE", TOKEN_RECURSIVE},
		// DuckDB-specific
		{"QUALIFY", TOKEN_QUALIFY},
		{"PIVOT", TOKEN_PIVOT},
		{"UNPIVOT", TOKEN_UNPIVOT},
		{"EXCLUDE", TOKEN_EXCLUDE},
		{"COLUMNS", TOKEN_COLUMNS},
		{"ILIKE", TOKEN_ILIKE},
		{"SEMI", TOKEN_SEMI},
		{"ANTI", TOKEN_ANTI},
		{"ASOF", TOKEN_ASOF},
		{"POSITIONAL", TOKEN_POSITIONAL},
	}

	for _, tc := range tests {
		t.Run(tc.input, func(t *testing.T) {
			l := NewLexer(tc.input)
			tok := l.NextToken()
			assert.Equal(t, tc.wantType, tok.Type)
		})
	}
}

func TestLexer_Identifiers(t *testing.T) {
	tests := []struct {
		input   string
		wantLit string
	}{
		{"foo", "foo"},
		{"_bar", "_bar"},
		{"col1", "col1"},
		{"my_table", "my_table"},
	}

	for _, tc := range tests {
		t.Run(tc.input, func(t *testing.T) {
			l := NewLexer(tc.input)
			tok := l.NextToken()
			assert.Equal(t, TOKEN_IDENT, tok.Type)
			assert.Equal(t, tc.wantLit, tok.Literal)
		})
	}
}

func TestLexer_Comments(t *testing.T) {
	t.Run("line_comment", func(t *testing.T) {
		l := NewLexer("-- this is a comment\nSELECT")
		tok := l.NextToken()
		assert.Equal(t, TOKEN_SELECT, tok.Type)
	})

	t.Run("block_comment", func(t *testing.T) {
		l := NewLexer("/* block comment */SELECT")
		tok := l.NextToken()
		assert.Equal(t, TOKEN_SELECT, tok.Type)
	})

	t.Run("block_comment_multiline", func(t *testing.T) {
		l := NewLexer("/* multi\nline\ncomment */42")
		tok := l.NextToken()
		assert.Equal(t, TOKEN_NUMBER, tok.Type)
		assert.Equal(t, "42", tok.Literal)
	})
}

func TestLexer_Whitespace(t *testing.T) {
	l := NewLexer("  \t\n\r  SELECT")
	tok := l.NextToken()
	assert.Equal(t, TOKEN_SELECT, tok.Type)
}

func TestLexer_IllegalChar(t *testing.T) {
	l := NewLexer("@")
	tok := l.NextToken()
	assert.Equal(t, TOKEN_ILLEGAL, tok.Type)
}

func TestLexer_EOF(t *testing.T) {
	l := NewLexer("")
	tok := l.NextToken()
	assert.Equal(t, TOKEN_EOF, tok.Type)
}

func TestLexer_CompleteStatement(t *testing.T) {
	l := NewLexer(`SELECT "Name", 42 FROM titanic WHERE id > 10`)

	expected := []struct {
		typ TokenType
		lit string
	}{
		{TOKEN_SELECT, "SELECT"},
		{TOKEN_IDENT, "Name"},
		{TOKEN_COMMA, ","},
		{TOKEN_NUMBER, "42"},
		{TOKEN_FROM, "FROM"},
		{TOKEN_IDENT, "titanic"},
		{TOKEN_WHERE, "WHERE"},
		{TOKEN_IDENT, "id"},
		{TOKEN_GT, ">"},
		{TOKEN_NUMBER, "10"},
		{TOKEN_EOF, ""},
	}

	for _, exp := range expected {
		tok := l.NextToken()
		assert.Equal(t, exp.typ, tok.Type, "type for %q", exp.lit)
		assert.Equal(t, exp.lit, tok.Literal, "literal")
	}
}

func TestLexer_DuckDBOperators(t *testing.T) {
	t.Run("double_colon_cast", func(t *testing.T) {
		l := NewLexer("x::INT")
		toks := collectTokens(l)
		require.Len(t, toks, 4) // x :: INT EOF
		assert.Equal(t, TOKEN_IDENT, toks[0].Type)
		assert.Equal(t, TOKEN_DCOLON, toks[1].Type)
		assert.Equal(t, TOKEN_IDENT, toks[2].Type) // INT is not a keyword
	})

	t.Run("arrow_lambda", func(t *testing.T) {
		l := NewLexer("x -> x + 1")
		toks := collectTokens(l)
		require.Len(t, toks, 6) // x -> x + 1 EOF
		assert.Equal(t, TOKEN_ARROW, toks[1].Type)
	})

	t.Run("integer_division", func(t *testing.T) {
		l := NewLexer("a // b")
		toks := collectTokens(l)
		require.Len(t, toks, 4) // a // b EOF
		assert.Equal(t, TOKEN_DSLASH, toks[1].Type)
	})
}

// collectTokens collects all tokens from a lexer until EOF.
func collectTokens(l *Lexer) []Token {
	var tokens []Token
	for {
		tok := l.NextToken()
		tokens = append(tokens, tok)
		if tok.Type == TOKEN_EOF {
			break
		}
	}
	return tokens
}
