package duckdbsql

import (
	"strings"
	"unicode"
)

// Lexer tokenizes SQL input for DuckDB.
type Lexer struct {
	input   string
	pos     int  // current position in input
	readPos int  // reading position (after current char)
	ch      byte // current char under examination
}

// NewLexer creates a new Lexer for the given input.
func NewLexer(input string) *Lexer {
	l := &Lexer{input: input}
	l.readChar()
	return l
}

// readChar advances to the next character.
func (l *Lexer) readChar() {
	if l.readPos >= len(l.input) {
		l.ch = 0 // NUL = EOF
	} else {
		l.ch = l.input[l.readPos]
	}
	l.pos = l.readPos
	l.readPos++
}

// peekChar returns the next character without advancing.
func (l *Lexer) peekChar() byte {
	if l.readPos >= len(l.input) {
		return 0
	}
	return l.input[l.readPos]
}

// NextToken returns the next token from the input.
func (l *Lexer) NextToken() Token {
	l.skipWhitespaceAndComments()

	var tok Token

	switch l.ch {
	case 0:
		tok.Type = TOKEN_EOF
		tok.Literal = ""
		return tok
	case '+':
		tok = Token{Type: TOKEN_PLUS, Literal: "+"}
	case '-':
		if l.peekChar() == '>' {
			l.readChar()
			tok = Token{Type: TOKEN_ARROW, Literal: "->"}
		} else {
			tok = Token{Type: TOKEN_MINUS, Literal: "-"}
		}
	case '*':
		tok = Token{Type: TOKEN_STAR, Literal: "*"}
	case '/':
		if l.peekChar() == '/' {
			l.readChar()
			tok = Token{Type: TOKEN_DSLASH, Literal: "//"}
		} else {
			tok = Token{Type: TOKEN_SLASH, Literal: "/"}
		}
	case '%':
		tok = Token{Type: TOKEN_MOD, Literal: "%"}
	case '=':
		if l.peekChar() == '=' {
			l.readChar()
			tok = Token{Type: TOKEN_DBLEQ, Literal: "=="}
		} else {
			tok = Token{Type: TOKEN_EQ, Literal: "="}
		}
	case '<':
		switch l.peekChar() {
		case '=':
			l.readChar()
			tok = Token{Type: TOKEN_LE, Literal: "<="}
		case '>':
			l.readChar()
			tok = Token{Type: TOKEN_NE, Literal: "<>"}
		case '<':
			l.readChar()
			tok = Token{Type: TOKEN_LSHIFT, Literal: "<<"}
		default:
			tok = Token{Type: TOKEN_LT, Literal: "<"}
		}
	case '>':
		switch l.peekChar() {
		case '=':
			l.readChar()
			tok = Token{Type: TOKEN_GE, Literal: ">="}
		case '>':
			l.readChar()
			tok = Token{Type: TOKEN_RSHIFT, Literal: ">>"}
		default:
			tok = Token{Type: TOKEN_GT, Literal: ">"}
		}
	case '!':
		if l.peekChar() == '=' {
			l.readChar()
			tok = Token{Type: TOKEN_NE, Literal: "!="}
		} else {
			tok = Token{Type: TOKEN_ILLEGAL, Literal: string(l.ch)}
		}
	case '|':
		if l.peekChar() == '|' {
			l.readChar()
			tok = Token{Type: TOKEN_DPIPE, Literal: "||"}
		} else {
			tok = Token{Type: TOKEN_PIPE, Literal: "|"}
		}
	case '.':
		tok = Token{Type: TOKEN_DOT, Literal: "."}
	case ',':
		tok = Token{Type: TOKEN_COMMA, Literal: ","}
	case ';':
		tok = Token{Type: TOKEN_SEMICOLON, Literal: ";"}
	case '(':
		tok = Token{Type: TOKEN_LPAREN, Literal: "("}
	case ')':
		tok = Token{Type: TOKEN_RPAREN, Literal: ")"}
	case '[':
		tok = Token{Type: TOKEN_LBRACKET, Literal: "["}
	case ']':
		tok = Token{Type: TOKEN_RBRACKET, Literal: "]"}
	case '{':
		tok = Token{Type: TOKEN_LBRACE, Literal: "{"}
	case '}':
		tok = Token{Type: TOKEN_RBRACE, Literal: "}"}
	case ':':
		switch l.peekChar() {
		case ':':
			l.readChar()
			tok = Token{Type: TOKEN_DCOLON, Literal: "::"}
		case '=':
			l.readChar()
			tok = Token{Type: TOKEN_COLONEQ, Literal: ":="}
		default:
			tok = Token{Type: TOKEN_COLON, Literal: ":"}
		}
	case '&':
		tok = Token{Type: TOKEN_AMP, Literal: "&"}
	case '^':
		tok = Token{Type: TOKEN_CARET, Literal: "^"}
	case '~':
		tok = Token{Type: TOKEN_TILDE, Literal: "~"}
	case '?':
		tok = Token{Type: TOKEN_QMARK, Literal: "?"}
	case '$':
		l.readChar() // advance past $
		start := l.pos
		for isLetter(l.ch) || isDigit(l.ch) || l.ch == '_' {
			l.readChar()
		}
		tok = Token{Type: TOKEN_DOLLAR, Literal: "$" + l.input[start:l.pos]}
		return tok
	case '\'':
		tok.Type = TOKEN_STRING
		tok.Literal = l.readString()
		return tok
	case '"':
		tok.Type = TOKEN_IDENT
		tok.Literal = l.readQuotedIdentifier()
		return tok
	default:
		switch {
		case isLetter(l.ch) || l.ch == '_':
			literal := l.readIdentifier()
			tok.Literal = literal
			tok.Type = lookupKeyword(strings.ToLower(literal))
			return tok
		case isDigit(l.ch):
			tok.Type = TOKEN_NUMBER
			tok.Literal = l.readNumber()
			return tok
		default:
			tok = Token{Type: TOKEN_ILLEGAL, Literal: string(l.ch)}
		}
	}

	l.readChar()
	return tok
}

// skipWhitespaceAndComments skips whitespace and SQL comments.
func (l *Lexer) skipWhitespaceAndComments() {
	for {
		// Skip whitespace
		for l.ch == ' ' || l.ch == '\t' || l.ch == '\n' || l.ch == '\r' {
			l.readChar()
		}
		// Line comment (-- ...)
		if l.ch == '-' && l.peekChar() == '-' {
			for l.ch != '\n' && l.ch != 0 {
				l.readChar()
			}
			continue
		}
		// Block comment (/* ... */)
		if l.ch == '/' && l.peekChar() == '*' {
			l.readChar() // skip /
			l.readChar() // skip *
			for l.ch != 0 {
				if l.ch == '*' && l.peekChar() == '/' {
					l.readChar() // skip *
					l.readChar() // skip /
					break
				}
				l.readChar()
			}
			continue
		}
		break
	}
}

// readString reads a single-quoted string literal.
// Handles ” escape for embedded quotes.
func (l *Lexer) readString() string {
	l.readChar() // skip opening quote
	var result strings.Builder
	for l.ch != 0 {
		if l.ch == '\'' {
			if l.peekChar() == '\'' {
				result.WriteByte('\'')
				l.readChar()
				l.readChar()
			} else {
				l.readChar() // skip closing quote
				break
			}
		} else {
			result.WriteByte(l.ch)
			l.readChar()
		}
	}
	return result.String()
}

// readQuotedIdentifier reads a double-quoted identifier.
// Handles "" escape for embedded double quotes.
func (l *Lexer) readQuotedIdentifier() string {
	l.readChar() // skip opening quote
	var result strings.Builder
	for l.ch != 0 {
		if l.ch == '"' {
			if l.peekChar() == '"' {
				result.WriteByte('"')
				l.readChar()
				l.readChar()
			} else {
				l.readChar() // skip closing quote
				break
			}
		} else {
			result.WriteByte(l.ch)
			l.readChar()
		}
	}
	return result.String()
}

// readIdentifier reads an unquoted identifier.
func (l *Lexer) readIdentifier() string {
	start := l.pos
	for isLetter(l.ch) || isDigit(l.ch) || l.ch == '_' {
		l.readChar()
	}
	return l.input[start:l.pos]
}

// readNumber reads a numeric literal (integer, decimal, or scientific).
func (l *Lexer) readNumber() string {
	start := l.pos
	for isDigit(l.ch) {
		l.readChar()
	}
	if l.ch == '.' && isDigit(l.peekChar()) {
		l.readChar() // skip .
		for isDigit(l.ch) {
			l.readChar()
		}
	}
	if l.ch == 'e' || l.ch == 'E' {
		l.readChar()
		if l.ch == '+' || l.ch == '-' {
			l.readChar()
		}
		for isDigit(l.ch) {
			l.readChar()
		}
	}
	return l.input[start:l.pos]
}

func isLetter(ch byte) bool {
	return unicode.IsLetter(rune(ch))
}

func isDigit(ch byte) bool {
	return ch >= '0' && ch <= '9'
}
