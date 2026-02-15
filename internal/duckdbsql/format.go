package duckdbsql

import (
	"strings"
)

// Format formats a statement AST back to a SQL string.
// The output is flat (no pretty-printing) and always double-quotes identifiers.
func Format(stmt Stmt) string {
	f := &formatter{}
	f.formatStmt(stmt)
	return strings.TrimSpace(f.buf.String())
}

// FormatExpr formats an expression AST back to a SQL string.
func FormatExpr(expr Expr) string {
	f := &formatter{}
	f.formatExpr(expr)
	return strings.TrimSpace(f.buf.String())
}

// formatter is a simple SQL string builder. No indentation or pretty-printing.
type formatter struct {
	buf strings.Builder
}

func (f *formatter) write(s string) {
	f.buf.WriteString(s)
}

func (f *formatter) space() {
	f.buf.WriteByte(' ')
}

// quoteIdent unconditionally double-quotes an identifier.
// Internal double quotes are escaped by doubling.
func quoteIdent(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}

// writeIdent writes a quoted identifier.
func (f *formatter) writeIdent(s string) {
	f.write(quoteIdent(s))
}

// commaSep writes items separated by ", ".
func (f *formatter) commaSep(n int, fn func(i int)) {
	for i := 0; i < n; i++ {
		if i > 0 {
			f.write(", ")
		}
		fn(i)
	}
}
