package ddl

import (
	"fmt"
	"regexp"
	"strings"
)

// identifierRe allows alphanumeric + underscores, starting with a letter or underscore.
var identifierRe = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)

// columnTypeRe matches simple DuckDB type names, optionally with precision/scale parameters.
// Accepted forms:
//
//	WORD                         → INTEGER, VARCHAR, BOOLEAN, etc.
//	WORD(digits)                 → VARCHAR(255), DECIMAL(10)
//	WORD(digits, digits)         → DECIMAL(10,2), NUMERIC(18,4)
//	WORD[]                       → INTEGER[], VARCHAR[]
//	WORD(digits)[]               → VARCHAR(255)[]
//	WORD(digits, digits)[]       → DECIMAL(10,2)[]
//
// Case-insensitive. Rejects anything with semicolons, parens in unexpected positions,
// comments, or other SQL injection vectors.
var columnTypeRe = regexp.MustCompile(`(?i)^[A-Z][A-Z0-9_ ]*(?:\(\s*\d+\s*(?:,\s*\d+\s*)?\))?(?:\[\])?$`)

// maxIdentifierLen is the maximum length allowed for a SQL identifier.
const maxIdentifierLen = 128

// maxColumnTypeLen is the maximum length allowed for a column type string.
const maxColumnTypeLen = 64

// ValidateIdentifier checks that name is a safe SQL identifier:
//   - Non-empty
//   - At most 128 characters
//   - Matches [a-zA-Z_][a-zA-Z0-9_]*
func ValidateIdentifier(name string) error {
	if name == "" {
		return fmt.Errorf("name is required")
	}
	if len(name) > maxIdentifierLen {
		return fmt.Errorf("name must be at most %d characters", maxIdentifierLen)
	}
	if !identifierRe.MatchString(name) {
		return fmt.Errorf("name must match [a-zA-Z_][a-zA-Z0-9_]*")
	}
	return nil
}

// QuoteIdentifier wraps a SQL identifier in double quotes, escaping any
// embedded double-quote characters by doubling them (standard SQL).
//
// Always quotes unconditionally — the caller should validate first if needed.
func QuoteIdentifier(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

// QuoteLiteral wraps a string value in single quotes, escaping any
// embedded single-quote characters by doubling them (standard SQL).
func QuoteLiteral(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "''") + "'"
}

// ValidateColumnType checks that typeName is a safe DuckDB column type:
//   - Non-empty
//   - At most 64 characters
//   - Matches the allowed type pattern (word, optionally with precision/scale, optionally array)
//   - Does not contain SQL injection patterns (semicolons, comments, etc.)
func ValidateColumnType(typeName string) error {
	if typeName == "" {
		return fmt.Errorf("column type is required")
	}
	if len(typeName) > maxColumnTypeLen {
		return fmt.Errorf("column type must be at most %d characters", maxColumnTypeLen)
	}
	// Reject obvious injection patterns before regex check
	if strings.ContainsAny(typeName, ";-'\"\\") {
		return fmt.Errorf("column type contains invalid characters")
	}
	if !columnTypeRe.MatchString(typeName) {
		return fmt.Errorf("column type %q is not a recognized type pattern", typeName)
	}
	return nil
}
