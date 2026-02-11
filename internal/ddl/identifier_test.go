package ddl

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValidateIdentifier(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantErr string
	}{
		// Valid cases
		{name: "simple", input: "users"},
		{name: "underscore_prefix", input: "_temp"},
		{name: "mixed_case", input: "MyTable"},
		{name: "with_digits", input: "table1"},
		{name: "all_upper", input: "SCHEMA"},
		{name: "max_length", input: strings.Repeat("a", 128)},

		// Invalid cases
		{name: "empty", input: "", wantErr: "name is required"},
		{name: "too_long", input: strings.Repeat("a", 129), wantErr: "at most 128 characters"},
		{name: "starts_with_digit", input: "1table", wantErr: "must match"},
		{name: "contains_space", input: "my table", wantErr: "must match"},
		{name: "contains_hyphen", input: "my-table", wantErr: "must match"},
		{name: "contains_dot", input: "schema.table", wantErr: "must match"},
		{name: "contains_semicolon", input: "foo;bar", wantErr: "must match"},
		{name: "contains_quote", input: `foo"bar`, wantErr: "must match"},
		{name: "sql_injection", input: "foo; DROP TABLE", wantErr: "must match"},
		{name: "contains_paren", input: "foo()", wantErr: "must match"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateIdentifier(tt.input)
			if tt.wantErr == "" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
			}
		})
	}
}

func TestQuoteIdentifier(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{name: "simple", input: "users", want: `"users"`},
		{name: "with_double_quote", input: `my"table`, want: `"my""table"`},
		{name: "multiple_quotes", input: `a"b"c`, want: `"a""b""c"`},
		{name: "empty", input: "", want: `""`},
		{name: "uppercase", input: "Users", want: `"Users"`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := QuoteIdentifier(tt.input)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestQuoteLiteral(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{name: "simple", input: "hello", want: "'hello'"},
		{name: "with_single_quote", input: "it's", want: "'it''s'"},
		{name: "multiple_quotes", input: "a'b'c", want: "'a''b''c'"},
		{name: "empty", input: "", want: "''"},
		{name: "with_backslash", input: `path\to\file`, want: `'path\to\file'`},
		{name: "s3_path", input: "s3://bucket/path", want: "'s3://bucket/path'"},
		{name: "path_with_quote", input: "/tmp/it's here/db", want: "'/tmp/it''s here/db'"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := QuoteLiteral(tt.input)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestValidateColumnType(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantErr string
	}{
		// Valid types
		{name: "integer", input: "INTEGER"},
		{name: "varchar", input: "VARCHAR"},
		{name: "boolean", input: "BOOLEAN"},
		{name: "timestamp", input: "TIMESTAMP"},
		{name: "double", input: "DOUBLE"},
		{name: "bigint", input: "BIGINT"},
		{name: "date", input: "DATE"},
		{name: "blob", input: "BLOB"},
		{name: "float", input: "FLOAT"},
		{name: "smallint", input: "SMALLINT"},
		{name: "tinyint", input: "TINYINT"},
		{name: "hugeint", input: "HUGEINT"},
		{name: "varchar_with_length", input: "VARCHAR(255)"},
		{name: "decimal_precision", input: "DECIMAL(10)"},
		{name: "decimal_precision_scale", input: "DECIMAL(10,2)"},
		{name: "numeric_precision_scale", input: "NUMERIC(18, 4)"},
		{name: "integer_array", input: "INTEGER[]"},
		{name: "varchar_array", input: "VARCHAR[]"},
		{name: "varchar_length_array", input: "VARCHAR(100)[]"},
		{name: "lowercase", input: "integer"},
		{name: "mixed_case", input: "Integer"},
		{name: "timestamp_with_tz", input: "TIMESTAMP WITH TIME ZONE"},
		{name: "double_precision", input: "DOUBLE PRECISION"},

		// Invalid types — injection attempts
		{name: "empty", input: "", wantErr: "column type is required"},
		{name: "too_long", input: strings.Repeat("A", 65), wantErr: "at most 64 characters"},
		{name: "semicolon_injection", input: "INTEGER); DROP TABLE foo; --", wantErr: "invalid characters"},
		{name: "quote_injection", input: "VARCHAR'; DROP TABLE foo; --", wantErr: "invalid characters"},
		{name: "double_quote", input: `VARCHAR"`, wantErr: "invalid characters"},
		{name: "backslash", input: `VARCHAR\`, wantErr: "invalid characters"},
		{name: "comment_injection", input: "INTEGER -- drop", wantErr: "invalid characters"},
		{name: "starts_with_digit", input: "123", wantErr: "not a recognized type"},
		{name: "starts_with_paren", input: "(10)", wantErr: "not a recognized type"},
		{name: "nested_parens", input: "DECIMAL((10))", wantErr: "not a recognized type"},
		{name: "just_parens", input: "()", wantErr: "not a recognized type"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateColumnType(tt.input)
			if tt.wantErr == "" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
			}
		})
	}
}
