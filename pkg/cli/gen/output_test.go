package gen

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// Bug-demonstrating tests (issue #102) — these FAIL against current code
// ---------------------------------------------------------------------------

func TestExtractField_MapValue(t *testing.T) {
	// Current code uses fmt.Sprintf("%v", v) which produces "map[k:v]"
	// instead of valid JSON. This test demonstrates the bug.
	data := map[string]interface{}{
		"nested": map[string]interface{}{"k": "v"},
	}

	got := ExtractField(data, "nested")

	// Should produce valid JSON, not Go's internal map representation.
	assert.JSONEq(t, `{"k":"v"}`, got, "map values should be serialized as JSON, not Go fmt output")
}

func TestExtractField_SliceValue(t *testing.T) {
	// Current code uses fmt.Sprintf("%v", v) which produces "[a b]"
	// instead of valid JSON. This test demonstrates the bug.
	data := map[string]interface{}{
		"tags": []interface{}{"a", "b"},
	}

	got := ExtractField(data, "tags")

	// Should produce valid JSON, not Go's internal slice representation.
	assert.JSONEq(t, `["a","b"]`, got, "slice values should be serialized as JSON, not Go fmt output")
}

func TestPrintDetail_NilField(t *testing.T) {
	// Current code outputs "<nil>" for nil values via %v.
	var buf bytes.Buffer
	fields := map[string]interface{}{
		"status": nil,
	}

	PrintDetail(&buf, fields)

	output := buf.String()
	assert.NotContains(t, output, "<nil>", "nil fields should not render as Go's <nil>")
}

func TestPrintDetail_MapField(t *testing.T) {
	// Current code outputs "map[k:v]" for nested map values via %v.
	var buf bytes.Buffer
	fields := map[string]interface{}{
		"config": map[string]interface{}{"key": "val"},
	}

	PrintDetail(&buf, fields)

	output := buf.String()
	assert.NotContains(t, output, "map[", "map fields should not render as Go's map[...] syntax")
}

func TestPrintDetail_SliceField(t *testing.T) {
	// Current code outputs "[a b]" for slice values via %v.
	var buf bytes.Buffer
	fields := map[string]interface{}{
		"items": []interface{}{"a", "b"},
	}

	PrintDetail(&buf, fields)

	output := buf.String()
	assert.NotContains(t, output, "[a b]", "slice fields should not render as Go's [a b] syntax")
}

// ---------------------------------------------------------------------------
// Passing baseline tests
// ---------------------------------------------------------------------------

func TestPrintTable_Basic(t *testing.T) {
	var buf bytes.Buffer
	columns := []string{"name", "age"}
	rows := [][]string{
		{"Alice", "30"},
		{"Bob", "25"},
	}

	PrintTable(&buf, columns, rows)
	output := buf.String()
	lines := strings.Split(strings.TrimRight(output, "\n"), "\n")

	require.Len(t, lines, 3, "expected header + 2 data rows")

	// Headers should be uppercased.
	assert.Contains(t, lines[0], "NAME")
	assert.Contains(t, lines[0], "AGE")

	// Rows should contain the data.
	assert.Contains(t, lines[1], "Alice")
	assert.Contains(t, lines[1], "30")
	assert.Contains(t, lines[2], "Bob")
	assert.Contains(t, lines[2], "25")
}

func TestPrintTable_EmptyColumns(t *testing.T) {
	var buf bytes.Buffer

	PrintTable(&buf, []string{}, [][]string{{"a"}})

	assert.Empty(t, buf.String(), "empty columns should produce no output")
}

func TestPrintTable_EmptyRows(t *testing.T) {
	var buf bytes.Buffer
	columns := []string{"id", "value"}

	PrintTable(&buf, columns, nil)
	output := buf.String()
	lines := strings.Split(strings.TrimRight(output, "\n"), "\n")

	require.Len(t, lines, 1, "only the header line should be present")
	assert.Contains(t, lines[0], "ID")
	assert.Contains(t, lines[0], "VALUE")
}

func TestPrintTable_ColumnSeparator(t *testing.T) {
	var buf bytes.Buffer
	columns := []string{"a", "b"}
	rows := [][]string{{"1", "2"}}

	PrintTable(&buf, columns, rows)
	output := buf.String()
	lines := strings.Split(strings.TrimRight(output, "\n"), "\n")

	require.Len(t, lines, 2)

	// The header should have exactly two spaces between column values.
	// Column "a" is width 1, so header is "A  B".
	assert.Contains(t, lines[0], "  ", "columns should be separated by two spaces")
	assert.Contains(t, lines[1], "  ", "row values should be separated by two spaces")
}

func TestPrintJSON_Basic(t *testing.T) {
	var buf bytes.Buffer
	data := map[string]string{"hello": "world"}

	err := PrintJSON(&buf, data)
	require.NoError(t, err)

	// Output should be valid JSON.
	var parsed map[string]string
	err = json.Unmarshal(buf.Bytes(), &parsed)
	require.NoError(t, err)
	assert.Equal(t, "world", parsed["hello"])

	// Should be indented (contains newline + spaces).
	assert.Contains(t, buf.String(), "\n  ")
}

func TestPrintJSON_NilInput(t *testing.T) {
	var buf bytes.Buffer

	err := PrintJSON(&buf, nil)
	require.NoError(t, err)

	assert.Equal(t, "null\n", buf.String())
}

func TestPrintDetail_SortedKeys(t *testing.T) {
	var buf bytes.Buffer
	fields := map[string]interface{}{
		"zebra":  "z",
		"apple":  "a",
		"mango":  "m",
		"banana": "b",
	}

	PrintDetail(&buf, fields)
	output := buf.String()
	lines := strings.Split(strings.TrimRight(output, "\n"), "\n")

	require.Len(t, lines, 4)

	// Extract key from each line (before the colon).
	keys := make([]string, len(lines))
	for i, line := range lines {
		parts := strings.SplitN(line, ":", 2)
		require.NotEmpty(t, parts, "line should contain a colon")
		keys[i] = parts[0]
	}

	assert.Equal(t, []string{"apple", "banana", "mango", "zebra"}, keys,
		"keys should appear in alphabetical order")
}

func TestPrintDetail_Padding(t *testing.T) {
	var buf bytes.Buffer
	fields := map[string]interface{}{
		"id":          "123",
		"description": "some text",
	}

	PrintDetail(&buf, fields)
	output := buf.String()
	lines := strings.Split(strings.TrimRight(output, "\n"), "\n")

	require.Len(t, lines, 2)

	// "description" is 11 chars, "id" is 2 chars.
	// "id" line should have 9 extra spaces of padding so colons align.
	// Format: "key:<padding>  value"
	idLine := lines[1] // "id" sorts after "description"
	if strings.HasPrefix(lines[0], "id") {
		idLine = lines[0]
	}
	// The id line should have padding between "id:" and the value.
	// maxKeyLen = len("description") = 11, len("id") = 2, padding = 9 spaces.
	assert.Contains(t, idLine, "id:"+strings.Repeat(" ", 9)+"  ")
}

func TestExtractField_StringValue(t *testing.T) {
	data := map[string]interface{}{"name": "alice"}
	assert.Equal(t, "alice", ExtractField(data, "name"))
}

func TestExtractField_MissingKey(t *testing.T) {
	data := map[string]interface{}{"name": "alice"}
	assert.Empty(t, ExtractField(data, "missing"))
}

func TestExtractField_NilValue(t *testing.T) {
	data := map[string]interface{}{"name": nil}
	assert.Empty(t, ExtractField(data, "name"))
}

func TestExtractField_FloatValue(t *testing.T) {
	data := map[string]interface{}{"count": 42.0}
	got := ExtractField(data, "count")
	// fmt.Sprintf("%v", 42.0) produces "42", which is acceptable.
	assert.Equal(t, "42", got)
}

func TestExtractRows_Basic(t *testing.T) {
	data := map[string]interface{}{
		"data": []interface{}{
			map[string]interface{}{"id": "1", "name": "foo"},
			map[string]interface{}{"id": "2", "name": "bar"},
		},
	}
	columns := []string{"id", "name"}

	rows := ExtractRows(data, columns)

	require.Len(t, rows, 2)
	assert.Equal(t, []string{"1", "foo"}, rows[0])
	assert.Equal(t, []string{"2", "bar"}, rows[1])
}

func TestExtractRows_MissingDataKey(t *testing.T) {
	data := map[string]interface{}{
		"items": []interface{}{},
	}

	rows := ExtractRows(data, []string{"id"})
	assert.Nil(t, rows)
}

func TestExtractRows_NonMapItems(t *testing.T) {
	data := map[string]interface{}{
		"data": []interface{}{
			map[string]interface{}{"id": "1"},
			"not a map",
			42,
			map[string]interface{}{"id": "3"},
		},
	}

	rows := ExtractRows(data, []string{"id"})

	require.Len(t, rows, 2, "non-map items should be skipped")
	assert.Equal(t, []string{"1"}, rows[0])
	assert.Equal(t, []string{"3"}, rows[1])
}

func TestExtractRows_MissingColumns(t *testing.T) {
	data := map[string]interface{}{
		"data": []interface{}{
			map[string]interface{}{"id": "1"},
		},
	}
	columns := []string{"id", "name", "email"}

	rows := ExtractRows(data, columns)

	require.Len(t, rows, 1)
	assert.Equal(t, []string{"1", "", ""}, rows[0],
		"missing columns should produce empty strings")
}
