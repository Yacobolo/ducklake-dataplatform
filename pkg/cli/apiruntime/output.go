package apiruntime

import (
	"io"

	cobraruntime "github.com/Yacobolo/quackstack/pkg/apigen/runtime/cobra"
)

// OutputFormat represents the output format shared by generated and hand-written CLI commands.
type OutputFormat = cobraruntime.OutputFormat

const (
	// OutputTable renders human-friendly columnar output.
	OutputTable = cobraruntime.OutputTable
	// OutputJSON renders machine-readable JSON output.
	OutputJSON = cobraruntime.OutputJSON
	// OutputCSV renders comma-separated tabular output.
	OutputCSV = cobraruntime.OutputCSV
)

// GetTerminalWidth returns the terminal width or a default.
func GetTerminalWidth() int {
	return cobraruntime.GetTerminalWidth()
}

// IsTTY returns true if stdout is a terminal.
func IsTTY() bool {
	return cobraruntime.IsTTY()
}

// PrintTable renders tabular data to stdout using a simple columnar layout.
func PrintTable(w io.Writer, columns []string, rows [][]string) {
	cobraruntime.PrintTable(w, columns, rows)
}

// PrintJSON outputs data as formatted JSON.
func PrintJSON(w io.Writer, data interface{}) error {
	return cobraruntime.PrintJSON(w, data)
}

// PrintDetail prints a single resource as key-value pairs.
func PrintDetail(w io.Writer, fields map[string]interface{}) {
	cobraruntime.PrintDetail(w, fields)
}

// IsStdinTTY returns true if stdin is a terminal.
func IsStdinTTY() bool {
	return cobraruntime.IsStdinTTY()
}

// ConfirmPrompt asks the user for confirmation.
func ConfirmPrompt(message string) bool {
	return cobraruntime.ConfirmPrompt(message)
}

// ExtractField extracts a field from a generic map.
func ExtractField(data map[string]interface{}, field string) string {
	return cobraruntime.ExtractField(data, field)
}

// FormatValue formats a value for human-readable CLI display.
func FormatValue(v interface{}) string {
	return cobraruntime.FormatValue(v)
}

// ExtractRows extracts table rows from a paginated response.
func ExtractRows(data map[string]interface{}, columns []string) [][]string {
	return cobraruntime.ExtractRows(data, columns)
}
