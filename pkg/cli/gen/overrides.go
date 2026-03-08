package gen

import "github.com/spf13/cobra"

var (
	runOverrides     = map[string]func(*Client) func(*cobra.Command, []string) error{}
	commandOverrides = map[string]func(*cobra.Command){}
)

// RegisterRunOverride installs a custom RunE factory for an operation.
func RegisterRunOverride(operationID string, fn func(*Client) func(*cobra.Command, []string) error) {
	runOverrides[operationID] = fn
}

// RegisterOverride installs a command mutation hook for an operation.
func RegisterOverride(operationID string, fn func(*cobra.Command)) {
	commandOverrides[operationID] = fn
}
