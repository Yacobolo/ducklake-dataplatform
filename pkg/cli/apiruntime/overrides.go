package apiruntime

import (
	cobraruntime "duck-demo/pkg/apigen/runtime/cobra"
	"github.com/spf13/cobra"
)

// RegisterRunOverride installs a custom RunE factory for an operation.
func RegisterRunOverride(operationID string, fn func(*Client) func(*cobra.Command, []string) error) {
	cobraruntime.RegisterRunOverride(operationID, fn)
}

// RegisterOverride installs a command mutation hook for an operation.
func RegisterOverride(operationID string, fn func(*cobra.Command)) {
	cobraruntime.RegisterOverride(operationID, fn)
}
