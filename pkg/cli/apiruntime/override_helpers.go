package apiruntime

import (
	cobraruntime "duck-demo/apigen/runtime/cobra"
	"github.com/spf13/cobra"
)

// ApplyRunOverride replaces cmd.RunE when an override exists.
func ApplyRunOverride(operationID string, cmd *cobra.Command, client *Client) {
	cobraruntime.ApplyRunOverride(operationID, cmd, client)
}

// ApplyCommandOverride mutates cmd when an override exists.
func ApplyCommandOverride(operationID string, cmd *cobra.Command) {
	cobraruntime.ApplyCommandOverride(operationID, cmd)
}
