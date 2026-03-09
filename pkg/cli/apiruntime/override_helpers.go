package apiruntime

import "github.com/spf13/cobra"

// ApplyRunOverride replaces cmd.RunE when an override exists.
func ApplyRunOverride(operationID string, cmd *cobra.Command, client *Client) {
	if fn, ok := runOverrides[operationID]; ok {
		cmd.RunE = fn(client)
	}
}

// ApplyCommandOverride mutates cmd when an override exists.
func ApplyCommandOverride(operationID string, cmd *cobra.Command) {
	if fn, ok := commandOverrides[operationID]; ok {
		fn(cmd)
	}
}
