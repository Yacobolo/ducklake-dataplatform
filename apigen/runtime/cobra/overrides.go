package cobra

import spcobra "github.com/spf13/cobra"

var (
	runOverrides     = map[string]func(*Client) func(*spcobra.Command, []string) error{}
	commandOverrides = map[string]func(*spcobra.Command){}
)

// RegisterRunOverride installs a custom RunE factory for an operation.
func RegisterRunOverride(operationID string, fn func(*Client) func(*spcobra.Command, []string) error) {
	runOverrides[operationID] = fn
}

// RegisterOverride installs a command mutation hook for an operation.
func RegisterOverride(operationID string, fn func(*spcobra.Command)) {
	commandOverrides[operationID] = fn
}

// ApplyRunOverride replaces cmd.RunE when an override exists.
func ApplyRunOverride(operationID string, cmd *spcobra.Command, client *Client) {
	if fn, ok := runOverrides[operationID]; ok {
		cmd.RunE = fn(client)
	}
}

// ApplyCommandOverride mutates cmd when an override exists.
func ApplyCommandOverride(operationID string, cmd *spcobra.Command) {
	if fn, ok := commandOverrides[operationID]; ok {
		fn(cmd)
	}
}
