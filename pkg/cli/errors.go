package cli

import "errors"

// CLIError carries an explicit process exit code and optional JSON payload.
// The root executor is responsible for rendering it exactly once.
//nolint:revive // exported name is intentional for callers outside this package
type CLIError struct {
	Code        int
	Message     string
	JSONPayload any
}

func (e *CLIError) Error() string {
	if e == nil {
		return ""
	}
	return e.Message
}

func exitCodeForError(err error) int {
	var cliErr *CLIError
	if errors.As(err, &cliErr) && cliErr != nil && cliErr.Code > 0 {
		return cliErr.Code
	}
	return 1
}
