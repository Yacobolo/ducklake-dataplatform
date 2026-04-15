package cli

// CLIError carries an explicit process exit code and optional JSON payload.
// The root executor is responsible for rendering it exactly once.
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
	if cliErr, ok := err.(*CLIError); ok && cliErr != nil && cliErr.Code > 0 {
		return cliErr.Code
	}
	return 1
}
