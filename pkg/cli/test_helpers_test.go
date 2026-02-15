package cli

import (
	"bytes"
	"os"
	"strings"
	"testing"
)

// captureStdout redirects os.Stdout to a pipe and returns a function
// that restores stdout and returns the captured output.
// Uses a goroutine to read concurrently, avoiding pipe buffer deadlocks.
func captureStdout(t *testing.T) func() string {
	t.Helper()
	old := os.Stdout
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	os.Stdout = w

	// Read concurrently to avoid pipe buffer deadlock on large outputs
	var buf bytes.Buffer
	done := make(chan struct{})
	go func() {
		_, _ = buf.ReadFrom(r)
		close(done)
	}()

	return func() string {
		_ = w.Close()
		<-done
		os.Stdout = old
		return buf.String()
	}
}

// containsIgnoreCase checks if s contains substr (case-insensitive).
func containsIgnoreCase(s, substr string) bool {
	return strings.Contains(strings.ToLower(s), strings.ToLower(substr))
}
