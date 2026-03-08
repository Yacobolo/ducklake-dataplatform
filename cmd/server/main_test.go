package main

import (
	"bytes"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandleTopLevelArgs_Help(t *testing.T) {
	t.Parallel()

	for _, arg := range []string{"help", "-h", "--help"} {
		t.Run(arg, func(t *testing.T) {
			t.Parallel()

			var out bytes.Buffer
			called := false
			handled, err := handleTopLevelArgs([]string{"server", arg}, func(_ []string) error {
				called = true
				return nil
			}, &out)

			require.NoError(t, err)
			assert.True(t, handled)
			assert.False(t, called)
			assert.Contains(t, out.String(), "Usage:")
			assert.Contains(t, out.String(), "server admin <subcommand>")
		})
	}
}

func TestHandleTopLevelArgs_AdminDelegates(t *testing.T) {
	t.Parallel()

	var out bytes.Buffer
	var gotArgs []string
	handled, err := handleTopLevelArgs([]string{"server", "admin", "users", "list"}, func(args []string) error {
		gotArgs = append([]string(nil), args...)
		return nil
	}, &out)

	require.NoError(t, err)
	assert.True(t, handled)
	assert.Equal(t, []string{"users", "list"}, gotArgs)
	assert.Empty(t, out.String())
}

func TestHandleTopLevelArgs_AdminReturnsError(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("boom")
	handled, err := handleTopLevelArgs([]string{"server", "admin"}, func(_ []string) error {
		return wantErr
	}, &bytes.Buffer{})

	require.ErrorIs(t, err, wantErr)
	assert.True(t, handled)
}

func TestHandleTopLevelArgs_NoTopLevelCommand(t *testing.T) {
	t.Parallel()

	handled, err := handleTopLevelArgs([]string{"server"}, func(_ []string) error {
		t.Fatal("admin runner should not be called")
		return nil
	}, &bytes.Buffer{})

	require.NoError(t, err)
	assert.False(t, handled)
}
