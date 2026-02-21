package cli

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestZeroArgCommandsRejectUnexpectedPositionalArgs(t *testing.T) {
	t.Setenv("HOME", t.TempDir())

	bootstrap := newRootCmd()
	bootstrap.SetArgs([]string{"config", "set-profile", "--name", "default", "--host", "http://127.0.0.1:65535"})
	require.NoError(t, bootstrap.Execute())

	tests := []struct {
		name string
		args []string
	}{
		{name: "version", args: []string{"version", "extra"}},
		{name: "commands", args: []string{"commands", "extra"}},
		{name: "config show", args: []string{"config", "show", "extra"}},
		{name: "commands json", args: []string{"commands", "--output", "json", "extra"}},
		{name: "config set-profile", args: []string{"config", "set-profile", "--name", "p", "--host", "http://127.0.0.1:65535", "extra"}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cmd := newRootCmd()
			cmd.SetArgs(tc.args)
			err := cmd.Execute()
			require.Error(t, err)
			require.Contains(t, err.Error(), "unknown command \"extra\"")
		})
	}
}
