package cli

import "github.com/spf13/cobra"

func newServerCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "server",
		Short: "Server and operator helpers",
	}

	env := &cobra.Command{
		Use:   "env",
		Short: "Manage server environment scaffolds",
	}
	env.AddCommand(newServerEnvInitCmd())
	cmd.AddCommand(env)

	return cmd
}

func newServerEnvInitCmd() *cobra.Command {
	cmd := newConfigInitCmd()
	cmd.Use = "init"
	cmd.Short = "Generate a starter server .env file"
	return cmd
}
