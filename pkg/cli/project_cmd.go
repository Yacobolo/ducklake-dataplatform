package cli

import "github.com/spf13/cobra"

func newProjectCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "project",
		Short: "Create and manage project scaffolds",
	}
	cmd.AddCommand(newProjectInitCmd())
	return cmd
}

func newProjectInitCmd() *cobra.Command {
	cmd := newDeclarativeInitCmd()
	cmd.Use = "init"
	cmd.Short = "Generate a starter project scaffold"
	return cmd
}
