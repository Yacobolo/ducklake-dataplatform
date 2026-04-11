package cli

import "github.com/spf13/cobra"

func newDeclarativeCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "declarative",
		Short: "Manage declarative CUE configuration",
		Long:  "Generate and work with declarative CUE configuration modules for Duck.",
	}

	cmd.AddCommand(newDeclarativeInitCmd())
	return cmd
}
