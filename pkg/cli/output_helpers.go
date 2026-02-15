package cli

import "github.com/spf13/cobra"

// getOutputFormat returns the effective output format from the root command's persistent flags.
func getOutputFormat(cmd *cobra.Command) string {
	v, _ := cmd.Root().PersistentFlags().GetString("output")
	return v
}
