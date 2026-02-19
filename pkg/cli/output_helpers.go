package cli

import (
	"fmt"

	"github.com/spf13/cobra"
)

// getOutputFormat returns the effective output format from the root command's persistent flags.
func getOutputFormat(cmd *cobra.Command) string {
	v, _ := cmd.Root().PersistentFlags().GetString("output")
	return v
}

func validateOutputFormat(output string) error {
	if output != "" && output != "table" && output != "json" {
		return fmt.Errorf("unsupported output format %q: use 'table' or 'json'", output)
	}
	return nil
}
