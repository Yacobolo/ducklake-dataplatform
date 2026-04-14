package cli

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"github.com/Yacobolo/quackstack/pkg/cli/apiruntime"
)

func newVersionCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Print the CLI version",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if getOutputFormat(cmd) == "json" {
				return apiruntime.PrintJSON(os.Stdout, map[string]string{
					"version": version,
					"commit":  commit,
				})
			}
			_, _ = fmt.Fprintf(os.Stdout, "quack version %s (commit: %s)\n", version, commit)
			return nil
		},
	}
}
