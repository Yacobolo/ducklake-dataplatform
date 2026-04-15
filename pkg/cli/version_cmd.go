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
		Short: "Print build information for the CLI",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if getOutputFormat(cmd) == "json" {
				return apiruntime.PrintJSON(os.Stdout, map[string]string{
					"version":    version,
					"commit":     commit,
					"branch":     branch,
					"tag":        tag,
					"build_time": buildTime,
				})
			}
			_, _ = fmt.Fprintf(os.Stdout, "quack version %s (commit: %s, branch: %s, tag: %s, build time: %s)\n", version, commit, branch, tag, buildTime)
			return nil
		},
	}
}
