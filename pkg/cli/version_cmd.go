package cli

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

func newVersionCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Print the CLI version",
		Run: func(_ *cobra.Command, _ []string) {
			_, _ = fmt.Fprintf(os.Stdout, "duck version %s (commit: %s)\n", version, commit)
		},
	}
}
