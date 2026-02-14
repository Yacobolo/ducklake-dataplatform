package cli

import (
	"fmt"

	"github.com/spf13/cobra"

	"duck-demo/pkg/cli/gen"
)

func newExportCmd(client *gen.Client) *cobra.Command {
	var (
		configDir string
		overwrite bool
	)

	cmd := &cobra.Command{
		Use:   "export",
		Short: "Export current server state as declarative YAML configuration",
		Long:  "Reads the current state from the server and writes it as YAML configuration files.",
		RunE: func(cmd *cobra.Command, _ []string) error {
			// TODO: implement exporter that reads server state and writes YAML.
			// For now, validate that the server is reachable by constructing
			// the state client, then report that the feature is pending.
			_ = NewAPIStateClient(client)
			_ = configDir
			_ = overwrite

			fmt.Fprintln(cmd.OutOrStdout(), "Export is not yet implemented.")
			return nil
		},
	}

	cmd.Flags().StringVar(&configDir, "config-dir", "./duck-config", "Path to output configuration directory")
	cmd.Flags().BoolVar(&overwrite, "overwrite", false, "Overwrite existing files in the output directory")

	return cmd
}
