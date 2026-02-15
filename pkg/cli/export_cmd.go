package cli

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"duck-demo/internal/declarative"
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
			isJSON := getOutputFormat(cmd) == "json"
			if !isJSON {
				_, _ = fmt.Fprintln(cmd.OutOrStdout(), "Fetching state from server...")
			}

			reader := NewAPIStateClient(client)
			state, err := reader.ReadState(cmd.Context())
			if err != nil {
				return fmt.Errorf("read server state: %w", err)
			}

			if err := declarative.ExportDirectory(configDir, state, overwrite); err != nil {
				return fmt.Errorf("export: %w", err)
			}

			if isJSON {
				return gen.PrintJSON(os.Stdout, map[string]string{
					"status": "ok",
					"path":   configDir,
				})
			}
			_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Exported configuration to %s\n", configDir)
			return nil
		},
	}

	cmd.Flags().StringVar(&configDir, "config-dir", "./duck-config", "Path to output configuration directory")
	cmd.Flags().BoolVar(&overwrite, "overwrite", false, "Overwrite existing files in the output directory")

	return cmd
}
