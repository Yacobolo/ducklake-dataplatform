package cli

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"github.com/Yacobolo/quackstack/internal/declarative"
	"github.com/Yacobolo/quackstack/pkg/cli/apiruntime"
)

func newExportCmd(client *apiruntime.Client) *cobra.Command {
	var (
		configDir string
		overwrite bool
	)

	cmd := &cobra.Command{
		Use:   "export",
		Short: "Export current server state as declarative CUE configuration",
		Long:  "Reads the current state from the server and writes it as declarative CUE files.",
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
				return apiruntime.PrintJSON(os.Stdout, map[string]string{
					"status": "ok",
					"path":   configDir,
				})
			}
			_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Exported configuration to %s\n", configDir)
			return nil
		},
	}

	cmd.Flags().StringVar(&configDir, "config-dir", "./quackstack-config", "Path to the output CUE configuration module")
	cmd.Flags().BoolVar(&overwrite, "overwrite", false, "Overwrite existing files in the output directory")

	return cmd
}
