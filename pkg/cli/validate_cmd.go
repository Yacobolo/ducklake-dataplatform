package cli

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"duck-demo/internal/declarative"
	"duck-demo/pkg/cli/gen"
)

func newValidateCmd(_ *gen.Client) *cobra.Command {
	var configDir string

	cmd := &cobra.Command{
		Use:   "validate",
		Short: "Validate declarative configuration files offline",
		Long:  "Reads YAML configuration files and checks them for errors without contacting the server.",
		RunE: func(_ *cobra.Command, _ []string) error {
			// 1. Load desired state from YAML files.
			desired, err := declarative.LoadDirectory(configDir)
			if err != nil {
				return fmt.Errorf("load config: %w", err)
			}

			// 2. Validate the desired state.
			validationErrs := declarative.Validate(desired)
			if len(validationErrs) > 0 {
				fmt.Fprintf(os.Stderr, "Configuration has %d validation error(s):\n", len(validationErrs))
				for _, ve := range validationErrs {
					fmt.Fprintf(os.Stderr, "  - %s\n", ve.Error())
				}
				os.Exit(1)
			}

			_, _ = fmt.Fprintln(os.Stdout, "Configuration is valid.")
			return nil
		},
	}

	cmd.Flags().StringVar(&configDir, "config-dir", "./duck-config", "Path to configuration directory")

	return cmd
}
