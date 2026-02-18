package cli

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"duck-demo/internal/declarative"
	"duck-demo/pkg/cli/gen"
)

func newValidateCmd(_ *gen.Client) *cobra.Command {
	var (
		configDir          string
		allowUnknownFields bool
	)

	cmd := &cobra.Command{
		Use:   "validate",
		Short: "Validate declarative configuration files offline",
		Long:  "Reads YAML configuration files and checks them for errors without contacting the server.",
		RunE: func(cmd *cobra.Command, _ []string) error {
			// 1. Load desired state from YAML files.
			desired, err := declarative.LoadDirectoryWithOptions(configDir, declarative.LoadOptions{
				AllowUnknownFields: allowUnknownFields,
			})
			if err != nil {
				return fmt.Errorf("load config: %w", err)
			}

			// 2. Validate the desired state.
			validationErrs := declarative.Validate(desired)
			if len(validationErrs) > 0 {
				if getOutputFormat(cmd) == "json" {
					errMsgs := make([]string, len(validationErrs))
					for i, ve := range validationErrs {
						errMsgs[i] = ve.Error()
					}
					if err := gen.PrintJSON(os.Stdout, map[string]interface{}{
						"valid":  false,
						"errors": errMsgs,
					}); err != nil {
						return err
					}
					os.Exit(1)
				}
				fmt.Fprintf(os.Stderr, "Configuration has %d validation error(s):\n", len(validationErrs))
				for _, ve := range validationErrs {
					fmt.Fprintf(os.Stderr, "  - %s\n", ve.Error())
				}
				os.Exit(1)
			}

			if getOutputFormat(cmd) == "json" {
				return gen.PrintJSON(os.Stdout, map[string]interface{}{
					"valid": true,
				})
			}
			_, _ = fmt.Fprintln(os.Stdout, "Configuration is valid.")
			return nil
		},
	}

	cmd.Flags().StringVar(&configDir, "config-dir", "./duck-config", "Path to configuration directory")
	cmd.Flags().BoolVar(&allowUnknownFields, "allow-unknown-fields", false, "Allow unknown YAML fields in declarative config")

	return cmd
}
