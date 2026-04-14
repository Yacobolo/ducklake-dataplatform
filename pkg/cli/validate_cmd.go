package cli

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"github.com/Yacobolo/quackstack/internal/declarative"
	"github.com/Yacobolo/quackstack/pkg/cli/apiruntime"
)

func newValidateCmd(_ *apiruntime.Client) *cobra.Command {
	var (
		configDir          string
		allowUnknownFields bool
	)

	cmd := &cobra.Command{
		Use:   "validate",
		Short: "Validate declarative configuration files offline",
		Long:  "Reads declarative CUE configuration and checks it for errors without contacting the server.",
		RunE: func(cmd *cobra.Command, _ []string) error {
			// 1. Load desired state from the CUE config tree.
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
					if err := apiruntime.PrintJSON(os.Stdout, map[string]interface{}{
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
				return apiruntime.PrintJSON(os.Stdout, map[string]interface{}{
					"valid": true,
				})
			}
			_, _ = fmt.Fprintln(os.Stdout, "Configuration is valid.")
			return nil
		},
	}

	cmd.Flags().StringVar(&configDir, "config-dir", "./quackstack-config", "Path to the CUE configuration module")
	cmd.Flags().BoolVar(&allowUnknownFields, "allow-unknown-fields", false, "Deprecated no-op retained for compatibility with existing CLI wiring")

	return cmd
}
