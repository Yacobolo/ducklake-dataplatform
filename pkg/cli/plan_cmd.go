package cli

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"duck-demo/internal/declarative"
	"duck-demo/pkg/cli/gen"
)

func newPlanCmd(client *gen.Client) *cobra.Command {
	var (
		configDir                string
		output                   string
		noColor                  bool
		allowUnknownFields       bool
		legacyOptionalReadErrors bool
	)

	cmd := &cobra.Command{
		Use:   "plan",
		Short: "Show changes required to match the declarative configuration",
		Long:  "Reads YAML configuration files, compares with the current server state, and shows a plan of changes.",
		RunE: func(cmd *cobra.Command, _ []string) error {
			// 1. Load desired state from YAML files.
			desired, err := declarative.LoadDirectoryWithOptions(configDir, declarative.LoadOptions{
				AllowUnknownFields: allowUnknownFields,
			})
			if err != nil {
				return fmt.Errorf("load config: %w", err)
			}

			// 2. Validate the desired state.
			if validationErrs := declarative.Validate(desired); len(validationErrs) > 0 {
				fmt.Fprintf(os.Stderr, "Configuration has %d validation error(s):\n", len(validationErrs))
				for _, ve := range validationErrs {
					fmt.Fprintf(os.Stderr, "  - %s\n", ve.Error())
				}
				os.Exit(1)
			}

			compatMode := CapabilityCompatibilityStrict
			if legacyOptionalReadErrors {
				compatMode = CapabilityCompatibilityLegacy
			}

			// 3. Read current state from server.
			reader := NewAPIStateClientWithOptions(client, APIStateClientOptions{CompatibilityMode: compatMode})
			actual, err := reader.ReadState(cmd.Context())
			if err != nil {
				return fmt.Errorf("read server state: %w", err)
			}
			for _, warning := range reader.OptionalReadWarnings() {
				_, _ = fmt.Fprintf(os.Stderr, "warning: %s\n", warning)
			}

			// 4. Diff desired vs actual.
			plan := declarative.Diff(desired, actual)

			// 5. Format output.
			// Check local -o flag first, then fall back to global --output
			effectiveOutput := output
			if effectiveOutput == "text" && getOutputFormat(cmd) == "json" {
				effectiveOutput = "json"
			}
			switch effectiveOutput {
			case "json":
				if err := declarative.FormatJSON(os.Stdout, plan); err != nil {
					return fmt.Errorf("format plan: %w", err)
				}
			default:
				declarative.FormatText(os.Stdout, plan, noColor)
			}

			// 6. Exit code 2 if there are changes (useful for CI).
			if plan.HasChanges() {
				os.Exit(2)
			}

			return nil
		},
	}

	cmd.Flags().StringVar(&configDir, "config-dir", "./duck-config", "Path to configuration directory")
	cmd.Flags().StringVarP(&output, "output", "o", "text", "Output format (text, json)")
	cmd.Flags().BoolVar(&noColor, "no-color", false, "Disable colored output")
	cmd.Flags().BoolVar(&allowUnknownFields, "allow-unknown-fields", false, "Allow unknown YAML fields in declarative config")
	cmd.Flags().BoolVar(&legacyOptionalReadErrors, "legacy-optional-read-errors", false, "Treat transport errors as optional for model/macro capability checks")

	return cmd
}
