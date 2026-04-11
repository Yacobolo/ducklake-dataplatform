package cli

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"duck-demo/internal/declarative"
	"duck-demo/pkg/cli/apiruntime"
)

func newPlanCmd(client *apiruntime.Client) *cobra.Command {
	var (
		configDir          string
		output             string
		noColor            bool
		allowUnknownFields bool
	)

	cmd := &cobra.Command{
		Use:   "plan",
		Short: "Show changes required to match the declarative configuration",
		Long:  "Reads declarative CUE configuration, compares with the current server state, and shows a plan of changes.",
		RunE: func(cmd *cobra.Command, _ []string) error {
			// Check local -o flag first, then fall back to global --output.
			effectiveOutput := output
			if effectiveOutput == "text" && getOutputFormat(cmd) == "json" {
				effectiveOutput = "json"
			}
			if effectiveOutput != "text" && effectiveOutput != "json" {
				return fmt.Errorf("unsupported output format %q: use 'text' or 'json'", effectiveOutput)
			}

			// 1. Load desired state from the CUE config tree.
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

			// 3. Read current state from server.
			reader := NewAPIStateClient(client)
			actual, err := reader.ReadState(cmd.Context())
			if err != nil {
				return fmt.Errorf("read server state: %w", err)
			}
			if effectiveOutput != "json" {
				for _, warning := range reader.OptionalReadWarnings() {
					_, _ = fmt.Fprintf(os.Stderr, "warning: %s\n", warning)
				}
			}

			// 4. Diff desired vs actual.
			plan := declarative.Diff(desired, actual)

			// 5. Format output.
			switch effectiveOutput {
			case "json":
				if err := declarative.FormatJSON(os.Stdout, plan); err != nil {
					return fmt.Errorf("format plan: %w", err)
				}
			case "text":
				declarative.FormatText(os.Stdout, plan, noColor)
			}

			switch planExitCode(plan) {
			case 1:
				os.Exit(1)
			case 2:
				os.Exit(2)
			}

			return nil
		},
	}

	cmd.Flags().StringVar(&configDir, "config-dir", "./duck-config", "Path to the CUE configuration module")
	cmd.Flags().StringVarP(&output, "output", "o", "text", "Output format (text, json)")
	cmd.Flags().BoolVar(&noColor, "no-color", false, "Disable colored output")
	cmd.Flags().BoolVar(&allowUnknownFields, "allow-unknown-fields", false, "Deprecated no-op retained for compatibility with existing CLI wiring")

	return cmd
}
