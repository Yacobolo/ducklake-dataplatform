package cli

import (
	"fmt"
	"os"
	"strings"

	"github.com/spf13/cobra"

	"github.com/Yacobolo/quackstack/internal/declarative"
	"github.com/Yacobolo/quackstack/pkg/cli/apiruntime"
)

func newPlanCmd(client *apiruntime.Client) *cobra.Command {
	var (
		configDir          string
		output             string
		noColor            bool
		allowUnknownFields bool
		loadFlags          declarativeLoadFlags
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
			loadOptions, err := loadFlags.loadOptions(allowUnknownFields)
			if err != nil {
				return err
			}
			desired, err := declarative.LoadDirectoryWithOptions(configDir, loadOptions)
			if err != nil {
				return fmt.Errorf("load config: %w", err)
			}

			// 2. Validate the desired state.
			if validationErrs := declarative.Validate(desired); len(validationErrs) > 0 {
				errMsgs := make([]string, 0, len(validationErrs))
				for _, ve := range validationErrs {
					errMsgs = append(errMsgs, ve.Error())
				}
				return &CLIError{
					Code:        1,
					Message:     fmt.Sprintf("configuration has %d validation error(s): %s", len(validationErrs), strings.Join(errMsgs, "; ")),
					JSONPayload: map[string]any{"valid": false, "errors": errMsgs},
				}
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
				return &CLIError{
					Code:        1,
					JSONPayload: map[string]any{"status": "error", "errors": planErrorMessages(plan)},
				}
			case 2:
				return &CLIError{Code: 2}
			}

			return nil
		},
	}

	cmd.Flags().StringVar(&configDir, "config-dir", "./quackstack-config", "Path to the CUE configuration module")
	cmd.Flags().StringVarP(&output, "output", "o", "text", "Output format (text, json)")
	cmd.Flags().BoolVar(&noColor, "no-color", false, "Disable colored output")
	cmd.Flags().BoolVar(&allowUnknownFields, "allow-unknown-fields", false, "Deprecated no-op retained for compatibility with existing CLI wiring")
	addDeclarativeLoadFlags(cmd, &loadFlags)

	return cmd
}
