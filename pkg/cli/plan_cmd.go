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
		configDir string
		output    string
		noColor   bool
	)

	cmd := &cobra.Command{
		Use:   "plan",
		Short: "Show changes required to match the declarative configuration",
		Long:  "Reads YAML configuration files, compares with the current server state, and shows a plan of changes.",
		RunE: func(cmd *cobra.Command, _ []string) error {
			// 1. Load desired state from YAML files.
			desired, err := declarative.LoadDirectory(configDir)
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

			// 4. Diff desired vs actual.
			plan := declarative.Diff(desired, actual)

			// 5. Format output.
			switch output {
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

	return cmd
}
