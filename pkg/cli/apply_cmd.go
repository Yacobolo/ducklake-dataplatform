package cli

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	"github.com/spf13/cobra"

	"duck-demo/internal/declarative"
	"duck-demo/pkg/cli/gen"
)

func newApplyCmd(client *gen.Client) *cobra.Command {
	var (
		configDir                string
		autoApprove              bool
		noColor                  bool
		allowUnknownFields       bool
		legacyOptionalReadErrors bool
	)

	cmd := &cobra.Command{
		Use:   "apply",
		Short: "Apply declarative configuration changes to the server",
		Long:  "Reads YAML configuration files, compares with the current server state, and applies the changes.",
		RunE: func(cmd *cobra.Command, _ []string) error {
			isJSON := getOutputFormat(cmd) == "json"

			compatMode := CapabilityCompatibilityStrict
			if legacyOptionalReadErrors {
				compatMode = CapabilityCompatibilityLegacy
			}

			// 1. Load desired state from YAML files.
			desired, err := declarative.LoadDirectoryWithOptions(configDir, declarative.LoadOptions{
				AllowUnknownFields: allowUnknownFields,
			})
			if err != nil {
				return fmt.Errorf("load config: %w", err)
			}

			// 2. Validate the desired state.
			if validationErrs := declarative.Validate(desired); len(validationErrs) > 0 {
				if isJSON {
					errMsgs := make([]string, len(validationErrs))
					for i, ve := range validationErrs {
						errMsgs[i] = ve.Error()
					}
					_ = gen.PrintJSON(os.Stdout, map[string]interface{}{
						"status": "error",
						"errors": errMsgs,
					})
					os.Exit(1)
				}
				fmt.Fprintf(os.Stderr, "Configuration has %d validation error(s):\n", len(validationErrs))
				for _, ve := range validationErrs {
					fmt.Fprintf(os.Stderr, "  - %s\n", ve.Error())
				}
				os.Exit(1)
			}

			// 3. Read current state from server.
			stateClient := NewAPIStateClientWithOptions(client, APIStateClientOptions{CompatibilityMode: compatMode})
			actual, err := stateClient.ReadState(cmd.Context())
			if err != nil {
				return fmt.Errorf("read server state: %w", err)
			}
			if !isJSON {
				for _, warning := range stateClient.OptionalReadWarnings() {
					_, _ = fmt.Fprintf(os.Stderr, "warning: %s\n", warning)
				}
			}

			// 4. Diff desired vs actual.
			plan := declarative.Diff(desired, actual)

			if !plan.HasChanges() {
				if isJSON {
					return gen.PrintJSON(os.Stdout, map[string]interface{}{
						"status":    "ok",
						"changes":   false,
						"succeeded": 0,
						"failed":    0,
					})
				}
				declarative.FormatText(os.Stdout, plan, noColor)
				return nil
			}

			// 5. Show the plan (text mode only).
			if !isJSON {
				declarative.FormatText(os.Stdout, plan, noColor)
			}

			// 6. Confirm unless auto-approved.
			if !autoApprove {
				if !gen.IsStdinTTY() {
					return fmt.Errorf("confirmation required but stdin is not a terminal; use --auto-approve")
				}
				_, _ = fmt.Fprint(os.Stdout, "\nApply these changes? [y/N] ")
				reader := bufio.NewReader(os.Stdin)
				answer, err := reader.ReadString('\n')
				if err != nil {
					return fmt.Errorf("read confirmation: %w", err)
				}
				answer = strings.TrimSpace(strings.ToLower(answer))
				if answer != "y" && answer != "yes" {
					_, _ = fmt.Fprintln(os.Stdout, "Apply cancelled.")
					return nil
				}
			}

			// 6.5. Fail fast if this apply would revoke the API key currently
			// used by the CLI itself.
			if err := stateClient.ValidateNoSelfAPIKeyDeletion(cmd.Context(), plan.Actions); err != nil {
				return fmt.Errorf("preflight auth validation: %w", err)
			}
			if err := stateClient.ValidateApplyCapabilities(cmd.Context(), plan.Actions); err != nil {
				return fmt.Errorf("preflight capability validation: %w", err)
			}

			// 7. Execute each action.
			type actionResult struct {
				Operation    string `json:"operation"`
				ResourceKind string `json:"resource_kind"`
				ResourceName string `json:"resource_name"`
				Status       string `json:"status"`
				Error        string `json:"error,omitempty"`
			}
			results := make([]actionResult, 0, len(plan.Actions))
			var succeeded, failed int
			failedAt := -1
			for i, action := range plan.Actions {
				if !isJSON {
					_, _ = fmt.Fprintf(os.Stdout, "  %s %s %q ... ",
						action.Operation, action.ResourceKind, action.ResourceName)
				}

				err := stateClient.Execute(cmd.Context(), action)
				if err != nil {
					if !isJSON {
						_, _ = fmt.Fprintf(os.Stdout, "failed: %v\n", err)
					}
					results = append(results, actionResult{
						Operation:    action.Operation.String(),
						ResourceKind: action.ResourceKind.String(),
						ResourceName: action.ResourceName,
						Status:       "failed",
						Error:        err.Error(),
					})
					failed++
					failedAt = i
					break
				}
				if !isJSON {
					_, _ = fmt.Fprintln(os.Stdout, "succeeded")
				}
				results = append(results, actionResult{
					Operation:    action.Operation.String(),
					ResourceKind: action.ResourceKind.String(),
					ResourceName: action.ResourceName,
					Status:       "succeeded",
				})
				succeeded++
			}

			if failedAt >= 0 {
				for _, action := range plan.Actions[failedAt+1:] {
					results = append(results, actionResult{
						Operation:    action.Operation.String(),
						ResourceKind: action.ResourceKind.String(),
						ResourceName: action.ResourceName,
						Status:       "skipped",
						Error:        "not executed due to earlier failure",
					})
				}
			}

			// 8. Print summary.
			if isJSON {
				status := "ok"
				if failed > 0 {
					status = "partial"
				}
				_ = gen.PrintJSON(os.Stdout, map[string]interface{}{
					"status":    status,
					"changes":   true,
					"succeeded": succeeded,
					"failed":    failed,
					"actions":   results,
				})
			} else {
				_, _ = fmt.Fprintf(os.Stdout, "\nApply complete: %d succeeded, %d failed.\n", succeeded, failed)
			}
			if failed > 0 {
				os.Exit(1)
			}

			return nil
		},
	}

	cmd.Flags().StringVar(&configDir, "config-dir", "./duck-config", "Path to configuration directory")
	cmd.Flags().BoolVar(&autoApprove, "auto-approve", false, "Skip interactive confirmation prompt")
	cmd.Flags().BoolVar(&noColor, "no-color", false, "Disable colored output")
	cmd.Flags().BoolVar(&allowUnknownFields, "allow-unknown-fields", false, "Allow unknown YAML fields in declarative config")
	cmd.Flags().BoolVar(&legacyOptionalReadErrors, "legacy-optional-read-errors", false, "Treat transport errors as optional for model/macro capability checks")

	return cmd
}
