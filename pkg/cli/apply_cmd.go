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
		configDir   string
		autoApprove bool
		noColor     bool
	)

	cmd := &cobra.Command{
		Use:   "apply",
		Short: "Apply declarative configuration changes to the server",
		Long:  "Reads YAML configuration files, compares with the current server state, and applies the changes.",
		RunE: func(cmd *cobra.Command, _ []string) error {
			isJSON := getOutputFormat(cmd) == "json"

			// 1. Load desired state from YAML files.
			desired, err := declarative.LoadDirectory(configDir)
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
			stateClient := NewAPIStateClient(client)
			actual, err := stateClient.ReadState(cmd.Context())
			if err != nil {
				return fmt.Errorf("read server state: %w", err)
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

			// 7. Execute each action.
			type actionResult struct {
				Operation    string `json:"operation"`
				ResourceKind string `json:"resource_kind"`
				ResourceName string `json:"resource_name"`
				Status       string `json:"status"`
				Error        string `json:"error,omitempty"`
			}
			var results []actionResult
			var succeeded, failed int
			for _, action := range plan.Actions {
				if !isJSON {
					_, _ = fmt.Fprintf(os.Stdout, "  %s %s %q ... ",
						action.Operation, action.ResourceKind, action.ResourceName)
				}

				if err := stateClient.Execute(cmd.Context(), action); err != nil {
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
				} else {
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

	return cmd
}
