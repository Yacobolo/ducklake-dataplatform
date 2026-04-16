package cli

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"github.com/Yacobolo/quackstack/internal/declarative"
	"github.com/Yacobolo/quackstack/pkg/cli/apiruntime"
)

func newSchemaCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "schema",
		Short: "Show the built-in declarative schema",
		Long:  "Outputs the declarative CUE schema used by project configuration.",
		RunE: func(cmd *cobra.Command, _ []string) error {
			payload := map[string]any{
				"format":  "cue",
				"package": "duckconfig",
				"schema":  declarative.CUESchemaSource(),
			}

			if getOutputFormat(cmd) == "json" {
				return apiruntime.PrintJSON(os.Stdout, payload)
			}
			_, _ = fmt.Fprint(os.Stdout, declarative.CUESchemaSource())
			if source := declarative.CUESchemaSource(); source == "" || source[len(source)-1] != '\n' {
				_, _ = fmt.Fprintln(os.Stdout)
			}
			return nil
		},
	}
}
