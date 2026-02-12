package cli

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/spf13/cobra"

	"duck-demo/pkg/cli/gen"
)

func init() {
	gen.RegisterRunOverride("executeQuery", func(client *gen.Client) func(*cobra.Command, []string) error {
		return func(cmd *cobra.Command, args []string) error {
			sql, _ := cmd.Flags().GetString("sql")

			// Read from stdin if no --sql flag
			if sql == "" {
				stat, _ := os.Stdin.Stat()
				if (stat.Mode() & os.ModeCharDevice) == 0 {
					data, err := io.ReadAll(os.Stdin)
					if err != nil {
						return fmt.Errorf("read stdin: %w", err)
					}
					sql = strings.TrimSpace(string(data))
				}
			}

			if sql == "" {
				return fmt.Errorf("provide SQL via --sql flag or stdin pipe")
			}

			body := map[string]interface{}{"sql": sql}
			resp, err := client.Do("POST", "/query", nil, body)
			if err != nil {
				return err
			}
			if err := gen.CheckError(resp); err != nil {
				return err
			}

			respBody, err := gen.ReadBody(resp)
			if err != nil {
				return fmt.Errorf("read response: %w", err)
			}

			var result struct {
				Columns  []string        `json:"columns"`
				Rows     [][]interface{} `json:"rows"`
				RowCount int             `json:"row_count"`
			}
			if err := json.Unmarshal(respBody, &result); err != nil {
				return fmt.Errorf("parse response: %w", err)
			}

			quiet, _ := cmd.Root().PersistentFlags().GetBool("quiet")
			if quiet {
				fmt.Fprintln(os.Stdout, result.RowCount)
				return nil
			}

			outputFlag, _ := cmd.Flags().GetString("output")
			switch gen.OutputFormat(outputFlag) {
			case gen.OutputJSON:
				var pretty interface{}
				json.Unmarshal(respBody, &pretty)
				return gen.PrintJSON(os.Stdout, pretty)
			case gen.OutputCSV:
				w := csv.NewWriter(os.Stdout)
				w.Write(result.Columns)
				for _, row := range result.Rows {
					record := make([]string, len(row))
					for i, v := range row {
						record[i] = fmt.Sprintf("%v", v)
					}
					w.Write(record)
				}
				w.Flush()
				return w.Error()
			default:
				// Table output with dynamic columns from query result
				rows := make([][]string, len(result.Rows))
				for i, row := range result.Rows {
					rows[i] = make([]string, len(row))
					for j, v := range row {
						rows[i][j] = fmt.Sprintf("%v", v)
					}
				}
				gen.PrintTable(os.Stdout, result.Columns, rows)
				fmt.Fprintf(os.Stderr, "\n(%d rows)\n", result.RowCount)
			}
			return nil
		}
	})
}
