package cli

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"duck-demo/pkg/cli/gen"
)

func init() {
	gen.RegisterRunOverride("executeQuery", func(client *gen.Client) func(*cobra.Command, []string) error {
		return func(cmd *cobra.Command, _ []string) error {
			sql, err := readSQLInput(cmd)
			if err != nil {
				return err
			}

			body := map[string]interface{}{"sql": sql}
			resp, err := client.Do("POST", "/query", nil, body)
			if err != nil {
				return err
			}
			if err := gen.CheckError(resp); err != nil {
				return err
			}

			return printQueryResult(cmd, resp)
		}
	})

	gen.RegisterRunOverride("submitQuery", func(client *gen.Client) func(*cobra.Command, []string) error {
		return func(cmd *cobra.Command, _ []string) error {
			sql, err := readSQLInput(cmd)
			if err != nil {
				return err
			}

			requestID, _ := cmd.Flags().GetString("request-id")
			body := map[string]interface{}{"sql": sql}
			if requestID != "" {
				body["request_id"] = requestID
			}

			resp, err := client.Do("POST", "/queries", nil, body)
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

			var submit struct {
				QueryID string `json:"query_id"`
			}
			if err := json.Unmarshal(respBody, &submit); err != nil {
				return fmt.Errorf("parse response: %w", err)
			}

			wait, _ := cmd.Flags().GetBool("wait")
			if !wait {
				return printAnyResponse(cmd, respBody)
			}

			pollInterval, _ := cmd.Flags().GetDuration("poll-interval")
			if pollInterval <= 0 {
				pollInterval = 1 * time.Second
			}
			timeout, _ := cmd.Flags().GetDuration("wait-timeout")

			statusResp, err := waitForQuery(client, submit.QueryID, pollInterval, timeout)
			if err != nil {
				return err
			}

			if statusResp.Status != "SUCCEEDED" {
				return printAnyResponse(cmd, statusResp.Raw)
			}

			showResults, _ := cmd.Flags().GetBool("results")
			if !showResults {
				return printAnyResponse(cmd, statusResp.Raw)
			}

			maxResults, _ := cmd.Flags().GetInt64("max-results")
			query := url.Values{}
			if maxResults > 0 {
				query.Set("max_results", fmt.Sprintf("%d", maxResults))
			}
			resultsResp, err := client.Do("GET", "/queries/"+submit.QueryID+"/results", query, nil)
			if err != nil {
				return err
			}
			if err := gen.CheckError(resultsResp); err != nil {
				return err
			}
			return printQueryResult(cmd, resultsResp)
		}
	})

	gen.RegisterRunOverride("getQuery", func(client *gen.Client) func(*cobra.Command, []string) error {
		return func(cmd *cobra.Command, args []string) error {
			if len(args) != 1 {
				return fmt.Errorf("requires query id argument")
			}
			resp, err := client.Do("GET", "/queries/"+args[0], nil, nil)
			if err != nil {
				return err
			}
			if err := gen.CheckError(resp); err != nil {
				return err
			}
			body, err := gen.ReadBody(resp)
			if err != nil {
				return fmt.Errorf("read response: %w", err)
			}
			return printAnyResponse(cmd, body)
		}
	})

	gen.RegisterRunOverride("getQueryResults", func(client *gen.Client) func(*cobra.Command, []string) error {
		return func(cmd *cobra.Command, args []string) error {
			if len(args) != 1 {
				return fmt.Errorf("requires query id argument")
			}
			query := url.Values{}
			maxResults, _ := cmd.Flags().GetInt64("max-results")
			if maxResults > 0 {
				query.Set("max_results", fmt.Sprintf("%d", maxResults))
			}
			pageToken, _ := cmd.Flags().GetString("page-token")
			if pageToken != "" {
				query.Set("page_token", pageToken)
			}
			resp, err := client.Do("GET", "/queries/"+args[0]+"/results", query, nil)
			if err != nil {
				return err
			}
			if err := gen.CheckError(resp); err != nil {
				return err
			}
			return printQueryResult(cmd, resp)
		}
	})

	gen.RegisterRunOverride("cancelQuery", func(client *gen.Client) func(*cobra.Command, []string) error {
		return func(cmd *cobra.Command, args []string) error {
			if len(args) != 1 {
				return fmt.Errorf("requires query id argument")
			}
			resp, err := client.Do("POST", "/queries/"+args[0]+"/cancel", nil, nil)
			if err != nil {
				return err
			}
			if err := gen.CheckError(resp); err != nil {
				return err
			}
			body, err := gen.ReadBody(resp)
			if err != nil {
				return fmt.Errorf("read response: %w", err)
			}
			return printAnyResponse(cmd, body)
		}
	})

	gen.RegisterRunOverride("deleteQuery", func(client *gen.Client) func(*cobra.Command, []string) error {
		return func(cmd *cobra.Command, args []string) error {
			if len(args) != 1 {
				return fmt.Errorf("requires query id argument")
			}
			if !cmd.Flags().Changed("yes") {
				if !gen.ConfirmPrompt("Delete query job?") {
					return nil
				}
			}
			resp, err := client.Do("DELETE", "/queries/"+args[0], nil, nil)
			if err != nil {
				return err
			}
			if err := gen.CheckError(resp); err != nil {
				return err
			}
			outputFlag, _ := cmd.Root().PersistentFlags().GetString("output")
			if gen.OutputFormat(outputFlag) == gen.OutputJSON {
				return gen.PrintJSON(os.Stdout, map[string]string{"status": "ok"})
			}
			_, _ = fmt.Fprintln(os.Stdout, "Deleted.")
			return nil
		}
	})

	gen.RegisterOverride("submitQuery", func(c *cobra.Command) {
		c.Flags().Bool("wait", false, "Wait for query completion")
		c.Flags().Duration("poll-interval", time.Second, "Status polling interval when --wait is enabled")
		c.Flags().Duration("wait-timeout", 0, "Max wait duration (0 means wait indefinitely)")
		c.Flags().Bool("results", false, "Fetch first page of results when query succeeds and --wait is enabled")
		c.Flags().Int64("max-results", 100, "Maximum rows to fetch when --results is enabled")
	})
}

func readSQLInput(cmd *cobra.Command) (string, error) {
	sql, _ := cmd.Flags().GetString("sql")

	if sql == "" {
		stat, _ := os.Stdin.Stat()
		if (stat.Mode() & os.ModeCharDevice) == 0 {
			data, err := io.ReadAll(os.Stdin)
			if err != nil {
				return "", fmt.Errorf("read stdin: %w", err)
			}
			sql = strings.TrimSpace(string(data))
		}
	}

	if sql == "" {
		return "", fmt.Errorf("provide SQL via --sql flag or stdin pipe")
	}

	return sql, nil
}

func printAnyResponse(cmd *cobra.Command, body []byte) error {
	quiet, _ := cmd.Root().PersistentFlags().GetBool("quiet")
	if quiet {
		_, _ = fmt.Fprintln(os.Stdout, string(body))
		return nil
	}

	outputFlag, _ := cmd.Flags().GetString("output")
	if outputFlag == "" {
		outputFlag, _ = cmd.Root().PersistentFlags().GetString("output")
	}

	switch gen.OutputFormat(outputFlag) {
	case gen.OutputJSON:
		var pretty interface{}
		if err := json.Unmarshal(body, &pretty); err != nil {
			return fmt.Errorf("parse JSON: %w", err)
		}
		return gen.PrintJSON(os.Stdout, pretty)
	default:
		var data map[string]interface{}
		if err := json.Unmarshal(body, &data); err != nil {
			return fmt.Errorf("parse response: %w", err)
		}
		gen.PrintDetail(os.Stdout, data)
		return nil
	}
}

func printQueryResult(cmd *cobra.Command, resp *http.Response) error {
	respBody, err := gen.ReadBody(resp)
	if err != nil {
		return fmt.Errorf("read response: %w", err)
	}

	var result struct {
		Columns       []string        `json:"columns"`
		Rows          [][]interface{} `json:"rows"`
		RowCount      int             `json:"row_count"`
		NextPageToken string          `json:"next_page_token"`
	}
	if err := json.Unmarshal(respBody, &result); err != nil {
		return fmt.Errorf("parse response: %w", err)
	}

	quiet, _ := cmd.Root().PersistentFlags().GetBool("quiet")
	if quiet {
		_, _ = fmt.Fprintln(os.Stdout, result.RowCount)
		return nil
	}

	outputFlag, _ := cmd.Flags().GetString("output")
	if outputFlag == "" {
		outputFlag, _ = cmd.Root().PersistentFlags().GetString("output")
	}

	switch gen.OutputFormat(outputFlag) {
	case gen.OutputJSON:
		var pretty interface{}
		if err := json.Unmarshal(respBody, &pretty); err != nil {
			return fmt.Errorf("parse JSON: %w", err)
		}
		return gen.PrintJSON(os.Stdout, pretty)
	case gen.OutputCSV:
		w := csv.NewWriter(os.Stdout)
		_ = w.Write(result.Columns)
		for _, row := range result.Rows {
			record := make([]string, len(row))
			for i, v := range row {
				record[i] = gen.FormatValue(v)
			}
			_ = w.Write(record)
		}
		w.Flush()
		return w.Error()
	default:
		rows := make([][]string, len(result.Rows))
		for i, row := range result.Rows {
			rows[i] = make([]string, len(row))
			for j, v := range row {
				rows[i][j] = gen.FormatValue(v)
			}
		}
		gen.PrintTable(os.Stdout, result.Columns, rows)
		if result.NextPageToken != "" {
			fmt.Fprintf(os.Stderr, "\n(%d rows, more pages available: --page-token %s)\n", result.RowCount, result.NextPageToken)
		} else {
			fmt.Fprintf(os.Stderr, "\n(%d rows)\n", result.RowCount)
		}
		return nil
	}
}

type waitedStatus struct {
	Status string
	Raw    []byte
}

func waitForQuery(client *gen.Client, queryID string, pollInterval, timeout time.Duration) (*waitedStatus, error) {
	start := time.Now()
	for {
		resp, err := client.Do("GET", "/queries/"+queryID, nil, nil)
		if err != nil {
			return nil, err
		}
		if err := gen.CheckError(resp); err != nil {
			return nil, err
		}
		body, err := gen.ReadBody(resp)
		if err != nil {
			return nil, fmt.Errorf("read status response: %w", err)
		}

		var status struct {
			Status string `json:"status"`
		}
		if err := json.Unmarshal(body, &status); err != nil {
			return nil, fmt.Errorf("parse status response: %w", err)
		}

		switch status.Status {
		case "SUCCEEDED", "FAILED", "CANCELED":
			return &waitedStatus{Status: status.Status, Raw: body}, nil
		}

		if timeout > 0 && time.Since(start) > timeout {
			return nil, fmt.Errorf("timed out waiting for query %s after %s", queryID, timeout)
		}
		time.Sleep(pollInterval)
	}
}
