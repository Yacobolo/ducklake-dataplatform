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

	"duck-demo/pkg/cli/apiruntime"
)

func init() {
	apiruntime.RegisterRunOverride("executeQuery", func(client *apiruntime.Client) func(*cobra.Command, []string) error {
		return func(cmd *cobra.Command, args []string) error {
			sql, err := readSQLInput(cmd, args)
			if err != nil {
				return err
			}

			body := map[string]interface{}{"sql": sql}
			addComputeSelectionToBody(cmd, body)
			resp, err := client.Do("POST", "/query", nil, body)
			if err != nil {
				return err
			}
			if err := apiruntime.CheckError(resp); err != nil {
				return err
			}

			return printQueryResult(cmd, resp)
		}
	})
	apiruntime.RegisterOverride("executeQuery", func(c *cobra.Command) {
		c.Args = cobra.MaximumNArgs(1)
		addComputeSelectionFlags(c)
	})

	apiruntime.RegisterRunOverride("submitQuery", func(client *apiruntime.Client) func(*cobra.Command, []string) error {
		return func(cmd *cobra.Command, args []string) error {
			sql, err := readSQLInput(cmd, args)
			if err != nil {
				return err
			}

			requestID, _ := cmd.Flags().GetString("request-id")
			body := map[string]interface{}{"sql": sql}
			if requestID != "" {
				body["request_id"] = requestID
			}
			addComputeSelectionToBody(cmd, body)

			resp, err := client.Do("POST", "/queries", nil, body)
			if err != nil {
				return err
			}
			if err := apiruntime.CheckError(resp); err != nil {
				return err
			}

			respBody, err := apiruntime.ReadBody(resp)
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
			if err := apiruntime.CheckError(resultsResp); err != nil {
				return err
			}
			return printQueryResult(cmd, resultsResp)
		}
	})

	apiruntime.RegisterRunOverride("getQuery", func(client *apiruntime.Client) func(*cobra.Command, []string) error {
		return func(cmd *cobra.Command, args []string) error {
			if len(args) != 1 {
				return fmt.Errorf("requires query id argument")
			}
			resp, err := client.Do("GET", "/queries/"+args[0], nil, nil)
			if err != nil {
				return err
			}
			if err := apiruntime.CheckError(resp); err != nil {
				return err
			}
			body, err := apiruntime.ReadBody(resp)
			if err != nil {
				return fmt.Errorf("read response: %w", err)
			}
			return printAnyResponse(cmd, body)
		}
	})

	apiruntime.RegisterRunOverride("getQueryResults", func(client *apiruntime.Client) func(*cobra.Command, []string) error {
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
			if err := apiruntime.CheckError(resp); err != nil {
				return err
			}
			return printQueryResult(cmd, resp)
		}
	})

	apiruntime.RegisterRunOverride("cancelQuery", func(client *apiruntime.Client) func(*cobra.Command, []string) error {
		return func(cmd *cobra.Command, args []string) error {
			if len(args) != 1 {
				return fmt.Errorf("requires query id argument")
			}
			resp, err := client.Do("POST", "/queries/"+args[0]+"/cancel", nil, nil)
			if err != nil {
				return err
			}
			if err := apiruntime.CheckError(resp); err != nil {
				return err
			}
			body, err := apiruntime.ReadBody(resp)
			if err != nil {
				return fmt.Errorf("read response: %w", err)
			}
			return printAnyResponse(cmd, body)
		}
	})

	apiruntime.RegisterRunOverride("deleteQuery", func(client *apiruntime.Client) func(*cobra.Command, []string) error {
		return func(cmd *cobra.Command, args []string) error {
			if len(args) != 1 {
				return fmt.Errorf("requires query id argument")
			}
			if !cmd.Flags().Changed("yes") {
				if !apiruntime.ConfirmPrompt("Delete query job?") {
					return nil
				}
			}
			resp, err := client.Do("DELETE", "/queries/"+args[0], nil, nil)
			if err != nil {
				return err
			}
			if err := apiruntime.CheckError(resp); err != nil {
				return err
			}
			outputFlag, _ := cmd.Root().PersistentFlags().GetString("output")
			if apiruntime.OutputFormat(outputFlag) == apiruntime.OutputJSON {
				return apiruntime.PrintJSON(os.Stdout, map[string]string{"status": "ok"})
			}
			_, _ = fmt.Fprintln(os.Stdout, "Deleted.")
			return nil
		}
	})

	apiruntime.RegisterOverride("submitQuery", func(c *cobra.Command) {
		addComputeSelectionFlags(c)
		c.Flags().Bool("wait", false, "Wait for query completion")
		c.Flags().Duration("poll-interval", time.Second, "Status polling interval when --wait is enabled")
		c.Flags().Duration("wait-timeout", 0, "Max wait duration (0 means wait indefinitely)")
		c.Flags().Bool("results", false, "Fetch first page of results when query succeeds and --wait is enabled")
		c.Flags().Int64("max-results", 100, "Maximum rows to fetch when --results is enabled")
	})
}

func addComputeSelectionFlags(cmd *cobra.Command) {
	if cmd.Flags().Lookup("compute-mode") == nil {
		cmd.Flags().String("compute-mode", "", "Compute routing mode: AUTO, BYOC_LOCAL, or SHARED_ENDPOINT")
	}
	if cmd.Flags().Lookup("compute-endpoint") == nil {
		cmd.Flags().String("compute-endpoint", "", "Explicit compute endpoint name when using SHARED_ENDPOINT")
	}
	if cmd.Flags().Lookup("workload-type") == nil {
		cmd.Flags().String("workload-type", "", "Workload type: INTERACTIVE, SCHEDULED, NOTEBOOK, or HEAVY")
	}
}

func addComputeSelectionToBody(cmd *cobra.Command, body map[string]interface{}) {
	if mode, _ := cmd.Flags().GetString("compute-mode"); strings.TrimSpace(mode) != "" {
		body["compute_mode"] = strings.TrimSpace(mode)
	}
	if endpoint, _ := cmd.Flags().GetString("compute-endpoint"); strings.TrimSpace(endpoint) != "" {
		body["endpoint_name"] = strings.TrimSpace(endpoint)
	}
	if workload, _ := cmd.Flags().GetString("workload-type"); strings.TrimSpace(workload) != "" {
		body["workload_type"] = strings.TrimSpace(workload)
	}
}

func readSQLInput(cmd *cobra.Command, args []string) (string, error) {
	sql, _ := cmd.Flags().GetString("sql")

	if sql == "" && len(args) > 0 {
		sql = args[0]
	}

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

	switch apiruntime.OutputFormat(outputFlag) {
	case apiruntime.OutputJSON:
		var pretty interface{}
		if err := json.Unmarshal(body, &pretty); err != nil {
			return fmt.Errorf("parse JSON: %w", err)
		}
		return apiruntime.PrintJSON(os.Stdout, pretty)
	default:
		var data map[string]interface{}
		if err := json.Unmarshal(body, &data); err != nil {
			return fmt.Errorf("parse response: %w", err)
		}
		apiruntime.PrintDetail(os.Stdout, data)
		return nil
	}
}

func printQueryResult(cmd *cobra.Command, resp *http.Response) error {
	respBody, err := apiruntime.ReadBody(resp)
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

	switch apiruntime.OutputFormat(outputFlag) {
	case apiruntime.OutputJSON:
		var pretty interface{}
		if err := json.Unmarshal(respBody, &pretty); err != nil {
			return fmt.Errorf("parse JSON: %w", err)
		}
		return apiruntime.PrintJSON(os.Stdout, pretty)
	case apiruntime.OutputCSV:
		w := csv.NewWriter(os.Stdout)
		_ = w.Write(result.Columns)
		for _, row := range result.Rows {
			record := make([]string, len(row))
			for i, v := range row {
				record[i] = apiruntime.FormatValue(v)
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
				rows[i][j] = apiruntime.FormatValue(v)
			}
		}
		apiruntime.PrintTable(os.Stdout, result.Columns, rows)
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

func waitForQuery(client *apiruntime.Client, queryID string, pollInterval, timeout time.Duration) (*waitedStatus, error) {
	start := time.Now()
	for {
		resp, err := client.Do("GET", "/queries/"+queryID, nil, nil)
		if err != nil {
			return nil, err
		}
		if err := apiruntime.CheckError(resp); err != nil {
			return nil, err
		}
		body, err := apiruntime.ReadBody(resp)
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
