package cli

import (
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/cobra"

	"duck-demo/pkg/cli/gen"
)

func newFindCmd(client *gen.Client) *cobra.Command {
	var (
		objectType string
		catalog    string
		maxResults int64
	)

	cmd := &cobra.Command{
		Use:   "find <query>",
		Short: "Search the data catalog for schemas, tables, and columns",
		Long: `Search across all catalog objects (schemas, tables, columns) by name, comment, tag, or property.
This is designed as the agent's "grep" for the data catalog.`,
		Example: `  # Search for anything matching "revenue"
  duck find "revenue"

  # Search only tables
  duck find "orders" --type table

  # Search columns across all tables
  duck find "customer_id" --type column

  # Scoped search with JSON output for agent consumption
  duck find "user" --catalog main --output json`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runFind(cmd, client, args[0], objectType, catalog, maxResults)
		},
	}

	cmd.Flags().StringVarP(&objectType, "type", "t", "", "Filter by object type: schema, table, column")
	cmd.Flags().StringVar(&catalog, "catalog", "", "Scope search to a specific catalog")
	cmd.Flags().Int64Var(&maxResults, "max-results", 100, "Maximum number of results")

	// Add convenience subcommands
	cmd.AddCommand(newFindTablesCmd(client, &catalog, &maxResults))
	cmd.AddCommand(newFindColumnsCmd(client, &catalog, &maxResults))

	return cmd
}

func newFindTablesCmd(client *gen.Client, catalog *string, maxResults *int64) *cobra.Command {
	return &cobra.Command{
		Use:   "tables <pattern>",
		Short: "Search for tables by name pattern",
		Long:  "Search for tables matching a name pattern. Supports * as wildcard for client-side filtering.",
		Example: `  duck find tables "order*"
  duck find tables "user" --catalog main --output json`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runFind(cmd, client, args[0], "table", *catalog, *maxResults)
		},
	}
}

func newFindColumnsCmd(client *gen.Client, catalog *string, maxResults *int64) *cobra.Command {
	return &cobra.Command{
		Use:   "columns <pattern>",
		Short: "Search for columns by name pattern across all tables",
		Long:  "Search for columns matching a name pattern. Supports * as wildcard for client-side filtering.",
		Example: `  duck find columns "id"
  duck find columns "email*" --catalog main --output json`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runFind(cmd, client, args[0], "column", *catalog, *maxResults)
		},
	}
}

func runFind(cmd *cobra.Command, client *gen.Client, query, objectType, catalog string, maxResults int64) error {
	// Strip glob wildcards for the API query — we filter client-side
	apiQuery := strings.ReplaceAll(query, "*", "")
	if apiQuery == "" {
		apiQuery = query
	}

	q := url.Values{}
	q.Set("query", apiQuery)
	if objectType != "" {
		q.Set("type", objectType)
	}
	if catalog != "" {
		q.Set("catalog", catalog)
	}
	q.Set("max_results", fmt.Sprintf("%d", maxResults))

	resp, err := client.Do("GET", "/search", q, nil)
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

	var data struct {
		Data []struct {
			Type       string  `json:"type"`
			Name       string  `json:"name"`
			SchemaName *string `json:"schema_name"`
			TableName  *string `json:"table_name"`
			Comment    *string `json:"comment"`
			MatchField string  `json:"match_field"`
		} `json:"data"`
		NextPageToken string `json:"next_page_token"`
	}
	if err := json.Unmarshal(respBody, &data); err != nil {
		return fmt.Errorf("parse response: %w", err)
	}

	// Client-side glob filtering if the query contains wildcards
	if strings.Contains(query, "*") {
		pattern := strings.ToLower(query)
		var filtered = data.Data[:0]
		for _, item := range data.Data {
			matched, _ := filepath.Match(pattern, strings.ToLower(item.Name))
			if matched {
				filtered = append(filtered, item)
			}
		}
		data.Data = filtered
	}

	// Output
	if getOutputFormat(cmd) == "json" {
		// Use filtered data.Data so glob filtering is reflected in JSON output
		return gen.PrintJSON(os.Stdout, data)
	}

	// Table output
	columns := []string{"type", "name", "schema", "match"}
	rows := make([][]string, 0, len(data.Data))
	for _, item := range data.Data {
		schema := ""
		if item.SchemaName != nil {
			schema = *item.SchemaName
		}
		displayName := item.Name
		if item.TableName != nil && item.Type == "column" {
			displayName = *item.TableName + "." + item.Name
		}
		rows = append(rows, []string{item.Type, displayName, schema, item.MatchField})
	}
	gen.PrintTable(os.Stdout, columns, rows)
	return nil
}
