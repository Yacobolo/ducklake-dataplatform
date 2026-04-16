package cli

import (
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/spf13/cobra"

	"github.com/Yacobolo/quackstack/pkg/cli/apiruntime"
)

func newFindCmd(client *apiruntime.Client) *cobra.Command {
	var (
		objectType string
		catalog    string
		maxResults int64
	)

	cmd := &cobra.Command{
		Use:   "find <query>",
		Short: "Search the data catalog for schemas, tables, and columns",
		Long: `Search across all catalog objects (schemas, tables, views, columns) by name, comment, tag, or property.
This is designed as the agent's "grep" for the data catalog.`,
		Example: `  # Search for anything matching "revenue"
  quack find "revenue"

  # Search only tables
  quack find "orders" --type table

  # Search columns across all tables
  quack find "customer_id" --type column

  # Scoped search with JSON output for agent consumption
  quack find "user" --catalog main --output json`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runFind(cmd, client, args[0], objectType, catalog, maxResults)
		},
	}

	cmd.Flags().StringVarP(&objectType, "type", "t", "", "Filter by object type: schema, table, view, column")
	cmd.PersistentFlags().StringVar(&catalog, "catalog", "", "Scope search to a specific catalog")
	cmd.PersistentFlags().Int64Var(&maxResults, "max-results", 100, "Maximum number of results")

	// Add convenience subcommands
	cmd.AddCommand(newFindTablesCmd(client, &catalog, &maxResults))
	cmd.AddCommand(newFindColumnsCmd(client, &catalog, &maxResults))

	return cmd
}

func newFindTablesCmd(client *apiruntime.Client, catalog *string, maxResults *int64) *cobra.Command {
	return &cobra.Command{
		Use:   "tables <pattern>",
		Short: "Search for tables by name pattern",
		Long:  "Search for tables matching a name pattern. Supports * as wildcard for client-side filtering.",
		Example: `  quack find tables "order*"
  quack find tables "user" --catalog main --output json`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runFind(cmd, client, args[0], "table", *catalog, *maxResults)
		},
	}
}

func newFindColumnsCmd(client *apiruntime.Client, catalog *string, maxResults *int64) *cobra.Command {
	return &cobra.Command{
		Use:   "columns <pattern>",
		Short: "Search for columns by name pattern across all tables",
		Long:  "Search for columns matching a name pattern. Supports * as wildcard for client-side filtering.",
		Example: `  quack find columns "id"
  quack find columns "email*" --catalog main --output json`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runFind(cmd, client, args[0], "column", *catalog, *maxResults)
		},
	}
}

func runFind(cmd *cobra.Command, client *apiruntime.Client, query, objectType, catalog string, maxResults int64) error {
	results, nextPageToken, err := collectFindResults(client, query, objectType, catalog, maxResults)
	if err != nil {
		return err
	}

	if strings.Contains(query, "*") {
		pattern := strings.ToLower(query)
		filtered := results[:0]
		for _, item := range results {
			matched, _ := filepath.Match(pattern, strings.ToLower(item.Name))
			if matched {
				filtered = append(filtered, item)
			}
		}
		results = filtered
	}

	if maxResults > 0 && int64(len(results)) > maxResults {
		results = results[:maxResults]
	}

	if getOutputFormat(cmd) == "json" {
		return apiruntime.PrintJSON(os.Stdout, map[string]any{
			"data":            results,
			"next_page_token": nextPageToken,
		})
	}

	columns := []string{"type", "name", "schema", "match"}
	rows := make([][]string, 0, len(results))
	for _, item := range results {
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
	apiruntime.PrintTable(os.Stdout, columns, rows)
	return nil
}

type findResult struct {
	Type       string  `json:"type"`
	Name       string  `json:"name"`
	SchemaName *string `json:"schema_name,omitempty"`
	TableName  *string `json:"table_name,omitempty"`
	Comment    *string `json:"comment,omitempty"`
	MatchField string  `json:"match_field"`
}

func collectFindResults(client *apiruntime.Client, query, objectType, catalog string, maxResults int64) ([]findResult, string, error) {
	// Strip glob wildcards for the API query — we filter client-side
	apiQuery := strings.ReplaceAll(query, "*", "")
	if apiQuery == "" {
		apiQuery = query
	}

	results := make([]findResult, 0)
	nextPageToken := ""

	if objectType == "" || objectType == "schema" || objectType == "table" || objectType == "column" {
		q := url.Values{}
		q.Set("query", apiQuery)
		if objectType != "" {
			q.Set("type", objectType)
		}
		if catalog != "" {
			q.Set("catalog", catalog)
		}
		q.Set("max_results", fmt.Sprintf("%d", maxResults))

		resp, err := client.Do("GET", "/catalogs/search", q, nil)
		if err != nil {
			return nil, "", err
		}
		if err := apiruntime.CheckError(resp); err != nil {
			return nil, "", err
		}

		respBody, err := apiruntime.ReadBody(resp)
		if err != nil {
			return nil, "", fmt.Errorf("read response: %w", err)
		}

		var data struct {
			Data          []findResult `json:"data"`
			NextPageToken string       `json:"next_page_token"`
		}
		if err := json.Unmarshal(respBody, &data); err != nil {
			return nil, "", fmt.Errorf("parse response: %w", err)
		}
		results = append(results, data.Data...)
		nextPageToken = data.NextPageToken
	}

	if objectType == "view" || (objectType == "" && strings.TrimSpace(catalog) != "") {
		viewResults, err := findViews(client, query, catalog, maxResults)
		if err != nil {
			return nil, "", err
		}
		results = append(results, viewResults...)
	}

	sort.SliceStable(results, func(i, j int) bool {
		if results[i].Type != results[j].Type {
			return results[i].Type < results[j].Type
		}
		leftSchema := ""
		if results[i].SchemaName != nil {
			leftSchema = *results[i].SchemaName
		}
		rightSchema := ""
		if results[j].SchemaName != nil {
			rightSchema = *results[j].SchemaName
		}
		if leftSchema != rightSchema {
			return leftSchema < rightSchema
		}
		return results[i].Name < results[j].Name
	})

	return results, nextPageToken, nil
}

func findViews(client *apiruntime.Client, query, catalog string, maxResults int64) ([]findResult, error) {
	catalogNames, err := findCatalogNames(client, catalog)
	if err != nil {
		return nil, err
	}

	needle := strings.ToLower(strings.TrimSpace(strings.ReplaceAll(query, "*", "")))
	results := make([]findResult, 0)
	for _, catalogName := range catalogNames {
		schemaNames, err := listSchemaNames(client, catalogName)
		if err != nil {
			return nil, err
		}
		for _, schemaName := range schemaNames {
			views, err := listViews(client, catalogName, schemaName)
			if err != nil {
				return nil, err
			}
			for _, view := range views {
				matchField, ok := matchesViewQuery(view, needle)
				if !ok {
					continue
				}
				schemaNameCopy := schemaName
				results = append(results, findResult{
					Type:       "view",
					Name:       view.Name,
					SchemaName: &schemaNameCopy,
					Comment:    view.Comment,
					MatchField: matchField,
				})
				if maxResults > 0 && int64(len(results)) >= maxResults {
					return results, nil
				}
			}
		}
	}
	return results, nil
}

func findCatalogNames(client *apiruntime.Client, catalog string) ([]string, error) {
	if strings.TrimSpace(catalog) != "" {
		return []string{catalog}, nil
	}

	resp, err := client.Do("GET", "/catalogs", nil, nil)
	if err != nil {
		return nil, err
	}
	if err := apiruntime.CheckError(resp); err != nil {
		return nil, err
	}
	respBody, err := apiruntime.ReadBody(resp)
	if err != nil {
		return nil, fmt.Errorf("read catalogs response: %w", err)
	}

	var payload struct {
		Catalogs []struct {
			Name string `json:"name"`
		} `json:"catalogs"`
		Data []struct {
			Name string `json:"name"`
		} `json:"data"`
	}
	if err := json.Unmarshal(respBody, &payload); err != nil {
		return nil, fmt.Errorf("parse catalogs response: %w", err)
	}

	names := make([]string, 0, len(payload.Catalogs)+len(payload.Data))
	for _, item := range payload.Catalogs {
		if item.Name != "" {
			names = append(names, item.Name)
		}
	}
	for _, item := range payload.Data {
		if item.Name != "" {
			names = append(names, item.Name)
		}
	}
	return names, nil
}

func listSchemaNames(client *apiruntime.Client, catalogName string) ([]string, error) {
	resp, err := client.Do("GET", "/catalogs/"+url.PathEscape(catalogName)+"/schemas", nil, nil)
	if err != nil {
		return nil, err
	}
	if err := apiruntime.CheckError(resp); err != nil {
		return nil, err
	}
	respBody, err := apiruntime.ReadBody(resp)
	if err != nil {
		return nil, fmt.Errorf("read schemas response: %w", err)
	}

	var payload struct {
		Data []struct {
			Name string `json:"name"`
		} `json:"data"`
	}
	if err := json.Unmarshal(respBody, &payload); err != nil {
		return nil, fmt.Errorf("parse schemas response: %w", err)
	}

	names := make([]string, 0, len(payload.Data))
	for _, item := range payload.Data {
		if item.Name != "" {
			names = append(names, item.Name)
		}
	}
	return names, nil
}

type viewSearchItem struct {
	Name    string  `json:"name"`
	Comment *string `json:"comment"`
}

func listViews(client *apiruntime.Client, catalogName, schemaName string) ([]viewSearchItem, error) {
	path := "/catalogs/" + url.PathEscape(catalogName) + "/schemas/" + url.PathEscape(schemaName) + "/views"
	resp, err := client.Do("GET", path, nil, nil)
	if err != nil {
		return nil, err
	}
	if err := apiruntime.CheckError(resp); err != nil {
		return nil, err
	}
	respBody, err := apiruntime.ReadBody(resp)
	if err != nil {
		return nil, fmt.Errorf("read views response: %w", err)
	}

	var payload struct {
		Data []viewSearchItem `json:"data"`
	}
	if err := json.Unmarshal(respBody, &payload); err != nil {
		return nil, fmt.Errorf("parse views response: %w", err)
	}
	return payload.Data, nil
}

func matchesViewQuery(view viewSearchItem, query string) (string, bool) {
	if query == "" {
		return "name", true
	}
	if strings.Contains(strings.ToLower(view.Name), query) {
		return "name", true
	}
	if view.Comment != nil && strings.Contains(strings.ToLower(*view.Comment), query) {
		return "comment", true
	}
	return "", false
}
