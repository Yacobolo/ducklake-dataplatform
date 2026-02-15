package cli

import (
	"fmt"
	"net/url"
	"os"
	"strings"

	"github.com/spf13/cobra"

	"duck-demo/pkg/cli/gen"
)

func newAPICmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "api",
		Short: "Explore the platform API endpoints",
		Long: `Introspect the HTTP API surface. Discover endpoints, view parameters,
and generate curl commands. Designed as the agent's "ripgrep" for the API.`,
	}

	cmd.AddCommand(newAPIListCmd())
	cmd.AddCommand(newAPISearchCmd())
	cmd.AddCommand(newAPIDescribeCmd())
	cmd.AddCommand(newAPICurlCmd())

	return cmd
}

func newAPIListCmd() *cobra.Command {
	var tag string

	cmd := &cobra.Command{
		Use:   "list",
		Short: "List all API endpoints",
		Long:  "Lists all API endpoints with their HTTP method, path, and description.",
		Example: `  duck api list
  duck api list --tag Security
  duck api list --output json`,
		RunE: func(cmd *cobra.Command, _ []string) error {
			endpoints := gen.APIEndpoints

			if tag != "" {
				lowerTag := strings.ToLower(tag)
				var filtered []gen.APIEndpoint
				for _, ep := range endpoints {
					for _, t := range ep.Tags {
						if strings.ToLower(t) == lowerTag {
							filtered = append(filtered, ep)
							break
						}
					}
				}
				endpoints = filtered
			}

			if getOutputFormat(cmd) == "json" {
				return gen.PrintJSON(os.Stdout, endpoints)
			}

			columns := []string{"method", "path", "operation_id", "summary"}
			rows := make([][]string, 0, len(endpoints))
			for _, ep := range endpoints {
				rows = append(rows, []string{ep.Method, ep.Path, ep.OperationID, ep.Summary})
			}
			gen.PrintTable(os.Stdout, columns, rows)
			return nil
		},
	}

	cmd.Flags().StringVar(&tag, "tag", "", "Filter by API tag (e.g., Security, Catalogs)")

	return cmd
}

func newAPISearchCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "search <query>",
		Short: "Search API endpoints by path, summary, or parameter names",
		Example: `  duck api search "row-filter"
  duck api search "schema" --output json`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			query := strings.ToLower(args[0])
			var matches []gen.APIEndpoint

			for _, ep := range gen.APIEndpoints {
				// Search across path, summary, description, operation ID, and parameter names
				searchText := strings.ToLower(ep.Path + " " + ep.Summary + " " + ep.Description + " " + ep.OperationID)
				for _, p := range ep.Parameters {
					searchText += " " + strings.ToLower(p.Name)
				}
				for _, f := range ep.BodyFields {
					searchText += " " + strings.ToLower(f.Name)
				}

				if strings.Contains(searchText, query) {
					matches = append(matches, ep)
				}
			}

			if getOutputFormat(cmd) == "json" {
				return gen.PrintJSON(os.Stdout, matches)
			}

			columns := []string{"method", "path", "operation_id", "summary"}
			rows := make([][]string, 0, len(matches))
			for _, ep := range matches {
				rows = append(rows, []string{ep.Method, ep.Path, ep.OperationID, ep.Summary})
			}
			gen.PrintTable(os.Stdout, columns, rows)
			return nil
		},
	}
}

func newAPIDescribeCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "describe <operation-id>",
		Short: "Show detailed information about an API endpoint",
		Long:  "Displays full endpoint detail: method, path, parameters, body fields, and the corresponding CLI command.",
		Example: `  duck api describe createSchema
  duck api describe executeQuery --output json`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			opID := args[0]
			var found *gen.APIEndpoint
			for i := range gen.APIEndpoints {
				if gen.APIEndpoints[i].OperationID == opID {
					found = &gen.APIEndpoints[i]
					break
				}
			}
			if found == nil {
				return fmt.Errorf("operation %q not found", opID)
			}

			if getOutputFormat(cmd) == "json" {
				return gen.PrintJSON(os.Stdout, found)
			}

			// Human-friendly detail
			fmt.Fprintf(os.Stdout, "ENDPOINT: %s %s\n", found.Method, found.Path)
			fmt.Fprintf(os.Stdout, "operation_id:  %s\n", found.OperationID)
			fmt.Fprintf(os.Stdout, "summary:       %s\n", found.Summary)
			if found.Description != "" {
				fmt.Fprintf(os.Stdout, "description:   %s\n", found.Description)
			}
			if len(found.Tags) > 0 {
				fmt.Fprintf(os.Stdout, "tags:          %s\n", strings.Join(found.Tags, ", "))
			}
			if found.CLICommand != "" {
				fmt.Fprintf(os.Stdout, "cli_command:   duck %s\n", found.CLICommand)
			}

			if len(found.Parameters) > 0 {
				fmt.Fprintln(os.Stdout, "\nPARAMETERS:")
				columns := []string{"name", "in", "type", "required", "enum"}
				var rows [][]string
				for _, p := range found.Parameters {
					req := ""
					if p.Required {
						req = "yes"
					}
					rows = append(rows, []string{p.Name, p.In, p.Type, req, strings.Join(p.Enum, ", ")})
				}
				gen.PrintTable(os.Stdout, columns, rows)
			}

			if len(found.BodyFields) > 0 {
				fmt.Fprintln(os.Stdout, "\nBODY FIELDS:")
				columns := []string{"name", "type", "required", "enum"}
				var rows [][]string
				for _, f := range found.BodyFields {
					req := ""
					if f.Required {
						req = "yes"
					}
					rows = append(rows, []string{f.Name, f.Type, req, strings.Join(f.Enum, ", ")})
				}
				gen.PrintTable(os.Stdout, columns, rows)
			}

			return nil
		},
	}
}

func newAPICurlCmd() *cobra.Command {
	var params []string

	cmd := &cobra.Command{
		Use:   "curl <operation-id>",
		Short: "Generate a curl command for an API endpoint",
		Long:  "Generates a ready-to-use curl command using the current authentication configuration.",
		Example: `  duck api curl createSchema --param catalogName=main --param name=analytics
  duck api curl listSchemas --param catalogName=main`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			opID := args[0]
			var found *gen.APIEndpoint
			for i := range gen.APIEndpoints {
				if gen.APIEndpoints[i].OperationID == opID {
					found = &gen.APIEndpoints[i]
					break
				}
			}
			if found == nil {
				return fmt.Errorf("operation %q not found", opID)
			}

			// Parse --param flags into a map
			paramMap := map[string]string{}
			for _, p := range params {
				parts := strings.SplitN(p, "=", 2)
				if len(parts) == 2 {
					paramMap[parts[0]] = parts[1]
				}
			}

			// Build URL with path parameter substitution
			host, _ := cmd.Root().PersistentFlags().GetString("host")
			path := found.Path
			for _, p := range found.Parameters {
				if p.In == "path" {
					if v, ok := paramMap[p.Name]; ok {
						path = strings.ReplaceAll(path, "{"+p.Name+"}", url.PathEscape(v))
						delete(paramMap, p.Name)
					}
				}
			}

			// Build query string from remaining query params
			var queryParts []string
			for _, p := range found.Parameters {
				if p.In == "query" {
					if v, ok := paramMap[p.Name]; ok {
						queryParts = append(queryParts, url.QueryEscape(p.Name)+"="+url.QueryEscape(v))
						delete(paramMap, p.Name)
					}
				}
			}

			fullURL := host + "/v1" + path
			if len(queryParts) > 0 {
				fullURL += "?" + strings.Join(queryParts, "&")
			}

			// Build curl command
			var curlParts []string
			curlParts = append(curlParts, "curl")
			curlParts = append(curlParts, "-X", found.Method)
			curlParts = append(curlParts, fmt.Sprintf("'%s'", fullURL))

			// Auth
			token, _ := cmd.Root().PersistentFlags().GetString("token")
			apiKey, _ := cmd.Root().PersistentFlags().GetString("api-key")
			if token != "" {
				curlParts = append(curlParts, "-H", fmt.Sprintf("'Authorization: Bearer %s'", token))
			} else if apiKey != "" {
				curlParts = append(curlParts, "-H", fmt.Sprintf("'X-API-Key: %s'", apiKey))
			}

			// Body from remaining params
			if len(found.BodyFields) > 0 && len(paramMap) > 0 {
				curlParts = append(curlParts, "-H", "'Content-Type: application/json'")
				bodyParts := make([]string, 0, len(paramMap))
				for k, v := range paramMap {
					bodyParts = append(bodyParts, fmt.Sprintf("%q:%q", k, v))
				}
				curlParts = append(curlParts, "-d", fmt.Sprintf("'{%s}'", strings.Join(bodyParts, ",")))
			}

			result := strings.Join(curlParts, " \\\n  ")

			if getOutputFormat(cmd) == "json" {
				return gen.PrintJSON(os.Stdout, map[string]string{
					"curl": result,
				})
			}

			fmt.Fprintln(os.Stdout, result)
			return nil
		},
	}

	cmd.Flags().StringArrayVar(&params, "param", nil, "Parameter values (key=value, repeatable)")

	return cmd
}
