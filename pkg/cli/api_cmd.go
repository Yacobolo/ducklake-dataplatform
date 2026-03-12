package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"

	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"

	"duck-demo/pkg/cli/apiruntime"
	"duck-demo/pkg/cli/gen"
)

func newAPICmd(client *apiruntime.Client) *cobra.Command {
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
	cmd.AddCommand(newAPISpecCmd(client))

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
			endpoints := allAPIEndpoints()

			if tag != "" {
				lowerTag := strings.ToLower(tag)
				var filtered []gen.APIGenEndpoint
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
				return apiruntime.PrintJSON(os.Stdout, endpoints)
			}

			columns := []string{"method", "path", "operation_id", "summary"}
			rows := make([][]string, 0, len(endpoints))
			for _, ep := range endpoints {
				rows = append(rows, []string{ep.Method, ep.Path, ep.OperationID, ep.Summary})
			}
			apiruntime.PrintTable(os.Stdout, columns, rows)
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
			var matches []gen.APIGenEndpoint

			for _, ep := range allAPIEndpoints() {
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
				return apiruntime.PrintJSON(os.Stdout, matches)
			}

			columns := []string{"method", "path", "operation_id", "summary"}
			rows := make([][]string, 0, len(matches))
			for _, ep := range matches {
				rows = append(rows, []string{ep.Method, ep.Path, ep.OperationID, ep.Summary})
			}
			apiruntime.PrintTable(os.Stdout, columns, rows)
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
			var found *gen.APIGenEndpoint
			endpoints := allAPIEndpoints()
			for i := range endpoints {
				if endpoints[i].OperationID == opID {
					found = &endpoints[i]
					break
				}
			}
			if found == nil {
				return fmt.Errorf("operation %q not found", opID)
			}

			corpus := loadDiscoveryCorpus(cmd.Root())
			relatedDocs := corpus.RelatedDocsForOperation(found.OperationID)
			relatedCommands := corpus.RelatedCommandsForOperation(found.OperationID)
			contentTypes := apiContentTypes(found.OperationID)

			if getOutputFormat(cmd) == "json" {
				payload := map[string]any{
					"operation_id":     found.OperationID,
					"method":           found.Method,
					"path":             found.Path,
					"summary":          found.Summary,
					"description":      found.Description,
					"tags":             found.Tags,
					"parameters":       found.Parameters,
					"body_fields":      found.BodyFields,
					"cli_command":      found.CLICommand,
					"content_types":    contentTypes,
					"parameter_count":  len(found.Parameters),
					"body_field_count": len(found.BodyFields),
					"related_docs":     relatedDocs,
					"related_commands": relatedCommands,
				}
				return apiruntime.PrintJSON(os.Stdout, payload)
			}

			// Human-friendly detail
			_, _ = fmt.Fprintf(os.Stdout, "ENDPOINT: %s %s\n", found.Method, found.Path)
			_, _ = fmt.Fprintf(os.Stdout, "operation_id:  %s\n", found.OperationID)
			_, _ = fmt.Fprintf(os.Stdout, "summary:       %s\n", found.Summary)
			if found.Description != "" {
				_, _ = fmt.Fprintf(os.Stdout, "description:   %s\n", found.Description)
			}
			if len(found.Tags) > 0 {
				_, _ = fmt.Fprintf(os.Stdout, "tags:          %s\n", strings.Join(found.Tags, ", "))
			}
			if found.CLICommand != "" {
				_, _ = fmt.Fprintf(os.Stdout, "cli_command:   duck %s\n", found.CLICommand)
			}
			if len(contentTypes) > 0 {
				_, _ = fmt.Fprintf(os.Stdout, "content_types: %s\n", strings.Join(contentTypes, ", "))
			}
			_, _ = fmt.Fprintf(os.Stdout, "parameters:    %d\n", len(found.Parameters))
			_, _ = fmt.Fprintf(os.Stdout, "body_fields:   %d\n", len(found.BodyFields))
			if len(relatedDocs) > 0 {
				_, _ = fmt.Fprintf(os.Stdout, "related_docs:  %s\n", strings.Join(relatedDocs, ", "))
			}
			if len(relatedCommands) > 0 {
				_, _ = fmt.Fprintf(os.Stdout, "related_cmds:  %s\n", strings.Join(relatedCommands, ", "))
			}

			if len(found.Parameters) > 0 {
				_, _ = fmt.Fprintln(os.Stdout, "\nPARAMETERS:")
				columns := []string{"name", "in", "type", "required", "enum", "description"}
				var rows [][]string
				for _, p := range found.Parameters {
					req := ""
					if p.Required {
						req = "yes"
					}
					rows = append(rows, []string{p.Name, p.In, p.Type, req, strings.Join(p.Enum, ", "), p.Description})
				}
				apiruntime.PrintTable(os.Stdout, columns, rows)
			}

			if len(found.BodyFields) > 0 {
				_, _ = fmt.Fprintln(os.Stdout, "\nBODY FIELDS:")
				columns := []string{"name", "type", "required", "enum", "description"}
				var rows [][]string
				for _, f := range found.BodyFields {
					req := ""
					if f.Required {
						req = "yes"
					}
					rows = append(rows, []string{f.Name, f.Type, req, strings.Join(f.Enum, ", "), f.Description})
				}
				apiruntime.PrintTable(os.Stdout, columns, rows)
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
			var found *gen.APIGenEndpoint
			endpoints := allAPIEndpoints()
			for i := range endpoints {
				if endpoints[i].OperationID == opID {
					found = &endpoints[i]
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
				bodyFieldTypes := make(map[string]string, len(found.BodyFields))
				for _, field := range found.BodyFields {
					bodyFieldTypes[field.Name] = field.Type
				}
				keys := make([]string, 0, len(paramMap))
				for k := range paramMap {
					keys = append(keys, k)
				}
				sort.Strings(keys)

				bodyParts := make([]string, 0, len(keys))
				for _, k := range keys {
					v := paramMap[k]
					bodyParts = append(bodyParts, fmt.Sprintf("%q:%s", k, apiCurlJSONValue(v, bodyFieldTypes[k])))
				}
				curlParts = append(curlParts, "-d", fmt.Sprintf("'{%s}'", strings.Join(bodyParts, ",")))
			}

			result := strings.Join(curlParts, " \\\n  ")

			if getOutputFormat(cmd) == "json" {
				return apiruntime.PrintJSON(os.Stdout, map[string]string{
					"curl": result,
				})
			}

			_, _ = fmt.Fprintln(os.Stdout, result)
			return nil
		},
	}

	cmd.Flags().StringArrayVar(&params, "param", nil, "Parameter values (key=value, repeatable)")

	return cmd
}

func newAPISpecCmd(client *apiruntime.Client) *cobra.Command {
	var (
		format string
		source string
	)

	cmd := &cobra.Command{
		Use:   "spec",
		Short: "Output the canonical OpenAPI spec",
		Long:  "Outputs the embedded OpenAPI spec by default, or fetches the live server spec when requested.",
		Args:  cobra.NoArgs,
		RunE: func(_ *cobra.Command, _ []string) error {
			specBytes, err := loadAPISpecBytes(client, source)
			if err != nil {
				return err
			}

			switch format {
			case "yaml":
				_, _ = os.Stdout.Write(specBytes)
				if len(specBytes) == 0 || specBytes[len(specBytes)-1] != '\n' {
					_, _ = fmt.Fprintln(os.Stdout)
				}
				return nil
			case "json":
				var payload any
				if err := yaml.Unmarshal(specBytes, &payload); err != nil {
					return fmt.Errorf("decode spec yaml: %w", err)
				}
				return apiruntime.PrintJSON(os.Stdout, payload)
			default:
				return fmt.Errorf("unsupported spec format %q", format)
			}
		},
	}

	cmd.Flags().StringVar(&format, "format", "yaml", "Output format: yaml or json")
	cmd.Flags().StringVar(&source, "source", "embedded", "Spec source: embedded or live")
	return cmd
}

func allAPIEndpoints() []gen.APIGenEndpoint {
	combined := make([]gen.APIGenEndpoint, 0, len(gen.APIGeneratedEndpoints))
	seen := make(map[string]struct{}, len(gen.APIGeneratedEndpoints))
	for _, generated := range gen.APIGeneratedEndpoints {
		if _, ok := seen[generated.OperationID]; ok {
			continue
		}
		seen[generated.OperationID] = struct{}{}
		combined = append(combined, applyEndpointOverrides(generated))
	}
	return combined
}

func applyEndpointOverrides(endpoint gen.APIGenEndpoint) gen.APIGenEndpoint {
	switch endpoint.OperationID {
	case "listDashboards":
		endpoint.CLICommand = "dashboards list"
	case "createDashboard":
		endpoint.CLICommand = "dashboards create"
	case "getDashboard":
		endpoint.CLICommand = "dashboards get"
	case "getResolvedDashboard":
		endpoint.CLICommand = "dashboards get-resolved"
	case "updateDashboard":
		endpoint.CLICommand = "dashboards update"
	case "deleteDashboard":
		endpoint.CLICommand = "dashboards delete"
	case "createDashboardWidget":
		endpoint.CLICommand = "dashboards widgets create"
	case "updateDashboardWidget":
		endpoint.CLICommand = "dashboards widgets update"
	case "deleteDashboardWidget":
		endpoint.CLICommand = "dashboards widgets delete"
	case "getAssetFreshness":
		endpoint.CLICommand = "assets freshness get"
	case "explainAssetFreshness":
		endpoint.CLICommand = "assets freshness explain"
	case "listAssetFreshnessRequirements":
		endpoint.CLICommand = "assets freshness requirements"
	case "listAssetFreshnessBlockers":
		endpoint.CLICommand = "assets freshness blockers"
	case "reconcileAssetFreshness":
		endpoint.CLICommand = "assets freshness reconcile"
	}
	return endpoint
}

func apiCurlJSONValue(raw string, fieldType string) string {
	switch fieldType {
	case "object", "array":
		if json.Valid([]byte(raw)) {
			return raw
		}
	case "boolean":
		if _, err := strconv.ParseBool(raw); err == nil {
			return strings.ToLower(raw)
		}
	case "integer", "number":
		if _, err := strconv.ParseFloat(raw, 64); err == nil {
			return raw
		}
	}

	encoded, err := json.Marshal(raw)
	if err != nil {
		return fmt.Sprintf("%q", raw)
	}
	return string(encoded)
}

func apiContentTypes(operationID string) []string {
	for _, operation := range gen.CLIReferenceIndex.Operations {
		if operation.OperationID == operationID {
			return append([]string(nil), operation.ContentTypes...)
		}
	}
	return nil
}

func loadAPISpecBytes(client *apiruntime.Client, source string) ([]byte, error) {
	switch source {
	case "", "embedded":
		return []byte(gen.CLIReferenceIndex.OpenAPISpecYAML), nil
	case "live":
		reqURL := strings.TrimRight(client.BaseURL, "/") + "/openapi.json"
		req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, reqURL, nil)
		if err != nil {
			return nil, fmt.Errorf("build live openapi request: %w", err)
		}
		resp, err := client.HTTPClient.Do(req)
		if err != nil {
			return nil, fmt.Errorf("fetch live openapi spec: %w", err)
		}
		body, err := apiruntime.ReadBody(resp)
		if err != nil {
			return nil, err
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, fmt.Errorf("fetch live openapi spec: HTTP %d", resp.StatusCode)
		}

		var payload any
		if err := json.Unmarshal(body, &payload); err != nil {
			return nil, fmt.Errorf("decode live openapi spec: %w", err)
		}
		yamlBytes, err := yaml.Marshal(payload)
		if err != nil {
			return nil, fmt.Errorf("encode live openapi spec as yaml: %w", err)
		}
		return yamlBytes, nil
	default:
		return nil, fmt.Errorf("unsupported spec source %q", source)
	}
}
