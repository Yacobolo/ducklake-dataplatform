package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"

	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"

	"github.com/Yacobolo/quackstack/pkg/cli/apiruntime"
	"github.com/Yacobolo/quackstack/pkg/cli/gen"
)

func jsonLiteralForField(fieldType, raw string) (string, error) {
	switch strings.ToLower(fieldType) {
	case "bool", "boolean":
		value, err := strconv.ParseBool(raw)
		if err != nil {
			return "", fmt.Errorf("parse boolean value %q: %w", raw, err)
		}
		encoded, err := json.Marshal(value)
		if err != nil {
			return "", err
		}
		return string(encoded), nil
	case "int", "int32", "int64", "integer":
		value, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			return "", fmt.Errorf("parse integer value %q: %w", raw, err)
		}
		encoded, err := json.Marshal(value)
		if err != nil {
			return "", err
		}
		return string(encoded), nil
	case "float", "float32", "float64", "number":
		value, err := strconv.ParseFloat(raw, 64)
		if err != nil {
			return "", fmt.Errorf("parse numeric value %q: %w", raw, err)
		}
		encoded, err := json.Marshal(value)
		if err != nil {
			return "", err
		}
		return string(encoded), nil
	case "array", "object":
		if !json.Valid([]byte(raw)) {
			return "", fmt.Errorf("%s values must be valid JSON", fieldType)
		}
		return raw, nil
	default:
		encoded, err := json.Marshal(raw)
		if err != nil {
			return "", err
		}
		return string(encoded), nil
	}
}

func newAPICmd(client *apiruntime.Client) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "api",
		Short: "Inspect and call the platform API",
		Long: `Inspect the HTTP API surface and perform raw API requests.
Use the introspection commands to understand the contract, and the verb
commands as an escape hatch when no dedicated UX exists yet.`,
	}

	cmd.AddCommand(newAPIListCmd())
	cmd.AddCommand(newAPISearchCmd())
	cmd.AddCommand(newAPIDescribeCmd())
	cmd.AddCommand(newAPISpecCmd(client))
	cmd.AddCommand(newAPIRawMethodCmd(client, http.MethodGet))
	cmd.AddCommand(newAPIRawMethodCmd(client, http.MethodHead))
	cmd.AddCommand(newAPIRawMethodCmd(client, http.MethodPost))
	cmd.AddCommand(newAPIRawMethodCmd(client, http.MethodPut))
	cmd.AddCommand(newAPIRawMethodCmd(client, http.MethodPatch))
	cmd.AddCommand(newAPIRawMethodCmd(client, http.MethodDelete))

	return cmd
}

func newAPIRawMethodCmd(client *apiruntime.Client, method string) *cobra.Command {
	var jsonInput string

	cmd := &cobra.Command{
		Use:   strings.ToLower(method) + " <path>",
		Short: "Perform " + method + " request",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			path, query, err := normalizeRawAPIPath(args[0])
			if err != nil {
				return err
			}

			var body any
			if strings.TrimSpace(jsonInput) != "" {
				body, err = readRawJSONInput(jsonInput)
				if err != nil {
					return err
				}
			}

			resp, err := client.Do(method, path, query, body)
			if err != nil {
				return err
			}
			if err := apiruntime.CheckError(resp); err != nil {
				return err
			}

			if method == http.MethodHead || resp.StatusCode == http.StatusNoContent {
				if getOutputFormat(cmd) == "json" {
					return apiruntime.PrintJSON(os.Stdout, map[string]any{"status": "ok", "http_status": resp.StatusCode})
				}
				_, _ = fmt.Fprintf(os.Stdout, "HTTP %d\n", resp.StatusCode)
				return nil
			}

			bodyBytes, err := apiruntime.ReadBody(resp)
			if err != nil {
				return fmt.Errorf("read response: %w", err)
			}
			if len(bodyBytes) == 0 {
				if getOutputFormat(cmd) == "json" {
					return apiruntime.PrintJSON(os.Stdout, map[string]any{"status": "ok", "http_status": resp.StatusCode})
				}
				_, _ = fmt.Fprintf(os.Stdout, "HTTP %d\n", resp.StatusCode)
				return nil
			}

			var pretty any
			if json.Unmarshal(bodyBytes, &pretty) == nil {
				if getOutputFormat(cmd) == "json" {
					return apiruntime.PrintJSON(os.Stdout, pretty)
				}
				if object, ok := pretty.(map[string]any); ok {
					apiruntime.PrintDetail(os.Stdout, object)
					return nil
				}
				return apiruntime.PrintJSON(os.Stdout, pretty)
			}

			if getOutputFormat(cmd) == "json" {
				return apiruntime.PrintJSON(os.Stdout, map[string]string{"body": string(bodyBytes)})
			}
			_, _ = fmt.Fprintln(os.Stdout, string(bodyBytes))
			return nil
		},
	}

	cmd.Flags().StringVar(&jsonInput, "json", "", "Inline JSON string, @path/to/file.json, or - for stdin")
	return cmd
}

func newAPIListCmd() *cobra.Command {
	var tag string

	cmd := &cobra.Command{
		Use:   "list",
		Short: "List all API endpoints",
		Long:  "Lists all API endpoints with their HTTP method, path, and description.",
		Example: `  quack api list
  quack api list --tag Identity
  quack api list --output json`,
		RunE: func(cmd *cobra.Command, _ []string) error {
			endpoints := allAPIEndpoints()

			if tag != "" {
				lowerTag := strings.ToLower(tag)
				var filtered []gen.ReferenceOperation
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

	cmd.Flags().StringVar(&tag, "tag", "", "Filter by API tag (e.g., Identity, Catalogs)")

	return cmd
}

func newAPISearchCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "search <query>",
		Short: "Search API endpoints by path, summary, or parameter names",
		Example: `  quack api search "row-filter"
  quack api search "schema" --output json`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			query := strings.ToLower(args[0])
			var matches []gen.ReferenceOperation

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
		Example: `  quack api describe createSchema
  quack api describe executeQuery --output json`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			opID := args[0]
			var found *gen.ReferenceOperation
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
				_, _ = fmt.Fprintf(os.Stdout, "cli_command:   quack %s\n", found.CLICommand)
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

func allAPIEndpoints() []gen.ReferenceOperation {
	combined := make([]gen.ReferenceOperation, 0, len(gen.CLIReferenceIndex.Operations))
	seen := make(map[string]struct{}, len(gen.CLIReferenceIndex.Operations))
	for _, generated := range gen.CLIReferenceIndex.Operations {
		if _, ok := seen[generated.OperationID]; ok {
			continue
		}
		seen[generated.OperationID] = struct{}{}
		combined = append(combined, generated)
	}
	return combined
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

func normalizeRawAPIPath(input string) (string, url.Values, error) {
	trimmed := strings.TrimSpace(input)
	if trimmed == "" {
		return "", nil, fmt.Errorf("path is required")
	}
	if !strings.HasPrefix(trimmed, "/") {
		trimmed = "/" + trimmed
	}
	if strings.HasPrefix(trimmed, "/v1/") {
		trimmed = strings.TrimPrefix(trimmed, "/v1")
	}
	parsed, err := url.Parse(trimmed)
	if err != nil {
		return "", nil, fmt.Errorf("parse path %q: %w", input, err)
	}
	return parsed.Path, parsed.Query(), nil
}

func readRawJSONInput(jsonInput string) (any, error) {
	var raw any
	jsonData := jsonInput

	switch {
	case jsonInput == "-":
		data, err := os.ReadFile("/dev/stdin")
		if err != nil {
			return nil, fmt.Errorf("read stdin: %w", err)
		}
		jsonData = string(data)
	case strings.HasPrefix(jsonInput, "@"):
		data, err := os.ReadFile(jsonInput[1:])
		if err != nil {
			return nil, fmt.Errorf("read file: %w", err)
		}
		jsonData = string(data)
	}

	if err := json.Unmarshal([]byte(jsonData), &raw); err != nil {
		return nil, fmt.Errorf("parse JSON input: %w", err)
	}

	return raw, nil
}
