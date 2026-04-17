package cobra

import (
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"unicode"

	spcobra "github.com/spf13/cobra"
)

type generatedCommandSpec struct {
	Endpoint             Endpoint
	CommandPath          []string
	PathParamNames       []string
	PositionalPathParams []string
	PositionalBodyName   bool
}

// AddGeneratedCommands builds Cobra commands from generated endpoint metadata.
func AddGeneratedCommands(rootCmd *spcobra.Command, client *Client, endpoints []Endpoint) error {
	specs, err := buildGeneratedCommandSpecsFromEndpoints(endpoints)
	if err != nil {
		return err
	}
	ensureGeneratedRootGroups(rootCmd, specs)

	groups := map[string]*spcobra.Command{}

	for _, spec := range specs {
		if len(spec.CommandPath) == 0 {
			continue
		}

		parent := rootCmd
		for i := 0; i < len(spec.CommandPath)-1; i++ {
			segment := spec.CommandPath[i]
			nodePath := strings.Join(spec.CommandPath[:i+1], " ")
			node, ok := groups[nodePath]
			if !ok {
				node = &spcobra.Command{
					Use:   segment,
					Short: generatedGroupDescription(nodePath),
					RunE: func(cmd *spcobra.Command, args []string) error {
						if len(args) == 0 {
							return cmd.Help()
						}
						_ = cmd.Help()
						return fmt.Errorf("unknown subcommand %q", args[0])
					},
				}
				if i == 0 {
					node.GroupID = generatedRootGroupID(segment)
				}
				parent.AddCommand(node)
				groups[nodePath] = node
			}
			parent = node
		}

		leaf := newGeneratedLeafCommand(spec, client)
		parent.AddCommand(leaf)
	}

	return nil
}

func ensureGeneratedRootGroups(rootCmd *spcobra.Command, specs []generatedCommandSpec) {
	required := map[string]struct{}{}
	for _, spec := range specs {
		if len(spec.CommandPath) == 0 {
			continue
		}
		required[generatedRootGroupID(spec.CommandPath[0])] = struct{}{}
	}

	for groupID := range required {
		if rootCmd.ContainsGroup(groupID) {
			continue
		}
		rootCmd.AddGroup(&spcobra.Group{
			ID:    groupID,
			Title: generatedRootGroupTitle(groupID),
		})
	}
}

func buildGeneratedCommandSpecsFromEndpoints(endpoints []Endpoint) ([]generatedCommandSpec, error) {
	specs := make([]generatedCommandSpec, 0, len(endpoints))
	seen := make(map[string]string, len(endpoints))
	parentRoots := map[string]bool{}

	for _, endpoint := range endpoints {
		parts := strings.Fields(endpoint.CLICommand)
		if len(parts) > 1 {
			parentRoots[parts[0]] = true
		}
	}

	for _, endpoint := range endpoints {
		if endpoint.CLICommand == "" {
			continue
		}
		commandPath := normalizeCommandPath(strings.Fields(endpoint.CLICommand))
		if len(commandPath) == 1 && parentRoots[commandPath[0]] {
			commandPath = append(commandPath, "execute")
		}

		normalizedPath := strings.Join(commandPath, " ")
		if existingOpID, ok := seen[normalizedPath]; ok {
			return nil, fmt.Errorf("duplicate generated CLI command %q for operations %q and %q", normalizedPath, existingOpID, endpoint.OperationID)
		}
		seen[normalizedPath] = endpoint.OperationID

		pathParams := pathParameterNames(endpoint.Path)
		positionalPath := selectPositionalPathParams(endpoint, pathParams)
		positionalBodyName := selectPositionalBodyName(endpoint, pathParams, positionalPath)

		specs = append(specs, generatedCommandSpec{
			Endpoint:             endpoint,
			CommandPath:          commandPath,
			PathParamNames:       pathParams,
			PositionalPathParams: positionalPath,
			PositionalBodyName:   positionalBodyName,
		})
	}

	sort.Slice(specs, func(i, j int) bool {
		left := strings.Join(specs[i].CommandPath, " ")
		right := strings.Join(specs[j].CommandPath, " ")
		if left == right {
			return specs[i].Endpoint.OperationID < specs[j].Endpoint.OperationID
		}
		return left < right
	})

	return specs, nil
}

func newGeneratedLeafCommand(spec generatedCommandSpec, client *Client) *spcobra.Command {
	endpoint := spec.Endpoint
	leaf := spec.CommandPath[len(spec.CommandPath)-1]

	argNames := make([]string, 0, len(spec.PositionalPathParams)+1)
	for _, p := range spec.PositionalPathParams {
		argNames = append(argNames, "<"+toArgName(p)+">")
	}
	if spec.PositionalBodyName {
		argNames = append(argNames, "<name>")
	}

	use := leaf
	if len(argNames) > 0 {
		use += " " + strings.Join(argNames, " ")
	}

	cmd := &spcobra.Command{
		Use:   use,
		Short: endpoint.Summary,
		Long:  endpoint.Description,
		Args:  spcobra.ExactArgs(len(argNames)),
		RunE: func(cmd *spcobra.Command, args []string) error {
			return runGeneratedEndpoint(cmd, client, spec, args)
		},
	}

	pathPositionalSet := make(map[string]struct{}, len(spec.PositionalPathParams))
	for _, p := range spec.PositionalPathParams {
		pathPositionalSet[p] = struct{}{}
	}

	for _, p := range endpoint.Parameters {
		if p.In == "path" {
			if _, positional := pathPositionalSet[p.Name]; positional {
				continue
			}
			flagName := toFlagName(p.Name)
			cmd.Flags().String(flagName, "", buildFlagUsage(p.Name, p.Type, p.Description, p.Enum, false))
			_ = cmd.MarkFlagRequired(flagName)
			continue
		}

		if p.In == "query" {
			flagName := toFlagName(p.Name)
			addTypedFlag(cmd, flagName, p.Type, p.Required, p.Enum, p.Description, p.Name, false)
			if p.Required {
				_ = cmd.MarkFlagRequired(flagName)
			}
		}
	}

	hasBody := len(endpoint.BodyFields) > 0
	if hasBody {
		cmd.Flags().String("json", "", "JSON input (raw string or @filename or - for stdin)")
		for _, field := range endpoint.BodyFields {
			if spec.PositionalBodyName && field.Name == "name" {
				continue
			}
			addTypedFlag(cmd, toFlagName(field.Name), field.Type, field.Required, field.Enum, field.Description, field.Name, true)
		}
	}

	if endpoint.Method == "DELETE" {
		cmd.Flags().Bool("yes", false, "Skip confirmation prompt")
	}

	ApplyRunOverride(endpoint.OperationID, cmd, client)
	ApplyCommandOverride(endpoint.OperationID, cmd)

	return cmd
}

func runGeneratedEndpoint(cmd *spcobra.Command, client *Client, spec generatedCommandSpec, args []string) error {
	endpoint := spec.Endpoint

	if endpoint.Method == "DELETE" && !cmd.Flags().Changed("yes") {
		if !ConfirmPrompt("Are you sure?") {
			return nil
		}
	}

	argIndexByPathParam := make(map[string]int, len(spec.PositionalPathParams))
	for i, p := range spec.PositionalPathParams {
		argIndexByPathParam[p] = i
	}

	urlPath := endpoint.Path
	for _, name := range spec.PathParamNames {
		if argIndex, ok := argIndexByPathParam[name]; ok {
			urlPath = strings.Replace(urlPath, "{"+name+"}", url.PathEscape(args[argIndex]), 1)
			continue
		}
		v, _ := cmd.Flags().GetString(toFlagName(name))
		if v != "" {
			urlPath = strings.Replace(urlPath, "{"+name+"}", url.PathEscape(v), 1)
		}
	}

	if strings.Contains(urlPath, "{") {
		return fmt.Errorf("unresolved path parameter in URL: %s", urlPath)
	}

	query := url.Values{}
	for _, p := range endpoint.Parameters {
		if p.In != "query" {
			continue
		}
		flagName := toFlagName(p.Name)
		if !cmd.Flags().Changed(flagName) {
			continue
		}
		if err := setQueryValueFromFlag(cmd, query, p.Name, flagName, p.Type); err != nil {
			return err
		}
	}

	var body interface{}
	jsonInput := ""
	hasBody := len(endpoint.BodyFields) > 0
	if hasBody {
		jsonInput, _ = cmd.Flags().GetString("json")
		if jsonInput != "" {
			raw, err := readRawJSONInput(jsonInput)
			if err != nil {
				return err
			}
			body = raw
		} else {
			m := map[string]interface{}{}
			if spec.PositionalBodyName {
				m["name"] = args[len(spec.PositionalPathParams)]
			}
			for _, field := range endpoint.BodyFields {
				if spec.PositionalBodyName && field.Name == "name" {
					continue
				}
				flagName := toFlagName(field.Name)
				if !cmd.Flags().Changed(flagName) {
					continue
				}
				value, err := getFlagValue(cmd, flagName, field.Type)
				if err != nil {
					return err
				}
				m[field.Name] = value
			}
			body = m
		}

		if jsonInput == "" {
			for _, field := range endpoint.BodyFields {
				if !field.Required || field.Name != "name" {
					continue
				}
				if spec.PositionalBodyName {
					continue
				}
				flagName := toFlagName(field.Name)
				if !cmd.Flags().Changed(flagName) {
					return fmt.Errorf("required flag %q not set (or use --json)", flagName)
				}
			}

			for _, field := range endpoint.BodyFields {
				if !field.Required {
					continue
				}
				if field.Name == "name" {
					continue
				}
				if spec.PositionalBodyName && field.Name == "name" {
					continue
				}
				flagName := toFlagName(field.Name)
				if !cmd.Flags().Changed(flagName) {
					return fmt.Errorf("required flag %q not set (or use --json)", flagName)
				}
			}
		}
	}

	if endpoint.OperationID == "setDefaultCatalog" {
		body = map[string]interface{}{}
	}

	resp, err := client.Do(endpoint.Method, urlPath, query, body)
	if err != nil {
		return err
	}
	if err := CheckError(resp); err != nil {
		return err
	}

	if endpoint.Method == "DELETE" {
		if outputFormat(cmd) == OutputJSON {
			return PrintJSON(os.Stdout, map[string]string{"status": "ok"})
		}
		_, _ = fmt.Fprintln(os.Stdout, "Done.")
		return nil
	}

	respBody, err := ReadBody(resp)
	if err != nil {
		return fmt.Errorf("read response: %w", err)
	}

	if quietMode(cmd) {
		var data map[string]interface{}
		if err := json.Unmarshal(respBody, &data); err == nil {
			if items, ok := data["data"].([]interface{}); ok {
				for _, item := range items {
					if m, ok := item.(map[string]interface{}); ok {
						for _, key := range []string{"id", "name", "key"} {
							if v, ok := m[key]; ok {
								_, _ = fmt.Fprintln(os.Stdout, v)
								break
							}
						}
					}
				}
				return nil
			}
			for _, key := range []string{"id", "name", "key"} {
				if v, ok := data[key]; ok {
					_, _ = fmt.Fprintln(os.Stdout, v)
					return nil
				}
			}
		}
		_, _ = fmt.Fprintln(os.Stdout, string(respBody))
		return nil
	}

	switch outputFormat(cmd) {
	case OutputJSON:
		var pretty interface{}
		_ = json.Unmarshal(respBody, &pretty)
		return PrintJSON(os.Stdout, pretty)
	default:
		var data map[string]interface{}
		if err := json.Unmarshal(respBody, &data); err != nil {
			return fmt.Errorf("parse response: %w", err)
		}
		PrintDetail(os.Stdout, data)
	}

	return nil
}

func selectPositionalPathParams(endpoint Endpoint, pathParams []string) []string {
	if len(pathParams) == 0 {
		return nil
	}

	if strings.HasPrefix(endpoint.OperationID, "create") {
		selected := make([]string, 0, len(pathParams))
		for _, p := range pathParams {
			if p == "catalog_name" {
				continue
			}
			selected = append(selected, p)
		}
		return selected
	}

	if strings.HasPrefix(endpoint.OperationID, "list") {
		return append([]string(nil), pathParams...)
	}

	selected := make([]string, 0, len(pathParams))
	for _, p := range pathParams {
		if p == "catalog_name" {
			continue
		}
		selected = append(selected, p)
	}
	if len(selected) == 0 {
		selected = append(selected, pathParams[len(pathParams)-1])
	}
	return selected
}

func selectPositionalBodyName(endpoint Endpoint, pathParams []string, positionalPath []string) bool {
	if !strings.HasPrefix(endpoint.OperationID, "create") {
		return false
	}
	if len(endpoint.BodyFields) == 0 {
		return false
	}

	hasRequiredName := false
	for _, field := range endpoint.BodyFields {
		if field.Name == "name" && field.Required {
			hasRequiredName = true
			break
		}
	}
	if !hasRequiredName {
		return false
	}

	if len(pathParams) == 0 {
		return false
	}

	if len(positionalPath) > 0 {
		return false
	}

	for _, p := range pathParams {
		if p != "catalog_name" {
			return false
		}
	}

	return true
}

func pathParameterNames(path string) []string {
	params := make([]string, 0, strings.Count(path, "{"))
	for i := 0; i < len(path); i++ {
		if path[i] != '{' {
			continue
		}
		j := i + 1
		for j < len(path) && path[j] != '}' {
			j++
		}
		if j >= len(path) || j == i+1 {
			continue
		}
		name := path[i+1 : j]
		valid := true
		for _, r := range name {
			if !unicode.IsLetter(r) && !unicode.IsDigit(r) && r != '_' {
				valid = false
				break
			}
		}
		if valid {
			params = append(params, name)
		}
		i = j
	}
	return params
}

func toFlagName(name string) string {
	var out []rune
	for i, r := range name {
		if r == '_' {
			out = append(out, '-')
			continue
		}
		if r >= 'A' && r <= 'Z' {
			if i > 0 {
				out = append(out, '-')
			}
			out = append(out, r+('a'-'A'))
			continue
		}
		out = append(out, r)
	}
	return string(out)
}

func toArgName(name string) string {
	return strings.ReplaceAll(toFlagName(name), "_", "-")
}

func addTypedFlag(cmd *spcobra.Command, name, typ string, required bool, enum []string, description, fallbackName string, bodyField bool) {
	usage := buildFlagUsage(fallbackName, typ, description, enum, bodyField)

	switch typ {
	case "integer", "int", "int32", "int64", "number":
		defaultValue := int64(0)
		if name == "max-results" {
			defaultValue = 100
		}
		cmd.Flags().Int64(name, defaultValue, usage)
	case "boolean", "bool":
		cmd.Flags().Bool(name, false, usage)
	case "array":
		cmd.Flags().StringSlice(name, nil, usage)
	default:
		cmd.Flags().String(name, "", usage)
	}

	_ = required
}

func buildFlagUsage(name, typ, description string, enum []string, bodyField bool) string {
	usage := strings.TrimSpace(description)
	if usage == "" {
		usage = humanizeIdentifier(name)
	}

	switch typ {
	case "object":
		if bodyField {
			usage += " (JSON object; use --json for nested input)"
		} else {
			usage += " (JSON object)"
		}
	case "array":
		if bodyField {
			usage += " (repeat flag or use --json for nested input)"
		} else {
			usage += " (repeat flag to pass multiple values)"
		}
	}

	if len(enum) > 0 {
		usage += " (one of: " + strings.Join(enum, ", ") + ")"
	}

	return usage
}

func humanizeIdentifier(name string) string {
	trimmed := strings.TrimSpace(name)
	if trimmed == "" {
		return "Value"
	}
	replaced := strings.NewReplacer("_", " ", "-", " ").Replace(toFlagName(trimmed))
	parts := strings.Fields(replaced)
	for i, part := range parts {
		if part == "" {
			continue
		}
		runes := []rune(strings.ToLower(part))
		runes[0] = []rune(strings.ToUpper(string(runes[0])))[0]
		parts[i] = string(runes)
	}
	return strings.Join(parts, " ")
}

func setQueryValueFromFlag(cmd *spcobra.Command, query url.Values, queryName, flagName, typ string) error {
	v, err := getFlagValue(cmd, flagName, typ)
	if err != nil {
		return err
	}
	if v == nil {
		return nil
	}

	switch value := v.(type) {
	case string:
		query.Set(queryName, value)
	case int64:
		query.Set(queryName, strconv.FormatInt(value, 10))
	case bool:
		query.Set(queryName, strconv.FormatBool(value))
	case []string:
		for _, item := range value {
			query.Add(queryName, item)
		}
	default:
		query.Set(queryName, fmt.Sprintf("%v", value))
	}

	return nil
}

func getFlagValue(cmd *spcobra.Command, flagName, typ string) (interface{}, error) {
	switch typ {
	case "integer", "int", "int32", "int64", "number":
		v, err := cmd.Flags().GetInt64(flagName)
		if err != nil {
			return nil, err
		}
		return v, nil
	case "boolean", "bool":
		v, err := cmd.Flags().GetBool(flagName)
		if err != nil {
			return nil, err
		}
		return v, nil
	case "array":
		v, err := cmd.Flags().GetStringSlice(flagName)
		if err != nil {
			return nil, err
		}
		return v, nil
	case "object":
		v, err := cmd.Flags().GetString(flagName)
		if err != nil {
			return nil, err
		}
		if strings.TrimSpace(v) == "" {
			return map[string]interface{}{}, nil
		}
		var obj map[string]interface{}
		if err := json.Unmarshal([]byte(v), &obj); err != nil {
			return nil, fmt.Errorf("parse --%s as JSON object: %w", flagName, err)
		}
		return obj, nil
	default:
		v, err := cmd.Flags().GetString(flagName)
		if err != nil {
			return nil, err
		}
		return v, nil
	}
}

func readRawJSONInput(jsonInput string) (interface{}, error) {
	var raw interface{}
	jsonData := jsonInput

	if jsonInput == "-" {
		data, err := os.ReadFile("/dev/stdin")
		if err != nil {
			return nil, fmt.Errorf("read stdin: %w", err)
		}
		jsonData = string(data)
	} else if strings.HasPrefix(jsonInput, "@") {
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

func outputFormat(cmd *spcobra.Command) OutputFormat {
	value, _ := cmd.Root().PersistentFlags().GetString("output")
	switch strings.TrimSpace(strings.ToLower(value)) {
	case "", "text", "table", "csv":
		return OutputText
	default:
		return OutputFormat(value)
	}
}

func quietMode(cmd *spcobra.Command) bool {
	value, _ := cmd.Root().PersistentFlags().GetBool("quiet")
	return value
}

func normalizeCommandPath(parts []string) []string {
	if len(parts) == 0 {
		return nil
	}
	normalized := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		if len(normalized) > 0 && normalized[len(normalized)-1] == part {
			continue
		}
		normalized = append(normalized, part)
	}
	return normalized
}

func generatedGroupDescription(nodePath string) string {
	if description, ok := generatedGroupDescriptions[nodePath]; ok {
		return description
	}
	if parts := strings.Fields(nodePath); len(parts) > 0 {
		return "Manage " + parts[len(parts)-1]
	}
	return "Manage resources"
}

func generatedRootGroupID(segment string) string {
	if groupID, ok := generatedRootGroupIDs[segment]; ok {
		return groupID
	}
	return "platform"
}

var generatedRootGroupIDs = map[string]string{
	"catalog":    "platform",
	"assets":     "platform",
	"audit":      "server",
	"compute":    "platform",
	"dashboards": "platform",
	"governance": "platform",
	"ingestion":  "platform",
	"lineage":    "explore",
	"me":         "explore",
	"models":     "platform",
	"notebooks":  "platform",
	"pipelines":  "platform",
	"query":      "platform",
	"security":   "server",
	"semantic":   "platform",
	"storage":    "platform",
}

var generatedGroupDescriptions = map[string]string{
	"catalog":               "Manage catalogs, schemas, tables, and registrations",
	"assets":                "Manage assets, runs, materializations, and freshness",
	"audit":                 "Inspect audit entries and platform activity",
	"compute":               "Manage compute endpoints, assignments, and health",
	"dashboards":            "Manage dashboards and widgets",
	"governance":            "Manage governance resources such as tags and policies",
	"ingestion":             "Manage ingestion jobs and commits",
	"lineage":               "Inspect lineage relationships and impact",
	"me":                    "Inspect personal saved and recent resources",
	"models":                "Manage models, macros, tests, and related resources",
	"notebooks":             "Manage notebooks, sessions, and jobs",
	"pipelines":             "Manage pipelines and their runs",
	"query":                 "Run queries and inspect query history",
	"security":              "Manage principals, groups, grants, and API keys",
	"semantic":              "Manage semantic models, metrics, and relationships",
	"storage":               "Manage storage credentials and locations",
	"catalog registrations": "Manage registered catalogs and defaults",
}

func generatedRootGroupTitle(groupID string) string {
	if title, ok := generatedRootGroupTitles[groupID]; ok {
		return title
	}
	return humanizeIdentifier(groupID)
}

var generatedRootGroupTitles = map[string]string{
	"explore":  "Exploration",
	"platform": "Platform Resources",
	"server":   "Server/Admin",
}
