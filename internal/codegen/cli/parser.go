package cli

import (
	"fmt"
	"sort"
	"strings"
	"unicode"

	"github.com/getkin/kin-openapi/openapi3"
)

// Parse reads the OpenAPI spec and CLI config, infers conventions, applies overrides,
// validates coverage and drift, and builds the command model.
func Parse(spec *openapi3.T, cfg *Config) ([]GroupModel, error) {
	// Build a map of operationId -> operation info from spec
	specOps := map[string]*opInfo{}
	for urlPath, pathItem := range spec.Paths.Map() {
		for method, op := range pathItem.Operations() {
			if op.OperationID == "" {
				continue
			}
			// Collect path-level + operation-level parameters
			var allParams []*openapi3.ParameterRef
			allParams = append(allParams, pathItem.Parameters...)
			allParams = append(allParams, op.Parameters...)
			specOps[op.OperationID] = &opInfo{
				method:  method,
				urlPath: urlPath,
				op:      op,
				params:  allParams,
			}
		}
	}

	skipSet := toSet(cfg.SkipOperations)
	implicitParams := toSet(cfg.Global.ImplicitParams)

	// Validate override keys reference real operations
	for opID := range cfg.CommandOverrides {
		if _, ok := specOps[opID]; !ok && !skipSet[opID] {
			return nil, fmt.Errorf("DRIFT ERROR: command_overrides references operationId %q which does not exist in spec (and is not in skip_operations)", opID)
		}
	}

	// Build resolved command configs from spec + conventions + overrides
	// Group operations by their inferred/overridden group name
	groupCommands := map[string]map[string]CommandConfig{} // group -> cmdName -> config
	groupShorts := map[string]string{}

	for opID, info := range specOps {
		if skipSet[opID] {
			continue
		}

		override := cfg.CommandOverrides[opID]

		// Resolve group
		group := resolveGroup(info, override, cfg.GroupOverrides)
		if _, ok := groupCommands[group]; !ok {
			groupCommands[group] = map[string]CommandConfig{}
			groupShorts[group] = groupDescription(group, cfg.GroupOverrides)
		}

		// Resolve command config
		cmdCfg := resolveCommandConfig(opID, info, override, implicitParams)

		// Generate command name from verb + command_path
		cmdName := generateCommandName(cmdCfg)

		groupCommands[group][cmdName] = cmdCfg
	}

	// Build group models
	groups := make([]GroupModel, 0, len(groupCommands))
	groupNames := sortedKeys(groupCommands)
	for _, groupName := range groupNames {
		commands := groupCommands[groupName]
		gm := GroupModel{
			Name:  groupName,
			Short: groupShorts[groupName],
		}

		cmdNames := sortedKeys(commands)
		for _, cmdName := range cmdNames {
			cmdCfg := commands[cmdName]
			info := specOps[cmdCfg.OperationID]

			cm, err := buildCommandModel(groupName, cmdName, cmdCfg, info, cfg)
			if err != nil {
				return nil, fmt.Errorf("build command %q: %w", cmdName, err)
			}
			gm.Commands = append(gm.Commands, *cm)
		}
		groups = append(groups, gm)
	}

	return groups, nil
}

// resolveGroup determines the CLI group for an operation.
func resolveGroup(info *opInfo, override CommandOverride, tagOverrides map[string]GroupOverride) string {
	if override.Group != "" {
		return override.Group
	}
	return inferGroup(info.op.Tags, tagOverrides)
}

// resolveCommandConfig builds a fully-resolved CommandConfig by merging
// convention-inferred defaults with explicit overrides.
func resolveCommandConfig(opID string, info *opInfo, override CommandOverride, implicitParams map[string]bool) CommandConfig {
	cfg := CommandConfig{
		OperationID: opID,
	}

	// Verb: override > convention
	if override.Verb != nil {
		cfg.Verb = *override.Verb
	} else {
		verb, ok := inferVerb(opID, info.method)
		if ok {
			cfg.Verb = verb
		} else {
			cfg.Verb = toKebabCase(opID)
		}
	}

	// Command path: override > convention
	if override.CommandPath != nil {
		cfg.CommandPath = *override.CommandPath
	} else {
		cfg.CommandPath = inferCommandPath(info.urlPath)
	}

	// Positional args: override > convention (path params only)
	if override.PositionalArgs != nil {
		cfg.PositionalArgs = *override.PositionalArgs
	} else {
		cfg.PositionalArgs = inferPositionalArgs(info.params, implicitParams)
	}

	// Table columns: override > convention (auto-derived)
	if override.TableColumns != nil {
		cfg.TableColumns = *override.TableColumns
	} else {
		cfg.TableColumns = inferTableColumns(info.op)
	}

	// Confirm: override > convention (DELETE = confirm)
	if override.Confirm != nil {
		cfg.Confirm = *override.Confirm
	} else {
		cfg.Confirm = inferConfirm(info.method)
	}

	// Pass-through fields (no convention, only overrides)
	cfg.Examples = override.Examples
	cfg.FlagAliases = override.FlagAliases
	cfg.FlattenFields = override.FlattenFields
	cfg.CompoundFlags = override.CompoundFlags
	cfg.ConditionalRequires = override.ConditionalRequires

	return cfg
}

// generateCommandName creates a kebab-case command name from verb and command path.
// The name is used as the key in the group's command map and must be unique within a group.
func generateCommandName(cfg CommandConfig) string {
	parts := []string{}
	if cfg.Verb != "" {
		parts = append(parts, cfg.Verb)
	}

	// For resource-specific operations, include the resource type in the name
	// to avoid collisions (e.g., "list" in schemas vs "list" in tables)
	if len(cfg.CommandPath) > 0 {
		// Singularize the command path for the name
		resource := cfg.CommandPath[len(cfg.CommandPath)-1]
		resource = strings.TrimSuffix(resource, "s") // naive singularize

		if cfg.Verb != "" {
			parts = append(parts, resource)
		} else {
			parts = []string{resource}
		}
	}

	if len(parts) == 0 {
		// Empty verb + empty command path: use the Use string from computeUseString
		use := computeUseString(cfg)
		return strings.Fields(use)[0] // take first word
	}
	return strings.Join(parts, "-")
}

type opInfo struct {
	method  string
	urlPath string
	op      *openapi3.Operation
	params  []*openapi3.ParameterRef
}

func buildCommandModel(groupName, _ string, cmdCfg CommandConfig, info *opInfo, _ *Config) (*CommandModel, error) {
	positionalSet := toSet(cmdCfg.PositionalArgs)

	cm := &CommandModel{
		OperationID: cmdCfg.OperationID,
		GroupName:   groupName,
		CommandPath: cmdCfg.CommandPath,
		Verb:        cmdCfg.Verb,
		Short:       info.op.Summary,
		Long:        info.op.Description,
		Examples:    cmdCfg.Examples,
		Method:      info.method,

		URLPath:        info.urlPath,
		PositionalArgs: cmdCfg.PositionalArgs,
		Confirm:        cmdCfg.Confirm,
		FlattenFields:  cmdCfg.FlattenFields,
		CompoundFlags:  cmdCfg.CompoundFlags,
	}

	// Auto-generate examples from spec if config doesn't provide any
	if len(cm.Examples) == 0 {
		cm.Examples = generateExamples(groupName, cmdCfg, info)
	}

	// Compute Use string
	cm.Use = computeUseString(cmdCfg)

	// Parse parameters
	for _, pRef := range info.params {
		p := pRef.Value
		if p == nil {
			continue
		}
		pm := paramToModel(p)
		if p.In == "path" {
			cm.PathParams = append(cm.PathParams, pm)
			if positionalSet[p.Name] {
				// Positional: validate it's required
				if !p.Required {
					return nil, fmt.Errorf("positional arg %q for %q must be required in spec", p.Name, cmdCfg.OperationID)
				}
				continue // don't add as flag
			}
		}
		if p.In == "query" {
			cm.QueryParams = append(cm.QueryParams, pm)
		}
		// Add as flag unless it's a positional
		if !positionalSet[p.Name] {
			fm := paramToFlag(p, cmdCfg.FlagAliases)
			cm.Flags = append(cm.Flags, fm)
		}
	}

	// Parse request body
	if info.op.RequestBody != nil && info.op.RequestBody.Value != nil {
		rb := info.op.RequestBody.Value
		if ct, ok := rb.Content["application/json"]; ok && ct.Schema != nil && ct.Schema.Value != nil {
			cm.HasBody = true
			schema := ct.Schema.Value
			requiredSet := toSet(schema.Required)
			flattenSet := toSet(cmdCfg.FlattenFields)

			for propName, propRef := range schema.Properties {
				prop := propRef.Value
				if prop == nil {
					continue
				}

				// Check if this is a nested object that should be flattened
				if flattenSet[propName] && prop.Type != nil && len(*prop.Type) > 0 && (*prop.Type)[0] == "object" && prop.Properties != nil {
					nestedRequired := toSet(prop.Required)
					for nestedName, nestedRef := range prop.Properties {
						np := nestedRef.Value
						if np == nil {
							continue
						}
						fm := fieldToFlag(nestedName, np, nestedRequired[nestedName], cmdCfg.FlagAliases)
						fm.IsBody = true
						fm.FieldName = propName + "." + nestedName
						cm.Flags = append(cm.Flags, fm)
						cm.BodyFields = append(cm.BodyFields, FieldModel{
							Name:     propName + "." + nestedName,
							GoName:   toPascalCase(nestedName),
							Type:     schemaType(np),
							GoType:   schemaGoType(np),
							Required: nestedRequired[nestedName],
						})
					}
					continue
				}

				// Check if this is a compound flag
				if compCfg, ok := cmdCfg.CompoundFlags[propName]; ok {
					fm := FlagModel{
						Name:           toKebabCase(propName),
						GoName:         toPascalCase(propName),
						GoType:         "[]string",
						CobraType:      "StringSlice",
						Required:       requiredSet[propName],
						Usage:          fmt.Sprintf("Compound flag: %s (format: %s)", propName, strings.Join(compCfg.Fields, compCfg.Separator)),
						IsBody:         true,
						FieldName:      propName,
						IsCompound:     true,
						CompoundFields: compCfg.Fields,
						CompoundSep:    compCfg.Separator,
					}
					cm.Flags = append(cm.Flags, fm)
					continue
				}

				// Skip positional body fields
				if positionalSet[propName] {
					cm.BodyFields = append(cm.BodyFields, FieldModel{
						Name:     propName,
						GoName:   toPascalCase(propName),
						Type:     schemaType(prop),
						GoType:   schemaGoType(prop),
						Required: requiredSet[propName],
					})
					continue
				}

				fm := fieldToFlag(propName, prop, requiredSet[propName], cmdCfg.FlagAliases)
				fm.IsBody = true
				fm.FieldName = propName
				cm.Flags = append(cm.Flags, fm)
				cm.BodyFields = append(cm.BodyFields, FieldModel{
					Name:     propName,
					GoName:   toPascalCase(propName),
					Type:     schemaType(prop),
					GoType:   schemaGoType(prop),
					Required: requiredSet[propName],
					IsArray:  isArrayType(prop),
					IsMap:    isMapType(prop),
				})
			}

			// Add --json flag for body commands
			cm.Flags = append(cm.Flags, FlagModel{
				Name:      "json",
				GoName:    "JSON",
				GoType:    "string",
				CobraType: "String",
				Usage:     "JSON input (raw string or @filename or - for stdin)",
				IsBody:    true,
				FieldName: "__json__",
			})
		}
	}

	// Validate positional args reference real path params or body fields
	if err := validatePositionalArgs(cmdCfg, cm); err != nil {
		return nil, err
	}

	// Sort flags for deterministic output
	sort.Slice(cm.Flags, func(i, j int) bool {
		return cm.Flags[i].Name < cm.Flags[j].Name
	})

	// Classify response
	cm.Response = classifyResponse(info.op, cmdCfg.TableColumns)

	// Validate table columns against response schema
	if err := validateTableColumns(cmdCfg, info.op); err != nil {
		return nil, err
	}

	return cm, nil
}

// validatePositionalArgs checks that every positional arg exists as a path param or body field.
func validatePositionalArgs(cmdCfg CommandConfig, cm *CommandModel) error {
	for _, pa := range cmdCfg.PositionalArgs {
		foundInPath := false
		for _, pp := range cm.PathParams {
			if pp.Name == pa {
				foundInPath = true
				break
			}
		}
		foundInBody := false
		for _, bf := range cm.BodyFields {
			if bf.Name == pa {
				foundInBody = true
				break
			}
		}
		// Also check if it's a body property that was skipped via positionalSet
		if !foundInPath && !foundInBody {
			// Check request body properties directly
			if cm.HasBody {
				foundInBody = true // if it's marked positional and body exists, it was skipped intentionally
			}
		}
		if !foundInPath && !foundInBody {
			return fmt.Errorf("positional_arg %q for %q not found as path param or body field", pa, cmdCfg.OperationID)
		}
	}
	return nil
}

// validateTableColumns checks that table columns reference fields that exist in the response schema.
func validateTableColumns(cmdCfg CommandConfig, op *openapi3.Operation) error {
	if len(cmdCfg.TableColumns) == 0 {
		return nil
	}

	itemSchema := getItemSchema(op)
	if itemSchema == nil || itemSchema.Properties == nil {
		return nil // no schema to validate against
	}

	for _, col := range cmdCfg.TableColumns {
		if _, ok := itemSchema.Properties[col]; !ok {
			return fmt.Errorf("table_columns: field %q not found in response schema for %s", col, cmdCfg.OperationID)
		}
	}
	return nil
}

func classifyResponse(op *openapi3.Operation, tableColumns []string) ResponseModel {
	rm := ResponseModel{
		TableColumns: tableColumns,
	}

	// Check for 204 first
	if _, ok := op.Responses.Map()["204"]; ok {
		// If 204 is the only success response, it's NoContent
		has200 := false
		has201 := false
		if _, ok2 := op.Responses.Map()["200"]; ok2 {
			has200 = true
		}
		if _, ok2 := op.Responses.Map()["201"]; ok2 {
			has201 = true
		}
		if !has200 && !has201 {
			rm.Pattern = NoContent
			rm.SuccessCode = 204
			return rm
		}
	}

	// Check 200 or 201 responses
	for _, code := range []string{"200", "201"} {
		resp, ok := op.Responses.Map()[code]
		if !ok || resp == nil || resp.Value == nil {
			continue
		}
		ct, ok := resp.Value.Content["application/json"]
		if !ok || ct.Schema == nil || ct.Schema.Value == nil {
			continue
		}
		schema := ct.Schema.Value

		// Check for paginated pattern: has "data" array + "next_page_token"
		hasData := false
		hasNextPageToken := false
		var itemTypeName string
		if dataProp, ok := schema.Properties["data"]; ok && dataProp.Value != nil {
			dataSchema := dataProp.Value
			if dataSchema.Type != nil && len(*dataSchema.Type) > 0 && (*dataSchema.Type)[0] == "array" {
				hasData = true
				if dataSchema.Items != nil && dataSchema.Items.Ref != "" {
					itemTypeName = refToTypeName(dataSchema.Items.Ref)
				}
			}
		}
		if _, ok := schema.Properties["next_page_token"]; ok {
			hasNextPageToken = true
		}

		typeName := ""
		if ct.Schema.Ref != "" {
			typeName = refToTypeName(ct.Schema.Ref)
		}

		if code == "200" {
			rm.SuccessCode = 200
		} else {
			rm.SuccessCode = 201
		}

		if hasData && hasNextPageToken {
			rm.Pattern = PaginatedList
			rm.GoTypeName = typeName
			rm.ItemTypeName = itemTypeName
			return rm
		}

		rm.Pattern = SingleResource
		rm.GoTypeName = typeName
		return rm
	}

	rm.Pattern = CustomResult
	return rm
}

// Helper functions

func computeUseString(cmdCfg CommandConfig) string {
	verb := cmdCfg.Verb
	if verb == "" {
		// Use the first part of command path or operation name
		return "execute"
	}
	args := ""
	for _, pa := range cmdCfg.PositionalArgs {
		args += " <" + toKebabCase(pa) + ">"
	}
	return verb + args
}

func paramToModel(p *openapi3.Parameter) ParamModel {
	pm := ParamModel{
		Name:     p.Name,
		GoName:   toPascalCase(p.Name),
		Required: p.Required,
		In:       p.In,
	}
	if p.Schema != nil && p.Schema.Value != nil {
		pm.Type = schemaType(p.Schema.Value)
		pm.GoType = schemaGoType(p.Schema.Value)
		if p.Schema.Value.Default != nil {
			pm.Default = fmt.Sprintf("%v", p.Schema.Value.Default)
		}
	} else {
		pm.Type = "string"
		pm.GoType = "string"
	}
	return pm
}

func paramToFlag(p *openapi3.Parameter, aliases map[string]FlagAliasConfig) FlagModel {
	fm := FlagModel{
		Name:      toKebabCase(p.Name),
		GoName:    toPascalCase(p.Name),
		Required:  p.Required,
		FieldName: p.Name,
	}
	if p.Schema != nil && p.Schema.Value != nil {
		fm.GoType = schemaGoType(p.Schema.Value)
		fm.CobraType = goTypeToCobraType(fm.GoType)
		if p.Schema.Value.Default != nil {
			fm.Default = fmt.Sprintf("%v", p.Schema.Value.Default)
		}
	} else {
		fm.GoType = "string"
		fm.CobraType = "String"
	}
	fm.Usage = p.Description
	if fm.Usage == "" {
		fm.Usage = inferUsageFromName(p.Name)
	}
	if p.Schema != nil && p.Schema.Value != nil {
		fm.Usage = appendEnumHint(fm.Usage, p.Schema.Value.Enum)
	}
	if alias, ok := aliases[p.Name]; ok {
		fm.Short = alias.Short
	}
	return fm
}

func fieldToFlag(name string, schema *openapi3.Schema, required bool, aliases map[string]FlagAliasConfig) FlagModel {
	fm := FlagModel{
		Name:      toKebabCase(name),
		GoName:    toPascalCase(name),
		Required:  required,
		FieldName: name,
		IsBody:    true,
	}

	switch {
	case isMapType(schema):
		fm.GoType = "[]string"
		fm.CobraType = "StringSlice"
		fm.Usage = fmt.Sprintf("%s (key=value pairs)", name)
	case isArrayType(schema):
		fm.GoType = "[]string"
		fm.CobraType = "StringSlice"
		fm.Usage = schema.Description
	default:
		fm.GoType = schemaGoType(schema)
		fm.CobraType = goTypeToCobraType(fm.GoType)
		fm.Usage = schema.Description
	}

	if fm.Usage == "" {
		fm.Usage = inferUsageFromName(name)
	}
	fm.Usage = appendEnumHint(fm.Usage, schema.Enum)

	if schema.Default != nil {
		fm.Default = fmt.Sprintf("%v", schema.Default)
	}

	if alias, ok := aliases[name]; ok {
		fm.Short = alias.Short
	}

	return fm
}

// inferUsageFromName creates a human-readable usage string from a field name.
// For example, "member_type" becomes "Member type".
func inferUsageFromName(name string) string {
	words := strings.ReplaceAll(name, "_", " ")
	words = strings.ReplaceAll(words, "-", " ")
	if len(words) > 0 {
		return strings.ToUpper(words[:1]) + words[1:]
	}
	return name
}

// appendEnumHint appends a list of valid values to the usage string if enums are defined.
func appendEnumHint(usage string, enums []interface{}) string {
	if len(enums) == 0 {
		return usage
	}
	vals := make([]string, len(enums))
	for i, e := range enums {
		vals[i] = fmt.Sprintf("%v", e)
	}
	hint := fmt.Sprintf(" (one of: %s)", strings.Join(vals, ", "))
	return usage + hint
}

func schemaType(s *openapi3.Schema) string {
	if s.Type == nil || len(*s.Type) == 0 {
		return "string"
	}
	return (*s.Type)[0]
}

func schemaGoType(s *openapi3.Schema) string {
	if s.Type == nil || len(*s.Type) == 0 {
		return "string"
	}
	t := (*s.Type)[0]
	switch t {
	case "integer":
		return "int64"
	case "boolean":
		return "bool"
	case "number":
		return "float64"
	case "array":
		return "[]string"
	case "object":
		if s.AdditionalProperties.Schema != nil {
			return "map[string]string"
		}
		return "string" // fallback to JSON string
	default:
		return "string"
	}
}

func isArrayType(s *openapi3.Schema) bool {
	if s.Type == nil || len(*s.Type) == 0 {
		return false
	}
	return (*s.Type)[0] == "array"
}

func isMapType(s *openapi3.Schema) bool {
	if s.Type == nil || len(*s.Type) == 0 {
		return false
	}
	return (*s.Type)[0] == "object" && s.AdditionalProperties.Schema != nil
}

func goTypeToCobraType(goType string) string {
	switch goType {
	case "int64":
		return "Int64"
	case "bool":
		return "Bool"
	case "float64":
		return "Float64"
	case "[]string":
		return "StringSlice"
	case "map[string]string":
		return "StringToString"
	default:
		return "String"
	}
}

func refToTypeName(ref string) string {
	parts := strings.Split(ref, "/")
	return parts[len(parts)-1]
}

func toKebabCase(s string) string {
	var result strings.Builder
	for i, r := range s {
		if r == '_' || r == '-' {
			result.WriteRune('-')
			continue
		}
		if unicode.IsUpper(r) {
			if i > 0 {
				prev := rune(s[i-1])
				if prev != '_' && prev != '-' && !unicode.IsUpper(prev) {
					result.WriteRune('-')
				}
			}
			result.WriteRune(unicode.ToLower(r))
		} else {
			result.WriteRune(r)
		}
	}
	return result.String()
}

func toPascalCase(s string) string {
	parts := strings.FieldsFunc(s, func(r rune) bool {
		return r == '_' || r == '-'
	})
	var result strings.Builder
	for _, part := range parts {
		if len(part) == 0 {
			continue
		}
		// Handle common acronyms
		upper := strings.ToUpper(part)
		switch upper {
		case "ID", "URL", "SQL", "API", "HTTP", "HTTPS", "JSON", "XML", "CSV", "S3", "GCS", "UUID":
			result.WriteString(upper)
		default:
			result.WriteRune(unicode.ToUpper(rune(part[0])))
			result.WriteString(part[1:])
		}
	}
	return result.String()
}

func toCamelCase(s string) string {
	p := toPascalCase(s)
	if len(p) == 0 {
		return p
	}
	// Find where the initial uppercase run ends
	i := 0
	for i < len(p) && unicode.IsUpper(rune(p[i])) {
		i++
	}
	if i <= 1 {
		return strings.ToLower(string(p[0])) + p[1:]
	}
	// For acronyms like "ID" at the start, lowercase the entire acronym except last char
	return strings.ToLower(p[:i-1]) + p[i-1:]
}

func toSet(ss []string) map[string]bool {
	m := make(map[string]bool, len(ss))
	for _, s := range ss {
		m[s] = true
	}
	return m
}

// ExtractAPIEndpoints extracts all API endpoints from the OpenAPI spec for the api registry.
func ExtractAPIEndpoints(spec *openapi3.T, groups []GroupModel) []APIEndpointModel {
	// Build a map of operationID -> CLI command path from the group models
	cliCmdMap := map[string]string{}
	for _, g := range groups {
		for _, cmd := range g.Commands {
			path := g.Name
			if len(cmd.CommandPath) > 0 {
				path += " " + strings.Join(cmd.CommandPath, " ")
			}
			path += " " + cmd.Verb
			cliCmdMap[cmd.OperationID] = strings.TrimSpace(path)
		}
	}

	var endpoints []APIEndpointModel
	for urlPath, pathItem := range spec.Paths.Map() {
		for method, op := range pathItem.Operations() {
			if op.OperationID == "" {
				continue
			}
			ep := APIEndpointModel{
				OperationID: op.OperationID,
				Method:      method,
				Path:        urlPath,
				Summary:     op.Summary,
				Description: op.Description,
				Tags:        op.Tags,
				CLICommand:  cliCmdMap[op.OperationID],
			}

			// Collect path-level + operation-level parameters
			var allParams []*openapi3.ParameterRef
			allParams = append(allParams, pathItem.Parameters...)
			allParams = append(allParams, op.Parameters...)
			for _, pRef := range allParams {
				p := pRef.Value
				if p == nil {
					continue
				}
				param := APIParamModel{
					Name:     p.Name,
					In:       p.In,
					Required: p.Required,
				}
				if p.Schema != nil && p.Schema.Value != nil {
					param.Type = schemaType(p.Schema.Value)
					for _, e := range p.Schema.Value.Enum {
						param.Enum = append(param.Enum, fmt.Sprintf("%v", e))
					}
				}
				ep.Parameters = append(ep.Parameters, param)
			}

			// Collect body fields
			if op.RequestBody != nil && op.RequestBody.Value != nil {
				if ct, ok := op.RequestBody.Value.Content["application/json"]; ok && ct.Schema != nil && ct.Schema.Value != nil {
					schema := ct.Schema.Value
					requiredSet := toSet(schema.Required)
					for propName, propRef := range schema.Properties {
						prop := propRef.Value
						if prop == nil {
							continue
						}
						field := APIFieldModel{
							Name:     propName,
							Type:     schemaType(prop),
							Required: requiredSet[propName],
						}
						for _, e := range prop.Enum {
							field.Enum = append(field.Enum, fmt.Sprintf("%v", e))
						}
						ep.BodyFields = append(ep.BodyFields, field)
					}
				}
			}

			endpoints = append(endpoints, ep)
		}
	}

	// Sort by path then method for deterministic output
	sort.Slice(endpoints, func(i, j int) bool {
		if endpoints[i].Path == endpoints[j].Path {
			return endpoints[i].Method < endpoints[j].Method
		}
		return endpoints[i].Path < endpoints[j].Path
	})

	return endpoints
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
