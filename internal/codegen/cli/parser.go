package cli

import (
	"fmt"
	"sort"
	"strings"
	"unicode"

	"github.com/getkin/kin-openapi/openapi3"
)

// Parse reads the OpenAPI spec and CLI config, validates coverage, and builds the model.
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

	// Coverage check: every spec operationId must be in config or skip_operations
	skipSet := toSet(cfg.SkipOperations)
	configOps := map[string]bool{}
	for _, group := range cfg.Groups {
		for _, cmd := range group.Commands {
			configOps[cmd.OperationID] = true
		}
	}
	for opID := range specOps {
		if !configOps[opID] && !skipSet[opID] {
			return nil, fmt.Errorf("SYNC ERROR: operationId %q exists in spec but not in cli-config.yaml (and not in skip_operations)", opID)
		}
	}
	// Reverse check: every config operationId must exist in spec
	for _, group := range cfg.Groups {
		for cmdName, cmd := range group.Commands {
			if _, ok := specOps[cmd.OperationID]; !ok {
				return nil, fmt.Errorf("ERROR: operationId %q (command %q) in config not found in spec", cmd.OperationID, cmdName)
			}
		}
	}

	// Build group models
	groups := make([]GroupModel, 0, len(cfg.Groups))
	// Sort group names for deterministic output
	groupNames := sortedKeys(cfg.Groups)
	for _, groupName := range groupNames {
		groupCfg := cfg.Groups[groupName]
		gm := GroupModel{
			Name:  groupName,
			Short: groupCfg.Short,
		}

		cmdNames := sortedKeys(groupCfg.Commands)
		for _, cmdName := range cmdNames {
			cmdCfg := groupCfg.Commands[cmdName]
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

type opInfo struct {
	method  string
	urlPath string
	op      *openapi3.Operation
	params  []*openapi3.ParameterRef
}

func buildCommandModel(groupName, _ string, cmdCfg CommandConfig, info *opInfo, _ *Config) (*CommandModel, error) {
	positionalSet := toSet(cmdCfg.PositionalArgs)

	cm := &CommandModel{
		OperationID:    cmdCfg.OperationID,
		GroupName:      groupName,
		CommandPath:    cmdCfg.CommandPath,
		Verb:           cmdCfg.Verb,
		Short:          info.op.Summary,
		Long:           info.op.Description,
		Examples:       cmdCfg.Examples,
		Method:         info.method,
		URLPath:        info.urlPath,
		PositionalArgs: cmdCfg.PositionalArgs,
		Confirm:        cmdCfg.Confirm,
		FlattenFields:  cmdCfg.FlattenFields,
		CompoundFlags:  cmdCfg.CompoundFlags,
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

	// Sort flags for deterministic output
	sort.Slice(cm.Flags, func(i, j int) bool {
		return cm.Flags[i].Name < cm.Flags[j].Name
	})

	// Classify response
	cm.Response = classifyResponse(info.op, cmdCfg.TableColumns)

	return cm, nil
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

	if schema.Default != nil {
		fm.Default = fmt.Sprintf("%v", schema.Default)
	}

	if alias, ok := aliases[name]; ok {
		fm.Short = alias.Short
	}

	return fm
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

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
