package cli

import (
	"fmt"
	"sort"
	"strings"
)

// generateExamples builds CLI usage example strings from an OpenAPI request body example.
// It returns nil if no example is available or cannot be converted.
// Config-level examples always take precedence (checked by the caller).
func generateExamples(groupName string, cmdCfg CommandConfig, info *opInfo) []string {
	prefix := buildCommandPrefix(groupName, cmdCfg)

	// Case 1: request body with an example object
	if info.op.RequestBody != nil && info.op.RequestBody.Value != nil {
		ct, ok := info.op.RequestBody.Value.Content["application/json"]
		if ok && ct != nil && ct.Example != nil {
			exMap, ok := ct.Example.(map[string]interface{})
			if !ok {
				return nil
			}
			example := buildBodyExample(prefix, cmdCfg, exMap, info)
			if example != "" {
				return []string{example}
			}
		}
		return nil
	}

	// Case 2: no request body — GET/DELETE with positional path params
	if len(cmdCfg.PositionalArgs) > 0 {
		example := buildNoBodyExample(prefix, cmdCfg, info)
		if example != "" {
			return []string{example}
		}
	}

	return nil
}

// buildCommandPrefix constructs "duck <group> <subcommand...> <verb>".
func buildCommandPrefix(groupName string, cmdCfg CommandConfig) string {
	parts := []string{"duck", groupName}
	parts = append(parts, cmdCfg.CommandPath...)
	if cmdCfg.Verb != "" {
		parts = append(parts, cmdCfg.Verb)
	} else {
		use := computeUseString(cmdCfg)
		useParts := strings.Fields(use)
		if len(useParts) > 0 {
			parts = append(parts, useParts[0])
		}
	}
	return strings.Join(parts, " ")
}

// buildBodyExample converts a request body example map into a CLI command string.
func buildBodyExample(
	prefix string,
	cmdCfg CommandConfig,
	exMap map[string]interface{},
	info *opInfo,
) string {
	var parts []string
	parts = append(parts, prefix)

	positionalSet := toSet(cmdCfg.PositionalArgs)
	compoundSet := make(map[string]bool)
	for k := range cmdCfg.CompoundFlags {
		compoundSet[k] = true
	}
	flattenSet := toSet(cmdCfg.FlattenFields)
	paramExamples := paramExampleValues(info)

	// 1. Positional arg values (in order) from body example or path param examples.
	for _, pa := range cmdCfg.PositionalArgs {
		if val, ok := exMap[pa]; ok {
			parts = append(parts, formatValue(val))
		} else if val, ok := paramExamples[pa]; ok {
			parts = append(parts, formatValue(val))
		} else {
			parts = append(parts, "<"+toKebabCase(pa)+">")
		}
	}

	// 2. Collect regular flag fields (non-positional, non-compound, non-flattened parent).
	var flagFields []flagField
	for k, v := range exMap {
		if positionalSet[k] || compoundSet[k] || flattenSet[k] {
			continue
		}
		flagFields = append(flagFields, flagField{name: k, value: v})
	}

	// 3. Flatten nested objects into flag fields.
	for _, flatField := range cmdCfg.FlattenFields {
		nested, ok := exMap[flatField]
		if !ok {
			continue
		}
		nestedMap, ok := nested.(map[string]interface{})
		if !ok {
			continue
		}
		for nk, nv := range nestedMap {
			flagFields = append(flagFields, flagField{name: nk, value: nv})
		}
	}

	// Sort flag fields for deterministic output.
	sort.Slice(flagFields, func(i, j int) bool {
		return flagFields[i].name < flagFields[j].name
	})

	// 4. Compound flags (sorted by field name for determinism).
	compoundNames := sortedKeys(cmdCfg.CompoundFlags)
	for _, flagName := range compoundNames {
		compCfg := cmdCfg.CompoundFlags[flagName]
		if arrVal, ok := exMap[flagName]; ok {
			parts = append(parts, formatCompoundFlag(flagName, compCfg, arrVal)...)
		}
	}

	// 5. Regular flags.
	for _, ff := range flagFields {
		parts = append(parts, formatFlag(ff.name, ff.value)...)
	}

	return strings.Join(parts, " ")
}

// buildNoBodyExample generates a simple example for GET/DELETE commands
// that have positional path params but no request body.
func buildNoBodyExample(
	prefix string,
	cmdCfg CommandConfig,
	info *opInfo,
) string {
	parts := []string{prefix}
	paramExamples := paramExampleValues(info)

	for _, pa := range cmdCfg.PositionalArgs {
		if val, ok := paramExamples[pa]; ok {
			parts = append(parts, formatValue(val))
		} else {
			parts = append(parts, "<"+toKebabCase(pa)+">")
		}
	}

	return strings.Join(parts, " ")
}

// flagField is a name-value pair for deterministic sorting.
type flagField struct {
	name  string
	value interface{}
}

// formatFlag converts a field name + value into CLI flag tokens.
func formatFlag(name string, value interface{}) []string {
	flagName := "--" + toKebabCase(name)

	switch v := value.(type) {
	case bool:
		if v {
			return []string{flagName}
		}
		// false booleans: omit entirely
		return nil
	case []interface{}:
		vals := make([]string, 0, len(v))
		for _, item := range v {
			vals = append(vals, formatValue(item))
		}
		if len(vals) == 0 {
			return nil
		}
		return []string{flagName, strings.Join(vals, ",")}
	case map[string]interface{}:
		keys := sortedMapKeys(v)
		var parts []string
		for _, mk := range keys {
			parts = append(parts, flagName, fmt.Sprintf("%s=%s", mk, formatValue(v[mk])))
		}
		return parts
	default:
		str := formatValue(v)
		if str == "" {
			return nil
		}
		if strings.ContainsAny(str, " \t") {
			return []string{flagName, fmt.Sprintf("%q", str)}
		}
		return []string{flagName, str}
	}
}

// formatCompoundFlag renders a compound flag (e.g. --columns id:BIGINT --columns email:VARCHAR).
func formatCompoundFlag(fieldName string, cfg CompoundFlagConfig, value interface{}) []string {
	arr, ok := value.([]interface{})
	if !ok {
		return nil
	}
	flagName := "--" + toKebabCase(fieldName)
	var parts []string
	for _, item := range arr {
		itemMap, ok := item.(map[string]interface{})
		if !ok {
			continue
		}
		var vals []string
		for _, field := range cfg.Fields {
			if fv, ok := itemMap[field]; ok {
				vals = append(vals, formatValue(fv))
			}
		}
		parts = append(parts, flagName, strings.Join(vals, cfg.Separator))
	}
	return parts
}

// paramExampleValues extracts example values from path/query parameters.
func paramExampleValues(info *opInfo) map[string]interface{} {
	m := make(map[string]interface{})
	for _, pRef := range info.params {
		p := pRef.Value
		if p == nil {
			continue
		}
		if p.Example != nil {
			m[p.Name] = p.Example
		} else if p.Schema != nil && p.Schema.Value != nil && p.Schema.Value.Example != nil {
			m[p.Name] = p.Schema.Value.Example
		}
	}
	return m
}

// formatValue converts an interface{} to its string representation.
func formatValue(v interface{}) string {
	if v == nil {
		return ""
	}
	return fmt.Sprintf("%v", v)
}

// sortedMapKeys returns the sorted keys of a map[string]interface{}.
func sortedMapKeys(m map[string]interface{}) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
