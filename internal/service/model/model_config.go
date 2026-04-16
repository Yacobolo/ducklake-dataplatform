package model

import (
	"fmt"
	"strings"

	"github.com/Yacobolo/quackstack/internal/domain"
)

func resolveEffectiveModel(model domain.Model) (domain.Model, []string, error) {
	effective := model
	effective.Tags = append([]string(nil), model.Tags...)
	effective.Config = normalizeModelConfig(model.Config)

	strippedSQL, inlineWarnings, err := applyInlineConfig(effective.SQL, &effective)
	if err != nil {
		return domain.Model{}, nil, err
	}
	effective.SQL = strippedSQL
	effective.Materialization = resolveModelMaterialization(effective.Materialization, effective.Config.Materialized)
	effective.Config.Materialized = strings.ToLower(strings.TrimSpace(materializationToConfigValue(effective.Materialization)))
	effective.Config.Schema = strings.TrimSpace(effective.Config.Schema)
	effective.Tags = dedupeSorted(append(effective.Tags, effective.Config.Tags...))

	return effective, inlineWarnings, nil
}

func normalizeModelConfig(config domain.ModelConfig) domain.ModelConfig {
	config.Materialized = strings.ToLower(strings.TrimSpace(config.Materialized))
	config.Schema = strings.TrimSpace(config.Schema)
	config.Tags = dedupeSorted(config.Tags)
	config.IncrementalStrategy = strings.TrimSpace(config.IncrementalStrategy)
	config.OnSchemaChange = strings.TrimSpace(config.OnSchemaChange)
	if len(config.UniqueKey) > 0 {
		config.UniqueKey = dedupeSorted(config.UniqueKey)
	}
	return config
}

func modelEnabled(model domain.Model) bool {
	return model.Config.Enabled == nil || *model.Config.Enabled
}

func applyInlineConfig(sqlText string, model *domain.Model) (string, []string, error) {
	var warnings []string
	var out strings.Builder

	for i := 0; i < len(sqlText); {
		if !strings.HasPrefix(sqlText[i:], "{{") {
			out.WriteByte(sqlText[i])
			i++
			continue
		}

		end := strings.Index(sqlText[i+2:], "}}")
		if end < 0 {
			return "", nil, domain.ErrValidation("unterminated expression tag")
		}

		expr := strings.TrimSpace(sqlText[i+2 : i+2+end])
		fnName, args, ok, err := parseFunctionCall(expr)
		if err != nil {
			return "", nil, err
		}
		if ok && fnName == "config" {
			nextWarnings, err := applyInlineConfigArgs(model, args)
			if err != nil {
				return "", nil, err
			}
			warnings = append(warnings, nextWarnings...)
			i += end + 4
			continue
		}

		out.WriteString(sqlText[i : i+end+4])
		i += end + 4
	}

	return strings.TrimSpace(out.String()), dedupeSorted(warnings), nil
}

func applyInlineConfigArgs(model *domain.Model, args []string) ([]string, error) {
	warnings := make([]string, 0)
	for _, arg := range args {
		key, raw, ok := strings.Cut(arg, "=")
		if !ok {
			warnings = append(warnings, fmt.Sprintf("ignored positional config() argument %q on model %s", strings.TrimSpace(arg), model.QualifiedName()))
			continue
		}
		key = strings.TrimSpace(key)
		raw = strings.TrimSpace(raw)

		switch key {
		case "materialized":
			value, err := unquoteString(raw)
			if err != nil {
				return nil, err
			}
			model.Config.Materialized = strings.ToLower(strings.TrimSpace(value))
		case "schema":
			value, err := unquoteString(raw)
			if err != nil {
				return nil, err
			}
			model.Config.Schema = strings.TrimSpace(value)
		case "incremental_strategy":
			value, err := unquoteString(raw)
			if err != nil {
				return nil, err
			}
			model.Config.IncrementalStrategy = strings.TrimSpace(value)
		case "on_schema_change":
			value, err := unquoteString(raw)
			if err != nil {
				return nil, err
			}
			model.Config.OnSchemaChange = strings.TrimSpace(value)
		case "enabled":
			value, err := parseBoolLiteral(raw)
			if err != nil {
				return nil, err
			}
			model.Config.Enabled = &value
		case "tags":
			values, err := parseStringListLiteral(raw)
			if err != nil {
				return nil, err
			}
			model.Config.Tags = values
		case "unique_key":
			values, err := parseStringOrStringListLiteral(raw)
			if err != nil {
				return nil, err
			}
			model.Config.UniqueKey = values
		default:
			warnings = append(warnings, fmt.Sprintf("ignored unsupported config key %q on model %s", key, model.QualifiedName()))
		}
	}

	model.Config = normalizeModelConfig(model.Config)
	return warnings, nil
}

func parseBoolLiteral(value string) (bool, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "true":
		return true, nil
	case "false":
		return false, nil
	default:
		return false, domain.ErrValidation("expected boolean config value, got %q", value)
	}
}

func parseStringOrStringListLiteral(value string) ([]string, error) {
	value = strings.TrimSpace(value)
	if strings.HasPrefix(value, "[") {
		return parseStringListLiteral(value)
	}
	item, err := unquoteString(value)
	if err != nil {
		return nil, err
	}
	return []string{item}, nil
}

func parseStringListLiteral(value string) ([]string, error) {
	value = strings.TrimSpace(value)
	if !strings.HasPrefix(value, "[") || !strings.HasSuffix(value, "]") {
		return nil, domain.ErrValidation("expected list literal, got %q", value)
	}
	items, err := splitArgs(strings.TrimSpace(value[1 : len(value)-1]))
	if err != nil {
		return nil, err
	}
	if len(items) == 0 {
		return nil, nil
	}
	out := make([]string, 0, len(items))
	for _, item := range items {
		parsed, err := unquoteString(item)
		if err != nil {
			return nil, err
		}
		out = append(out, parsed)
	}
	return dedupeSorted(out), nil
}

func resolveModelMaterialization(current, configured string) string {
	value := strings.TrimSpace(configured)
	if value == "" {
		return strings.ToUpper(strings.TrimSpace(current))
	}
	switch strings.ToLower(value) {
	case "view":
		return domain.MaterializationView
	case "table":
		return domain.MaterializationTable
	case "incremental":
		return domain.MaterializationIncremental
	case "ephemeral":
		return domain.MaterializationEphemeral
	case "seed":
		return domain.MaterializationSeed
	case "snapshot":
		return domain.MaterializationSnapshot
	default:
		return strings.ToUpper(strings.TrimSpace(current))
	}
}

func materializationToConfigValue(materialization string) string {
	switch strings.ToUpper(strings.TrimSpace(materialization)) {
	case domain.MaterializationView:
		return "view"
	case domain.MaterializationTable:
		return "table"
	case domain.MaterializationIncremental:
		return "incremental"
	case domain.MaterializationEphemeral:
		return "ephemeral"
	case domain.MaterializationSeed:
		return "seed"
	case domain.MaterializationSnapshot:
		return "snapshot"
	default:
		return strings.ToLower(strings.TrimSpace(materialization))
	}
}
