package model

import (
	"strings"

	"duck-demo/internal/domain"
)

// SelectModels filters models based on a selector string.
// Supported syntax:
//   - "" or "*"          — all models
//   - "model_name"       — single model (unqualified or project.name)
//   - "model_name+"      — model and all downstream dependents
//   - "+model_name"      — model and all upstream dependencies
//   - "+model_name+"     — upstream + model + downstream
//   - "tag:finance"      — all models with tag "finance"
//   - "project:sales"    — all models in project "sales"
func SelectModels(selector string, allModels []domain.Model) ([]domain.Model, error) {
	selector = strings.TrimSpace(selector)
	if selector == "" || selector == "*" {
		return allModels, nil
	}

	// Tag selector
	if strings.HasPrefix(selector, "tag:") {
		tag := strings.TrimPrefix(selector, "tag:")
		return filterByTag(allModels, tag), nil
	}

	// Project selector
	if strings.HasPrefix(selector, "project:") {
		project := strings.TrimPrefix(selector, "project:")
		return filterByProject(allModels, project), nil
	}

	// Graph selectors: +model+, +model, model+, model
	upstream := strings.HasPrefix(selector, "+")
	downstream := strings.HasSuffix(selector, "+")
	modelName := strings.Trim(selector, "+")

	// Find the target model
	target := findModel(allModels, modelName)
	if target == nil {
		return nil, domain.ErrNotFound("model %q not found", modelName)
	}

	if !upstream && !downstream {
		return []domain.Model{*target}, nil
	}

	// Build adjacency for graph traversal
	byName := indexByQualifiedName(allModels)
	selected := make(map[string]bool)
	selected[target.QualifiedName()] = true

	if upstream {
		collectUpstream(target, byName, selected)
	}
	if downstream {
		collectDownstream(target, allModels, selected)
	}

	var result []domain.Model
	for _, m := range allModels {
		if selected[m.QualifiedName()] {
			result = append(result, m)
		}
	}
	return result, nil
}

func filterByTag(models []domain.Model, tag string) []domain.Model {
	var result []domain.Model
	for _, m := range models {
		for _, t := range m.Tags {
			if t == tag {
				result = append(result, m)
				break
			}
		}
	}
	return result
}

func filterByProject(models []domain.Model, project string) []domain.Model {
	var result []domain.Model
	for _, m := range models {
		if m.ProjectName == project {
			result = append(result, m)
		}
	}
	return result
}

func findModel(models []domain.Model, name string) *domain.Model {
	// Try qualified name first
	for i, m := range models {
		if m.QualifiedName() == name {
			return &models[i]
		}
	}
	// Try unqualified name
	for i, m := range models {
		if m.Name == name {
			return &models[i]
		}
	}
	return nil
}

func indexByQualifiedName(models []domain.Model) map[string]*domain.Model {
	idx := make(map[string]*domain.Model, len(models))
	for i := range models {
		idx[models[i].QualifiedName()] = &models[i]
	}
	return idx
}

// collectUpstream traverses the dependency graph upward.
func collectUpstream(model *domain.Model, byName map[string]*domain.Model, selected map[string]bool) {
	for _, dep := range model.DependsOn {
		if selected[dep] {
			continue
		}
		selected[dep] = true
		if upstream, ok := byName[dep]; ok {
			collectUpstream(upstream, byName, selected)
		}
	}
}

// collectDownstream traverses the dependency graph downward.
func collectDownstream(model *domain.Model, allModels []domain.Model, selected map[string]bool) {
	target := model.QualifiedName()
	for i, m := range allModels {
		if selected[m.QualifiedName()] {
			continue
		}
		for _, dep := range m.DependsOn {
			if dep == target {
				selected[m.QualifiedName()] = true
				collectDownstream(&allModels[i], allModels, selected)
				break
			}
		}
	}
}
