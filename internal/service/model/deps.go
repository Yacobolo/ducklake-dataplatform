package model

import (
	"fmt"
	"sort"
	"strings"

	"duck-demo/internal/domain"
	"duck-demo/internal/duckdbsql"
)

// ExtractDependencies parses a model's SQL and resolves references to known models.
// Returns qualified model names: "project.name".
func ExtractDependencies(sqlText string, thisProject string, allModels []domain.Model) ([]string, error) {
	stmt, err := duckdbsql.Parse(sqlText)
	if err != nil {
		return nil, fmt.Errorf("parse model SQL: %w", err)
	}

	// Collect all table names from the statement
	tableRefs := duckdbsql.CollectTableNames(stmt)

	// Build lookup maps
	byQualified := make(map[string]*domain.Model, len(allModels))
	byName := make(map[string][]*domain.Model, len(allModels))
	for i := range allModels {
		m := &allModels[i]
		byQualified[m.ProjectName+"."+m.Name] = m
		byName[m.Name] = append(byName[m.Name], m)
	}

	seen := make(map[string]bool)
	var deps []string

	for _, ref := range tableRefs {
		if strings.HasPrefix(ref, "__func__") {
			continue
		}

		parts := strings.SplitN(ref, ".", 2)

		if len(parts) == 2 {
			qualName := ref
			if _, ok := byQualified[qualName]; ok {
				if !seen[qualName] {
					seen[qualName] = true
					deps = append(deps, qualName)
				}
				continue
			}
		}

		unqualName := ref
		if len(parts) == 2 {
			unqualName = parts[1]
		}

		sameProjectQual := thisProject + "." + unqualName
		if _, ok := byQualified[sameProjectQual]; ok {
			if !seen[sameProjectQual] {
				seen[sameProjectQual] = true
				deps = append(deps, sameProjectQual)
			}
			continue
		}

		if candidates, ok := byName[unqualName]; ok && len(candidates) == 1 {
			qualName := candidates[0].ProjectName + "." + candidates[0].Name
			if !seen[qualName] {
				seen[qualName] = true
				deps = append(deps, qualName)
			}
		}
	}

	sort.Strings(deps)
	return deps, nil
}
