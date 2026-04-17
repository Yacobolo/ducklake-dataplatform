package model

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/Yacobolo/quackstack/internal/domain"
)

type compileMacroLayer struct {
	defs     map[string]compileMacroDefinition
	runtimes map[string]*starlarkMacroRuntime
}

func (s *Service) loadCompilationModelScope(ctx context.Context, runCtx *resolvedRunContext) ([]domain.Model, []string, error) {
	allModels, err := s.models.ListAll(ctx)
	if err != nil {
		return nil, nil, err
	}

	warnings := make([]string, 0)
	scope := make([]domain.Model, 0, len(allModels))
	for _, model := range allModels {
		if !projectAllowed(runCtx.allowedRefProjects, model.ProjectName) {
			continue
		}
		effective, modelWarnings, err := resolveEffectiveModel(model)
		if err != nil {
			return nil, nil, fmt.Errorf("resolve model config for %s: %w", model.QualifiedName(), err)
		}
		warnings = append(warnings, modelWarnings...)
		if !modelEnabled(effective) {
			continue
		}
		scope = append(scope, effective)
	}

	if s.seeds != nil {
		for projectName := range runCtx.allowedRefProjects {
			seeds, err := s.seeds.ListByProject(ctx, projectName)
			if err != nil {
				return nil, nil, fmt.Errorf("list seeds for project %s: %w", projectName, err)
			}
			for _, seed := range seeds {
				seedModel, err := seedToModel(seed)
				if err != nil {
					return nil, nil, fmt.Errorf("compile seed %s.%s: %w", seed.ProjectName, seed.Name, err)
				}
				if modelNameTaken(scope, seedModel.QualifiedName()) {
					return nil, nil, domain.ErrValidation("seed %s conflicts with an existing model in the same project", seedModel.QualifiedName())
				}
				scope = append(scope, seedModel)
			}
		}
	}

	return scope, dedupeSorted(warnings), nil
}

func sourceOverrideForKey(overrides map[string]string, projectName, sourceName, tableName string) (string, bool) {
	keys := []string{
		strings.TrimSpace(sourceName) + "." + strings.TrimSpace(tableName),
		strings.TrimSpace(projectName) + "." + strings.TrimSpace(sourceName) + "." + strings.TrimSpace(tableName),
	}
	for _, key := range keys {
		if value, ok := overrides[key]; ok {
			return value, true
		}
	}
	return "", false
}

func isSourceOverrideUsed(key string, usedSourceKeys map[string]struct{}, projectName string) bool {
	key = strings.TrimSpace(key)
	if key == "" {
		return false
	}
	for usedKey := range usedSourceKeys {
		parts := strings.SplitN(usedKey, ":", 2)
		if len(parts) != 2 {
			continue
		}
		qualified := parts[0] + "." + parts[1]
		if key == parts[1] || key == qualified {
			return true
		}
		if parts[0] == projectName && key == parts[1] {
			return true
		}
	}
	return false
}

func (s *Service) compileMacroLayers(
	ctx context.Context,
	projectName string,
	runCtx *resolvedRunContext,
) ([]compileMacroLayer, error) {
	projectOrder := append([]string(nil), runCtx.dependencyProjects...)
	layers := make([]compileMacroLayer, 0, 2+len(projectOrder)+1)

	root, err := os.Getwd()
	if err != nil {
		return nil, fmt.Errorf("resolve working directory for star macros: %w", err)
	}
	macrosDir := filepath.Join(root, "macros")
	_, statErr := os.Stat(macrosDir)
	hasMacrosDir := statErr == nil
	if statErr != nil && !os.IsNotExist(statErr) {
		return nil, fmt.Errorf("stat macros directory: %w", statErr)
	}

	appendLayer := func(scopeKey string, dbMacros []domain.Macro, scopeDir string) error {
		layer := compileMacroLayer{
			defs:     map[string]compileMacroDefinition{},
			runtimes: map[string]*starlarkMacroRuntime{},
		}

		if len(dbMacros) > 0 {
			runtimeKey := scopeKey + "/db"
			dbDefs := make(map[string]compileMacroDefinition, len(dbMacros))
			for _, item := range dbMacros {
				name := strings.TrimSpace(item.Name)
				if name == "" {
					continue
				}
				if _, exists := layer.defs[name]; exists {
					return domain.ErrValidation("duplicate macro %q at precedence scope %s", name, scopeKey)
				}
				def := compileMacroDefinition{
					name:       name,
					parameters: item.Parameters,
					body:       item.Body,
					starlark:   strings.Contains(name, "."),
					runtimeKey: runtimeKey,
					status:     macroStatusOrDefault(item.Status),
					origin:     scopeKey,
				}
				layer.defs[name] = def
				dbDefs[name] = def
			}
			runtime, err := newStarlarkMacroRuntime(dbDefs)
			if err != nil {
				return fmt.Errorf("load %s db macro runtime: %w", scopeKey, err)
			}
			layer.runtimes[runtimeKey] = runtime
		}

		if hasMacrosDir && strings.TrimSpace(scopeDir) != "" {
			moduleSources, err := loadStarModules(scopeDir)
			if err != nil {
				return err
			}
			if len(moduleSources) > 0 {
				runtimeKey := scopeKey + "/star"
				runtime, err := newStarlarkMacroRuntimeFromModules(moduleSources)
				if err != nil {
					return fmt.Errorf("load %s star runtime: %w", scopeKey, err)
				}
				layer.runtimes[runtimeKey] = runtime

				moduleNames := make([]string, 0, len(moduleSources))
				for module := range moduleSources {
					moduleNames = append(moduleNames, module)
				}
				sort.Strings(moduleNames)

				for _, module := range moduleNames {
					fnNames := topLevelFunctionNames(moduleSources[module])
					for _, fn := range fnNames {
						name := module + "." + fn
						if _, exists := layer.defs[name]; exists {
							return domain.ErrValidation("duplicate macro %q at precedence scope %s", name, scopeKey)
						}
						layer.defs[name] = compileMacroDefinition{
							name:       name,
							starlark:   true,
							runtimeKey: runtimeKey,
							status:     domain.MacroStatusActive,
							origin:     scopeKey,
						}
					}
				}
			}
		}

		if len(layer.defs) > 0 || len(layer.runtimes) > 0 {
			layers = append(layers, layer)
		}
		return nil
	}

	allMacros := make([]domain.Macro, 0)
	if s.macros != nil {
		allMacros, err = s.macros.ListAll(ctx)
		if err != nil {
			return nil, fmt.Errorf("list macros: %w", err)
		}
	}

	systemMacros, catalogMacros, projectMacros := partitionMacrosForCompilation(allMacros, projectName, runCtx.targetCatalog)
	if err := appendLayer("system", systemMacros, filepath.Join(macrosDir, "system")); err != nil {
		return nil, err
	}
	if err := appendLayer("catalog_global", catalogMacros, filepath.Join(macrosDir, "catalog_global")); err != nil {
		return nil, err
	}
	for i := len(projectOrder) - 1; i >= 0; i-- {
		dependencyProject := projectOrder[i]
		if err := appendLayer("dependency:"+dependencyProject, projectMacros[dependencyProject], filepath.Join(macrosDir, dependencyProject)); err != nil {
			return nil, err
		}
	}
	if err := appendLayer("project:"+projectName, projectMacros[projectName], filepath.Join(macrosDir, projectName)); err != nil {
		return nil, err
	}

	return layers, nil
}

func partitionMacrosForCompilation(
	all []domain.Macro,
	projectName string,
	targetCatalog string,
) ([]domain.Macro, []domain.Macro, map[string][]domain.Macro) {
	system := make([]domain.Macro, 0)
	catalogGlobal := make([]domain.Macro, 0)
	projectScoped := make(map[string][]domain.Macro)

	for _, macro := range all {
		status := macroStatusOrDefault(macro.Status)
		if status != domain.MacroStatusActive && status != domain.MacroStatusDeprecated {
			continue
		}
		switch macro.Visibility {
		case domain.MacroVisibilitySystem:
			system = append(system, macro)
		case domain.MacroVisibilityCatalogGlobal:
			if strings.TrimSpace(macro.CatalogName) == "" || strings.TrimSpace(macro.CatalogName) == strings.TrimSpace(targetCatalog) {
				catalogGlobal = append(catalogGlobal, macro)
			}
		default:
			project := strings.TrimSpace(macro.ProjectName)
			if project == "" {
				project = projectName
			}
			projectScoped[project] = append(projectScoped[project], macro)
		}
	}

	return system, catalogGlobal, projectScoped
}

func macroStatusOrDefault(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return domain.MacroStatusActive
	}
	return value
}

func (s *Service) loadStarMacroScopes(projectName string) (map[string]compileMacroDefinition, map[string]*starlarkMacroRuntime, error) {
	root, err := os.Getwd()
	if err != nil {
		return nil, nil, fmt.Errorf("resolve working directory for star macros: %w", err)
	}
	macrosDir := filepath.Join(root, "macros")
	if _, err := os.Stat(macrosDir); err != nil {
		if os.IsNotExist(err) {
			return map[string]compileMacroDefinition{}, map[string]*starlarkMacroRuntime{}, nil
		}
		return nil, nil, fmt.Errorf("stat macros directory: %w", err)
	}

	defs := make(map[string]compileMacroDefinition)
	runtimes := make(map[string]*starlarkMacroRuntime)
	scopes := []struct {
		key string
		dir string
	}{
		{key: "system", dir: filepath.Join(macrosDir, "system")},
		{key: "catalog_global", dir: filepath.Join(macrosDir, "catalog_global")},
		{key: "project", dir: filepath.Join(macrosDir, projectName)},
	}

	for _, scope := range scopes {
		moduleSources, err := loadStarModules(scope.dir)
		if err != nil {
			return nil, nil, err
		}
		if len(moduleSources) == 0 {
			continue
		}
		runtime, err := newStarlarkMacroRuntimeFromModules(moduleSources)
		if err != nil {
			return nil, nil, fmt.Errorf("load %s star runtime: %w", scope.key, err)
		}
		runtimes[scope.key] = runtime

		moduleNames := make([]string, 0, len(moduleSources))
		for module := range moduleSources {
			moduleNames = append(moduleNames, module)
		}
		sort.Strings(moduleNames)
		for _, module := range moduleNames {
			fnNames := topLevelFunctionNames(moduleSources[module])
			for _, fn := range fnNames {
				name := module + "." + fn
				defs[name] = compileMacroDefinition{
					name:       name,
					starlark:   true,
					runtimeKey: scope.key,
					status:     domain.MacroStatusActive,
					origin:     scope.key,
				}
			}
		}
	}

	return defs, runtimes, nil
}

func parseRelationRef(relationRef, defaultCatalog, defaultSchema string) (string, string, string) {
	parts := strings.Split(strings.TrimSpace(relationRef), ".")
	filtered := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.Trim(strings.TrimSpace(part), `"`)
		if part == "" {
			continue
		}
		filtered = append(filtered, part)
	}

	switch len(filtered) {
	case 0:
		return defaultCatalog, defaultSchema, ""
	case 1:
		return defaultCatalog, defaultSchema, filtered[0]
	case 2:
		return defaultCatalog, filtered[0], filtered[1]
	default:
		return filtered[0], filtered[1], filtered[2]
	}
}

func modelNameTaken(models []domain.Model, qualifiedName string) bool {
	for _, item := range models {
		if item.QualifiedName() == qualifiedName {
			return true
		}
	}
	return false
}

func seedToModel(seed domain.Seed) (domain.Model, error) {
	sqlText, err := buildSeedSQL(seed)
	if err != nil {
		return domain.Model{}, err
	}
	return domain.Model{
		ID:              "seed:" + seed.ID,
		ProjectName:     seed.ProjectName,
		Name:            seed.Name,
		SQL:             sqlText,
		Materialization: domain.MaterializationSeed,
		Description:     seed.Description,
		Tags:            append([]string(nil), seed.Tags...),
		Config: domain.ModelConfig{
			Materialized: "seed",
		},
		CreatedBy: seed.CreatedBy,
		CreatedAt: seed.CreatedAt,
		UpdatedAt: seed.UpdatedAt,
	}, nil
}

func buildSeedSQL(seed domain.Seed) (string, error) {
	path := strings.TrimSpace(seed.InputRef)
	if path == "" {
		return "", domain.ErrValidation("seed %s has no input_ref", seed.QualifiedName())
	}
	escapedPath := "'" + strings.ReplaceAll(path, "'", "''") + "'"
	switch domain.NormalizeSeedFormat(seed.Format) {
	case "parquet":
		return "SELECT * FROM read_parquet(" + escapedPath + ")", nil
	case "json":
		return "SELECT * FROM read_json_auto(" + escapedPath + ")", nil
	default:
		header := "true"
		if !seed.HasHeader {
			header = "false"
		}
		delimiter := strings.TrimSpace(seed.Delimiter)
		if delimiter == "" {
			delimiter = ","
		}
		args := []string{
			escapedPath,
			"HEADER=" + header,
			"delim='" + strings.ReplaceAll(delimiter, "'", "''") + "'",
		}
		if len(seed.ColumnTypes) > 0 {
			keys := make([]string, 0, len(seed.ColumnTypes))
			for key := range seed.ColumnTypes {
				keys = append(keys, key)
			}
			sort.Strings(keys)
			typePairs := make([]string, 0, len(keys))
			for _, key := range keys {
				typePairs = append(typePairs, "'"+strings.ReplaceAll(key, "'", "''")+"': '"+strings.ReplaceAll(seed.ColumnTypes[key], "'", "''")+"'")
			}
			args = append(args, "types={"+strings.Join(typePairs, ", ")+"}")
		}
		return "SELECT * FROM read_csv_auto(" + strings.Join(args, ", ") + ")", nil
	}
}
