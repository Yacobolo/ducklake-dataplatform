package declarative

import (
	"fmt"
	"reflect"
	"strings"
)

func resolveLoadOptions(state *DesiredState, opts LoadOptions) error {
	if state == nil {
		return nil
	}

	var (
		targetEnv *EnvironmentResource
		err       error
	)
	if strings.TrimSpace(opts.Target) != "" {
		targetEnv, err = resolveTargetEnvironment(state, opts.Target)
		if err != nil {
			return err
		}
	}

	mergedVars := make(map[string]string)
	if targetEnv != nil {
		for key, value := range targetEnv.Spec.Variables {
			mergedVars[key] = value
		}
		for key, value := range builtInTargetVars(*targetEnv) {
			mergedVars[key] = value
		}
	}
	for key, value := range opts.Vars {
		mergedVars[key] = value
	}
	if len(mergedVars) > 0 {
		mergedVars = resolveVariableMap(mergedVars)
		applyVariables(reflect.ValueOf(state), mergedVars)
	}

	if targetEnv != nil || len(mergedVars) > 0 {
		resolution := &LoadResolution{
			RequestedTarget: strings.TrimSpace(opts.Target),
			Variables:       cloneVars(mergedVars),
		}
		if targetEnv != nil {
			workspace, project, _ := parseEnvironmentResourceRef(*targetEnv)
			resolution.TargetName = targetEnv.Name
			resolution.TargetRef = environmentResourceRef(*targetEnv)
			resolution.Workspace = workspace
			resolution.Project = project
			resolution.Environment = targetEnv.Name
			resolution.Kind = targetEnv.Spec.Kind
			resolution.TargetCatalog = targetEnv.Spec.TargetCatalog
			resolution.TargetSchema = targetEnv.Spec.TargetSchema
			resolution.ComputeEndpoint = targetEnv.Spec.ComputeEndpoint
		}
		state.Resolution = resolution
	}

	return nil
}

func resolveTargetEnvironment(state *DesiredState, requested string) (*EnvironmentResource, error) {
	requested = strings.TrimSpace(requested)
	if requested == "" {
		return nil, nil
	}

	exactMatches := make([]EnvironmentResource, 0, 1)
	suffixMatches := make([]EnvironmentResource, 0, 1)
	nameMatches := make([]EnvironmentResource, 0, 1)
	for _, environment := range state.Environments {
		ref := environmentResourceRef(environment)
		if ref == requested {
			exactMatches = append(exactMatches, environment)
			continue
		}

		_, project, ok := parseEnvironmentResourceRef(environment)
		if ok && project+"/"+environment.Name == requested {
			suffixMatches = append(suffixMatches, environment)
		}
		if environment.Name == requested {
			nameMatches = append(nameMatches, environment)
		}
	}

	switch {
	case len(exactMatches) == 1:
		match := exactMatches[0]
		return &match, nil
	case len(exactMatches) > 1:
		return nil, fmt.Errorf("target %q matched multiple environments", requested)
	case len(suffixMatches) == 1:
		match := suffixMatches[0]
		return &match, nil
	case len(suffixMatches) > 1:
		return nil, fmt.Errorf("target %q is ambiguous; use workspace/project/environment", requested)
	case len(nameMatches) == 1:
		match := nameMatches[0]
		return &match, nil
	case len(nameMatches) > 1:
		return nil, fmt.Errorf("target %q is ambiguous; use workspace/project/environment", requested)
	default:
		return nil, fmt.Errorf("target %q not found", requested)
	}
}

func environmentResourceRef(environment EnvironmentResource) string {
	workspace, project, ok := parseEnvironmentResourceRef(environment)
	if !ok {
		return ""
	}
	return workspace + "/" + project + "/" + environment.Name
}

func parseEnvironmentResourceRef(environment EnvironmentResource) (string, string, bool) {
	workspace, project, ok := parseProjectRef(environment.Spec.ProjectRef)
	if !ok {
		return "", "", false
	}
	return workspace, project, true
}

func builtInTargetVars(environment EnvironmentResource) map[string]string {
	workspace, project, _ := parseEnvironmentResourceRef(environment)
	ref := environmentResourceRef(environment)
	return map[string]string{
		"target":                  environment.Name,
		"target_name":             environment.Name,
		"target_ref":              ref,
		"target_workspace":        workspace,
		"target_project":          project,
		"target_environment":      environment.Name,
		"target_environment_kind": environment.Spec.Kind,
		"target_catalog":          environment.Spec.TargetCatalog,
		"target_schema":           environment.Spec.TargetSchema,
		"target_compute_endpoint": environment.Spec.ComputeEndpoint,
	}
}

func resolveVariableMap(vars map[string]string) map[string]string {
	resolved := cloneVars(vars)
	for range len(resolved) + 1 {
		changed := false
		for key, value := range resolved {
			next := interpolateString(value, resolved)
			if next != value {
				resolved[key] = next
				changed = true
			}
		}
		if !changed {
			break
		}
	}
	return resolved
}

func cloneVars(vars map[string]string) map[string]string {
	if len(vars) == 0 {
		return nil
	}
	cloned := make(map[string]string, len(vars))
	for key, value := range vars {
		cloned[key] = value
	}
	return cloned
}

func applyVariables(value reflect.Value, vars map[string]string) {
	if !value.IsValid() || len(vars) == 0 {
		return
	}

	switch value.Kind() {
	case reflect.Pointer:
		if !value.IsNil() {
			applyVariables(value.Elem(), vars)
		}
	case reflect.Interface:
		if !value.IsNil() {
			updated := reflect.New(value.Elem().Type()).Elem()
			updated.Set(value.Elem())
			applyVariables(updated, vars)
			value.Set(updated)
		}
	case reflect.Struct:
		for i := 0; i < value.NumField(); i++ {
			field := value.Field(i)
			if !field.CanSet() && field.Kind() != reflect.Pointer && field.Kind() != reflect.Struct && field.Kind() != reflect.Map && field.Kind() != reflect.Slice {
				continue
			}
			applyVariables(field, vars)
		}
	case reflect.Slice:
		for i := 0; i < value.Len(); i++ {
			applyVariables(value.Index(i), vars)
		}
	case reflect.Map:
		iter := value.MapRange()
		updates := make([]struct {
			key   reflect.Value
			value reflect.Value
		}, 0, value.Len())
		for iter.Next() {
			key := iter.Key()
			mapValue := iter.Value()
			updated := reflect.New(mapValue.Type()).Elem()
			updated.Set(mapValue)
			applyVariables(updated, vars)
			updates = append(updates, struct {
				key   reflect.Value
				value reflect.Value
			}{key: key, value: updated})
		}
		for _, update := range updates {
			value.SetMapIndex(update.key, update.value)
		}
	case reflect.String:
		if value.CanSet() {
			value.SetString(interpolateString(value.String(), vars))
		}
	}
}

func interpolateString(input string, vars map[string]string) string {
	if input == "" || len(vars) == 0 || !strings.Contains(input, "${") {
		return input
	}

	output := input
	for key, value := range vars {
		output = strings.ReplaceAll(output, "${"+key+"}", value)
	}
	return output
}
