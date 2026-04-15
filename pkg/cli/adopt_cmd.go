package cli

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/cobra"

	"github.com/Yacobolo/quackstack/internal/declarative"
	"github.com/Yacobolo/quackstack/pkg/cli/apiruntime"
)

type adoptedCatalogFragment struct {
	DeletionProtection      bool `yaml:"deletion_protection,omitempty"`
	declarative.CatalogSpec `yaml:",inline"`
	Schemas                 map[string]adoptedSchemaFragment `yaml:"schemas,omitempty"`
}

type adoptedSchemaFragment struct {
	DeletionProtection     bool `yaml:"deletion_protection,omitempty"`
	declarative.SchemaSpec `yaml:",inline"`
	Tables                 map[string]adoptedTableFragment   `yaml:"tables,omitempty"`
	Views                  map[string]declarative.ViewSpec   `yaml:"views,omitempty"`
	Volumes                map[string]declarative.VolumeSpec `yaml:"volumes,omitempty"`
}

type adoptedTableFragment struct {
	DeletionProtection    bool `yaml:"deletion_protection,omitempty"`
	declarative.TableSpec `yaml:",inline"`
	RowFilters            []declarative.RowFilterSpec  `yaml:"row_filters,omitempty"`
	ColumnMasks           []declarative.ColumnMaskSpec `yaml:"column_masks,omitempty"`
}

type adoptedProjectFragment struct {
	declarative.ProjectSpec `yaml:",inline"`
	Environments            map[string]declarative.EnvironmentSpec `yaml:"environments,omitempty"`
	Macros                  map[string]declarative.MacroSpec       `yaml:"macros,omitempty"`
	Models                  map[string]declarative.ModelSpec       `yaml:"models,omitempty"`
}

func newAdoptCmd(client *apiruntime.Client) *cobra.Command {
	var (
		configDir string
		overwrite bool
	)

	cmd := &cobra.Command{
		Use:   "adopt <resource-type> <ref>",
		Short: "Adopt an existing server resource into declarative config",
		Long:  "Fetches an existing server resource and writes a focused CUE fragment so it can be managed declaratively.",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			resourceType := strings.ToLower(strings.TrimSpace(args[0]))
			resourceRef := strings.TrimSpace(args[1])

			stateClient := NewAPIStateClient(client)
			state, err := stateClient.ReadState(cmd.Context())
			if err != nil {
				return fmt.Errorf("read server state: %w", err)
			}

			relativePath, fragment, err := buildAdoptFragment(state, resourceType, resourceRef)
			if err != nil {
				return err
			}

			if err := declarative.EnsureModuleRoot(configDir); err != nil {
				return fmt.Errorf("ensure config module: %w", err)
			}

			outputPath := filepath.Join(configDir, relativePath)
			if _, err := os.Stat(outputPath); err == nil && !overwrite {
				return fmt.Errorf("fragment %s already exists; use --overwrite to replace it", outputPath)
			} else if err != nil && !os.IsNotExist(err) {
				return fmt.Errorf("stat fragment %s: %w", outputPath, err)
			}

			if err := declarative.WriteFragmentFile(outputPath, fragment); err != nil {
				return fmt.Errorf("write fragment: %w", err)
			}

			payload := map[string]string{
				"status":        "ok",
				"resource_type": resourceType,
				"resource_ref":  resourceRef,
				"path":          outputPath,
			}
			if getOutputFormat(cmd) == "json" {
				return apiruntime.PrintJSON(os.Stdout, payload)
			}
			_, _ = fmt.Fprintf(os.Stdout, "Adopted %s %q into %s\n", resourceType, resourceRef, outputPath)
			return nil
		},
	}

	cmd.Flags().StringVar(&configDir, "config-dir", "./quackstack-config", "Path to the CUE configuration module")
	cmd.Flags().BoolVar(&overwrite, "overwrite", false, "Overwrite an existing adopted fragment for the same resource")
	return cmd
}

func buildAdoptFragment(state *declarative.DesiredState, resourceType, resourceRef string) (string, any, error) {
	switch resourceType {
	case "catalog":
		catalog, ok := findCatalog(state, resourceRef)
		if !ok {
			return "", nil, fmt.Errorf("catalog %q not found", resourceRef)
		}
		return filepath.Join("catalogs", safeAdoptSlug(catalog.CatalogName), "adopted.cue"), map[string]any{
			"catalogs": map[string]adoptedCatalogFragment{
				catalog.CatalogName: buildCatalogAdoption(state, catalog, ""),
			},
		}, nil
	case "schema":
		catalogName, schemaName, ok := strings.Cut(resourceRef, ".")
		if !ok {
			return "", nil, fmt.Errorf("schema ref %q must be catalog.schema", resourceRef)
		}
		catalog, found := findCatalog(state, catalogName)
		if !found {
			return "", nil, fmt.Errorf("catalog %q not found", catalogName)
		}
		if !schemaExists(state, catalogName, schemaName) {
			return "", nil, fmt.Errorf("schema %q not found", resourceRef)
		}
		return filepath.Join("catalogs", safeAdoptSlug(catalogName), "adopted-"+safeAdoptSlug(schemaName)+".cue"), map[string]any{
			"catalogs": map[string]adoptedCatalogFragment{
				catalogName: buildCatalogAdoption(state, catalog, schemaName),
			},
		}, nil
	case "project":
		project, workspace, ok := findProject(state, resourceRef)
		if !ok {
			return "", nil, fmt.Errorf("project %q not found", resourceRef)
		}
		return filepath.Join("projects", safeAdoptSlug(project.Name), "adopted.cue"), map[string]any{
			"workspaces": map[string]declarative.WorkspaceSpec{
				workspace.Name: workspace.Spec,
			},
			"projects": map[string]adoptedProjectFragment{
				project.Name: buildProjectAdoption(state, project, ""),
			},
		}, nil
	case "environment":
		environment, project, workspace, ok := findEnvironment(state, resourceRef)
		if !ok {
			return "", nil, fmt.Errorf("environment %q not found", resourceRef)
		}
		return filepath.Join("projects", safeAdoptSlug(project.Name), "environment-"+safeAdoptSlug(environment.Name)+".cue"), map[string]any{
			"workspaces": map[string]declarative.WorkspaceSpec{
				workspace.Name: workspace.Spec,
			},
			"projects": map[string]adoptedProjectFragment{
				project.Name: buildProjectAdoption(state, project, environment.Name),
			},
		}, nil
	case "storage-credential":
		credential, ok := findStorageCredential(state, resourceRef)
		if !ok {
			return "", nil, fmt.Errorf("storage credential %q not found", resourceRef)
		}
		return filepath.Join("storage", "adopted-credential-"+safeAdoptSlug(resourceRef)+".cue"), map[string]any{
			"storage": map[string]any{
				"credentials": map[string]declarative.StorageCredentialSpec{
					credential.Name: stripStorageCredentialName(credential),
				},
			},
		}, nil
	case "external-location":
		location, ok := findExternalLocation(state, resourceRef)
		if !ok {
			return "", nil, fmt.Errorf("external location %q not found", resourceRef)
		}
		return filepath.Join("storage", "adopted-location-"+safeAdoptSlug(resourceRef)+".cue"), map[string]any{
			"storage": map[string]any{
				"external_locations": map[string]declarative.ExternalLocationSpec{
					location.Name: stripExternalLocationName(location),
				},
			},
		}, nil
	default:
		return "", nil, fmt.Errorf("unsupported adopt resource type %q; supported: catalog, schema, project, environment, storage-credential, external-location", resourceType)
	}
}

func buildCatalogAdoption(state *declarative.DesiredState, catalog declarative.CatalogResource, schemaFilter string) adoptedCatalogFragment {
	fragment := adoptedCatalogFragment{
		DeletionProtection: catalog.DeletionProtection,
		CatalogSpec:        catalog.Spec,
		Schemas:            map[string]adoptedSchemaFragment{},
	}

	for _, schema := range state.Schemas {
		if schema.CatalogName != catalog.CatalogName {
			continue
		}
		if schemaFilter != "" && schema.SchemaName != schemaFilter {
			continue
		}

		schemaFragment := adoptedSchemaFragment{
			DeletionProtection: schema.DeletionProtection,
			SchemaSpec:         schema.Spec,
			Tables:             map[string]adoptedTableFragment{},
			Views:              map[string]declarative.ViewSpec{},
			Volumes:            map[string]declarative.VolumeSpec{},
		}

		for _, table := range state.Tables {
			if table.CatalogName != schema.CatalogName || table.SchemaName != schema.SchemaName {
				continue
			}
			schemaFragment.Tables[table.TableName] = adoptedTableFragment{
				DeletionProtection: table.DeletionProtection,
				TableSpec:          table.Spec,
				RowFilters:         rowFiltersForTable(state, table),
				ColumnMasks:        columnMasksForTable(state, table),
			}
		}
		for _, view := range state.Views {
			if view.CatalogName == schema.CatalogName && view.SchemaName == schema.SchemaName {
				schemaFragment.Views[view.ViewName] = view.Spec
			}
		}
		for _, volume := range state.Volumes {
			if volume.CatalogName == schema.CatalogName && volume.SchemaName == schema.SchemaName {
				schemaFragment.Volumes[volume.VolumeName] = volume.Spec
			}
		}

		fragment.Schemas[schema.SchemaName] = schemaFragment
	}

	return fragment
}

func buildProjectAdoption(state *declarative.DesiredState, project declarative.ProjectResource, environmentFilter string) adoptedProjectFragment {
	fragment := adoptedProjectFragment{
		ProjectSpec:  project.Spec,
		Environments: map[string]declarative.EnvironmentSpec{},
		Macros:       map[string]declarative.MacroSpec{},
		Models:       map[string]declarative.ModelSpec{},
	}

	projectRef := project.Spec.WorkspaceRef + "/" + project.Name
	for _, environment := range state.Environments {
		if environment.Spec.ProjectRef != projectRef {
			continue
		}
		if environmentFilter != "" && environment.Name != environmentFilter {
			continue
		}
		spec := environment.Spec
		spec.ProjectRef = ""
		fragment.Environments[environment.Name] = spec
	}

	if environmentFilter == "" {
		for _, macro := range state.Macros {
			if macro.Spec.ProjectName != project.Name {
				continue
			}
			spec := macro.Spec
			spec.ProjectName = ""
			fragment.Macros[macro.Name] = spec
		}
		for _, model := range state.Models {
			if model.ProjectName != project.Name {
				continue
			}
			fragment.Models[model.ModelName] = model.Spec
		}
	}

	return fragment
}

func findCatalog(state *declarative.DesiredState, name string) (declarative.CatalogResource, bool) {
	for _, catalog := range state.Catalogs {
		if catalog.CatalogName == name {
			return catalog, true
		}
	}
	return declarative.CatalogResource{}, false
}

func schemaExists(state *declarative.DesiredState, catalogName, schemaName string) bool {
	for _, schema := range state.Schemas {
		if schema.CatalogName == catalogName && schema.SchemaName == schemaName {
			return true
		}
	}
	return false
}

func findProject(state *declarative.DesiredState, ref string) (declarative.ProjectResource, declarative.WorkspaceResource, bool) {
	ref = strings.TrimSpace(ref)
	matches := make([]declarative.ProjectResource, 0, 1)
	for _, project := range state.Projects {
		projectRef := project.Spec.WorkspaceRef + "/" + project.Name
		if projectRef == ref || project.Name == ref {
			matches = append(matches, project)
		}
	}
	if len(matches) != 1 {
		return declarative.ProjectResource{}, declarative.WorkspaceResource{}, false
	}
	workspace, ok := findWorkspace(state, matches[0].Spec.WorkspaceRef)
	if !ok {
		return declarative.ProjectResource{}, declarative.WorkspaceResource{}, false
	}
	return matches[0], workspace, true
}

func findEnvironment(state *declarative.DesiredState, ref string) (declarative.EnvironmentResource, declarative.ProjectResource, declarative.WorkspaceResource, bool) {
	ref = strings.TrimSpace(ref)
	type match struct {
		environment declarative.EnvironmentResource
		project     declarative.ProjectResource
		workspace   declarative.WorkspaceResource
	}

	matches := make([]match, 0, 1)
	for _, environment := range state.Environments {
		workspaceName, projectName, ok := parseProjectReference(environment.Spec.ProjectRef)
		if !ok {
			continue
		}
		environmentRef := workspaceName + "/" + projectName + "/" + environment.Name
		projectEnvRef := projectName + "/" + environment.Name
		if environmentRef != ref && projectEnvRef != ref && environment.Name != ref {
			continue
		}

		project, workspace, found := resolveEnvironmentParents(state, workspaceName, projectName)
		if !found {
			continue
		}
		matches = append(matches, match{environment: environment, project: project, workspace: workspace})
	}
	if len(matches) != 1 {
		return declarative.EnvironmentResource{}, declarative.ProjectResource{}, declarative.WorkspaceResource{}, false
	}
	return matches[0].environment, matches[0].project, matches[0].workspace, true
}

func resolveEnvironmentParents(state *declarative.DesiredState, workspaceName, projectName string) (declarative.ProjectResource, declarative.WorkspaceResource, bool) {
	workspace, ok := findWorkspace(state, workspaceName)
	if !ok {
		return declarative.ProjectResource{}, declarative.WorkspaceResource{}, false
	}
	for _, project := range state.Projects {
		if project.Name == projectName && project.Spec.WorkspaceRef == workspaceName {
			return project, workspace, true
		}
	}
	return declarative.ProjectResource{}, declarative.WorkspaceResource{}, false
}

func parseProjectReference(ref string) (string, string, bool) {
	parts := strings.Split(strings.TrimSpace(ref), "/")
	if len(parts) != 2 {
		return "", "", false
	}
	if strings.TrimSpace(parts[0]) == "" || strings.TrimSpace(parts[1]) == "" {
		return "", "", false
	}
	return strings.TrimSpace(parts[0]), strings.TrimSpace(parts[1]), true
}

func findWorkspace(state *declarative.DesiredState, name string) (declarative.WorkspaceResource, bool) {
	for _, workspace := range state.Workspaces {
		if workspace.Name == name {
			return workspace, true
		}
	}
	return declarative.WorkspaceResource{}, false
}

func findStorageCredential(state *declarative.DesiredState, name string) (declarative.StorageCredentialSpec, bool) {
	for _, credential := range state.StorageCredentials {
		if credential.Name == name {
			return credential, true
		}
	}
	return declarative.StorageCredentialSpec{}, false
}

func findExternalLocation(state *declarative.DesiredState, name string) (declarative.ExternalLocationSpec, bool) {
	for _, location := range state.ExternalLocations {
		if location.Name == name {
			return location, true
		}
	}
	return declarative.ExternalLocationSpec{}, false
}

func rowFiltersForTable(state *declarative.DesiredState, table declarative.TableResource) []declarative.RowFilterSpec {
	filters := make([]declarative.RowFilterSpec, 0)
	for _, filter := range state.RowFilters {
		if filter.CatalogName == table.CatalogName && filter.SchemaName == table.SchemaName && filter.TableName == table.TableName {
			filters = append(filters, filter.Filters...)
		}
	}
	return filters
}

func columnMasksForTable(state *declarative.DesiredState, table declarative.TableResource) []declarative.ColumnMaskSpec {
	masks := make([]declarative.ColumnMaskSpec, 0)
	for _, mask := range state.ColumnMasks {
		if mask.CatalogName == table.CatalogName && mask.SchemaName == table.SchemaName && mask.TableName == table.TableName {
			masks = append(masks, mask.Masks...)
		}
	}
	return masks
}

func stripStorageCredentialName(spec declarative.StorageCredentialSpec) declarative.StorageCredentialSpec {
	spec.Name = ""
	return spec
}

func stripExternalLocationName(spec declarative.ExternalLocationSpec) declarative.ExternalLocationSpec {
	spec.Name = ""
	return spec
}

func safeAdoptSlug(value string) string {
	value = strings.TrimSpace(value)
	replacer := strings.NewReplacer("/", "-", ".", "-", " ", "-")
	value = replacer.Replace(value)
	if value == "" {
		return "resource"
	}
	return value
}
