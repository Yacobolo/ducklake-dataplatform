package declarative

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"gopkg.in/yaml.v3"
)

// LoadDirectory reads all YAML files from the given directory and returns
// the desired state. It infers resource context (catalog, schema, table)
// from the directory structure.
func LoadDirectory(dir string) (*DesiredState, error) {
	state := &DesiredState{}

	// Check root dir exists.
	info, err := os.Stat(dir)
	if err != nil {
		return nil, fmt.Errorf("config directory: %w", err)
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("config directory: %s is not a directory", dir)
	}

	// Load each section. Missing directories are OK (partial configs).

	// 1. security/
	if err := loadSecurity(dir, state); err != nil {
		return nil, err
	}

	// 2. governance/
	if err := loadGovernance(dir, state); err != nil {
		return nil, err
	}

	// 3. storage/
	if err := loadStorage(dir, state); err != nil {
		return nil, err
	}

	// 4. compute/
	if err := loadCompute(dir, state); err != nil {
		return nil, err
	}

	// 5. catalogs/ (hierarchical walk)
	if err := loadCatalogs(dir, state); err != nil {
		return nil, err
	}

	// 6. notebooks/
	if err := loadNotebooks(dir, state); err != nil {
		return nil, err
	}

	// 7. pipelines/
	if err := loadPipelines(dir, state); err != nil {
		return nil, err
	}

	// 8. models/
	if err := loadModels(dir, state); err != nil {
		return nil, err
	}

	// 9. macros/
	if err := loadMacros(dir, state); err != nil {
		return nil, err
	}

	return state, nil
}

// loadYAMLFile reads and unmarshals a YAML file into the given target.
// Returns (false, nil) if file doesn't exist (optional files).
// Returns (false, err) on read/parse errors.
// Returns (true, nil) on success.
func loadYAMLFile(path string, target interface{}) (bool, error) {
	data, err := os.ReadFile(path) //nolint:gosec // intentional: reading user-specified config files
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, fmt.Errorf("read %s: %w", path, err)
	}
	if err := yaml.Unmarshal(data, target); err != nil {
		return false, fmt.Errorf("parse %s: %w", path, err)
	}
	return true, nil
}

// validateDocument checks the apiVersion and kind fields.
func validateDocument(path string, apiVersion, kind, expectedKind string) error {
	if apiVersion != SupportedAPIVersion {
		return fmt.Errorf("%s: unsupported apiVersion %q (expected %q)", path, apiVersion, SupportedAPIVersion)
	}
	if kind != expectedKind {
		return fmt.Errorf("%s: unexpected kind %q (expected %q)", path, kind, expectedKind)
	}
	return nil
}

// dirExists returns true if path exists and is a directory.
func dirExists(path string) bool {
	info, err := os.Stat(path)
	return err == nil && info.IsDir()
}

// loadSecurity reads security/principals.yaml, security/groups.yaml,
// security/grants.yaml, and security/api-keys.yaml. All files are optional.
func loadSecurity(root string, state *DesiredState) error {
	secDir := filepath.Join(root, "security")
	if !dirExists(secDir) {
		return nil
	}

	// principals.yaml
	principalsPath := filepath.Join(secDir, "principals.yaml")
	var principalDoc PrincipalListDoc
	if found, err := loadYAMLFile(principalsPath, &principalDoc); err != nil {
		return err
	} else if found {
		if err := validateDocument(principalsPath, principalDoc.APIVersion, principalDoc.Kind, KindNamePrincipalList); err != nil {
			return err
		}
		state.Principals = principalDoc.Principals
	}

	// groups.yaml
	groupsPath := filepath.Join(secDir, "groups.yaml")
	var groupDoc GroupListDoc
	if found, err := loadYAMLFile(groupsPath, &groupDoc); err != nil {
		return err
	} else if found {
		if err := validateDocument(groupsPath, groupDoc.APIVersion, groupDoc.Kind, KindNameGroupList); err != nil {
			return err
		}
		state.Groups = groupDoc.Groups
	}

	// grants.yaml
	grantsPath := filepath.Join(secDir, "grants.yaml")
	var grantDoc GrantListDoc
	if found, err := loadYAMLFile(grantsPath, &grantDoc); err != nil {
		return err
	} else if found {
		if err := validateDocument(grantsPath, grantDoc.APIVersion, grantDoc.Kind, KindNameGrantList); err != nil {
			return err
		}
		state.Grants = grantDoc.Grants
	}

	// privilege-presets.yaml
	presetsPath := filepath.Join(secDir, "privilege-presets.yaml")
	var presetDoc PrivilegePresetListDoc
	if found, err := loadYAMLFile(presetsPath, &presetDoc); err != nil {
		return err
	} else if found {
		if err := validateDocument(presetsPath, presetDoc.APIVersion, presetDoc.Kind, KindNamePrivilegePresetList); err != nil {
			return err
		}
		state.PrivilegePresets = presetDoc.Presets
	}

	// bindings.yaml
	bindingsPath := filepath.Join(secDir, "bindings.yaml")
	var bindingDoc BindingListDoc
	if found, err := loadYAMLFile(bindingsPath, &bindingDoc); err != nil {
		return err
	} else if found {
		if err := validateDocument(bindingsPath, bindingDoc.APIVersion, bindingDoc.Kind, KindNameBindingList); err != nil {
			return err
		}
		state.Bindings = bindingDoc.Bindings
	}

	// api-keys.yaml (optional)
	apiKeysPath := filepath.Join(secDir, "api-keys.yaml")
	var apiKeyDoc APIKeyListDoc
	if found, err := loadYAMLFile(apiKeysPath, &apiKeyDoc); err != nil {
		return err
	} else if found {
		if err := validateDocument(apiKeysPath, apiKeyDoc.APIVersion, apiKeyDoc.Kind, KindNameAPIKeyList); err != nil {
			return err
		}
		state.APIKeys = apiKeyDoc.APIKeys
	}

	return nil
}

// loadGovernance reads governance/tags.yaml.
func loadGovernance(root string, state *DesiredState) error {
	govDir := filepath.Join(root, "governance")
	if !dirExists(govDir) {
		return nil
	}

	tagsPath := filepath.Join(govDir, "tags.yaml")
	var tagDoc TagConfigDoc
	if found, err := loadYAMLFile(tagsPath, &tagDoc); err != nil {
		return err
	} else if found {
		if err := validateDocument(tagsPath, tagDoc.APIVersion, tagDoc.Kind, KindNameTagConfig); err != nil {
			return err
		}
		state.Tags = tagDoc.Tags
		state.TagAssignments = tagDoc.Assignments
	}

	return nil
}

// loadStorage reads storage/credentials.yaml and storage/locations.yaml.
func loadStorage(root string, state *DesiredState) error {
	storDir := filepath.Join(root, "storage")
	if !dirExists(storDir) {
		return nil
	}

	// credentials.yaml
	credsPath := filepath.Join(storDir, "credentials.yaml")
	var credDoc StorageCredentialListDoc
	if found, err := loadYAMLFile(credsPath, &credDoc); err != nil {
		return err
	} else if found {
		if err := validateDocument(credsPath, credDoc.APIVersion, credDoc.Kind, KindNameStorageCredentialList); err != nil {
			return err
		}
		state.StorageCredentials = credDoc.Credentials
	}

	// locations.yaml
	locsPath := filepath.Join(storDir, "locations.yaml")
	var locDoc ExternalLocationListDoc
	if found, err := loadYAMLFile(locsPath, &locDoc); err != nil {
		return err
	} else if found {
		if err := validateDocument(locsPath, locDoc.APIVersion, locDoc.Kind, KindNameExternalLocationList); err != nil {
			return err
		}
		state.ExternalLocations = locDoc.Locations
	}

	return nil
}

// loadCompute reads compute/endpoints.yaml and compute/assignments.yaml.
func loadCompute(root string, state *DesiredState) error {
	compDir := filepath.Join(root, "compute")
	if !dirExists(compDir) {
		return nil
	}

	// endpoints.yaml
	endpointsPath := filepath.Join(compDir, "endpoints.yaml")
	var epDoc ComputeEndpointListDoc
	if found, err := loadYAMLFile(endpointsPath, &epDoc); err != nil {
		return err
	} else if found {
		if err := validateDocument(endpointsPath, epDoc.APIVersion, epDoc.Kind, KindNameComputeEndpointList); err != nil {
			return err
		}
		state.ComputeEndpoints = epDoc.Endpoints
	}

	// assignments.yaml
	assignPath := filepath.Join(compDir, "assignments.yaml")
	var assignDoc ComputeAssignmentListDoc
	if found, err := loadYAMLFile(assignPath, &assignDoc); err != nil {
		return err
	} else if found {
		if err := validateDocument(assignPath, assignDoc.APIVersion, assignDoc.Kind, KindNameComputeAssignmentList); err != nil {
			return err
		}
		state.ComputeAssignments = assignDoc.Assignments
	}

	return nil
}

// loadCatalogs walks the catalogs/ directory tree, loading catalogs, schemas,
// tables, views, volumes, row filters, and column masks.
func loadCatalogs(root string, state *DesiredState) error {
	catDir := filepath.Join(root, "catalogs")
	if !dirExists(catDir) {
		return nil
	}

	entries, err := os.ReadDir(catDir)
	if err != nil {
		return fmt.Errorf("read catalogs directory: %w", err)
	}

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		catalogName := entry.Name()
		catalogPath := filepath.Join(catDir, catalogName)

		if err := loadOneCatalog(catalogPath, catalogName, state); err != nil {
			return err
		}
	}

	return nil
}

// loadOneCatalog loads a single catalog directory: catalog.yaml and schemas/.
func loadOneCatalog(catalogPath, catalogName string, state *DesiredState) error {
	// catalog.yaml
	catFile := filepath.Join(catalogPath, "catalog.yaml")
	var catDoc CatalogDoc
	found, err := loadYAMLFile(catFile, &catDoc)
	if err != nil {
		return err
	}
	if found {
		if err := validateDocument(catFile, catDoc.APIVersion, catDoc.Kind, KindNameCatalog); err != nil {
			return err
		}
		if catDoc.Metadata.Name != catalogName {
			return fmt.Errorf("%s: metadata.name %q does not match directory name %q", catFile, catDoc.Metadata.Name, catalogName)
		}
		state.Catalogs = append(state.Catalogs, CatalogResource{
			CatalogName:        catalogName,
			DeletionProtection: catDoc.Metadata.DeletionProtection,
			Spec:               catDoc.Spec,
		})
	}

	// schemas/
	schemasDir := filepath.Join(catalogPath, "schemas")
	if !dirExists(schemasDir) {
		return nil
	}

	schemaEntries, err := os.ReadDir(schemasDir)
	if err != nil {
		return fmt.Errorf("read schemas directory %s: %w", schemasDir, err)
	}

	for _, se := range schemaEntries {
		if !se.IsDir() {
			continue
		}
		schemaName := se.Name()
		schemaPath := filepath.Join(schemasDir, schemaName)

		if err := loadOneSchema(schemaPath, catalogName, schemaName, state); err != nil {
			return err
		}
	}

	return nil
}

// loadOneSchema loads a single schema directory: schema.yaml, tables/, views/, volumes/.
func loadOneSchema(schemaPath, catalogName, schemaName string, state *DesiredState) error {
	// schema.yaml
	schemaFile := filepath.Join(schemaPath, "schema.yaml")
	var schemaDoc SchemaDoc
	found, err := loadYAMLFile(schemaFile, &schemaDoc)
	if err != nil {
		return err
	}
	if found {
		if err := validateDocument(schemaFile, schemaDoc.APIVersion, schemaDoc.Kind, KindNameSchema); err != nil {
			return err
		}
		if schemaDoc.Metadata.Name != schemaName {
			return fmt.Errorf("%s: metadata.name %q does not match directory name %q", schemaFile, schemaDoc.Metadata.Name, schemaName)
		}
		state.Schemas = append(state.Schemas, SchemaResource{
			CatalogName:        catalogName,
			SchemaName:         schemaName,
			DeletionProtection: schemaDoc.Metadata.DeletionProtection,
			Spec:               schemaDoc.Spec,
		})
	}

	// tables/
	if err := loadTables(schemaPath, catalogName, schemaName, state); err != nil {
		return err
	}

	// views/
	if err := loadViews(schemaPath, catalogName, schemaName, state); err != nil {
		return err
	}

	// volumes/
	if err := loadVolumes(schemaPath, catalogName, schemaName, state); err != nil {
		return err
	}

	return nil
}

// loadTables walks tables/ within a schema directory.
func loadTables(schemaPath, catalogName, schemaName string, state *DesiredState) error {
	tablesDir := filepath.Join(schemaPath, "tables")
	if !dirExists(tablesDir) {
		return nil
	}

	entries, err := os.ReadDir(tablesDir)
	if err != nil {
		return fmt.Errorf("read tables directory %s: %w", tablesDir, err)
	}

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		tableName := entry.Name()
		tablePath := filepath.Join(tablesDir, tableName)

		if err := loadOneTable(tablePath, catalogName, schemaName, tableName, state); err != nil {
			return err
		}
	}

	return nil
}

// loadOneTable loads a single table directory: table.yaml, row-filters.yaml, column-masks.yaml.
func loadOneTable(tablePath, catalogName, schemaName, tableName string, state *DesiredState) error {
	// table.yaml
	tableFile := filepath.Join(tablePath, "table.yaml")
	var tableDoc TableDoc
	found, err := loadYAMLFile(tableFile, &tableDoc)
	if err != nil {
		return err
	}
	if found {
		if err := validateDocument(tableFile, tableDoc.APIVersion, tableDoc.Kind, KindNameTable); err != nil {
			return err
		}
		if tableDoc.Metadata.Name != tableName {
			return fmt.Errorf("%s: metadata.name %q does not match directory name %q", tableFile, tableDoc.Metadata.Name, tableName)
		}
		state.Tables = append(state.Tables, TableResource{
			CatalogName:        catalogName,
			SchemaName:         schemaName,
			TableName:          tableName,
			DeletionProtection: tableDoc.Metadata.DeletionProtection,
			Spec:               tableDoc.Spec,
		})
	}

	// row-filters.yaml (optional)
	rfFile := filepath.Join(tablePath, "row-filters.yaml")
	var rfDoc RowFilterListDoc
	if rfFound, rfErr := loadYAMLFile(rfFile, &rfDoc); rfErr != nil {
		return rfErr
	} else if rfFound {
		if err := validateDocument(rfFile, rfDoc.APIVersion, rfDoc.Kind, KindNameRowFilterList); err != nil {
			return err
		}
		state.RowFilters = append(state.RowFilters, RowFilterResource{
			CatalogName: catalogName,
			SchemaName:  schemaName,
			TableName:   tableName,
			Filters:     rfDoc.Filters,
		})
	}

	// column-masks.yaml (optional)
	cmFile := filepath.Join(tablePath, "column-masks.yaml")
	var cmDoc ColumnMaskListDoc
	if cmFound, cmErr := loadYAMLFile(cmFile, &cmDoc); cmErr != nil {
		return cmErr
	} else if cmFound {
		if err := validateDocument(cmFile, cmDoc.APIVersion, cmDoc.Kind, KindNameColumnMaskList); err != nil {
			return err
		}
		state.ColumnMasks = append(state.ColumnMasks, ColumnMaskResource{
			CatalogName: catalogName,
			SchemaName:  schemaName,
			TableName:   tableName,
			Masks:       cmDoc.Masks,
		})
	}

	return nil
}

// loadViews walks views/ within a schema directory. Each .yaml file is a view.
func loadViews(schemaPath, catalogName, schemaName string, state *DesiredState) error {
	viewsDir := filepath.Join(schemaPath, "views")
	if !dirExists(viewsDir) {
		return nil
	}

	entries, err := os.ReadDir(viewsDir)
	if err != nil {
		return fmt.Errorf("read views directory %s: %w", viewsDir, err)
	}

	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".yaml") {
			continue
		}

		viewName := strings.TrimSuffix(entry.Name(), ".yaml")
		viewFile := filepath.Join(viewsDir, entry.Name())

		var viewDoc ViewDoc
		found, err := loadYAMLFile(viewFile, &viewDoc)
		if err != nil {
			return err
		}
		if !found {
			continue
		}

		if err := validateDocument(viewFile, viewDoc.APIVersion, viewDoc.Kind, KindNameView); err != nil {
			return err
		}
		if viewDoc.Metadata.Name != viewName {
			return fmt.Errorf("%s: metadata.name %q does not match file name %q", viewFile, viewDoc.Metadata.Name, viewName)
		}
		state.Views = append(state.Views, ViewResource{
			CatalogName: catalogName,
			SchemaName:  schemaName,
			ViewName:    viewName,
			Spec:        viewDoc.Spec,
		})
	}

	return nil
}

// loadVolumes walks volumes/ within a schema directory. Each .yaml file is a volume.
func loadVolumes(schemaPath, catalogName, schemaName string, state *DesiredState) error {
	volumesDir := filepath.Join(schemaPath, "volumes")
	if !dirExists(volumesDir) {
		return nil
	}

	entries, err := os.ReadDir(volumesDir)
	if err != nil {
		return fmt.Errorf("read volumes directory %s: %w", volumesDir, err)
	}

	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".yaml") {
			continue
		}

		volumeName := strings.TrimSuffix(entry.Name(), ".yaml")
		volumeFile := filepath.Join(volumesDir, entry.Name())

		var volDoc VolumeDoc
		found, err := loadYAMLFile(volumeFile, &volDoc)
		if err != nil {
			return err
		}
		if !found {
			continue
		}

		if err := validateDocument(volumeFile, volDoc.APIVersion, volDoc.Kind, KindNameVolume); err != nil {
			return err
		}
		if volDoc.Metadata.Name != volumeName {
			return fmt.Errorf("%s: metadata.name %q does not match file name %q", volumeFile, volDoc.Metadata.Name, volumeName)
		}
		state.Volumes = append(state.Volumes, VolumeResource{
			CatalogName: catalogName,
			SchemaName:  schemaName,
			VolumeName:  volumeName,
			Spec:        volDoc.Spec,
		})
	}

	return nil
}

// loadNotebooks walks the notebooks/ directory. Each .yaml file is a notebook.
func loadNotebooks(root string, state *DesiredState) error {
	nbDir := filepath.Join(root, "notebooks")
	if !dirExists(nbDir) {
		return nil
	}

	entries, err := os.ReadDir(nbDir)
	if err != nil {
		return fmt.Errorf("read notebooks directory: %w", err)
	}

	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".yaml") {
			continue
		}

		nbName := strings.TrimSuffix(entry.Name(), ".yaml")
		nbFile := filepath.Join(nbDir, entry.Name())

		var nbDoc NotebookDoc
		found, err := loadYAMLFile(nbFile, &nbDoc)
		if err != nil {
			return err
		}
		if !found {
			continue
		}

		if err := validateDocument(nbFile, nbDoc.APIVersion, nbDoc.Kind, KindNameNotebook); err != nil {
			return err
		}
		if nbDoc.Metadata.Name != nbName {
			return fmt.Errorf("%s: metadata.name %q does not match file name %q", nbFile, nbDoc.Metadata.Name, nbName)
		}
		state.Notebooks = append(state.Notebooks, NotebookResource{
			Name: nbName,
			Spec: nbDoc.Spec,
		})
	}

	return nil
}

// loadPipelines walks the pipelines/ directory. Each .yaml file is a pipeline.
func loadPipelines(root string, state *DesiredState) error {
	plDir := filepath.Join(root, "pipelines")
	if !dirExists(plDir) {
		return nil
	}

	entries, err := os.ReadDir(plDir)
	if err != nil {
		return fmt.Errorf("read pipelines directory: %w", err)
	}

	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".yaml") {
			continue
		}

		plName := strings.TrimSuffix(entry.Name(), ".yaml")
		plFile := filepath.Join(plDir, entry.Name())

		var plDoc PipelineDoc
		found, err := loadYAMLFile(plFile, &plDoc)
		if err != nil {
			return err
		}
		if !found {
			continue
		}

		if err := validateDocument(plFile, plDoc.APIVersion, plDoc.Kind, KindNamePipeline); err != nil {
			return err
		}
		if plDoc.Metadata.Name != plName {
			return fmt.Errorf("%s: metadata.name %q does not match file name %q", plFile, plDoc.Metadata.Name, plName)
		}
		state.Pipelines = append(state.Pipelines, PipelineResource{
			Name: plName,
			Spec: plDoc.Spec,
		})
	}

	return nil
}

// loadModels walks the models/<project>/**/*.yaml directory tree recursively.
// The first-level directory is the project name; model name is from the filename.
// Subdirectories within a project are organizational only.
func loadModels(root string, state *DesiredState) error {
	modelsDir := filepath.Join(root, "models")
	if !dirExists(modelsDir) {
		return nil
	}

	// Each top-level entry under models/ is a project directory.
	projectEntries, err := os.ReadDir(modelsDir)
	if err != nil {
		return fmt.Errorf("read models directory: %w", err)
	}

	for _, projEntry := range projectEntries {
		if !projEntry.IsDir() {
			continue
		}
		projectName := projEntry.Name()
		projectPath := filepath.Join(modelsDir, projectName)

		if err := loadModelsRecursive(projectPath, projectName, state); err != nil {
			return err
		}
	}

	return nil
}

// loadMacros walks the macros/ directory. Each .yaml file is a macro.
func loadMacros(root string, state *DesiredState) error {
	macroDir := filepath.Join(root, "macros")
	if !dirExists(macroDir) {
		return nil
	}

	entries, err := os.ReadDir(macroDir)
	if err != nil {
		return fmt.Errorf("read macros directory: %w", err)
	}

	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".yaml") {
			continue
		}

		macroName := strings.TrimSuffix(entry.Name(), ".yaml")
		macroFile := filepath.Join(macroDir, entry.Name())

		var macroDoc MacroDoc
		found, err := loadYAMLFile(macroFile, &macroDoc)
		if err != nil {
			return err
		}
		if !found {
			continue
		}

		if err := validateDocument(macroFile, macroDoc.APIVersion, macroDoc.Kind, KindNameMacro); err != nil {
			return err
		}
		if macroDoc.Metadata.Name != macroName {
			return fmt.Errorf("%s: metadata.name %q does not match file name %q", macroFile, macroDoc.Metadata.Name, macroName)
		}
		state.Macros = append(state.Macros, MacroResource{
			Name: macroName,
			Spec: macroDoc.Spec,
		})
	}

	return nil
}

// loadModelsRecursive walks a directory tree under a project, loading all .yaml files as models.
func loadModelsRecursive(dir, projectName string, state *DesiredState) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return fmt.Errorf("read models directory %s: %w", dir, err)
	}

	for _, entry := range entries {
		if entry.IsDir() {
			// Recurse into subdirectories (organizational only).
			if err := loadModelsRecursive(filepath.Join(dir, entry.Name()), projectName, state); err != nil {
				return err
			}
			continue
		}

		if !strings.HasSuffix(entry.Name(), ".yaml") {
			continue
		}

		modelName := strings.TrimSuffix(entry.Name(), ".yaml")
		modelFile := filepath.Join(dir, entry.Name())

		var modelDoc ModelDoc
		found, err := loadYAMLFile(modelFile, &modelDoc)
		if err != nil {
			return err
		}
		if !found {
			continue
		}

		if err := validateDocument(modelFile, modelDoc.APIVersion, modelDoc.Kind, KindNameModel); err != nil {
			return err
		}
		if modelDoc.Metadata.Name != modelName {
			return fmt.Errorf("%s: metadata.name %q does not match file name %q", modelFile, modelDoc.Metadata.Name, modelName)
		}
		state.Models = append(state.Models, ModelResource{
			ProjectName: projectName,
			ModelName:   modelName,
			Spec:        modelDoc.Spec,
		})
	}

	return nil
}
