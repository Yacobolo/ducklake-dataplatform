package declarative

import "reflect"

// SchemaDocumentType describes one declarative YAML document type used for
// generated JSON Schema artifacts.
type SchemaDocumentType struct {
	Kind     string
	FileName string
	Type     reflect.Type
}

// SchemaDocumentTypes returns all supported declarative document envelopes for
// schema generation.
func SchemaDocumentTypes() []SchemaDocumentType {
	return []SchemaDocumentType{
		{Kind: KindNamePrincipalList, FileName: "principal-list", Type: reflect.TypeOf(PrincipalListDoc{})},
		{Kind: KindNameGroupList, FileName: "group-list", Type: reflect.TypeOf(GroupListDoc{})},
		{Kind: KindNameGrantList, FileName: "grant-list", Type: reflect.TypeOf(GrantListDoc{})},
		{Kind: KindNamePrivilegePresetList, FileName: "privilege-preset-list", Type: reflect.TypeOf(PrivilegePresetListDoc{})},
		{Kind: KindNameBindingList, FileName: "binding-list", Type: reflect.TypeOf(BindingListDoc{})},
		{Kind: KindNameAPIKeyList, FileName: "api-key-list", Type: reflect.TypeOf(APIKeyListDoc{})},
		{Kind: KindNameCatalog, FileName: "catalog", Type: reflect.TypeOf(CatalogDoc{})},
		{Kind: KindNameSchema, FileName: "schema", Type: reflect.TypeOf(SchemaDoc{})},
		{Kind: KindNameTable, FileName: "table", Type: reflect.TypeOf(TableDoc{})},
		{Kind: KindNameView, FileName: "view", Type: reflect.TypeOf(ViewDoc{})},
		{Kind: KindNameVolume, FileName: "volume", Type: reflect.TypeOf(VolumeDoc{})},
		{Kind: KindNameRowFilterList, FileName: "row-filter-list", Type: reflect.TypeOf(RowFilterListDoc{})},
		{Kind: KindNameColumnMaskList, FileName: "column-mask-list", Type: reflect.TypeOf(ColumnMaskListDoc{})},
		{Kind: KindNameTagConfig, FileName: "tag-config", Type: reflect.TypeOf(TagConfigDoc{})},
		{Kind: KindNameStorageCredentialList, FileName: "storage-credential-list", Type: reflect.TypeOf(StorageCredentialListDoc{})},
		{Kind: KindNameExternalLocationList, FileName: "external-location-list", Type: reflect.TypeOf(ExternalLocationListDoc{})},
		{Kind: KindNameComputeEndpointList, FileName: "compute-endpoint-list", Type: reflect.TypeOf(ComputeEndpointListDoc{})},
		{Kind: KindNameComputeAssignmentList, FileName: "compute-assignment-list", Type: reflect.TypeOf(ComputeAssignmentListDoc{})},
		{Kind: KindNameNotebook, FileName: "notebook", Type: reflect.TypeOf(NotebookDoc{})},
		{Kind: KindNamePipeline, FileName: "pipeline", Type: reflect.TypeOf(PipelineDoc{})},
		{Kind: KindNameModel, FileName: "model", Type: reflect.TypeOf(ModelDoc{})},
		{Kind: KindNameMacro, FileName: "macro", Type: reflect.TypeOf(MacroDoc{})},
	}
}
