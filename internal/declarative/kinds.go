package declarative

// ResourceKind identifies a type of managed resource.
type ResourceKind int

// Resource kind constants identify each type of managed resource.
// They are ordered by dependency layer (0-9) for correct apply/delete sequencing.
const (
	KindStorageCredential      ResourceKind = iota // layer 0
	KindPrincipal                                  // layer 0
	KindTag                                        // layer 0
	KindMacro                                      // layer 0
	KindDomain                                     // layer 0
	KindWorkspace                                  // layer 0
	KindGroup                                      // layer 1
	KindExternalLocation                           // layer 1
	KindComputeEndpoint                            // layer 1
	KindComputeRoutingDefaults                     // layer 1
	KindTeam                                       // layer 1
	KindFolder                                     // layer 1
	KindProject                                    // layer 1
	KindGroupMembership                            // layer 2
	KindCatalogRegistration                        // layer 2
	KindEnvironment                                // layer 2
	KindSchema                                     // layer 3
	KindComputeAssignment                          // layer 3
	KindTable                                      // layer 4
	KindView                                       // layer 4
	KindVolume                                     // layer 4
	KindPrivilegeGrant                             // layer 5
	KindTagAssignment                              // layer 5
	KindRowFilter                                  // layer 5
	KindColumnMask                                 // layer 5
	KindRowFilterBinding                           // layer 6
	KindColumnMaskBinding                          // layer 6
	KindAPIKey                                     // layer 6
	KindNotebook                                   // layer 6
	KindDataProduct                                // layer 7
	KindAsset                                      // layer 8
	KindModel                                      // layer 9
	KindSemanticModel                              // layer 10
	KindDashboard                                  // layer 11
)

// String returns a human-readable kebab-case name for the resource kind.
func (k ResourceKind) String() string {
	switch k {
	case KindStorageCredential:
		return "storage-credential"
	case KindPrincipal:
		return "principal"
	case KindTag:
		return "tag"
	case KindMacro:
		return "macro"
	case KindDomain:
		return "domain"
	case KindWorkspace:
		return "workspace"
	case KindGroup:
		return "group"
	case KindExternalLocation:
		return "external-location"
	case KindComputeEndpoint:
		return "compute-endpoint"
	case KindComputeRoutingDefaults:
		return "compute-routing-defaults"
	case KindTeam:
		return "team"
	case KindFolder:
		return "folder"
	case KindProject:
		return "project"
	case KindGroupMembership:
		return "group-membership"
	case KindCatalogRegistration:
		return "catalog-registration"
	case KindEnvironment:
		return "environment"
	case KindSchema:
		return "schema"
	case KindComputeAssignment:
		return "compute-assignment"
	case KindTable:
		return "table"
	case KindView:
		return "view"
	case KindVolume:
		return "volume"
	case KindPrivilegeGrant:
		return "privilege-grant"
	case KindTagAssignment:
		return "tag-assignment"
	case KindRowFilter:
		return "row-filter"
	case KindColumnMask:
		return "column-mask"
	case KindRowFilterBinding:
		return "row-filter-binding"
	case KindColumnMaskBinding:
		return "column-mask-binding"
	case KindAPIKey:
		return "api-key"
	case KindNotebook:
		return "notebook"
	case KindDataProduct:
		return "data-product"
	case KindAsset:
		return "asset"
	case KindModel:
		return "model"
	case KindSemanticModel:
		return "semantic-model"
	case KindDashboard:
		return "dashboard"
	default:
		return "unknown"
	}
}

// Layer returns the dependency layer (0-9) for ordering.
// Layer 0 has no dependencies; higher layers depend on lower ones.
func (k ResourceKind) Layer() int {
	switch k {
	case KindStorageCredential, KindPrincipal, KindTag, KindMacro, KindDomain, KindWorkspace:
		return 0
	case KindGroup, KindExternalLocation, KindComputeEndpoint, KindComputeRoutingDefaults, KindTeam, KindFolder, KindProject:
		return 1
	case KindGroupMembership, KindCatalogRegistration, KindEnvironment:
		return 2
	case KindSchema, KindComputeAssignment:
		return 3
	case KindTable, KindView, KindVolume:
		return 4
	case KindPrivilegeGrant, KindTagAssignment, KindRowFilter, KindColumnMask:
		return 5
	case KindRowFilterBinding, KindColumnMaskBinding, KindAPIKey, KindNotebook:
		return 6
	case KindDataProduct:
		return 7
	case KindAsset:
		return 8
	case KindModel:
		return 9
	case KindSemanticModel:
		return 10
	case KindDashboard:
		return 11
	default:
		return 99
	}
}

// MaxLayer is the highest dependency layer.
const MaxLayer = 11

// Operation represents a planned change type.
type Operation int

const (
	// OpCreate indicates a resource should be created.
	OpCreate Operation = iota
	// OpUpdate indicates a resource should be updated.
	OpUpdate
	// OpDelete indicates a resource should be deleted.
	OpDelete
)

// String returns the operation name.
func (o Operation) String() string {
	switch o {
	case OpCreate:
		return "create"
	case OpUpdate:
		return "update"
	case OpDelete:
		return "delete"
	default:
		return "unknown"
	}
}

// Known Kind strings used in YAML documents.
const (
	KindNamePrincipalList          = "PrincipalList"
	KindNameGroupList              = "GroupList"
	KindNameGrantList              = "GrantList"
	KindNamePrivilegePresetList    = "PrivilegePresetList"
	KindNameBindingList            = "BindingList"
	KindNameAPIKeyList             = "APIKeyList"
	KindNameCatalog                = "Catalog"
	KindNameSchema                 = "Schema"
	KindNameTable                  = "Table"
	KindNameView                   = "View"
	KindNameVolume                 = "Volume"
	KindNameRowFilterList          = "RowFilterList"
	KindNameColumnMaskList         = "ColumnMaskList"
	KindNameTagConfig              = "TagConfig"
	KindNameStorageCredentialList  = "StorageCredentialList"
	KindNameExternalLocationList   = "ExternalLocationList"
	KindNameComputeEndpointList    = "ComputeEndpointList"
	KindNameComputeAssignmentList  = "ComputeAssignmentList"
	KindNameComputeRoutingDefaults = "ComputeRoutingDefaults"
	KindNameDomain                 = "Domain"
	KindNameWorkspace              = "Workspace"
	KindNameFolder                 = "Folder"
	KindNameProject                = "Project"
	KindNameEnvironment            = "Environment"
	KindNameTeam                   = "Team"
	KindNameDataProduct            = "DataProduct"
	KindNameNotebook               = "Notebook"
	KindNameAsset                  = "Asset"
	KindNameModel                  = "Model"
	KindNameSemanticModel          = "SemanticModel"
	KindNameMacro                  = "Macro"
	KindNameDashboard              = "Dashboard"
)

// SupportedAPIVersion is the current API version for YAML documents.
const SupportedAPIVersion = "duck/v1"
