package api

// Authored paginated schemas.

schemas_pagination: {
  PaginatedAPIKeys: #paginatedItemsSchema & {
    #item_ref: "APIKeyInfo"
  },
  PaginatedAssetCheckResults: #paginatedItemsSchema & {
    #item_ref: "AssetCheckResult"
  },
  PaginatedAssetMaterializations: #paginatedItemsSchema & {
    #item_ref: "AssetMaterialization"
  },
  PaginatedAssetPartitions: #paginatedItemsSchema & {
    #item_ref: "AssetPartition"
  },
  PaginatedAssetRuns: #paginatedItemsSchema & {
    #item_ref: "AssetRun"
  },
  PaginatedAssets: #paginatedItemsSchema & {
    #item_ref: "Asset"
  },
  PaginatedAuditLogs: #paginatedItemsSchema & {
    #item_ref: "AuditEntry"
  },
  PaginatedBackfillRequests: #paginatedItemsSchema & {
    #item_ref: "BackfillRequest"
  },
  PaginatedBuilds: #paginatedItemsSchema & {
    #item_ref: "Build"
  },
  PaginatedColumnDetails: #paginatedItemsSchema & {
    #item_ref: "ColumnDetail"
  },
  PaginatedColumnLineageEdges: #paginatedItemsSchema & {
    #item_ref: "ColumnLineageEdge"
  },
  PaginatedColumnMaskBindings: #paginatedItemsSchema & {
    #item_ref: "ColumnMaskBinding"
  },
  PaginatedColumnMasks: #paginatedItemsSchema & {
    #item_ref: "ColumnMask"
  },
  PaginatedComputeAssignments: #paginatedItemsSchema & {
    #item_ref: "ComputeAssignment"
  },
  PaginatedComputeEndpoints: #paginatedItemsSchema & {
    #item_ref: "ComputeEndpoint"
  },
  PaginatedDashboards: #paginatedItemsSchema & {
    #item_ref: "Dashboard"
  },
  PaginatedDataProducts: #paginatedItemsSchema & {
    #item_ref: "DataProductListItem"
  },
  PaginatedEnvironments: #paginatedItemsSchema & {
    #item_ref: "Environment"
  },
  PaginatedExternalLocations: #paginatedItemsSchema & {
    #item_ref: "ExternalLocation"
  },
  PaginatedFolderContents: #paginatedItemsSchema & {
    #item_ref: "FolderContentItem"
  },
  PaginatedFolders: #paginatedItemsSchema & {
    #item_ref: "Folder"
  },
  PaginatedGitRepos: #paginatedItemsSchema & {
    #item_ref: "GitRepo"
  },
  PaginatedGrants: #paginatedItemsSchema & {
    #item_ref: "PrivilegeGrant"
  },
  PaginatedGroupMembers: #paginatedItemsSchema & {
    #item_ref: "GroupMember"
  },
  PaginatedGroups: #paginatedItemsSchema & {
    #item_ref: "Group"
  },
  PaginatedLineageEdges: #paginatedItemsSchema & {
    #item_ref: "LineageEdge"
  },
  PaginatedMacros: #paginatedItemsSchema & {
    #item_ref: "Macro"
  },
  PaginatedModelRuns: #paginatedItemsSchema & {
    #item_ref: "ModelRun"
  },
  PaginatedModels: #paginatedItemsSchema & {
    #item_ref: "Model"
  },
  PaginatedNotebookJobs: #paginatedItemsSchema & {
    #item_ref: "NotebookJob"
  },
  PaginatedNotebooks: #paginatedItemsSchema & {
    #item_ref: "Notebook"
  },
  PaginatedPipelineRuns: #paginatedItemsSchema & {
    #item_ref: "PipelineRun"
  },
  PaginatedPipelines: #paginatedItemsSchema & {
    #item_ref: "Pipeline"
  },
  PaginatedPrincipals: #paginatedItemsSchema & {
    #item_ref: "Principal"
  },
  PaginatedProductDomains: #paginatedItemsSchema & {
    #item_ref: "ProductDomain"
  },
  PaginatedProductTeams: #paginatedItemsSchema & {
    #item_ref: "ProductTeam"
  },
  PaginatedProjects: #paginatedItemsSchema & {
    #item_ref: "Project"
  },
  PaginatedQueryHistoryEntries: #paginatedItemsSchema & {
    #item_ref: "QueryHistoryEntry"
  },
  PaginatedQueryJobs: #paginatedItemsSchema & {
    #item_ref: "QueryJob"
  },
  PaginatedRecentResources: #paginatedItemsSchema & {
    #item_ref: "RecentResource"
  },
  PaginatedRowFilterBindings: #paginatedItemsSchema & {
    #item_ref: "RowFilterBinding"
  },
  PaginatedRowFilters: #paginatedItemsSchema & {
    #item_ref: "RowFilter"
  },
  PaginatedSavedResources: #paginatedItemsSchema & {
    #item_ref: "SavedResource"
  },
  PaginatedSchemaDetails: #paginatedItemsSchema & {
    #item_ref: "SchemaDetail"
  },
  PaginatedSearchResults: #paginatedItemsSchema & {
    #item_ref: "SearchResult"
  },
  PaginatedSemanticModels: #paginatedItemsSchema & {
    #item_ref: "SemanticModel"
  },
  PaginatedStorageCredentials: #paginatedItemsSchema & {
    #item_ref: "StorageCredential"
  },
  PaginatedTableDetails: #paginatedItemsSchema & {
    #item_ref: "TableDetail"
  },
  PaginatedTagAssignments: #paginatedItemsSchema & {
    #item_ref: "TagAssignment"
  },
  PaginatedTags: #paginatedItemsSchema & {
    #item_ref: "Tag"
  },
  PaginatedViewDetails: #paginatedItemsSchema & {
    #item_ref: "ViewDetail"
  },
  PaginatedVolumes: #paginatedItemsSchema & {
    #item_ref: "VolumeDetail"
  },
  PaginatedWorkspaces: #paginatedItemsSchema & {
    #item_ref: "Workspace"
  },
}
