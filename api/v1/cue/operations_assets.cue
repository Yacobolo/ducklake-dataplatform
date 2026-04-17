package api

// Authored asset operations.

#assetsTag: "Assets"

#plainAssetOperation: #genericOperationSpec & {
	wrapped: false
}

#assetKeyPathParameter: #pathStringParameter & {
	#name: "asset_key"
}

#backfillIDPathParameter: #pathStringParameter & {
	#name: "backfill_id"
}

#assetRunStatusQueryParameter: {
	name:    "status"
	in:      "query"
	explode: false
	schema: {
		ref: "AssetRunStatus"
	}
}

#assetPathParameters: [
	#assetKeyPathParameter,
]

#assetPaginationParameters: [
	#assetKeyPathParameter,
	#paginationParameters[0],
	#paginationParameters[1],
]

#assetStatusPaginationParameters: [
	#assetKeyPathParameter,
	#assetRunStatusQueryParameter,
	#paginationParameters[0],
	#paginationParameters[1],
]

#assetBackfillPathParameters: [
	#assetKeyPathParameter,
	#backfillIDPathParameter,
]

#manageAssetDefinitionsAuthz: {
	mode: "privilege"
	checks: [
		{
			securable_type:     "catalog"
			privilege:          "MANAGE_ASSET_DEFINITIONS"
			securable_id_source: "catalog_sentinel"
		},
	]
}

#executeAssetMaterializationAuthz: {
	mode: "privilege"
	checks: [
		{
			securable_type:     "catalog"
			privilege:          "EXECUTE_ASSET_MATERIALIZATION"
			securable_id_source: "catalog_sentinel"
		},
	]
}

#assetOps: [
	#plainAssetOperation & {
		kind:         "response"
		method:       "get"
		op:           "listAssets"
		path:         "/assets"
		summary:      "List assets"
		cli: {
			command: ["assets", "list"]
		}
		returns:      "PaginatedAssets"
		error_family: "standard"
		params:       #paginationParameters
	},
	#plainAssetOperation & {
		kind:           "response"
		method:         "post"
		op:             "createAsset"
		path:           "/assets"
		summary:        "Create asset"
		description:    "Creates a managed asset definition together with its ownership, checks, tags, and upstream lineage metadata."
		cli: {
			command: ["assets", "create"]
		}
		returns:        "Asset"
		success_status: 201
		error_family:   "mutating_conflict"
		body_ref:       "CreateAssetRequest"
		body_description: "Request payload"
		authz_default:   false
		authz:           #manageAssetDefinitionsAuthz
	},
	#plainAssetOperation & {
		kind:         "response"
		method:       "get"
		op:           "getAsset"
		path:         "/assets/{asset_key}"
		summary:      "Get asset"
		cli: {
			command: ["assets", "get"]
		}
		returns:      "Asset"
		error_family: "resource"
		params:       #assetPathParameters
	},
	#plainAssetOperation & {
		kind:         "response"
		method:       "patch"
		op:           "updateAsset"
		path:         "/assets/{asset_key}"
		summary:      "Update asset"
		cli: {
			command: ["assets", "update"]
		}
		returns:      "Asset"
		error_family: "resource"
		params:       #assetPathParameters
		body_ref:     "UpdateAssetRequest"
		body_description: "Request payload"
		authz_default:   false
		authz:           #manageAssetDefinitionsAuthz
	},
	#plainAssetOperation & {
		kind:          "no_content"
		method:        "delete"
		op:            "deleteAsset"
		path:          "/assets/{asset_key}"
		summary:       "Delete asset"
		cli: {
			command: ["assets", "delete"]
		}
		error_family:  "resource"
		params:        #assetPathParameters
		authz_default: false
		authz:         #manageAssetDefinitionsAuthz
	},
	#plainAssetOperation & {
		kind:         "response"
		method:       "get"
		op:           "getAssetGraph"
		path:         "/assets/{asset_key}/graph"
		summary:      "Get asset graph"
		cli: {
			command: ["assets", "graph", "get"]
		}
		returns:      "AssetGraph"
		error_family: "resource"
		params:       #assetPathParameters
	},
	#plainAssetOperation & {
		kind:         "response"
		method:       "get"
		op:           "getAssetFreshness"
		path:         "/assets/{asset_key}/freshness"
		summary:      "Get asset freshness"
		cli: {
			command: ["assets", "freshness", "get"]
		}
		returns:      "AssetFreshnessStatus"
		error_family: "resource"
		params:       #assetPathParameters
	},
	#plainAssetOperation & {
		kind:         "response"
		method:       "get"
		op:           "explainAssetFreshness"
		path:         "/assets/{asset_key}/freshness/explanation"
		summary:      "Explain asset freshness"
		cli: {
			command: ["assets", "freshness", "explain"]
		}
		returns:      "AssetFreshnessExplanation"
		error_family: "resource"
		params:       #assetPathParameters
	},
	#plainAssetOperation & {
		kind:         "response"
		method:       "get"
		op:           "listAssetFreshnessRequirements"
		path:         "/assets/{asset_key}/freshness/requirements"
		summary:      "List asset freshness requirements"
		cli: {
			command: ["assets", "freshness", "requirements"]
		}
		returns:      "AssetFreshnessRequirementsResponse"
		error_family: "resource"
		params:       #assetPathParameters
	},
	#plainAssetOperation & {
		kind:         "response"
		method:       "get"
		op:           "listAssetFreshnessBlockers"
		path:         "/assets/{asset_key}/freshness/blockers"
		summary:      "List asset freshness blockers"
		cli: {
			command: ["assets", "freshness", "blockers"]
		}
		returns:      "AssetFreshnessBlockersResponse"
		error_family: "resource"
		params:       #assetPathParameters
	},
	#plainAssetOperation & {
		kind:           "response"
		method:         "post"
		op:             "reconcileAssetFreshness"
		path:           "/assets/{asset_key}/freshness-reconciliations"
		summary:        "Reconcile asset freshness"
		cli: {
			command: ["assets", "freshness", "reconcile"]
		}
		returns:        "AssetFreshnessReconcileResponse"
		success_status: 202
		error_family:   "resource"
		params:         #assetPathParameters
	},
	#plainAssetOperation & {
		kind:         "response"
		method:       "get"
		op:           "listAssetPartitions"
		path:         "/assets/{asset_key}/partitions"
		summary:      "List asset partitions"
		cli: {
			command: ["assets", "partitions", "list"]
		}
		returns:      "PaginatedAssetPartitions"
		error_family: "resource"
		params:       #assetPaginationParameters
	},
	#plainAssetOperation & {
		kind:         "response"
		method:       "get"
		op:           "listAssetRuns"
		path:         "/assets/{asset_key}/runs"
		summary:      "List asset runs"
		cli: {
			command: ["assets", "runs", "list"]
		}
		returns:      "PaginatedAssetRuns"
		error_family: "resource"
		params:       #assetStatusPaginationParameters
	},
	#plainAssetOperation & {
		kind:           "response"
		method:         "post"
		op:             "triggerAssetMaterialization"
		path:           "/assets/{asset_key}/materializations"
		summary:        "Trigger asset materialization"
		description:    "Starts a materialization run for the specified asset and returns the queued execution metadata."
		cli: {
			command: ["assets", "materialize"]
		}
		returns:        "AssetTriggerResponse"
		success_status: 202
		error_family:   "resource"
		params:         #assetPathParameters
		body_ref:       "TriggerAssetMaterializationRequest"
		body_required:  false
		body_description: "Request payload"
		authz_default:   false
		authz:           #executeAssetMaterializationAuthz
	},
	#plainAssetOperation & {
		kind:         "response"
		method:       "get"
		op:           "listAssetMaterializations"
		path:         "/assets/{asset_key}/materializations"
		summary:      "List asset materializations"
		cli: {
			command: ["assets", "materializations", "list"]
		}
		returns:      "PaginatedAssetMaterializations"
		error_family: "resource"
		params:       #assetPaginationParameters
	},
	#plainAssetOperation & {
		kind:         "response"
		method:       "get"
		op:           "listAssetChecks"
		path:         "/assets/{asset_key}/checks"
		summary:      "List asset checks"
		cli: {
			command: ["assets", "checks", "list"]
		}
		returns:      "AssetCheckList"
		error_family: "resource"
		params:       #assetPathParameters
	},
	#plainAssetOperation & {
		kind:         "response"
		method:       "get"
		op:           "listAssetCheckResults"
		path:         "/assets/{asset_key}/checks/results"
		summary:      "List asset check results"
		cli: {
			command: ["assets", "check-results", "list"]
		}
		returns:      "PaginatedAssetCheckResults"
		error_family: "resource"
		params:       #assetPaginationParameters
	},
	#plainAssetOperation & {
		kind:         "response"
		method:       "get"
		op:           "listAssetBackfills"
		path:         "/assets/{asset_key}/backfills"
		summary:      "List asset backfills"
		cli: {
			command: ["assets", "backfills", "list"]
		}
		returns:      "PaginatedBackfillRequests"
		error_family: "resource"
		params:       #assetStatusPaginationParameters
	},
	#plainAssetOperation & {
		kind:           "response"
		method:         "post"
		op:             "createAssetBackfill"
		path:           "/assets/{asset_key}/backfills"
		summary:        "Create asset backfill"
		cli: {
			command: ["assets", "backfills", "create"]
		}
		returns:        "CreateAssetBackfillResponse"
		success_status: 201
		error_family:   "resource"
		params:         #assetPathParameters
		body_ref:       "CreateAssetBackfillRequest"
		body_description: "Request payload"
		authz_default:   false
		authz:           #executeAssetMaterializationAuthz
	},
	#plainAssetOperation & {
		kind:         "response"
		method:       "get"
		op:           "getAssetBackfill"
		path:         "/assets/{asset_key}/backfills/{backfill_id}"
		summary:      "Get asset backfill"
		cli: {
			command: ["assets", "backfills", "get"]
		}
		returns:      "AssetBackfillDetails"
		error_family: "resource"
		params:       #assetBackfillPathParameters
	},
]

endpoints_assets: [
	for op in #assetOps {
		(#endpointFromGenericOperation & {
			tag:  #assetsTag
			spec: op
		}).endpoint
	},
]
