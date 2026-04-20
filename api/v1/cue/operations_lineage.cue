package api

import "list"

// Authored lineage operations.

#lineageTag: "Lineage"
#tableLineagePath:        "/lineage/tables/{schema_name}/{table_name}"
#columnLineagePath:       "/lineage/columns/{schema_name}/{table_name}"
#catalogTableLineagePath: "/catalog/lineage/tables/{schema_name}/{table_name}"
#catalogColumnLineagePath: "/catalog/lineage/columns/{schema_name}/{table_name}"

#schemaNamePathParameter: #pathStringParameter & {
	#name: "schema_name"
}

#tableNamePathParameter: #pathStringParameter & {
	#name: "table_name"
}

#columnNamePathParameter: #pathStringParameter & {
	#name: "column_name"
}

#buildIDPathParameter: #pathStringParameter & {
	#name: "build_id"
}

#projectNamePathParameter: #pathStringParameter & {
	#name: "project_name"
}

#modelNamePathParameter: #pathStringParameter & {
	#name: "model_name"
}

#macroNamePathParameter: #pathStringParameter & {
	#name: "macro_name"
}

#edgeIDPathParameter: #pathStringParameter & {
	#name: "edge_id"
}

#buildIDQueryParameter: #queryStringParameter & {
	#name: "build_id"
}

#modelNameQueryParameter: #queryStringParameter & {
	#name: "model_name"
}

#diagnosticSeverityQueryParameter: #queryStringParameter & {
	#name: "severity"
}

#diagnosticCodeQueryParameter: #queryStringParameter & {
	#name: "code"
}

#tableLineageParameters: list.Concat([
	[
		#schemaNamePathParameter,
		#tableNamePathParameter,
	],
	#paginationParameters,
])

#columnImpactParameters: list.Concat([
	[
		#schemaNamePathParameter,
		#tableNamePathParameter,
		#columnNamePathParameter,
	],
	#paginationParameters,
])

#buildColumnLineageParameters: list.Concat([
	[
		#buildIDPathParameter,
		#modelNameQueryParameter,
	],
	#paginationParameters,
])

#buildDiagnosticsParameters: list.Concat([
	[
		#buildIDPathParameter,
		#modelNameQueryParameter,
		#diagnosticSeverityQueryParameter,
		#diagnosticCodeQueryParameter,
	],
	#paginationParameters,
])

#buildSourceImpactParameters: list.Concat([
	[
		#buildIDPathParameter,
		#schemaNamePathParameter,
		#tableNamePathParameter,
		#columnNamePathParameter,
	],
	#paginationParameters,
])

#lineageOps: [
	#getResource & {
		op:      "getTableLineage"
		path:    #tableLineagePath
		summary: "Get table lineage"
		description: "Deprecated migration alias. Use /catalog/lineage/tables/{schema_name}/{table_name}."
		deprecated: true
		returns: "LineageNode"
		params:  #tableLineageParameters
	},
	#getResource & {
		op:      "getUpstreamLineage"
		path:    #tableLineagePath + "/upstream"
		summary: "Get upstream lineage"
		description: "Deprecated migration alias. Use /catalog/lineage/tables/{schema_name}/{table_name}/upstream."
		deprecated: true
		returns: "PaginatedLineageEdges"
		params:  #tableLineageParameters
	},
	#getResource & {
		op:      "getDownstreamLineage"
		path:    #tableLineagePath + "/downstream"
		summary: "Get downstream lineage"
		description: "Deprecated migration alias. Use /catalog/lineage/tables/{schema_name}/{table_name}/downstream."
		deprecated: true
		returns: "PaginatedLineageEdges"
		params:  #tableLineageParameters
	},
	#deleteNoContent & {
		op:      "deleteLineageEdge"
		path:    "/lineage/edges/{edge_id}"
		summary: "Delete lineage edge"
		description: "Deprecated migration alias. Use /catalog/lineage/edges/{edge_id}."
		deprecated: true
		params: [
			#edgeIDPathParameter,
		]
	},
	#getResource & {
		op:      "getColumnLineage"
		path:    #columnLineagePath
		summary: "Get column lineage"
		description: "Deprecated migration alias. Use /catalog/lineage/columns/{schema_name}/{table_name}."
		deprecated: true
		returns: "PaginatedColumnLineageEdges"
		params:  #tableLineageParameters
	},
	#getResource & {
		op:      "getColumnImpact"
		path:    #columnLineagePath + "/{column_name}/impacts"
		summary: "Get column impact"
		description: "Deprecated migration alias. Use /catalog/lineage/columns/{schema_name}/{table_name}/{column_name}/impacts."
		deprecated: true
		returns: "PaginatedColumnLineageEdges"
		params:  #columnImpactParameters
	},
	#getResource & {
		op:      "getBuildColumnLineage"
		path:    "/lineage/builds/{build_id}/columns"
		summary: "Get compile-time build column lineage"
		description: "Deprecated migration alias. Use project environment build lineage endpoints."
		deprecated: true
		returns: "PaginatedCompiledColumnLineage"
		params:  #buildColumnLineageParameters
	},
	#getResource & {
		op:      "getBuildDiagnostics"
		path:    "/lineage/builds/{build_id}/diagnostics"
		summary: "Get build diagnostics"
		description: "Deprecated migration alias. Use project environment build diagnostic endpoints."
		deprecated: true
		returns: "PaginatedCompileDiagnostics"
		params:  #buildDiagnosticsParameters
	},
	#getResource & {
		op:      "getBuildSourceColumnImpact"
		path:    "/lineage/builds/{build_id}/impacts/sources/{schema_name}/{table_name}/{column_name}"
		summary: "Get build impact for a source column"
		description: "Deprecated migration alias. Use project environment build source impact endpoints."
		deprecated: true
		returns: "PaginatedCompiledColumnLineage"
		params:  #buildSourceImpactParameters
	},
	#getResource & {
		op:      "getModelImpactAnalysis"
		path:    "/lineage/impacts/models/{project_name}/{model_name}"
		summary: "Get build impact for a model"
		description: "Deprecated migration alias. Use project environment build or compilation impact endpoints."
		deprecated: true
		returns: "BuildImpactResult"
		params: [
			#projectNamePathParameter,
			#modelNamePathParameter,
			#buildIDQueryParameter,
		]
	},
	#getResource & {
		op:      "getMacroImpactAnalysis"
		path:    "/lineage/impacts/macros/{project_name}/{macro_name}"
		summary: "Get build impact for a macro"
		description: "Deprecated migration alias. Use project environment build or compilation impact endpoints."
		deprecated: true
		returns: "BuildImpactResult"
		params: [
			#projectNamePathParameter,
			#macroNamePathParameter,
			#buildIDQueryParameter,
		]
	},
	#postWrapped & {
		op:               "planRebuild"
		path:             "/lineage/plans/rebuild"
		summary:          "Plan a code and data aware rebuild"
		description:      "Deprecated migration alias. Use project environment rebuild plans."
		deprecated:       true
		returns:          "RebuildPlan"
		success_status:   201
		body_ref:         "PlanRebuildRequest"
		body_description: "Request payload"
	},
	#postWrapped & {
		op:               "compareBuilds"
		path:             "/lineage/builds/compare"
		summary:          "Compare builds or compare a build to head"
		description:      "Deprecated migration alias. Use project environment build comparisons."
		deprecated:       true
		returns:          "BuildCompareResult"
		success_status:   201
		body_ref:         "CompareBuildsRequest"
		body_description: "Request payload"
	},
	#postWrapped & {
		op:               "purgeLineage"
		path:             "/lineage/purges"
		summary:          "Purge lineage"
		description:      "Deprecated migration alias. Use /catalog/lineage/purges."
		deprecated:       true
		returns:          "PurgeLineageResponse"
		body_ref:         "PurgeLineageRequest"
		body_description: "Request payload"
	},
	#getResource & {
		op:      "getCatalogTableLineage"
		path:    #catalogTableLineagePath
		summary: "Get catalog table lineage"
		cli:     "catalog lineage tables get"
		returns: "LineageNode"
		params:  #tableLineageParameters
	},
	#getResource & {
		op:      "getCatalogUpstreamLineage"
		path:    #catalogTableLineagePath + "/upstream"
		summary: "Get catalog upstream lineage"
		cli:     "catalog lineage tables upstream"
		returns: "PaginatedLineageEdges"
		params:  #tableLineageParameters
	},
	#getResource & {
		op:      "getCatalogDownstreamLineage"
		path:    #catalogTableLineagePath + "/downstream"
		summary: "Get catalog downstream lineage"
		cli:     "catalog lineage tables downstream"
		returns: "PaginatedLineageEdges"
		params:  #tableLineageParameters
	},
	#deleteNoContent & {
		op:      "deleteCatalogLineageEdge"
		path:    "/catalog/lineage/edges/{edge_id}"
		summary: "Delete catalog lineage edge"
		cli:     "catalog lineage edges delete"
		params: [
			#edgeIDPathParameter,
		]
	},
	#getResource & {
		op:      "getCatalogColumnLineage"
		path:    #catalogColumnLineagePath
		summary: "Get catalog column lineage"
		cli:     "catalog lineage columns get"
		returns: "PaginatedColumnLineageEdges"
		params:  #tableLineageParameters
	},
	#getResource & {
		op:      "getCatalogColumnImpact"
		path:    #catalogColumnLineagePath + "/{column_name}/impacts"
		summary: "Get catalog column impact"
		cli:     "catalog lineage impact get"
		returns: "PaginatedColumnLineageEdges"
		params:  #columnImpactParameters
	},
	#postWrapped & {
		op:               "purgeCatalogLineage"
		path:             "/catalog/lineage/purges"
		summary:          "Purge catalog lineage"
		cli:              "catalog lineage purge"
		returns:          "PurgeLineageResponse"
		success_status:   201
		body_ref:         "PurgeLineageRequest"
		body_description: "Request payload"
	},
]

endpoints_lineage: [
	for op in #lineageOps {
		(#endpointFromOperation & {
			tag:  #lineageTag
			spec: op
		}).endpoint
	},
]
