package api

import "list"

// Authored lineage operations.

#lineageTag: "Lineage"
#tableLineagePath:  "/lineage/tables/{schema_name}/{table_name}"
#columnLineagePath: "/lineage/columns/{schema_name}/{table_name}"

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
		cli:     "lineage tables get"
		returns: "LineageNode"
		params:  #tableLineageParameters
	},
	#getResource & {
		op:      "getUpstreamLineage"
		path:    #tableLineagePath + "/upstream"
		summary: "Get upstream lineage"
		cli:     "lineage tables upstream"
		returns: "PaginatedLineageEdges"
		params:  #tableLineageParameters
	},
	#getResource & {
		op:      "getDownstreamLineage"
		path:    #tableLineagePath + "/downstream"
		summary: "Get downstream lineage"
		cli:     "lineage tables downstream"
		returns: "PaginatedLineageEdges"
		params:  #tableLineageParameters
	},
	#deleteNoContent & {
		op:      "deleteLineageEdge"
		path:    "/lineage/edges/{edge_id}"
		summary: "Delete lineage edge"
		cli:     "lineage edges delete"
		params: [
			#edgeIDPathParameter,
		]
	},
	#getResource & {
		op:      "getColumnLineage"
		path:    #columnLineagePath
		summary: "Get column lineage"
		cli:     "lineage columns get"
		returns: "PaginatedColumnLineageEdges"
		params:  #tableLineageParameters
	},
	#getResource & {
		op:      "getColumnImpact"
		path:    #columnLineagePath + "/{column_name}/impacts"
		summary: "Get column impact"
		cli:     "lineage impact get"
		returns: "PaginatedColumnLineageEdges"
		params:  #columnImpactParameters
	},
	#getResource & {
		op:      "getBuildColumnLineage"
		path:    "/lineage/builds/{build_id}/columns"
		summary: "Get compile-time build column lineage"
		returns: "PaginatedCompiledColumnLineage"
		params:  #buildColumnLineageParameters
	},
	#getResource & {
		op:      "getBuildDiagnostics"
		path:    "/lineage/builds/{build_id}/diagnostics"
		summary: "Get build diagnostics"
		returns: "PaginatedCompileDiagnostics"
		params:  #buildDiagnosticsParameters
	},
	#getResource & {
		op:      "getBuildSourceColumnImpact"
		path:    "/lineage/builds/{build_id}/impacts/sources/{schema_name}/{table_name}/{column_name}"
		summary: "Get build impact for a source column"
		returns: "PaginatedCompiledColumnLineage"
		params:  #buildSourceImpactParameters
	},
	#getResource & {
		op:      "getModelImpactAnalysis"
		path:    "/lineage/impacts/models/{project_name}/{model_name}"
		summary: "Get build impact for a model"
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
		returns:          "RebuildPlan"
		body_ref:         "PlanRebuildRequest"
		body_description: "Request payload"
	},
	#postWrapped & {
		op:               "compareBuilds"
		path:             "/lineage/builds/compare"
		summary:          "Compare builds or compare a build to head"
		returns:          "BuildCompareResult"
		body_ref:         "CompareBuildsRequest"
		body_description: "Request payload"
	},
	#postWrapped & {
		op:               "purgeLineage"
		path:             "/lineage/purges"
		summary:          "Purge lineage"
		cli:              "lineage purge"
		returns:          "PurgeLineageResponse"
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
