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

#edgeIDPathParameter: #pathStringParameter & {
	#name: "edge_id"
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

#lineageOps: [
	#getResource & {
		op:      "getTableLineage"
		path:    #tableLineagePath
		summary: "Get table lineage"
		cli: {
			command: ["lineage", "tables", "get"]
		}
		returns: "LineageNode"
		params:  #tableLineageParameters
	},
	#getResource & {
		op:      "getUpstreamLineage"
		path:    #tableLineagePath + "/upstream"
		summary: "Get upstream lineage"
		cli: {
			command: ["lineage", "tables", "upstream"]
		}
		returns: "PaginatedLineageEdges"
		params:  #tableLineageParameters
	},
	#getResource & {
		op:      "getDownstreamLineage"
		path:    #tableLineagePath + "/downstream"
		summary: "Get downstream lineage"
		cli: {
			command: ["lineage", "tables", "downstream"]
		}
		returns: "PaginatedLineageEdges"
		params:  #tableLineageParameters
	},
	#deleteNoContent & {
		op:      "deleteLineageEdge"
		path:    "/lineage/edges/{edge_id}"
		summary: "Delete lineage edge"
		cli: {
			command: ["lineage", "edges", "delete"]
		}
		params: [
			#edgeIDPathParameter,
		]
	},
	#getResource & {
		op:      "getColumnLineage"
		path:    #columnLineagePath
		summary: "Get column lineage"
		cli: {
			command: ["lineage", "columns", "get"]
		}
		returns: "PaginatedColumnLineageEdges"
		params:  #tableLineageParameters
	},
	#getResource & {
		op:      "getColumnImpact"
		path:    #columnLineagePath + "/{column_name}/impacts"
		summary: "Get column impact"
		cli: {
			command: ["lineage", "impact", "get"]
		}
		returns: "PaginatedColumnLineageEdges"
		params:  #columnImpactParameters
	},
	#postWrapped & {
		op:               "purgeLineage"
		path:             "/lineage/purges"
		summary:          "Purge lineage"
		cli: {
			command: ["lineage", "purge"]
		}
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
