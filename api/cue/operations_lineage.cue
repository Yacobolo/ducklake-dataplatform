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
		{
			method:       op.method
			path:         op.path
			operation_id: op.op
			summary:      op.summary
			tags:         [#lineageTag]
			if op.params != _|_ {
				parameters: op.params
			}
			if op.kind == "wrapped_resource" {
				responses: list.Concat([
					[
						#wrappedJSONSuccessResponse & {
							#body_type: op.returns
						},
					],
					[
						for template in #resourceErrorTemplates {
							#wrappedJSONResponse & {
								#status_code: template.status_code
								#description: template.description
								#schema_ref:  "Error"
								#body_type:   op.returns
							}
						},
					],
				])
			}
			if op.kind == "wrapped_mutating" {
				request_body: {
					required: *true | bool
					required: *true | op.body_required
					if op.body_description != _|_ {
						description: op.body_description
					}
					schema: {
						ref: op.body_ref
					}
				}
				responses: list.Concat([
					[
						#wrappedJSONSuccessResponse & {
							#body_type: op.returns
						},
					],
					[
						for template in #mutatingErrorTemplates {
							#wrappedJSONResponse & {
								#status_code: template.status_code
								#description: template.description
								#schema_ref:  "Error"
								#body_type:   op.returns
							}
						},
					],
				])
			}
			if op.kind == "no_content_mutating" {
				responses: list.Concat([
					[
						#noContentResponse & {
							#status_code: 204
							#description: "There is no content to send for this request, but the headers may be useful."
						},
					],
					#mutatingErrorResponses,
				])
			}
			extensions: #authenticatedExtensions & {
				#cli_command: op.cli
			}
		}
	},
]
