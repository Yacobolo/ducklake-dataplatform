package api

import "list"

// Authored lineage operations.

#lineageTag: "Lineage"

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

#lineageNodeResourceResponses: [
	#wrappedJSONSuccessResponse & {
		#body_type: "LineageNode"
	},
	#wrappedJSONResponse & {
		#status_code: 400
		#description: "The server could not understand the request due to invalid syntax."
		#schema_ref:  "Error"
		#body_type:   "LineageNode"
	},
	#wrappedJSONResponse & {
		#status_code: 401
		#description: "Access is unauthorized."
		#schema_ref:  "Error"
		#body_type:   "LineageNode"
	},
	#wrappedJSONResponse & {
		#status_code: 403
		#description: "Access is forbidden."
		#schema_ref:  "Error"
		#body_type:   "LineageNode"
	},
	#wrappedJSONResponse & {
		#status_code: 404
		#description: "The server cannot find the requested resource."
		#schema_ref:  "Error"
		#body_type:   "LineageNode"
	},
	#wrappedJSONResponse & {
		#status_code: 429
		#description: "Client error"
		#schema_ref:  "Error"
		#body_type:   "LineageNode"
	},
	#wrappedJSONResponse & {
		#status_code: 500
		#description: "Server error"
		#schema_ref:  "Error"
		#body_type:   "LineageNode"
	},
]

#paginatedLineageResourceResponses: [
	#wrappedJSONSuccessResponse & {
		#body_type: "PaginatedLineageEdges"
	},
	#wrappedJSONResponse & {
		#status_code: 400
		#description: "The server could not understand the request due to invalid syntax."
		#schema_ref:  "Error"
		#body_type:   "PaginatedLineageEdges"
	},
	#wrappedJSONResponse & {
		#status_code: 401
		#description: "Access is unauthorized."
		#schema_ref:  "Error"
		#body_type:   "PaginatedLineageEdges"
	},
	#wrappedJSONResponse & {
		#status_code: 403
		#description: "Access is forbidden."
		#schema_ref:  "Error"
		#body_type:   "PaginatedLineageEdges"
	},
	#wrappedJSONResponse & {
		#status_code: 404
		#description: "The server cannot find the requested resource."
		#schema_ref:  "Error"
		#body_type:   "PaginatedLineageEdges"
	},
	#wrappedJSONResponse & {
		#status_code: 429
		#description: "Client error"
		#schema_ref:  "Error"
		#body_type:   "PaginatedLineageEdges"
	},
	#wrappedJSONResponse & {
		#status_code: 500
		#description: "Server error"
		#schema_ref:  "Error"
		#body_type:   "PaginatedLineageEdges"
	},
]

#paginatedColumnLineageResourceResponses: [
	#wrappedJSONSuccessResponse & {
		#body_type: "PaginatedColumnLineageEdges"
	},
	#wrappedJSONResponse & {
		#status_code: 400
		#description: "The server could not understand the request due to invalid syntax."
		#schema_ref:  "Error"
		#body_type:   "PaginatedColumnLineageEdges"
	},
	#wrappedJSONResponse & {
		#status_code: 401
		#description: "Access is unauthorized."
		#schema_ref:  "Error"
		#body_type:   "PaginatedColumnLineageEdges"
	},
	#wrappedJSONResponse & {
		#status_code: 403
		#description: "Access is forbidden."
		#schema_ref:  "Error"
		#body_type:   "PaginatedColumnLineageEdges"
	},
	#wrappedJSONResponse & {
		#status_code: 404
		#description: "The server cannot find the requested resource."
		#schema_ref:  "Error"
		#body_type:   "PaginatedColumnLineageEdges"
	},
	#wrappedJSONResponse & {
		#status_code: 429
		#description: "Client error"
		#schema_ref:  "Error"
		#body_type:   "PaginatedColumnLineageEdges"
	},
	#wrappedJSONResponse & {
		#status_code: 500
		#description: "Server error"
		#schema_ref:  "Error"
		#body_type:   "PaginatedColumnLineageEdges"
	},
]

#purgeLineageResponses: [
	#wrappedJSONSuccessResponse & {
		#body_type: "PurgeLineageResponse"
	},
	#wrappedJSONResponse & {
		#status_code: 400
		#description: "The server could not understand the request due to invalid syntax."
		#schema_ref:  "Error"
		#body_type:   "PurgeLineageResponse"
	},
	#wrappedJSONResponse & {
		#status_code: 401
		#description: "Access is unauthorized."
		#schema_ref:  "Error"
		#body_type:   "PurgeLineageResponse"
	},
	#wrappedJSONResponse & {
		#status_code: 403
		#description: "Access is forbidden."
		#schema_ref:  "Error"
		#body_type:   "PurgeLineageResponse"
	},
	#wrappedJSONResponse & {
		#status_code: 429
		#description: "Client error"
		#schema_ref:  "Error"
		#body_type:   "PurgeLineageResponse"
	},
	#wrappedJSONResponse & {
		#status_code: 500
		#description: "Server error"
		#schema_ref:  "Error"
		#body_type:   "PurgeLineageResponse"
	},
]

endpoints_lineage: [
	{
		method:       "get"
		path:         "/lineage/tables/{schema_name}/{table_name}"
		operation_id: "getTableLineage"
		summary:      "Get table lineage"
		tags:         [#lineageTag]
		parameters:   #tableLineageParameters
		responses:    #lineageNodeResourceResponses
		extensions: #authenticatedExtensions & {
			#cli_command: "lineage tables get"
		}
	},
	{
		method:       "get"
		path:         "/lineage/tables/{schema_name}/{table_name}/upstream"
		operation_id: "getUpstreamLineage"
		summary:      "Get upstream lineage"
		tags:         [#lineageTag]
		parameters:   #tableLineageParameters
		responses:    #paginatedLineageResourceResponses
		extensions: #authenticatedExtensions & {
			#cli_command: "lineage tables upstream"
		}
	},
	{
		method:       "get"
		path:         "/lineage/tables/{schema_name}/{table_name}/downstream"
		operation_id: "getDownstreamLineage"
		summary:      "Get downstream lineage"
		tags:         [#lineageTag]
		parameters:   #tableLineageParameters
		responses:    #paginatedLineageResourceResponses
		extensions: #authenticatedExtensions & {
			#cli_command: "lineage tables downstream"
		}
	},
	{
		method:       "delete"
		path:         "/lineage/edges/{edge_id}"
		operation_id: "deleteLineageEdge"
		summary:      "Delete lineage edge"
		tags:         [#lineageTag]
		parameters: [
			#edgeIDPathParameter,
		]
		responses: list.Concat([
			[
				#noContentResponse & {
					#status_code: 204
					#description: "There is no content to send for this request, but the headers may be useful."
				},
			],
			#mutatingErrorResponses,
		])
		extensions: #authenticatedExtensions & {
			#cli_command: "lineage edges delete"
		}
	},
	{
		method:       "get"
		path:         "/lineage/columns/{schema_name}/{table_name}"
		operation_id: "getColumnLineage"
		summary:      "Get column lineage"
		tags:         [#lineageTag]
		parameters:   #tableLineageParameters
		responses:    #paginatedColumnLineageResourceResponses
		extensions: #authenticatedExtensions & {
			#cli_command: "lineage columns get"
		}
	},
	{
		method:       "get"
		path:         "/lineage/columns/{schema_name}/{table_name}/{column_name}/impacts"
		operation_id: "getColumnImpact"
		summary:      "Get column impact"
		tags:         [#lineageTag]
		parameters:   #columnImpactParameters
		responses:    #paginatedColumnLineageResourceResponses
		extensions: #authenticatedExtensions & {
			#cli_command: "lineage impact get"
		}
	},
	{
		method:       "post"
		path:         "/lineage/purges"
		operation_id: "purgeLineage"
		summary:      "Purge lineage"
		tags:         [#lineageTag]
		responses:    #purgeLineageResponses
		request_body: {
			required:    true
			description: "Request payload"
			schema: {
				ref: "PurgeLineageRequest"
			}
		}
		extensions: #authenticatedExtensions & {
			#cli_command: "lineage purge"
		}
	},
]
