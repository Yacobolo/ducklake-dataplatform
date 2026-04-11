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

#wrappedResourceOps: [
	{
		path:         "/lineage/tables/{schema_name}/{table_name}"
		operation_id: "getTableLineage"
		summary:      "Get table lineage"
		cli_command:  "lineage tables get"
		body_type:    "LineageNode"
		parameters:   #tableLineageParameters
	},
	{
		path:         "/lineage/tables/{schema_name}/{table_name}/upstream"
		operation_id: "getUpstreamLineage"
		summary:      "Get upstream lineage"
		cli_command:  "lineage tables upstream"
		body_type:    "PaginatedLineageEdges"
		parameters:   #tableLineageParameters
	},
	{
		path:         "/lineage/tables/{schema_name}/{table_name}/downstream"
		operation_id: "getDownstreamLineage"
		summary:      "Get downstream lineage"
		cli_command:  "lineage tables downstream"
		body_type:    "PaginatedLineageEdges"
		parameters:   #tableLineageParameters
	},
	{
		path:         "/lineage/columns/{schema_name}/{table_name}"
		operation_id: "getColumnLineage"
		summary:      "Get column lineage"
		cli_command:  "lineage columns get"
		body_type:    "PaginatedColumnLineageEdges"
		parameters:   #tableLineageParameters
	},
	{
		path:         "/lineage/columns/{schema_name}/{table_name}/{column_name}/impacts"
		operation_id: "getColumnImpact"
		summary:      "Get column impact"
		cli_command:  "lineage impact get"
		body_type:    "PaginatedColumnLineageEdges"
		parameters:   #columnImpactParameters
	},
]

#noContentMutatingOps: [
	{
		method:       "delete"
		path:         "/lineage/edges/{edge_id}"
		operation_id: "deleteLineageEdge"
		summary:      "Delete lineage edge"
		cli_command:  "lineage edges delete"
		parameters: [
			#edgeIDPathParameter,
		]
	},
]

#wrappedMutatingOps: [
	{
		method:       "post"
		path:         "/lineage/purges"
		operation_id: "purgeLineage"
		summary:      "Purge lineage"
		cli_command:  "lineage purge"
		body_type:    "PurgeLineageResponse"
		request_body: {
			required:    true
			description: "Request payload"
			schema: {
				ref: "PurgeLineageRequest"
			}
		}
	},
]

endpoints_lineage: list.Concat([
	[
		for op in #wrappedResourceOps {
			{
				method:       "get"
				path:         op.path
				operation_id: op.operation_id
				summary:      op.summary
				tags:         [#lineageTag]
				parameters:   op.parameters
				responses: list.Concat([
					[
						#wrappedJSONSuccessResponse & {
							#body_type: op.body_type
						},
					],
					[
						for template in #resourceErrorTemplates {
							#wrappedJSONResponse & {
								#status_code: template.status_code
								#description: template.description
								#schema_ref:  "Error"
								#body_type:   op.body_type
							}
						},
					],
				])
				extensions: #authenticatedExtensions & {
					#cli_command: op.cli_command
				}
			}
		},
	],
	[
		for op in #noContentMutatingOps {
			{
				method:       op.method
				path:         op.path
				operation_id: op.operation_id
				summary:      op.summary
				tags:         [#lineageTag]
				parameters:   op.parameters
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
					#cli_command: op.cli_command
				}
			}
		},
	],
	[
		for op in #wrappedMutatingOps {
			{
				method:       op.method
				path:         op.path
				operation_id: op.operation_id
				summary:      op.summary
				tags:         [#lineageTag]
				request_body: op.request_body
				responses: list.Concat([
					[
						#wrappedJSONSuccessResponse & {
							#body_type: op.body_type
						},
					],
					[
						for template in #mutatingErrorTemplates {
							#wrappedJSONResponse & {
								#status_code: template.status_code
								#description: template.description
								#schema_ref:  "Error"
								#body_type:   op.body_type
							}
						},
					],
				])
				extensions: #authenticatedExtensions & {
					#cli_command: op.cli_command
				}
			}
		},
	],
])
