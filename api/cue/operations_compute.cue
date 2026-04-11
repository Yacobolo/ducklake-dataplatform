package api

import "list"

// Authored compute operations.

#computeTag: "Compute"

#wrappedComputeOperation: #genericOperationSpec & {
	wrapped: true
}

#endpointNamePathParameter: #pathStringParameter & {
	#name: "endpoint_name"
}

#assignmentIDPathParameter: #pathStringParameter & {
	#name: "assignment_id"
}

#computeEndpointPathParameters: [
	#endpointNamePathParameter,
]

#computeAssignmentPathParameters: [
	#endpointNamePathParameter,
	#assignmentIDPathParameter,
]

#computeAssignmentListParameters: [
	#endpointNamePathParameter,
	#paginationParameters[0],
	#paginationParameters[1],
]

#adminOnlyComputeAuthz: {
	mode: "admin_only"
}

#createComputeEndpointAuthz: {
	mode: "privilege"
	checks: [
		{
			securable_type:     "catalog"
			privilege:          "MANAGE_COMPUTE"
			securable_id_source: "catalog_sentinel"
		},
	]
}

#manageComputeEndpointAuthz: {
	mode: "privilege"
	checks: [
		{
			securable_type:     "compute_endpoint"
			privilege:          "MANAGE_COMPUTE"
			securable_id_source: "runtime_resolved_object_id"
		},
	]
}

#deleteComputeAssignmentAuthz: {
	mode: "privilege"
	checks: [
		{
			securable_type:     "catalog"
			privilege:          "MANAGE_COMPUTE"
			securable_id_source: "catalog_sentinel"
		},
	]
}

#computeOps: [
	#wrappedComputeOperation & {
		kind:          "response"
		method:        "get"
		op:            "listComputeEndpoints"
		path:          "/compute-endpoints"
		summary:       "List compute endpoints"
		cli:           "compute endpoints list"
		returns:       "PaginatedComputeEndpoints"
		error_family:  "guarded_read"
		params:        #paginationParameters
		authz_default: false
		authz:         #adminOnlyComputeAuthz
	},
	#wrappedComputeOperation & {
		kind:           "response"
		method:         "post"
		op:             "createComputeEndpoint"
		path:           "/compute-endpoints"
		summary:        "Create compute endpoint"
		description:    "Registers a compute endpoint that can execute remote workloads and accept assignment-based routing."
		cli:            "compute endpoints create"
		returns:        "ComputeEndpoint"
		success_status: 201
		error_family:   "resource"
		body_ref:       "CreateComputeEndpointRequest"
		body_description: "Request payload"
		authz_default:   false
		authz:           #createComputeEndpointAuthz
		response_any_of: {
			"400": [{ref: "Error"}, {ref: "Error"}]
		}
	},
	#wrappedComputeOperation & {
		kind:          "response"
		method:        "get"
		op:            "getComputeEndpoint"
		path:          "/compute-endpoints/{endpoint_name}"
		summary:       "Get compute endpoint"
		cli:           "compute endpoints get"
		returns:       "ComputeEndpoint"
		error_family:  "resource"
		params:        #computeEndpointPathParameters
		authz_default: false
	},
	#wrappedComputeOperation & {
		kind:          "response"
		method:        "patch"
		op:            "updateComputeEndpoint"
		path:          "/compute-endpoints/{endpoint_name}"
		summary:       "Update compute endpoint"
		cli:           "compute endpoints update"
		returns:       "ComputeEndpoint"
		error_family:  "resource"
		params:        #computeEndpointPathParameters
		body_ref:      "UpdateComputeEndpointRequest"
		body_description: "Request payload"
		authz_default:   false
		authz:           #manageComputeEndpointAuthz
	},
	#genericOperationSpec & {
		wrapped:       false
		kind:          "no_content"
		method:        "delete"
		op:            "deleteComputeEndpoint"
		path:          "/compute-endpoints/{endpoint_name}"
		summary:       "Delete compute endpoint"
		cli:           "compute endpoints delete"
		error_family:  "resource"
		params:        #computeEndpointPathParameters
		authz_default: false
		authz:         #manageComputeEndpointAuthz
	},
	#wrappedComputeOperation & {
		kind:          "response"
		method:        "get"
		op:            "listComputeAssignments"
		path:          "/compute-endpoints/{endpoint_name}/assignments"
		summary:       "List compute assignments"
		cli:           "compute assignments list"
		returns:       "PaginatedComputeAssignments"
		error_family:  "resource"
		params:        #computeAssignmentListParameters
		authz_default: false
	},
	#wrappedComputeOperation & {
		kind:           "response"
		method:         "post"
		op:             "createComputeAssignment"
		path:           "/compute-endpoints/{endpoint_name}/assignments"
		summary:        "Create compute assignment"
		cli:            "compute assignments create"
		returns:        "ComputeAssignment"
		success_status: 201
		error_family:   "resource"
		params:         #computeEndpointPathParameters
		body_ref:       "CreateComputeAssignmentRequest"
		body_description: "Request payload"
		authz_default:   false
		authz:           #manageComputeEndpointAuthz
		response_any_of: {
			"400": [{ref: "Error"}, {ref: "Error"}]
		}
	},
	#genericOperationSpec & {
		wrapped:       false
		kind:          "no_content"
		method:        "delete"
		op:            "deleteComputeAssignment"
		path:          "/compute-endpoints/{endpoint_name}/assignments/{assignment_id}"
		summary:       "Delete compute assignment"
		cli:           "compute assignments delete"
		error_family:  "resource"
		params:        #computeAssignmentPathParameters
		authz_default: false
		authz:         #deleteComputeAssignmentAuthz
	},
	#wrappedComputeOperation & {
		kind:          "response"
		method:        "get"
		op:            "getComputeRoutingDefaults"
		path:          "/compute-routing-defaults"
		summary:       "Get compute routing defaults"
		returns:       "ComputeRoutingDefaults"
		error_family:  "guarded_read"
		authz_default: false
		authz:         #adminOnlyComputeAuthz
	},
	#wrappedComputeOperation & {
		kind:          "response"
		method:        "patch"
		op:            "updateComputeRoutingDefaults"
		path:          "/compute-routing-defaults"
		summary:       "Update compute routing defaults"
		returns:       "ComputeRoutingDefaults"
		error_family:  "mutating"
		body_ref:      "ComputeRoutingDefaults"
		body_description: "Request payload"
		authz_default: false
		authz:         #adminOnlyComputeAuthz
		response_any_of: {
			"400": [{ref: "Error"}, {ref: "Error"}]
		}
	},
]

endpoints_compute: list.Concat([
	[
		for op in #computeOps {
			(#endpointFromGenericOperation & {
				tag:  #computeTag
				spec: op
			}).endpoint
		},
	],
	[
		{
			method:       "get"
			path:         "/compute-endpoints/{endpoint_name}/health"
			operation_id: "getComputeEndpointHealth"
			summary:      "Get compute endpoint health"
			tags:         [#computeTag]
			parameters:   #computeEndpointPathParameters
			responses: list.Concat([
				[
					#wrappedJSONSuccessResponse & {
						#body_type: "ComputeEndpointHealth"
					},
				],
				[
					for template in #resourceErrorTemplates {
						#wrappedJSONResponse & {
							#status_code: template.status_code
							#description: template.description
							#schema_ref:  "Error"
							#body_type:   "ComputeEndpointHealth"
							if template.status_code == 400 {
								any_of: [{ref: "Error"}, {ref: "Error"}]
							}
						}
					},
				],
				[
					#wrappedJSONResponse & {
						#status_code: 502
						#description: "Server error"
						#schema_ref:  "Error"
						#body_type:   "ComputeEndpointHealth"
					},
				],
			])
			extensions: #authenticatedExtensions & {
				#cli_command: "compute endpoints health"
			}
		},
	],
])
