package api

// Authored governance operations.

#governanceTag: "Governance"

#grantIDPathParameter: #pathStringParameter & {
	#name: "grant_id"
}

#rowFilterIDPathParameter: #pathStringParameter & {
	#name: "row_filter_id"
}

#columnMaskIDPathParameter: #pathStringParameter & {
	#name: "column_mask_id"
}

#principalIDPathParameter: #pathStringParameter & {
	#name: "principal_id"
}

#tagIDPathParameter: #pathStringParameter & {
	#name: "tag_id"
}

#assignmentIDPathParameter: #pathStringParameter & {
	#name: "assignment_id"
}

#principalIDQueryParameter: #queryStringParameter & {
	#name: "principal_id"
}

#principalTypeQueryParameter: {
	name:    "principal_type"
	in:      "query"
	explode: false
	schema: {
		ref: "PrincipalType"
	}
}

#principalTypePathParameter: {
	name:     "principal_type"
	in:       "path"
	required: true
	schema: {
		ref: "PrincipalType"
	}
}

#tableIDQueryParameter: #queryStringParameter & {
	#name: "table_id"
}

#securableIDQueryParameter: #queryStringParameter & {
	#name: "securable_id"
}

#securableTypeQueryParameter: #queryStringParameter & {
	#name: "securable_type"
}

#listGrantsParameters: [
	#principalIDQueryParameter,
	#principalTypeQueryParameter,
	#securableTypeQueryParameter,
	#securableIDQueryParameter,
	#paginationParameters[0],
	#paginationParameters[1],
]

#tableScopedListParameters: [
	#tableIDQueryParameter,
	#paginationParameters[0],
	#paginationParameters[1],
]

#tagListParameters: #paginationParameters

#rowFilterBindingListParameters: [
	#rowFilterIDPathParameter,
	#paginationParameters[0],
	#paginationParameters[1],
]

#columnMaskBindingListParameters: [
	#columnMaskIDPathParameter,
	#paginationParameters[0],
	#paginationParameters[1],
]

#tagAssignmentListParameters: [
	#tagIDPathParameter,
	#paginationParameters[0],
	#paginationParameters[1],
]

#rowFilterBindingPathParameters: [
	#rowFilterIDPathParameter,
	#principalTypePathParameter,
	#principalIDPathParameter,
]

#columnMaskBindingPathParameters: [
	#columnMaskIDPathParameter,
	#principalTypePathParameter,
	#principalIDPathParameter,
]

#tagAssignmentPathParameters: [
	#tagIDPathParameter,
	#assignmentIDPathParameter,
]

#adminOnlyGovernanceAuthz: {
	mode: "admin_only"
}

#governanceOps: [
	#genericOperationSpec & {
		kind:          "response"
		method:        "get"
		op:            "listGrants"
		path:          "/grants"
		summary:       "List grants"
		cli:           "security grants list"
		returns:       "PaginatedGrants"
		error_family:  "guarded_read"
		params:        #listGrantsParameters
		authz_default: false
		authz:         #adminOnlyGovernanceAuthz
	},
	#genericOperationSpec & {
		kind:           "response"
		method:         "post"
		op:             "createGrant"
		path:           "/grants"
		summary:        "Create grant"
		cli:            "security grants create"
		returns:        "PrivilegeGrant"
		success_status: 201
		error_family:   "resource"
		body_ref:       "CreateGrantRequest"
		body_description: "Request payload"
		authz_default:   false
		authz:           #adminOnlyGovernanceAuthz
		response_any_of: {
			"400": [{ref: "Error"}, {ref: "Error"}]
		}
	},
	#genericOperationSpec & {
		kind:          "no_content"
		method:        "delete"
		op:            "deleteGrant"
		path:          "/grants/{grant_id}"
		summary:       "Delete grant"
		cli:           "security grants revoke"
		error_family:  "mutating"
		params: [
			#grantIDPathParameter,
		]
		authz_default: false
		authz:         #adminOnlyGovernanceAuthz
	},
	#genericOperationSpec & {
		kind:          "response"
		method:        "get"
		op:            "listRowFilters"
		path:          "/row-filters"
		summary:       "List row filters"
		cli:           "security row-filters list"
		returns:       "PaginatedRowFilters"
		error_family:  "resource"
		params:        #tableScopedListParameters
		authz_default: false
	},
	#genericOperationSpec & {
		kind:           "response"
		method:         "post"
		op:             "createRowFilter"
		path:           "/row-filters"
		summary:        "Create row filter"
		cli:            "security row-filters create"
		returns:        "RowFilter"
		success_status: 201
		error_family:   "mutating"
		body_ref:       "CreateRowFilterRequest"
		body_description: "Request payload"
		authz_default:   false
	},
	#genericOperationSpec & {
		kind:          "response"
		method:        "get"
		op:            "getRowFilter"
		path:          "/row-filters/{row_filter_id}"
		summary:       "Get row filter"
		returns:       "RowFilter"
		error_family:  "resource"
		params: [
			#rowFilterIDPathParameter,
		]
		authz_default: false
	},
	#genericOperationSpec & {
		kind:          "response"
		method:        "patch"
		op:            "updateRowFilter"
		path:          "/row-filters/{row_filter_id}"
		summary:       "Update row filter"
		returns:       "RowFilter"
		error_family:  "mutating"
		params: [
			#rowFilterIDPathParameter,
		]
		body_ref:       "UpdateRowFilterRequest"
		body_description: "Request payload"
		authz_default:   false
	},
	#genericOperationSpec & {
		kind:          "no_content"
		method:        "delete"
		op:            "deleteRowFilter"
		path:          "/row-filters/{row_filter_id}"
		summary:       "Delete row filter"
		cli:           "security row-filters delete"
		error_family:  "mutating"
		params: [
			#rowFilterIDPathParameter,
		]
		authz_default: false
	},
	#genericOperationSpec & {
		kind:          "response"
		method:        "get"
		op:            "listRowFilterBindings"
		path:          "/row-filters/{row_filter_id}/bindings"
		summary:       "List row filter bindings"
		returns:       "PaginatedRowFilterBindings"
		error_family:  "resource"
		params:        #rowFilterBindingListParameters
		authz_default: false
	},
	#genericOperationSpec & {
		kind:           "created_empty"
		method:         "post"
		op:             "bindRowFilter"
		path:           "/row-filters/{row_filter_id}/bindings"
		summary:        "Bind row filter"
		cli:            "security row-filters bind"
		error_family:   "mutating"
		params: [
			#rowFilterIDPathParameter,
		]
		body_ref:       "RowFilterBindingRequest"
		body_description: "Request payload"
		authz_default:   false
	},
	#genericOperationSpec & {
		kind:          "no_content"
		method:        "delete"
		op:            "unbindRowFilter"
		path:          "/row-filters/{row_filter_id}/bindings/{principal_type}/{principal_id}"
		summary:       "Unbind row filter"
		cli:           "security row-filters unbind"
		error_family:  "mutating"
		params:        #rowFilterBindingPathParameters
		authz_default: false
	},
	#genericOperationSpec & {
		kind:          "response"
		method:        "get"
		op:            "listColumnMasks"
		path:          "/column-masks"
		summary:       "List column masks"
		cli:           "security column-masks list"
		returns:       "PaginatedColumnMasks"
		error_family:  "resource"
		params:        #tableScopedListParameters
		authz_default: false
	},
	#genericOperationSpec & {
		kind:           "response"
		method:         "post"
		op:             "createColumnMask"
		path:           "/column-masks"
		summary:        "Create column mask"
		cli:            "security column-masks create"
		returns:        "ColumnMask"
		success_status: 201
		error_family:   "mutating"
		body_ref:       "CreateColumnMaskRequest"
		body_description: "Request payload"
		authz_default:   false
	},
	#genericOperationSpec & {
		kind:          "response"
		method:        "get"
		op:            "getColumnMask"
		path:          "/column-masks/{column_mask_id}"
		summary:       "Get column mask"
		returns:       "ColumnMask"
		error_family:  "resource"
		params: [
			#columnMaskIDPathParameter,
		]
		authz_default: false
	},
	#genericOperationSpec & {
		kind:          "response"
		method:        "patch"
		op:            "updateColumnMask"
		path:          "/column-masks/{column_mask_id}"
		summary:       "Update column mask"
		returns:       "ColumnMask"
		error_family:  "mutating"
		params: [
			#columnMaskIDPathParameter,
		]
		body_ref:       "UpdateColumnMaskRequest"
		body_description: "Request payload"
		authz_default:   false
	},
	#genericOperationSpec & {
		kind:          "no_content"
		method:        "delete"
		op:            "deleteColumnMask"
		path:          "/column-masks/{column_mask_id}"
		summary:       "Delete column mask"
		cli:           "security column-masks delete"
		error_family:  "mutating"
		params: [
			#columnMaskIDPathParameter,
		]
		authz_default: false
	},
	#genericOperationSpec & {
		kind:          "response"
		method:        "get"
		op:            "listColumnMaskBindings"
		path:          "/column-masks/{column_mask_id}/bindings"
		summary:       "List column mask bindings"
		returns:       "PaginatedColumnMaskBindings"
		error_family:  "resource"
		params:        #columnMaskBindingListParameters
		authz_default: false
	},
	#genericOperationSpec & {
		kind:           "created_empty"
		method:         "post"
		op:             "bindColumnMask"
		path:           "/column-masks/{column_mask_id}/bindings"
		summary:        "Bind column mask"
		cli:            "security column-masks bind"
		error_family:   "mutating"
		params: [
			#columnMaskIDPathParameter,
		]
		body_ref:       "ColumnMaskBindingRequest"
		body_description: "Request payload"
		authz_default:   false
	},
	#genericOperationSpec & {
		kind:          "no_content"
		method:        "delete"
		op:            "unbindColumnMask"
		path:          "/column-masks/{column_mask_id}/bindings/{principal_type}/{principal_id}"
		summary:       "Unbind column mask"
		cli:           "security column-masks unbind"
		error_family:  "mutating"
		params:        #columnMaskBindingPathParameters
		authz_default: false
	},
	#genericOperationSpec & {
		kind:          "response"
		method:        "get"
		op:            "listTags"
		path:          "/tags"
		summary:       "List tags"
		cli:           "governance tags list"
		returns:       "PaginatedTags"
		error_family:  "standard"
		params:        #tagListParameters
		authz_default: false
	},
	#genericOperationSpec & {
		kind:           "response"
		method:         "post"
		op:             "createTag"
		path:           "/tags"
		summary:        "Create tag"
		cli:            "governance tags create"
		returns:        "Tag"
		success_status: 201
		error_family:   "mutating"
		body_ref:       "CreateTagRequest"
		body_description: "Request payload"
		authz_default:   false
	},
	#genericOperationSpec & {
		kind:          "response"
		method:        "get"
		op:            "getTag"
		path:          "/tags/{tag_id}"
		summary:       "Get tag"
		returns:       "Tag"
		error_family:  "resource"
		params: [
			#tagIDPathParameter,
		]
		authz_default: false
	},
	#genericOperationSpec & {
		kind:          "response"
		method:        "patch"
		op:            "updateTag"
		path:          "/tags/{tag_id}"
		summary:       "Update tag"
		returns:       "Tag"
		error_family:  "mutating"
		params: [
			#tagIDPathParameter,
		]
		body_ref:       "UpdateTagRequest"
		body_description: "Request payload"
		authz_default:   false
	},
	#genericOperationSpec & {
		kind:          "no_content"
		method:        "delete"
		op:            "deleteTag"
		path:          "/tags/{tag_id}"
		summary:       "Delete tag"
		cli:           "governance tags delete"
		error_family:  "mutating"
		params: [
			#tagIDPathParameter,
		]
		authz_default: false
	},
	#genericOperationSpec & {
		kind:          "response"
		method:        "get"
		op:            "listTagAssignments"
		path:          "/tags/{tag_id}/assignments"
		summary:       "List tag assignments"
		returns:       "PaginatedTagAssignments"
		error_family:  "resource"
		params:        #tagAssignmentListParameters
		authz_default: false
	},
	#genericOperationSpec & {
		kind:           "response"
		method:         "post"
		op:             "createTagAssignment"
		path:           "/tags/{tag_id}/assignments"
		summary:        "Create tag assignment"
		cli:            "governance tag-assignments create"
		returns:        "TagAssignment"
		success_status: 201
		error_family:   "resource"
		params: [
			#tagIDPathParameter,
		]
		body_ref:       "CreateTagAssignmentRequest"
		body_description: "Request payload"
		authz_default:   false
		response_any_of: {
			"400": [{ref: "Error"}, {ref: "Error"}]
		}
	},
	#genericOperationSpec & {
		kind:          "no_content"
		method:        "delete"
		op:            "deleteTagAssignment"
		path:          "/tags/{tag_id}/assignments/{assignment_id}"
		summary:       "Delete tag assignment"
		cli:           "governance tag-assignments delete"
		error_family:  "mutating"
		params:        #tagAssignmentPathParameters
		authz_default: false
	},
	#genericOperationSpec & {
		kind:          "response"
		method:        "get"
		op:            "listClassifications"
		path:          "/classifications"
		summary:       "List classifications"
		cli:           "governance classifications list"
		returns:       "PaginatedTags"
		error_family:  "standard"
		params:        #tagListParameters
		authz_default: false
	},
]

endpoints_governance: [
	for op in #governanceOps {
		(#endpointFromGenericOperation & {
			tag:  #governanceTag
			spec: op
		}).endpoint
	},
]
