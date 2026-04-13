package api

// Authored workspace operations.

#workspacesTag: "Workspaces"

#plainWorkspaceOperation: #genericOperationSpec & {
	wrapped: false
}

#workspaceIDPathParameter: #pathStringParameter & {
	#name: "workspace_id"
}

#workspacePrincipalNamePathParameter: #pathStringParameter & {
	#name: "principal_name"
}

#workspacePathParameters: [
	#workspaceIDPathParameter,
]

#workspaceMemberPathParameters: [
	#workspaceIDPathParameter,
	#workspacePrincipalNamePathParameter,
]

#workspaceOps: [
	#plainWorkspaceOperation & {
		kind:         "response"
		method:       "get"
		op:           "listWorkspaces"
		path:         "/workspaces"
		summary:      "List workspaces"
		returns:      "PaginatedWorkspaces"
		error_family: "guarded_read"
		params:       #paginationParameters
	},
	#plainWorkspaceOperation & {
		kind:           "response"
		method:         "post"
		op:             "createWorkspace"
		path:           "/workspaces"
		summary:        "Create workspace"
		returns:        "Workspace"
		success_status: 201
		error_family:   "mutating_conflict"
		body_ref:       "CreateWorkspaceRequest"
		body_description: "Request payload"
	},
	#plainWorkspaceOperation & {
		kind:         "response"
		method:       "get"
		op:           "getWorkspace"
		path:         "/workspaces/{workspace_id}"
		summary:      "Get workspace"
		returns:      "Workspace"
		error_family: "resource"
		params:       #workspacePathParameters
	},
	#plainWorkspaceOperation & {
		kind:         "response"
		method:       "patch"
		op:           "updateWorkspace"
		path:         "/workspaces/{workspace_id}"
		summary:      "Update workspace"
		returns:      "Workspace"
		error_family: "resource_conflict"
		params:       #workspacePathParameters
		body_ref:     "UpdateWorkspaceRequest"
		body_description: "Request payload"
	},
	#plainWorkspaceOperation & {
		kind:         "no_content"
		method:       "delete"
		op:           "deleteWorkspace"
		path:         "/workspaces/{workspace_id}"
		summary:      "Delete workspace"
		error_family: "resource_conflict"
		params:       #workspacePathParameters
	},
	#plainWorkspaceOperation & {
		kind:         "response"
		method:       "get"
		op:           "listWorkspaceMembers"
		path:         "/workspaces/{workspace_id}/members"
		summary:      "List workspace members"
		error_family: "resource"
		params:       #workspacePathParameters
		success_schema: {
			type: "array"
			items: {
				ref: "WorkspaceMember"
			}
		}
	},
	#plainWorkspaceOperation & {
		kind:         "response"
		method:       "post"
		op:           "addWorkspaceMember"
		path:         "/workspaces/{workspace_id}/members"
		summary:      "Add or update workspace member"
		returns:      "WorkspaceMember"
		error_family: "resource_conflict"
		params:       #workspacePathParameters
		body_ref:     "AddWorkspaceMemberRequest"
		body_description: "Request payload"
	},
	#plainWorkspaceOperation & {
		kind:         "no_content"
		method:       "delete"
		op:           "removeWorkspaceMember"
		path:         "/workspaces/{workspace_id}/members/{principal_name}"
		summary:      "Remove workspace member"
		error_family: "resource_conflict"
		params:       #workspaceMemberPathParameters
	},
]

endpoints_workspaces: [
	for op in #workspaceOps {
		(#endpointFromGenericOperation & {
			tag:  #workspacesTag
			spec: op
		}).endpoint
	},
]
