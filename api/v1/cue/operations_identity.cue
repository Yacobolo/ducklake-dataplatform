package api

// Authored identity operations.

#identityTag: "Identity"

#wrappedIdentityOperation: #genericOperationSpec & {
	wrapped: true
}

#plainIdentityOperation: #genericOperationSpec & {
	wrapped: false
}

#principalIDPathParameter: #pathStringParameter & {
	#name: "principal_id"
}

#groupIDPathParameter: #pathStringParameter & {
	#name: "group_id"
}

#memberIDPathParameter: #pathStringParameter & {
	#name: "member_id"
}

#apiKeyIDPathParameter: #pathStringParameter & {
	#name: "api_key_id"
}

#memberTypePathParameter: {
	name:     "member_type"
	in:       "path"
	required: true
	schema: {
		ref: "PrincipalType"
	}
}

#principalIDQueryParameter: #queryStringParameter & {
	#name: "principal_id"
}

#principalPathParameters: [
	#principalIDPathParameter,
]

#groupPathParameters: [
	#groupIDPathParameter,
]

#groupMemberListParameters: [
	#groupIDPathParameter,
	#paginationParameters[0],
	#paginationParameters[1],
]

#groupMemberPathParameters: [
	#groupIDPathParameter,
	#memberTypePathParameter,
	#memberIDPathParameter,
]

#apiKeyPathParameters: [
	#apiKeyIDPathParameter,
]

#listAPIKeysParameters: [
	#principalIDQueryParameter,
	#paginationParameters[0],
	#paginationParameters[1],
]

#adminOnlyIdentityAuthz: {
	mode: "admin_only"
}

#identityOps: [
	#wrappedIdentityOperation & {
		kind:         "response"
		method:       "get"
		op:           "listPrincipals"
		path:         "/principals"
		summary:      "List principals"
		cli: {
			command: ["security", "principals", "list"]
		}
		returns:      "PaginatedPrincipals"
		error_family: "guarded_read"
		params:       #paginationParameters
		authz:        #adminOnlyIdentityAuthz
	},
	#wrappedIdentityOperation & {
		kind:           "response"
		method:         "post"
		op:             "createPrincipal"
		path:           "/principals"
		summary:        "Create principal"
		cli: {
			command: ["security", "principals", "create"]
		}
		returns:        "Principal"
		success_status: 201
		error_family:   "mutating"
		body_ref:       "CreatePrincipalRequest"
		body_description: "Request payload"
	},
	#wrappedIdentityOperation & {
		kind:         "response"
		method:       "get"
		op:           "getPrincipal"
		path:         "/principals/{principal_id}"
		summary:      "Get principal"
		cli: {
			command: ["security", "principals", "get"]
		}
		returns:      "Principal"
		error_family: "resource"
		params:       #principalPathParameters
	},
	#plainIdentityOperation & {
		kind:         "no_content"
		method:       "delete"
		op:           "deletePrincipal"
		path:         "/principals/{principal_id}"
		summary:      "Delete principal"
		cli: {
			command: ["security", "principals", "delete"]
		}
		error_family: "mutating"
		params:       #principalPathParameters
	},
	#plainIdentityOperation & {
		kind:         "response"
		method:       "patch"
		op:           "updatePrincipal"
		path:         "/principals/{principal_id}"
		summary:      "Update principal"
		cli: {
			command: ["security", "principals", "set-admin"]
		}
		returns:      "Principal"
		error_family: "mutating"
		params:       #principalPathParameters
		body_ref:     "UpdatePrincipalRequest"
		body_description: "Request payload"
	},
	#wrappedIdentityOperation & {
		kind:         "response"
		method:       "get"
		op:           "listGroups"
		path:         "/groups"
		summary:      "List groups"
		cli: {
			command: ["security", "groups", "list"]
		}
		returns:      "PaginatedGroups"
		error_family: "guarded_read"
		params:       #paginationParameters
		authz:        #adminOnlyIdentityAuthz
	},
	#wrappedIdentityOperation & {
		kind:           "response"
		method:         "post"
		op:             "createGroup"
		path:           "/groups"
		summary:        "Create group"
		cli: {
			command: ["security", "groups", "create"]
		}
		returns:        "Group"
		success_status: 201
		error_family:   "mutating"
		body_ref:       "CreateGroupRequest"
		body_description: "Request payload"
	},
	#wrappedIdentityOperation & {
		kind:         "response"
		method:       "get"
		op:           "getGroup"
		path:         "/groups/{group_id}"
		summary:      "Get group"
		cli: {
			command: ["security", "groups", "get"]
		}
		returns:      "Group"
		error_family: "resource"
		params:       #groupPathParameters
	},
	#plainIdentityOperation & {
		kind:         "response"
		method:       "patch"
		op:           "updateGroup"
		path:         "/groups/{group_id}"
		summary:      "Update group"
		returns:      "Group"
		error_family: "resource"
		params:       #groupPathParameters
		body_ref:     "UpdateGroupRequest"
		body_description: "Request payload"
		response_any_of: {
			"400": [
				{ref: "Error"},
				{ref: "Error"},
			]
		}
	},
	#plainIdentityOperation & {
		kind:         "no_content"
		method:       "delete"
		op:           "deleteGroup"
		path:         "/groups/{group_id}"
		summary:      "Delete group"
		cli: {
			command: ["security", "groups", "delete"]
		}
		error_family: "mutating"
		params:       #groupPathParameters
	},
	#wrappedIdentityOperation & {
		kind:         "response"
		method:       "get"
		op:           "listGroupMembers"
		path:         "/groups/{group_id}/members"
		summary:      "List group members"
		cli: {
			command: ["security", "members", "list"]
		}
		returns:      "PaginatedGroupMembers"
		error_family: "resource"
		params:       #groupMemberListParameters
	},
	#plainIdentityOperation & {
		kind:           "created_empty"
		method:         "post"
		op:             "createGroupMember"
		path:           "/groups/{group_id}/members"
		summary:        "Create group member"
		cli: {
			command: ["security", "members", "add"]
		}
		error_family:   "mutating"
		params:         #groupPathParameters
		body_ref:       "CreateGroupMemberRequest"
		body_description: "Request payload"
	},
	#plainIdentityOperation & {
		kind:         "no_content"
		method:       "delete"
		op:           "deleteGroupMember"
		path:         "/groups/{group_id}/members/{member_type}/{member_id}"
		summary:      "Delete group member"
		cli: {
			command: ["security", "members", "remove"]
		}
		error_family: "mutating"
		params:       #groupMemberPathParameters
	},
	#wrappedIdentityOperation & {
		kind:         "response"
		method:       "get"
		op:           "listAPIKeys"
		path:         "/api-keys"
		summary:      "List API keys"
		cli: {
			command: ["security", "api-keys", "list"]
		}
		returns:      "PaginatedAPIKeys"
		error_family: "standard"
		params:       #listAPIKeysParameters
	},
	#wrappedIdentityOperation & {
		kind:           "response"
		method:         "post"
		op:             "createAPIKey"
		path:           "/api-keys"
		summary:        "Create API key"
		cli: {
			command: ["security", "api-keys", "create"]
		}
		returns:        "CreateAPIKeyResponse"
		success_status: 201
		error_family:   "mutating"
		body_ref:       "CreateAPIKeyRequest"
		body_description: "Request payload"
	},
	#plainIdentityOperation & {
		kind:         "no_content"
		method:       "delete"
		op:           "deleteAPIKey"
		path:         "/api-keys/{api_key_id}"
		summary:      "Delete API key"
		cli: {
			command: ["security", "api-keys", "delete"]
		}
		error_family: "mutating"
		params:       #apiKeyPathParameters
	},
	#wrappedIdentityOperation & {
		kind:         "response"
		method:       "post"
		op:           "cleanupExpiredAPIKeys"
		path:         "/api-key-cleanup-runs"
		summary:      "Clean up expired API keys"
		cli: {
			command: ["security", "api-keys", "cleanup"]
		}
		returns:      "CleanupAPIKeysResponse"
		error_family: "mutating"
	},
]

endpoints_identity: [
	for op in #identityOps {
		(#endpointFromGenericOperation & {
			tag:  #identityTag
			spec: op
		}).endpoint
	},
]
