package api

// Authored audit operations.

#auditTag: "Audit"

#wrappedAuditOperation: #genericOperationSpec & {
	wrapped: true
}

#auditPrincipalNameQueryParameter: #queryStringParameter & {
	#name: "principal_name"
}

#auditActionQueryParameter: #queryStringParameter & {
	#name: "action"
}

#auditDecisionStatusQueryParameter: {
	name:    "status"
	in:      "query"
	explode: false
	schema: {
		ref: "AuditDecisionStatus"
	}
}

#auditListParameters: [
	#auditPrincipalNameQueryParameter,
	#auditActionQueryParameter,
	#auditDecisionStatusQueryParameter,
	#paginationParameters[0],
	#paginationParameters[1],
]

endpoints_audit: [
	(#endpointFromGenericOperation & {
		tag: #auditTag
		spec: #wrappedAuditOperation & {
			kind:         "response"
			method:       "get"
			op:           "listAuditLogs"
			path:         "/audit-entries"
			summary:      "List audit entries"
			cli:          "audit entries list"
			returns:      "PaginatedAuditLogs"
			error_family: "guarded_read"
			params:       #auditListParameters
			authz: {
				mode: "admin_only"
			}
		}
	}).endpoint,
]
