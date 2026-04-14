package api

// Authored health operations.

#healthTag: "Health"

#publicSecurity: [
	{},
]

#healthOps: [
	#genericOperationSpec & {
		kind:          "response"
		method:        "get"
		op:            "getHealth"
		path:          "/healthz"
		summary:       "Get health"
		returns:       "HealthResponse"
		wrapped:       false
		error_family:  "standard"
		authz_default: false
		authz: {
			mode: "none"
		}
		security: #publicSecurity
	},
]

endpoints_health: [
	for op in #healthOps {
		(#endpointFromGenericOperation & {
			tag:  #healthTag
			spec: op
		}).endpoint
	},
]
