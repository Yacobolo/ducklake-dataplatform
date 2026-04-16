package api

// Authored resource operations.

#resourcesTag: "Resources"

#resourceTypePathParameter: #pathStringParameter & {
	#name: "resource_type"
}

#resourceKeyPathParameter: #pathStringParameter & {
	#name: "resource_key"
}

#savedResourcePathParameters: [
	#resourceTypePathParameter,
	#resourceKeyPathParameter,
]

#plainResourceOperation: #genericOperationSpec & {
	wrapped: false
}

#resourceOps: [
	#plainResourceOperation & {
		kind:         "response"
		method:       "get"
		op:           "listRecentResources"
		path:         "/me/recent-resources"
		summary:      "List recent resources"
		description:  "Lists the authenticated principal's recent UUID-backed resources for personalized navigation."
		cli:          "me recent-resources list"
		returns:      "PaginatedRecentResources"
		error_family: "mutating"
		params:       #paginationParameters
	},
	#plainResourceOperation & {
		kind:         "response"
		method:       "get"
		op:           "listSavedResources"
		path:         "/me/saved-resources"
		summary:      "List saved resources"
		description:  "Lists the authenticated principal's saved UUID-backed resources."
		cli:          "me saved-resources list"
		returns:      "PaginatedSavedResources"
		error_family: "mutating"
		params:       #paginationParameters
	},
	#plainResourceOperation & {
		kind:             "response"
		method:           "post"
		op:               "createSavedResource"
		path:             "/me/saved-resources"
		summary:          "Save resource"
		description:      "Saves a UUID-backed resource for the authenticated principal."
		cli:              "me saved-resources create"
		returns:          "SavedResource"
		success_status:   201
		error_family:     "mutating"
		body_ref:         "CreateSavedResourceRequest"
		body_description: "Request payload"
	},
	#plainResourceOperation & {
		kind:         "no_content"
		method:       "delete"
		op:           "deleteSavedResource"
		path:         "/me/saved-resources/{resource_type}/{resource_key}"
		summary:      "Delete saved resource"
		description:  "Removes a saved resource for the authenticated principal."
		cli:          "me saved-resources delete"
		error_family: "mutating"
		params:       #savedResourcePathParameters
	},
]

endpoints_resources: [
	for op in #resourceOps {
		(#endpointFromGenericOperation & {
			tag:  #resourcesTag
			spec: op
		}).endpoint
	},
]
