package api

import "list"

// Authored integration operations.

#integrationsTag: "Integrations"

#wrappedIntegrationOperation: #genericOperationSpec & {
	wrapped: true
}

#gitRepoIDPathParameter: #pathStringParameter & {
	#name: "git_repo_id"
}

#gitRepoPathParameters: [
	#gitRepoIDPathParameter,
]

#deleteGitRepoOperation: #genericOperationSpec & {
	wrapped: false
	kind:    "no_content"
	method:  "delete"
	op:      "deleteGitRepo"
	path:    "/git-repos/{git_repo_id}"
	summary: "Delete git repo"
	cli: {
		command: ["notebooks", "git-repos", "delete"]
	}
	error_family: "resource"
	params:  #gitRepoPathParameters
}

#integrationOps: [
	#wrappedIntegrationOperation & {
		kind:         "response"
		method:       "get"
		op:           "listGitRepos"
		path:         "/git-repos"
		summary:      "List git repos"
		cli: {
			command: ["notebooks", "git-repos", "list"]
		}
		returns:      "PaginatedGitRepos"
		error_family: "standard"
		params:       #paginationParameters
	},
	#wrappedIntegrationOperation & {
		kind:           "response"
		method:         "post"
		op:             "createGitRepo"
		path:           "/git-repos"
		summary:        "Create git repo"
		cli: {
			command: ["notebooks", "git-repos", "create"]
		}
		returns:        "GitRepo"
		success_status: 201
		error_family:   "mutating"
		body_ref:       "CreateGitRepoRequest"
		body_description: "Request payload"
	},
	#wrappedIntegrationOperation & {
		kind:         "response"
		method:       "get"
		op:           "getGitRepo"
		path:         "/git-repos/{git_repo_id}"
		summary:      "Get git repo"
		cli: {
			command: ["notebooks", "git-repos", "get"]
		}
		returns:      "GitRepo"
		error_family: "resource"
		params:       #gitRepoPathParameters
	},
	#deleteGitRepoOperation,
]

endpoints_integrations: list.Concat([
	[
		for op in #integrationOps {
			(#endpointFromGenericOperation & {
				tag:  #integrationsTag
				spec: op
			}).endpoint
		},
	],
	[
		{
			method:       "post"
			path:         "/git-repos/{git_repo_id}/sync-runs"
			operation_id: "syncGitRepo"
			summary:      "Sync git repo"
			tags:         [#integrationsTag]
			parameters:   #gitRepoPathParameters
			responses: list.Concat([
				[
					#wrappedJSONSuccessResponse & {
						#body_type: "GitSyncResult"
					},
				],
				[
					for template in #resourceErrorTemplates {
						#wrappedJSONResponse & {
							#status_code: template.status_code
							#description: template.description
							#schema_ref:  "Error"
							#body_type:   "GitSyncResult"
						}
					},
				],
				[
					#wrappedJSONResponse & {
						#status_code: 501
						#description: "Server error"
						#schema_ref:  "Error"
						#body_type:   "GitSyncResult"
					},
				],
			])
			extensions: #authenticatedExtensions
			cli: {
				command: ["notebooks", "git-repos", "sync"]
			}
		},
	],
])
