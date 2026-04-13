package api

// Authored project operations.

#projectsTag: "Projects"

#plainProjectOperation: #genericOperationSpec & {
	wrapped: false
}

#projectIDPathParameter: #pathStringParameter & {
	#name: "project_id"
}

#projectWorkspaceIDPathParameter: #pathStringParameter & {
	#name: "workspace_id"
}

#projectPathParameters: [
	#projectIDPathParameter,
]

#environmentIDPathParameter: #pathStringParameter & {
	#name: "environment_id"
}

#workspaceProjectPathParameters: [
	#projectWorkspaceIDPathParameter,
]

#projectEnvironmentPathParameters: [
	#projectIDPathParameter,
	#environmentIDPathParameter,
]

#workspaceProjectListParameters: [
	#projectWorkspaceIDPathParameter,
	#paginationParameters[0],
	#paginationParameters[1],
]

#projectListParameters: [
	#projectIDPathParameter,
	#paginationParameters[0],
	#paginationParameters[1],
]

#projectOps: [
	#plainProjectOperation & {
		kind:         "response"
		method:       "get"
		op:           "listWorkspaceProjects"
		path:         "/workspaces/{workspace_id}/projects"
		summary:      "List projects in a workspace"
		returns:      "PaginatedProjects"
		error_family: "resource"
		params:       #workspaceProjectListParameters
	},
	#plainProjectOperation & {
		kind:           "response"
		method:         "post"
		op:             "createWorkspaceProject"
		path:           "/workspaces/{workspace_id}/projects"
		summary:        "Create project in a workspace"
		returns:        "Project"
		success_status: 201
		error_family:   "resource_conflict"
		params:         #workspaceProjectPathParameters
		body_ref:       "CreateProjectRequest"
		body_description: "Request payload"
	},
	#plainProjectOperation & {
		kind:         "response"
		method:       "get"
		op:           "getProject"
		path:         "/projects/{project_id}"
		summary:      "Get project"
		returns:      "Project"
		error_family: "resource"
		params:       #projectPathParameters
	},
	#plainProjectOperation & {
		kind:         "response"
		method:       "patch"
		op:           "updateProject"
		path:         "/projects/{project_id}"
		summary:      "Update project"
		returns:      "Project"
		error_family: "resource_conflict"
		params:       #projectPathParameters
		body_ref:     "UpdateProjectRequest"
		body_description: "Request payload"
	},
	#plainProjectOperation & {
		kind:         "no_content"
		method:       "delete"
		op:           "deleteProject"
		path:         "/projects/{project_id}"
		summary:      "Delete project"
		error_family: "resource_conflict"
		params:       #projectPathParameters
	},
	#plainProjectOperation & {
		kind:         "response"
		method:       "get"
		op:           "listProjectEnvironments"
		path:         "/projects/{project_id}/environments"
		summary:      "List project environments"
		returns:      "PaginatedEnvironments"
		error_family: "resource"
		params:       #projectListParameters
	},
	#plainProjectOperation & {
		kind:           "response"
		method:         "post"
		op:             "createProjectEnvironment"
		path:           "/projects/{project_id}/environments"
		summary:        "Create project environment"
		returns:        "Environment"
		success_status: 201
		error_family:   "resource_conflict"
		params:         #projectPathParameters
		body_ref:       "CreateEnvironmentRequest"
		body_description: "Request payload"
	},
	#plainProjectOperation & {
		kind:         "response"
		method:       "patch"
		op:           "updateProjectEnvironment"
		path:         "/projects/{project_id}/environments/{environment_id}"
		summary:      "Update project environment"
		returns:      "Environment"
		error_family: "resource_conflict"
		params:       #projectEnvironmentPathParameters
		body_ref:     "UpdateEnvironmentRequest"
		body_description: "Request payload"
	},
	#plainProjectOperation & {
		kind:         "no_content"
		method:       "delete"
		op:           "deleteProjectEnvironment"
		path:         "/projects/{project_id}/environments/{environment_id}"
		summary:      "Delete project environment"
		error_family: "resource_conflict"
		params:       #projectEnvironmentPathParameters
	},
	#plainProjectOperation & {
		kind:         "response"
		method:       "get"
		op:           "listProjectBuilds"
		path:         "/projects/{project_id}/builds"
		summary:      "List project builds"
		returns:      "PaginatedBuilds"
		error_family: "resource"
		params:       #projectListParameters
	},
	#plainProjectOperation & {
		kind:           "response"
		method:         "post"
		op:             "createProjectBuild"
		path:           "/projects/{project_id}/builds"
		summary:        "Create project build"
		returns:        "Build"
		success_status: 201
		error_family:   "resource_conflict"
		params:         #projectPathParameters
		body_ref:       "CreateBuildRequest"
		body_description: "Request payload"
	},
]

endpoints_projects: [
	for op in #projectOps {
		(#endpointFromGenericOperation & {
			tag:  #projectsTag
			spec: op
		}).endpoint
	},
]
