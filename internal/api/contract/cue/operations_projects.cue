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

#dependencyProjectPathParameter: #pathStringParameter & {
	#name: "dependency_project"
}

#sourceNamePathParameter: #pathStringParameter & {
	#name: "source_name"
}

#sourceTableNamePathParameter: #pathStringParameter & {
	#name: "table_name"
}

#seedNamePathParameter: #pathStringParameter & {
	#name: "seed_name"
}

#workspaceProjectPathParameters: [
	#projectWorkspaceIDPathParameter,
]

#projectEnvironmentPathParameters: [
	#projectIDPathParameter,
	#environmentIDPathParameter,
]

#projectDependencyPathParameters: [
	#projectIDPathParameter,
	#dependencyProjectPathParameter,
]

#projectSourcePathParameters: [
	#projectIDPathParameter,
	#sourceNamePathParameter,
	#sourceTableNamePathParameter,
]

#projectSeedPathParameters: [
	#projectIDPathParameter,
	#seedNamePathParameter,
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
	#plainProjectOperation & {
		kind:         "response"
		method:       "get"
		op:           "listProjectDependencies"
		path:         "/projects/{project_id}/dependencies"
		summary:      "List project dependencies"
		returns:      "PaginatedProjectDependencies"
		error_family: "resource"
		params:       #projectListParameters
	},
	#plainProjectOperation & {
		kind:           "response"
		method:         "post"
		op:             "createProjectDependency"
		path:           "/projects/{project_id}/dependencies"
		summary:        "Create project dependency"
		returns:        "ProjectDependency"
		success_status: 201
		error_family:   "resource_conflict"
		params:         #projectPathParameters
		body_ref:       "CreateProjectDependencyRequest"
		body_description: "Request payload"
	},
	#plainProjectOperation & {
		kind:         "no_content"
		method:       "delete"
		op:           "deleteProjectDependency"
		path:         "/projects/{project_id}/dependencies/{dependency_project}"
		summary:      "Delete project dependency"
		error_family: "resource_conflict"
		params:       #projectDependencyPathParameters
	},
	#plainProjectOperation & {
		kind:         "response"
		method:       "get"
		op:           "listProjectSources"
		path:         "/projects/{project_id}/sources"
		summary:      "List project sources"
		returns:      "PaginatedProjectSources"
		error_family: "resource"
		params:       #projectListParameters
	},
	#plainProjectOperation & {
		kind:           "response"
		method:         "post"
		op:             "createProjectSource"
		path:           "/projects/{project_id}/sources"
		summary:        "Create project source"
		returns:        "SourceDefinition"
		success_status: 201
		error_family:   "resource_conflict"
		params:         #projectPathParameters
		body_ref:       "CreateSourceDefinitionRequest"
		body_description: "Request payload"
	},
	#plainProjectOperation & {
		kind:         "response"
		method:       "get"
		op:           "getProjectSource"
		path:         "/projects/{project_id}/sources/{source_name}/{table_name}"
		summary:      "Get project source"
		returns:      "SourceDefinition"
		error_family: "resource"
		params:       #projectSourcePathParameters
	},
	#plainProjectOperation & {
		kind:         "response"
		method:       "patch"
		op:           "updateProjectSource"
		path:         "/projects/{project_id}/sources/{source_name}/{table_name}"
		summary:      "Update project source"
		returns:      "SourceDefinition"
		error_family: "resource_conflict"
		params:       #projectSourcePathParameters
		body_ref:     "UpdateSourceDefinitionRequest"
		body_description: "Request payload"
	},
	#plainProjectOperation & {
		kind:         "no_content"
		method:       "delete"
		op:           "deleteProjectSource"
		path:         "/projects/{project_id}/sources/{source_name}/{table_name}"
		summary:      "Delete project source"
		error_family: "resource_conflict"
		params:       #projectSourcePathParameters
	},
	#plainProjectOperation & {
		kind:         "response"
		method:       "get"
		op:           "listProjectSeeds"
		path:         "/projects/{project_id}/seeds"
		summary:      "List project seeds"
		returns:      "PaginatedProjectSeeds"
		error_family: "resource"
		params:       #projectListParameters
	},
	#plainProjectOperation & {
		kind:           "response"
		method:         "post"
		op:             "createProjectSeed"
		path:           "/projects/{project_id}/seeds"
		summary:        "Create project seed"
		returns:        "ProjectSeed"
		success_status: 201
		error_family:   "resource_conflict"
		params:         #projectPathParameters
		body_ref:       "CreateProjectSeedRequest"
		body_description: "Request payload"
	},
	#plainProjectOperation & {
		kind:         "response"
		method:       "get"
		op:           "getProjectSeed"
		path:         "/projects/{project_id}/seeds/{seed_name}"
		summary:      "Get project seed"
		returns:      "ProjectSeed"
		error_family: "resource"
		params:       #projectSeedPathParameters
	},
	#plainProjectOperation & {
		kind:         "response"
		method:       "patch"
		op:           "updateProjectSeed"
		path:         "/projects/{project_id}/seeds/{seed_name}"
		summary:      "Update project seed"
		returns:      "ProjectSeed"
		error_family: "resource_conflict"
		params:       #projectSeedPathParameters
		body_ref:     "UpdateProjectSeedRequest"
		body_description: "Request payload"
	},
	#plainProjectOperation & {
		kind:         "no_content"
		method:       "delete"
		op:           "deleteProjectSeed"
		path:         "/projects/{project_id}/seeds/{seed_name}"
		summary:      "Delete project seed"
		error_family: "resource_conflict"
		params:       #projectSeedPathParameters
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
