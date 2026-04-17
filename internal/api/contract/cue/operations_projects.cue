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

#dependencyIDPathParameter: #pathStringParameter & {
	#name: "dependency_id"
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

#releaseIDPathParameter: #pathStringParameter & {
	#name: "release_id"
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
	#dependencyIDPathParameter,
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

#projectReleasePathParameters: [
	#projectIDPathParameter,
	#releaseIDPathParameter,
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
		cli:          "projects list"
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
		cli:            "projects create"
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
		cli:          "projects get"
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
		cli:          "projects update"
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
		cli:          "projects delete"
		error_family: "resource_conflict"
		params:       #projectPathParameters
	},
	#plainProjectOperation & {
		kind:         "response"
		method:       "get"
		op:           "listProjectEnvironments"
		path:         "/projects/{project_id}/environments"
		summary:      "List project environments"
		cli:          "projects environments list"
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
		cli:            "projects environments create"
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
		cli:          "projects environments update"
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
		cli:          "projects environments delete"
		error_family: "resource_conflict"
		params:       #projectEnvironmentPathParameters
	},
	#plainProjectOperation & {
		kind:         "response"
		method:       "get"
		op:           "listProjectBuilds"
		path:         "/projects/{project_id}/builds"
		summary:      "List project builds"
		cli:          "projects builds list"
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
		cli:            "projects builds create"
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
		cli:          "projects dependencies list"
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
		cli:            "projects dependencies create"
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
		path:         "/projects/{project_id}/dependencies/{dependency_id}"
		summary:      "Delete project dependency"
		cli:          "projects dependencies delete"
		error_family: "resource_conflict"
		params:       #projectDependencyPathParameters
	},
	#plainProjectOperation & {
		kind:         "response"
		method:       "get"
		op:           "listProjectSources"
		path:         "/projects/{project_id}/sources"
		summary:      "List project sources"
		cli:          "projects sources list"
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
		cli:            "projects sources create"
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
		cli:          "projects sources get"
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
		cli:          "projects sources update"
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
		cli:          "projects sources delete"
		error_family: "resource_conflict"
		params:       #projectSourcePathParameters
	},
	#plainProjectOperation & {
		kind:         "response"
		method:       "get"
		op:           "listProjectSeeds"
		path:         "/projects/{project_id}/seeds"
		summary:      "List project seeds"
		cli:          "projects seeds list"
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
		cli:            "projects seeds create"
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
		cli:          "projects seeds get"
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
		cli:          "projects seeds update"
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
		cli:          "projects seeds delete"
		error_family: "resource_conflict"
		params:       #projectSeedPathParameters
	},
	#plainProjectOperation & {
		kind:         "response"
		method:       "get"
		op:           "listProjectReleases"
		path:         "/projects/{project_id}/releases"
		summary:      "List project releases"
		cli:          "projects releases list"
		returns:      "PaginatedProjectReleases"
		error_family: "resource"
		params:       #projectListParameters
	},
	#plainProjectOperation & {
		kind:           "response"
		method:         "post"
		op:             "createProjectRelease"
		path:           "/projects/{project_id}/releases"
		summary:        "Create project release"
		cli:            "projects releases create"
		returns:        "ProjectRelease"
		success_status: 201
		error_family:   "resource_conflict"
		params:         #projectPathParameters
		body_ref:       "CreateProjectReleaseRequest"
		body_description: "Request payload"
	},
	#plainProjectOperation & {
		kind:         "response"
		method:       "get"
		op:           "getProjectRelease"
		path:         "/projects/{project_id}/releases/{release_id}"
		summary:      "Get project release"
		cli:          "projects releases get"
		returns:      "ProjectRelease"
		error_family: "resource"
		params:       #projectReleasePathParameters
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
