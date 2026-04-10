using TypeSpec.Http;

namespace Duck;

@route("/workspaces/{workspace_id}/projects")
@get
@tag("Projects")
@summary("List projects in a workspace")
op listWorkspaceProjects(workspace_id: string, @query max_results?: int32, @query page_token?: string): Ok<PaginatedProjects> | ResourceErrorResponses;

@route("/workspaces/{workspace_id}/projects")
@post
@tag("Projects")
@summary("Create project in a workspace")
op createWorkspaceProject(workspace_id: string, @body body: CreateProjectRequest): Created<Project> | ResourceErrorResponses | ConflictResponse;

@route("/projects/{project_id}")
@get
@tag("Projects")
@summary("Get project")
op getProject(project_id: string): Ok<Project> | ResourceErrorResponses;

@route("/projects/{project_id}")
@patch
@tag("Projects")
@summary("Update project")
op updateProject(project_id: string, @body body: UpdateProjectRequest): Ok<Project> | ResourceErrorResponses | ConflictResponse;

@route("/projects/{project_id}")
@delete
@tag("Projects")
@summary("Delete project")
op deleteProject(project_id: string): NoContent | ResourceErrorResponses | ConflictResponse;

@route("/projects/{project_id}/environments")
@get
@tag("Projects")
@summary("List project environments")
op listProjectEnvironments(project_id: string, @query max_results?: int32, @query page_token?: string): Ok<PaginatedEnvironments> | ResourceErrorResponses;

@route("/projects/{project_id}/environments")
@post
@tag("Projects")
@summary("Create project environment")
op createProjectEnvironment(project_id: string, @body body: CreateEnvironmentRequest): Created<Environment> | ResourceErrorResponses | ConflictResponse;

@route("/projects/{project_id}/environments/{environment_id}")
@patch
@tag("Projects")
@summary("Update project environment")
op updateProjectEnvironment(project_id: string, environment_id: string, @body body: UpdateEnvironmentRequest): Ok<Environment> | ResourceErrorResponses | ConflictResponse;

@route("/projects/{project_id}/environments/{environment_id}")
@delete
@tag("Projects")
@summary("Delete project environment")
op deleteProjectEnvironment(project_id: string, environment_id: string): NoContent | ResourceErrorResponses | ConflictResponse;

@route("/projects/{project_id}/builds")
@get
@tag("Projects")
@summary("List project builds")
op listProjectBuilds(project_id: string, @query max_results?: int32, @query page_token?: string): Ok<PaginatedBuilds> | ResourceErrorResponses;

@route("/projects/{project_id}/builds")
@post
@tag("Projects")
@summary("Create project build")
op createProjectBuild(project_id: string, @body body: CreateBuildRequest): Created<Build> | ResourceErrorResponses | ConflictResponse;
