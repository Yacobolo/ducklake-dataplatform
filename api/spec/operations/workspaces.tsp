using TypeSpec.Http;

namespace Duck;

@route("/workspaces")
@get
@tag("Workspaces")
@summary("List workspaces")
op listWorkspaces(@query max_results?: int32, @query page_token?: string): Ok<PaginatedWorkspaces> | GuardedReadErrorResponses;

@route("/workspaces")
@post
@tag("Workspaces")
@summary("Create workspace")
op createWorkspace(@body body: CreateWorkspaceRequest): Created<Workspace> | MutatingErrorResponses | ConflictResponse;

@route("/workspaces/{workspace_id}")
@get
@tag("Workspaces")
@summary("Get workspace")
op getWorkspace(workspace_id: string): Ok<Workspace> | ResourceErrorResponses;

@route("/workspaces/{workspace_id}")
@patch(#{ implicitOptionality: true })
@tag("Workspaces")
@summary("Update workspace")
op updateWorkspace(workspace_id: string, @body body: UpdateWorkspaceRequest): Ok<Workspace> | ResourceErrorResponses | ConflictResponse;

@route("/workspaces/{workspace_id}")
@delete
@tag("Workspaces")
@summary("Delete workspace")
op deleteWorkspace(workspace_id: string): NoContent | ResourceErrorResponses | ConflictResponse;

@route("/workspaces/{workspace_id}/members")
@get
@tag("Workspaces")
@summary("List workspace members")
op listWorkspaceMembers(workspace_id: string): Ok<WorkspaceMember[]> | ResourceErrorResponses;

@route("/workspaces/{workspace_id}/members")
@post
@tag("Workspaces")
@summary("Add or update workspace member")
op addWorkspaceMember(workspace_id: string, @body body: AddWorkspaceMemberRequest): Ok<WorkspaceMember> | ResourceErrorResponses | ConflictResponse;

@route("/workspaces/{workspace_id}/members/{principal_name}")
@delete
@tag("Workspaces")
@summary("Remove workspace member")
op removeWorkspaceMember(workspace_id: string, principal_name: string): NoContent | ResourceErrorResponses | ConflictResponse;
