using TypeSpec.Http;

namespace Duck;

@route("/git-repos")
@get
@tag("Integrations")
@summary("List git repos")
op listGitRepos(@query max_results?: int32, @query page_token?: string): Ok<PaginatedGitRepos> | StandardErrorResponses;

@route("/git-repos")
@post
@tag("Integrations")
@summary("Create git repo")
op createGitRepo(@body body: CreateGitRepoRequest): Created<GitRepo> | MutatingErrorResponses;

@route("/git-repos/{git_repo_id}")
@get
@tag("Integrations")
@summary("Get git repo")
op getGitRepo(git_repo_id: string): Ok<GitRepo> | ResourceErrorResponses;

@route("/git-repos/{git_repo_id}")
@delete
@tag("Integrations")
@summary("Delete git repo")
op deleteGitRepo(git_repo_id: string): NoContent | ResourceErrorResponses;

@route("/git-repos/{git_repo_id}/sync-runs")
@post
@tag("Integrations")
@summary("Sync git repo")
op syncGitRepo(git_repo_id: string): Ok<GitSyncResult> | ResourceErrorResponses | NotImplementedResponse;
