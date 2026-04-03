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

@route("/git-repos/{gitRepoId}")
@get
@tag("Integrations")
@summary("Get git repo")
op getGitRepo(gitRepoId: string): Ok<GitRepo> | ResourceErrorResponses;

@route("/git-repos/{gitRepoId}")
@delete
@tag("Integrations")
@summary("Delete git repo")
op deleteGitRepo(gitRepoId: string): NoContent | ResourceErrorResponses;

@route("/git-repos/{gitRepoId}/sync-runs")
@post
@tag("Integrations")
@summary("Sync git repo")
op syncGitRepo(gitRepoId: string): Ok<GitSyncResult> | ResourceErrorResponses | NotImplementedResponse;
