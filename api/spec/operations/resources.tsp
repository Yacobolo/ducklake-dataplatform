using TypeSpec.Http;

namespace Duck;

@route("/me/recent-resources")
@get
@tag("Resources")
@summary("List recent resources")
@doc("Lists the authenticated principal's recent UUID-backed resources for personalized navigation.")
op listRecentResources(
  @query max_results?: int32,
  @query page_token?: string,
): Ok<PaginatedRecentResources> | GuardedReadErrorResponses;

@route("/me/saved-resources")
@get
@tag("Resources")
@summary("List saved resources")
@doc("Lists the authenticated principal's saved UUID-backed resources.")
op listSavedResources(
  @query max_results?: int32,
  @query page_token?: string,
): Ok<PaginatedSavedResources> | GuardedReadErrorResponses;

@route("/me/saved-resources")
@post
@tag("Resources")
@summary("Save resource")
@doc("Saves a UUID-backed resource for the authenticated principal.")
op createSavedResource(@body body: CreateSavedResourceRequest): Created<SavedResource> | MutatingErrorResponses;

@route("/me/saved-resources/{resourceType}/{resourceKey}")
@delete
@tag("Resources")
@summary("Delete saved resource")
@doc("Removes a saved resource for the authenticated principal.")
op deleteSavedResource(resourceType: string, resourceKey: string): NoContent | MutatingErrorResponses;
