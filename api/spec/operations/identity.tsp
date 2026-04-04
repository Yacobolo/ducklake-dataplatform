using TypeSpec.Http;

namespace Duck;

@route("/principals")
@get
@tag("Identity")
@summary("List principals")
op listPrincipals(@query max_results?: int32, @query page_token?: string): Ok<PaginatedPrincipals> | GuardedReadErrorResponses;

@route("/principals")
@post
@tag("Identity")
@summary("Create principal")
op createPrincipal(@body body: CreatePrincipalRequest): Created<Principal> | MutatingErrorResponses;

@route("/principals/{principal_id}")
@get
@tag("Identity")
@summary("Get principal")
op getPrincipal(principal_id: string): Ok<Principal> | ResourceErrorResponses;

@route("/principals/{principal_id}")
@delete
@tag("Identity")
@summary("Delete principal")
op deletePrincipal(principal_id: string): NoContent | MutatingErrorResponses;

@route("/principals/{principal_id}")
@patch(#{implicitOptionality: true})
@tag("Identity")
@summary("Update principal")
op updatePrincipal(principal_id: string, @body body: UpdatePrincipalRequest): Ok<Principal> | MutatingErrorResponses;

@route("/groups")
@get
@tag("Identity")
@summary("List groups")
op listGroups(@query max_results?: int32, @query page_token?: string): Ok<PaginatedGroups> | GuardedReadErrorResponses;

@route("/groups")
@post
@tag("Identity")
@summary("Create group")
op createGroup(@body body: CreateGroupRequest): Created<Group> | MutatingErrorResponses;

@route("/groups/{group_id}")
@get
@tag("Identity")
@summary("Get group")
op getGroup(group_id: string): Ok<Group> | ResourceErrorResponses;

@route("/groups/{group_id}")
@patch(#{implicitOptionality: true})
@tag("Identity")
@summary("Update group")
op updateGroup(group_id: string, @body body: UpdateGroupRequest): Ok<Group> | ValidationResourceErrorResponses;

@route("/groups/{group_id}")
@delete
@tag("Identity")
@summary("Delete group")
op deleteGroup(group_id: string): NoContent | MutatingErrorResponses;

@route("/groups/{group_id}/members")
@get
@tag("Identity")
@summary("List group members")
op listGroupMembers(group_id: string, @query max_results?: int32, @query page_token?: string): Ok<PaginatedGroupMembers> | ResourceErrorResponses;

@route("/groups/{group_id}/members")
@post
@tag("Identity")
@summary("Create group member")
op createGroupMember(group_id: string, @body body: CreateGroupMemberRequest): CreatedEmpty | MutatingErrorResponses;

@route("/groups/{group_id}/members/{member_type}/{member_id}")
@delete
@tag("Identity")
@summary("Delete group member")
op deleteGroupMember(group_id: string, member_type: PrincipalType, member_id: string): NoContent | MutatingErrorResponses;

@route("/api-keys")
@get
@tag("Identity")
@summary("List API keys")
op listAPIKeys(@query principal_id?: string, @query max_results?: int32, @query page_token?: string): Ok<PaginatedAPIKeys> | StandardErrorResponses;

@route("/api-keys")
@post
@tag("Identity")
@summary("Create API key")
op createAPIKey(@body body: CreateAPIKeyRequest): Created<CreateAPIKeyResponse> | MutatingErrorResponses;

@route("/api-keys/{api_key_id}")
@delete
@tag("Identity")
@summary("Delete API key")
op deleteAPIKey(api_key_id: string): NoContent | MutatingErrorResponses;

@route("/api-key-cleanup-runs")
@post
@tag("Identity")
@summary("Clean up expired API keys")
op cleanupExpiredAPIKeys(): Ok<CleanupAPIKeysResponse> | MutatingErrorResponses;
