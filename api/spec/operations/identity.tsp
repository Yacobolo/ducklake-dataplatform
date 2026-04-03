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

@route("/principals/{principalId}")
@get
@tag("Identity")
@summary("Get principal")
op getPrincipal(principalId: string): Ok<Principal> | ResourceErrorResponses;

@route("/principals/{principalId}")
@delete
@tag("Identity")
@summary("Delete principal")
op deletePrincipal(principalId: string): NoContent | MutatingErrorResponses;

@route("/principals/{principalId}/admin")
@put
@tag("Identity")
@summary("Update principal admin")
op updatePrincipalAdmin(principalId: string, @body body: UpdatePrincipalAdminRequest): NoContent | MutatingErrorResponses;

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

@route("/groups/{groupId}")
@get
@tag("Identity")
@summary("Get group")
op getGroup(groupId: string): Ok<Group> | ResourceErrorResponses;

@route("/groups/{groupId}")
@patch(#{implicitOptionality: true})
@tag("Identity")
@summary("Update group")
op updateGroup(groupId: string, @body body: UpdateGroupRequest): Ok<Group> | ValidationResourceErrorResponses;

@route("/groups/{groupId}")
@delete
@tag("Identity")
@summary("Delete group")
op deleteGroup(groupId: string): NoContent | MutatingErrorResponses;

@route("/groups/{groupId}/members")
@get
@tag("Identity")
@summary("List group members")
op listGroupMembers(groupId: string, @query max_results?: int32, @query page_token?: string): Ok<PaginatedGroupMembers> | ResourceErrorResponses;

@route("/groups/{groupId}/members")
@post
@tag("Identity")
@summary("Create group member")
op createGroupMember(groupId: string, @body body: CreateGroupMemberRequest): CreatedEmpty | MutatingErrorResponses;

@route("/groups/{groupId}/members/{memberType}/{memberId}")
@delete
@tag("Identity")
@summary("Delete group member")
op deleteGroupMember(groupId: string, memberType: PrincipalType, memberId: string): NoContent | MutatingErrorResponses;

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

@route("/api-keys/{apiKeyId}")
@delete
@tag("Identity")
@summary("Delete API key")
op deleteAPIKey(apiKeyId: string): NoContent | MutatingErrorResponses;

@route("/api-key-cleanup-runs")
@post
@tag("Identity")
@summary("Clean up expired API keys")
op cleanupExpiredAPIKeys(): Ok<CleanupAPIKeysResponse> | MutatingErrorResponses;
