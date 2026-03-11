using TypeSpec.Http;

namespace Duck;

@route("/auth/bootstrap/complete")
@post
@useAuth([])
@tag("Auth")
@summary("Bootstrap complete")
op bootstrapComplete(@body body: BootstrapCompleteRequest): Created<AuthLoginResponse> | BadRequestResponse | UnauthorizedResponse | ForbiddenResponse | ConflictResponse | TooManyRequestsResponse | InternalServerErrorResponse;

@route("/auth/local/login")
@post
@useAuth([])
@tag("Auth")
@summary("Local login")
@doc("Authenticates a local user with username and password and returns a bearer token for subsequent API calls.")
op localLogin(@body body: LocalLoginRequest): Created<AuthLoginResponse> | BadRequestResponse | UnauthorizedResponse | ForbiddenResponse | TooManyRequestsResponse | InternalServerErrorResponse;

@route("/auth/bootstrap/tokens")
@post
@tag("Auth")
@summary("Create bootstrap token")
op createBootstrapToken(@body body: BootstrapTokenRequest): Created<BootstrapTokenResponse> | BadRequestResponse | UnauthorizedResponse | ForbiddenResponse | TooManyRequestsResponse | InternalServerErrorResponse;

@route("/auth/provider/oidc")
@get
@tag("Auth")
@summary("Get OIDC provider")
op getOIDCProvider(): Ok<OIDCProviderResponse> | UnauthorizedResponse | ForbiddenResponse | TooManyRequestsResponse | InternalServerErrorResponse;

@route("/auth/provider/oidc")
@put
@tag("Auth")
@summary("Upsert OIDC provider")
op upsertOIDCProvider(@body body: OIDCProviderRequest): NoContent | BadRequestResponse | UnauthorizedResponse | ForbiddenResponse | TooManyRequestsResponse | InternalServerErrorResponse;

@route("/auth/sessions/revoke-all")
@post
@tag("Auth")
@summary("Revoke all web sessions")
op revokeAllWebSessions(@body body: RevokeWebSessionsRequest): NoContent | BadRequestResponse | UnauthorizedResponse | ForbiddenResponse | TooManyRequestsResponse | InternalServerErrorResponse | ServiceUnavailableResponse;

@route("/auth/sessions/stats")
@get
@tag("Auth")
@summary("Get web session stats")
op getWebSessionStats(): Ok<WebSessionStatsResponse> | UnauthorizedResponse | ForbiddenResponse | TooManyRequestsResponse | InternalServerErrorResponse | ServiceUnavailableResponse;
