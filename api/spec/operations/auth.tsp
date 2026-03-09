using TypeSpec.Http;

namespace Duck;

@route("/auth/bootstrap/complete")
@post
@useAuth([])
op bootstrapComplete(@body body: BootstrapCompleteRequest): Created<AuthLoginResponse> | BadRequestResponse | UnauthorizedResponse | ForbiddenResponse | ConflictResponse | TooManyRequestsResponse | InternalServerErrorResponse;

@route("/auth/local/login")
@post
@useAuth([])
op localLogin(@body body: LocalLoginRequest): Created<AuthLoginResponse> | BadRequestResponse | UnauthorizedResponse | ForbiddenResponse | TooManyRequestsResponse | InternalServerErrorResponse;

@route("/auth/bootstrap/tokens")
@post
op createBootstrapToken(@body body: BootstrapTokenRequest): Created<BootstrapTokenResponse> | BadRequestResponse | UnauthorizedResponse | ForbiddenResponse | TooManyRequestsResponse | InternalServerErrorResponse;

@route("/auth/provider/oidc")
@get
op getOIDCProvider(): Ok<OIDCProviderResponse> | UnauthorizedResponse | ForbiddenResponse | TooManyRequestsResponse | InternalServerErrorResponse;

@route("/auth/provider/oidc")
@put
op upsertOIDCProvider(@body body: OIDCProviderRequest): NoContent | BadRequestResponse | UnauthorizedResponse | ForbiddenResponse | TooManyRequestsResponse | InternalServerErrorResponse;

@route("/auth/sessions/revoke-all")
@post
op revokeAllWebSessions(@body body: RevokeWebSessionsRequest): NoContent | BadRequestResponse | UnauthorizedResponse | ForbiddenResponse | TooManyRequestsResponse | InternalServerErrorResponse | ServiceUnavailableResponse;

@route("/auth/sessions/stats")
@get
op getWebSessionStats(): Ok<WebSessionStatsResponse> | UnauthorizedResponse | ForbiddenResponse | TooManyRequestsResponse | InternalServerErrorResponse | ServiceUnavailableResponse;
