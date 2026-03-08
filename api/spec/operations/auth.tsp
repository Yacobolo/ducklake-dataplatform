using TypeSpec.Http;

namespace Duck;

@route("/auth/bootstrap/complete")
@post
@useAuth(NoAuth)
op bootstrapComplete(@body body: BootstrapCompleteRequest): Created<AuthLoginResponse> | BadRequestResponse | ForbiddenResponse | ConflictResponse | TooManyRequestsResponse | InternalServerErrorResponse;

@route("/auth/local/login")
@post
@useAuth(NoAuth)
op localLogin(@body body: LocalLoginRequest): Ok<AuthLoginResponse> | BadRequestResponse | ForbiddenResponse | TooManyRequestsResponse | InternalServerErrorResponse;

@route("/auth/bootstrap/tokens")
@post
op createBootstrapToken(@body body: BootstrapTokenRequest): Created<BootstrapTokenResponse> | BadRequestResponse | ForbiddenResponse | TooManyRequestsResponse | InternalServerErrorResponse;

@route("/auth/provider/oidc")
@get
op getOIDCProvider(): Ok<OIDCProviderResponse> | ForbiddenResponse | TooManyRequestsResponse | InternalServerErrorResponse;

@route("/auth/provider/oidc")
@put
op upsertOIDCProvider(@body body: OIDCProviderRequest): NoContent | BadRequestResponse | ForbiddenResponse | TooManyRequestsResponse | InternalServerErrorResponse;

@route("/auth/sessions/revoke-all")
@post
op revokeAllWebSessions(@body body: RevokeWebSessionsRequest): NoContent | BadRequestResponse | ForbiddenResponse | TooManyRequestsResponse | InternalServerErrorResponse | ServiceUnavailableResponse;

@route("/auth/sessions/stats")
@get
op getWebSessionStats(): Ok<WebSessionStatsResponse> | ForbiddenResponse | TooManyRequestsResponse | InternalServerErrorResponse | ServiceUnavailableResponse;
