using TypeSpec.Http;

namespace Duck;

@route("/healthz")
@get
@useAuth([])
@tag("Health")
@summary("Get health")
op getHealth(): Ok<HealthResponse> | StandardErrorResponses;
