using TypeSpec.Http;

namespace Duck;

@route("/compute-endpoints")
@get
@tag("Compute")
@summary("List compute endpoints")
op listComputeEndpoints(@query max_results?: int32, @query page_token?: string): Ok<PaginatedComputeEndpoints> | GuardedReadErrorResponses;

@route("/compute-endpoints")
@post
@tag("Compute")
@summary("Create compute endpoint")
@doc("Registers a compute endpoint that can execute remote workloads and accept assignment-based routing.")
op createComputeEndpoint(@body body: CreateComputeEndpointRequest): Created<ComputeEndpoint> | ValidationResourceErrorResponses;

@route("/compute-endpoints/{endpointName}")
@get
@tag("Compute")
@summary("Get compute endpoint")
op getComputeEndpoint(endpointName: string): Ok<ComputeEndpoint> | ResourceErrorResponses;

@route("/compute-endpoints/{endpointName}")
@patch(#{ implicitOptionality: true })
@tag("Compute")
@summary("Update compute endpoint")
op updateComputeEndpoint(endpointName: string, @body body: UpdateComputeEndpointRequest): Ok<ComputeEndpoint> | ResourceErrorResponses;

@route("/compute-endpoints/{endpointName}")
@delete
@tag("Compute")
@summary("Delete compute endpoint")
op deleteComputeEndpoint(endpointName: string): NoContent | ResourceErrorResponses;

@route("/compute-endpoints/{endpointName}/assignments")
@get
@tag("Compute")
@summary("List compute assignments")
op listComputeAssignments(endpointName: string, @query max_results?: int32, @query page_token?: string): Ok<PaginatedComputeAssignments> | ResourceErrorResponses;

@route("/compute-endpoints/{endpointName}/assignments")
@post
@tag("Compute")
@summary("Create compute assignment")
op createComputeAssignment(endpointName: string, @body body: CreateComputeAssignmentRequest): Created<ComputeAssignment> | ValidationResourceErrorResponses;

@route("/compute-endpoints/{endpointName}/health")
@get
@tag("Compute")
@summary("Get compute endpoint health")
op getComputeEndpointHealth(endpointName: string): Ok<ComputeEndpointHealth> | BadRequestResponse | ResourceErrorResponses | BadGatewayResponse;

@route("/compute-endpoints/{endpointName}/assignments/{assignmentId}")
@delete
@tag("Compute")
@summary("Delete compute assignment")
op deleteComputeAssignment(endpointName: string, assignmentId: string): NoContent | ResourceErrorResponses;

@route("/compute-defaults")
@get
@tag("Compute")
@summary("Get compute routing defaults")
op getComputeRoutingDefaults(): Ok<ComputeRoutingDefaults> | GuardedReadErrorResponses;

@route("/compute-defaults")
@patch(#{ implicitOptionality: true })
@tag("Compute")
@summary("Update compute routing defaults")
op updateComputeRoutingDefaults(@body body: ComputeRoutingDefaults): Ok<ComputeRoutingDefaults> | MutatingErrorResponses | BadRequestResponse;
