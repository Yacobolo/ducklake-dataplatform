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

@route("/compute-endpoints/{endpoint_name}")
@get
@tag("Compute")
@summary("Get compute endpoint")
op getComputeEndpoint(endpoint_name: string): Ok<ComputeEndpoint> | ResourceErrorResponses;

@route("/compute-endpoints/{endpoint_name}")
@patch(#{ implicitOptionality: true })
@tag("Compute")
@summary("Update compute endpoint")
op updateComputeEndpoint(endpoint_name: string, @body body: UpdateComputeEndpointRequest): Ok<ComputeEndpoint> | ResourceErrorResponses;

@route("/compute-endpoints/{endpoint_name}")
@delete
@tag("Compute")
@summary("Delete compute endpoint")
op deleteComputeEndpoint(endpoint_name: string): NoContent | ResourceErrorResponses;

@route("/compute-endpoints/{endpoint_name}/assignments")
@get
@tag("Compute")
@summary("List compute assignments")
op listComputeAssignments(endpoint_name: string, @query max_results?: int32, @query page_token?: string): Ok<PaginatedComputeAssignments> | ResourceErrorResponses;

@route("/compute-endpoints/{endpoint_name}/assignments")
@post
@tag("Compute")
@summary("Create compute assignment")
op createComputeAssignment(endpoint_name: string, @body body: CreateComputeAssignmentRequest): Created<ComputeAssignment> | ValidationResourceErrorResponses;

@route("/compute-endpoints/{endpoint_name}/health")
@get
@tag("Compute")
@summary("Get compute endpoint health")
op getComputeEndpointHealth(endpoint_name: string): Ok<ComputeEndpointHealth> | BadRequestResponse | ResourceErrorResponses | BadGatewayResponse;

@route("/compute-endpoints/{endpoint_name}/assignments/{assignment_id}")
@delete
@tag("Compute")
@summary("Delete compute assignment")
op deleteComputeAssignment(endpoint_name: string, assignment_id: string): NoContent | ResourceErrorResponses;

@route("/compute-routing-defaults")
@get
@tag("Compute")
@summary("Get compute routing defaults")
op getComputeRoutingDefaults(): Ok<ComputeRoutingDefaults> | GuardedReadErrorResponses;

@route("/compute-routing-defaults")
@patch(#{ implicitOptionality: true })
@tag("Compute")
@summary("Update compute routing defaults")
op updateComputeRoutingDefaults(@body body: ComputeRoutingDefaults): Ok<ComputeRoutingDefaults> | MutatingErrorResponses | BadRequestResponse;
