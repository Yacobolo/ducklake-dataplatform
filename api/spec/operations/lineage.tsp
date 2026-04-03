using TypeSpec.Http;

namespace Duck;

@route("/lineage/tables/{schemaName}/{tableName}")
@get
@tag("Lineage")
@summary("Get table lineage")
op getTableLineage(schemaName: string, tableName: string, @query max_results?: int32, @query page_token?: string): Ok<LineageNode> | ResourceErrorResponses;

@route("/lineage/tables/{schemaName}/{tableName}/upstream")
@get
@tag("Lineage")
@summary("Get upstream lineage")
op getUpstreamLineage(schemaName: string, tableName: string, @query max_results?: int32, @query page_token?: string): Ok<PaginatedLineageEdges> | ResourceErrorResponses;

@route("/lineage/tables/{schemaName}/{tableName}/downstream")
@get
@tag("Lineage")
@summary("Get downstream lineage")
op getDownstreamLineage(schemaName: string, tableName: string, @query max_results?: int32, @query page_token?: string): Ok<PaginatedLineageEdges> | ResourceErrorResponses;

@route("/lineage/edges/{edgeId}")
@delete
@tag("Lineage")
@summary("Delete lineage edge")
op deleteLineageEdge(edgeId: string): NoContent | MutatingErrorResponses;

@route("/lineage/columns/{schemaName}/{tableName}")
@get
@tag("Lineage")
@summary("Get column lineage")
op getColumnLineage(schemaName: string, tableName: string, @query max_results?: int32, @query page_token?: string): Ok<PaginatedColumnLineageEdges> | ResourceErrorResponses;

@route("/lineage/columns/{schemaName}/{tableName}/{columnName}/impacts")
@get
@tag("Lineage")
@summary("Get column impact")
op getColumnImpact(schemaName: string, tableName: string, columnName: string, @query max_results?: int32, @query page_token?: string): Ok<PaginatedColumnLineageEdges> | ResourceErrorResponses;

@route("/lineage-purges")
@post
@tag("Lineage")
@summary("Purge lineage")
op purgeLineage(@body body: PurgeLineageRequest): Ok<PurgeLineageResponse> | MutatingErrorResponses;
