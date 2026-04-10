using TypeSpec.Http;

namespace Duck;

@route("/lineage/tables/{schema_name}/{table_name}")
@get
@tag("Lineage")
@summary("Get table lineage")
op getTableLineage(schema_name: string, table_name: string, @query max_results?: int32, @query page_token?: string): Ok<LineageNode> | ResourceErrorResponses;

@route("/lineage/tables/{schema_name}/{table_name}/upstream")
@get
@tag("Lineage")
@summary("Get upstream lineage")
op getUpstreamLineage(schema_name: string, table_name: string, @query max_results?: int32, @query page_token?: string): Ok<PaginatedLineageEdges> | ResourceErrorResponses;

@route("/lineage/tables/{schema_name}/{table_name}/downstream")
@get
@tag("Lineage")
@summary("Get downstream lineage")
op getDownstreamLineage(schema_name: string, table_name: string, @query max_results?: int32, @query page_token?: string): Ok<PaginatedLineageEdges> | ResourceErrorResponses;

@route("/lineage/edges/{edge_id}")
@delete
@tag("Lineage")
@summary("Delete lineage edge")
op deleteLineageEdge(edge_id: string): NoContent | MutatingErrorResponses;

@route("/lineage/columns/{schema_name}/{table_name}")
@get
@tag("Lineage")
@summary("Get column lineage")
op getColumnLineage(schema_name: string, table_name: string, @query max_results?: int32, @query page_token?: string): Ok<PaginatedColumnLineageEdges> | ResourceErrorResponses;

@route("/lineage/columns/{schema_name}/{table_name}/{column_name}/impacts")
@get
@tag("Lineage")
@summary("Get column impact")
op getColumnImpact(schema_name: string, table_name: string, column_name: string, @query max_results?: int32, @query page_token?: string): Ok<PaginatedColumnLineageEdges> | ResourceErrorResponses;

@route("/lineage/purges")
@post
@tag("Lineage")
@summary("Purge lineage")
op purgeLineage(@body body: PurgeLineageRequest): Ok<PurgeLineageResponse> | MutatingErrorResponses;
