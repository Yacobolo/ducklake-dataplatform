using TypeSpec.Http;

namespace Duck;

@route("/query-executions")
@post
@tag("Queries")
@summary("Execute query")
@doc("Executes a SQL statement synchronously and returns the first page of results in the response body.")
op executeQuery(@body body: QueryRequest): Ok<QueryResult> | MutatingErrorResponses;

@route("/queries")
@post
@tag("Queries")
@summary("Submit query")
@doc("Submits a SQL query for asynchronous execution and returns a query job identifier for polling and result retrieval.")
op submitQuery(@body body: SubmitQueryRequest): Accepted<SubmitQueryResponse> | MutatingErrorResponses;

@route("/queries")
@get
@tag("Queries")
@summary("List queries")
@doc("Lists asynchronous query jobs created by the authenticated principal and supports filtering by lifecycle status.")
op listQueries(
  @query status?: QueryJobStatus,
  @query max_results?: int32,
  @query page_token?: string,
): Ok<PaginatedQueryJobs> | StandardErrorResponses;

@route("/queries/{query_id}")
@get
@tag("Queries")
@summary("Get query")
op getQuery(query_id: string): Ok<QueryJob> | ResourceErrorResponses;

@route("/queries/{query_id}")
@delete
@tag("Queries")
@summary("Delete query")
op deleteQuery(query_id: string): NoContent | MutatingErrorResponses;

@route("/queries/{query_id}/results")
@get
@tag("Queries")
@summary("Get query results")
@doc("Returns a page of rows for a previously submitted query using the stored query job identifier.")
op getQueryResults(query_id: string, @query max_results?: int32, @query page_token?: string): Ok<QueryResult> | ResourceErrorResponses;

@route("/queries/{query_id}/cancellations")
@post
@tag("Queries")
@summary("Cancel query")
op cancelQuery(query_id: string): Accepted<CancelQueryResponse> | ResourceErrorResponses;

@route("/queries/history")
@get
@tag("Queries")
@summary("List query history")
@doc("Lists recorded query execution history and supports filtering by principal, decision status, and time window.")
op listQueryHistory(
  @query principal_name?: string,
  @query status?: AuditDecisionStatus,
  @query from?: utcDateTime,
  @query to?: utcDateTime,
  @query max_results?: int32,
  @query page_token?: string,
): Ok<PaginatedQueryHistoryEntries> | GuardedReadErrorResponses;
