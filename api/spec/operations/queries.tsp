using TypeSpec.Http;

namespace Duck;

@route("/queries:execute")
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

@route("/queries/{queryId}")
@get
@tag("Queries")
@summary("Get query")
op getQuery(queryId: string): Ok<QueryJob> | ResourceErrorResponses;

@route("/queries/{queryId}")
@delete
@tag("Queries")
@summary("Delete query")
op deleteQuery(queryId: string): NoContent | MutatingErrorResponses;

@route("/queries/{queryId}/results")
@get
@tag("Queries")
@summary("Get query results")
@doc("Returns a page of rows for a previously submitted query using the stored query job identifier.")
op getQueryResults(queryId: string, @query max_results?: int32, @query page_token?: string): Ok<QueryResult> | ResourceErrorResponses;

@route("/queries/{queryId}:cancel")
@post
@tag("Queries")
@summary("Cancel query")
op cancelQuery(queryId: string): Accepted<CancelQueryResponse> | ResourceErrorResponses;

@route("/queries/history")
@get
@tag("Queries")
@summary("List query history")
@doc("Lists recorded query executions and supports filtering by principal, decision status, and time window.")
op listQueryHistory(
  @query principal_name?: string,
  @query status?: AuditDecisionStatus,
  @query from?: utcDateTime,
  @query to?: utcDateTime,
  @query max_results?: int32,
  @query page_token?: string,
): Ok<PaginatedQueryHistoryEntries> | GuardedReadErrorResponses;
