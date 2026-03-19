using TypeSpec.Http;

namespace Duck;

@route("/audit-entries")
@get
@tag("Audit")
@summary("List audit entries")
op listAuditLogs(
  @query principal_name?: string,
  @query action?: string,
  @query status?: AuditDecisionStatus,
  @query max_results?: int32,
  @query page_token?: string,
): Ok<PaginatedAuditLogs> | GuardedReadErrorResponses;
