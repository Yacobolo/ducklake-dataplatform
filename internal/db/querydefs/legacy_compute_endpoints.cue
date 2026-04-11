package querydefs

queries: [
	{
		name: "CountComputeEndpoints"
		kind: "one"
		result: {scalar: "int64"}
		select: {
			from: "compute_endpoints"
			columns: [
				{expr: "COUNT(*)"},
			]
		}
	},
	{
		name: "CreateComputeEndpoint"
		kind: "one"
		params: [
			{name: "ID", type: "string"},
			{name: "ExternalID", type: "string"},
			{name: "Name", type: "string"},
			{name: "Url", type: "string"},
			{name: "Type", type: "string"},
			{name: "SelectionPolicy", type: "string"},
			{name: "WorkloadClass", type: "string"},
			{name: "ReadinessStatus", type: "string"},
			{name: "Size", type: "string"},
			{name: "MaxMemoryGb", type: "sql.NullInt64"},
			{name: "MaxConcurrency", type: "sql.NullInt64"},
			{name: "MaxResultSizeMb", type: "sql.NullInt64"},
			{name: "RecommendedForLargeQueries", type: "int64"},
			{name: "IsDraining", type: "int64"},
			{name: "AuthToken", type: "string"},
			{name: "Owner", type: "string"},
		]
		result: {
			row: "ComputeEndpoint"
			fields: [
				{name: "ID", type: "string"},
				{name: "ExternalID", type: "string"},
				{name: "Name", type: "string"},
				{name: "Url", type: "string"},
				{name: "Type", type: "string"},
				{name: "Status", type: "string"},
				{name: "Size", type: "string"},
				{name: "MaxMemoryGb", type: "sql.NullInt64"},
				{name: "AuthToken", type: "string"},
				{name: "Owner", type: "string"},
				{name: "CreatedAt", type: "time.Time"},
				{name: "UpdatedAt", type: "time.Time"},
				{name: "SelectionPolicy", type: "string"},
				{name: "WorkloadClass", type: "string"},
				{name: "ReadinessStatus", type: "string"},
				{name: "MaxConcurrency", type: "sql.NullInt64"},
				{name: "MaxResultSizeMb", type: "sql.NullInt64"},
				{name: "RecommendedForLargeQueries", type: "int64"},
				{name: "IsDraining", type: "int64"},
				{name: "LastHealthStatus", type: "sql.NullString"},
				{name: "LastHealthCheckedAt", type: "sql.NullTime"},
				{name: "ActiveQueries", type: "sql.NullInt64"},
				{name: "QueuedJobs", type: "sql.NullInt64"},
				{name: "RunningJobs", type: "sql.NullInt64"},
				{name: "CompletedJobs", type: "sql.NullInt64"},
				{name: "StoredJobs", type: "sql.NullInt64"},
				{name: "CleanedJobs", type: "sql.NullInt64"},
				{name: "QueryResultTtlSeconds", type: "sql.NullInt64"},
			]
		}
		insert: {
			into: "compute_endpoints"
			columns: [
				"id",
				"external_id",
				"name",
				"url",
				"type",
				"status",
				"selection_policy",
				"workload_class",
				"readiness_status",
				"size",
				"max_memory_gb",
				"max_concurrency",
				"max_result_size_mb",
				"recommended_for_large_queries",
				"is_draining",
				"auth_token",
				"owner",
			]
			values: [
				{param: "ID"},
				{param: "ExternalID"},
				{param: "Name"},
				{param: "Url"},
				{param: "Type"},
				{sql: "'INACTIVE'"},
				{param: "SelectionPolicy"},
				{param: "WorkloadClass"},
				{param: "ReadinessStatus"},
				{param: "Size"},
				{param: "MaxMemoryGb"},
				{param: "MaxConcurrency"},
				{param: "MaxResultSizeMb"},
				{param: "RecommendedForLargeQueries"},
				{param: "IsDraining"},
				{param: "AuthToken"},
				{param: "Owner"},
			]
			returningColumns: [
				{expr: "id"},
				{expr: "external_id"},
				{expr: "name"},
				{expr: "url"},
				{expr: "type"},
				{expr: "status"},
				{expr: "size"},
				{expr: "max_memory_gb"},
				{expr: "auth_token"},
				{expr: "owner"},
				{expr: "created_at"},
				{expr: "updated_at"},
				{expr: "selection_policy"},
				{expr: "workload_class"},
				{expr: "readiness_status"},
				{expr: "max_concurrency"},
				{expr: "max_result_size_mb"},
				{expr: "recommended_for_large_queries"},
				{expr: "is_draining"},
				{expr: "last_health_status"},
				{expr: "last_health_checked_at"},
				{expr: "active_queries"},
				{expr: "queued_jobs"},
				{expr: "running_jobs"},
				{expr: "completed_jobs"},
				{expr: "stored_jobs"},
				{expr: "cleaned_jobs"},
				{expr: "query_result_ttl_seconds"},
			]
		}
	},
	{
		name: "DeleteComputeEndpoint"
		kind: "exec"
		params: [
			{name: "id", type: "string"},
		]
		delete: {
			from: "compute_endpoints"
			where: [
				{column: "id", op: "=", param: "id"},
			]
		}
	},
	{
		name: "GetAssignmentsForPrincipal"
		kind: "many"
		params: [
			{name: "PrincipalID", type: "string"},
			{name: "PrincipalType", type: "string"},
		]
		result: {
			row: "ComputeEndpoint"
			fields: [
				{name: "ID", type: "string"},
				{name: "ExternalID", type: "string"},
				{name: "Name", type: "string"},
				{name: "Url", type: "string"},
				{name: "Type", type: "string"},
				{name: "Status", type: "string"},
				{name: "Size", type: "string"},
				{name: "MaxMemoryGb", type: "sql.NullInt64"},
				{name: "AuthToken", type: "string"},
				{name: "Owner", type: "string"},
				{name: "CreatedAt", type: "time.Time"},
				{name: "UpdatedAt", type: "time.Time"},
				{name: "SelectionPolicy", type: "string"},
				{name: "WorkloadClass", type: "string"},
				{name: "ReadinessStatus", type: "string"},
				{name: "MaxConcurrency", type: "sql.NullInt64"},
				{name: "MaxResultSizeMb", type: "sql.NullInt64"},
				{name: "RecommendedForLargeQueries", type: "int64"},
				{name: "IsDraining", type: "int64"},
				{name: "LastHealthStatus", type: "sql.NullString"},
				{name: "LastHealthCheckedAt", type: "sql.NullTime"},
				{name: "ActiveQueries", type: "sql.NullInt64"},
				{name: "QueuedJobs", type: "sql.NullInt64"},
				{name: "RunningJobs", type: "sql.NullInt64"},
				{name: "CompletedJobs", type: "sql.NullInt64"},
				{name: "StoredJobs", type: "sql.NullInt64"},
				{name: "CleanedJobs", type: "sql.NullInt64"},
				{name: "QueryResultTtlSeconds", type: "sql.NullInt64"},
			]
		}
		select: {
			from: "compute_endpoints"
			alias: "ce"
			columns: [
				{expr: "ce.id"},
				{expr: "ce.external_id"},
				{expr: "ce.name"},
				{expr: "ce.url"},
				{expr: "ce.type"},
				{expr: "ce.status"},
				{expr: "ce.size"},
				{expr: "ce.max_memory_gb"},
				{expr: "ce.auth_token"},
				{expr: "ce.owner"},
				{expr: "ce.created_at"},
				{expr: "ce.updated_at"},
				{expr: "ce.selection_policy"},
				{expr: "ce.workload_class"},
				{expr: "ce.readiness_status"},
				{expr: "ce.max_concurrency"},
				{expr: "ce.max_result_size_mb"},
				{expr: "ce.recommended_for_large_queries"},
				{expr: "ce.is_draining"},
				{expr: "ce.last_health_status"},
				{expr: "ce.last_health_checked_at"},
				{expr: "ce.active_queries"},
				{expr: "ce.queued_jobs"},
				{expr: "ce.running_jobs"},
				{expr: "ce.completed_jobs"},
				{expr: "ce.stored_jobs"},
				{expr: "ce.cleaned_jobs"},
				{expr: "ce.query_result_ttl_seconds"},
			]
			joins: [
				{type: "JOIN", table: "compute_assignments", alias: "ca", on: "ca.endpoint_id = ce.id"},
			]
			where: [
				{column: "ca.principal_id", op: "=", param: "PrincipalID"},
				{column: "ca.principal_type", op: "=", param: "PrincipalType"},
			]
			orderBy: [
				{expr: "ca.is_default", desc: true},
				{expr: "ce.name"},
			]
		}
	},
	{
		name: "GetComputeEndpoint"
		kind: "one"
		params: [
			{name: "id", type: "string"},
		]
		result: {
			row: "ComputeEndpoint"
			fields: [
				{name: "ID", type: "string"},
				{name: "ExternalID", type: "string"},
				{name: "Name", type: "string"},
				{name: "Url", type: "string"},
				{name: "Type", type: "string"},
				{name: "Status", type: "string"},
				{name: "Size", type: "string"},
				{name: "MaxMemoryGb", type: "sql.NullInt64"},
				{name: "AuthToken", type: "string"},
				{name: "Owner", type: "string"},
				{name: "CreatedAt", type: "time.Time"},
				{name: "UpdatedAt", type: "time.Time"},
				{name: "SelectionPolicy", type: "string"},
				{name: "WorkloadClass", type: "string"},
				{name: "ReadinessStatus", type: "string"},
				{name: "MaxConcurrency", type: "sql.NullInt64"},
				{name: "MaxResultSizeMb", type: "sql.NullInt64"},
				{name: "RecommendedForLargeQueries", type: "int64"},
				{name: "IsDraining", type: "int64"},
				{name: "LastHealthStatus", type: "sql.NullString"},
				{name: "LastHealthCheckedAt", type: "sql.NullTime"},
				{name: "ActiveQueries", type: "sql.NullInt64"},
				{name: "QueuedJobs", type: "sql.NullInt64"},
				{name: "RunningJobs", type: "sql.NullInt64"},
				{name: "CompletedJobs", type: "sql.NullInt64"},
				{name: "StoredJobs", type: "sql.NullInt64"},
				{name: "CleanedJobs", type: "sql.NullInt64"},
				{name: "QueryResultTtlSeconds", type: "sql.NullInt64"},
			]
		}
		select: {
			from: "compute_endpoints"
			columns: [
				{expr: "id"},
				{expr: "external_id"},
				{expr: "name"},
				{expr: "url"},
				{expr: "type"},
				{expr: "status"},
				{expr: "size"},
				{expr: "max_memory_gb"},
				{expr: "auth_token"},
				{expr: "owner"},
				{expr: "created_at"},
				{expr: "updated_at"},
				{expr: "selection_policy"},
				{expr: "workload_class"},
				{expr: "readiness_status"},
				{expr: "max_concurrency"},
				{expr: "max_result_size_mb"},
				{expr: "recommended_for_large_queries"},
				{expr: "is_draining"},
				{expr: "last_health_status"},
				{expr: "last_health_checked_at"},
				{expr: "active_queries"},
				{expr: "queued_jobs"},
				{expr: "running_jobs"},
				{expr: "completed_jobs"},
				{expr: "stored_jobs"},
				{expr: "cleaned_jobs"},
				{expr: "query_result_ttl_seconds"},
			]
			where: [
				{column: "id", op: "=", param: "id"},
			]
		}
	},
	{
		name: "GetComputeEndpointByName"
		kind: "one"
		params: [
			{name: "name", type: "string"},
		]
		result: {
			row: "ComputeEndpoint"
			fields: [
				{name: "ID", type: "string"},
				{name: "ExternalID", type: "string"},
				{name: "Name", type: "string"},
				{name: "Url", type: "string"},
				{name: "Type", type: "string"},
				{name: "Status", type: "string"},
				{name: "Size", type: "string"},
				{name: "MaxMemoryGb", type: "sql.NullInt64"},
				{name: "AuthToken", type: "string"},
				{name: "Owner", type: "string"},
				{name: "CreatedAt", type: "time.Time"},
				{name: "UpdatedAt", type: "time.Time"},
				{name: "SelectionPolicy", type: "string"},
				{name: "WorkloadClass", type: "string"},
				{name: "ReadinessStatus", type: "string"},
				{name: "MaxConcurrency", type: "sql.NullInt64"},
				{name: "MaxResultSizeMb", type: "sql.NullInt64"},
				{name: "RecommendedForLargeQueries", type: "int64"},
				{name: "IsDraining", type: "int64"},
				{name: "LastHealthStatus", type: "sql.NullString"},
				{name: "LastHealthCheckedAt", type: "sql.NullTime"},
				{name: "ActiveQueries", type: "sql.NullInt64"},
				{name: "QueuedJobs", type: "sql.NullInt64"},
				{name: "RunningJobs", type: "sql.NullInt64"},
				{name: "CompletedJobs", type: "sql.NullInt64"},
				{name: "StoredJobs", type: "sql.NullInt64"},
				{name: "CleanedJobs", type: "sql.NullInt64"},
				{name: "QueryResultTtlSeconds", type: "sql.NullInt64"},
			]
		}
		select: {
			from: "compute_endpoints"
			columns: [
				{expr: "id"},
				{expr: "external_id"},
				{expr: "name"},
				{expr: "url"},
				{expr: "type"},
				{expr: "status"},
				{expr: "size"},
				{expr: "max_memory_gb"},
				{expr: "auth_token"},
				{expr: "owner"},
				{expr: "created_at"},
				{expr: "updated_at"},
				{expr: "selection_policy"},
				{expr: "workload_class"},
				{expr: "readiness_status"},
				{expr: "max_concurrency"},
				{expr: "max_result_size_mb"},
				{expr: "recommended_for_large_queries"},
				{expr: "is_draining"},
				{expr: "last_health_status"},
				{expr: "last_health_checked_at"},
				{expr: "active_queries"},
				{expr: "queued_jobs"},
				{expr: "running_jobs"},
				{expr: "completed_jobs"},
				{expr: "stored_jobs"},
				{expr: "cleaned_jobs"},
				{expr: "query_result_ttl_seconds"},
			]
			where: [
				{column: "name", op: "=", param: "name"},
			]
		}
	},
	{
		name: "GetDefaultEndpointForPrincipal"
		kind: "one"
		params: [
			{name: "PrincipalID", type: "string"},
			{name: "PrincipalType", type: "string"},
		]
		result: {
			row: "ComputeEndpoint"
			fields: [
				{name: "ID", type: "string"},
				{name: "ExternalID", type: "string"},
				{name: "Name", type: "string"},
				{name: "Url", type: "string"},
				{name: "Type", type: "string"},
				{name: "Status", type: "string"},
				{name: "Size", type: "string"},
				{name: "MaxMemoryGb", type: "sql.NullInt64"},
				{name: "AuthToken", type: "string"},
				{name: "Owner", type: "string"},
				{name: "CreatedAt", type: "time.Time"},
				{name: "UpdatedAt", type: "time.Time"},
				{name: "SelectionPolicy", type: "string"},
				{name: "WorkloadClass", type: "string"},
				{name: "ReadinessStatus", type: "string"},
				{name: "MaxConcurrency", type: "sql.NullInt64"},
				{name: "MaxResultSizeMb", type: "sql.NullInt64"},
				{name: "RecommendedForLargeQueries", type: "int64"},
				{name: "IsDraining", type: "int64"},
				{name: "LastHealthStatus", type: "sql.NullString"},
				{name: "LastHealthCheckedAt", type: "sql.NullTime"},
				{name: "ActiveQueries", type: "sql.NullInt64"},
				{name: "QueuedJobs", type: "sql.NullInt64"},
				{name: "RunningJobs", type: "sql.NullInt64"},
				{name: "CompletedJobs", type: "sql.NullInt64"},
				{name: "StoredJobs", type: "sql.NullInt64"},
				{name: "CleanedJobs", type: "sql.NullInt64"},
				{name: "QueryResultTtlSeconds", type: "sql.NullInt64"},
			]
		}
		select: {
			from: "compute_endpoints"
			alias: "ce"
			columns: [
				{expr: "ce.id"},
				{expr: "ce.external_id"},
				{expr: "ce.name"},
				{expr: "ce.url"},
				{expr: "ce.type"},
				{expr: "ce.status"},
				{expr: "ce.size"},
				{expr: "ce.max_memory_gb"},
				{expr: "ce.auth_token"},
				{expr: "ce.owner"},
				{expr: "ce.created_at"},
				{expr: "ce.updated_at"},
				{expr: "ce.selection_policy"},
				{expr: "ce.workload_class"},
				{expr: "ce.readiness_status"},
				{expr: "ce.max_concurrency"},
				{expr: "ce.max_result_size_mb"},
				{expr: "ce.recommended_for_large_queries"},
				{expr: "ce.is_draining"},
				{expr: "ce.last_health_status"},
				{expr: "ce.last_health_checked_at"},
				{expr: "ce.active_queries"},
				{expr: "ce.queued_jobs"},
				{expr: "ce.running_jobs"},
				{expr: "ce.completed_jobs"},
				{expr: "ce.stored_jobs"},
				{expr: "ce.cleaned_jobs"},
				{expr: "ce.query_result_ttl_seconds"},
			]
			joins: [
				{type: "JOIN", table: "compute_assignments", alias: "ca", on: "ca.endpoint_id = ce.id"},
			]
			where: [
				{column: "ca.principal_id", op: "=", param: "PrincipalID"},
				{column: "ca.principal_type", op: "=", param: "PrincipalType"},
				{column: "ca.is_default", op: "=", valueSQL: "1"},
				{column: "ce.status", op: "=", valueSQL: "'ACTIVE'"},
			]
			limitSQL: "1"
		}
	},
	{
		name: "ListComputeEndpoints"
		kind: "many"
		params: [
			{name: "Limit", type: "int64"},
			{name: "Offset", type: "int64"},
		]
		result: {
			row: "ComputeEndpoint"
			fields: [
				{name: "ID", type: "string"},
				{name: "ExternalID", type: "string"},
				{name: "Name", type: "string"},
				{name: "Url", type: "string"},
				{name: "Type", type: "string"},
				{name: "Status", type: "string"},
				{name: "Size", type: "string"},
				{name: "MaxMemoryGb", type: "sql.NullInt64"},
				{name: "AuthToken", type: "string"},
				{name: "Owner", type: "string"},
				{name: "CreatedAt", type: "time.Time"},
				{name: "UpdatedAt", type: "time.Time"},
				{name: "SelectionPolicy", type: "string"},
				{name: "WorkloadClass", type: "string"},
				{name: "ReadinessStatus", type: "string"},
				{name: "MaxConcurrency", type: "sql.NullInt64"},
				{name: "MaxResultSizeMb", type: "sql.NullInt64"},
				{name: "RecommendedForLargeQueries", type: "int64"},
				{name: "IsDraining", type: "int64"},
				{name: "LastHealthStatus", type: "sql.NullString"},
				{name: "LastHealthCheckedAt", type: "sql.NullTime"},
				{name: "ActiveQueries", type: "sql.NullInt64"},
				{name: "QueuedJobs", type: "sql.NullInt64"},
				{name: "RunningJobs", type: "sql.NullInt64"},
				{name: "CompletedJobs", type: "sql.NullInt64"},
				{name: "StoredJobs", type: "sql.NullInt64"},
				{name: "CleanedJobs", type: "sql.NullInt64"},
				{name: "QueryResultTtlSeconds", type: "sql.NullInt64"},
			]
		}
		select: {
			from: "compute_endpoints"
			columns: [
				{expr: "id"},
				{expr: "external_id"},
				{expr: "name"},
				{expr: "url"},
				{expr: "type"},
				{expr: "status"},
				{expr: "size"},
				{expr: "max_memory_gb"},
				{expr: "auth_token"},
				{expr: "owner"},
				{expr: "created_at"},
				{expr: "updated_at"},
				{expr: "selection_policy"},
				{expr: "workload_class"},
				{expr: "readiness_status"},
				{expr: "max_concurrency"},
				{expr: "max_result_size_mb"},
				{expr: "recommended_for_large_queries"},
				{expr: "is_draining"},
				{expr: "last_health_status"},
				{expr: "last_health_checked_at"},
				{expr: "active_queries"},
				{expr: "queued_jobs"},
				{expr: "running_jobs"},
				{expr: "completed_jobs"},
				{expr: "stored_jobs"},
				{expr: "cleaned_jobs"},
				{expr: "query_result_ttl_seconds"},
			]
			orderBy: [
				{expr: "name"},
			]
			limitParam: "Limit"
			offsetParam: "Offset"
		}
	},
	{
		name: "ResolveEndpointForPrincipalByName"
		kind: "one"
		params: [
			{name: "name", type: "string"},
		]
		result: {
			row: "ComputeEndpoint"
			fields: [
				{name: "ID", type: "string"},
				{name: "ExternalID", type: "string"},
				{name: "Name", type: "string"},
				{name: "Url", type: "string"},
				{name: "Type", type: "string"},
				{name: "Status", type: "string"},
				{name: "Size", type: "string"},
				{name: "MaxMemoryGb", type: "sql.NullInt64"},
				{name: "AuthToken", type: "string"},
				{name: "Owner", type: "string"},
				{name: "CreatedAt", type: "time.Time"},
				{name: "UpdatedAt", type: "time.Time"},
				{name: "SelectionPolicy", type: "string"},
				{name: "WorkloadClass", type: "string"},
				{name: "ReadinessStatus", type: "string"},
				{name: "MaxConcurrency", type: "sql.NullInt64"},
				{name: "MaxResultSizeMb", type: "sql.NullInt64"},
				{name: "RecommendedForLargeQueries", type: "int64"},
				{name: "IsDraining", type: "int64"},
				{name: "LastHealthStatus", type: "sql.NullString"},
				{name: "LastHealthCheckedAt", type: "sql.NullTime"},
				{name: "ActiveQueries", type: "sql.NullInt64"},
				{name: "QueuedJobs", type: "sql.NullInt64"},
				{name: "RunningJobs", type: "sql.NullInt64"},
				{name: "CompletedJobs", type: "sql.NullInt64"},
				{name: "StoredJobs", type: "sql.NullInt64"},
				{name: "CleanedJobs", type: "sql.NullInt64"},
				{name: "QueryResultTtlSeconds", type: "sql.NullInt64"},
			]
		}
		select: {
			from: "compute_endpoints"
			alias: "ce"
			columns: [
				{expr: "ce.id"},
				{expr: "ce.external_id"},
				{expr: "ce.name"},
				{expr: "ce.url"},
				{expr: "ce.type"},
				{expr: "ce.status"},
				{expr: "ce.size"},
				{expr: "ce.max_memory_gb"},
				{expr: "ce.auth_token"},
				{expr: "ce.owner"},
				{expr: "ce.created_at"},
				{expr: "ce.updated_at"},
				{expr: "ce.selection_policy"},
				{expr: "ce.workload_class"},
				{expr: "ce.readiness_status"},
				{expr: "ce.max_concurrency"},
				{expr: "ce.max_result_size_mb"},
				{expr: "ce.recommended_for_large_queries"},
				{expr: "ce.is_draining"},
				{expr: "ce.last_health_status"},
				{expr: "ce.last_health_checked_at"},
				{expr: "ce.active_queries"},
				{expr: "ce.queued_jobs"},
				{expr: "ce.running_jobs"},
				{expr: "ce.completed_jobs"},
				{expr: "ce.stored_jobs"},
				{expr: "ce.cleaned_jobs"},
				{expr: "ce.query_result_ttl_seconds"},
			]
			joins: [
				{type: "JOIN", table: "compute_assignments", alias: "ca", on: "ca.endpoint_id = ce.id"},
				{type: "JOIN", table: "principals", alias: "p", on: "p.id = ca.principal_id AND ca.principal_type = 'user'"},
			]
			where: [
				{column: "p.name", op: "=", param: "name"},
				{column: "ca.is_default", op: "=", valueSQL: "1"},
				{column: "ce.status", op: "=", valueSQL: "'ACTIVE'"},
			]
			limitSQL: "1"
		}
	},
	{
		name: "UpdateComputeEndpoint"
		kind: "exec"
		params: [
			{name: "Url", type: "string"},
			{name: "Size", type: "string"},
			{name: "MaxMemoryGb", type: "sql.NullInt64"},
			{name: "MaxConcurrency", type: "sql.NullInt64"},
			{name: "MaxResultSizeMb", type: "sql.NullInt64"},
			{name: "SelectionPolicy", type: "string"},
			{name: "WorkloadClass", type: "string"},
			{name: "ReadinessStatus", type: "string"},
			{name: "RecommendedForLargeQueries", type: "int64"},
			{name: "IsDraining", type: "int64"},
			{name: "AuthToken", type: "string"},
			{name: "ID", type: "string"},
		]
		update: {
			table: "compute_endpoints"
			set: [
				{column: "url", value: {param: "Url"}},
				{column: "size", value: {param: "Size"}},
				{column: "max_memory_gb", value: {param: "MaxMemoryGb"}},
				{column: "max_concurrency", value: {param: "MaxConcurrency"}},
				{column: "max_result_size_mb", value: {param: "MaxResultSizeMb"}},
				{column: "selection_policy", value: {param: "SelectionPolicy"}},
				{column: "workload_class", value: {param: "WorkloadClass"}},
				{column: "readiness_status", value: {param: "ReadinessStatus"}},
				{column: "recommended_for_large_queries", value: {param: "RecommendedForLargeQueries"}},
				{column: "is_draining", value: {param: "IsDraining"}},
				{column: "auth_token", value: {param: "AuthToken"}},
				{column: "updated_at", value: {sql: "datetime('now')"}},
			]
			where: [
				{column: "id", op: "=", param: "ID"},
			]
		}
	},
	{
		name: "UpdateComputeEndpointHealth"
		kind: "exec"
		params: [
			{name: "LastHealthStatus", type: "sql.NullString"},
			{name: "ActiveQueries", type: "sql.NullInt64"},
			{name: "QueuedJobs", type: "sql.NullInt64"},
			{name: "RunningJobs", type: "sql.NullInt64"},
			{name: "CompletedJobs", type: "sql.NullInt64"},
			{name: "StoredJobs", type: "sql.NullInt64"},
			{name: "CleanedJobs", type: "sql.NullInt64"},
			{name: "QueryResultTtlSeconds", type: "sql.NullInt64"},
			{name: "ReadinessStatus", type: "string"},
			{name: "ID", type: "string"},
		]
		update: {
			table: "compute_endpoints"
			set: [
				{column: "last_health_status", value: {param: "LastHealthStatus"}},
				{column: "last_health_checked_at", value: {sql: "datetime('now')"}},
				{column: "active_queries", value: {param: "ActiveQueries"}},
				{column: "queued_jobs", value: {param: "QueuedJobs"}},
				{column: "running_jobs", value: {param: "RunningJobs"}},
				{column: "completed_jobs", value: {param: "CompletedJobs"}},
				{column: "stored_jobs", value: {param: "StoredJobs"}},
				{column: "cleaned_jobs", value: {param: "CleanedJobs"}},
				{column: "query_result_ttl_seconds", value: {param: "QueryResultTtlSeconds"}},
				{column: "readiness_status", value: {param: "ReadinessStatus"}},
				{column: "updated_at", value: {sql: "datetime('now')"}},
			]
			where: [
				{column: "id", op: "=", param: "ID"},
			]
		}
	},
	{
		name: "UpdateComputeEndpointStatus"
		kind: "exec"
		params: [
			{name: "Status", type: "string"},
			{name: "ID", type: "string"},
		]
		update: {
			table: "compute_endpoints"
			set: [
				{column: "status", value: {param: "Status"}},
				{column: "updated_at", value: {sql: "datetime('now')"}},
			]
			where: [
				{column: "id", op: "=", param: "ID"},
			]
		}
	},
]
