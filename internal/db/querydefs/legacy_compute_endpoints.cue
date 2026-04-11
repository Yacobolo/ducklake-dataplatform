package querydefs

queries: [
	#CountAll & {
		name:   "CountComputeEndpoints"
		_table: "compute_endpoints"
	},
	#InsertReturningTable & {
		name:   "CreateComputeEndpoint"
		_table: "compute_endpoints"
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
		insert: {
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
		}
	},
	#DeleteByID & {
		name:   "DeleteComputeEndpoint"
		_table: "compute_endpoints"
	},
	{
		name: "GetAssignmentsForPrincipal"
		kind: "many"
		params: [
			{name: "PrincipalID", type: "string"},
			{name: "PrincipalType", type: "string"},
		]
		result: {table: "compute_endpoints"}
		select: {
			from:  "compute_endpoints"
			alias: "ce"
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
	#GetByID & {
		name:   "GetComputeEndpoint"
		_table: "compute_endpoints"
	},
	#GetByStringField & {
		name:   "GetComputeEndpointByName"
		_table: "compute_endpoints"
		_field: "name"
		_param: "name"
	},
	{
		name: "GetDefaultEndpointForPrincipal"
		kind: "one"
		params: [
			{name: "PrincipalID", type: "string"},
			{name: "PrincipalType", type: "string"},
		]
		result: {table: "compute_endpoints"}
		select: {
			from:  "compute_endpoints"
			alias: "ce"
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
	#ListPaginatedOrdered & {
		name:   "ListComputeEndpoints"
		_table: "compute_endpoints"
		_order: [{expr: "name"}]
	},
	{
		name: "ResolveEndpointForPrincipalByName"
		kind: "one"
		params: [
			{name: "name", type: "string"},
		]
		result: {table: "compute_endpoints"}
		select: {
			from:  "compute_endpoints"
			alias: "ce"
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
	#UpdateByIDTouch & {
		name:   "UpdateComputeEndpoint"
		_table: "compute_endpoints"
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
		_set: [
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
		]
	},
	#UpdateByIDTouch & {
		name:   "UpdateComputeEndpointHealth"
		_table: "compute_endpoints"
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
		_set: [
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
		]
	},
	#UpdateByIDTouch & {
		name:   "UpdateComputeEndpointStatus"
		_table: "compute_endpoints"
		params: [
			{name: "Status", type: "string"},
			{name: "ID", type: "string"},
		]
		_set: [
			{column: "status", value: {param: "Status"}},
		]
	},
]
