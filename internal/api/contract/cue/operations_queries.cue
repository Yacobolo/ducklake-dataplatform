package api

// Authored query operations.

#queriesTag: "Queries"

#wrappedQueryOperation: #genericOperationSpec & {
	wrapped: true
}

#queryIDPathParameter: #pathStringParameter & {
	#name: "query_id"
}

#queryJobStatusParameter: {
	name:    "status"
	in:      "query"
	explode: false
	schema: {
		ref: "QueryJobStatus"
	}
}

#auditDecisionStatusParameter: {
	name:    "status"
	in:      "query"
	explode: false
	schema: {
		ref: "AuditDecisionStatus"
	}
}

#principalNameQueryParameter: #queryStringParameter & {
	#name: "principal_name"
}

#fromDateTimeQueryParameter: {
	name:    "from"
	in:      "query"
	explode: false
	schema: {
		type:   "string"
		format: "date-time"
	}
}

#toDateTimeQueryParameter: {
	name:    "to"
	in:      "query"
	explode: false
	schema: {
		type:   "string"
		format: "date-time"
	}
}

#queryPathParameters: [
	#queryIDPathParameter,
]

#queryResultParameters: [
	#queryIDPathParameter,
	#paginationParameters[0],
	#paginationParameters[1],
]

#listQueriesParameters: [
	#queryJobStatusParameter,
	#paginationParameters[0],
	#paginationParameters[1],
]

#queryHistoryParameters: [
	#principalNameQueryParameter,
	#auditDecisionStatusParameter,
	#fromDateTimeQueryParameter,
	#toDateTimeQueryParameter,
	#paginationParameters[0],
	#paginationParameters[1],
]

#deleteQueryOperation: #genericOperationSpec & {
	wrapped: false
	kind:    "no_content"
	method:  "delete"
	op:      "deleteQuery"
	path:    "/queries/{query_id}"
	summary: "Delete query"
	cli:     "query delete"
	error_family: "mutating"
	params:  #queryPathParameters
}

#adminOnlyQueryAuthz: {
	mode: "admin_only"
}

#queryOps: [
	#wrappedQueryOperation & {
		kind:         "response"
		method:       "post"
		op:           "executeQuery"
		path:         "/query-executions"
		summary:      "Execute query"
		description:  "Executes a SQL statement synchronously and returns the first page of results in the response body."
		cli:          "query"
		returns:      "QueryResult"
		error_family: "mutating"
		body_ref:     "QueryRequest"
		body_description: "Request payload"
	},
	#wrappedQueryOperation & {
		kind:           "response"
		method:         "post"
		op:             "submitQuery"
		path:           "/queries"
		summary:        "Submit query"
		description:    "Submits a SQL query for asynchronous execution and returns a query job identifier for polling and result retrieval."
		cli:            "query submit"
		returns:        "SubmitQueryResponse"
		success_status: 202
		error_family:   "mutating"
		body_ref:       "SubmitQueryRequest"
		body_description: "Request payload"
	},
	#wrappedQueryOperation & {
		kind:         "response"
		method:       "get"
		op:           "listQueries"
		path:         "/queries"
		summary:      "List queries"
		description:  "Lists asynchronous query jobs created by the authenticated principal and supports filtering by lifecycle status."
		returns:      "PaginatedQueryJobs"
		error_family: "standard"
		params:       #listQueriesParameters
	},
	#wrappedQueryOperation & {
		kind:         "response"
		method:       "get"
		op:           "getQuery"
		path:         "/queries/{query_id}"
		summary:      "Get query"
		cli:          "query status"
		returns:      "QueryJob"
		error_family: "resource"
		params:       #queryPathParameters
	},
	#deleteQueryOperation,
	#wrappedQueryOperation & {
		kind:         "response"
		method:       "get"
		op:           "getQueryResults"
		path:         "/queries/{query_id}/results"
		summary:      "Get query results"
		description:  "Returns a page of rows for a previously submitted query using the stored query job identifier."
		cli:          "query results"
		returns:      "QueryResult"
		error_family: "resource"
		params:       #queryResultParameters
	},
	#wrappedQueryOperation & {
		kind:           "response"
		method:         "post"
		op:             "cancelQuery"
		path:           "/queries/{query_id}/cancellations"
		summary:        "Cancel query"
		cli:            "query cancel"
		returns:        "CancelQueryResponse"
		success_status: 202
		error_family:   "resource"
		params:         #queryPathParameters
	},
	#wrappedQueryOperation & {
		kind:         "response"
		method:       "get"
		op:           "listQueryHistory"
		path:         "/queries/history"
		summary:      "List query history"
		description:  "Lists recorded query execution history and supports filtering by principal, decision status, and time window."
		cli:          "query history list"
		returns:      "PaginatedQueryHistoryEntries"
		error_family: "guarded_read"
		params:       #queryHistoryParameters
		authz_default: false
		authz:         #adminOnlyQueryAuthz
	},
]

endpoints_queries: [
	for op in #queryOps {
		(#endpointFromGenericOperation & {
			tag:  #queriesTag
			spec: op
		}).endpoint
	},
]
