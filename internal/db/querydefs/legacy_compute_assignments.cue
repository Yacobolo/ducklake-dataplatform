package querydefs

#ComputeAssignmentResult: {
	row: "ComputeAssignment"
	fields: [
		{name: "ID", type: "string"},
		{name: "PrincipalID", type: "string"},
		{name: "PrincipalType", type: "string"},
		{name: "EndpointID", type: "string"},
		{name: "IsDefault", type: "int64"},
		{name: "FallbackLocal", type: "int64"},
		{name: "CreatedAt", type: "time.Time"},
	]
}

queries: [
	#CountFiltered & {
		name:   "CountAssignmentsForEndpoint"
		_table: "compute_assignments"
		_params: [
			{name: "endpointID", type: "string"},
		]
		_where: [
			{column: "endpoint_id", op: "=", param: "endpointID"},
		]
	},
	{
		name: "CreateComputeAssignment"
		kind: "one"
		params: [
			{name: "ID", type: "string"},
			{name: "PrincipalID", type: "string"},
			{name: "PrincipalType", type: "string"},
			{name: "EndpointID", type: "string"},
			{name: "IsDefault", type: "int64"},
			{name: "FallbackLocal", type: "int64"},
		]
		result: #ComputeAssignmentResult
		insert: {
			into: "compute_assignments"
			columns: [
				"id",
				"principal_id",
				"principal_type",
				"endpoint_id",
				"is_default",
				"fallback_local",
			]
			values: [
				{param: "ID"},
				{param: "PrincipalID"},
				{param: "PrincipalType"},
				{param: "EndpointID"},
				{param: "IsDefault"},
				{param: "FallbackLocal"},
			]
			returningColumns: [
				{expr: "id"},
				{expr: "principal_id"},
				{expr: "principal_type"},
				{expr: "endpoint_id"},
				{expr: "is_default"},
				{expr: "fallback_local"},
				{expr: "created_at"},
			]
		}
	},
	#DeleteByID & {
		name:   "DeleteComputeAssignment"
		_table: "compute_assignments"
	},
	{
		name: "ListAssignmentsForEndpoint"
		kind: "many"
		params: [
			{name: "EndpointID", type: "string"},
			{name: "Limit", type: "int64"},
			{name: "Offset", type: "int64"},
		]
		result: #ComputeAssignmentResult
		select: {
			from: "compute_assignments"
			columns: [
				{expr: "id"},
				{expr: "principal_id"},
				{expr: "principal_type"},
				{expr: "endpoint_id"},
				{expr: "is_default"},
				{expr: "fallback_local"},
				{expr: "created_at"},
			]
			where: [
				{column: "endpoint_id", op: "=", param: "EndpointID"},
			]
			orderBy: [
				{expr: "id"},
			]
			limitParam:  "Limit"
			offsetParam: "Offset"
		}
	},
]
