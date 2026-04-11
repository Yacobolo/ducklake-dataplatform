package querydefs

queries: [
	{
		name: "CountAssignmentsForEndpoint"
		kind: "one"
		params: [
			{name: "endpointID", type: "string"},
		]
		result: {scalar: "int64"}
		select: {
			from: "compute_assignments"
			columns: [
				{expr: "COUNT(*)"},
			]
			where: [
				{column: "endpoint_id", op: "=", param: "endpointID"},
			]
		}
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
		result: {
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
	{
		name: "DeleteComputeAssignment"
		kind: "exec"
		params: [
			{name: "id", type: "string"},
		]
		delete: {
			from: "compute_assignments"
			where: [
				{column: "id", op: "=", param: "id"},
			]
		}
	},
	{
		name: "ListAssignmentsForEndpoint"
		kind: "many"
		params: [
			{name: "EndpointID", type: "string"},
			{name: "Limit", type: "int64"},
			{name: "Offset", type: "int64"},
		]
		result: {
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
			limitParam: "Limit"
			offsetParam: "Offset"
		}
	},
]
