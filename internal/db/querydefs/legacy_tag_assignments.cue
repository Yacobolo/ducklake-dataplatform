package querydefs

queries: [
	#CountAll & {
		name:   "CountTagAssignments"
		_table: "tag_assignments"
	},
	#InsertReturningTable & {
		name:   "CreateTagAssignment"
		_table: "tag_assignments"
		params: [
			{name: "ID", type: "string"},
			{name: "TagID", type: "string"},
			{name: "SecurableType", type: "string"},
			{name: "SecurableID", type: "string"},
			{name: "ColumnName", type: "sql.NullString"},
			{name: "AssignedBy", type: "string"},
		]
		insert: {
			columns: [
				"id",
				"tag_id",
				"securable_type",
				"securable_id",
				"column_name",
				"assigned_by",
			]
			values: [
				{param: "ID"},
				{param: "TagID"},
				{param: "SecurableType"},
				{param: "SecurableID"},
				{param: "ColumnName"},
				{param: "AssignedBy"},
			]
		}
	},
	#DeleteByID & {
		name:   "DeleteTagAssignment"
		_table: "tag_assignments"
	},
	{
		name: "DeleteTagAssignmentsBySecurable"
		kind: "exec"
		params: [
			{name: "SecurableType", type: "string"},
			{name: "SecurableID", type: "string"},
		]
		delete: {
			from: "tag_assignments"
			where: [
				{column: "securable_type", op: "=", param: "SecurableType"},
				{column: "securable_id", op: "=", param: "SecurableID"},
			]
		}
	},
	{
		name: "DeleteTagAssignmentsBySecurableTypes"
		kind: "exec"
		params: [
			{name: "SecurableType", type: "string"},
			{name: "SecurableType_2", type: "string"},
			{name: "SecurableID", type: "string"},
		]
		delete: {
			from: "tag_assignments"
			where: [
				{
					any: [
						{column: "securable_type", op: "=", param: "SecurableType"},
						{column: "securable_type", op: "=", param: "SecurableType_2"},
					]
				},
				{column: "securable_id", op: "=", param: "SecurableID"},
			]
		}
	},
	{
		name: "ListAssignmentsForTag"
		kind: "many"
		params: [
			{name: "tagID", type: "string"},
		]
		result: {table: "tag_assignments"}
		select: {
			from: "tag_assignments"
			where: [
				{column: "tag_id", op: "=", param: "tagID"},
			]
		}
	},
	#ListPaginatedOrdered & {
		name:   "ListTagAssignments"
		_table: "tag_assignments"
		_order: [
			{expr: "id"},
		]
	},
]
