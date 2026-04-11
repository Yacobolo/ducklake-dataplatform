package querydefs

queries: [
	{
		name: "CountTagAssignments"
		kind: "one"
		result: {scalar: "int64"}
		select: {
			from: "tag_assignments"
			columns: [
				{expr: "COUNT(*)", alias: "cnt"},
			]
		}
	},
	{
		name: "CreateTagAssignment"
		kind: "one"
		params: [
			{name: "ID", type: "string"},
			{name: "TagID", type: "string"},
			{name: "SecurableType", type: "string"},
			{name: "SecurableID", type: "string"},
			{name: "ColumnName", type: "sql.NullString"},
			{name: "AssignedBy", type: "string"},
		]
		result: {
			row: "TagAssignment"
			fields: [
				{name: "ID", type: "string"},
				{name: "TagID", type: "string"},
				{name: "SecurableType", type: "string"},
				{name: "SecurableID", type: "string"},
				{name: "ColumnName", type: "sql.NullString"},
				{name: "AssignedBy", type: "string"},
				{name: "AssignedAt", type: "string"},
			]
		}
		insert: {
			into: "tag_assignments"
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
			returningColumns: [
				{expr: "id"},
				{expr: "tag_id"},
				{expr: "securable_type"},
				{expr: "securable_id"},
				{expr: "column_name"},
				{expr: "assigned_by"},
				{expr: "assigned_at"},
			]
		}
	},
	{
		name: "DeleteTagAssignment"
		kind: "exec"
		params: [
			{name: "id", type: "string"},
		]
		delete: {
			from: "tag_assignments"
			where: [
				{column: "id", op: "=", param: "id"},
			]
		}
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
		result: {
			row: "TagAssignment"
			fields: [
				{name: "ID", type: "string"},
				{name: "TagID", type: "string"},
				{name: "SecurableType", type: "string"},
				{name: "SecurableID", type: "string"},
				{name: "ColumnName", type: "sql.NullString"},
				{name: "AssignedBy", type: "string"},
				{name: "AssignedAt", type: "string"},
			]
		}
		select: {
			from: "tag_assignments"
			columns: [
				{expr: "id"},
				{expr: "tag_id"},
				{expr: "securable_type"},
				{expr: "securable_id"},
				{expr: "column_name"},
				{expr: "assigned_by"},
				{expr: "assigned_at"},
			]
			where: [
				{column: "tag_id", op: "=", param: "tagID"},
			]
		}
	},
	{
		name: "ListTagAssignments"
		kind: "many"
		params: [
			{name: "Limit", type: "int64"},
			{name: "Offset", type: "int64"},
		]
		result: {
			row: "TagAssignment"
			fields: [
				{name: "ID", type: "string"},
				{name: "TagID", type: "string"},
				{name: "SecurableType", type: "string"},
				{name: "SecurableID", type: "string"},
				{name: "ColumnName", type: "sql.NullString"},
				{name: "AssignedBy", type: "string"},
				{name: "AssignedAt", type: "string"},
			]
		}
		select: {
			from: "tag_assignments"
			columns: [
				{expr: "id"},
				{expr: "tag_id"},
				{expr: "securable_type"},
				{expr: "securable_id"},
				{expr: "column_name"},
				{expr: "assigned_by"},
				{expr: "assigned_at"},
			]
			orderBy: [
				{expr: "id"},
			]
			limitParam: "Limit"
			offsetParam: "Offset"
		}
	},
]
