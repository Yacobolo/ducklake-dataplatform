package querydefs

queries: [
	#CountAll & {
		name:   "CountGroups"
		_table: "groups"
	},
	#InsertReturningTable & {
		name:   "CreateGroup"
		_table: "groups"
		params: [
			{name: "ID", type: "string"},
			{name: "Name", type: "string"},
			{name: "Description", type: "sql.NullString"},
		]
		insert: {
			columns: ["id", "name", "description"]
			values: [
				{param: "ID"},
				{param: "Name"},
				{param: "Description"},
			]
		}
	},
	#DeleteByID & {
		name:   "DeleteGroup"
		_table: "groups"
	},
	#GetByID & {
		name:   "GetGroup"
		_table: "groups"
	},
	#GetByStringField & {
		name:   "GetGroupByName"
		_table: "groups"
		_field: "name"
		_param: "name"
	},
	{
		name: "GetGroupsForMember"
		kind: "many"
		params: [
			{name: "MemberType", type: "string"},
			{name: "MemberID", type: "string"},
		]
		result: {table: "groups"}
		select: {
			from:  "groups"
			alias: "g"
			joins: [
				{type: "JOIN", table: "group_members", alias: "gm", on: "g.id = gm.group_id"},
			]
			where: [
				{column: "gm.member_type", op: "=", param: "MemberType"},
				{column: "gm.member_id", op: "=", param: "MemberID"},
			]
		}
	},
	#ListAllOrdered & {
		name:   "ListGroups"
		_table: "groups"
		_order: [{expr: "name"}]
	},
	#ListPaginatedOrdered & {
		name:   "ListGroupsPaginated"
		_table: "groups"
		_order: [{expr: "id"}]
	},
	{
		name: "UpdateGroup"
		kind: "one"
		params: [
			{name: "Description", type: "sql.NullString"},
			{name: "ID", type: "string"},
		]
		result: {table: "groups"}
		update: {
			table: "groups"
			set: [
				{column: "description", value: {param: "Description"}},
			]
			where: [{column: "id", op: "=", param: "ID"}]
			returning: true
		}
	},
]
