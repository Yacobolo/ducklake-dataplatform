package querydefs

queries: [
	{
		name: "CountGroups"
		kind: "one"
		result: {scalar: "int64"}
		select: {
			from: "groups"
			columns: [
				{expr: "COUNT(*)", alias: "cnt"},
			]
		}
	},
	{
		name: "CreateGroup"
		kind: "one"
		params: [
			{name: "ID", type: "string"},
			{name: "Name", type: "string"},
			{name: "Description", type: "sql.NullString"},
		]
		result: {
			row: "Group"
			fields: [
				{name: "ID", type: "string"},
				{name: "Name", type: "string"},
				{name: "Description", type: "sql.NullString"},
				{name: "CreatedAt", type: "string"},
			]
		}
		insert: {
			into: "groups"
			columns: ["id", "name", "description"]
			values: [
				{param: "ID"},
				{param: "Name"},
				{param: "Description"},
			]
			returningColumns: [
				{expr: "id"},
				{expr: "name"},
				{expr: "description"},
				{expr: "created_at"},
			]
		}
	},
	{
		name: "DeleteGroup"
		kind: "exec"
		params: [
			{name: "id", type: "string"},
		]
		delete: {
			from: "groups"
			where: [
				{column: "id", op: "=", param: "id"},
			]
		}
	},
	{
		name: "GetGroup"
		kind: "one"
		params: [
			{name: "id", type: "string"},
		]
		result: {
			row: "Group"
			fields: [
				{name: "ID", type: "string"},
				{name: "Name", type: "string"},
				{name: "Description", type: "sql.NullString"},
				{name: "CreatedAt", type: "string"},
			]
		}
		select: {
			from: "groups"
			columns: [
				{expr: "id"},
				{expr: "name"},
				{expr: "description"},
				{expr: "created_at"},
			]
			where: [
				{column: "id", op: "=", param: "id"},
			]
		}
	},
	{
		name: "GetGroupByName"
		kind: "one"
		params: [
			{name: "name", type: "string"},
		]
		result: {
			row: "Group"
			fields: [
				{name: "ID", type: "string"},
				{name: "Name", type: "string"},
				{name: "Description", type: "sql.NullString"},
				{name: "CreatedAt", type: "string"},
			]
		}
		select: {
			from: "groups"
			columns: [
				{expr: "id"},
				{expr: "name"},
				{expr: "description"},
				{expr: "created_at"},
			]
			where: [
				{column: "name", op: "=", param: "name"},
			]
		}
	},
	{
		name: "GetGroupsForMember"
		kind: "many"
		params: [
			{name: "MemberType", type: "string"},
			{name: "MemberID", type: "string"},
		]
		result: {
			row: "Group"
			fields: [
				{name: "ID", type: "string"},
				{name: "Name", type: "string"},
				{name: "Description", type: "sql.NullString"},
				{name: "CreatedAt", type: "string"},
			]
		}
		select: {
			from: "groups"
			alias: "g"
			columns: [
				{expr: "g.id"},
				{expr: "g.name"},
				{expr: "g.description"},
				{expr: "g.created_at"},
			]
			joins: [
				{type: "JOIN", table: "group_members", alias: "gm", on: "g.id = gm.group_id"},
			]
			where: [
				{column: "gm.member_type", op: "=", param: "MemberType"},
				{column: "gm.member_id", op: "=", param: "MemberID"},
			]
		}
	},
	{
		name: "ListGroups"
		kind: "many"
		result: {
			row: "Group"
			fields: [
				{name: "ID", type: "string"},
				{name: "Name", type: "string"},
				{name: "Description", type: "sql.NullString"},
				{name: "CreatedAt", type: "string"},
			]
		}
		select: {
			from: "groups"
			columns: [
				{expr: "id"},
				{expr: "name"},
				{expr: "description"},
				{expr: "created_at"},
			]
			orderBy: [
				{expr: "name"},
			]
		}
	},
	{
		name: "ListGroupsPaginated"
		kind: "many"
		params: [
			{name: "Limit", type: "int64"},
			{name: "Offset", type: "int64"},
		]
		result: {
			row: "Group"
			fields: [
				{name: "ID", type: "string"},
				{name: "Name", type: "string"},
				{name: "Description", type: "sql.NullString"},
				{name: "CreatedAt", type: "string"},
			]
		}
		select: {
			from: "groups"
			columns: [
				{expr: "id"},
				{expr: "name"},
				{expr: "description"},
				{expr: "created_at"},
			]
			orderBy: [
				{expr: "id"},
			]
			limitParam: "Limit"
			offsetParam: "Offset"
		}
	},
	{
		name: "UpdateGroup"
		kind: "one"
		params: [
			{name: "Description", type: "sql.NullString"},
			{name: "ID", type: "string"},
		]
		result: {
			row: "Group"
			fields: [
				{name: "ID", type: "string"},
				{name: "Name", type: "string"},
				{name: "Description", type: "sql.NullString"},
				{name: "CreatedAt", type: "string"},
			]
		}
		update: {
			table: "groups"
			set: [
				{column: "description", value: {param: "Description"}},
			]
			where: [
				{column: "id", op: "=", param: "ID"},
			]
			returningColumns: [
				{expr: "id"},
				{expr: "name"},
				{expr: "description"},
				{expr: "created_at"},
			]
		}
	},
]
