package querydefs

queries: [
	#CountFiltered & {
		name:   "CountGroupMembers"
		_table: "group_members"
		_params: [
			{name: "groupID", type: "string"},
		]
		_where: [
			{column: "group_id", op: "=", param: "groupID"},
		]
	},
	{
		name: "ListGroupMembers"
		kind: "many"
		params: [
			{name: "groupID", type: "string"},
		]
		result: {table: "group_members"}
		select: {
			from: "group_members"
			where: [
				{column: "group_id", op: "=", param: "groupID"},
			]
		}
	},
	#ListFilteredPaginatedOrdered & {
		name:   "ListGroupMembersPaginated"
		_table: "group_members"
		_params: [
			{name: "GroupID", type: "string"},
		]
		_where: [
			{column: "group_id", op: "=", param: "GroupID"},
		]
		_order: [
			{expr: "member_id"},
		]
	},
	{
		name: "RemoveGroupMember"
		kind: "exec"
		params: [
			{name: "GroupID", type: "string"},
			{name: "MemberType", type: "string"},
			{name: "MemberID", type: "string"},
		]
		delete: {
			from: "group_members"
			where: [
				{column: "group_id", op: "=", param: "GroupID"},
				{column: "member_type", op: "=", param: "MemberType"},
				{column: "member_id", op: "=", param: "MemberID"},
			]
		}
	},
]
