package querydefs

queries: [
	{
		name: "CountGroupMembers"
		kind: "one"
		params: [
			{name: "groupID", type: "string"},
		]
		result: {scalar: "int64"}
		select: {
			from: "group_members"
			columns: [
				{expr: "COUNT(*)", alias: "cnt"},
			]
			where: [
				{column: "group_id", op: "=", param: "groupID"},
			]
		}
	},
	{
		name: "ListGroupMembers"
		kind: "many"
		params: [
			{name: "groupID", type: "string"},
		]
		result: {
			row: "GroupMember"
			fields: [
				{name: "GroupID", type: "string"},
				{name: "MemberType", type: "string"},
				{name: "MemberID", type: "string"},
			]
		}
		select: {
			from: "group_members"
			columns: [
				{expr: "group_id"},
				{expr: "member_type"},
				{expr: "member_id"},
			]
			where: [
				{column: "group_id", op: "=", param: "groupID"},
			]
		}
	},
	{
		name: "ListGroupMembersPaginated"
		kind: "many"
		params: [
			{name: "GroupID", type: "string"},
			{name: "Limit", type: "int64"},
			{name: "Offset", type: "int64"},
		]
		result: {
			row: "GroupMember"
			fields: [
				{name: "GroupID", type: "string"},
				{name: "MemberType", type: "string"},
				{name: "MemberID", type: "string"},
			]
		}
		select: {
			from: "group_members"
			columns: [
				{expr: "group_id"},
				{expr: "member_type"},
				{expr: "member_id"},
			]
			where: [
				{column: "group_id", op: "=", param: "GroupID"},
			]
			orderBy: [
				{expr: "member_id"},
			]
			limitParam: "Limit"
			offsetParam: "Offset"
		}
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
