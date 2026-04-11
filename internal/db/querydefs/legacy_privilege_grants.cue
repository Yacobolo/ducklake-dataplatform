package querydefs

queries: [
	#CountFiltered & {
		name:   "CheckDirectGrant"
		_table: "privilege_grants"
		_params: [
			{name: "PrincipalID", type: "string"},
			{name: "PrincipalType", type: "string"},
			{name: "SecurableType", type: "string"},
			{name: "SecurableID", type: "string"},
			{name: "Privilege", type: "string"},
		]
		_where: [
			{column: "principal_id", op: "=", param: "PrincipalID"},
			{column: "principal_type", op: "=", param: "PrincipalType"},
			{column: "securable_type", op: "=", param: "SecurableType"},
			{column: "securable_id", op: "=", param: "SecurableID"},
			{column: "privilege", op: "=", param: "Privilege"},
		]
	},
	#CountFiltered & {
		name:   "CheckDirectGrantAny"
		_table: "privilege_grants"
		_params: [
			{name: "PrincipalID", type: "string"},
			{name: "PrincipalType", type: "string"},
			{name: "SecurableType", type: "string"},
			{name: "SecurableID", type: "string"},
			{name: "Privilege", type: "string"},
		]
		_where: [
			{column: "principal_id", op: "=", param: "PrincipalID"},
			{column: "principal_type", op: "=", param: "PrincipalType"},
			{column: "securable_type", op: "=", param: "SecurableType"},
			{column: "securable_id", op: "=", param: "SecurableID"},
			{
				any: [
					{column: "privilege", op: "=", valueSQL: "'ALL_PRIVILEGES'"},
					{column: "privilege", op: "=", param: "Privilege"},
				]
			},
		]
	},
	#CountAll & {
		name:   "CountAllGrants"
		_table: "privilege_grants"
	},
	#CountFiltered & {
		name:   "CountGrantsForPrincipal"
		_table: "privilege_grants"
		_params: [
			{name: "PrincipalID", type: "string"},
			{name: "PrincipalType", type: "string"},
		]
		_where: [
			{column: "principal_id", op: "=", param: "PrincipalID"},
			{column: "principal_type", op: "=", param: "PrincipalType"},
		]
	},
	#CountFiltered & {
		name:   "CountGrantsForSecurable"
		_table: "privilege_grants"
		_params: [
			{name: "SecurableType", type: "string"},
			{name: "SecurableID", type: "string"},
		]
		_where: [
			{column: "securable_type", op: "=", param: "SecurableType"},
			{column: "securable_id", op: "=", param: "SecurableID"},
		]
	},
	#InsertReturningTable & {
		name:   "GrantPrivilege"
		_table: "privilege_grants"
		params: [
			{name: "ID", type: "string"},
			{name: "PrincipalID", type: "string"},
			{name: "PrincipalType", type: "string"},
			{name: "SecurableType", type: "string"},
			{name: "SecurableID", type: "string"},
			{name: "Privilege", type: "string"},
			{name: "GrantedBy", type: "sql.NullString"},
		]
		insert: {
			columns: [
				"id",
				"principal_id",
				"principal_type",
				"securable_type",
				"securable_id",
				"privilege",
				"granted_by",
			]
			values: [
				{param: "ID"},
				{param: "PrincipalID"},
				{param: "PrincipalType"},
				{param: "SecurableType"},
				{param: "SecurableID"},
				{param: "Privilege"},
				{param: "GrantedBy"},
			]
		}
	},
	{
		name: "ListAllGrantsForIdentities"
		kind: "many"
		params: [
			{name: "PrincipalID", type: "string"},
			{name: "MemberID", type: "string"},
		]
		result: {table: "privilege_grants"}
		raw: {
			sql: "-- name: ListAllGrantsForIdentities :many\nSELECT id, principal_id, principal_type, securable_type, securable_id, privilege, granted_by, granted_at FROM privilege_grants\nWHERE (principal_type = 'user' AND principal_id = ?)\n   OR (principal_type = 'group' AND principal_id IN (\n       SELECT group_id FROM group_members WHERE member_type = 'user' AND member_id = ?\n   ))"
			bind: ["PrincipalID", "MemberID"]
		}
	},
	#ListPaginatedOrdered & {
		name:   "ListAllGrantsPaginated"
		_table: "privilege_grants"
		_order: [{expr: "id"}]
	},
	{
		name: "ListGrantsForPrincipal"
		kind: "many"
		params: [
			{name: "PrincipalID", type: "string"},
			{name: "PrincipalType", type: "string"},
		]
		result: {table: "privilege_grants"}
		select: {
			from: "privilege_grants"
			where: [
				{column: "principal_id", op: "=", param: "PrincipalID"},
				{column: "principal_type", op: "=", param: "PrincipalType"},
			]
		}
	},
	{
		name: "ListGrantsForPrincipalOnSecurable"
		kind: "many"
		params: [
			{name: "PrincipalID", type: "string"},
			{name: "PrincipalType", type: "string"},
			{name: "SecurableType", type: "string"},
			{name: "SecurableID", type: "string"},
		]
		result: {table: "privilege_grants"}
		select: {
			from: "privilege_grants"
			where: [
				{column: "principal_id", op: "=", param: "PrincipalID"},
				{column: "principal_type", op: "=", param: "PrincipalType"},
				{column: "securable_type", op: "=", param: "SecurableType"},
				{column: "securable_id", op: "=", param: "SecurableID"},
			]
		}
	},
	#ListFilteredPaginatedOrdered & {
		name:   "ListGrantsForPrincipalPaginated"
		_table: "privilege_grants"
		_params: [
			{name: "PrincipalID", type: "string"},
			{name: "PrincipalType", type: "string"},
		]
		_where: [
			{column: "principal_id", op: "=", param: "PrincipalID"},
			{column: "principal_type", op: "=", param: "PrincipalType"},
		]
		_order: [{expr: "id"}]
	},
	{
		name: "ListGrantsForSecurable"
		kind: "many"
		params: [
			{name: "SecurableType", type: "string"},
			{name: "SecurableID", type: "string"},
		]
		result: {table: "privilege_grants"}
		select: {
			from: "privilege_grants"
			where: [
				{column: "securable_type", op: "=", param: "SecurableType"},
				{column: "securable_id", op: "=", param: "SecurableID"},
			]
		}
	},
	#ListFilteredPaginatedOrdered & {
		name:   "ListGrantsForSecurablePaginated"
		_table: "privilege_grants"
		_params: [
			{name: "SecurableType", type: "string"},
			{name: "SecurableID", type: "string"},
		]
		_where: [
			{column: "securable_type", op: "=", param: "SecurableType"},
			{column: "securable_id", op: "=", param: "SecurableID"},
		]
		_order: [{expr: "id"}]
	},
	{
		name: "RevokePrivilege"
		kind: "exec"
		params: [
			{name: "PrincipalID", type: "string"},
			{name: "PrincipalType", type: "string"},
			{name: "SecurableType", type: "string"},
			{name: "SecurableID", type: "string"},
			{name: "Privilege", type: "string"},
		]
		delete: {
			from: "privilege_grants"
			where: [
				{column: "principal_id", op: "=", param: "PrincipalID"},
				{column: "principal_type", op: "=", param: "PrincipalType"},
				{column: "securable_type", op: "=", param: "SecurableType"},
				{column: "securable_id", op: "=", param: "SecurableID"},
				{column: "privilege", op: "=", param: "Privilege"},
			]
		}
	},
	#DeleteByID & {
		name:   "RevokePrivilegeByID"
		_table: "privilege_grants"
	},
]
