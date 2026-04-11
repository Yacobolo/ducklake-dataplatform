package querydefs

queries: [
	{
		name: "CheckDirectGrant"
		kind: "one"
		params: [
			{name: "PrincipalID", type: "string"},
			{name: "PrincipalType", type: "string"},
			{name: "SecurableType", type: "string"},
			{name: "SecurableID", type: "string"},
			{name: "Privilege", type: "string"},
		]
		result: {scalar: "int64"}
		select: {
			from: "privilege_grants"
			columns: [
				{expr: "COUNT(*)", alias: "cnt"},
			]
			where: [
				{column: "principal_id", op: "=", param: "PrincipalID"},
				{column: "principal_type", op: "=", param: "PrincipalType"},
				{column: "securable_type", op: "=", param: "SecurableType"},
				{column: "securable_id", op: "=", param: "SecurableID"},
				{column: "privilege", op: "=", param: "Privilege"},
			]
		}
	},
	{
		name: "CheckDirectGrantAny"
		kind: "one"
		params: [
			{name: "PrincipalID", type: "string"},
			{name: "PrincipalType", type: "string"},
			{name: "SecurableType", type: "string"},
			{name: "SecurableID", type: "string"},
			{name: "Privilege", type: "string"},
		]
		result: {scalar: "int64"}
		select: {
			from: "privilege_grants"
			columns: [
				{expr: "COUNT(*)", alias: "cnt"},
			]
			where: [
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
		}
	},
	{
		name: "CountAllGrants"
		kind: "one"
		result: {scalar: "int64"}
		select: {
			from: "privilege_grants"
			columns: [
				{expr: "COUNT(*)", alias: "cnt"},
			]
		}
	},
	{
		name: "CountGrantsForPrincipal"
		kind: "one"
		params: [
			{name: "PrincipalID", type: "string"},
			{name: "PrincipalType", type: "string"},
		]
		result: {scalar: "int64"}
		select: {
			from: "privilege_grants"
			columns: [
				{expr: "COUNT(*)", alias: "cnt"},
			]
			where: [
				{column: "principal_id", op: "=", param: "PrincipalID"},
				{column: "principal_type", op: "=", param: "PrincipalType"},
			]
		}
	},
	{
		name: "CountGrantsForSecurable"
		kind: "one"
		params: [
			{name: "SecurableType", type: "string"},
			{name: "SecurableID", type: "string"},
		]
		result: {scalar: "int64"}
		select: {
			from: "privilege_grants"
			columns: [
				{expr: "COUNT(*)", alias: "cnt"},
			]
			where: [
				{column: "securable_type", op: "=", param: "SecurableType"},
				{column: "securable_id", op: "=", param: "SecurableID"},
			]
		}
	},
	{
		name: "GrantPrivilege"
		kind: "one"
		params: [
			{name: "ID", type: "string"},
			{name: "PrincipalID", type: "string"},
			{name: "PrincipalType", type: "string"},
			{name: "SecurableType", type: "string"},
			{name: "SecurableID", type: "string"},
			{name: "Privilege", type: "string"},
			{name: "GrantedBy", type: "sql.NullString"},
		]
		result: {
			row: "PrivilegeGrant"
			fields: [
				{name: "ID", type: "string"},
				{name: "PrincipalID", type: "string"},
				{name: "PrincipalType", type: "string"},
				{name: "SecurableType", type: "string"},
				{name: "SecurableID", type: "string"},
				{name: "Privilege", type: "string"},
				{name: "GrantedBy", type: "sql.NullString"},
				{name: "GrantedAt", type: "string"},
			]
		}
		insert: {
			into: "privilege_grants"
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
			returningColumns: [
				{expr: "id"},
				{expr: "principal_id"},
				{expr: "principal_type"},
				{expr: "securable_type"},
				{expr: "securable_id"},
				{expr: "privilege"},
				{expr: "granted_by"},
				{expr: "granted_at"},
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
		result: {
			row: "PrivilegeGrant"
			fields: [
				{name: "ID", type: "string"},
				{name: "PrincipalID", type: "string"},
				{name: "PrincipalType", type: "string"},
				{name: "SecurableType", type: "string"},
				{name: "SecurableID", type: "string"},
				{name: "Privilege", type: "string"},
				{name: "GrantedBy", type: "sql.NullString"},
				{name: "GrantedAt", type: "string"},
			]
		}
		raw: {
			sql: "-- name: ListAllGrantsForIdentities :many\nSELECT id, principal_id, principal_type, securable_type, securable_id, privilege, granted_by, granted_at FROM privilege_grants\nWHERE (principal_type = 'user' AND principal_id = ?)\n   OR (principal_type = 'group' AND principal_id IN (\n       SELECT group_id FROM group_members WHERE member_type = 'user' AND member_id = ?\n   ))"
			bind: ["PrincipalID", "MemberID"]
		}
	},
	{
		name: "ListAllGrantsPaginated"
		kind: "many"
		params: [
			{name: "Limit", type: "int64"},
			{name: "Offset", type: "int64"},
		]
		result: {
			row: "PrivilegeGrant"
			fields: [
				{name: "ID", type: "string"},
				{name: "PrincipalID", type: "string"},
				{name: "PrincipalType", type: "string"},
				{name: "SecurableType", type: "string"},
				{name: "SecurableID", type: "string"},
				{name: "Privilege", type: "string"},
				{name: "GrantedBy", type: "sql.NullString"},
				{name: "GrantedAt", type: "string"},
			]
		}
		select: {
			from: "privilege_grants"
			columns: [
				{expr: "id"},
				{expr: "principal_id"},
				{expr: "principal_type"},
				{expr: "securable_type"},
				{expr: "securable_id"},
				{expr: "privilege"},
				{expr: "granted_by"},
				{expr: "granted_at"},
			]
			orderBy: [
				{expr: "id"},
			]
			limitParam: "Limit"
			offsetParam: "Offset"
		}
	},
	{
		name: "ListGrantsForPrincipal"
		kind: "many"
		params: [
			{name: "PrincipalID", type: "string"},
			{name: "PrincipalType", type: "string"},
		]
		result: {
			row: "PrivilegeGrant"
			fields: [
				{name: "ID", type: "string"},
				{name: "PrincipalID", type: "string"},
				{name: "PrincipalType", type: "string"},
				{name: "SecurableType", type: "string"},
				{name: "SecurableID", type: "string"},
				{name: "Privilege", type: "string"},
				{name: "GrantedBy", type: "sql.NullString"},
				{name: "GrantedAt", type: "string"},
			]
		}
		select: {
			from: "privilege_grants"
			columns: [
				{expr: "id"},
				{expr: "principal_id"},
				{expr: "principal_type"},
				{expr: "securable_type"},
				{expr: "securable_id"},
				{expr: "privilege"},
				{expr: "granted_by"},
				{expr: "granted_at"},
			]
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
		result: {
			row: "PrivilegeGrant"
			fields: [
				{name: "ID", type: "string"},
				{name: "PrincipalID", type: "string"},
				{name: "PrincipalType", type: "string"},
				{name: "SecurableType", type: "string"},
				{name: "SecurableID", type: "string"},
				{name: "Privilege", type: "string"},
				{name: "GrantedBy", type: "sql.NullString"},
				{name: "GrantedAt", type: "string"},
			]
		}
		select: {
			from: "privilege_grants"
			columns: [
				{expr: "id"},
				{expr: "principal_id"},
				{expr: "principal_type"},
				{expr: "securable_type"},
				{expr: "securable_id"},
				{expr: "privilege"},
				{expr: "granted_by"},
				{expr: "granted_at"},
			]
			where: [
				{column: "principal_id", op: "=", param: "PrincipalID"},
				{column: "principal_type", op: "=", param: "PrincipalType"},
				{column: "securable_type", op: "=", param: "SecurableType"},
				{column: "securable_id", op: "=", param: "SecurableID"},
			]
		}
	},
	{
		name: "ListGrantsForPrincipalPaginated"
		kind: "many"
		params: [
			{name: "PrincipalID", type: "string"},
			{name: "PrincipalType", type: "string"},
			{name: "Limit", type: "int64"},
			{name: "Offset", type: "int64"},
		]
		result: {
			row: "PrivilegeGrant"
			fields: [
				{name: "ID", type: "string"},
				{name: "PrincipalID", type: "string"},
				{name: "PrincipalType", type: "string"},
				{name: "SecurableType", type: "string"},
				{name: "SecurableID", type: "string"},
				{name: "Privilege", type: "string"},
				{name: "GrantedBy", type: "sql.NullString"},
				{name: "GrantedAt", type: "string"},
			]
		}
		select: {
			from: "privilege_grants"
			columns: [
				{expr: "id"},
				{expr: "principal_id"},
				{expr: "principal_type"},
				{expr: "securable_type"},
				{expr: "securable_id"},
				{expr: "privilege"},
				{expr: "granted_by"},
				{expr: "granted_at"},
			]
			where: [
				{column: "principal_id", op: "=", param: "PrincipalID"},
				{column: "principal_type", op: "=", param: "PrincipalType"},
			]
			orderBy: [
				{expr: "id"},
			]
			limitParam: "Limit"
			offsetParam: "Offset"
		}
	},
	{
		name: "ListGrantsForSecurable"
		kind: "many"
		params: [
			{name: "SecurableType", type: "string"},
			{name: "SecurableID", type: "string"},
		]
		result: {
			row: "PrivilegeGrant"
			fields: [
				{name: "ID", type: "string"},
				{name: "PrincipalID", type: "string"},
				{name: "PrincipalType", type: "string"},
				{name: "SecurableType", type: "string"},
				{name: "SecurableID", type: "string"},
				{name: "Privilege", type: "string"},
				{name: "GrantedBy", type: "sql.NullString"},
				{name: "GrantedAt", type: "string"},
			]
		}
		select: {
			from: "privilege_grants"
			columns: [
				{expr: "id"},
				{expr: "principal_id"},
				{expr: "principal_type"},
				{expr: "securable_type"},
				{expr: "securable_id"},
				{expr: "privilege"},
				{expr: "granted_by"},
				{expr: "granted_at"},
			]
			where: [
				{column: "securable_type", op: "=", param: "SecurableType"},
				{column: "securable_id", op: "=", param: "SecurableID"},
			]
		}
	},
	{
		name: "ListGrantsForSecurablePaginated"
		kind: "many"
		params: [
			{name: "SecurableType", type: "string"},
			{name: "SecurableID", type: "string"},
			{name: "Limit", type: "int64"},
			{name: "Offset", type: "int64"},
		]
		result: {
			row: "PrivilegeGrant"
			fields: [
				{name: "ID", type: "string"},
				{name: "PrincipalID", type: "string"},
				{name: "PrincipalType", type: "string"},
				{name: "SecurableType", type: "string"},
				{name: "SecurableID", type: "string"},
				{name: "Privilege", type: "string"},
				{name: "GrantedBy", type: "sql.NullString"},
				{name: "GrantedAt", type: "string"},
			]
		}
		select: {
			from: "privilege_grants"
			columns: [
				{expr: "id"},
				{expr: "principal_id"},
				{expr: "principal_type"},
				{expr: "securable_type"},
				{expr: "securable_id"},
				{expr: "privilege"},
				{expr: "granted_by"},
				{expr: "granted_at"},
			]
			where: [
				{column: "securable_type", op: "=", param: "SecurableType"},
				{column: "securable_id", op: "=", param: "SecurableID"},
			]
			orderBy: [
				{expr: "id"},
			]
			limitParam: "Limit"
			offsetParam: "Offset"
		}
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
	{
		name: "RevokePrivilegeByID"
		kind: "exec"
		params: [
			{name: "id", type: "string"},
		]
		delete: {
			from: "privilege_grants"
			where: [
				{column: "id", op: "=", param: "id"},
			]
		}
	},
]
