package querydefs

queries: [
	{
		name: "CreateNotebook"
		kind: "one"
		params: [
			{name: "ID", type: "string"},
			{name: "Name", type: "string"},
			{name: "Description", type: "sql.NullString"},
			{name: "Owner", type: "string"},
		]
		result: {
			row: "Notebook"
			fields: [
				{name: "ID", type: "string"},
				{name: "Name", type: "string"},
				{name: "Description", type: "sql.NullString"},
				{name: "Owner", type: "string"},
				{name: "CreatedAt", type: "string"},
				{name: "UpdatedAt", type: "string"},
				{name: "GitRepoID", type: "sql.NullString"},
				{name: "GitPath", type: "sql.NullString"},
				{name: "FolderID", type: "sql.NullString"},
				{name: "ProjectOverrideID", type: "sql.NullString"},
				{name: "EnvironmentOverrideID", type: "sql.NullString"},
			]
		}
		insert: {
			into: "notebooks"
			columns: ["id", "name", "description", "owner"]
			values: [
				{param: "ID"},
				{param: "Name"},
				{param: "Description"},
				{param: "Owner"},
			]
			returningColumns: [
				{expr: "id"},
				{expr: "name"},
				{expr: "description"},
				{expr: "owner"},
				{expr: "created_at"},
				{expr: "updated_at"},
				{expr: "git_repo_id"},
				{expr: "git_path"},
				{expr: "folder_id"},
				{expr: "project_override_id"},
				{expr: "environment_override_id"},
			]
		}
	},
	{
		name: "DeleteNotebook"
		kind: "exec"
		params: [
			{name: "id", type: "string"},
		]
		delete: {
			from: "notebooks"
			where: [
				{column: "id", op: "=", param: "id"},
			]
		}
	},
	{
		name: "GetNotebook"
		kind: "one"
		params: [
			{name: "id", type: "string"},
		]
		result: {
			row: "Notebook"
			fields: [
				{name: "ID", type: "string"},
				{name: "Name", type: "string"},
				{name: "Description", type: "sql.NullString"},
				{name: "Owner", type: "string"},
				{name: "CreatedAt", type: "string"},
				{name: "UpdatedAt", type: "string"},
				{name: "GitRepoID", type: "sql.NullString"},
				{name: "GitPath", type: "sql.NullString"},
				{name: "FolderID", type: "sql.NullString"},
				{name: "ProjectOverrideID", type: "sql.NullString"},
				{name: "EnvironmentOverrideID", type: "sql.NullString"},
			]
		}
		select: {
			from: "notebooks"
			columns: [
				{expr: "id"},
				{expr: "name"},
				{expr: "description"},
				{expr: "owner"},
				{expr: "created_at"},
				{expr: "updated_at"},
				{expr: "git_repo_id"},
				{expr: "git_path"},
				{expr: "folder_id"},
				{expr: "project_override_id"},
				{expr: "environment_override_id"},
			]
			where: [
				{column: "id", op: "=", param: "id"},
			]
		}
	},
	{
		name: "UpdateNotebook"
		kind: "one"
		params: [
			{name: "Name", type: "string"},
			{name: "Description", type: "sql.NullString"},
			{name: "ID", type: "string"},
		]
		result: {
			row: "Notebook"
			fields: [
				{name: "ID", type: "string"},
				{name: "Name", type: "string"},
				{name: "Description", type: "sql.NullString"},
				{name: "Owner", type: "string"},
				{name: "CreatedAt", type: "string"},
				{name: "UpdatedAt", type: "string"},
				{name: "GitRepoID", type: "sql.NullString"},
				{name: "GitPath", type: "sql.NullString"},
				{name: "FolderID", type: "sql.NullString"},
				{name: "ProjectOverrideID", type: "sql.NullString"},
				{name: "EnvironmentOverrideID", type: "sql.NullString"},
			]
		}
		update: {
			table: "notebooks"
			set: [
				{column: "name", value: {param: "Name"}},
				{column: "description", value: {param: "Description"}},
				{column: "updated_at", value: {sql: "datetime('now')"}},
			]
			where: [
				{column: "id", op: "=", param: "ID"},
			]
			returningColumns: [
				{expr: "id"},
				{expr: "name"},
				{expr: "description"},
				{expr: "owner"},
				{expr: "created_at"},
				{expr: "updated_at"},
				{expr: "git_repo_id"},
				{expr: "git_path"},
				{expr: "folder_id"},
				{expr: "project_override_id"},
				{expr: "environment_override_id"},
			]
		}
	},
]
