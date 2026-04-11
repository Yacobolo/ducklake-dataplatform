package querydefs

queries: [
	#CountAll & {
		name:   "CountGitRepos"
		_table: "git_repos"
	},
	#InsertReturningTable & {
		name:   "CreateGitRepo"
		_table: "git_repos"
		params: [
			{name: "ID", type: "string"},
			{name: "Url", type: "string"},
			{name: "Branch", type: "string"},
			{name: "Path", type: "string"},
			{name: "AuthToken", type: "string"},
			{name: "WebhookSecret", type: "sql.NullString"},
			{name: "Owner", type: "string"},
		]
		insert: {
			columns: [
				"id",
				"url",
				"branch",
				"path",
				"auth_token",
				"webhook_secret",
				"owner",
			]
			values: [
				{param: "ID"},
				{param: "Url"},
				{param: "Branch"},
				{param: "Path"},
				{param: "AuthToken"},
				{param: "WebhookSecret"},
				{param: "Owner"},
			]
		}
	},
	#DeleteByID & {
		name:   "DeleteGitRepo"
		_table: "git_repos"
	},
	#GetByID & {
		name:   "GetGitRepo"
		_table: "git_repos"
	},
	#ListPaginatedOrdered & {
		name:   "ListGitRepos"
		_table: "git_repos"
		_order: [
			{expr: "created_at", desc: true},
		]
	},
	#UpdateByIDTouch & {
		name:   "UpdateGitRepoSyncStatus"
		_table: "git_repos"
		params: [
			{name: "LastCommit", type: "sql.NullString"},
			{name: "LastSyncAt", type: "sql.NullString"},
			{name: "ID", type: "string"},
		]
		_set: [
			{column: "last_commit", value: {param: "LastCommit"}},
			{column: "last_sync_at", value: {param: "LastSyncAt"}},
		]
	},
]
