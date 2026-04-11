package querydefs

queries: [
	{
		name: "AddGroupMember"
		kind: "exec"
		paramMode: "struct"
		params: [
			{name: "GroupID", type: "string"},
			{name: "MemberType", type: "string"},
			{name: "MemberID", type: "string"},
		]
		raw: {
			sql: """
				-- name: AddGroupMember :exec
				INSERT OR IGNORE INTO group_members (group_id, member_type, member_id)
				VALUES (?, ?, ?)
				"""
			bind: ["GroupID", "MemberType", "MemberID"]
		}
	},
	{
		name: "BindColumnMask"
		kind: "exec"
		paramMode: "struct"
		params: [
			{name: "ID", type: "string"},
			{name: "ColumnMaskID", type: "string"},
			{name: "PrincipalID", type: "string"},
			{name: "PrincipalType", type: "string"},
			{name: "SeeOriginal", type: "int64"},
		]
		raw: {
			sql: """
				-- name: BindColumnMask :exec
				INSERT OR IGNORE INTO column_mask_bindings (id, column_mask_id, principal_id, principal_type, see_original)
				VALUES (?, ?, ?, ?, ?)
				"""
			bind: ["ID", "ColumnMaskID", "PrincipalID", "PrincipalType", "SeeOriginal"]
		}
	},
	{
		name: "BindExternalID"
		kind: "exec"
		paramMode: "struct"
		params: [
			{name: "ExternalID", type: "sql.NullString"},
			{name: "ExternalIssuer", type: "sql.NullString"},
			{name: "ID", type: "string"},
		]
		raw: {
			sql: """
				-- name: BindExternalID :exec
				UPDATE principals SET external_id = ?, external_issuer = ? WHERE id = ?
				"""
			bind: ["ExternalID", "ExternalIssuer", "ID"]
		}
	},
	{
		name: "BindRowFilter"
		kind: "exec"
		paramMode: "struct"
		params: [
			{name: "ID", type: "string"},
			{name: "RowFilterID", type: "string"},
			{name: "PrincipalID", type: "string"},
			{name: "PrincipalType", type: "string"},
		]
		raw: {
			sql: """
				-- name: BindRowFilter :exec
				INSERT OR IGNORE INTO row_filter_bindings (id, row_filter_id, principal_id, principal_type)
				VALUES (?, ?, ?, ?)
				"""
			bind: ["ID", "RowFilterID", "PrincipalID", "PrincipalType"]
		}
	},
	{
		name: "CancelPendingPipelineRuns"
		kind: "exec"
		paramMode: "single"
		params: [
			{name: "pipelineID", type: "string"},
		]
		raw: {
			sql: """
				-- name: CancelPendingPipelineRuns :exec
				UPDATE pipeline_runs SET status = 'CANCELLED' WHERE pipeline_id = ? AND status = 'PENDING'
				"""
			bind: ["pipelineID"]
		}
	},
	{
		name: "CheckDirectGrant"
		kind: "one"
		paramMode: "struct"
		params: [
			{name: "PrincipalID", type: "string"},
			{name: "PrincipalType", type: "string"},
			{name: "SecurableType", type: "string"},
			{name: "SecurableID", type: "string"},
			{name: "Privilege", type: "string"},
		]
		result: {
			scalar: "int64"
		}
		raw: {
			sql: """
				-- name: CheckDirectGrant :one
				SELECT COUNT(*) as cnt FROM privilege_grants
				WHERE principal_id = ? AND principal_type = ? AND securable_type = ? AND securable_id = ? AND privilege = ?
				"""
			bind: ["PrincipalID", "PrincipalType", "SecurableType", "SecurableID", "Privilege"]
		}
	},
	{
		name: "CheckDirectGrantAny"
		kind: "one"
		paramMode: "struct"
		params: [
			{name: "PrincipalID", type: "string"},
			{name: "PrincipalType", type: "string"},
			{name: "SecurableType", type: "string"},
			{name: "SecurableID", type: "string"},
			{name: "Privilege", type: "string"},
		]
		result: {
			scalar: "int64"
		}
		raw: {
			sql: """
				-- name: CheckDirectGrantAny :one
				SELECT COUNT(*) as cnt FROM privilege_grants
				WHERE principal_id = ? AND principal_type = ? AND securable_type = ? AND securable_id = ?
				  AND privilege IN ('ALL_PRIVILEGES', ?)
				"""
			bind: ["PrincipalID", "PrincipalType", "SecurableType", "SecurableID", "Privilege"]
		}
	},
	{
		name: "ClearDefaultCatalog"
		kind: "exec"
		raw: {
			sql: """
				-- name: ClearDefaultCatalog :exec
				UPDATE catalogs SET is_default = 0, updated_at = datetime('now') WHERE is_default = 1
				"""
		}
	},
	{
		name: "ClearSetupBootstrapToken"
		kind: "exec"
		raw: {
			sql: """
				-- name: ClearSetupBootstrapToken :exec
				UPDATE setup_state
				SET bootstrap_token_hash = NULL,
				    bootstrap_token_expires_at = NULL,
				    updated_at = CURRENT_TIMESTAMP
				WHERE id = 1
				"""
		}
	},
	{
		name: "CompleteSetupState"
		kind: "exec"
		paramMode: "single"
		params: [
			{name: "setupCompletedBy", type: "sql.NullString"},
		]
		raw: {
			sql: """
				-- name: CompleteSetupState :exec
				UPDATE setup_state
				SET setup_completed = 1,
				    setup_completed_at = CURRENT_TIMESTAMP,
				    setup_completed_by = ?,
				    updated_at = CURRENT_TIMESTAMP
				WHERE id = 1
				"""
			bind: ["setupCompletedBy"]
		}
	},
	{
		name: "CountAPIKeysForPrincipal"
		kind: "one"
		paramMode: "single"
		params: [
			{name: "principalID", type: "string"},
		]
		result: {
			scalar: "int64"
		}
		raw: {
			sql: """
				-- name: CountAPIKeysForPrincipal :one
				SELECT COUNT(*) as cnt FROM api_keys WHERE principal_id = ?
				"""
			bind: ["principalID"]
		}
	},
	{
		name: "CountActivePipelineRuns"
		kind: "one"
		paramMode: "single"
		params: [
			{name: "pipelineID", type: "string"},
		]
		result: {
			scalar: "int64"
		}
		raw: {
			sql: """
				-- name: CountActivePipelineRuns :one
				SELECT COUNT(*) FROM pipeline_runs WHERE pipeline_id = ? AND status IN ('PENDING', 'RUNNING')
				"""
			bind: ["pipelineID"]
		}
	},
	{
		name: "CountActiveWebSessions"
		kind: "one"
		result: {
			scalar: "int64"
		}
		raw: {
			sql: """
				-- name: CountActiveWebSessions :one
				SELECT COUNT(*)
				FROM web_sessions
				WHERE revoked_at IS NULL
				  AND expires_at > CURRENT_TIMESTAMP
				  AND idle_expires_at > CURRENT_TIMESTAMP
				"""
		}
	},
	{
		name: "CountAllAPIKeys"
		kind: "one"
		result: {
			scalar: "int64"
		}
		raw: {
			sql: """
				-- name: CountAllAPIKeys :one
				SELECT COUNT(*) as cnt FROM api_keys
				"""
		}
	},
	{
		name: "CountAllGrants"
		kind: "one"
		result: {
			scalar: "int64"
		}
		raw: {
			sql: """
				-- name: CountAllGrants :one
				SELECT COUNT(*) as cnt FROM privilege_grants
				"""
		}
	},
	{
		name: "CountAssignmentsForEndpoint"
		kind: "one"
		paramMode: "single"
		params: [
			{name: "endpointID", type: "string"},
		]
		result: {
			scalar: "int64"
		}
		raw: {
			sql: """
				-- name: CountAssignmentsForEndpoint :one
				SELECT COUNT(*) FROM compute_assignments WHERE endpoint_id = ?
				"""
			bind: ["endpointID"]
		}
	},
	{
		name: "CountAuditLogs"
		kind: "one"
		paramMode: "struct"
		params: [
			{name: "Column1", type: "interface{}"},
			{name: "PrincipalName", type: "string"},
			{name: "Column3", type: "interface{}"},
			{name: "Action", type: "string"},
			{name: "Column5", type: "interface{}"},
			{name: "Status", type: "string"},
		]
		result: {
			scalar: "int64"
		}
		raw: {
			sql: """
				-- name: CountAuditLogs :one
				SELECT COUNT(*) as cnt FROM audit_log
				WHERE (? IS NULL OR principal_name = ?)
				  AND (? IS NULL OR action = ?)
				  AND (? IS NULL OR status = ?)
				"""
			bind: ["Column1", "PrincipalName", "Column3", "Action", "Column5", "Status"]
		}
	},
	{
		name: "CountCatalogs"
		kind: "one"
		result: {
			scalar: "int64"
		}
		raw: {
			sql: """
				-- name: CountCatalogs :one
				SELECT COUNT(*) FROM catalogs
				"""
		}
	},
	{
		name: "CountColumnMasksForTable"
		kind: "one"
		paramMode: "single"
		params: [
			{name: "tableID", type: "string"},
		]
		result: {
			scalar: "int64"
		}
		raw: {
			sql: """
				-- name: CountColumnMasksForTable :one
				SELECT COUNT(*) as cnt FROM column_masks WHERE table_id = ?
				"""
			bind: ["tableID"]
		}
	},
	{
		name: "CountComputeEndpoints"
		kind: "one"
		result: {
			scalar: "int64"
		}
		raw: {
			sql: """
				-- name: CountComputeEndpoints :one
				SELECT COUNT(*) FROM compute_endpoints
				"""
		}
	},
	{
		name: "CountDownstreamLineage"
		kind: "one"
		paramMode: "single"
		params: [
			{name: "sourceTable", type: "string"},
		]
		result: {
			scalar: "int64"
		}
		raw: {
			sql: """
				-- name: CountDownstreamLineage :one
				SELECT COUNT(DISTINCT source_table || '->' || COALESCE(target_table, '')) as cnt FROM lineage_edges WHERE source_table = ?
				"""
			bind: ["sourceTable"]
		}
	},
	{
		name: "CountExternalLocations"
		kind: "one"
		result: {
			scalar: "int64"
		}
		raw: {
			sql: """
				-- name: CountExternalLocations :one
				SELECT COUNT(*) FROM external_locations
				"""
		}
	},
	{
		name: "CountExternalTables"
		kind: "one"
		paramMode: "single"
		params: [
			{name: "schemaName", type: "string"},
		]
		result: {
			scalar: "int64"
		}
		raw: {
			sql: """
				-- name: CountExternalTables :one
				SELECT COUNT(*) FROM external_tables
				WHERE schema_name = ? AND deleted_at IS NULL
				"""
			bind: ["schemaName"]
		}
	},
	{
		name: "CountGitRepos"
		kind: "one"
		result: {
			scalar: "int64"
		}
		raw: {
			sql: """
				-- name: CountGitRepos :one
				SELECT COUNT(*) FROM git_repos
				"""
		}
	},
	{
		name: "CountGrantsForPrincipal"
		kind: "one"
		paramMode: "struct"
		params: [
			{name: "PrincipalID", type: "string"},
			{name: "PrincipalType", type: "string"},
		]
		result: {
			scalar: "int64"
		}
		raw: {
			sql: """
				-- name: CountGrantsForPrincipal :one
				SELECT COUNT(*) as cnt FROM privilege_grants
				WHERE principal_id = ? AND principal_type = ?
				"""
			bind: ["PrincipalID", "PrincipalType"]
		}
	},
	{
		name: "CountGrantsForSecurable"
		kind: "one"
		paramMode: "struct"
		params: [
			{name: "SecurableType", type: "string"},
			{name: "SecurableID", type: "string"},
		]
		result: {
			scalar: "int64"
		}
		raw: {
			sql: """
				-- name: CountGrantsForSecurable :one
				SELECT COUNT(*) as cnt FROM privilege_grants
				WHERE securable_type = ? AND securable_id = ?
				"""
			bind: ["SecurableType", "SecurableID"]
		}
	},
	{
		name: "CountGroupMembers"
		kind: "one"
		paramMode: "single"
		params: [
			{name: "groupID", type: "string"},
		]
		result: {
			scalar: "int64"
		}
		raw: {
			sql: """
				-- name: CountGroupMembers :one
				SELECT COUNT(*) as cnt FROM group_members WHERE group_id = ?
				"""
			bind: ["groupID"]
		}
	},
	{
		name: "CountGroups"
		kind: "one"
		result: {
			scalar: "int64"
		}
		raw: {
			sql: """
				-- name: CountGroups :one
				SELECT COUNT(*) as cnt FROM groups
				"""
		}
	},
	{
		name: "CountMacros"
		kind: "one"
		result: {
			scalar: "int64"
		}
		raw: {
			sql: """
				-- name: CountMacros :one
				SELECT COUNT(*) FROM macros
				"""
		}
	},
	{
		name: "CountNotebookJobs"
		kind: "one"
		paramMode: "single"
		params: [
			{name: "notebookID", type: "string"},
		]
		result: {
			scalar: "int64"
		}
		raw: {
			sql: """
				-- name: CountNotebookJobs :one
				SELECT COUNT(*) FROM notebook_jobs WHERE notebook_id = ?
				"""
			bind: ["notebookID"]
		}
	},
	{
		name: "CountPipelines"
		kind: "one"
		result: {
			scalar: "int64"
		}
		raw: {
			sql: """
				-- name: CountPipelines :one
				SELECT COUNT(*) FROM pipelines
				"""
		}
	},
	{
		name: "CountPrincipals"
		kind: "one"
		result: {
			scalar: "int64"
		}
		raw: {
			sql: """
				-- name: CountPrincipals :one
				SELECT COUNT(*) as cnt FROM principals
				"""
		}
	},
	{
		name: "CountQueryHistory"
		kind: "one"
		paramMode: "struct"
		params: [
			{name: "Column1", type: "interface{}"},
			{name: "PrincipalName", type: "string"},
			{name: "Column3", type: "interface{}"},
			{name: "Status", type: "string"},
			{name: "Column5", type: "interface{}"},
			{name: "CreatedAt", type: "string"},
			{name: "Column7", type: "interface{}"},
			{name: "CreatedAt_2", type: "string"},
		]
		result: {
			scalar: "int64"
		}
		raw: {
			sql: """
				-- name: CountQueryHistory :one
				SELECT COUNT(*) as cnt FROM audit_log
				WHERE action = 'QUERY'
				  AND (? IS NULL OR principal_name = ?)
				  AND (? IS NULL OR status = ?)
				  AND (? IS NULL OR created_at >= ?)
				  AND (? IS NULL OR created_at <= ?)
				"""
			bind: ["Column1", "PrincipalName", "Column3", "Status", "Column5", "CreatedAt", "Column7", "CreatedAt_2"]
		}
	},
	{
		name: "CountRecentFailedAuthLoginAttemptsByIP"
		kind: "one"
		paramMode: "struct"
		params: [
			{name: "IpAddress", type: "sql.NullString"},
			{name: "CreatedAt", type: "time.Time"},
		]
		result: {
			scalar: "int64"
		}
		raw: {
			sql: """
				-- name: CountRecentFailedAuthLoginAttemptsByIP :one
				SELECT COUNT(*)
				FROM auth_login_attempts
				WHERE ip_address = ?
				  AND success = 0
				  AND created_at >= ?
				"""
			bind: ["IpAddress", "CreatedAt"]
		}
	},
	{
		name: "CountRecentFailedAuthLoginAttemptsByUsername"
		kind: "one"
		paramMode: "struct"
		params: [
			{name: "Username", type: "sql.NullString"},
			{name: "CreatedAt", type: "time.Time"},
		]
		result: {
			scalar: "int64"
		}
		raw: {
			sql: """
				-- name: CountRecentFailedAuthLoginAttemptsByUsername :one
				SELECT COUNT(*)
				FROM auth_login_attempts
				WHERE username = ?
				  AND success = 0
				  AND created_at >= ?
				"""
			bind: ["Username", "CreatedAt"]
		}
	},
	{
		name: "CountRowFiltersForTable"
		kind: "one"
		paramMode: "single"
		params: [
			{name: "tableID", type: "string"},
		]
		result: {
			scalar: "int64"
		}
		raw: {
			sql: """
				-- name: CountRowFiltersForTable :one
				SELECT COUNT(*) as cnt FROM row_filters WHERE table_id = ?
				"""
			bind: ["tableID"]
		}
	},
	{
		name: "CountSemanticModels"
		kind: "one"
		result: {
			scalar: "int64"
		}
		raw: {
			sql: """
				-- name: CountSemanticModels :one
				SELECT COUNT(*) FROM semantic_models
				"""
		}
	},
	{
		name: "CountSemanticRelationships"
		kind: "one"
		result: {
			scalar: "int64"
		}
		raw: {
			sql: """
				-- name: CountSemanticRelationships :one
				SELECT COUNT(*) FROM semantic_relationships
				"""
		}
	},
	{
		name: "CountStorageCredentials"
		kind: "one"
		result: {
			scalar: "int64"
		}
		raw: {
			sql: """
				-- name: CountStorageCredentials :one
				SELECT COUNT(*) FROM storage_credentials
				"""
		}
	},
	{
		name: "CountTagAssignments"
		kind: "one"
		result: {
			scalar: "int64"
		}
		raw: {
			sql: """
				-- name: CountTagAssignments :one
				SELECT COUNT(*) as cnt FROM tag_assignments
				"""
		}
	},
	{
		name: "CountTags"
		kind: "one"
		result: {
			scalar: "int64"
		}
		raw: {
			sql: """
				-- name: CountTags :one
				SELECT COUNT(*) as cnt FROM tags
				"""
		}
	},
	{
		name: "CountUpstreamLineage"
		kind: "one"
		paramMode: "single"
		params: [
			{name: "targetTable", type: "sql.NullString"},
		]
		result: {
			scalar: "int64"
		}
		raw: {
			sql: """
				-- name: CountUpstreamLineage :one
				SELECT COUNT(DISTINCT source_table) as cnt FROM lineage_edges WHERE target_table = ?
				"""
			bind: ["targetTable"]
		}
	},
	{
		name: "CountViews"
		kind: "one"
		paramMode: "single"
		params: [
			{name: "schemaID", type: "string"},
		]
		result: {
			scalar: "int64"
		}
		raw: {
			sql: """
				-- name: CountViews :one
				SELECT COUNT(*) as cnt FROM views WHERE schema_id = ? AND deleted_at IS NULL
				"""
			bind: ["schemaID"]
		}
	},
	{
		name: "CountVolumes"
		kind: "one"
		paramMode: "single"
		params: [
			{name: "schemaName", type: "string"},
		]
		result: {
			scalar: "int64"
		}
		raw: {
			sql: """
				-- name: CountVolumes :one
				SELECT COUNT(*) FROM volumes WHERE schema_name = ?
				"""
			bind: ["schemaName"]
		}
	},
	{
		name: "CreateAPIKey"
		kind: "one"
		paramMode: "struct"
		params: [
			{name: "ID", type: "string"},
			{name: "KeyHash", type: "string"},
			{name: "KeyPrefix", type: "sql.NullString"},
			{name: "PrincipalID", type: "string"},
			{name: "Name", type: "string"},
			{name: "ExpiresAt", type: "sql.NullString"},
		]
		result: {
			row: "ApiKey"
			fields: [
				{name: "ID", type: "string"},
				{name: "KeyHash", type: "string"},
				{name: "PrincipalID", type: "string"},
				{name: "Name", type: "string"},
				{name: "ExpiresAt", type: "sql.NullString"},
				{name: "CreatedAt", type: "string"},
				{name: "KeyPrefix", type: "sql.NullString"},
			]
		}
		raw: {
			sql: """
				-- name: CreateAPIKey :one
				INSERT INTO api_keys (id, key_hash, key_prefix, principal_id, name, expires_at)
				VALUES (?, ?, ?, ?, ?, ?)
				RETURNING id, key_hash, principal_id, name, expires_at, created_at, key_prefix
				"""
			bind: ["ID", "KeyHash", "KeyPrefix", "PrincipalID", "Name", "ExpiresAt"]
		}
	},
	{
		name: "CreateAuthIdentity"
		kind: "one"
		paramMode: "struct"
		params: [
			{name: "ID", type: "string"},
			{name: "PrincipalID", type: "string"},
			{name: "Provider", type: "string"},
			{name: "Issuer", type: "sql.NullString"},
			{name: "Subject", type: "string"},
			{name: "Email", type: "sql.NullString"},
			{name: "EmailVerified", type: "int64"},
		]
		result: {
			row: "AuthIdentity"
			fields: [
				{name: "ID", type: "string"},
				{name: "PrincipalID", type: "string"},
				{name: "Provider", type: "string"},
				{name: "Issuer", type: "sql.NullString"},
				{name: "Subject", type: "string"},
				{name: "Email", type: "sql.NullString"},
				{name: "EmailVerified", type: "int64"},
				{name: "CreatedAt", type: "time.Time"},
				{name: "UpdatedAt", type: "time.Time"},
			]
		}
		raw: {
			sql: """
				-- name: CreateAuthIdentity :one
				INSERT INTO auth_identities (
				  id, principal_id, provider, issuer, subject, email, email_verified
				)
				VALUES (?, ?, ?, ?, ?, ?, ?)
				RETURNING id, principal_id, provider, issuer, subject, email, email_verified, created_at, updated_at
				"""
			bind: ["ID", "PrincipalID", "Provider", "Issuer", "Subject", "Email", "EmailVerified"]
		}
	},
	{
		name: "CreateAuthRecoveryCode"
		kind: "one"
		paramMode: "struct"
		params: [
			{name: "ID", type: "string"},
			{name: "PrincipalID", type: "string"},
			{name: "CodeHash", type: "string"},
			{name: "ExpiresAt", type: "time.Time"},
		]
		result: {
			row: "AuthRecoveryCode"
			fields: [
				{name: "ID", type: "string"},
				{name: "PrincipalID", type: "string"},
				{name: "CodeHash", type: "string"},
				{name: "UsedAt", type: "sql.NullTime"},
				{name: "ExpiresAt", type: "time.Time"},
				{name: "CreatedAt", type: "time.Time"},
			]
		}
		raw: {
			sql: """
				-- name: CreateAuthRecoveryCode :one
				INSERT INTO auth_recovery_codes (
				  id, principal_id, code_hash, expires_at
				)
				VALUES (?, ?, ?, ?)
				RETURNING id, principal_id, code_hash, used_at, expires_at, created_at
				"""
			bind: ["ID", "PrincipalID", "CodeHash", "ExpiresAt"]
		}
	},
	{
		name: "CreateCatalog"
		kind: "one"
		paramMode: "struct"
		params: [
			{name: "ID", type: "string"},
			{name: "Name", type: "string"},
			{name: "MetastoreType", type: "string"},
			{name: "Dsn", type: "string"},
			{name: "DataPath", type: "string"},
			{name: "Status", type: "string"},
			{name: "StatusMessage", type: "sql.NullString"},
			{name: "IsDefault", type: "int64"},
			{name: "Comment", type: "sql.NullString"},
		]
		result: {
			row: "Catalog"
			fields: [
				{name: "ID", type: "string"},
				{name: "Name", type: "string"},
				{name: "MetastoreType", type: "string"},
				{name: "Dsn", type: "string"},
				{name: "DataPath", type: "string"},
				{name: "Status", type: "string"},
				{name: "StatusMessage", type: "sql.NullString"},
				{name: "IsDefault", type: "int64"},
				{name: "Comment", type: "sql.NullString"},
				{name: "CreatedAt", type: "string"},
				{name: "UpdatedAt", type: "string"},
			]
		}
		raw: {
			sql: """
				-- name: CreateCatalog :one
				INSERT INTO catalogs (id, name, metastore_type, dsn, data_path, status, status_message, is_default, comment)
				VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
				RETURNING id, name, metastore_type, dsn, data_path, status, status_message, is_default, comment, created_at, updated_at
				"""
			bind: ["ID", "Name", "MetastoreType", "Dsn", "DataPath", "Status", "StatusMessage", "IsDefault", "Comment"]
		}
	},
	{
		name: "CreateCell"
		kind: "one"
		paramMode: "struct"
		params: [
			{name: "ID", type: "string"},
			{name: "NotebookID", type: "string"},
			{name: "CellType", type: "string"},
			{name: "Name", type: "sql.NullString"},
			{name: "Role", type: "string"},
			{name: "Disabled", type: "int64"},
			{name: "TestConfig", type: "string"},
			{name: "VisualSpec", type: "string"},
			{name: "Content", type: "string"},
			{name: "Position", type: "int64"},
		]
		result: {
			row: "Cell"
			fields: [
				{name: "ID", type: "string"},
				{name: "NotebookID", type: "string"},
				{name: "CellType", type: "string"},
				{name: "Content", type: "string"},
				{name: "Position", type: "int64"},
				{name: "LastResult", type: "sql.NullString"},
				{name: "CreatedAt", type: "string"},
				{name: "UpdatedAt", type: "string"},
				{name: "Name", type: "sql.NullString"},
				{name: "Role", type: "string"},
				{name: "Disabled", type: "int64"},
				{name: "TestConfig", type: "string"},
				{name: "VisualSpec", type: "string"},
			]
		}
		raw: {
			sql: """
				-- name: CreateCell :one
				INSERT INTO cells (id, notebook_id, cell_type, name, role, disabled, test_config, visual_spec, content, position)
				VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
				RETURNING id, notebook_id, cell_type, content, position, last_result, created_at, updated_at, name, role, disabled, test_config, visual_spec
				"""
			bind: ["ID", "NotebookID", "CellType", "Name", "Role", "Disabled", "TestConfig", "VisualSpec", "Content", "Position"]
		}
	},
	{
		name: "CreateColumnMask"
		kind: "one"
		paramMode: "struct"
		params: [
			{name: "ID", type: "string"},
			{name: "TableID", type: "string"},
			{name: "Name", type: "sql.NullString"},
			{name: "ColumnName", type: "string"},
			{name: "MaskExpression", type: "string"},
			{name: "Description", type: "sql.NullString"},
		]
		result: {
			row: "ColumnMask"
			fields: [
				{name: "ID", type: "string"},
				{name: "TableID", type: "string"},
				{name: "ColumnName", type: "string"},
				{name: "MaskExpression", type: "string"},
				{name: "Description", type: "sql.NullString"},
				{name: "CreatedAt", type: "string"},
				{name: "Name", type: "sql.NullString"},
			]
		}
		raw: {
			sql: """
				-- name: CreateColumnMask :one
				INSERT INTO column_masks (id, table_id, name, column_name, mask_expression, description)
				VALUES (?, ?, ?, ?, ?, ?)
				RETURNING id, table_id, column_name, mask_expression, description, created_at, name
				"""
			bind: ["ID", "TableID", "Name", "ColumnName", "MaskExpression", "Description"]
		}
	},
	{
		name: "CreateComputeAssignment"
		kind: "one"
		paramMode: "struct"
		params: [
			{name: "ID", type: "string"},
			{name: "PrincipalID", type: "string"},
			{name: "PrincipalType", type: "string"},
			{name: "EndpointID", type: "string"},
			{name: "IsDefault", type: "int64"},
			{name: "FallbackLocal", type: "int64"},
		]
		result: {
			row: "ComputeAssignment"
			fields: [
				{name: "ID", type: "string"},
				{name: "PrincipalID", type: "string"},
				{name: "PrincipalType", type: "string"},
				{name: "EndpointID", type: "string"},
				{name: "IsDefault", type: "int64"},
				{name: "FallbackLocal", type: "int64"},
				{name: "CreatedAt", type: "time.Time"},
			]
		}
		raw: {
			sql: """
				-- name: CreateComputeAssignment :one
				INSERT INTO compute_assignments (
				    id, principal_id, principal_type, endpoint_id, is_default, fallback_local
				) VALUES (?, ?, ?, ?, ?, ?)
				RETURNING id, principal_id, principal_type, endpoint_id, is_default, fallback_local, created_at
				"""
			bind: ["ID", "PrincipalID", "PrincipalType", "EndpointID", "IsDefault", "FallbackLocal"]
		}
	},
	{
		name: "CreateComputeEndpoint"
		kind: "one"
		paramMode: "struct"
		params: [
			{name: "ID", type: "string"},
			{name: "ExternalID", type: "string"},
			{name: "Name", type: "string"},
			{name: "Url", type: "string"},
			{name: "Type", type: "string"},
			{name: "SelectionPolicy", type: "string"},
			{name: "WorkloadClass", type: "string"},
			{name: "ReadinessStatus", type: "string"},
			{name: "Size", type: "string"},
			{name: "MaxMemoryGb", type: "sql.NullInt64"},
			{name: "MaxConcurrency", type: "sql.NullInt64"},
			{name: "MaxResultSizeMb", type: "sql.NullInt64"},
			{name: "RecommendedForLargeQueries", type: "int64"},
			{name: "IsDraining", type: "int64"},
			{name: "AuthToken", type: "string"},
			{name: "Owner", type: "string"},
		]
		result: {
			row: "ComputeEndpoint"
			fields: [
				{name: "ID", type: "string"},
				{name: "ExternalID", type: "string"},
				{name: "Name", type: "string"},
				{name: "Url", type: "string"},
				{name: "Type", type: "string"},
				{name: "Status", type: "string"},
				{name: "Size", type: "string"},
				{name: "MaxMemoryGb", type: "sql.NullInt64"},
				{name: "AuthToken", type: "string"},
				{name: "Owner", type: "string"},
				{name: "CreatedAt", type: "time.Time"},
				{name: "UpdatedAt", type: "time.Time"},
				{name: "SelectionPolicy", type: "string"},
				{name: "WorkloadClass", type: "string"},
				{name: "ReadinessStatus", type: "string"},
				{name: "MaxConcurrency", type: "sql.NullInt64"},
				{name: "MaxResultSizeMb", type: "sql.NullInt64"},
				{name: "RecommendedForLargeQueries", type: "int64"},
				{name: "IsDraining", type: "int64"},
				{name: "LastHealthStatus", type: "sql.NullString"},
				{name: "LastHealthCheckedAt", type: "sql.NullTime"},
				{name: "ActiveQueries", type: "sql.NullInt64"},
				{name: "QueuedJobs", type: "sql.NullInt64"},
				{name: "RunningJobs", type: "sql.NullInt64"},
				{name: "CompletedJobs", type: "sql.NullInt64"},
				{name: "StoredJobs", type: "sql.NullInt64"},
				{name: "CleanedJobs", type: "sql.NullInt64"},
				{name: "QueryResultTtlSeconds", type: "sql.NullInt64"},
			]
		}
		raw: {
			sql: """
				-- name: CreateComputeEndpoint :one
				INSERT INTO compute_endpoints (
				    id, external_id, name, url, type, status, selection_policy, workload_class, readiness_status,
				    size, max_memory_gb, max_concurrency, max_result_size_mb, recommended_for_large_queries,
				    is_draining, auth_token, owner
				) VALUES (?, ?, ?, ?, ?, 'INACTIVE', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
				RETURNING id, external_id, name, url, type, status, size, max_memory_gb, auth_token, owner, created_at, updated_at, selection_policy, workload_class, readiness_status, max_concurrency, max_result_size_mb, recommended_for_large_queries, is_draining, last_health_status, last_health_checked_at, active_queries, queued_jobs, running_jobs, completed_jobs, stored_jobs, cleaned_jobs, query_result_ttl_seconds
				"""
			bind: ["ID", "ExternalID", "Name", "Url", "Type", "SelectionPolicy", "WorkloadClass", "ReadinessStatus", "Size", "MaxMemoryGb", "MaxConcurrency", "MaxResultSizeMb", "RecommendedForLargeQueries", "IsDraining", "AuthToken", "Owner"]
		}
	},
	{
		name: "CreateDashboard"
		kind: "one"
		paramMode: "struct"
		params: [
			{name: "ID", type: "string"},
			{name: "Name", type: "string"},
			{name: "Description", type: "string"},
			{name: "Owner", type: "string"},
			{name: "FolderID", type: "sql.NullString"},
			{name: "SemanticProjectName", type: "string"},
			{name: "SemanticModelName", type: "string"},
			{name: "ComputeMode", type: "string"},
			{name: "ComputeEndpointName", type: "string"},
			{name: "ComputeFallbackLocal", type: "int64"},
		]
		result: {
			row: "Dashboard"
			fields: [
				{name: "ID", type: "string"},
				{name: "Name", type: "string"},
				{name: "Description", type: "string"},
				{name: "Owner", type: "string"},
				{name: "CreatedAt", type: "string"},
				{name: "UpdatedAt", type: "string"},
				{name: "FolderID", type: "sql.NullString"},
				{name: "SemanticProjectName", type: "string"},
				{name: "SemanticModelName", type: "string"},
				{name: "ComputeMode", type: "string"},
				{name: "ComputeEndpointName", type: "string"},
				{name: "ComputeFallbackLocal", type: "int64"},
			]
		}
		raw: {
			sql: """
				-- name: CreateDashboard :one
				INSERT INTO dashboards (id, name, description, owner, folder_id, semantic_project_name, semantic_model_name, compute_mode, compute_endpoint_name, compute_fallback_local)
				VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
				RETURNING id, name, description, owner, created_at, updated_at, folder_id, semantic_project_name, semantic_model_name, compute_mode, compute_endpoint_name, compute_fallback_local
				"""
			bind: ["ID", "Name", "Description", "Owner", "FolderID", "SemanticProjectName", "SemanticModelName", "ComputeMode", "ComputeEndpointName", "ComputeFallbackLocal"]
		}
	},
	{
		name: "CreateDashboardWidget"
		kind: "one"
		paramMode: "struct"
		params: [
			{name: "ID", type: "string"},
			{name: "DashboardID", type: "string"},
			{name: "FilterOriginKey", type: "string"},
			{name: "PageName", type: "string"},
			{name: "Name", type: "string"},
			{name: "Description", type: "string"},
			{name: "SourceJson", type: "string"},
			{name: "VisualSpec", type: "string"},
			{name: "LayoutX", type: "int64"},
			{name: "LayoutY", type: "int64"},
			{name: "LayoutW", type: "int64"},
			{name: "LayoutH", type: "int64"},
		]
		result: {
			row: "DashboardWidget"
			fields: [
				{name: "ID", type: "string"},
				{name: "DashboardID", type: "string"},
				{name: "Name", type: "string"},
				{name: "Description", type: "string"},
				{name: "SourceJson", type: "string"},
				{name: "VisualSpec", type: "string"},
				{name: "LayoutX", type: "int64"},
				{name: "LayoutY", type: "int64"},
				{name: "LayoutW", type: "int64"},
				{name: "LayoutH", type: "int64"},
				{name: "CreatedAt", type: "string"},
				{name: "UpdatedAt", type: "string"},
				{name: "FilterOriginKey", type: "string"},
				{name: "PageName", type: "string"},
			]
		}
		raw: {
			sql: """
				-- name: CreateDashboardWidget :one
				INSERT INTO dashboard_widgets (
				    id, dashboard_id, filter_origin_key, page_name, name, description, source_json, visual_spec,
				    layout_x, layout_y, layout_w, layout_h
				)
				VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
				RETURNING id, dashboard_id, name, description, source_json, visual_spec, layout_x, layout_y, layout_w, layout_h, created_at, updated_at, filter_origin_key, page_name
				"""
			bind: ["ID", "DashboardID", "FilterOriginKey", "PageName", "Name", "Description", "SourceJson", "VisualSpec", "LayoutX", "LayoutY", "LayoutW", "LayoutH"]
		}
	},
	{
		name: "CreateExternalLocation"
		kind: "one"
		paramMode: "struct"
		params: [
			{name: "ID", type: "string"},
			{name: "Name", type: "string"},
			{name: "Url", type: "string"},
			{name: "CredentialName", type: "string"},
			{name: "StorageType", type: "string"},
			{name: "Comment", type: "string"},
			{name: "Owner", type: "string"},
			{name: "ReadOnly", type: "int64"},
		]
		result: {
			row: "ExternalLocation"
			fields: [
				{name: "ID", type: "string"},
				{name: "Name", type: "string"},
				{name: "Url", type: "string"},
				{name: "CredentialName", type: "string"},
				{name: "StorageType", type: "string"},
				{name: "Comment", type: "string"},
				{name: "Owner", type: "string"},
				{name: "ReadOnly", type: "int64"},
				{name: "CreatedAt", type: "string"},
				{name: "UpdatedAt", type: "string"},
			]
		}
		raw: {
			sql: """
				-- name: CreateExternalLocation :one
				INSERT INTO external_locations (id, name, url, credential_name, storage_type, comment, owner, read_only)
				VALUES (?, ?, ?, ?, ?, ?, ?, ?)
				RETURNING id, name, url, credential_name, storage_type, comment, owner, read_only, created_at, updated_at
				"""
			bind: ["ID", "Name", "Url", "CredentialName", "StorageType", "Comment", "Owner", "ReadOnly"]
		}
	},
	{
		name: "CreateExternalTable"
		kind: "one"
		paramMode: "struct"
		params: [
			{name: "ID", type: "string"},
			{name: "SchemaName", type: "string"},
			{name: "TableName", type: "string"},
			{name: "FileFormat", type: "string"},
			{name: "SourcePath", type: "string"},
			{name: "LocationName", type: "string"},
			{name: "Comment", type: "string"},
			{name: "Owner", type: "string"},
			{name: "CatalogName", type: "string"},
		]
		result: {
			row: "ExternalTable"
			fields: [
				{name: "ID", type: "string"},
				{name: "SchemaName", type: "string"},
				{name: "TableName", type: "string"},
				{name: "FileFormat", type: "string"},
				{name: "SourcePath", type: "string"},
				{name: "LocationName", type: "string"},
				{name: "Comment", type: "string"},
				{name: "Owner", type: "string"},
				{name: "CreatedAt", type: "string"},
				{name: "UpdatedAt", type: "string"},
				{name: "DeletedAt", type: "sql.NullString"},
				{name: "CatalogName", type: "string"},
			]
		}
		raw: {
			sql: """
				-- name: CreateExternalTable :one
				INSERT INTO external_tables (id, schema_name, table_name, file_format, source_path, location_name, comment, owner, catalog_name)
				VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
				RETURNING id, schema_name, table_name, file_format, source_path, location_name, comment, owner, created_at, updated_at, deleted_at, catalog_name
				"""
			bind: ["ID", "SchemaName", "TableName", "FileFormat", "SourcePath", "LocationName", "Comment", "Owner", "CatalogName"]
		}
	},
	{
		name: "CreateGitRepo"
		kind: "one"
		paramMode: "struct"
		params: [
			{name: "ID", type: "string"},
			{name: "Url", type: "string"},
			{name: "Branch", type: "string"},
			{name: "Path", type: "string"},
			{name: "AuthToken", type: "string"},
			{name: "WebhookSecret", type: "sql.NullString"},
			{name: "Owner", type: "string"},
		]
		result: {
			row: "GitRepo"
			fields: [
				{name: "ID", type: "string"},
				{name: "Url", type: "string"},
				{name: "Branch", type: "string"},
				{name: "Path", type: "string"},
				{name: "AuthToken", type: "string"},
				{name: "WebhookSecret", type: "sql.NullString"},
				{name: "Owner", type: "string"},
				{name: "LastSyncAt", type: "sql.NullString"},
				{name: "LastCommit", type: "sql.NullString"},
				{name: "CreatedAt", type: "string"},
				{name: "UpdatedAt", type: "string"},
			]
		}
		raw: {
			sql: """
				-- name: CreateGitRepo :one
				INSERT INTO git_repos (id, url, branch, path, auth_token, webhook_secret, owner)
				VALUES (?, ?, ?, ?, ?, ?, ?)
				RETURNING id, url, branch, path, auth_token, webhook_secret, owner, last_sync_at, last_commit, created_at, updated_at
				"""
			bind: ["ID", "Url", "Branch", "Path", "AuthToken", "WebhookSecret", "Owner"]
		}
	},
	{
		name: "CreateGroup"
		kind: "one"
		paramMode: "struct"
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
		raw: {
			sql: """
				-- name: CreateGroup :one
				INSERT INTO groups (id, name, description)
				VALUES (?, ?, ?)
				RETURNING id, name, description, created_at
				"""
			bind: ["ID", "Name", "Description"]
		}
	},
	{
		name: "CreateMacro"
		kind: "one"
		paramMode: "struct"
		params: [
			{name: "ID", type: "string"},
			{name: "Name", type: "string"},
			{name: "MacroType", type: "string"},
			{name: "Parameters", type: "string"},
			{name: "Body", type: "string"},
			{name: "Description", type: "string"},
			{name: "CatalogName", type: "string"},
			{name: "ProjectName", type: "string"},
			{name: "Visibility", type: "string"},
			{name: "Owner", type: "string"},
			{name: "Properties", type: "string"},
			{name: "Tags", type: "string"},
			{name: "Status", type: "string"},
			{name: "CreatedBy", type: "string"},
		]
		result: {
			row: "Macro"
			fields: [
				{name: "ID", type: "string"},
				{name: "Name", type: "string"},
				{name: "MacroType", type: "string"},
				{name: "Parameters", type: "string"},
				{name: "Body", type: "string"},
				{name: "Description", type: "string"},
				{name: "CreatedBy", type: "string"},
				{name: "CreatedAt", type: "string"},
				{name: "UpdatedAt", type: "string"},
				{name: "CatalogName", type: "string"},
				{name: "ProjectName", type: "string"},
				{name: "Visibility", type: "string"},
				{name: "Owner", type: "string"},
				{name: "Properties", type: "string"},
				{name: "Tags", type: "string"},
				{name: "Status", type: "string"},
			]
		}
		raw: {
			sql: """
				-- name: CreateMacro :one
				INSERT INTO macros (id, name, macro_type, parameters, body, description, catalog_name, project_name, visibility, owner, properties, tags, status, created_by)
				VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
				RETURNING id, name, macro_type, parameters, body, description, created_by, created_at, updated_at, catalog_name, project_name, visibility, owner, properties, tags, status
				"""
			bind: ["ID", "Name", "MacroType", "Parameters", "Body", "Description", "CatalogName", "ProjectName", "Visibility", "Owner", "Properties", "Tags", "Status", "CreatedBy"]
		}
	},
	{
		name: "CreateMacroRevision"
		kind: "one"
		paramMode: "struct"
		params: [
			{name: "ID", type: "string"},
			{name: "MacroID", type: "string"},
			{name: "MacroName", type: "string"},
			{name: "Version", type: "int64"},
			{name: "ContentHash", type: "string"},
			{name: "Parameters", type: "string"},
			{name: "Body", type: "string"},
			{name: "Description", type: "string"},
			{name: "Status", type: "string"},
			{name: "CreatedBy", type: "string"},
		]
		result: {
			row: "MacroRevision"
			fields: [
				{name: "ID", type: "string"},
				{name: "MacroID", type: "string"},
				{name: "MacroName", type: "string"},
				{name: "Version", type: "int64"},
				{name: "ContentHash", type: "string"},
				{name: "Parameters", type: "string"},
				{name: "Body", type: "string"},
				{name: "Description", type: "string"},
				{name: "Status", type: "string"},
				{name: "CreatedBy", type: "string"},
				{name: "CreatedAt", type: "string"},
			]
		}
		raw: {
			sql: """
				-- name: CreateMacroRevision :one
				INSERT INTO macro_revisions (id, macro_id, macro_name, version, content_hash, parameters, body, description, status, created_by)
				VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
				RETURNING id, macro_id, macro_name, version, content_hash, parameters, body, description, status, created_by, created_at
				"""
			bind: ["ID", "MacroID", "MacroName", "Version", "ContentHash", "Parameters", "Body", "Description", "Status", "CreatedBy"]
		}
	},
	{
		name: "CreateModel"
		kind: "one"
		paramMode: "struct"
		params: [
			{name: "ID", type: "string"},
			{name: "ProjectName", type: "string"},
			{name: "Name", type: "string"},
			{name: "SqlBody", type: "string"},
			{name: "Materialization", type: "string"},
			{name: "Description", type: "string"},
			{name: "Owner", type: "string"},
			{name: "Tags", type: "string"},
			{name: "DependsOn", type: "string"},
			{name: "Config", type: "string"},
			{name: "CreatedBy", type: "string"},
			{name: "Contract", type: "string"},
			{name: "FreshnessMaxLag", type: "sql.NullInt64"},
			{name: "FreshnessCron", type: "sql.NullString"},
		]
		result: {
			row: "Model"
			fields: [
				{name: "ID", type: "string"},
				{name: "ProjectName", type: "string"},
				{name: "Name", type: "string"},
				{name: "SqlBody", type: "string"},
				{name: "Materialization", type: "string"},
				{name: "Description", type: "string"},
				{name: "Owner", type: "string"},
				{name: "Tags", type: "string"},
				{name: "DependsOn", type: "string"},
				{name: "Config", type: "string"},
				{name: "CreatedBy", type: "string"},
				{name: "CreatedAt", type: "string"},
				{name: "UpdatedAt", type: "string"},
				{name: "Contract", type: "string"},
				{name: "FreshnessMaxLag", type: "sql.NullInt64"},
				{name: "FreshnessCron", type: "sql.NullString"},
			]
		}
		raw: {
			sql: """
				-- name: CreateModel :one
				INSERT INTO models (id, project_name, name, sql_body, materialization, description, owner, tags, depends_on, config, created_by, contract, freshness_max_lag, freshness_cron)
				VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
				RETURNING id, project_name, name, sql_body, materialization, description, owner, tags, depends_on, config, created_by, created_at, updated_at, contract, freshness_max_lag, freshness_cron
				"""
			bind: ["ID", "ProjectName", "Name", "SqlBody", "Materialization", "Description", "Owner", "Tags", "DependsOn", "Config", "CreatedBy", "Contract", "FreshnessMaxLag", "FreshnessCron"]
		}
	},
	{
		name: "CreateModelRun"
		kind: "one"
		paramMode: "struct"
		params: [
			{name: "ID", type: "string"},
			{name: "Status", type: "string"},
			{name: "TriggerType", type: "string"},
			{name: "TriggeredBy", type: "string"},
			{name: "ProjectName", type: "string"},
			{name: "EnvironmentName", type: "string"},
			{name: "BuildID", type: "sql.NullString"},
			{name: "TargetCatalog", type: "string"},
			{name: "TargetSchema", type: "string"},
			{name: "ModelSelector", type: "string"},
			{name: "Variables", type: "string"},
			{name: "FullRefresh", type: "int64"},
			{name: "CompileManifest", type: "string"},
			{name: "CompileDiagnostics", type: "string"},
		]
		result: {
			row: "ModelRun"
			fields: [
				{name: "ID", type: "string"},
				{name: "Status", type: "string"},
				{name: "TriggerType", type: "string"},
				{name: "TriggeredBy", type: "string"},
				{name: "TargetCatalog", type: "string"},
				{name: "TargetSchema", type: "string"},
				{name: "ModelSelector", type: "string"},
				{name: "Variables", type: "string"},
				{name: "StartedAt", type: "sql.NullString"},
				{name: "FinishedAt", type: "sql.NullString"},
				{name: "ErrorMessage", type: "sql.NullString"},
				{name: "CreatedAt", type: "string"},
				{name: "FullRefresh", type: "int64"},
				{name: "CompileManifest", type: "string"},
				{name: "CompileDiagnostics", type: "string"},
				{name: "ProjectName", type: "string"},
				{name: "EnvironmentName", type: "string"},
				{name: "BuildID", type: "sql.NullString"},
			]
		}
		raw: {
			sql: """
				-- name: CreateModelRun :one
				INSERT INTO model_runs (
				    id, status, trigger_type, triggered_by, project_name, environment_name, build_id,
				    target_catalog, target_schema, model_selector, variables, full_refresh, compile_manifest, compile_diagnostics
				)
				VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
				RETURNING id, status, trigger_type, triggered_by, target_catalog, target_schema, model_selector, variables, started_at, finished_at, error_message, created_at, full_refresh, compile_manifest, compile_diagnostics, project_name, environment_name, build_id
				"""
			bind: ["ID", "Status", "TriggerType", "TriggeredBy", "ProjectName", "EnvironmentName", "BuildID", "TargetCatalog", "TargetSchema", "ModelSelector", "Variables", "FullRefresh", "CompileManifest", "CompileDiagnostics"]
		}
	},
	{
		name: "CreateModelRunStep"
		kind: "one"
		paramMode: "struct"
		params: [
			{name: "ID", type: "string"},
			{name: "RunID", type: "string"},
			{name: "ModelID", type: "string"},
			{name: "ModelName", type: "string"},
			{name: "CompiledSql", type: "sql.NullString"},
			{name: "CompiledHash", type: "sql.NullString"},
			{name: "DependsOn", type: "string"},
			{name: "VarsUsed", type: "string"},
			{name: "MacrosUsed", type: "string"},
			{name: "Status", type: "string"},
			{name: "Tier", type: "int64"},
		]
		result: {
			row: "ModelRunStep"
			fields: [
				{name: "ID", type: "string"},
				{name: "RunID", type: "string"},
				{name: "ModelID", type: "string"},
				{name: "ModelName", type: "string"},
				{name: "Status", type: "string"},
				{name: "Tier", type: "int64"},
				{name: "RowsAffected", type: "sql.NullInt64"},
				{name: "StartedAt", type: "sql.NullString"},
				{name: "FinishedAt", type: "sql.NullString"},
				{name: "ErrorMessage", type: "sql.NullString"},
				{name: "CreatedAt", type: "string"},
				{name: "CompiledSql", type: "sql.NullString"},
				{name: "CompiledHash", type: "sql.NullString"},
				{name: "DependsOn", type: "string"},
				{name: "VarsUsed", type: "string"},
				{name: "MacrosUsed", type: "string"},
			]
		}
		raw: {
			sql: """
				-- name: CreateModelRunStep :one
				INSERT INTO model_run_steps (id, run_id, model_id, model_name, compiled_sql, compiled_hash, depends_on, vars_used, macros_used, status, tier)
				VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
				RETURNING id, run_id, model_id, model_name, status, tier, rows_affected, started_at, finished_at, error_message, created_at, compiled_sql, compiled_hash, depends_on, vars_used, macros_used
				"""
			bind: ["ID", "RunID", "ModelID", "ModelName", "CompiledSql", "CompiledHash", "DependsOn", "VarsUsed", "MacrosUsed", "Status", "Tier"]
		}
	},
	{
		name: "CreateModelTest"
		kind: "one"
		paramMode: "struct"
		params: [
			{name: "ID", type: "string"},
			{name: "ModelID", type: "string"},
			{name: "Name", type: "string"},
			{name: "TestType", type: "string"},
			{name: "ColumnName", type: "string"},
			{name: "Config", type: "string"},
		]
		result: {
			row: "ModelTest"
			fields: [
				{name: "ID", type: "string"},
				{name: "ModelID", type: "string"},
				{name: "Name", type: "string"},
				{name: "TestType", type: "string"},
				{name: "ColumnName", type: "string"},
				{name: "Config", type: "string"},
				{name: "CreatedAt", type: "string"},
			]
		}
		raw: {
			sql: """
				-- name: CreateModelTest :one
				INSERT INTO model_tests (id, model_id, name, test_type, column_name, config)
				VALUES (?, ?, ?, ?, ?, ?)
				RETURNING id, model_id, name, test_type, column_name, config, created_at
				"""
			bind: ["ID", "ModelID", "Name", "TestType", "ColumnName", "Config"]
		}
	},
	{
		name: "CreateModelTestResult"
		kind: "one"
		paramMode: "struct"
		params: [
			{name: "ID", type: "string"},
			{name: "RunStepID", type: "string"},
			{name: "TestID", type: "string"},
			{name: "TestName", type: "string"},
			{name: "Status", type: "string"},
			{name: "RowsReturned", type: "sql.NullInt64"},
			{name: "ErrorMessage", type: "sql.NullString"},
		]
		result: {
			row: "ModelTestResult"
			fields: [
				{name: "ID", type: "string"},
				{name: "RunStepID", type: "string"},
				{name: "TestID", type: "string"},
				{name: "TestName", type: "string"},
				{name: "Status", type: "string"},
				{name: "RowsReturned", type: "sql.NullInt64"},
				{name: "ErrorMessage", type: "sql.NullString"},
				{name: "CreatedAt", type: "string"},
			]
		}
		raw: {
			sql: """
				-- name: CreateModelTestResult :one
				INSERT INTO model_test_results (id, run_step_id, test_id, test_name, status, rows_returned, error_message)
				VALUES (?, ?, ?, ?, ?, ?, ?)
				RETURNING id, run_step_id, test_id, test_name, status, rows_returned, error_message, created_at
				"""
			bind: ["ID", "RunStepID", "TestID", "TestName", "Status", "RowsReturned", "ErrorMessage"]
		}
	},
	{
		name: "CreateNotebook"
		kind: "one"
		paramMode: "struct"
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
		raw: {
			sql: """
				-- name: CreateNotebook :one
				INSERT INTO notebooks (id, name, description, owner)
				VALUES (?, ?, ?, ?)
				RETURNING id, name, description, owner, created_at, updated_at, git_repo_id, git_path, folder_id, project_override_id, environment_override_id
				"""
			bind: ["ID", "Name", "Description", "Owner"]
		}
	},
	{
		name: "CreateNotebookJob"
		kind: "one"
		paramMode: "struct"
		params: [
			{name: "ID", type: "string"},
			{name: "NotebookID", type: "string"},
			{name: "SessionID", type: "string"},
			{name: "State", type: "string"},
		]
		result: {
			row: "NotebookJob"
			fields: [
				{name: "ID", type: "string"},
				{name: "NotebookID", type: "string"},
				{name: "SessionID", type: "string"},
				{name: "State", type: "string"},
				{name: "Result", type: "sql.NullString"},
				{name: "Error", type: "sql.NullString"},
				{name: "CreatedAt", type: "string"},
				{name: "UpdatedAt", type: "string"},
			]
		}
		raw: {
			sql: """
				-- name: CreateNotebookJob :one
				INSERT INTO notebook_jobs (id, notebook_id, session_id, state)
				VALUES (?, ?, ?, ?)
				RETURNING id, notebook_id, session_id, state, result, error, created_at, updated_at
				"""
			bind: ["ID", "NotebookID", "SessionID", "State"]
		}
	},
	{
		name: "CreatePipeline"
		kind: "one"
		paramMode: "struct"
		params: [
			{name: "ID", type: "string"},
			{name: "Name", type: "string"},
			{name: "Description", type: "string"},
			{name: "ScheduleCron", type: "sql.NullString"},
			{name: "IsPaused", type: "int64"},
			{name: "ConcurrencyLimit", type: "int64"},
			{name: "CreatedBy", type: "string"},
			{name: "FolderID", type: "sql.NullString"},
		]
		result: {
			row: "Pipeline"
			fields: [
				{name: "ID", type: "string"},
				{name: "Name", type: "string"},
				{name: "Description", type: "string"},
				{name: "ScheduleCron", type: "sql.NullString"},
				{name: "IsPaused", type: "int64"},
				{name: "ConcurrencyLimit", type: "int64"},
				{name: "CreatedBy", type: "string"},
				{name: "CreatedAt", type: "string"},
				{name: "UpdatedAt", type: "string"},
				{name: "FolderID", type: "sql.NullString"},
			]
		}
		raw: {
			sql: """
				-- name: CreatePipeline :one
				INSERT INTO pipelines (id, name, description, schedule_cron, is_paused, concurrency_limit, created_by, folder_id)
				VALUES (?, ?, ?, ?, ?, ?, ?, ?)
				RETURNING id, name, description, schedule_cron, is_paused, concurrency_limit, created_by, created_at, updated_at, folder_id
				"""
			bind: ["ID", "Name", "Description", "ScheduleCron", "IsPaused", "ConcurrencyLimit", "CreatedBy", "FolderID"]
		}
	},
	{
		name: "CreatePipelineJob"
		kind: "one"
		paramMode: "struct"
		params: [
			{name: "ID", type: "string"},
			{name: "PipelineID", type: "string"},
			{name: "Name", type: "string"},
			{name: "ComputeEndpointID", type: "sql.NullString"},
			{name: "DependsOn", type: "string"},
			{name: "NotebookID", type: "string"},
			{name: "TimeoutSeconds", type: "sql.NullInt64"},
			{name: "RetryCount", type: "int64"},
			{name: "JobOrder", type: "int64"},
			{name: "JobType", type: "string"},
			{name: "ModelSelector", type: "string"},
		]
		result: {
			row: "PipelineJob"
			fields: [
				{name: "ID", type: "string"},
				{name: "PipelineID", type: "string"},
				{name: "Name", type: "string"},
				{name: "ComputeEndpointID", type: "sql.NullString"},
				{name: "DependsOn", type: "string"},
				{name: "NotebookID", type: "string"},
				{name: "TimeoutSeconds", type: "sql.NullInt64"},
				{name: "RetryCount", type: "int64"},
				{name: "JobOrder", type: "int64"},
				{name: "CreatedAt", type: "string"},
				{name: "JobType", type: "string"},
				{name: "ModelSelector", type: "string"},
			]
		}
		raw: {
			sql: """
				-- name: CreatePipelineJob :one
				INSERT INTO pipeline_jobs (id, pipeline_id, name, compute_endpoint_id, depends_on, notebook_id, timeout_seconds, retry_count, job_order, job_type, model_selector)
				VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
				RETURNING id, pipeline_id, name, compute_endpoint_id, depends_on, notebook_id, timeout_seconds, retry_count, job_order, created_at, job_type, model_selector
				"""
			bind: ["ID", "PipelineID", "Name", "ComputeEndpointID", "DependsOn", "NotebookID", "TimeoutSeconds", "RetryCount", "JobOrder", "JobType", "ModelSelector"]
		}
	},
	{
		name: "CreatePipelineJobRun"
		kind: "one"
		paramMode: "struct"
		params: [
			{name: "ID", type: "string"},
			{name: "RunID", type: "string"},
			{name: "JobID", type: "string"},
			{name: "JobName", type: "string"},
			{name: "Status", type: "string"},
			{name: "RetryAttempt", type: "int64"},
		]
		result: {
			row: "PipelineJobRun"
			fields: [
				{name: "ID", type: "string"},
				{name: "RunID", type: "string"},
				{name: "JobID", type: "string"},
				{name: "JobName", type: "string"},
				{name: "Status", type: "string"},
				{name: "StartedAt", type: "sql.NullString"},
				{name: "FinishedAt", type: "sql.NullString"},
				{name: "ErrorMessage", type: "sql.NullString"},
				{name: "RetryAttempt", type: "int64"},
				{name: "CreatedAt", type: "string"},
			]
		}
		raw: {
			sql: """
				-- name: CreatePipelineJobRun :one
				INSERT INTO pipeline_job_runs (id, run_id, job_id, job_name, status, retry_attempt)
				VALUES (?, ?, ?, ?, ?, ?)
				RETURNING id, run_id, job_id, job_name, status, started_at, finished_at, error_message, retry_attempt, created_at
				"""
			bind: ["ID", "RunID", "JobID", "JobName", "Status", "RetryAttempt"]
		}
	},
	{
		name: "CreatePipelineRun"
		kind: "one"
		paramMode: "struct"
		params: [
			{name: "ID", type: "string"},
			{name: "PipelineID", type: "string"},
			{name: "Status", type: "string"},
			{name: "TriggerType", type: "string"},
			{name: "TriggeredBy", type: "string"},
			{name: "Parameters", type: "string"},
			{name: "GitCommitHash", type: "sql.NullString"},
		]
		result: {
			row: "PipelineRun"
			fields: [
				{name: "ID", type: "string"},
				{name: "PipelineID", type: "string"},
				{name: "Status", type: "string"},
				{name: "TriggerType", type: "string"},
				{name: "TriggeredBy", type: "string"},
				{name: "Parameters", type: "string"},
				{name: "GitCommitHash", type: "sql.NullString"},
				{name: "StartedAt", type: "sql.NullString"},
				{name: "FinishedAt", type: "sql.NullString"},
				{name: "ErrorMessage", type: "sql.NullString"},
				{name: "CreatedAt", type: "string"},
			]
		}
		raw: {
			sql: """
				-- name: CreatePipelineRun :one
				INSERT INTO pipeline_runs (id, pipeline_id, status, trigger_type, triggered_by, parameters, git_commit_hash)
				VALUES (?, ?, ?, ?, ?, ?, ?)
				RETURNING id, pipeline_id, status, trigger_type, triggered_by, parameters, git_commit_hash, started_at, finished_at, error_message, created_at
				"""
			bind: ["ID", "PipelineID", "Status", "TriggerType", "TriggeredBy", "Parameters", "GitCommitHash"]
		}
	},
	{
		name: "CreatePrincipal"
		kind: "one"
		paramMode: "struct"
		params: [
			{name: "ID", type: "string"},
			{name: "Name", type: "string"},
			{name: "Type", type: "string"},
			{name: "IsAdmin", type: "int64"},
		]
		result: {
			row: "Principal"
			fields: [
				{name: "ID", type: "string"},
				{name: "Name", type: "string"},
				{name: "Type", type: "string"},
				{name: "IsAdmin", type: "int64"},
				{name: "CreatedAt", type: "string"},
				{name: "ExternalID", type: "sql.NullString"},
				{name: "ExternalIssuer", type: "sql.NullString"},
			]
		}
		raw: {
			sql: """
				-- name: CreatePrincipal :one
				INSERT INTO principals (id, name, type, is_admin)
				VALUES (?, ?, ?, ?)
				RETURNING id, name, type, is_admin, created_at, external_id, external_issuer
				"""
			bind: ["ID", "Name", "Type", "IsAdmin"]
		}
	},
	{
		name: "CreatePrincipalWithExternalID"
		kind: "one"
		paramMode: "struct"
		params: [
			{name: "ID", type: "string"},
			{name: "Name", type: "string"},
			{name: "Type", type: "string"},
			{name: "IsAdmin", type: "int64"},
			{name: "ExternalID", type: "sql.NullString"},
			{name: "ExternalIssuer", type: "sql.NullString"},
		]
		result: {
			row: "Principal"
			fields: [
				{name: "ID", type: "string"},
				{name: "Name", type: "string"},
				{name: "Type", type: "string"},
				{name: "IsAdmin", type: "int64"},
				{name: "CreatedAt", type: "string"},
				{name: "ExternalID", type: "sql.NullString"},
				{name: "ExternalIssuer", type: "sql.NullString"},
			]
		}
		raw: {
			sql: """
				-- name: CreatePrincipalWithExternalID :one
				INSERT INTO principals (id, name, type, is_admin, external_id, external_issuer)
				VALUES (?, ?, ?, ?, ?, ?)
				RETURNING id, name, type, is_admin, created_at, external_id, external_issuer
				"""
			bind: ["ID", "Name", "Type", "IsAdmin", "ExternalID", "ExternalIssuer"]
		}
	},
	{
		name: "CreateRowFilter"
		kind: "one"
		paramMode: "struct"
		params: [
			{name: "ID", type: "string"},
			{name: "TableID", type: "string"},
			{name: "Name", type: "sql.NullString"},
			{name: "FilterSql", type: "string"},
			{name: "Description", type: "sql.NullString"},
		]
		result: {
			row: "RowFilter"
			fields: [
				{name: "ID", type: "string"},
				{name: "TableID", type: "string"},
				{name: "FilterSql", type: "string"},
				{name: "Description", type: "sql.NullString"},
				{name: "CreatedAt", type: "string"},
				{name: "Name", type: "sql.NullString"},
			]
		}
		raw: {
			sql: """
				-- name: CreateRowFilter :one
				INSERT INTO row_filters (id, table_id, name, filter_sql, description)
				VALUES (?, ?, ?, ?, ?)
				RETURNING id, table_id, filter_sql, description, created_at, name
				"""
			bind: ["ID", "TableID", "Name", "FilterSql", "Description"]
		}
	},
	{
		name: "CreateSemanticMetric"
		kind: "one"
		paramMode: "struct"
		params: [
			{name: "ID", type: "string"},
			{name: "SemanticModelID", type: "string"},
			{name: "Name", type: "string"},
			{name: "Description", type: "string"},
			{name: "MetricType", type: "string"},
			{name: "ExpressionMode", type: "string"},
			{name: "Expression", type: "string"},
			{name: "Label", type: "string"},
			{name: "RelationshipNames", type: "string"},
			{name: "FilterSql", type: "string"},
			{name: "DefaultTimeGrain", type: "string"},
			{name: "Format", type: "string"},
			{name: "Owner", type: "string"},
			{name: "CertificationState", type: "string"},
			{name: "CreatedBy", type: "string"},
		]
		result: {
			row: "SemanticMetric"
			fields: [
				{name: "ID", type: "string"},
				{name: "SemanticModelID", type: "string"},
				{name: "Name", type: "string"},
				{name: "Description", type: "string"},
				{name: "MetricType", type: "string"},
				{name: "ExpressionMode", type: "string"},
				{name: "Expression", type: "string"},
				{name: "DefaultTimeGrain", type: "string"},
				{name: "Format", type: "string"},
				{name: "Owner", type: "string"},
				{name: "CertificationState", type: "string"},
				{name: "CreatedBy", type: "string"},
				{name: "CreatedAt", type: "string"},
				{name: "UpdatedAt", type: "string"},
				{name: "Label", type: "string"},
				{name: "FilterSql", type: "string"},
				{name: "RelationshipNames", type: "string"},
			]
		}
		raw: {
			sql: """
				-- name: CreateSemanticMetric :one
				INSERT INTO semantic_metrics (
				    id, semantic_model_id, name, description, metric_type, expression_mode,
				    expression, label, relationship_names, filter_sql, default_time_grain, format, owner, certification_state, created_by
				)
				VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
				RETURNING id, semantic_model_id, name, description, metric_type, expression_mode, expression, default_time_grain, format, owner, certification_state, created_by, created_at, updated_at, label, filter_sql, relationship_names
				"""
			bind: ["ID", "SemanticModelID", "Name", "Description", "MetricType", "ExpressionMode", "Expression", "Label", "RelationshipNames", "FilterSql", "DefaultTimeGrain", "Format", "Owner", "CertificationState", "CreatedBy"]
		}
	},
	{
		name: "CreateSemanticModel"
		kind: "one"
		paramMode: "struct"
		params: [
			{name: "ID", type: "string"},
			{name: "Name", type: "string"},
			{name: "Description", type: "string"},
			{name: "Owner", type: "string"},
			{name: "BaseModelRef", type: "string"},
			{name: "DefaultTimeDimension", type: "string"},
			{name: "Tags", type: "string"},
			{name: "CreatedBy", type: "string"},
		]
		result: {
			row: "SemanticModel"
			fields: [
				{name: "ID", type: "string"},
				{name: "Name", type: "string"},
				{name: "Description", type: "string"},
				{name: "Owner", type: "string"},
				{name: "BaseModelRef", type: "string"},
				{name: "DefaultTimeDimension", type: "string"},
				{name: "Tags", type: "string"},
				{name: "CreatedBy", type: "string"},
				{name: "CreatedAt", type: "string"},
				{name: "UpdatedAt", type: "string"},
			]
		}
		raw: {
			sql: """
				-- name: CreateSemanticModel :one
				INSERT INTO semantic_models (
				    id, name, description, owner, base_model_ref,
				    default_time_dimension, tags, created_by
				)
				VALUES (?, ?, ?, ?, ?, ?, ?, ?)
				RETURNING id, name, description, owner, base_model_ref, default_time_dimension, tags, created_by, created_at, updated_at
				"""
			bind: ["ID", "Name", "Description", "Owner", "BaseModelRef", "DefaultTimeDimension", "Tags", "CreatedBy"]
		}
	},
	{
		name: "CreateSemanticPreAggregation"
		kind: "one"
		paramMode: "struct"
		params: [
			{name: "ID", type: "string"},
			{name: "SemanticModelID", type: "string"},
			{name: "Name", type: "string"},
			{name: "MetricSet", type: "string"},
			{name: "DimensionSet", type: "string"},
			{name: "Grain", type: "string"},
			{name: "TargetRelation", type: "string"},
			{name: "RefreshPolicy", type: "string"},
			{name: "CreatedBy", type: "string"},
		]
		result: {
			row: "SemanticPreAggregation"
			fields: [
				{name: "ID", type: "string"},
				{name: "SemanticModelID", type: "string"},
				{name: "Name", type: "string"},
				{name: "MetricSet", type: "string"},
				{name: "DimensionSet", type: "string"},
				{name: "Grain", type: "string"},
				{name: "TargetRelation", type: "string"},
				{name: "RefreshPolicy", type: "string"},
				{name: "CreatedBy", type: "string"},
				{name: "CreatedAt", type: "string"},
				{name: "UpdatedAt", type: "string"},
			]
		}
		raw: {
			sql: """
				-- name: CreateSemanticPreAggregation :one
				INSERT INTO semantic_pre_aggregations (
				    id, semantic_model_id, name, metric_set, dimension_set,
				    grain, target_relation, refresh_policy, created_by
				)
				VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
				RETURNING id, semantic_model_id, name, metric_set, dimension_set, grain, target_relation, refresh_policy, created_by, created_at, updated_at
				"""
			bind: ["ID", "SemanticModelID", "Name", "MetricSet", "DimensionSet", "Grain", "TargetRelation", "RefreshPolicy", "CreatedBy"]
		}
	},
	{
		name: "CreateSemanticRelationship"
		kind: "one"
		paramMode: "struct"
		params: [
			{name: "ID", type: "string"},
			{name: "Name", type: "string"},
			{name: "FromSemanticID", type: "string"},
			{name: "ToSemanticID", type: "string"},
			{name: "RelationshipType", type: "string"},
			{name: "JoinSql", type: "string"},
			{name: "Cost", type: "int64"},
			{name: "MaxHops", type: "int64"},
			{name: "CreatedBy", type: "string"},
		]
		result: {
			row: "SemanticRelationship"
			fields: [
				{name: "ID", type: "string"},
				{name: "Name", type: "string"},
				{name: "FromSemanticID", type: "string"},
				{name: "ToSemanticID", type: "string"},
				{name: "RelationshipType", type: "string"},
				{name: "JoinSql", type: "string"},
				{name: "Cost", type: "int64"},
				{name: "MaxHops", type: "int64"},
				{name: "CreatedBy", type: "string"},
				{name: "CreatedAt", type: "string"},
				{name: "UpdatedAt", type: "string"},
			]
		}
		raw: {
			sql: """
				-- name: CreateSemanticRelationship :one
				INSERT INTO semantic_relationships (
				    id, name, from_semantic_id, to_semantic_id, relationship_type,
				    join_sql, cost, max_hops, created_by
				)
				VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
				RETURNING id, name, from_semantic_id, to_semantic_id, relationship_type, join_sql, cost, max_hops, created_by, created_at, updated_at
				"""
			bind: ["ID", "Name", "FromSemanticID", "ToSemanticID", "RelationshipType", "JoinSql", "Cost", "MaxHops", "CreatedBy"]
		}
	},
	{
		name: "CreateStorageCredential"
		kind: "one"
		paramMode: "struct"
		params: [
			{name: "ID", type: "string"},
			{name: "Name", type: "string"},
			{name: "CredentialType", type: "string"},
			{name: "KeyIDEncrypted", type: "string"},
			{name: "SecretEncrypted", type: "string"},
			{name: "Endpoint", type: "string"},
			{name: "Region", type: "string"},
			{name: "UrlStyle", type: "string"},
			{name: "AzureAccountName", type: "string"},
			{name: "AzureAccountKeyEncrypted", type: "string"},
			{name: "AzureClientID", type: "string"},
			{name: "AzureTenantID", type: "string"},
			{name: "AzureClientSecretEncrypted", type: "string"},
			{name: "GcsKeyFilePath", type: "string"},
			{name: "Comment", type: "string"},
			{name: "Owner", type: "string"},
		]
		result: {
			row: "StorageCredential"
			fields: [
				{name: "ID", type: "string"},
				{name: "Name", type: "string"},
				{name: "CredentialType", type: "string"},
				{name: "KeyIDEncrypted", type: "string"},
				{name: "SecretEncrypted", type: "string"},
				{name: "Endpoint", type: "string"},
				{name: "Region", type: "string"},
				{name: "UrlStyle", type: "string"},
				{name: "Comment", type: "string"},
				{name: "Owner", type: "string"},
				{name: "CreatedAt", type: "string"},
				{name: "UpdatedAt", type: "string"},
				{name: "AzureAccountName", type: "string"},
				{name: "AzureAccountKeyEncrypted", type: "string"},
				{name: "AzureClientID", type: "string"},
				{name: "AzureTenantID", type: "string"},
				{name: "AzureClientSecretEncrypted", type: "string"},
				{name: "GcsKeyFilePath", type: "string"},
			]
		}
		raw: {
			sql: """
				-- name: CreateStorageCredential :one
				INSERT INTO storage_credentials (
				    id, name, credential_type,
				    key_id_encrypted, secret_encrypted, endpoint, region, url_style,
				    azure_account_name, azure_account_key_encrypted, azure_client_id, azure_tenant_id, azure_client_secret_encrypted,
				    gcs_key_file_path,
				    comment, owner
				) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
				RETURNING id, name, credential_type, key_id_encrypted, secret_encrypted, endpoint, region, url_style, comment, owner, created_at, updated_at, azure_account_name, azure_account_key_encrypted, azure_client_id, azure_tenant_id, azure_client_secret_encrypted, gcs_key_file_path
				"""
			bind: ["ID", "Name", "CredentialType", "KeyIDEncrypted", "SecretEncrypted", "Endpoint", "Region", "UrlStyle", "AzureAccountName", "AzureAccountKeyEncrypted", "AzureClientID", "AzureTenantID", "AzureClientSecretEncrypted", "GcsKeyFilePath", "Comment", "Owner"]
		}
	},
	{
		name: "CreateTag"
		kind: "one"
		paramMode: "struct"
		params: [
			{name: "ID", type: "string"},
			{name: "Key", type: "string"},
			{name: "Value", type: "sql.NullString"},
			{name: "CreatedBy", type: "string"},
		]
		result: {
			row: "Tag"
			fields: [
				{name: "ID", type: "string"},
				{name: "Key", type: "string"},
				{name: "Value", type: "sql.NullString"},
				{name: "CreatedBy", type: "string"},
				{name: "CreatedAt", type: "string"},
			]
		}
		raw: {
			sql: """
				-- name: CreateTag :one
				INSERT INTO tags (id, key, value, created_by) VALUES (?, ?, ?, ?) RETURNING id, "key", value, created_by, created_at
				"""
			bind: ["ID", "Key", "Value", "CreatedBy"]
		}
	},
	{
		name: "CreateTagAssignment"
		kind: "one"
		paramMode: "struct"
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
		raw: {
			sql: """
				-- name: CreateTagAssignment :one
				INSERT INTO tag_assignments (id, tag_id, securable_type, securable_id, column_name, assigned_by)
				VALUES (?, ?, ?, ?, ?, ?) RETURNING id, tag_id, securable_type, securable_id, column_name, assigned_by, assigned_at
				"""
			bind: ["ID", "TagID", "SecurableType", "SecurableID", "ColumnName", "AssignedBy"]
		}
	},
	{
		name: "CreateView"
		kind: "one"
		paramMode: "struct"
		params: [
			{name: "ID", type: "string"},
			{name: "SchemaID", type: "string"},
			{name: "Name", type: "string"},
			{name: "ViewDefinition", type: "string"},
			{name: "Comment", type: "sql.NullString"},
			{name: "Properties", type: "sql.NullString"},
			{name: "Owner", type: "string"},
			{name: "SourceTables", type: "sql.NullString"},
		]
		result: {
			row: "View"
			fields: [
				{name: "ID", type: "string"},
				{name: "SchemaID", type: "string"},
				{name: "Name", type: "string"},
				{name: "ViewDefinition", type: "string"},
				{name: "Comment", type: "sql.NullString"},
				{name: "Properties", type: "sql.NullString"},
				{name: "Owner", type: "string"},
				{name: "SourceTables", type: "sql.NullString"},
				{name: "CreatedAt", type: "string"},
				{name: "UpdatedAt", type: "string"},
				{name: "DeletedAt", type: "sql.NullString"},
			]
		}
		raw: {
			sql: """
				-- name: CreateView :one
				INSERT INTO views (id, schema_id, name, view_definition, comment, properties, owner, source_tables)
				VALUES (?, ?, ?, ?, ?, ?, ?, ?) RETURNING id, schema_id, name, view_definition, comment, properties, owner, source_tables, created_at, updated_at, deleted_at
				"""
			bind: ["ID", "SchemaID", "Name", "ViewDefinition", "Comment", "Properties", "Owner", "SourceTables"]
		}
	},
	{
		name: "CreateVolume"
		kind: "one"
		paramMode: "struct"
		params: [
			{name: "ID", type: "string"},
			{name: "Name", type: "string"},
			{name: "SchemaName", type: "string"},
			{name: "CatalogName", type: "string"},
			{name: "VolumeType", type: "string"},
			{name: "StorageLocation", type: "string"},
			{name: "Comment", type: "string"},
			{name: "Owner", type: "string"},
		]
		result: {
			row: "Volume"
			fields: [
				{name: "ID", type: "string"},
				{name: "Name", type: "string"},
				{name: "SchemaName", type: "string"},
				{name: "CatalogName", type: "string"},
				{name: "VolumeType", type: "string"},
				{name: "StorageLocation", type: "string"},
				{name: "Comment", type: "string"},
				{name: "Owner", type: "string"},
				{name: "CreatedAt", type: "string"},
				{name: "UpdatedAt", type: "string"},
			]
		}
		raw: {
			sql: """
				-- name: CreateVolume :one
				INSERT INTO volumes (id, name, schema_name, catalog_name, volume_type, storage_location, comment, owner)
				VALUES (?, ?, ?, ?, ?, ?, ?, ?)
				RETURNING id, name, schema_name, catalog_name, volume_type, storage_location, comment, owner, created_at, updated_at
				"""
			bind: ["ID", "Name", "SchemaName", "CatalogName", "VolumeType", "StorageLocation", "Comment", "Owner"]
		}
	},
	{
		name: "CreateWebSession"
		kind: "one"
		paramMode: "struct"
		params: [
			{name: "ID", type: "string"},
			{name: "PrincipalID", type: "string"},
			{name: "SessionHash", type: "string"},
			{name: "AuthMethod", type: "string"},
			{name: "UserAgent", type: "sql.NullString"},
			{name: "IpAddress", type: "sql.NullString"},
			{name: "ExpiresAt", type: "time.Time"},
			{name: "IdleExpiresAt", type: "time.Time"},
		]
		result: {
			row: "WebSession"
			fields: [
				{name: "ID", type: "string"},
				{name: "PrincipalID", type: "string"},
				{name: "SessionHash", type: "string"},
				{name: "AuthMethod", type: "string"},
				{name: "UserAgent", type: "sql.NullString"},
				{name: "IpAddress", type: "sql.NullString"},
				{name: "ExpiresAt", type: "time.Time"},
				{name: "IdleExpiresAt", type: "time.Time"},
				{name: "LastSeenAt", type: "time.Time"},
				{name: "RevokedAt", type: "sql.NullTime"},
				{name: "CreatedAt", type: "time.Time"},
				{name: "UpdatedAt", type: "time.Time"},
			]
		}
		raw: {
			sql: """
				-- name: CreateWebSession :one
				INSERT INTO web_sessions (
				  id, principal_id, session_hash, auth_method, user_agent, ip_address,
				  expires_at, idle_expires_at, last_seen_at
				)
				VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
				RETURNING id, principal_id, session_hash, auth_method, user_agent, ip_address, expires_at, idle_expires_at, last_seen_at, revoked_at, created_at, updated_at
				"""
			bind: ["ID", "PrincipalID", "SessionHash", "AuthMethod", "UserAgent", "IpAddress", "ExpiresAt", "IdleExpiresAt"]
		}
	},
	{
		name: "CreateWebauthnCredential"
		kind: "one"
		paramMode: "struct"
		params: [
			{name: "ID", type: "string"},
			{name: "PrincipalID", type: "string"},
			{name: "CredentialID", type: "string"},
			{name: "PublicKey", type: "string"},
			{name: "SignCount", type: "int64"},
			{name: "Transports", type: "sql.NullString"},
			{name: "BackupEligible", type: "int64"},
			{name: "BackupState", type: "int64"},
		]
		result: {
			row: "WebauthnCredential"
			fields: [
				{name: "ID", type: "string"},
				{name: "PrincipalID", type: "string"},
				{name: "CredentialID", type: "string"},
				{name: "PublicKey", type: "string"},
				{name: "SignCount", type: "int64"},
				{name: "Transports", type: "sql.NullString"},
				{name: "BackupEligible", type: "int64"},
				{name: "BackupState", type: "int64"},
				{name: "CreatedAt", type: "time.Time"},
				{name: "LastUsedAt", type: "sql.NullTime"},
			]
		}
		raw: {
			sql: """
				-- name: CreateWebauthnCredential :one
				INSERT INTO webauthn_credentials (
				  id, principal_id, credential_id, public_key, sign_count, transports, backup_eligible, backup_state
				)
				VALUES (?, ?, ?, ?, ?, ?, ?, ?)
				RETURNING id, principal_id, credential_id, public_key, sign_count, transports, backup_eligible, backup_state, created_at, last_used_at
				"""
			bind: ["ID", "PrincipalID", "CredentialID", "PublicKey", "SignCount", "Transports", "BackupEligible", "BackupState"]
		}
	},
	{
		name: "DeleteAPIKey"
		kind: "exec"
		paramMode: "single"
		params: [
			{name: "id", type: "string"},
		]
		raw: {
			sql: """
				-- name: DeleteAPIKey :exec
				DELETE FROM api_keys WHERE id = ?
				"""
			bind: ["id"]
		}
	},
	{
		name: "DeleteAuthIdentity"
		kind: "exec"
		paramMode: "single"
		params: [
			{name: "id", type: "string"},
		]
		raw: {
			sql: """
				-- name: DeleteAuthIdentity :exec
				DELETE FROM auth_identities
				WHERE id = ?
				"""
			bind: ["id"]
		}
	},
	{
		name: "DeleteCatalog"
		kind: "exec"
		paramMode: "single"
		params: [
			{name: "id", type: "string"},
		]
		raw: {
			sql: """
				-- name: DeleteCatalog :exec
				DELETE FROM catalogs WHERE id = ?
				"""
			bind: ["id"]
		}
	},
	{
		name: "DeleteCatalogMetadataByTypeAndName"
		kind: "exec"
		paramMode: "struct"
		params: [
			{name: "SecurableType", type: "string"},
			{name: "SecurableName", type: "string"},
		]
		raw: {
			sql: """
				-- name: DeleteCatalogMetadataByTypeAndName :exec
				DELETE FROM catalog_metadata
				WHERE securable_type = ? AND securable_name = ?
				"""
			bind: ["SecurableType", "SecurableName"]
		}
	},
	{
		name: "DeleteCatalogMetadataByTypeAndPattern"
		kind: "exec"
		paramMode: "struct"
		params: [
			{name: "SecurableType", type: "string"},
			{name: "SecurableName", type: "string"},
		]
		raw: {
			sql: """
				-- name: DeleteCatalogMetadataByTypeAndPattern :exec
				DELETE FROM catalog_metadata
				WHERE securable_type = ? AND securable_name LIKE ?
				"""
			bind: ["SecurableType", "SecurableName"]
		}
	},
	{
		name: "DeleteCell"
		kind: "exec"
		paramMode: "single"
		params: [
			{name: "id", type: "string"},
		]
		raw: {
			sql: """
				-- name: DeleteCell :exec
				DELETE FROM cells WHERE id = ?
				"""
			bind: ["id"]
		}
	},
	{
		name: "DeleteColumnLineageByEdgeID"
		kind: "exec"
		paramMode: "single"
		params: [
			{name: "lineageEdgeID", type: "string"},
		]
		raw: {
			sql: """
				-- name: DeleteColumnLineageByEdgeID :exec
				DELETE FROM column_lineage_edges WHERE lineage_edge_id = ?
				"""
			bind: ["lineageEdgeID"]
		}
	},
	{
		name: "DeleteColumnMask"
		kind: "exec"
		paramMode: "single"
		params: [
			{name: "id", type: "string"},
		]
		raw: {
			sql: """
				-- name: DeleteColumnMask :exec
				DELETE FROM column_masks WHERE id = ?
				"""
			bind: ["id"]
		}
	},
	{
		name: "DeleteColumnMasksByTable"
		kind: "exec"
		paramMode: "single"
		params: [
			{name: "tableID", type: "string"},
		]
		raw: {
			sql: """
				-- name: DeleteColumnMasksByTable :exec
				DELETE FROM column_masks WHERE table_id = ?
				"""
			bind: ["tableID"]
		}
	},
	{
		name: "DeleteColumnMetadataByTable"
		kind: "exec"
		paramMode: "single"
		params: [
			{name: "tableSecurableName", type: "string"},
		]
		raw: {
			sql: """
				-- name: DeleteColumnMetadataByTable :exec
				DELETE FROM column_metadata WHERE table_securable_name = ?
				"""
			bind: ["tableSecurableName"]
		}
	},
	{
		name: "DeleteColumnMetadataByTablePattern"
		kind: "exec"
		paramMode: "single"
		params: [
			{name: "tableSecurableName", type: "string"},
		]
		raw: {
			sql: """
				-- name: DeleteColumnMetadataByTablePattern :exec
				DELETE FROM column_metadata WHERE table_securable_name LIKE ?
				"""
			bind: ["tableSecurableName"]
		}
	},
	{
		name: "DeleteComputeAssignment"
		kind: "exec"
		paramMode: "single"
		params: [
			{name: "id", type: "string"},
		]
		raw: {
			sql: """
				-- name: DeleteComputeAssignment :exec
				DELETE FROM compute_assignments WHERE id = ?
				"""
			bind: ["id"]
		}
	},
	{
		name: "DeleteComputeEndpoint"
		kind: "exec"
		paramMode: "single"
		params: [
			{name: "id", type: "string"},
		]
		raw: {
			sql: """
				-- name: DeleteComputeEndpoint :exec
				DELETE FROM compute_endpoints WHERE id = ?
				"""
			bind: ["id"]
		}
	},
	{
		name: "DeleteDashboard"
		kind: "exec"
		paramMode: "single"
		params: [
			{name: "id", type: "string"},
		]
		raw: {
			sql: """
				-- name: DeleteDashboard :exec
				DELETE FROM dashboards WHERE id = ?
				"""
			bind: ["id"]
		}
	},
	{
		name: "DeleteDashboardWidget"
		kind: "exec"
		paramMode: "single"
		params: [
			{name: "id", type: "string"},
		]
		raw: {
			sql: """
				-- name: DeleteDashboardWidget :exec
				DELETE FROM dashboard_widgets WHERE id = ?
				"""
			bind: ["id"]
		}
	},
	{
		name: "DeleteExpiredAuthRecoveryCodes"
		kind: "execrows"
		raw: {
			sql: """
				-- name: DeleteExpiredAuthRecoveryCodes :execrows
				DELETE FROM auth_recovery_codes
				WHERE used_at IS NOT NULL OR expires_at <= CURRENT_TIMESTAMP
				"""
		}
	},
	{
		name: "DeleteExpiredKeys"
		kind: "execresult"
		raw: {
			sql: """
				-- name: DeleteExpiredKeys :execresult
				DELETE FROM api_keys WHERE expires_at IS NOT NULL AND expires_at <= datetime('now', 'localtime')
				"""
		}
	},
	{
		name: "DeleteExpiredOrRevokedWebSessions"
		kind: "execrows"
		raw: {
			sql: """
				-- name: DeleteExpiredOrRevokedWebSessions :execrows
				DELETE FROM web_sessions
				WHERE revoked_at IS NOT NULL
				   OR expires_at <= CURRENT_TIMESTAMP
				   OR idle_expires_at <= CURRENT_TIMESTAMP
				"""
		}
	},
	{
		name: "DeleteExternalLocation"
		kind: "exec"
		paramMode: "single"
		params: [
			{name: "id", type: "string"},
		]
		raw: {
			sql: """
				-- name: DeleteExternalLocation :exec
				DELETE FROM external_locations WHERE id = ?
				"""
			bind: ["id"]
		}
	},
	{
		name: "DeleteExternalTableColumns"
		kind: "exec"
		paramMode: "single"
		params: [
			{name: "externalTableID", type: "string"},
		]
		raw: {
			sql: """
				-- name: DeleteExternalTableColumns :exec
				DELETE FROM external_table_columns
				WHERE external_table_id = ?
				"""
			bind: ["externalTableID"]
		}
	},
	{
		name: "DeleteGitRepo"
		kind: "exec"
		paramMode: "single"
		params: [
			{name: "id", type: "string"},
		]
		raw: {
			sql: """
				-- name: DeleteGitRepo :exec
				DELETE FROM git_repos WHERE id = ?
				"""
			bind: ["id"]
		}
	},
	{
		name: "DeleteGroup"
		kind: "exec"
		paramMode: "single"
		params: [
			{name: "id", type: "string"},
		]
		raw: {
			sql: """
				-- name: DeleteGroup :exec
				DELETE FROM groups WHERE id = ?
				"""
			bind: ["id"]
		}
	},
	{
		name: "DeleteLineageByTable"
		kind: "exec"
		paramMode: "struct"
		params: [
			{name: "SourceTable", type: "string"},
			{name: "TargetTable", type: "sql.NullString"},
		]
		raw: {
			sql: """
				-- name: DeleteLineageByTable :exec
				DELETE FROM lineage_edges WHERE source_table = ? OR target_table = ?
				"""
			bind: ["SourceTable", "TargetTable"]
		}
	},
	{
		name: "DeleteLineageByTablePattern"
		kind: "exec"
		paramMode: "struct"
		params: [
			{name: "SourceTable", type: "string"},
			{name: "TargetTable", type: "sql.NullString"},
		]
		raw: {
			sql: """
				-- name: DeleteLineageByTablePattern :exec
				DELETE FROM lineage_edges WHERE source_table LIKE ? OR target_table LIKE ?
				"""
			bind: ["SourceTable", "TargetTable"]
		}
	},
	{
		name: "DeleteLineageEdge"
		kind: "exec"
		paramMode: "single"
		params: [
			{name: "id", type: "string"},
		]
		raw: {
			sql: """
				-- name: DeleteLineageEdge :exec
				DELETE FROM lineage_edges WHERE id = ?
				"""
			bind: ["id"]
		}
	},
	{
		name: "DeleteLocalCredential"
		kind: "exec"
		paramMode: "single"
		params: [
			{name: "principalID", type: "string"},
		]
		raw: {
			sql: """
				-- name: DeleteLocalCredential :exec
				DELETE FROM local_credentials
				WHERE principal_id = ?
				"""
			bind: ["principalID"]
		}
	},
	{
		name: "DeleteMacro"
		kind: "exec"
		paramMode: "single"
		params: [
			{name: "name", type: "string"},
		]
		raw: {
			sql: """
				-- name: DeleteMacro :exec
				DELETE FROM macros WHERE name = ?
				"""
			bind: ["name"]
		}
	},
	{
		name: "DeleteModel"
		kind: "exec"
		paramMode: "single"
		params: [
			{name: "id", type: "string"},
		]
		raw: {
			sql: """
				-- name: DeleteModel :exec
				DELETE FROM models WHERE id = ?
				"""
			bind: ["id"]
		}
	},
	{
		name: "DeleteModelTest"
		kind: "exec"
		paramMode: "single"
		params: [
			{name: "id", type: "string"},
		]
		raw: {
			sql: """
				-- name: DeleteModelTest :exec
				DELETE FROM model_tests WHERE id = ?
				"""
			bind: ["id"]
		}
	},
	{
		name: "DeleteNotebook"
		kind: "exec"
		paramMode: "single"
		params: [
			{name: "id", type: "string"},
		]
		raw: {
			sql: """
				-- name: DeleteNotebook :exec
				DELETE FROM notebooks WHERE id = ?
				"""
			bind: ["id"]
		}
	},
	{
		name: "DeleteNotebookModelLinkByNotebookID"
		kind: "exec"
		paramMode: "single"
		params: [
			{name: "notebookID", type: "string"},
		]
		raw: {
			sql: """
				-- name: DeleteNotebookModelLinkByNotebookID :exec
				DELETE FROM notebook_model_links WHERE notebook_id = ?
				"""
			bind: ["notebookID"]
		}
	},
	{
		name: "DeletePipeline"
		kind: "exec"
		paramMode: "single"
		params: [
			{name: "id", type: "string"},
		]
		raw: {
			sql: """
				-- name: DeletePipeline :exec
				DELETE FROM pipelines WHERE id = ?
				"""
			bind: ["id"]
		}
	},
	{
		name: "DeletePipelineJob"
		kind: "exec"
		paramMode: "single"
		params: [
			{name: "id", type: "string"},
		]
		raw: {
			sql: """
				-- name: DeletePipelineJob :exec
				DELETE FROM pipeline_jobs WHERE id = ?
				"""
			bind: ["id"]
		}
	},
	{
		name: "DeletePipelineJobsByPipeline"
		kind: "exec"
		paramMode: "single"
		params: [
			{name: "pipelineID", type: "string"},
		]
		raw: {
			sql: """
				-- name: DeletePipelineJobsByPipeline :exec
				DELETE FROM pipeline_jobs WHERE pipeline_id = ?
				"""
			bind: ["pipelineID"]
		}
	},
	{
		name: "DeletePrincipal"
		kind: "exec"
		paramMode: "single"
		params: [
			{name: "id", type: "string"},
		]
		raw: {
			sql: """
				-- name: DeletePrincipal :exec
				DELETE FROM principals WHERE id = ?
				"""
			bind: ["id"]
		}
	},
	{
		name: "DeleteRowFilter"
		kind: "execresult"
		paramMode: "single"
		params: [
			{name: "id", type: "string"},
		]
		raw: {
			sql: """
				-- name: DeleteRowFilter :execresult
				DELETE FROM row_filters WHERE id = ?
				"""
			bind: ["id"]
		}
	},
	{
		name: "DeleteRowFiltersByTable"
		kind: "exec"
		paramMode: "single"
		params: [
			{name: "tableID", type: "string"},
		]
		raw: {
			sql: """
				-- name: DeleteRowFiltersByTable :exec
				DELETE FROM row_filters WHERE table_id = ?
				"""
			bind: ["tableID"]
		}
	},
	{
		name: "DeleteSemanticMetric"
		kind: "exec"
		paramMode: "single"
		params: [
			{name: "id", type: "string"},
		]
		raw: {
			sql: """
				-- name: DeleteSemanticMetric :exec
				DELETE FROM semantic_metrics WHERE id = ?
				"""
			bind: ["id"]
		}
	},
	{
		name: "DeleteSemanticModel"
		kind: "exec"
		paramMode: "single"
		params: [
			{name: "id", type: "string"},
		]
		raw: {
			sql: """
				-- name: DeleteSemanticModel :exec
				DELETE FROM semantic_models WHERE id = ?
				"""
			bind: ["id"]
		}
	},
	{
		name: "DeleteSemanticPreAggregation"
		kind: "exec"
		paramMode: "single"
		params: [
			{name: "id", type: "string"},
		]
		raw: {
			sql: """
				-- name: DeleteSemanticPreAggregation :exec
				DELETE FROM semantic_pre_aggregations WHERE id = ?
				"""
			bind: ["id"]
		}
	},
	{
		name: "DeleteSemanticRelationship"
		kind: "exec"
		paramMode: "single"
		params: [
			{name: "id", type: "string"},
		]
		raw: {
			sql: """
				-- name: DeleteSemanticRelationship :exec
				DELETE FROM semantic_relationships WHERE id = ?
				"""
			bind: ["id"]
		}
	},
	{
		name: "DeleteStorageCredential"
		kind: "exec"
		paramMode: "single"
		params: [
			{name: "id", type: "string"},
		]
		raw: {
			sql: """
				-- name: DeleteStorageCredential :exec
				DELETE FROM storage_credentials WHERE id = ?
				"""
			bind: ["id"]
		}
	},
	{
		name: "DeleteTableStatistics"
		kind: "exec"
		paramMode: "single"
		params: [
			{name: "tableSecurableName", type: "string"},
		]
		raw: {
			sql: """
				-- name: DeleteTableStatistics :exec
				DELETE FROM table_statistics WHERE table_securable_name = ?
				"""
			bind: ["tableSecurableName"]
		}
	},
	{
		name: "DeleteTableStatisticsByPattern"
		kind: "exec"
		paramMode: "single"
		params: [
			{name: "tableSecurableName", type: "string"},
		]
		raw: {
			sql: """
				-- name: DeleteTableStatisticsByPattern :exec
				DELETE FROM table_statistics WHERE table_securable_name LIKE ?
				"""
			bind: ["tableSecurableName"]
		}
	},
	{
		name: "DeleteTag"
		kind: "exec"
		paramMode: "single"
		params: [
			{name: "id", type: "string"},
		]
		raw: {
			sql: """
				-- name: DeleteTag :exec
				DELETE FROM tags WHERE id = ?
				"""
			bind: ["id"]
		}
	},
	{
		name: "DeleteTagAssignment"
		kind: "exec"
		paramMode: "single"
		params: [
			{name: "id", type: "string"},
		]
		raw: {
			sql: """
				-- name: DeleteTagAssignment :exec
				DELETE FROM tag_assignments WHERE id = ?
				"""
			bind: ["id"]
		}
	},
	{
		name: "DeleteTagAssignmentsBySecurable"
		kind: "exec"
		paramMode: "struct"
		params: [
			{name: "SecurableType", type: "string"},
			{name: "SecurableID", type: "string"},
		]
		raw: {
			sql: """
				-- name: DeleteTagAssignmentsBySecurable :exec
				DELETE FROM tag_assignments WHERE securable_type = ? AND securable_id = ?
				"""
			bind: ["SecurableType", "SecurableID"]
		}
	},
	{
		name: "DeleteTagAssignmentsBySecurableTypes"
		kind: "exec"
		paramMode: "struct"
		params: [
			{name: "SecurableType", type: "string"},
			{name: "SecurableType_2", type: "string"},
			{name: "SecurableID", type: "string"},
		]
		raw: {
			sql: """
				-- name: DeleteTagAssignmentsBySecurableTypes :exec
				DELETE FROM tag_assignments WHERE securable_type IN (?, ?) AND securable_id = ?
				"""
			bind: ["SecurableType", "SecurableType_2", "SecurableID"]
		}
	},
	{
		name: "DeleteView"
		kind: "exec"
		paramMode: "struct"
		params: [
			{name: "SchemaID", type: "string"},
			{name: "Name", type: "string"},
		]
		raw: {
			sql: """
				-- name: DeleteView :exec
				UPDATE views SET deleted_at = datetime('now') WHERE schema_id = ? AND name = ?
				"""
			bind: ["SchemaID", "Name"]
		}
	},
	{
		name: "DeleteViewsBySchema"
		kind: "exec"
		paramMode: "single"
		params: [
			{name: "schemaID", type: "string"},
		]
		raw: {
			sql: """
				-- name: DeleteViewsBySchema :exec
				UPDATE views SET deleted_at = datetime('now') WHERE schema_id = ?
				"""
			bind: ["schemaID"]
		}
	},
	{
		name: "DeleteVolume"
		kind: "exec"
		paramMode: "single"
		params: [
			{name: "id", type: "string"},
		]
		raw: {
			sql: """
				-- name: DeleteVolume :exec
				DELETE FROM volumes WHERE id = ?
				"""
			bind: ["id"]
		}
	},
	{
		name: "DeleteVolumesBySchema"
		kind: "exec"
		paramMode: "single"
		params: [
			{name: "schemaName", type: "string"},
		]
		raw: {
			sql: """
				-- name: DeleteVolumesBySchema :exec
				DELETE FROM volumes WHERE schema_name = ?
				"""
			bind: ["schemaName"]
		}
	},
	{
		name: "DeleteWebauthnCredential"
		kind: "exec"
		paramMode: "single"
		params: [
			{name: "id", type: "string"},
		]
		raw: {
			sql: """
				-- name: DeleteWebauthnCredential :exec
				DELETE FROM webauthn_credentials
				WHERE id = ?
				"""
			bind: ["id"]
		}
	},
	{
		name: "GetAPIKeyByHash"
		kind: "one"
		paramMode: "single"
		params: [
			{name: "keyHash", type: "string"},
		]
		result: {
			row: "GetAPIKeyByHashRow"
			fields: [
				{name: "ID", type: "string"},
				{name: "KeyHash", type: "string"},
				{name: "PrincipalID", type: "string"},
				{name: "Name", type: "string"},
				{name: "ExpiresAt", type: "sql.NullString"},
				{name: "CreatedAt", type: "string"},
				{name: "KeyPrefix", type: "sql.NullString"},
				{name: "PrincipalName", type: "string"},
			]
		}
		raw: {
			sql: """
				-- name: GetAPIKeyByHash :one
				SELECT ak.id, ak.key_hash, ak.principal_id, ak.name, ak.expires_at, ak.created_at, ak.key_prefix, p.name as principal_name FROM api_keys ak
				JOIN principals p ON ak.principal_id = p.id
				WHERE ak.key_hash = ? AND (ak.expires_at IS NULL OR ak.expires_at > datetime('now', 'localtime'))
				"""
			bind: ["keyHash"]
		}
	},
	{
		name: "GetAPIKeyByID"
		kind: "one"
		paramMode: "single"
		params: [
			{name: "id", type: "string"},
		]
		result: {
			row: "ApiKey"
			fields: [
				{name: "ID", type: "string"},
				{name: "KeyHash", type: "string"},
				{name: "PrincipalID", type: "string"},
				{name: "Name", type: "string"},
				{name: "ExpiresAt", type: "sql.NullString"},
				{name: "CreatedAt", type: "string"},
				{name: "KeyPrefix", type: "sql.NullString"},
			]
		}
		raw: {
			sql: """
				-- name: GetAPIKeyByID :one
				SELECT id, key_hash, principal_id, name, expires_at, created_at, key_prefix FROM api_keys WHERE id = ?
				"""
			bind: ["id"]
		}
	},
	{
		name: "GetActiveWebSessionByHash"
		kind: "one"
		paramMode: "single"
		params: [
			{name: "sessionHash", type: "string"},
		]
		result: {
			row: "WebSession"
			fields: [
				{name: "ID", type: "string"},
				{name: "PrincipalID", type: "string"},
				{name: "SessionHash", type: "string"},
				{name: "AuthMethod", type: "string"},
				{name: "UserAgent", type: "sql.NullString"},
				{name: "IpAddress", type: "sql.NullString"},
				{name: "ExpiresAt", type: "time.Time"},
				{name: "IdleExpiresAt", type: "time.Time"},
				{name: "LastSeenAt", type: "time.Time"},
				{name: "RevokedAt", type: "sql.NullTime"},
				{name: "CreatedAt", type: "time.Time"},
				{name: "UpdatedAt", type: "time.Time"},
			]
		}
		raw: {
			sql: """
				-- name: GetActiveWebSessionByHash :one
				SELECT id, principal_id, session_hash, auth_method, user_agent, ip_address, expires_at, idle_expires_at, last_seen_at, revoked_at, created_at, updated_at
				FROM web_sessions
				WHERE session_hash = ?
				  AND revoked_at IS NULL
				  AND expires_at > CURRENT_TIMESTAMP
				  AND idle_expires_at > CURRENT_TIMESTAMP
				LIMIT 1
				"""
			bind: ["sessionHash"]
		}
	},
	{
		name: "GetAssignmentsForPrincipal"
		kind: "many"
		paramMode: "struct"
		params: [
			{name: "PrincipalID", type: "string"},
			{name: "PrincipalType", type: "string"},
		]
		result: {
			row: "ComputeEndpoint"
			fields: [
				{name: "ID", type: "string"},
				{name: "ExternalID", type: "string"},
				{name: "Name", type: "string"},
				{name: "Url", type: "string"},
				{name: "Type", type: "string"},
				{name: "Status", type: "string"},
				{name: "Size", type: "string"},
				{name: "MaxMemoryGb", type: "sql.NullInt64"},
				{name: "AuthToken", type: "string"},
				{name: "Owner", type: "string"},
				{name: "CreatedAt", type: "time.Time"},
				{name: "UpdatedAt", type: "time.Time"},
				{name: "SelectionPolicy", type: "string"},
				{name: "WorkloadClass", type: "string"},
				{name: "ReadinessStatus", type: "string"},
				{name: "MaxConcurrency", type: "sql.NullInt64"},
				{name: "MaxResultSizeMb", type: "sql.NullInt64"},
				{name: "RecommendedForLargeQueries", type: "int64"},
				{name: "IsDraining", type: "int64"},
				{name: "LastHealthStatus", type: "sql.NullString"},
				{name: "LastHealthCheckedAt", type: "sql.NullTime"},
				{name: "ActiveQueries", type: "sql.NullInt64"},
				{name: "QueuedJobs", type: "sql.NullInt64"},
				{name: "RunningJobs", type: "sql.NullInt64"},
				{name: "CompletedJobs", type: "sql.NullInt64"},
				{name: "StoredJobs", type: "sql.NullInt64"},
				{name: "CleanedJobs", type: "sql.NullInt64"},
				{name: "QueryResultTtlSeconds", type: "sql.NullInt64"},
			]
		}
		raw: {
			sql: """
				-- name: GetAssignmentsForPrincipal :many
				SELECT ce.id, ce.external_id, ce.name, ce.url, ce.type, ce.status, ce.size, ce.max_memory_gb, ce.auth_token, ce.owner, ce.created_at, ce.updated_at, ce.selection_policy, ce.workload_class, ce.readiness_status, ce.max_concurrency, ce.max_result_size_mb, ce.recommended_for_large_queries, ce.is_draining, ce.last_health_status, ce.last_health_checked_at, ce.active_queries, ce.queued_jobs, ce.running_jobs, ce.completed_jobs, ce.stored_jobs, ce.cleaned_jobs, ce.query_result_ttl_seconds
				FROM compute_endpoints ce
				JOIN compute_assignments ca ON ca.endpoint_id = ce.id
				WHERE ca.principal_id = ?
				  AND ca.principal_type = ?
				ORDER BY ca.is_default DESC, ce.name
				"""
			bind: ["PrincipalID", "PrincipalType"]
		}
	},
	{
		name: "GetAuthIdentityByProviderSubject"
		kind: "one"
		paramMode: "struct"
		params: [
			{name: "Provider", type: "string"},
			{name: "Issuer", type: "sql.NullString"},
			{name: "Subject", type: "string"},
		]
		result: {
			row: "AuthIdentity"
			fields: [
				{name: "ID", type: "string"},
				{name: "PrincipalID", type: "string"},
				{name: "Provider", type: "string"},
				{name: "Issuer", type: "sql.NullString"},
				{name: "Subject", type: "string"},
				{name: "Email", type: "sql.NullString"},
				{name: "EmailVerified", type: "int64"},
				{name: "CreatedAt", type: "time.Time"},
				{name: "UpdatedAt", type: "time.Time"},
			]
		}
		raw: {
			sql: """
				-- name: GetAuthIdentityByProviderSubject :one
				SELECT id, principal_id, provider, issuer, subject, email, email_verified, created_at, updated_at
				FROM auth_identities
				WHERE provider = ? AND issuer IS ? AND subject = ?
				LIMIT 1
				"""
			bind: ["Provider", "Issuer", "Subject"]
		}
	},
	{
		name: "GetAuthProviderConfig"
		kind: "one"
		result: {
			row: "AuthProvider"
			fields: [
				{name: "ID", type: "int64"},
				{name: "OidcEnabled", type: "int64"},
				{name: "OidcIssuerUrl", type: "sql.NullString"},
				{name: "OidcJwksUrl", type: "sql.NullString"},
				{name: "OidcAudience", type: "sql.NullString"},
				{name: "OidcClientID", type: "sql.NullString"},
				{name: "OidcClientSecretEnc", type: "sql.NullString"},
				{name: "OidcScopes", type: "sql.NullString"},
				{name: "CreatedAt", type: "time.Time"},
				{name: "UpdatedAt", type: "time.Time"},
			]
		}
		raw: {
			sql: """
				-- name: GetAuthProviderConfig :one
				SELECT id, oidc_enabled, oidc_issuer_url, oidc_jwks_url, oidc_audience, oidc_client_id, oidc_client_secret_enc, oidc_scopes, created_at, updated_at FROM auth_providers WHERE id = 1
				"""
		}
	},
	{
		name: "GetCatalogByID"
		kind: "one"
		paramMode: "single"
		params: [
			{name: "id", type: "string"},
		]
		result: {
			row: "Catalog"
			fields: [
				{name: "ID", type: "string"},
				{name: "Name", type: "string"},
				{name: "MetastoreType", type: "string"},
				{name: "Dsn", type: "string"},
				{name: "DataPath", type: "string"},
				{name: "Status", type: "string"},
				{name: "StatusMessage", type: "sql.NullString"},
				{name: "IsDefault", type: "int64"},
				{name: "Comment", type: "sql.NullString"},
				{name: "CreatedAt", type: "string"},
				{name: "UpdatedAt", type: "string"},
			]
		}
		raw: {
			sql: """
				-- name: GetCatalogByID :one
				SELECT id, name, metastore_type, dsn, data_path, status, status_message, is_default, comment, created_at, updated_at FROM catalogs WHERE id = ?
				"""
			bind: ["id"]
		}
	},
	{
		name: "GetCatalogByName"
		kind: "one"
		paramMode: "single"
		params: [
			{name: "name", type: "string"},
		]
		result: {
			row: "Catalog"
			fields: [
				{name: "ID", type: "string"},
				{name: "Name", type: "string"},
				{name: "MetastoreType", type: "string"},
				{name: "Dsn", type: "string"},
				{name: "DataPath", type: "string"},
				{name: "Status", type: "string"},
				{name: "StatusMessage", type: "sql.NullString"},
				{name: "IsDefault", type: "int64"},
				{name: "Comment", type: "sql.NullString"},
				{name: "CreatedAt", type: "string"},
				{name: "UpdatedAt", type: "string"},
			]
		}
		raw: {
			sql: """
				-- name: GetCatalogByName :one
				SELECT id, name, metastore_type, dsn, data_path, status, status_message, is_default, comment, created_at, updated_at FROM catalogs WHERE name = ?
				"""
			bind: ["name"]
		}
	},
	{
		name: "GetCatalogMetadata"
		kind: "one"
		paramMode: "struct"
		params: [
			{name: "SecurableType", type: "string"},
			{name: "SecurableName", type: "string"},
		]
		result: {
			row: "CatalogMetadatum"
			fields: [
				{name: "SecurableType", type: "string"},
				{name: "SecurableName", type: "string"},
				{name: "Comment", type: "sql.NullString"},
				{name: "Properties", type: "sql.NullString"},
				{name: "Owner", type: "sql.NullString"},
				{name: "CreatedAt", type: "string"},
				{name: "UpdatedAt", type: "string"},
				{name: "DeletedAt", type: "sql.NullString"},
			]
		}
		raw: {
			sql: """
				-- name: GetCatalogMetadata :one
				SELECT securable_type, securable_name, comment, properties, owner, created_at, updated_at, deleted_at FROM catalog_metadata
				WHERE securable_type = ? AND securable_name = ?
				"""
			bind: ["SecurableType", "SecurableName"]
		}
	},
	{
		name: "GetCell"
		kind: "one"
		paramMode: "single"
		params: [
			{name: "id", type: "string"},
		]
		result: {
			row: "Cell"
			fields: [
				{name: "ID", type: "string"},
				{name: "NotebookID", type: "string"},
				{name: "CellType", type: "string"},
				{name: "Content", type: "string"},
				{name: "Position", type: "int64"},
				{name: "LastResult", type: "sql.NullString"},
				{name: "CreatedAt", type: "string"},
				{name: "UpdatedAt", type: "string"},
				{name: "Name", type: "sql.NullString"},
				{name: "Role", type: "string"},
				{name: "Disabled", type: "int64"},
				{name: "TestConfig", type: "string"},
				{name: "VisualSpec", type: "string"},
			]
		}
		raw: {
			sql: """
				-- name: GetCell :one
				SELECT id, notebook_id, cell_type, content, position, last_result, created_at, updated_at, name, role, disabled, test_config, visual_spec FROM cells WHERE id = ?
				"""
			bind: ["id"]
		}
	},
	{
		name: "GetColumnLineageByEdgeID"
		kind: "many"
		paramMode: "single"
		params: [
			{name: "lineageEdgeID", type: "string"},
		]
		result: {
			row: "ColumnLineageEdge"
			fields: [
				{name: "ID", type: "int64"},
				{name: "LineageEdgeID", type: "string"},
				{name: "TargetColumn", type: "string"},
				{name: "SourceSchema", type: "string"},
				{name: "SourceTable", type: "string"},
				{name: "SourceColumn", type: "string"},
				{name: "TransformType", type: "string"},
				{name: "FunctionName", type: "string"},
				{name: "CreatedAt", type: "time.Time"},
			]
		}
		raw: {
			sql: """
				-- name: GetColumnLineageByEdgeID :many
				SELECT id, lineage_edge_id, target_column, source_schema, source_table,
				       source_column, transform_type, function_name, created_at
				FROM column_lineage_edges
				WHERE lineage_edge_id = ?
				ORDER BY target_column, source_table, source_column
				"""
			bind: ["lineageEdgeID"]
		}
	},
	{
		name: "GetColumnLineageForSourceColumn"
		kind: "many"
		paramMode: "struct"
		params: [
			{name: "SourceSchema", type: "string"},
			{name: "SourceTable", type: "string"},
			{name: "SourceColumn", type: "string"},
		]
		result: {
			row: "ColumnLineageEdge"
			fields: [
				{name: "ID", type: "int64"},
				{name: "LineageEdgeID", type: "string"},
				{name: "TargetColumn", type: "string"},
				{name: "SourceSchema", type: "string"},
				{name: "SourceTable", type: "string"},
				{name: "SourceColumn", type: "string"},
				{name: "TransformType", type: "string"},
				{name: "FunctionName", type: "string"},
				{name: "CreatedAt", type: "time.Time"},
			]
		}
		raw: {
			sql: """
				-- name: GetColumnLineageForSourceColumn :many
				SELECT cle.id, cle.lineage_edge_id, cle.target_column, cle.source_schema,
				       cle.source_table, cle.source_column, cle.transform_type, cle.function_name,
				       cle.created_at
				FROM column_lineage_edges cle
				JOIN lineage_edges le ON le.id = cle.lineage_edge_id
				WHERE cle.source_schema = ? AND cle.source_table = ? AND cle.source_column = ?
				ORDER BY cle.target_column
				"""
			bind: ["SourceSchema", "SourceTable", "SourceColumn"]
		}
	},
	{
		name: "GetColumnLineageForTable"
		kind: "many"
		paramMode: "struct"
		params: [
			{name: "TargetSchema", type: "sql.NullString"},
			{name: "TargetTable", type: "sql.NullString"},
		]
		result: {
			row: "ColumnLineageEdge"
			fields: [
				{name: "ID", type: "int64"},
				{name: "LineageEdgeID", type: "string"},
				{name: "TargetColumn", type: "string"},
				{name: "SourceSchema", type: "string"},
				{name: "SourceTable", type: "string"},
				{name: "SourceColumn", type: "string"},
				{name: "TransformType", type: "string"},
				{name: "FunctionName", type: "string"},
				{name: "CreatedAt", type: "time.Time"},
			]
		}
		raw: {
			sql: """
				-- name: GetColumnLineageForTable :many
				SELECT cle.id, cle.lineage_edge_id, cle.target_column, cle.source_schema,
				       cle.source_table, cle.source_column, cle.transform_type, cle.function_name,
				       cle.created_at
				FROM column_lineage_edges cle
				JOIN lineage_edges le ON le.id = cle.lineage_edge_id
				WHERE le.target_schema = ? AND le.target_table = ?
				ORDER BY cle.target_column, cle.source_table, cle.source_column
				"""
			bind: ["TargetSchema", "TargetTable"]
		}
	},
	{
		name: "GetColumnMaskBindingsForMask"
		kind: "many"
		paramMode: "single"
		params: [
			{name: "columnMaskID", type: "string"},
		]
		result: {
			row: "ColumnMaskBinding"
			fields: [
				{name: "ID", type: "string"},
				{name: "ColumnMaskID", type: "string"},
				{name: "PrincipalID", type: "string"},
				{name: "PrincipalType", type: "string"},
				{name: "SeeOriginal", type: "int64"},
			]
		}
		raw: {
			sql: """
				-- name: GetColumnMaskBindingsForMask :many
				SELECT id, column_mask_id, principal_id, principal_type, see_original FROM column_mask_bindings WHERE column_mask_id = ?
				"""
			bind: ["columnMaskID"]
		}
	},
	{
		name: "GetColumnMaskBindingsForPrincipal"
		kind: "many"
		paramMode: "struct"
		params: [
			{name: "PrincipalID", type: "string"},
			{name: "PrincipalType", type: "string"},
		]
		result: {
			row: "GetColumnMaskBindingsForPrincipalRow"
			fields: [
				{name: "TableID", type: "string"},
				{name: "ColumnName", type: "string"},
				{name: "MaskExpression", type: "string"},
				{name: "SeeOriginal", type: "int64"},
			]
		}
		raw: {
			sql: """
				-- name: GetColumnMaskBindingsForPrincipal :many
				SELECT cm.table_id, cm.column_name, cm.mask_expression, cmb.see_original
				FROM column_masks cm
				JOIN column_mask_bindings cmb ON cm.id = cmb.column_mask_id
				WHERE cmb.principal_id = ? AND cmb.principal_type = ?
				"""
			bind: ["PrincipalID", "PrincipalType"]
		}
	},
	{
		name: "GetColumnMaskForTableAndPrincipal"
		kind: "many"
		paramMode: "struct"
		params: [
			{name: "TableID", type: "string"},
			{name: "PrincipalID", type: "string"},
			{name: "PrincipalType", type: "string"},
		]
		result: {
			row: "GetColumnMaskForTableAndPrincipalRow"
			fields: [
				{name: "ColumnName", type: "string"},
				{name: "MaskExpression", type: "string"},
				{name: "SeeOriginal", type: "int64"},
			]
		}
		raw: {
			sql: """
				-- name: GetColumnMaskForTableAndPrincipal :many
				SELECT cm.column_name, cm.mask_expression, cmb.see_original
				FROM column_masks cm
				JOIN column_mask_bindings cmb ON cm.id = cmb.column_mask_id
				WHERE cm.table_id = ? AND cmb.principal_id = ? AND cmb.principal_type = ?
				"""
			bind: ["TableID", "PrincipalID", "PrincipalType"]
		}
	},
	{
		name: "GetColumnMasksForTable"
		kind: "many"
		paramMode: "single"
		params: [
			{name: "tableID", type: "string"},
		]
		result: {
			row: "ColumnMask"
			fields: [
				{name: "ID", type: "string"},
				{name: "TableID", type: "string"},
				{name: "ColumnName", type: "string"},
				{name: "MaskExpression", type: "string"},
				{name: "Description", type: "sql.NullString"},
				{name: "CreatedAt", type: "string"},
				{name: "Name", type: "sql.NullString"},
			]
		}
		raw: {
			sql: """
				-- name: GetColumnMasksForTable :many
				SELECT id, table_id, column_name, mask_expression, description, created_at, name FROM column_masks WHERE table_id = ?
				"""
			bind: ["tableID"]
		}
	},
	{
		name: "GetColumnMetadata"
		kind: "one"
		paramMode: "struct"
		params: [
			{name: "TableSecurableName", type: "string"},
			{name: "ColumnName", type: "string"},
		]
		result: {
			row: "ColumnMetadatum"
			fields: [
				{name: "TableSecurableName", type: "string"},
				{name: "ColumnName", type: "string"},
				{name: "Comment", type: "sql.NullString"},
				{name: "Properties", type: "sql.NullString"},
				{name: "UpdatedAt", type: "sql.NullString"},
			]
		}
		raw: {
			sql: """
				-- name: GetColumnMetadata :one
				SELECT table_securable_name, column_name, comment, properties, updated_at FROM column_metadata
				WHERE table_securable_name = ? AND column_name = ?
				"""
			bind: ["TableSecurableName", "ColumnName"]
		}
	},
	{
		name: "GetComputeEndpoint"
		kind: "one"
		paramMode: "single"
		params: [
			{name: "id", type: "string"},
		]
		result: {
			row: "ComputeEndpoint"
			fields: [
				{name: "ID", type: "string"},
				{name: "ExternalID", type: "string"},
				{name: "Name", type: "string"},
				{name: "Url", type: "string"},
				{name: "Type", type: "string"},
				{name: "Status", type: "string"},
				{name: "Size", type: "string"},
				{name: "MaxMemoryGb", type: "sql.NullInt64"},
				{name: "AuthToken", type: "string"},
				{name: "Owner", type: "string"},
				{name: "CreatedAt", type: "time.Time"},
				{name: "UpdatedAt", type: "time.Time"},
				{name: "SelectionPolicy", type: "string"},
				{name: "WorkloadClass", type: "string"},
				{name: "ReadinessStatus", type: "string"},
				{name: "MaxConcurrency", type: "sql.NullInt64"},
				{name: "MaxResultSizeMb", type: "sql.NullInt64"},
				{name: "RecommendedForLargeQueries", type: "int64"},
				{name: "IsDraining", type: "int64"},
				{name: "LastHealthStatus", type: "sql.NullString"},
				{name: "LastHealthCheckedAt", type: "sql.NullTime"},
				{name: "ActiveQueries", type: "sql.NullInt64"},
				{name: "QueuedJobs", type: "sql.NullInt64"},
				{name: "RunningJobs", type: "sql.NullInt64"},
				{name: "CompletedJobs", type: "sql.NullInt64"},
				{name: "StoredJobs", type: "sql.NullInt64"},
				{name: "CleanedJobs", type: "sql.NullInt64"},
				{name: "QueryResultTtlSeconds", type: "sql.NullInt64"},
			]
		}
		raw: {
			sql: """
				-- name: GetComputeEndpoint :one
				SELECT id, external_id, name, url, type, status, size, max_memory_gb, auth_token, owner, created_at, updated_at, selection_policy, workload_class, readiness_status, max_concurrency, max_result_size_mb, recommended_for_large_queries, is_draining, last_health_status, last_health_checked_at, active_queries, queued_jobs, running_jobs, completed_jobs, stored_jobs, cleaned_jobs, query_result_ttl_seconds FROM compute_endpoints WHERE id = ?
				"""
			bind: ["id"]
		}
	},
	{
		name: "GetComputeEndpointByName"
		kind: "one"
		paramMode: "single"
		params: [
			{name: "name", type: "string"},
		]
		result: {
			row: "ComputeEndpoint"
			fields: [
				{name: "ID", type: "string"},
				{name: "ExternalID", type: "string"},
				{name: "Name", type: "string"},
				{name: "Url", type: "string"},
				{name: "Type", type: "string"},
				{name: "Status", type: "string"},
				{name: "Size", type: "string"},
				{name: "MaxMemoryGb", type: "sql.NullInt64"},
				{name: "AuthToken", type: "string"},
				{name: "Owner", type: "string"},
				{name: "CreatedAt", type: "time.Time"},
				{name: "UpdatedAt", type: "time.Time"},
				{name: "SelectionPolicy", type: "string"},
				{name: "WorkloadClass", type: "string"},
				{name: "ReadinessStatus", type: "string"},
				{name: "MaxConcurrency", type: "sql.NullInt64"},
				{name: "MaxResultSizeMb", type: "sql.NullInt64"},
				{name: "RecommendedForLargeQueries", type: "int64"},
				{name: "IsDraining", type: "int64"},
				{name: "LastHealthStatus", type: "sql.NullString"},
				{name: "LastHealthCheckedAt", type: "sql.NullTime"},
				{name: "ActiveQueries", type: "sql.NullInt64"},
				{name: "QueuedJobs", type: "sql.NullInt64"},
				{name: "RunningJobs", type: "sql.NullInt64"},
				{name: "CompletedJobs", type: "sql.NullInt64"},
				{name: "StoredJobs", type: "sql.NullInt64"},
				{name: "CleanedJobs", type: "sql.NullInt64"},
				{name: "QueryResultTtlSeconds", type: "sql.NullInt64"},
			]
		}
		raw: {
			sql: """
				-- name: GetComputeEndpointByName :one
				SELECT id, external_id, name, url, type, status, size, max_memory_gb, auth_token, owner, created_at, updated_at, selection_policy, workload_class, readiness_status, max_concurrency, max_result_size_mb, recommended_for_large_queries, is_draining, last_health_status, last_health_checked_at, active_queries, queued_jobs, running_jobs, completed_jobs, stored_jobs, cleaned_jobs, query_result_ttl_seconds FROM compute_endpoints WHERE name = ?
				"""
			bind: ["name"]
		}
	},
	{
		name: "GetDashboard"
		kind: "one"
		paramMode: "single"
		params: [
			{name: "id", type: "string"},
		]
		result: {
			row: "Dashboard"
			fields: [
				{name: "ID", type: "string"},
				{name: "Name", type: "string"},
				{name: "Description", type: "string"},
				{name: "Owner", type: "string"},
				{name: "CreatedAt", type: "string"},
				{name: "UpdatedAt", type: "string"},
				{name: "FolderID", type: "sql.NullString"},
				{name: "SemanticProjectName", type: "string"},
				{name: "SemanticModelName", type: "string"},
				{name: "ComputeMode", type: "string"},
				{name: "ComputeEndpointName", type: "string"},
				{name: "ComputeFallbackLocal", type: "int64"},
			]
		}
		raw: {
			sql: """
				-- name: GetDashboard :one
				SELECT id, name, description, owner, created_at, updated_at, folder_id, semantic_project_name, semantic_model_name, compute_mode, compute_endpoint_name, compute_fallback_local FROM dashboards WHERE id = ?
				"""
			bind: ["id"]
		}
	},
	{
		name: "GetDashboardWidget"
		kind: "one"
		paramMode: "single"
		params: [
			{name: "id", type: "string"},
		]
		result: {
			row: "DashboardWidget"
			fields: [
				{name: "ID", type: "string"},
				{name: "DashboardID", type: "string"},
				{name: "Name", type: "string"},
				{name: "Description", type: "string"},
				{name: "SourceJson", type: "string"},
				{name: "VisualSpec", type: "string"},
				{name: "LayoutX", type: "int64"},
				{name: "LayoutY", type: "int64"},
				{name: "LayoutW", type: "int64"},
				{name: "LayoutH", type: "int64"},
				{name: "CreatedAt", type: "string"},
				{name: "UpdatedAt", type: "string"},
				{name: "FilterOriginKey", type: "string"},
				{name: "PageName", type: "string"},
			]
		}
		raw: {
			sql: """
				-- name: GetDashboardWidget :one
				SELECT id, dashboard_id, name, description, source_json, visual_spec, layout_x, layout_y, layout_w, layout_h, created_at, updated_at, filter_origin_key, page_name FROM dashboard_widgets WHERE id = ?
				"""
			bind: ["id"]
		}
	},
	{
		name: "GetDefaultCatalog"
		kind: "one"
		result: {
			row: "Catalog"
			fields: [
				{name: "ID", type: "string"},
				{name: "Name", type: "string"},
				{name: "MetastoreType", type: "string"},
				{name: "Dsn", type: "string"},
				{name: "DataPath", type: "string"},
				{name: "Status", type: "string"},
				{name: "StatusMessage", type: "sql.NullString"},
				{name: "IsDefault", type: "int64"},
				{name: "Comment", type: "sql.NullString"},
				{name: "CreatedAt", type: "string"},
				{name: "UpdatedAt", type: "string"},
			]
		}
		raw: {
			sql: """
				-- name: GetDefaultCatalog :one
				SELECT id, name, metastore_type, dsn, data_path, status, status_message, is_default, comment, created_at, updated_at FROM catalogs WHERE is_default = 1
				"""
		}
	},
	{
		name: "GetDefaultEndpointForPrincipal"
		kind: "one"
		paramMode: "struct"
		params: [
			{name: "PrincipalID", type: "string"},
			{name: "PrincipalType", type: "string"},
		]
		result: {
			row: "ComputeEndpoint"
			fields: [
				{name: "ID", type: "string"},
				{name: "ExternalID", type: "string"},
				{name: "Name", type: "string"},
				{name: "Url", type: "string"},
				{name: "Type", type: "string"},
				{name: "Status", type: "string"},
				{name: "Size", type: "string"},
				{name: "MaxMemoryGb", type: "sql.NullInt64"},
				{name: "AuthToken", type: "string"},
				{name: "Owner", type: "string"},
				{name: "CreatedAt", type: "time.Time"},
				{name: "UpdatedAt", type: "time.Time"},
				{name: "SelectionPolicy", type: "string"},
				{name: "WorkloadClass", type: "string"},
				{name: "ReadinessStatus", type: "string"},
				{name: "MaxConcurrency", type: "sql.NullInt64"},
				{name: "MaxResultSizeMb", type: "sql.NullInt64"},
				{name: "RecommendedForLargeQueries", type: "int64"},
				{name: "IsDraining", type: "int64"},
				{name: "LastHealthStatus", type: "sql.NullString"},
				{name: "LastHealthCheckedAt", type: "sql.NullTime"},
				{name: "ActiveQueries", type: "sql.NullInt64"},
				{name: "QueuedJobs", type: "sql.NullInt64"},
				{name: "RunningJobs", type: "sql.NullInt64"},
				{name: "CompletedJobs", type: "sql.NullInt64"},
				{name: "StoredJobs", type: "sql.NullInt64"},
				{name: "CleanedJobs", type: "sql.NullInt64"},
				{name: "QueryResultTtlSeconds", type: "sql.NullInt64"},
			]
		}
		raw: {
			sql: """
				-- name: GetDefaultEndpointForPrincipal :one
				SELECT ce.id, ce.external_id, ce.name, ce.url, ce.type, ce.status, ce.size, ce.max_memory_gb, ce.auth_token, ce.owner, ce.created_at, ce.updated_at, ce.selection_policy, ce.workload_class, ce.readiness_status, ce.max_concurrency, ce.max_result_size_mb, ce.recommended_for_large_queries, ce.is_draining, ce.last_health_status, ce.last_health_checked_at, ce.active_queries, ce.queued_jobs, ce.running_jobs, ce.completed_jobs, ce.stored_jobs, ce.cleaned_jobs, ce.query_result_ttl_seconds
				FROM compute_endpoints ce
				JOIN compute_assignments ca ON ca.endpoint_id = ce.id
				WHERE ca.principal_id = ?
				  AND ca.principal_type = ?
				  AND ca.is_default = 1
				  AND ce.status = 'ACTIVE'
				LIMIT 1
				"""
			bind: ["PrincipalID", "PrincipalType"]
		}
	},
	{
		name: "GetDownstreamLineage"
		kind: "many"
		paramMode: "struct"
		params: [
			{name: "SourceTable", type: "string"},
			{name: "Limit", type: "int64"},
			{name: "Offset", type: "int64"},
		]
		result: {
			row: "GetDownstreamLineageRow"
			fields: [
				{name: "SourceTable", type: "string"},
				{name: "TargetTable", type: "sql.NullString"},
				{name: "EdgeType", type: "string"},
				{name: "PrincipalName", type: "string"},
				{name: "CreatedAt", type: "string"},
				{name: "SourceSchema", type: "sql.NullString"},
				{name: "TargetSchema", type: "sql.NullString"},
			]
		}
		raw: {
			sql: """
				-- name: GetDownstreamLineage :many
				SELECT DISTINCT source_table, target_table, edge_type, principal_name, created_at, source_schema, target_schema
				FROM lineage_edges
				WHERE source_table = ?
				ORDER BY created_at DESC
				LIMIT ? OFFSET ?
				"""
			bind: ["SourceTable", "Limit", "Offset"]
		}
	},
	{
		name: "GetExternalLocation"
		kind: "one"
		paramMode: "single"
		params: [
			{name: "id", type: "string"},
		]
		result: {
			row: "ExternalLocation"
			fields: [
				{name: "ID", type: "string"},
				{name: "Name", type: "string"},
				{name: "Url", type: "string"},
				{name: "CredentialName", type: "string"},
				{name: "StorageType", type: "string"},
				{name: "Comment", type: "string"},
				{name: "Owner", type: "string"},
				{name: "ReadOnly", type: "int64"},
				{name: "CreatedAt", type: "string"},
				{name: "UpdatedAt", type: "string"},
			]
		}
		raw: {
			sql: """
				-- name: GetExternalLocation :one
				SELECT id, name, url, credential_name, storage_type, comment, owner, read_only, created_at, updated_at FROM external_locations WHERE id = ?
				"""
			bind: ["id"]
		}
	},
	{
		name: "GetExternalLocationByName"
		kind: "one"
		paramMode: "single"
		params: [
			{name: "name", type: "string"},
		]
		result: {
			row: "ExternalLocation"
			fields: [
				{name: "ID", type: "string"},
				{name: "Name", type: "string"},
				{name: "Url", type: "string"},
				{name: "CredentialName", type: "string"},
				{name: "StorageType", type: "string"},
				{name: "Comment", type: "string"},
				{name: "Owner", type: "string"},
				{name: "ReadOnly", type: "int64"},
				{name: "CreatedAt", type: "string"},
				{name: "UpdatedAt", type: "string"},
			]
		}
		raw: {
			sql: """
				-- name: GetExternalLocationByName :one
				SELECT id, name, url, credential_name, storage_type, comment, owner, read_only, created_at, updated_at FROM external_locations WHERE name = ?
				"""
			bind: ["name"]
		}
	},
	{
		name: "GetExternalTableByID"
		kind: "one"
		paramMode: "single"
		params: [
			{name: "id", type: "string"},
		]
		result: {
			row: "ExternalTable"
			fields: [
				{name: "ID", type: "string"},
				{name: "SchemaName", type: "string"},
				{name: "TableName", type: "string"},
				{name: "FileFormat", type: "string"},
				{name: "SourcePath", type: "string"},
				{name: "LocationName", type: "string"},
				{name: "Comment", type: "string"},
				{name: "Owner", type: "string"},
				{name: "CreatedAt", type: "string"},
				{name: "UpdatedAt", type: "string"},
				{name: "DeletedAt", type: "sql.NullString"},
				{name: "CatalogName", type: "string"},
			]
		}
		raw: {
			sql: """
				-- name: GetExternalTableByID :one
				SELECT id, schema_name, table_name, file_format, source_path, location_name, comment, owner, created_at, updated_at, deleted_at, catalog_name FROM external_tables
				WHERE id = ? AND deleted_at IS NULL
				"""
			bind: ["id"]
		}
	},
	{
		name: "GetExternalTableByName"
		kind: "one"
		paramMode: "struct"
		params: [
			{name: "SchemaName", type: "string"},
			{name: "TableName", type: "string"},
		]
		result: {
			row: "ExternalTable"
			fields: [
				{name: "ID", type: "string"},
				{name: "SchemaName", type: "string"},
				{name: "TableName", type: "string"},
				{name: "FileFormat", type: "string"},
				{name: "SourcePath", type: "string"},
				{name: "LocationName", type: "string"},
				{name: "Comment", type: "string"},
				{name: "Owner", type: "string"},
				{name: "CreatedAt", type: "string"},
				{name: "UpdatedAt", type: "string"},
				{name: "DeletedAt", type: "sql.NullString"},
				{name: "CatalogName", type: "string"},
			]
		}
		raw: {
			sql: """
				-- name: GetExternalTableByName :one
				SELECT id, schema_name, table_name, file_format, source_path, location_name, comment, owner, created_at, updated_at, deleted_at, catalog_name FROM external_tables
				WHERE schema_name = ? AND table_name = ? AND deleted_at IS NULL
				"""
			bind: ["SchemaName", "TableName"]
		}
	},
	{
		name: "GetExternalTableByTableName"
		kind: "one"
		paramMode: "single"
		params: [
			{name: "tableName", type: "string"},
		]
		result: {
			row: "ExternalTable"
			fields: [
				{name: "ID", type: "string"},
				{name: "SchemaName", type: "string"},
				{name: "TableName", type: "string"},
				{name: "FileFormat", type: "string"},
				{name: "SourcePath", type: "string"},
				{name: "LocationName", type: "string"},
				{name: "Comment", type: "string"},
				{name: "Owner", type: "string"},
				{name: "CreatedAt", type: "string"},
				{name: "UpdatedAt", type: "string"},
				{name: "DeletedAt", type: "sql.NullString"},
				{name: "CatalogName", type: "string"},
			]
		}
		raw: {
			sql: """
				-- name: GetExternalTableByTableName :one
				SELECT id, schema_name, table_name, file_format, source_path, location_name, comment, owner, created_at, updated_at, deleted_at, catalog_name FROM external_tables
				WHERE table_name = ? AND deleted_at IS NULL
				"""
			bind: ["tableName"]
		}
	},
	{
		name: "GetGitRepo"
		kind: "one"
		paramMode: "single"
		params: [
			{name: "id", type: "string"},
		]
		result: {
			row: "GitRepo"
			fields: [
				{name: "ID", type: "string"},
				{name: "Url", type: "string"},
				{name: "Branch", type: "string"},
				{name: "Path", type: "string"},
				{name: "AuthToken", type: "string"},
				{name: "WebhookSecret", type: "sql.NullString"},
				{name: "Owner", type: "string"},
				{name: "LastSyncAt", type: "sql.NullString"},
				{name: "LastCommit", type: "sql.NullString"},
				{name: "CreatedAt", type: "string"},
				{name: "UpdatedAt", type: "string"},
			]
		}
		raw: {
			sql: """
				-- name: GetGitRepo :one
				SELECT id, url, branch, path, auth_token, webhook_secret, owner, last_sync_at, last_commit, created_at, updated_at FROM git_repos WHERE id = ?
				"""
			bind: ["id"]
		}
	},
	{
		name: "GetGroup"
		kind: "one"
		paramMode: "single"
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
		raw: {
			sql: """
				-- name: GetGroup :one
				SELECT id, name, description, created_at FROM groups WHERE id = ?
				"""
			bind: ["id"]
		}
	},
	{
		name: "GetGroupByName"
		kind: "one"
		paramMode: "single"
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
		raw: {
			sql: """
				-- name: GetGroupByName :one
				SELECT id, name, description, created_at FROM groups WHERE name = ?
				"""
			bind: ["name"]
		}
	},
	{
		name: "GetGroupsForMember"
		kind: "many"
		paramMode: "struct"
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
		raw: {
			sql: """
				-- name: GetGroupsForMember :many
				SELECT g.id, g.name, g.description, g.created_at FROM groups g
				JOIN group_members gm ON g.id = gm.group_id
				WHERE gm.member_type = ? AND gm.member_id = ?
				"""
			bind: ["MemberType", "MemberID"]
		}
	},
	{
		name: "GetLatestMacroRevisionVersion"
		kind: "one"
		paramMode: "single"
		params: [
			{name: "macroID", type: "string"},
		]
		result: {
			scalar: "int64"
		}
		raw: {
			sql: """
				-- name: GetLatestMacroRevisionVersion :one
				SELECT CAST(COALESCE(MAX(version), 0) AS INTEGER) FROM macro_revisions WHERE macro_id = ?
				"""
			bind: ["macroID"]
		}
	},
	{
		name: "GetLocalCredentialByPrincipalID"
		kind: "one"
		paramMode: "single"
		params: [
			{name: "principalID", type: "string"},
		]
		result: {
			row: "LocalCredential"
			fields: [
				{name: "PrincipalID", type: "string"},
				{name: "Username", type: "string"},
				{name: "PasswordHash", type: "string"},
				{name: "PasswordChangedAt", type: "time.Time"},
				{name: "MustChangePassword", type: "int64"},
				{name: "CreatedAt", type: "time.Time"},
				{name: "UpdatedAt", type: "time.Time"},
			]
		}
		raw: {
			sql: """
				-- name: GetLocalCredentialByPrincipalID :one
				SELECT principal_id, username, password_hash, password_changed_at, must_change_password, created_at, updated_at
				FROM local_credentials
				WHERE principal_id = ?
				LIMIT 1
				"""
			bind: ["principalID"]
		}
	},
	{
		name: "GetLocalCredentialByUsername"
		kind: "one"
		paramMode: "single"
		params: [
			{name: "username", type: "string"},
		]
		result: {
			row: "LocalCredential"
			fields: [
				{name: "PrincipalID", type: "string"},
				{name: "Username", type: "string"},
				{name: "PasswordHash", type: "string"},
				{name: "PasswordChangedAt", type: "time.Time"},
				{name: "MustChangePassword", type: "int64"},
				{name: "CreatedAt", type: "time.Time"},
				{name: "UpdatedAt", type: "time.Time"},
			]
		}
		raw: {
			sql: """
				-- name: GetLocalCredentialByUsername :one
				SELECT principal_id, username, password_hash, password_changed_at, must_change_password, created_at, updated_at
				FROM local_credentials
				WHERE username = ?
				LIMIT 1
				"""
			bind: ["username"]
		}
	},
	{
		name: "GetMacroByName"
		kind: "one"
		paramMode: "single"
		params: [
			{name: "name", type: "string"},
		]
		result: {
			row: "Macro"
			fields: [
				{name: "ID", type: "string"},
				{name: "Name", type: "string"},
				{name: "MacroType", type: "string"},
				{name: "Parameters", type: "string"},
				{name: "Body", type: "string"},
				{name: "Description", type: "string"},
				{name: "CreatedBy", type: "string"},
				{name: "CreatedAt", type: "string"},
				{name: "UpdatedAt", type: "string"},
				{name: "CatalogName", type: "string"},
				{name: "ProjectName", type: "string"},
				{name: "Visibility", type: "string"},
				{name: "Owner", type: "string"},
				{name: "Properties", type: "string"},
				{name: "Tags", type: "string"},
				{name: "Status", type: "string"},
			]
		}
		raw: {
			sql: """
				-- name: GetMacroByName :one
				SELECT id, name, macro_type, parameters, body, description, created_by, created_at, updated_at, catalog_name, project_name, visibility, owner, properties, tags, status FROM macros WHERE name = ?
				"""
			bind: ["name"]
		}
	},
	{
		name: "GetMacroRevisionByVersion"
		kind: "one"
		paramMode: "struct"
		params: [
			{name: "MacroName", type: "string"},
			{name: "Version", type: "int64"},
		]
		result: {
			row: "MacroRevision"
			fields: [
				{name: "ID", type: "string"},
				{name: "MacroID", type: "string"},
				{name: "MacroName", type: "string"},
				{name: "Version", type: "int64"},
				{name: "ContentHash", type: "string"},
				{name: "Parameters", type: "string"},
				{name: "Body", type: "string"},
				{name: "Description", type: "string"},
				{name: "Status", type: "string"},
				{name: "CreatedBy", type: "string"},
				{name: "CreatedAt", type: "string"},
			]
		}
		raw: {
			sql: """
				-- name: GetMacroRevisionByVersion :one
				SELECT id, macro_id, macro_name, version, content_hash, parameters, body, description, status, created_by, created_at FROM macro_revisions WHERE macro_name = ? AND version = ?
				"""
			bind: ["MacroName", "Version"]
		}
	},
	{
		name: "GetMaxCellPosition"
		kind: "one"
		paramMode: "single"
		params: [
			{name: "notebookID", type: "string"},
		]
		result: {
			scalar: "interface{}"
		}
		raw: {
			sql: """
				-- name: GetMaxCellPosition :one
				SELECT COALESCE(MAX(position), -1) FROM cells WHERE notebook_id = ?
				"""
			bind: ["notebookID"]
		}
	},
	{
		name: "GetModelByID"
		kind: "one"
		paramMode: "single"
		params: [
			{name: "id", type: "string"},
		]
		result: {
			row: "Model"
			fields: [
				{name: "ID", type: "string"},
				{name: "ProjectName", type: "string"},
				{name: "Name", type: "string"},
				{name: "SqlBody", type: "string"},
				{name: "Materialization", type: "string"},
				{name: "Description", type: "string"},
				{name: "Owner", type: "string"},
				{name: "Tags", type: "string"},
				{name: "DependsOn", type: "string"},
				{name: "Config", type: "string"},
				{name: "CreatedBy", type: "string"},
				{name: "CreatedAt", type: "string"},
				{name: "UpdatedAt", type: "string"},
				{name: "Contract", type: "string"},
				{name: "FreshnessMaxLag", type: "sql.NullInt64"},
				{name: "FreshnessCron", type: "sql.NullString"},
			]
		}
		raw: {
			sql: """
				-- name: GetModelByID :one
				SELECT id, project_name, name, sql_body, materialization, description, owner, tags, depends_on, config, created_by, created_at, updated_at, contract, freshness_max_lag, freshness_cron FROM models WHERE id = ?
				"""
			bind: ["id"]
		}
	},
	{
		name: "GetModelByName"
		kind: "one"
		paramMode: "struct"
		params: [
			{name: "ProjectName", type: "string"},
			{name: "Name", type: "string"},
		]
		result: {
			row: "Model"
			fields: [
				{name: "ID", type: "string"},
				{name: "ProjectName", type: "string"},
				{name: "Name", type: "string"},
				{name: "SqlBody", type: "string"},
				{name: "Materialization", type: "string"},
				{name: "Description", type: "string"},
				{name: "Owner", type: "string"},
				{name: "Tags", type: "string"},
				{name: "DependsOn", type: "string"},
				{name: "Config", type: "string"},
				{name: "CreatedBy", type: "string"},
				{name: "CreatedAt", type: "string"},
				{name: "UpdatedAt", type: "string"},
				{name: "Contract", type: "string"},
				{name: "FreshnessMaxLag", type: "sql.NullInt64"},
				{name: "FreshnessCron", type: "sql.NullString"},
			]
		}
		raw: {
			sql: """
				-- name: GetModelByName :one
				SELECT id, project_name, name, sql_body, materialization, description, owner, tags, depends_on, config, created_by, created_at, updated_at, contract, freshness_max_lag, freshness_cron FROM models WHERE project_name = ? AND name = ?
				"""
			bind: ["ProjectName", "Name"]
		}
	},
	{
		name: "GetModelRunByID"
		kind: "one"
		paramMode: "single"
		params: [
			{name: "id", type: "string"},
		]
		result: {
			row: "ModelRun"
			fields: [
				{name: "ID", type: "string"},
				{name: "Status", type: "string"},
				{name: "TriggerType", type: "string"},
				{name: "TriggeredBy", type: "string"},
				{name: "TargetCatalog", type: "string"},
				{name: "TargetSchema", type: "string"},
				{name: "ModelSelector", type: "string"},
				{name: "Variables", type: "string"},
				{name: "StartedAt", type: "sql.NullString"},
				{name: "FinishedAt", type: "sql.NullString"},
				{name: "ErrorMessage", type: "sql.NullString"},
				{name: "CreatedAt", type: "string"},
				{name: "FullRefresh", type: "int64"},
				{name: "CompileManifest", type: "string"},
				{name: "CompileDiagnostics", type: "string"},
				{name: "ProjectName", type: "string"},
				{name: "EnvironmentName", type: "string"},
				{name: "BuildID", type: "sql.NullString"},
			]
		}
		raw: {
			sql: """
				-- name: GetModelRunByID :one
				SELECT id, status, trigger_type, triggered_by, target_catalog, target_schema, model_selector, variables, started_at, finished_at, error_message, created_at, full_refresh, compile_manifest, compile_diagnostics, project_name, environment_name, build_id FROM model_runs WHERE id = ?
				"""
			bind: ["id"]
		}
	},
	{
		name: "GetModelTestByID"
		kind: "one"
		paramMode: "single"
		params: [
			{name: "id", type: "string"},
		]
		result: {
			row: "ModelTest"
			fields: [
				{name: "ID", type: "string"},
				{name: "ModelID", type: "string"},
				{name: "Name", type: "string"},
				{name: "TestType", type: "string"},
				{name: "ColumnName", type: "string"},
				{name: "Config", type: "string"},
				{name: "CreatedAt", type: "string"},
			]
		}
		raw: {
			sql: """
				-- name: GetModelTestByID :one
				SELECT id, model_id, name, test_type, column_name, config, created_at FROM model_tests WHERE id = ?
				"""
			bind: ["id"]
		}
	},
	{
		name: "GetNotebook"
		kind: "one"
		paramMode: "single"
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
		raw: {
			sql: """
				-- name: GetNotebook :one
				SELECT id, name, description, owner, created_at, updated_at, git_repo_id, git_path, folder_id, project_override_id, environment_override_id FROM notebooks WHERE id = ?
				"""
			bind: ["id"]
		}
	},
	{
		name: "GetNotebookJob"
		kind: "one"
		paramMode: "single"
		params: [
			{name: "id", type: "string"},
		]
		result: {
			row: "NotebookJob"
			fields: [
				{name: "ID", type: "string"},
				{name: "NotebookID", type: "string"},
				{name: "SessionID", type: "string"},
				{name: "State", type: "string"},
				{name: "Result", type: "sql.NullString"},
				{name: "Error", type: "sql.NullString"},
				{name: "CreatedAt", type: "string"},
				{name: "UpdatedAt", type: "string"},
			]
		}
		raw: {
			sql: """
				-- name: GetNotebookJob :one
				SELECT id, notebook_id, session_id, state, result, error, created_at, updated_at FROM notebook_jobs WHERE id = ?
				"""
			bind: ["id"]
		}
	},
	{
		name: "GetNotebookModelLinkByModelID"
		kind: "one"
		paramMode: "single"
		params: [
			{name: "modelID", type: "string"},
		]
		result: {
			row: "NotebookModelLink"
			fields: [
				{name: "ID", type: "string"},
				{name: "NotebookID", type: "string"},
				{name: "ModelID", type: "string"},
				{name: "OutputCellID", type: "string"},
				{name: "CreatedAt", type: "string"},
				{name: "UpdatedAt", type: "string"},
			]
		}
		raw: {
			sql: """
				-- name: GetNotebookModelLinkByModelID :one
				SELECT id, notebook_id, model_id, output_cell_id, created_at, updated_at FROM notebook_model_links WHERE model_id = ?
				"""
			bind: ["modelID"]
		}
	},
	{
		name: "GetNotebookModelLinkByNotebookID"
		kind: "one"
		paramMode: "single"
		params: [
			{name: "notebookID", type: "string"},
		]
		result: {
			row: "NotebookModelLink"
			fields: [
				{name: "ID", type: "string"},
				{name: "NotebookID", type: "string"},
				{name: "ModelID", type: "string"},
				{name: "OutputCellID", type: "string"},
				{name: "CreatedAt", type: "string"},
				{name: "UpdatedAt", type: "string"},
			]
		}
		raw: {
			sql: """
				-- name: GetNotebookModelLinkByNotebookID :one
				SELECT id, notebook_id, model_id, output_cell_id, created_at, updated_at FROM notebook_model_links WHERE notebook_id = ?
				"""
			bind: ["notebookID"]
		}
	},
	{
		name: "GetPipelineByID"
		kind: "one"
		paramMode: "single"
		params: [
			{name: "id", type: "string"},
		]
		result: {
			row: "Pipeline"
			fields: [
				{name: "ID", type: "string"},
				{name: "Name", type: "string"},
				{name: "Description", type: "string"},
				{name: "ScheduleCron", type: "sql.NullString"},
				{name: "IsPaused", type: "int64"},
				{name: "ConcurrencyLimit", type: "int64"},
				{name: "CreatedBy", type: "string"},
				{name: "CreatedAt", type: "string"},
				{name: "UpdatedAt", type: "string"},
				{name: "FolderID", type: "sql.NullString"},
			]
		}
		raw: {
			sql: """
				-- name: GetPipelineByID :one
				SELECT id, name, description, schedule_cron, is_paused, concurrency_limit, created_by, created_at, updated_at, folder_id FROM pipelines WHERE id = ?
				"""
			bind: ["id"]
		}
	},
	{
		name: "GetPipelineByName"
		kind: "one"
		paramMode: "single"
		params: [
			{name: "name", type: "string"},
		]
		result: {
			row: "Pipeline"
			fields: [
				{name: "ID", type: "string"},
				{name: "Name", type: "string"},
				{name: "Description", type: "string"},
				{name: "ScheduleCron", type: "sql.NullString"},
				{name: "IsPaused", type: "int64"},
				{name: "ConcurrencyLimit", type: "int64"},
				{name: "CreatedBy", type: "string"},
				{name: "CreatedAt", type: "string"},
				{name: "UpdatedAt", type: "string"},
				{name: "FolderID", type: "sql.NullString"},
			]
		}
		raw: {
			sql: """
				-- name: GetPipelineByName :one
				SELECT id, name, description, schedule_cron, is_paused, concurrency_limit, created_by, created_at, updated_at, folder_id FROM pipelines WHERE name = ?
				"""
			bind: ["name"]
		}
	},
	{
		name: "GetPipelineJobByID"
		kind: "one"
		paramMode: "single"
		params: [
			{name: "id", type: "string"},
		]
		result: {
			row: "PipelineJob"
			fields: [
				{name: "ID", type: "string"},
				{name: "PipelineID", type: "string"},
				{name: "Name", type: "string"},
				{name: "ComputeEndpointID", type: "sql.NullString"},
				{name: "DependsOn", type: "string"},
				{name: "NotebookID", type: "string"},
				{name: "TimeoutSeconds", type: "sql.NullInt64"},
				{name: "RetryCount", type: "int64"},
				{name: "JobOrder", type: "int64"},
				{name: "CreatedAt", type: "string"},
				{name: "JobType", type: "string"},
				{name: "ModelSelector", type: "string"},
			]
		}
		raw: {
			sql: """
				-- name: GetPipelineJobByID :one
				SELECT id, pipeline_id, name, compute_endpoint_id, depends_on, notebook_id, timeout_seconds, retry_count, job_order, created_at, job_type, model_selector FROM pipeline_jobs WHERE id = ?
				"""
			bind: ["id"]
		}
	},
	{
		name: "GetPipelineJobRunByID"
		kind: "one"
		paramMode: "single"
		params: [
			{name: "id", type: "string"},
		]
		result: {
			row: "PipelineJobRun"
			fields: [
				{name: "ID", type: "string"},
				{name: "RunID", type: "string"},
				{name: "JobID", type: "string"},
				{name: "JobName", type: "string"},
				{name: "Status", type: "string"},
				{name: "StartedAt", type: "sql.NullString"},
				{name: "FinishedAt", type: "sql.NullString"},
				{name: "ErrorMessage", type: "sql.NullString"},
				{name: "RetryAttempt", type: "int64"},
				{name: "CreatedAt", type: "string"},
			]
		}
		raw: {
			sql: """
				-- name: GetPipelineJobRunByID :one
				SELECT id, run_id, job_id, job_name, status, started_at, finished_at, error_message, retry_attempt, created_at FROM pipeline_job_runs WHERE id = ?
				"""
			bind: ["id"]
		}
	},
	{
		name: "GetPipelineRunByID"
		kind: "one"
		paramMode: "single"
		params: [
			{name: "id", type: "string"},
		]
		result: {
			row: "PipelineRun"
			fields: [
				{name: "ID", type: "string"},
				{name: "PipelineID", type: "string"},
				{name: "Status", type: "string"},
				{name: "TriggerType", type: "string"},
				{name: "TriggeredBy", type: "string"},
				{name: "Parameters", type: "string"},
				{name: "GitCommitHash", type: "sql.NullString"},
				{name: "StartedAt", type: "sql.NullString"},
				{name: "FinishedAt", type: "sql.NullString"},
				{name: "ErrorMessage", type: "sql.NullString"},
				{name: "CreatedAt", type: "string"},
			]
		}
		raw: {
			sql: """
				-- name: GetPipelineRunByID :one
				SELECT id, pipeline_id, status, trigger_type, triggered_by, parameters, git_commit_hash, started_at, finished_at, error_message, created_at FROM pipeline_runs WHERE id = ?
				"""
			bind: ["id"]
		}
	},
	{
		name: "GetPrincipal"
		kind: "one"
		paramMode: "single"
		params: [
			{name: "id", type: "string"},
		]
		result: {
			row: "Principal"
			fields: [
				{name: "ID", type: "string"},
				{name: "Name", type: "string"},
				{name: "Type", type: "string"},
				{name: "IsAdmin", type: "int64"},
				{name: "CreatedAt", type: "string"},
				{name: "ExternalID", type: "sql.NullString"},
				{name: "ExternalIssuer", type: "sql.NullString"},
			]
		}
		raw: {
			sql: """
				-- name: GetPrincipal :one
				SELECT id, name, type, is_admin, created_at, external_id, external_issuer FROM principals WHERE id = ?
				"""
			bind: ["id"]
		}
	},
	{
		name: "GetPrincipalByExternalID"
		kind: "one"
		paramMode: "struct"
		params: [
			{name: "ExternalIssuer", type: "sql.NullString"},
			{name: "ExternalID", type: "sql.NullString"},
		]
		result: {
			row: "Principal"
			fields: [
				{name: "ID", type: "string"},
				{name: "Name", type: "string"},
				{name: "Type", type: "string"},
				{name: "IsAdmin", type: "int64"},
				{name: "CreatedAt", type: "string"},
				{name: "ExternalID", type: "sql.NullString"},
				{name: "ExternalIssuer", type: "sql.NullString"},
			]
		}
		raw: {
			sql: """
				-- name: GetPrincipalByExternalID :one
				SELECT id, name, type, is_admin, created_at, external_id, external_issuer FROM principals
				WHERE external_issuer IS ? AND external_id = ?
				LIMIT 1
				"""
			bind: ["ExternalIssuer", "ExternalID"]
		}
	},
	{
		name: "GetPrincipalByName"
		kind: "one"
		paramMode: "single"
		params: [
			{name: "name", type: "string"},
		]
		result: {
			row: "Principal"
			fields: [
				{name: "ID", type: "string"},
				{name: "Name", type: "string"},
				{name: "Type", type: "string"},
				{name: "IsAdmin", type: "int64"},
				{name: "CreatedAt", type: "string"},
				{name: "ExternalID", type: "sql.NullString"},
				{name: "ExternalIssuer", type: "sql.NullString"},
			]
		}
		raw: {
			sql: """
				-- name: GetPrincipalByName :one
				SELECT id, name, type, is_admin, created_at, external_id, external_issuer FROM principals WHERE name = ?
				"""
			bind: ["name"]
		}
	},
	{
		name: "GetRowFilterBindingsForFilter"
		kind: "many"
		paramMode: "single"
		params: [
			{name: "rowFilterID", type: "string"},
		]
		result: {
			row: "RowFilterBinding"
			fields: [
				{name: "ID", type: "string"},
				{name: "RowFilterID", type: "string"},
				{name: "PrincipalID", type: "string"},
				{name: "PrincipalType", type: "string"},
			]
		}
		raw: {
			sql: """
				-- name: GetRowFilterBindingsForFilter :many
				SELECT id, row_filter_id, principal_id, principal_type FROM row_filter_bindings WHERE row_filter_id = ?
				"""
			bind: ["rowFilterID"]
		}
	},
	{
		name: "GetRowFiltersForTable"
		kind: "many"
		paramMode: "single"
		params: [
			{name: "tableID", type: "string"},
		]
		result: {
			row: "RowFilter"
			fields: [
				{name: "ID", type: "string"},
				{name: "TableID", type: "string"},
				{name: "FilterSql", type: "string"},
				{name: "Description", type: "sql.NullString"},
				{name: "CreatedAt", type: "string"},
				{name: "Name", type: "sql.NullString"},
			]
		}
		raw: {
			sql: """
				-- name: GetRowFiltersForTable :many
				SELECT id, table_id, filter_sql, description, created_at, name FROM row_filters WHERE table_id = ?
				"""
			bind: ["tableID"]
		}
	},
	{
		name: "GetRowFiltersForTableAndPrincipal"
		kind: "many"
		paramMode: "struct"
		params: [
			{name: "TableID", type: "string"},
			{name: "PrincipalID", type: "string"},
			{name: "PrincipalType", type: "string"},
		]
		result: {
			row: "RowFilter"
			fields: [
				{name: "ID", type: "string"},
				{name: "TableID", type: "string"},
				{name: "FilterSql", type: "string"},
				{name: "Description", type: "sql.NullString"},
				{name: "CreatedAt", type: "string"},
				{name: "Name", type: "sql.NullString"},
			]
		}
		raw: {
			sql: """
				-- name: GetRowFiltersForTableAndPrincipal :many
				SELECT rf.id, rf.table_id, rf.filter_sql, rf.description, rf.created_at, rf.name FROM row_filters rf
				JOIN row_filter_bindings rfb ON rf.id = rfb.row_filter_id
				WHERE rf.table_id = ? AND rfb.principal_id = ? AND rfb.principal_type = ?
				"""
			bind: ["TableID", "PrincipalID", "PrincipalType"]
		}
	},
	{
		name: "GetSemanticMetricByID"
		kind: "one"
		paramMode: "single"
		params: [
			{name: "id", type: "string"},
		]
		result: {
			row: "SemanticMetric"
			fields: [
				{name: "ID", type: "string"},
				{name: "SemanticModelID", type: "string"},
				{name: "Name", type: "string"},
				{name: "Description", type: "string"},
				{name: "MetricType", type: "string"},
				{name: "ExpressionMode", type: "string"},
				{name: "Expression", type: "string"},
				{name: "DefaultTimeGrain", type: "string"},
				{name: "Format", type: "string"},
				{name: "Owner", type: "string"},
				{name: "CertificationState", type: "string"},
				{name: "CreatedBy", type: "string"},
				{name: "CreatedAt", type: "string"},
				{name: "UpdatedAt", type: "string"},
				{name: "Label", type: "string"},
				{name: "FilterSql", type: "string"},
				{name: "RelationshipNames", type: "string"},
			]
		}
		raw: {
			sql: """
				-- name: GetSemanticMetricByID :one
				SELECT id, semantic_model_id, name, description, metric_type, expression_mode, expression, default_time_grain, format, owner, certification_state, created_by, created_at, updated_at, label, filter_sql, relationship_names FROM semantic_metrics WHERE id = ?
				"""
			bind: ["id"]
		}
	},
	{
		name: "GetSemanticMetricByName"
		kind: "one"
		paramMode: "struct"
		params: [
			{name: "SemanticModelID", type: "string"},
			{name: "Name", type: "string"},
		]
		result: {
			row: "SemanticMetric"
			fields: [
				{name: "ID", type: "string"},
				{name: "SemanticModelID", type: "string"},
				{name: "Name", type: "string"},
				{name: "Description", type: "string"},
				{name: "MetricType", type: "string"},
				{name: "ExpressionMode", type: "string"},
				{name: "Expression", type: "string"},
				{name: "DefaultTimeGrain", type: "string"},
				{name: "Format", type: "string"},
				{name: "Owner", type: "string"},
				{name: "CertificationState", type: "string"},
				{name: "CreatedBy", type: "string"},
				{name: "CreatedAt", type: "string"},
				{name: "UpdatedAt", type: "string"},
				{name: "Label", type: "string"},
				{name: "FilterSql", type: "string"},
				{name: "RelationshipNames", type: "string"},
			]
		}
		raw: {
			sql: """
				-- name: GetSemanticMetricByName :one
				SELECT id, semantic_model_id, name, description, metric_type, expression_mode, expression, default_time_grain, format, owner, certification_state, created_by, created_at, updated_at, label, filter_sql, relationship_names FROM semantic_metrics WHERE semantic_model_id = ? AND name = ?
				"""
			bind: ["SemanticModelID", "Name"]
		}
	},
	{
		name: "GetSemanticModelByID"
		kind: "one"
		paramMode: "single"
		params: [
			{name: "id", type: "string"},
		]
		result: {
			row: "SemanticModel"
			fields: [
				{name: "ID", type: "string"},
				{name: "Name", type: "string"},
				{name: "Description", type: "string"},
				{name: "Owner", type: "string"},
				{name: "BaseModelRef", type: "string"},
				{name: "DefaultTimeDimension", type: "string"},
				{name: "Tags", type: "string"},
				{name: "CreatedBy", type: "string"},
				{name: "CreatedAt", type: "string"},
				{name: "UpdatedAt", type: "string"},
			]
		}
		raw: {
			sql: """
				-- name: GetSemanticModelByID :one
				SELECT id, name, description, owner, base_model_ref, default_time_dimension, tags, created_by, created_at, updated_at FROM semantic_models WHERE id = ?
				"""
			bind: ["id"]
		}
	},
	{
		name: "GetSemanticModelByName"
		kind: "one"
		paramMode: "single"
		params: [
			{name: "name", type: "string"},
		]
		result: {
			row: "SemanticModel"
			fields: [
				{name: "ID", type: "string"},
				{name: "Name", type: "string"},
				{name: "Description", type: "string"},
				{name: "Owner", type: "string"},
				{name: "BaseModelRef", type: "string"},
				{name: "DefaultTimeDimension", type: "string"},
				{name: "Tags", type: "string"},
				{name: "CreatedBy", type: "string"},
				{name: "CreatedAt", type: "string"},
				{name: "UpdatedAt", type: "string"},
			]
		}
		raw: {
			sql: """
				-- name: GetSemanticModelByName :one
				SELECT id, name, description, owner, base_model_ref, default_time_dimension, tags, created_by, created_at, updated_at FROM semantic_models WHERE name = ?
				"""
			bind: ["name"]
		}
	},
	{
		name: "GetSemanticPreAggregationByID"
		kind: "one"
		paramMode: "single"
		params: [
			{name: "id", type: "string"},
		]
		result: {
			row: "SemanticPreAggregation"
			fields: [
				{name: "ID", type: "string"},
				{name: "SemanticModelID", type: "string"},
				{name: "Name", type: "string"},
				{name: "MetricSet", type: "string"},
				{name: "DimensionSet", type: "string"},
				{name: "Grain", type: "string"},
				{name: "TargetRelation", type: "string"},
				{name: "RefreshPolicy", type: "string"},
				{name: "CreatedBy", type: "string"},
				{name: "CreatedAt", type: "string"},
				{name: "UpdatedAt", type: "string"},
			]
		}
		raw: {
			sql: """
				-- name: GetSemanticPreAggregationByID :one
				SELECT id, semantic_model_id, name, metric_set, dimension_set, grain, target_relation, refresh_policy, created_by, created_at, updated_at FROM semantic_pre_aggregations WHERE id = ?
				"""
			bind: ["id"]
		}
	},
	{
		name: "GetSemanticPreAggregationByName"
		kind: "one"
		paramMode: "struct"
		params: [
			{name: "SemanticModelID", type: "string"},
			{name: "Name", type: "string"},
		]
		result: {
			row: "SemanticPreAggregation"
			fields: [
				{name: "ID", type: "string"},
				{name: "SemanticModelID", type: "string"},
				{name: "Name", type: "string"},
				{name: "MetricSet", type: "string"},
				{name: "DimensionSet", type: "string"},
				{name: "Grain", type: "string"},
				{name: "TargetRelation", type: "string"},
				{name: "RefreshPolicy", type: "string"},
				{name: "CreatedBy", type: "string"},
				{name: "CreatedAt", type: "string"},
				{name: "UpdatedAt", type: "string"},
			]
		}
		raw: {
			sql: """
				-- name: GetSemanticPreAggregationByName :one
				SELECT id, semantic_model_id, name, metric_set, dimension_set, grain, target_relation, refresh_policy, created_by, created_at, updated_at FROM semantic_pre_aggregations WHERE semantic_model_id = ? AND name = ?
				"""
			bind: ["SemanticModelID", "Name"]
		}
	},
	{
		name: "GetSemanticRelationshipByID"
		kind: "one"
		paramMode: "single"
		params: [
			{name: "id", type: "string"},
		]
		result: {
			row: "SemanticRelationship"
			fields: [
				{name: "ID", type: "string"},
				{name: "Name", type: "string"},
				{name: "FromSemanticID", type: "string"},
				{name: "ToSemanticID", type: "string"},
				{name: "RelationshipType", type: "string"},
				{name: "JoinSql", type: "string"},
				{name: "Cost", type: "int64"},
				{name: "MaxHops", type: "int64"},
				{name: "CreatedBy", type: "string"},
				{name: "CreatedAt", type: "string"},
				{name: "UpdatedAt", type: "string"},
			]
		}
		raw: {
			sql: """
				-- name: GetSemanticRelationshipByID :one
				SELECT id, name, from_semantic_id, to_semantic_id, relationship_type, join_sql, cost, max_hops, created_by, created_at, updated_at FROM semantic_relationships WHERE id = ?
				"""
			bind: ["id"]
		}
	},
	{
		name: "GetSemanticRelationshipByName"
		kind: "one"
		paramMode: "struct"
		params: [
			{name: "FromSemanticID", type: "string"},
			{name: "Name", type: "string"},
		]
		result: {
			row: "SemanticRelationship"
			fields: [
				{name: "ID", type: "string"},
				{name: "Name", type: "string"},
				{name: "FromSemanticID", type: "string"},
				{name: "ToSemanticID", type: "string"},
				{name: "RelationshipType", type: "string"},
				{name: "JoinSql", type: "string"},
				{name: "Cost", type: "int64"},
				{name: "MaxHops", type: "int64"},
				{name: "CreatedBy", type: "string"},
				{name: "CreatedAt", type: "string"},
				{name: "UpdatedAt", type: "string"},
			]
		}
		raw: {
			sql: """
				-- name: GetSemanticRelationshipByName :one
				SELECT id, name, from_semantic_id, to_semantic_id, relationship_type, join_sql, cost, max_hops, created_by, created_at, updated_at FROM semantic_relationships WHERE from_semantic_id = ? AND name = ?
				"""
			bind: ["FromSemanticID", "Name"]
		}
	},
	{
		name: "GetSetupState"
		kind: "one"
		result: {
			row: "SetupState"
			fields: [
				{name: "ID", type: "int64"},
				{name: "SetupCompleted", type: "int64"},
				{name: "SetupCompletedAt", type: "sql.NullTime"},
				{name: "SetupCompletedBy", type: "sql.NullString"},
				{name: "BootstrapTokenHash", type: "sql.NullString"},
				{name: "BootstrapTokenExpiresAt", type: "sql.NullTime"},
				{name: "CreatedAt", type: "time.Time"},
				{name: "UpdatedAt", type: "time.Time"},
			]
		}
		raw: {
			sql: """
				-- name: GetSetupState :one
				SELECT id, setup_completed, setup_completed_at, setup_completed_by, bootstrap_token_hash, bootstrap_token_expires_at, created_at, updated_at FROM setup_state WHERE id = 1
				"""
		}
	},
	{
		name: "GetStorageCredential"
		kind: "one"
		paramMode: "single"
		params: [
			{name: "id", type: "string"},
		]
		result: {
			row: "StorageCredential"
			fields: [
				{name: "ID", type: "string"},
				{name: "Name", type: "string"},
				{name: "CredentialType", type: "string"},
				{name: "KeyIDEncrypted", type: "string"},
				{name: "SecretEncrypted", type: "string"},
				{name: "Endpoint", type: "string"},
				{name: "Region", type: "string"},
				{name: "UrlStyle", type: "string"},
				{name: "Comment", type: "string"},
				{name: "Owner", type: "string"},
				{name: "CreatedAt", type: "string"},
				{name: "UpdatedAt", type: "string"},
				{name: "AzureAccountName", type: "string"},
				{name: "AzureAccountKeyEncrypted", type: "string"},
				{name: "AzureClientID", type: "string"},
				{name: "AzureTenantID", type: "string"},
				{name: "AzureClientSecretEncrypted", type: "string"},
				{name: "GcsKeyFilePath", type: "string"},
			]
		}
		raw: {
			sql: """
				-- name: GetStorageCredential :one
				SELECT id, name, credential_type, key_id_encrypted, secret_encrypted, endpoint, region, url_style, comment, owner, created_at, updated_at, azure_account_name, azure_account_key_encrypted, azure_client_id, azure_tenant_id, azure_client_secret_encrypted, gcs_key_file_path FROM storage_credentials WHERE id = ?
				"""
			bind: ["id"]
		}
	},
	{
		name: "GetStorageCredentialByName"
		kind: "one"
		paramMode: "single"
		params: [
			{name: "name", type: "string"},
		]
		result: {
			row: "StorageCredential"
			fields: [
				{name: "ID", type: "string"},
				{name: "Name", type: "string"},
				{name: "CredentialType", type: "string"},
				{name: "KeyIDEncrypted", type: "string"},
				{name: "SecretEncrypted", type: "string"},
				{name: "Endpoint", type: "string"},
				{name: "Region", type: "string"},
				{name: "UrlStyle", type: "string"},
				{name: "Comment", type: "string"},
				{name: "Owner", type: "string"},
				{name: "CreatedAt", type: "string"},
				{name: "UpdatedAt", type: "string"},
				{name: "AzureAccountName", type: "string"},
				{name: "AzureAccountKeyEncrypted", type: "string"},
				{name: "AzureClientID", type: "string"},
				{name: "AzureTenantID", type: "string"},
				{name: "AzureClientSecretEncrypted", type: "string"},
				{name: "GcsKeyFilePath", type: "string"},
			]
		}
		raw: {
			sql: """
				-- name: GetStorageCredentialByName :one
				SELECT id, name, credential_type, key_id_encrypted, secret_encrypted, endpoint, region, url_style, comment, owner, created_at, updated_at, azure_account_name, azure_account_key_encrypted, azure_client_id, azure_tenant_id, azure_client_secret_encrypted, gcs_key_file_path FROM storage_credentials WHERE name = ?
				"""
			bind: ["name"]
		}
	},
	{
		name: "GetTableStatistics"
		kind: "one"
		paramMode: "single"
		params: [
			{name: "tableSecurableName", type: "string"},
		]
		result: {
			row: "TableStatistic"
			fields: [
				{name: "TableSecurableName", type: "string"},
				{name: "RowCount", type: "sql.NullInt64"},
				{name: "SizeBytes", type: "sql.NullInt64"},
				{name: "ColumnCount", type: "sql.NullInt64"},
				{name: "LastProfiledAt", type: "sql.NullString"},
				{name: "ProfiledBy", type: "sql.NullString"},
			]
		}
		raw: {
			sql: """
				-- name: GetTableStatistics :one
				SELECT table_securable_name, row_count, size_bytes, column_count, last_profiled_at, profiled_by FROM table_statistics WHERE table_securable_name = ?
				"""
			bind: ["tableSecurableName"]
		}
	},
	{
		name: "GetTag"
		kind: "one"
		paramMode: "single"
		params: [
			{name: "id", type: "string"},
		]
		result: {
			row: "Tag"
			fields: [
				{name: "ID", type: "string"},
				{name: "Key", type: "string"},
				{name: "Value", type: "sql.NullString"},
				{name: "CreatedBy", type: "string"},
				{name: "CreatedAt", type: "string"},
			]
		}
		raw: {
			sql: """
				-- name: GetTag :one
				SELECT id, "key", value, created_by, created_at FROM tags WHERE id = ?
				"""
			bind: ["id"]
		}
	},
	{
		name: "GetUnusedAuthRecoveryCodeByHash"
		kind: "one"
		paramMode: "single"
		params: [
			{name: "codeHash", type: "string"},
		]
		result: {
			row: "AuthRecoveryCode"
			fields: [
				{name: "ID", type: "string"},
				{name: "PrincipalID", type: "string"},
				{name: "CodeHash", type: "string"},
				{name: "UsedAt", type: "sql.NullTime"},
				{name: "ExpiresAt", type: "time.Time"},
				{name: "CreatedAt", type: "time.Time"},
			]
		}
		raw: {
			sql: """
				-- name: GetUnusedAuthRecoveryCodeByHash :one
				SELECT id, principal_id, code_hash, used_at, expires_at, created_at
				FROM auth_recovery_codes
				WHERE code_hash = ?
				  AND used_at IS NULL
				  AND expires_at > CURRENT_TIMESTAMP
				LIMIT 1
				"""
			bind: ["codeHash"]
		}
	},
	{
		name: "GetUpstreamLineage"
		kind: "many"
		paramMode: "struct"
		params: [
			{name: "TargetTable", type: "sql.NullString"},
			{name: "Limit", type: "int64"},
			{name: "Offset", type: "int64"},
		]
		result: {
			row: "GetUpstreamLineageRow"
			fields: [
				{name: "SourceTable", type: "string"},
				{name: "TargetTable", type: "sql.NullString"},
				{name: "EdgeType", type: "string"},
				{name: "PrincipalName", type: "string"},
				{name: "CreatedAt", type: "string"},
				{name: "SourceSchema", type: "sql.NullString"},
				{name: "TargetSchema", type: "sql.NullString"},
			]
		}
		raw: {
			sql: """
				-- name: GetUpstreamLineage :many
				SELECT DISTINCT source_table, target_table, edge_type, principal_name, created_at, source_schema, target_schema
				FROM lineage_edges
				WHERE target_table = ?
				ORDER BY created_at DESC
				LIMIT ? OFFSET ?
				"""
			bind: ["TargetTable", "Limit", "Offset"]
		}
	},
	{
		name: "GetViewByName"
		kind: "one"
		paramMode: "struct"
		params: [
			{name: "SchemaID", type: "string"},
			{name: "Name", type: "string"},
		]
		result: {
			row: "View"
			fields: [
				{name: "ID", type: "string"},
				{name: "SchemaID", type: "string"},
				{name: "Name", type: "string"},
				{name: "ViewDefinition", type: "string"},
				{name: "Comment", type: "sql.NullString"},
				{name: "Properties", type: "sql.NullString"},
				{name: "Owner", type: "string"},
				{name: "SourceTables", type: "sql.NullString"},
				{name: "CreatedAt", type: "string"},
				{name: "UpdatedAt", type: "string"},
				{name: "DeletedAt", type: "sql.NullString"},
			]
		}
		raw: {
			sql: """
				-- name: GetViewByName :one
				SELECT id, schema_id, name, view_definition, comment, properties, owner, source_tables, created_at, updated_at, deleted_at FROM views WHERE schema_id = ? AND name = ? AND deleted_at IS NULL
				"""
			bind: ["SchemaID", "Name"]
		}
	},
	{
		name: "GetVolumeByName"
		kind: "one"
		paramMode: "struct"
		params: [
			{name: "SchemaName", type: "string"},
			{name: "Name", type: "string"},
		]
		result: {
			row: "Volume"
			fields: [
				{name: "ID", type: "string"},
				{name: "Name", type: "string"},
				{name: "SchemaName", type: "string"},
				{name: "CatalogName", type: "string"},
				{name: "VolumeType", type: "string"},
				{name: "StorageLocation", type: "string"},
				{name: "Comment", type: "string"},
				{name: "Owner", type: "string"},
				{name: "CreatedAt", type: "string"},
				{name: "UpdatedAt", type: "string"},
			]
		}
		raw: {
			sql: """
				-- name: GetVolumeByName :one
				SELECT id, name, schema_name, catalog_name, volume_type, storage_location, comment, owner, created_at, updated_at FROM volumes WHERE schema_name = ? AND name = ?
				"""
			bind: ["SchemaName", "Name"]
		}
	},
	{
		name: "GetWebauthnCredentialByCredentialID"
		kind: "one"
		paramMode: "single"
		params: [
			{name: "credentialID", type: "string"},
		]
		result: {
			row: "WebauthnCredential"
			fields: [
				{name: "ID", type: "string"},
				{name: "PrincipalID", type: "string"},
				{name: "CredentialID", type: "string"},
				{name: "PublicKey", type: "string"},
				{name: "SignCount", type: "int64"},
				{name: "Transports", type: "sql.NullString"},
				{name: "BackupEligible", type: "int64"},
				{name: "BackupState", type: "int64"},
				{name: "CreatedAt", type: "time.Time"},
				{name: "LastUsedAt", type: "sql.NullTime"},
			]
		}
		raw: {
			sql: """
				-- name: GetWebauthnCredentialByCredentialID :one
				SELECT id, principal_id, credential_id, public_key, sign_count, transports, backup_eligible, backup_state, created_at, last_used_at
				FROM webauthn_credentials
				WHERE credential_id = ?
				LIMIT 1
				"""
			bind: ["credentialID"]
		}
	},
	{
		name: "GrantPrivilege"
		kind: "one"
		paramMode: "struct"
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
		raw: {
			sql: """
				-- name: GrantPrivilege :one
				INSERT INTO privilege_grants (id, principal_id, principal_type, securable_type, securable_id, privilege, granted_by)
				VALUES (?, ?, ?, ?, ?, ?, ?)
				RETURNING id, principal_id, principal_type, securable_type, securable_id, privilege, granted_by, granted_at
				"""
			bind: ["ID", "PrincipalID", "PrincipalType", "SecurableType", "SecurableID", "Privilege", "GrantedBy"]
		}
	},
	{
		name: "InsertAuditLog"
		kind: "exec"
		paramMode: "struct"
		params: [
			{name: "ID", type: "string"},
			{name: "PrincipalName", type: "string"},
			{name: "Action", type: "string"},
			{name: "StatementType", type: "sql.NullString"},
			{name: "OriginalSql", type: "sql.NullString"},
			{name: "RewrittenSql", type: "sql.NullString"},
			{name: "TablesAccessed", type: "sql.NullString"},
			{name: "Status", type: "string"},
			{name: "ErrorMessage", type: "sql.NullString"},
			{name: "DurationMs", type: "sql.NullInt64"},
			{name: "RowsReturned", type: "sql.NullInt64"},
		]
		raw: {
			sql: """
				-- name: InsertAuditLog :exec
				INSERT INTO audit_log (id, principal_name, action, statement_type, original_sql, rewritten_sql, tables_accessed, status, error_message, duration_ms, rows_returned)
				VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
				"""
			bind: ["ID", "PrincipalName", "Action", "StatementType", "OriginalSql", "RewrittenSql", "TablesAccessed", "Status", "ErrorMessage", "DurationMs", "RowsReturned"]
		}
	},
	{
		name: "InsertAuthLoginAttempt"
		kind: "exec"
		paramMode: "struct"
		params: [
			{name: "ID", type: "string"},
			{name: "Username", type: "sql.NullString"},
			{name: "IpAddress", type: "sql.NullString"},
			{name: "Success", type: "int64"},
			{name: "Reason", type: "sql.NullString"},
		]
		raw: {
			sql: """
				-- name: InsertAuthLoginAttempt :exec
				INSERT INTO auth_login_attempts (id, username, ip_address, success, reason)
				VALUES (?, ?, ?, ?, ?)
				"""
			bind: ["ID", "Username", "IpAddress", "Success", "Reason"]
		}
	},
	{
		name: "InsertColumnLineageEdge"
		kind: "exec"
		paramMode: "struct"
		params: [
			{name: "LineageEdgeID", type: "string"},
			{name: "TargetColumn", type: "string"},
			{name: "SourceSchema", type: "string"},
			{name: "SourceTable", type: "string"},
			{name: "SourceColumn", type: "string"},
			{name: "TransformType", type: "string"},
			{name: "FunctionName", type: "string"},
		]
		raw: {
			sql: """
				-- name: InsertColumnLineageEdge :exec
				INSERT INTO column_lineage_edges (lineage_edge_id, target_column, source_schema, source_table, source_column, transform_type, function_name)
				VALUES (?, ?, ?, ?, ?, ?, ?)
				"""
			bind: ["LineageEdgeID", "TargetColumn", "SourceSchema", "SourceTable", "SourceColumn", "TransformType", "FunctionName"]
		}
	},
	{
		name: "InsertExternalTableColumn"
		kind: "exec"
		paramMode: "struct"
		params: [
			{name: "ID", type: "string"},
			{name: "ExternalTableID", type: "string"},
			{name: "ColumnName", type: "string"},
			{name: "ColumnType", type: "string"},
			{name: "Position", type: "int64"},
		]
		raw: {
			sql: """
				-- name: InsertExternalTableColumn :exec
				INSERT INTO external_table_columns (id, external_table_id, column_name, column_type, position)
				VALUES (?, ?, ?, ?, ?)
				"""
			bind: ["ID", "ExternalTableID", "ColumnName", "ColumnType", "Position"]
		}
	},
	{
		name: "InsertLineageEdge"
		kind: "exec"
		paramMode: "struct"
		params: [
			{name: "ID", type: "string"},
			{name: "SourceTable", type: "string"},
			{name: "TargetTable", type: "sql.NullString"},
			{name: "EdgeType", type: "string"},
			{name: "PrincipalName", type: "string"},
			{name: "QueryHash", type: "sql.NullString"},
			{name: "SourceSchema", type: "sql.NullString"},
			{name: "TargetSchema", type: "sql.NullString"},
		]
		raw: {
			sql: """
				-- name: InsertLineageEdge :exec
				INSERT INTO lineage_edges (id, source_table, target_table, edge_type, principal_name, query_hash, source_schema, target_schema)
				VALUES (?, ?, ?, ?, ?, ?, ?, ?)
				"""
			bind: ["ID", "SourceTable", "TargetTable", "EdgeType", "PrincipalName", "QueryHash", "SourceSchema", "TargetSchema"]
		}
	},
	{
		name: "InsertOrReplaceCatalogMetadata"
		kind: "exec"
		paramMode: "struct"
		params: [
			{name: "SecurableType", type: "string"},
			{name: "SecurableName", type: "string"},
			{name: "Comment", type: "sql.NullString"},
			{name: "Owner", type: "sql.NullString"},
		]
		raw: {
			sql: """
				-- name: InsertOrReplaceCatalogMetadata :exec
				INSERT OR REPLACE INTO catalog_metadata (securable_type, securable_name, comment, owner)
				VALUES (?, ?, ?, ?)
				"""
			bind: ["SecurableType", "SecurableName", "Comment", "Owner"]
		}
	},
	{
		name: "ListAPIKeysForPrincipal"
		kind: "many"
		paramMode: "single"
		params: [
			{name: "principalID", type: "string"},
		]
		result: {
			row: "ApiKey"
			fields: [
				{name: "ID", type: "string"},
				{name: "KeyHash", type: "string"},
				{name: "PrincipalID", type: "string"},
				{name: "Name", type: "string"},
				{name: "ExpiresAt", type: "sql.NullString"},
				{name: "CreatedAt", type: "string"},
				{name: "KeyPrefix", type: "sql.NullString"},
			]
		}
		raw: {
			sql: """
				-- name: ListAPIKeysForPrincipal :many
				SELECT id, key_hash, principal_id, name, expires_at, created_at, key_prefix FROM api_keys WHERE principal_id = ? ORDER BY created_at DESC
				"""
			bind: ["principalID"]
		}
	},
	{
		name: "ListAPIKeysForPrincipalPaginated"
		kind: "many"
		paramMode: "struct"
		params: [
			{name: "PrincipalID", type: "string"},
			{name: "Limit", type: "int64"},
			{name: "Offset", type: "int64"},
		]
		result: {
			row: "ApiKey"
			fields: [
				{name: "ID", type: "string"},
				{name: "KeyHash", type: "string"},
				{name: "PrincipalID", type: "string"},
				{name: "Name", type: "string"},
				{name: "ExpiresAt", type: "sql.NullString"},
				{name: "CreatedAt", type: "string"},
				{name: "KeyPrefix", type: "sql.NullString"},
			]
		}
		raw: {
			sql: """
				-- name: ListAPIKeysForPrincipalPaginated :many
				SELECT id, key_hash, principal_id, name, expires_at, created_at, key_prefix FROM api_keys WHERE principal_id = ? ORDER BY created_at DESC LIMIT ? OFFSET ?
				"""
			bind: ["PrincipalID", "Limit", "Offset"]
		}
	},
	{
		name: "ListAllAPIKeysPaginated"
		kind: "many"
		paramMode: "struct"
		params: [
			{name: "Limit", type: "int64"},
			{name: "Offset", type: "int64"},
		]
		result: {
			row: "ApiKey"
			fields: [
				{name: "ID", type: "string"},
				{name: "KeyHash", type: "string"},
				{name: "PrincipalID", type: "string"},
				{name: "Name", type: "string"},
				{name: "ExpiresAt", type: "sql.NullString"},
				{name: "CreatedAt", type: "string"},
				{name: "KeyPrefix", type: "sql.NullString"},
			]
		}
		raw: {
			sql: """
				-- name: ListAllAPIKeysPaginated :many
				SELECT id, key_hash, principal_id, name, expires_at, created_at, key_prefix FROM api_keys ORDER BY created_at DESC LIMIT ? OFFSET ?
				"""
			bind: ["Limit", "Offset"]
		}
	},
	{
		name: "ListAllExternalTables"
		kind: "many"
		result: {
			row: "ExternalTable"
			fields: [
				{name: "ID", type: "string"},
				{name: "SchemaName", type: "string"},
				{name: "TableName", type: "string"},
				{name: "FileFormat", type: "string"},
				{name: "SourcePath", type: "string"},
				{name: "LocationName", type: "string"},
				{name: "Comment", type: "string"},
				{name: "Owner", type: "string"},
				{name: "CreatedAt", type: "string"},
				{name: "UpdatedAt", type: "string"},
				{name: "DeletedAt", type: "sql.NullString"},
				{name: "CatalogName", type: "string"},
			]
		}
		raw: {
			sql: """
				-- name: ListAllExternalTables :many
				SELECT id, schema_name, table_name, file_format, source_path, location_name, comment, owner, created_at, updated_at, deleted_at, catalog_name FROM external_tables
				WHERE deleted_at IS NULL
				"""
		}
	},
	{
		name: "ListAllGrantsForIdentities"
		kind: "many"
		paramMode: "struct"
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
			sql: """
				-- name: ListAllGrantsForIdentities :many
				SELECT id, principal_id, principal_type, securable_type, securable_id, privilege, granted_by, granted_at FROM privilege_grants
				WHERE (principal_type = 'user' AND principal_id = ?)
				   OR (principal_type = 'group' AND principal_id IN (
				       SELECT group_id FROM group_members WHERE member_type = 'user' AND member_id = ?
				   ))
				"""
			bind: ["PrincipalID", "MemberID"]
		}
	},
	{
		name: "ListAllGrantsPaginated"
		kind: "many"
		paramMode: "struct"
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
		raw: {
			sql: """
				-- name: ListAllGrantsPaginated :many
				SELECT id, principal_id, principal_type, securable_type, securable_id, privilege, granted_by, granted_at FROM privilege_grants
				ORDER BY id LIMIT ? OFFSET ?
				"""
			bind: ["Limit", "Offset"]
		}
	},
	{
		name: "ListAllMacros"
		kind: "many"
		result: {
			row: "Macro"
			fields: [
				{name: "ID", type: "string"},
				{name: "Name", type: "string"},
				{name: "MacroType", type: "string"},
				{name: "Parameters", type: "string"},
				{name: "Body", type: "string"},
				{name: "Description", type: "string"},
				{name: "CreatedBy", type: "string"},
				{name: "CreatedAt", type: "string"},
				{name: "UpdatedAt", type: "string"},
				{name: "CatalogName", type: "string"},
				{name: "ProjectName", type: "string"},
				{name: "Visibility", type: "string"},
				{name: "Owner", type: "string"},
				{name: "Properties", type: "string"},
				{name: "Tags", type: "string"},
				{name: "Status", type: "string"},
			]
		}
		raw: {
			sql: """
				-- name: ListAllMacros :many
				SELECT id, name, macro_type, parameters, body, description, created_by, created_at, updated_at, catalog_name, project_name, visibility, owner, properties, tags, status FROM macros ORDER BY name
				"""
		}
	},
	{
		name: "ListAllModels"
		kind: "many"
		result: {
			row: "Model"
			fields: [
				{name: "ID", type: "string"},
				{name: "ProjectName", type: "string"},
				{name: "Name", type: "string"},
				{name: "SqlBody", type: "string"},
				{name: "Materialization", type: "string"},
				{name: "Description", type: "string"},
				{name: "Owner", type: "string"},
				{name: "Tags", type: "string"},
				{name: "DependsOn", type: "string"},
				{name: "Config", type: "string"},
				{name: "CreatedBy", type: "string"},
				{name: "CreatedAt", type: "string"},
				{name: "UpdatedAt", type: "string"},
				{name: "Contract", type: "string"},
				{name: "FreshnessMaxLag", type: "sql.NullInt64"},
				{name: "FreshnessCron", type: "sql.NullString"},
			]
		}
		raw: {
			sql: """
				-- name: ListAllModels :many
				SELECT id, project_name, name, sql_body, materialization, description, owner, tags, depends_on, config, created_by, created_at, updated_at, contract, freshness_max_lag, freshness_cron FROM models ORDER BY project_name, name
				"""
		}
	},
	{
		name: "ListAllSemanticModels"
		kind: "many"
		result: {
			row: "SemanticModel"
			fields: [
				{name: "ID", type: "string"},
				{name: "Name", type: "string"},
				{name: "Description", type: "string"},
				{name: "Owner", type: "string"},
				{name: "BaseModelRef", type: "string"},
				{name: "DefaultTimeDimension", type: "string"},
				{name: "Tags", type: "string"},
				{name: "CreatedBy", type: "string"},
				{name: "CreatedAt", type: "string"},
				{name: "UpdatedAt", type: "string"},
			]
		}
		raw: {
			sql: """
				-- name: ListAllSemanticModels :many
				SELECT id, name, description, owner, base_model_ref, default_time_dimension, tags, created_by, created_at, updated_at FROM semantic_models ORDER BY name
				"""
		}
	},
	{
		name: "ListAssignmentsForEndpoint"
		kind: "many"
		paramMode: "struct"
		params: [
			{name: "EndpointID", type: "string"},
			{name: "Limit", type: "int64"},
			{name: "Offset", type: "int64"},
		]
		result: {
			row: "ComputeAssignment"
			fields: [
				{name: "ID", type: "string"},
				{name: "PrincipalID", type: "string"},
				{name: "PrincipalType", type: "string"},
				{name: "EndpointID", type: "string"},
				{name: "IsDefault", type: "int64"},
				{name: "FallbackLocal", type: "int64"},
				{name: "CreatedAt", type: "time.Time"},
			]
		}
		raw: {
			sql: """
				-- name: ListAssignmentsForEndpoint :many
				SELECT id, principal_id, principal_type, endpoint_id, is_default, fallback_local, created_at FROM compute_assignments WHERE endpoint_id = ? ORDER BY id LIMIT ? OFFSET ?
				"""
			bind: ["EndpointID", "Limit", "Offset"]
		}
	},
	{
		name: "ListAssignmentsForTag"
		kind: "many"
		paramMode: "single"
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
		raw: {
			sql: """
				-- name: ListAssignmentsForTag :many
				SELECT id, tag_id, securable_type, securable_id, column_name, assigned_by, assigned_at FROM tag_assignments WHERE tag_id = ?
				"""
			bind: ["tagID"]
		}
	},
	{
		name: "ListAuditLogs"
		kind: "many"
		paramMode: "struct"
		params: [
			{name: "Column1", type: "interface{}"},
			{name: "PrincipalName", type: "string"},
			{name: "Column3", type: "interface{}"},
			{name: "Action", type: "string"},
			{name: "Column5", type: "interface{}"},
			{name: "Status", type: "string"},
			{name: "Limit", type: "int64"},
			{name: "Offset", type: "int64"},
		]
		result: {
			row: "AuditLog"
			fields: [
				{name: "ID", type: "string"},
				{name: "PrincipalName", type: "string"},
				{name: "Action", type: "string"},
				{name: "StatementType", type: "sql.NullString"},
				{name: "OriginalSql", type: "sql.NullString"},
				{name: "RewrittenSql", type: "sql.NullString"},
				{name: "TablesAccessed", type: "sql.NullString"},
				{name: "Status", type: "string"},
				{name: "ErrorMessage", type: "sql.NullString"},
				{name: "DurationMs", type: "sql.NullInt64"},
				{name: "CreatedAt", type: "string"},
				{name: "RowsReturned", type: "sql.NullInt64"},
			]
		}
		raw: {
			sql: """
				-- name: ListAuditLogs :many
				SELECT id, principal_name, "action", statement_type, original_sql, rewritten_sql, tables_accessed, status, error_message, duration_ms, created_at, rows_returned FROM audit_log
				WHERE (? IS NULL OR principal_name = ?)
				  AND (? IS NULL OR action = ?)
				  AND (? IS NULL OR status = ?)
				ORDER BY created_at DESC
				LIMIT ? OFFSET ?
				"""
			bind: ["Column1", "PrincipalName", "Column3", "Action", "Column5", "Status", "Limit", "Offset"]
		}
	},
	{
		name: "ListAuthIdentitiesByPrincipal"
		kind: "many"
		paramMode: "single"
		params: [
			{name: "principalID", type: "string"},
		]
		result: {
			row: "AuthIdentity"
			fields: [
				{name: "ID", type: "string"},
				{name: "PrincipalID", type: "string"},
				{name: "Provider", type: "string"},
				{name: "Issuer", type: "sql.NullString"},
				{name: "Subject", type: "string"},
				{name: "Email", type: "sql.NullString"},
				{name: "EmailVerified", type: "int64"},
				{name: "CreatedAt", type: "time.Time"},
				{name: "UpdatedAt", type: "time.Time"},
			]
		}
		raw: {
			sql: """
				-- name: ListAuthIdentitiesByPrincipal :many
				SELECT id, principal_id, provider, issuer, subject, email, email_verified, created_at, updated_at
				FROM auth_identities
				WHERE principal_id = ?
				ORDER BY created_at DESC
				"""
			bind: ["principalID"]
		}
	},
	{
		name: "ListAuthRecoveryCodesByPrincipal"
		kind: "many"
		paramMode: "single"
		params: [
			{name: "principalID", type: "string"},
		]
		result: {
			row: "AuthRecoveryCode"
			fields: [
				{name: "ID", type: "string"},
				{name: "PrincipalID", type: "string"},
				{name: "CodeHash", type: "string"},
				{name: "UsedAt", type: "sql.NullTime"},
				{name: "ExpiresAt", type: "time.Time"},
				{name: "CreatedAt", type: "time.Time"},
			]
		}
		raw: {
			sql: """
				-- name: ListAuthRecoveryCodesByPrincipal :many
				SELECT id, principal_id, code_hash, used_at, expires_at, created_at
				FROM auth_recovery_codes
				WHERE principal_id = ?
				ORDER BY created_at DESC
				"""
			bind: ["principalID"]
		}
	},
	{
		name: "ListCatalogs"
		kind: "many"
		paramMode: "struct"
		params: [
			{name: "Limit", type: "int64"},
			{name: "Offset", type: "int64"},
		]
		result: {
			row: "Catalog"
			fields: [
				{name: "ID", type: "string"},
				{name: "Name", type: "string"},
				{name: "MetastoreType", type: "string"},
				{name: "Dsn", type: "string"},
				{name: "DataPath", type: "string"},
				{name: "Status", type: "string"},
				{name: "StatusMessage", type: "sql.NullString"},
				{name: "IsDefault", type: "int64"},
				{name: "Comment", type: "sql.NullString"},
				{name: "CreatedAt", type: "string"},
				{name: "UpdatedAt", type: "string"},
			]
		}
		raw: {
			sql: """
				-- name: ListCatalogs :many
				SELECT id, name, metastore_type, dsn, data_path, status, status_message, is_default, comment, created_at, updated_at FROM catalogs ORDER BY name LIMIT ? OFFSET ?
				"""
			bind: ["Limit", "Offset"]
		}
	},
	{
		name: "ListCells"
		kind: "many"
		paramMode: "single"
		params: [
			{name: "notebookID", type: "string"},
		]
		result: {
			row: "Cell"
			fields: [
				{name: "ID", type: "string"},
				{name: "NotebookID", type: "string"},
				{name: "CellType", type: "string"},
				{name: "Content", type: "string"},
				{name: "Position", type: "int64"},
				{name: "LastResult", type: "sql.NullString"},
				{name: "CreatedAt", type: "string"},
				{name: "UpdatedAt", type: "string"},
				{name: "Name", type: "sql.NullString"},
				{name: "Role", type: "string"},
				{name: "Disabled", type: "int64"},
				{name: "TestConfig", type: "string"},
				{name: "VisualSpec", type: "string"},
			]
		}
		raw: {
			sql: """
				-- name: ListCells :many
				SELECT id, notebook_id, cell_type, content, position, last_result, created_at, updated_at, name, role, disabled, test_config, visual_spec FROM cells WHERE notebook_id = ? ORDER BY position ASC
				"""
			bind: ["notebookID"]
		}
	},
	{
		name: "ListColumnMasksForTablePaginated"
		kind: "many"
		paramMode: "struct"
		params: [
			{name: "TableID", type: "string"},
			{name: "Limit", type: "int64"},
			{name: "Offset", type: "int64"},
		]
		result: {
			row: "ColumnMask"
			fields: [
				{name: "ID", type: "string"},
				{name: "TableID", type: "string"},
				{name: "ColumnName", type: "string"},
				{name: "MaskExpression", type: "string"},
				{name: "Description", type: "sql.NullString"},
				{name: "CreatedAt", type: "string"},
				{name: "Name", type: "sql.NullString"},
			]
		}
		raw: {
			sql: """
				-- name: ListColumnMasksForTablePaginated :many
				SELECT id, table_id, column_name, mask_expression, description, created_at, name FROM column_masks WHERE table_id = ? ORDER BY id LIMIT ? OFFSET ?
				"""
			bind: ["TableID", "Limit", "Offset"]
		}
	},
	{
		name: "ListComputeEndpoints"
		kind: "many"
		paramMode: "struct"
		params: [
			{name: "Limit", type: "int64"},
			{name: "Offset", type: "int64"},
		]
		result: {
			row: "ComputeEndpoint"
			fields: [
				{name: "ID", type: "string"},
				{name: "ExternalID", type: "string"},
				{name: "Name", type: "string"},
				{name: "Url", type: "string"},
				{name: "Type", type: "string"},
				{name: "Status", type: "string"},
				{name: "Size", type: "string"},
				{name: "MaxMemoryGb", type: "sql.NullInt64"},
				{name: "AuthToken", type: "string"},
				{name: "Owner", type: "string"},
				{name: "CreatedAt", type: "time.Time"},
				{name: "UpdatedAt", type: "time.Time"},
				{name: "SelectionPolicy", type: "string"},
				{name: "WorkloadClass", type: "string"},
				{name: "ReadinessStatus", type: "string"},
				{name: "MaxConcurrency", type: "sql.NullInt64"},
				{name: "MaxResultSizeMb", type: "sql.NullInt64"},
				{name: "RecommendedForLargeQueries", type: "int64"},
				{name: "IsDraining", type: "int64"},
				{name: "LastHealthStatus", type: "sql.NullString"},
				{name: "LastHealthCheckedAt", type: "sql.NullTime"},
				{name: "ActiveQueries", type: "sql.NullInt64"},
				{name: "QueuedJobs", type: "sql.NullInt64"},
				{name: "RunningJobs", type: "sql.NullInt64"},
				{name: "CompletedJobs", type: "sql.NullInt64"},
				{name: "StoredJobs", type: "sql.NullInt64"},
				{name: "CleanedJobs", type: "sql.NullInt64"},
				{name: "QueryResultTtlSeconds", type: "sql.NullInt64"},
			]
		}
		raw: {
			sql: """
				-- name: ListComputeEndpoints :many
				SELECT id, external_id, name, url, type, status, size, max_memory_gb, auth_token, owner, created_at, updated_at, selection_policy, workload_class, readiness_status, max_concurrency, max_result_size_mb, recommended_for_large_queries, is_draining, last_health_status, last_health_checked_at, active_queries, queued_jobs, running_jobs, completed_jobs, stored_jobs, cleaned_jobs, query_result_ttl_seconds FROM compute_endpoints ORDER BY name LIMIT ? OFFSET ?
				"""
			bind: ["Limit", "Offset"]
		}
	},
	{
		name: "ListDashboardWidgetsByDashboard"
		kind: "many"
		paramMode: "single"
		params: [
			{name: "dashboardID", type: "string"},
		]
		result: {
			row: "DashboardWidget"
			fields: [
				{name: "ID", type: "string"},
				{name: "DashboardID", type: "string"},
				{name: "Name", type: "string"},
				{name: "Description", type: "string"},
				{name: "SourceJson", type: "string"},
				{name: "VisualSpec", type: "string"},
				{name: "LayoutX", type: "int64"},
				{name: "LayoutY", type: "int64"},
				{name: "LayoutW", type: "int64"},
				{name: "LayoutH", type: "int64"},
				{name: "CreatedAt", type: "string"},
				{name: "UpdatedAt", type: "string"},
				{name: "FilterOriginKey", type: "string"},
				{name: "PageName", type: "string"},
			]
		}
		raw: {
			sql: """
				-- name: ListDashboardWidgetsByDashboard :many
				SELECT id, dashboard_id, name, description, source_json, visual_spec, layout_x, layout_y, layout_w, layout_h, created_at, updated_at, filter_origin_key, page_name FROM dashboard_widgets WHERE dashboard_id = ? ORDER BY layout_y, layout_x, created_at
				"""
			bind: ["dashboardID"]
		}
	},
	{
		name: "ListExternalLocations"
		kind: "many"
		paramMode: "struct"
		params: [
			{name: "Limit", type: "int64"},
			{name: "Offset", type: "int64"},
		]
		result: {
			row: "ExternalLocation"
			fields: [
				{name: "ID", type: "string"},
				{name: "Name", type: "string"},
				{name: "Url", type: "string"},
				{name: "CredentialName", type: "string"},
				{name: "StorageType", type: "string"},
				{name: "Comment", type: "string"},
				{name: "Owner", type: "string"},
				{name: "ReadOnly", type: "int64"},
				{name: "CreatedAt", type: "string"},
				{name: "UpdatedAt", type: "string"},
			]
		}
		raw: {
			sql: """
				-- name: ListExternalLocations :many
				SELECT id, name, url, credential_name, storage_type, comment, owner, read_only, created_at, updated_at FROM external_locations ORDER BY name LIMIT ? OFFSET ?
				"""
			bind: ["Limit", "Offset"]
		}
	},
	{
		name: "ListExternalTableColumns"
		kind: "many"
		paramMode: "single"
		params: [
			{name: "externalTableID", type: "string"},
		]
		result: {
			row: "ExternalTableColumn"
			fields: [
				{name: "ID", type: "string"},
				{name: "ExternalTableID", type: "string"},
				{name: "ColumnName", type: "string"},
				{name: "ColumnType", type: "string"},
				{name: "Position", type: "int64"},
			]
		}
		raw: {
			sql: """
				-- name: ListExternalTableColumns :many
				SELECT id, external_table_id, column_name, column_type, position FROM external_table_columns
				WHERE external_table_id = ?
				ORDER BY position
				"""
			bind: ["externalTableID"]
		}
	},
	{
		name: "ListExternalTables"
		kind: "many"
		paramMode: "struct"
		params: [
			{name: "SchemaName", type: "string"},
			{name: "Limit", type: "int64"},
			{name: "Offset", type: "int64"},
		]
		result: {
			row: "ExternalTable"
			fields: [
				{name: "ID", type: "string"},
				{name: "SchemaName", type: "string"},
				{name: "TableName", type: "string"},
				{name: "FileFormat", type: "string"},
				{name: "SourcePath", type: "string"},
				{name: "LocationName", type: "string"},
				{name: "Comment", type: "string"},
				{name: "Owner", type: "string"},
				{name: "CreatedAt", type: "string"},
				{name: "UpdatedAt", type: "string"},
				{name: "DeletedAt", type: "sql.NullString"},
				{name: "CatalogName", type: "string"},
			]
		}
		raw: {
			sql: """
				-- name: ListExternalTables :many
				SELECT id, schema_name, table_name, file_format, source_path, location_name, comment, owner, created_at, updated_at, deleted_at, catalog_name FROM external_tables
				WHERE schema_name = ? AND deleted_at IS NULL
				ORDER BY table_name
				LIMIT ? OFFSET ?
				"""
			bind: ["SchemaName", "Limit", "Offset"]
		}
	},
	{
		name: "ListGitRepos"
		kind: "many"
		paramMode: "struct"
		params: [
			{name: "Limit", type: "int64"},
			{name: "Offset", type: "int64"},
		]
		result: {
			row: "GitRepo"
			fields: [
				{name: "ID", type: "string"},
				{name: "Url", type: "string"},
				{name: "Branch", type: "string"},
				{name: "Path", type: "string"},
				{name: "AuthToken", type: "string"},
				{name: "WebhookSecret", type: "sql.NullString"},
				{name: "Owner", type: "string"},
				{name: "LastSyncAt", type: "sql.NullString"},
				{name: "LastCommit", type: "sql.NullString"},
				{name: "CreatedAt", type: "string"},
				{name: "UpdatedAt", type: "string"},
			]
		}
		raw: {
			sql: """
				-- name: ListGitRepos :many
				SELECT id, url, branch, path, auth_token, webhook_secret, owner, last_sync_at, last_commit, created_at, updated_at FROM git_repos ORDER BY created_at DESC LIMIT ? OFFSET ?
				"""
			bind: ["Limit", "Offset"]
		}
	},
	{
		name: "ListGrantsForPrincipal"
		kind: "many"
		paramMode: "struct"
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
		raw: {
			sql: """
				-- name: ListGrantsForPrincipal :many
				SELECT id, principal_id, principal_type, securable_type, securable_id, privilege, granted_by, granted_at FROM privilege_grants
				WHERE principal_id = ? AND principal_type = ?
				"""
			bind: ["PrincipalID", "PrincipalType"]
		}
	},
	{
		name: "ListGrantsForPrincipalOnSecurable"
		kind: "many"
		paramMode: "struct"
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
		raw: {
			sql: """
				-- name: ListGrantsForPrincipalOnSecurable :many
				SELECT id, principal_id, principal_type, securable_type, securable_id, privilege, granted_by, granted_at FROM privilege_grants
				WHERE principal_id = ? AND principal_type = ? AND securable_type = ? AND securable_id = ?
				"""
			bind: ["PrincipalID", "PrincipalType", "SecurableType", "SecurableID"]
		}
	},
	{
		name: "ListGrantsForPrincipalPaginated"
		kind: "many"
		paramMode: "struct"
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
		raw: {
			sql: """
				-- name: ListGrantsForPrincipalPaginated :many
				SELECT id, principal_id, principal_type, securable_type, securable_id, privilege, granted_by, granted_at FROM privilege_grants
				WHERE principal_id = ? AND principal_type = ?
				ORDER BY id LIMIT ? OFFSET ?
				"""
			bind: ["PrincipalID", "PrincipalType", "Limit", "Offset"]
		}
	},
	{
		name: "ListGrantsForSecurable"
		kind: "many"
		paramMode: "struct"
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
		raw: {
			sql: """
				-- name: ListGrantsForSecurable :many
				SELECT id, principal_id, principal_type, securable_type, securable_id, privilege, granted_by, granted_at FROM privilege_grants
				WHERE securable_type = ? AND securable_id = ?
				"""
			bind: ["SecurableType", "SecurableID"]
		}
	},
	{
		name: "ListGrantsForSecurablePaginated"
		kind: "many"
		paramMode: "struct"
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
		raw: {
			sql: """
				-- name: ListGrantsForSecurablePaginated :many
				SELECT id, principal_id, principal_type, securable_type, securable_id, privilege, granted_by, granted_at FROM privilege_grants
				WHERE securable_type = ? AND securable_id = ?
				ORDER BY id LIMIT ? OFFSET ?
				"""
			bind: ["SecurableType", "SecurableID", "Limit", "Offset"]
		}
	},
	{
		name: "ListGroupMembers"
		kind: "many"
		paramMode: "single"
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
		raw: {
			sql: """
				-- name: ListGroupMembers :many
				SELECT group_id, member_type, member_id FROM group_members WHERE group_id = ?
				"""
			bind: ["groupID"]
		}
	},
	{
		name: "ListGroupMembersPaginated"
		kind: "many"
		paramMode: "struct"
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
		raw: {
			sql: """
				-- name: ListGroupMembersPaginated :many
				SELECT group_id, member_type, member_id FROM group_members WHERE group_id = ? ORDER BY member_id LIMIT ? OFFSET ?
				"""
			bind: ["GroupID", "Limit", "Offset"]
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
		raw: {
			sql: """
				-- name: ListGroups :many
				SELECT id, name, description, created_at FROM groups ORDER BY name
				"""
		}
	},
	{
		name: "ListGroupsPaginated"
		kind: "many"
		paramMode: "struct"
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
		raw: {
			sql: """
				-- name: ListGroupsPaginated :many
				SELECT id, name, description, created_at FROM groups ORDER BY id LIMIT ? OFFSET ?
				"""
			bind: ["Limit", "Offset"]
		}
	},
	{
		name: "ListMacroRevisions"
		kind: "many"
		paramMode: "single"
		params: [
			{name: "macroName", type: "string"},
		]
		result: {
			row: "MacroRevision"
			fields: [
				{name: "ID", type: "string"},
				{name: "MacroID", type: "string"},
				{name: "MacroName", type: "string"},
				{name: "Version", type: "int64"},
				{name: "ContentHash", type: "string"},
				{name: "Parameters", type: "string"},
				{name: "Body", type: "string"},
				{name: "Description", type: "string"},
				{name: "Status", type: "string"},
				{name: "CreatedBy", type: "string"},
				{name: "CreatedAt", type: "string"},
			]
		}
		raw: {
			sql: """
				-- name: ListMacroRevisions :many
				SELECT id, macro_id, macro_name, version, content_hash, parameters, body, description, status, created_by, created_at FROM macro_revisions WHERE macro_name = ? ORDER BY version DESC
				"""
			bind: ["macroName"]
		}
	},
	{
		name: "ListMacros"
		kind: "many"
		paramMode: "struct"
		params: [
			{name: "Limit", type: "int64"},
			{name: "Offset", type: "int64"},
		]
		result: {
			row: "Macro"
			fields: [
				{name: "ID", type: "string"},
				{name: "Name", type: "string"},
				{name: "MacroType", type: "string"},
				{name: "Parameters", type: "string"},
				{name: "Body", type: "string"},
				{name: "Description", type: "string"},
				{name: "CreatedBy", type: "string"},
				{name: "CreatedAt", type: "string"},
				{name: "UpdatedAt", type: "string"},
				{name: "CatalogName", type: "string"},
				{name: "ProjectName", type: "string"},
				{name: "Visibility", type: "string"},
				{name: "Owner", type: "string"},
				{name: "Properties", type: "string"},
				{name: "Tags", type: "string"},
				{name: "Status", type: "string"},
			]
		}
		raw: {
			sql: """
				-- name: ListMacros :many
				SELECT id, name, macro_type, parameters, body, description, created_by, created_at, updated_at, catalog_name, project_name, visibility, owner, properties, tags, status FROM macros ORDER BY name LIMIT ? OFFSET ?
				"""
			bind: ["Limit", "Offset"]
		}
	},
	{
		name: "ListModelRunStepsByRun"
		kind: "many"
		paramMode: "single"
		params: [
			{name: "runID", type: "string"},
		]
		result: {
			row: "ModelRunStep"
			fields: [
				{name: "ID", type: "string"},
				{name: "RunID", type: "string"},
				{name: "ModelID", type: "string"},
				{name: "ModelName", type: "string"},
				{name: "Status", type: "string"},
				{name: "Tier", type: "int64"},
				{name: "RowsAffected", type: "sql.NullInt64"},
				{name: "StartedAt", type: "sql.NullString"},
				{name: "FinishedAt", type: "sql.NullString"},
				{name: "ErrorMessage", type: "sql.NullString"},
				{name: "CreatedAt", type: "string"},
				{name: "CompiledSql", type: "sql.NullString"},
				{name: "CompiledHash", type: "sql.NullString"},
				{name: "DependsOn", type: "string"},
				{name: "VarsUsed", type: "string"},
				{name: "MacrosUsed", type: "string"},
			]
		}
		raw: {
			sql: """
				-- name: ListModelRunStepsByRun :many
				SELECT id, run_id, model_id, model_name, status, tier, rows_affected, started_at, finished_at, error_message, created_at, compiled_sql, compiled_hash, depends_on, vars_used, macros_used FROM model_run_steps WHERE run_id = ? ORDER BY tier, model_name
				"""
			bind: ["runID"]
		}
	},
	{
		name: "ListModelTestResultsByStep"
		kind: "many"
		paramMode: "single"
		params: [
			{name: "runStepID", type: "string"},
		]
		result: {
			row: "ModelTestResult"
			fields: [
				{name: "ID", type: "string"},
				{name: "RunStepID", type: "string"},
				{name: "TestID", type: "string"},
				{name: "TestName", type: "string"},
				{name: "Status", type: "string"},
				{name: "RowsReturned", type: "sql.NullInt64"},
				{name: "ErrorMessage", type: "sql.NullString"},
				{name: "CreatedAt", type: "string"},
			]
		}
		raw: {
			sql: """
				-- name: ListModelTestResultsByStep :many
				SELECT id, run_step_id, test_id, test_name, status, rows_returned, error_message, created_at FROM model_test_results WHERE run_step_id = ? ORDER BY test_name
				"""
			bind: ["runStepID"]
		}
	},
	{
		name: "ListModelTestsByModel"
		kind: "many"
		paramMode: "single"
		params: [
			{name: "modelID", type: "string"},
		]
		result: {
			row: "ModelTest"
			fields: [
				{name: "ID", type: "string"},
				{name: "ModelID", type: "string"},
				{name: "Name", type: "string"},
				{name: "TestType", type: "string"},
				{name: "ColumnName", type: "string"},
				{name: "Config", type: "string"},
				{name: "CreatedAt", type: "string"},
			]
		}
		raw: {
			sql: """
				-- name: ListModelTestsByModel :many
				SELECT id, model_id, name, test_type, column_name, config, created_at FROM model_tests WHERE model_id = ? ORDER BY name
				"""
			bind: ["modelID"]
		}
	},
	{
		name: "ListNotebookJobs"
		kind: "many"
		paramMode: "struct"
		params: [
			{name: "NotebookID", type: "string"},
			{name: "Limit", type: "int64"},
			{name: "Offset", type: "int64"},
		]
		result: {
			row: "NotebookJob"
			fields: [
				{name: "ID", type: "string"},
				{name: "NotebookID", type: "string"},
				{name: "SessionID", type: "string"},
				{name: "State", type: "string"},
				{name: "Result", type: "sql.NullString"},
				{name: "Error", type: "sql.NullString"},
				{name: "CreatedAt", type: "string"},
				{name: "UpdatedAt", type: "string"},
			]
		}
		raw: {
			sql: """
				-- name: ListNotebookJobs :many
				SELECT id, notebook_id, session_id, state, result, error, created_at, updated_at FROM notebook_jobs
				WHERE notebook_id = ?
				ORDER BY created_at DESC
				LIMIT ? OFFSET ?
				"""
			bind: ["NotebookID", "Limit", "Offset"]
		}
	},
	{
		name: "ListPipelineJobRunsByRun"
		kind: "many"
		paramMode: "single"
		params: [
			{name: "runID", type: "string"},
		]
		result: {
			row: "PipelineJobRun"
			fields: [
				{name: "ID", type: "string"},
				{name: "RunID", type: "string"},
				{name: "JobID", type: "string"},
				{name: "JobName", type: "string"},
				{name: "Status", type: "string"},
				{name: "StartedAt", type: "sql.NullString"},
				{name: "FinishedAt", type: "sql.NullString"},
				{name: "ErrorMessage", type: "sql.NullString"},
				{name: "RetryAttempt", type: "int64"},
				{name: "CreatedAt", type: "string"},
			]
		}
		raw: {
			sql: """
				-- name: ListPipelineJobRunsByRun :many
				SELECT id, run_id, job_id, job_name, status, started_at, finished_at, error_message, retry_attempt, created_at FROM pipeline_job_runs WHERE run_id = ? ORDER BY created_at
				"""
			bind: ["runID"]
		}
	},
	{
		name: "ListPipelineJobsByPipeline"
		kind: "many"
		paramMode: "single"
		params: [
			{name: "pipelineID", type: "string"},
		]
		result: {
			row: "PipelineJob"
			fields: [
				{name: "ID", type: "string"},
				{name: "PipelineID", type: "string"},
				{name: "Name", type: "string"},
				{name: "ComputeEndpointID", type: "sql.NullString"},
				{name: "DependsOn", type: "string"},
				{name: "NotebookID", type: "string"},
				{name: "TimeoutSeconds", type: "sql.NullInt64"},
				{name: "RetryCount", type: "int64"},
				{name: "JobOrder", type: "int64"},
				{name: "CreatedAt", type: "string"},
				{name: "JobType", type: "string"},
				{name: "ModelSelector", type: "string"},
			]
		}
		raw: {
			sql: """
				-- name: ListPipelineJobsByPipeline :many
				SELECT id, pipeline_id, name, compute_endpoint_id, depends_on, notebook_id, timeout_seconds, retry_count, job_order, created_at, job_type, model_selector FROM pipeline_jobs WHERE pipeline_id = ? ORDER BY job_order, name
				"""
			bind: ["pipelineID"]
		}
	},
	{
		name: "ListPipelines"
		kind: "many"
		paramMode: "struct"
		params: [
			{name: "Limit", type: "int64"},
			{name: "Offset", type: "int64"},
		]
		result: {
			row: "Pipeline"
			fields: [
				{name: "ID", type: "string"},
				{name: "Name", type: "string"},
				{name: "Description", type: "string"},
				{name: "ScheduleCron", type: "sql.NullString"},
				{name: "IsPaused", type: "int64"},
				{name: "ConcurrencyLimit", type: "int64"},
				{name: "CreatedBy", type: "string"},
				{name: "CreatedAt", type: "string"},
				{name: "UpdatedAt", type: "string"},
				{name: "FolderID", type: "sql.NullString"},
			]
		}
		raw: {
			sql: """
				-- name: ListPipelines :many
				SELECT id, name, description, schedule_cron, is_paused, concurrency_limit, created_by, created_at, updated_at, folder_id FROM pipelines ORDER BY name LIMIT ? OFFSET ?
				"""
			bind: ["Limit", "Offset"]
		}
	},
	{
		name: "ListPrincipals"
		kind: "many"
		result: {
			row: "Principal"
			fields: [
				{name: "ID", type: "string"},
				{name: "Name", type: "string"},
				{name: "Type", type: "string"},
				{name: "IsAdmin", type: "int64"},
				{name: "CreatedAt", type: "string"},
				{name: "ExternalID", type: "sql.NullString"},
				{name: "ExternalIssuer", type: "sql.NullString"},
			]
		}
		raw: {
			sql: """
				-- name: ListPrincipals :many
				SELECT id, name, type, is_admin, created_at, external_id, external_issuer FROM principals ORDER BY name
				"""
		}
	},
	{
		name: "ListPrincipalsPaginated"
		kind: "many"
		paramMode: "struct"
		params: [
			{name: "Limit", type: "int64"},
			{name: "Offset", type: "int64"},
		]
		result: {
			row: "Principal"
			fields: [
				{name: "ID", type: "string"},
				{name: "Name", type: "string"},
				{name: "Type", type: "string"},
				{name: "IsAdmin", type: "int64"},
				{name: "CreatedAt", type: "string"},
				{name: "ExternalID", type: "sql.NullString"},
				{name: "ExternalIssuer", type: "sql.NullString"},
			]
		}
		raw: {
			sql: """
				-- name: ListPrincipalsPaginated :many
				SELECT id, name, type, is_admin, created_at, external_id, external_issuer FROM principals ORDER BY id LIMIT ? OFFSET ?
				"""
			bind: ["Limit", "Offset"]
		}
	},
	{
		name: "ListQueryHistory"
		kind: "many"
		paramMode: "struct"
		params: [
			{name: "Column1", type: "interface{}"},
			{name: "PrincipalName", type: "string"},
			{name: "Column3", type: "interface{}"},
			{name: "Status", type: "string"},
			{name: "Column5", type: "interface{}"},
			{name: "CreatedAt", type: "string"},
			{name: "Column7", type: "interface{}"},
			{name: "CreatedAt_2", type: "string"},
			{name: "Limit", type: "int64"},
			{name: "Offset", type: "int64"},
		]
		result: {
			row: "AuditLog"
			fields: [
				{name: "ID", type: "string"},
				{name: "PrincipalName", type: "string"},
				{name: "Action", type: "string"},
				{name: "StatementType", type: "sql.NullString"},
				{name: "OriginalSql", type: "sql.NullString"},
				{name: "RewrittenSql", type: "sql.NullString"},
				{name: "TablesAccessed", type: "sql.NullString"},
				{name: "Status", type: "string"},
				{name: "ErrorMessage", type: "sql.NullString"},
				{name: "DurationMs", type: "sql.NullInt64"},
				{name: "CreatedAt", type: "string"},
				{name: "RowsReturned", type: "sql.NullInt64"},
			]
		}
		raw: {
			sql: """
				-- name: ListQueryHistory :many
				SELECT id, principal_name, "action", statement_type, original_sql, rewritten_sql, tables_accessed, status, error_message, duration_ms, created_at, rows_returned FROM audit_log
				WHERE action = 'QUERY'
				  AND (? IS NULL OR principal_name = ?)
				  AND (? IS NULL OR status = ?)
				  AND (? IS NULL OR created_at >= ?)
				  AND (? IS NULL OR created_at <= ?)
				ORDER BY created_at DESC
				LIMIT ? OFFSET ?
				"""
			bind: ["Column1", "PrincipalName", "Column3", "Status", "Column5", "CreatedAt", "Column7", "CreatedAt_2", "Limit", "Offset"]
		}
	},
	{
		name: "ListRowFiltersForTablePaginated"
		kind: "many"
		paramMode: "struct"
		params: [
			{name: "TableID", type: "string"},
			{name: "Limit", type: "int64"},
			{name: "Offset", type: "int64"},
		]
		result: {
			row: "RowFilter"
			fields: [
				{name: "ID", type: "string"},
				{name: "TableID", type: "string"},
				{name: "FilterSql", type: "string"},
				{name: "Description", type: "sql.NullString"},
				{name: "CreatedAt", type: "string"},
				{name: "Name", type: "sql.NullString"},
			]
		}
		raw: {
			sql: """
				-- name: ListRowFiltersForTablePaginated :many
				SELECT id, table_id, filter_sql, description, created_at, name FROM row_filters WHERE table_id = ? ORDER BY id LIMIT ? OFFSET ?
				"""
			bind: ["TableID", "Limit", "Offset"]
		}
	},
	{
		name: "ListScheduledPipelines"
		kind: "many"
		result: {
			row: "Pipeline"
			fields: [
				{name: "ID", type: "string"},
				{name: "Name", type: "string"},
				{name: "Description", type: "string"},
				{name: "ScheduleCron", type: "sql.NullString"},
				{name: "IsPaused", type: "int64"},
				{name: "ConcurrencyLimit", type: "int64"},
				{name: "CreatedBy", type: "string"},
				{name: "CreatedAt", type: "string"},
				{name: "UpdatedAt", type: "string"},
				{name: "FolderID", type: "sql.NullString"},
			]
		}
		raw: {
			sql: """
				-- name: ListScheduledPipelines :many
				SELECT id, name, description, schedule_cron, is_paused, concurrency_limit, created_by, created_at, updated_at, folder_id FROM pipelines WHERE schedule_cron IS NOT NULL AND is_paused = 0
				"""
		}
	},
	{
		name: "ListSemanticMetricsByModel"
		kind: "many"
		paramMode: "single"
		params: [
			{name: "semanticModelID", type: "string"},
		]
		result: {
			row: "SemanticMetric"
			fields: [
				{name: "ID", type: "string"},
				{name: "SemanticModelID", type: "string"},
				{name: "Name", type: "string"},
				{name: "Description", type: "string"},
				{name: "MetricType", type: "string"},
				{name: "ExpressionMode", type: "string"},
				{name: "Expression", type: "string"},
				{name: "DefaultTimeGrain", type: "string"},
				{name: "Format", type: "string"},
				{name: "Owner", type: "string"},
				{name: "CertificationState", type: "string"},
				{name: "CreatedBy", type: "string"},
				{name: "CreatedAt", type: "string"},
				{name: "UpdatedAt", type: "string"},
				{name: "Label", type: "string"},
				{name: "FilterSql", type: "string"},
				{name: "RelationshipNames", type: "string"},
			]
		}
		raw: {
			sql: """
				-- name: ListSemanticMetricsByModel :many
				SELECT id, semantic_model_id, name, description, metric_type, expression_mode, expression, default_time_grain, format, owner, certification_state, created_by, created_at, updated_at, label, filter_sql, relationship_names FROM semantic_metrics
				WHERE semantic_model_id = ?
				ORDER BY name
				"""
			bind: ["semanticModelID"]
		}
	},
	{
		name: "ListSemanticModels"
		kind: "many"
		paramMode: "struct"
		params: [
			{name: "Limit", type: "int64"},
			{name: "Offset", type: "int64"},
		]
		result: {
			row: "SemanticModel"
			fields: [
				{name: "ID", type: "string"},
				{name: "Name", type: "string"},
				{name: "Description", type: "string"},
				{name: "Owner", type: "string"},
				{name: "BaseModelRef", type: "string"},
				{name: "DefaultTimeDimension", type: "string"},
				{name: "Tags", type: "string"},
				{name: "CreatedBy", type: "string"},
				{name: "CreatedAt", type: "string"},
				{name: "UpdatedAt", type: "string"},
			]
		}
		raw: {
			sql: """
				-- name: ListSemanticModels :many
				SELECT id, name, description, owner, base_model_ref, default_time_dimension, tags, created_by, created_at, updated_at FROM semantic_models
				ORDER BY name
				LIMIT ? OFFSET ?
				"""
			bind: ["Limit", "Offset"]
		}
	},
	{
		name: "ListSemanticPreAggregationsByModel"
		kind: "many"
		paramMode: "single"
		params: [
			{name: "semanticModelID", type: "string"},
		]
		result: {
			row: "SemanticPreAggregation"
			fields: [
				{name: "ID", type: "string"},
				{name: "SemanticModelID", type: "string"},
				{name: "Name", type: "string"},
				{name: "MetricSet", type: "string"},
				{name: "DimensionSet", type: "string"},
				{name: "Grain", type: "string"},
				{name: "TargetRelation", type: "string"},
				{name: "RefreshPolicy", type: "string"},
				{name: "CreatedBy", type: "string"},
				{name: "CreatedAt", type: "string"},
				{name: "UpdatedAt", type: "string"},
			]
		}
		raw: {
			sql: """
				-- name: ListSemanticPreAggregationsByModel :many
				SELECT id, semantic_model_id, name, metric_set, dimension_set, grain, target_relation, refresh_policy, created_by, created_at, updated_at FROM semantic_pre_aggregations
				WHERE semantic_model_id = ?
				ORDER BY name
				"""
			bind: ["semanticModelID"]
		}
	},
	{
		name: "ListSemanticRelationships"
		kind: "many"
		paramMode: "struct"
		params: [
			{name: "Limit", type: "int64"},
			{name: "Offset", type: "int64"},
		]
		result: {
			row: "SemanticRelationship"
			fields: [
				{name: "ID", type: "string"},
				{name: "Name", type: "string"},
				{name: "FromSemanticID", type: "string"},
				{name: "ToSemanticID", type: "string"},
				{name: "RelationshipType", type: "string"},
				{name: "JoinSql", type: "string"},
				{name: "Cost", type: "int64"},
				{name: "MaxHops", type: "int64"},
				{name: "CreatedBy", type: "string"},
				{name: "CreatedAt", type: "string"},
				{name: "UpdatedAt", type: "string"},
			]
		}
		raw: {
			sql: """
				-- name: ListSemanticRelationships :many
				SELECT id, name, from_semantic_id, to_semantic_id, relationship_type, join_sql, cost, max_hops, created_by, created_at, updated_at FROM semantic_relationships
				ORDER BY name
				LIMIT ? OFFSET ?
				"""
			bind: ["Limit", "Offset"]
		}
	},
	{
		name: "ListStorageCredentials"
		kind: "many"
		paramMode: "struct"
		params: [
			{name: "Limit", type: "int64"},
			{name: "Offset", type: "int64"},
		]
		result: {
			row: "StorageCredential"
			fields: [
				{name: "ID", type: "string"},
				{name: "Name", type: "string"},
				{name: "CredentialType", type: "string"},
				{name: "KeyIDEncrypted", type: "string"},
				{name: "SecretEncrypted", type: "string"},
				{name: "Endpoint", type: "string"},
				{name: "Region", type: "string"},
				{name: "UrlStyle", type: "string"},
				{name: "Comment", type: "string"},
				{name: "Owner", type: "string"},
				{name: "CreatedAt", type: "string"},
				{name: "UpdatedAt", type: "string"},
				{name: "AzureAccountName", type: "string"},
				{name: "AzureAccountKeyEncrypted", type: "string"},
				{name: "AzureClientID", type: "string"},
				{name: "AzureTenantID", type: "string"},
				{name: "AzureClientSecretEncrypted", type: "string"},
				{name: "GcsKeyFilePath", type: "string"},
			]
		}
		raw: {
			sql: """
				-- name: ListStorageCredentials :many
				SELECT id, name, credential_type, key_id_encrypted, secret_encrypted, endpoint, region, url_style, comment, owner, created_at, updated_at, azure_account_name, azure_account_key_encrypted, azure_client_id, azure_tenant_id, azure_client_secret_encrypted, gcs_key_file_path FROM storage_credentials ORDER BY name LIMIT ? OFFSET ?
				"""
			bind: ["Limit", "Offset"]
		}
	},
	{
		name: "ListTagAssignments"
		kind: "many"
		paramMode: "struct"
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
		raw: {
			sql: """
				-- name: ListTagAssignments :many
				SELECT id, tag_id, securable_type, securable_id, column_name, assigned_by, assigned_at FROM tag_assignments ORDER BY id LIMIT ? OFFSET ?
				"""
			bind: ["Limit", "Offset"]
		}
	},
	{
		name: "ListTags"
		kind: "many"
		paramMode: "struct"
		params: [
			{name: "Limit", type: "int64"},
			{name: "Offset", type: "int64"},
		]
		result: {
			row: "Tag"
			fields: [
				{name: "ID", type: "string"},
				{name: "Key", type: "string"},
				{name: "Value", type: "sql.NullString"},
				{name: "CreatedBy", type: "string"},
				{name: "CreatedAt", type: "string"},
			]
		}
		raw: {
			sql: """
				-- name: ListTags :many
				SELECT id, "key", value, created_by, created_at FROM tags ORDER BY key, value LIMIT ? OFFSET ?
				"""
			bind: ["Limit", "Offset"]
		}
	},
	{
		name: "ListTagsForSecurable"
		kind: "many"
		paramMode: "struct"
		params: [
			{name: "SecurableType", type: "string"},
			{name: "SecurableID", type: "string"},
			{name: "Column3", type: "interface{}"},
			{name: "ColumnName", type: "sql.NullString"},
		]
		result: {
			row: "Tag"
			fields: [
				{name: "ID", type: "string"},
				{name: "Key", type: "string"},
				{name: "Value", type: "sql.NullString"},
				{name: "CreatedBy", type: "string"},
				{name: "CreatedAt", type: "string"},
			]
		}
		raw: {
			sql: """
				-- name: ListTagsForSecurable :many
				SELECT t.id, t."key", t.value, t.created_by, t.created_at FROM tags t
				JOIN tag_assignments ta ON t.id = ta.tag_id
				WHERE ta.securable_type = ? AND ta.securable_id = ?
				  AND (? IS NULL OR ta.column_name = ?)
				ORDER BY t.key, t.value
				"""
			bind: ["SecurableType", "SecurableID", "Column3", "ColumnName"]
		}
	},
	{
		name: "ListViews"
		kind: "many"
		paramMode: "struct"
		params: [
			{name: "SchemaID", type: "string"},
			{name: "Limit", type: "int64"},
			{name: "Offset", type: "int64"},
		]
		result: {
			row: "View"
			fields: [
				{name: "ID", type: "string"},
				{name: "SchemaID", type: "string"},
				{name: "Name", type: "string"},
				{name: "ViewDefinition", type: "string"},
				{name: "Comment", type: "sql.NullString"},
				{name: "Properties", type: "sql.NullString"},
				{name: "Owner", type: "string"},
				{name: "SourceTables", type: "sql.NullString"},
				{name: "CreatedAt", type: "string"},
				{name: "UpdatedAt", type: "string"},
				{name: "DeletedAt", type: "sql.NullString"},
			]
		}
		raw: {
			sql: """
				-- name: ListViews :many
				SELECT id, schema_id, name, view_definition, comment, properties, owner, source_tables, created_at, updated_at, deleted_at FROM views WHERE schema_id = ? AND deleted_at IS NULL ORDER BY name LIMIT ? OFFSET ?
				"""
			bind: ["SchemaID", "Limit", "Offset"]
		}
	},
	{
		name: "ListVolumes"
		kind: "many"
		paramMode: "struct"
		params: [
			{name: "SchemaName", type: "string"},
			{name: "Limit", type: "int64"},
			{name: "Offset", type: "int64"},
		]
		result: {
			row: "Volume"
			fields: [
				{name: "ID", type: "string"},
				{name: "Name", type: "string"},
				{name: "SchemaName", type: "string"},
				{name: "CatalogName", type: "string"},
				{name: "VolumeType", type: "string"},
				{name: "StorageLocation", type: "string"},
				{name: "Comment", type: "string"},
				{name: "Owner", type: "string"},
				{name: "CreatedAt", type: "string"},
				{name: "UpdatedAt", type: "string"},
			]
		}
		raw: {
			sql: """
				-- name: ListVolumes :many
				SELECT id, name, schema_name, catalog_name, volume_type, storage_location, comment, owner, created_at, updated_at FROM volumes WHERE schema_name = ? ORDER BY name LIMIT ? OFFSET ?
				"""
			bind: ["SchemaName", "Limit", "Offset"]
		}
	},
	{
		name: "ListWebauthnCredentialsByPrincipal"
		kind: "many"
		paramMode: "single"
		params: [
			{name: "principalID", type: "string"},
		]
		result: {
			row: "WebauthnCredential"
			fields: [
				{name: "ID", type: "string"},
				{name: "PrincipalID", type: "string"},
				{name: "CredentialID", type: "string"},
				{name: "PublicKey", type: "string"},
				{name: "SignCount", type: "int64"},
				{name: "Transports", type: "sql.NullString"},
				{name: "BackupEligible", type: "int64"},
				{name: "BackupState", type: "int64"},
				{name: "CreatedAt", type: "time.Time"},
				{name: "LastUsedAt", type: "sql.NullTime"},
			]
		}
		raw: {
			sql: """
				-- name: ListWebauthnCredentialsByPrincipal :many
				SELECT id, principal_id, credential_id, public_key, sign_count, transports, backup_eligible, backup_state, created_at, last_used_at
				FROM webauthn_credentials
				WHERE principal_id = ?
				ORDER BY created_at DESC
				"""
			bind: ["principalID"]
		}
	},
	{
		name: "MarkAuthRecoveryCodeUsed"
		kind: "exec"
		paramMode: "single"
		params: [
			{name: "id", type: "string"},
		]
		raw: {
			sql: """
				-- name: MarkAuthRecoveryCodeUsed :exec
				UPDATE auth_recovery_codes
				SET used_at = CURRENT_TIMESTAMP
				WHERE id = ?
				"""
			bind: ["id"]
		}
	},
	{
		name: "PurgeLineageOlderThan"
		kind: "execrows"
		paramMode: "single"
		params: [
			{name: "createdAt", type: "string"},
		]
		raw: {
			sql: """
				-- name: PurgeLineageOlderThan :execrows
				DELETE FROM lineage_edges WHERE created_at < ?
				"""
			bind: ["createdAt"]
		}
	},
	{
		name: "RemoveGroupMember"
		kind: "exec"
		paramMode: "struct"
		params: [
			{name: "GroupID", type: "string"},
			{name: "MemberType", type: "string"},
			{name: "MemberID", type: "string"},
		]
		raw: {
			sql: """
				-- name: RemoveGroupMember :exec
				DELETE FROM group_members
				WHERE group_id = ? AND member_type = ? AND member_id = ?
				"""
			bind: ["GroupID", "MemberType", "MemberID"]
		}
	},
	{
		name: "ResolveEndpointForPrincipalByName"
		kind: "one"
		paramMode: "single"
		params: [
			{name: "name", type: "string"},
		]
		result: {
			row: "ComputeEndpoint"
			fields: [
				{name: "ID", type: "string"},
				{name: "ExternalID", type: "string"},
				{name: "Name", type: "string"},
				{name: "Url", type: "string"},
				{name: "Type", type: "string"},
				{name: "Status", type: "string"},
				{name: "Size", type: "string"},
				{name: "MaxMemoryGb", type: "sql.NullInt64"},
				{name: "AuthToken", type: "string"},
				{name: "Owner", type: "string"},
				{name: "CreatedAt", type: "time.Time"},
				{name: "UpdatedAt", type: "time.Time"},
				{name: "SelectionPolicy", type: "string"},
				{name: "WorkloadClass", type: "string"},
				{name: "ReadinessStatus", type: "string"},
				{name: "MaxConcurrency", type: "sql.NullInt64"},
				{name: "MaxResultSizeMb", type: "sql.NullInt64"},
				{name: "RecommendedForLargeQueries", type: "int64"},
				{name: "IsDraining", type: "int64"},
				{name: "LastHealthStatus", type: "sql.NullString"},
				{name: "LastHealthCheckedAt", type: "sql.NullTime"},
				{name: "ActiveQueries", type: "sql.NullInt64"},
				{name: "QueuedJobs", type: "sql.NullInt64"},
				{name: "RunningJobs", type: "sql.NullInt64"},
				{name: "CompletedJobs", type: "sql.NullInt64"},
				{name: "StoredJobs", type: "sql.NullInt64"},
				{name: "CleanedJobs", type: "sql.NullInt64"},
				{name: "QueryResultTtlSeconds", type: "sql.NullInt64"},
			]
		}
		raw: {
			sql: """
				-- name: ResolveEndpointForPrincipalByName :one
				SELECT ce.id, ce.external_id, ce.name, ce.url, ce.type, ce.status, ce.size, ce.max_memory_gb, ce.auth_token, ce.owner, ce.created_at, ce.updated_at, ce.selection_policy, ce.workload_class, ce.readiness_status, ce.max_concurrency, ce.max_result_size_mb, ce.recommended_for_large_queries, ce.is_draining, ce.last_health_status, ce.last_health_checked_at, ce.active_queries, ce.queued_jobs, ce.running_jobs, ce.completed_jobs, ce.stored_jobs, ce.cleaned_jobs, ce.query_result_ttl_seconds
				FROM compute_endpoints ce
				JOIN compute_assignments ca ON ca.endpoint_id = ce.id
				JOIN principals p ON p.id = ca.principal_id AND ca.principal_type = 'user'
				WHERE p.name = ?
				  AND ca.is_default = 1
				  AND ce.status = 'ACTIVE'
				LIMIT 1
				"""
			bind: ["name"]
		}
	},
	{
		name: "RevokePrivilege"
		kind: "exec"
		paramMode: "struct"
		params: [
			{name: "PrincipalID", type: "string"},
			{name: "PrincipalType", type: "string"},
			{name: "SecurableType", type: "string"},
			{name: "SecurableID", type: "string"},
			{name: "Privilege", type: "string"},
		]
		raw: {
			sql: """
				-- name: RevokePrivilege :exec
				DELETE FROM privilege_grants
				WHERE principal_id = ? AND principal_type = ? AND securable_type = ? AND securable_id = ? AND privilege = ?
				"""
			bind: ["PrincipalID", "PrincipalType", "SecurableType", "SecurableID", "Privilege"]
		}
	},
	{
		name: "RevokePrivilegeByID"
		kind: "exec"
		paramMode: "single"
		params: [
			{name: "id", type: "string"},
		]
		raw: {
			sql: """
				-- name: RevokePrivilegeByID :exec
				DELETE FROM privilege_grants WHERE id = ?
				"""
			bind: ["id"]
		}
	},
	{
		name: "RevokeWebSession"
		kind: "exec"
		paramMode: "single"
		params: [
			{name: "id", type: "string"},
		]
		raw: {
			sql: """
				-- name: RevokeWebSession :exec
				UPDATE web_sessions
				SET revoked_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP
				WHERE id = ?
				"""
			bind: ["id"]
		}
	},
	{
		name: "RevokeWebSessionByHash"
		kind: "exec"
		paramMode: "single"
		params: [
			{name: "sessionHash", type: "string"},
		]
		raw: {
			sql: """
				-- name: RevokeWebSessionByHash :exec
				UPDATE web_sessions
				SET revoked_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP
				WHERE session_hash = ?
				"""
			bind: ["sessionHash"]
		}
	},
	{
		name: "RevokeWebSessionsByPrincipal"
		kind: "exec"
		paramMode: "single"
		params: [
			{name: "principalID", type: "string"},
		]
		raw: {
			sql: """
				-- name: RevokeWebSessionsByPrincipal :exec
				UPDATE web_sessions
				SET revoked_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP
				WHERE principal_id = ?
				  AND revoked_at IS NULL
				"""
			bind: ["principalID"]
		}
	},
	{
		name: "SetAdmin"
		kind: "exec"
		paramMode: "struct"
		params: [
			{name: "IsAdmin", type: "int64"},
			{name: "ID", type: "string"},
		]
		raw: {
			sql: """
				-- name: SetAdmin :exec
				UPDATE principals SET is_admin = ? WHERE id = ?
				"""
			bind: ["IsAdmin", "ID"]
		}
	},
	{
		name: "SetDefaultCatalog"
		kind: "exec"
		paramMode: "single"
		params: [
			{name: "id", type: "string"},
		]
		raw: {
			sql: """
				-- name: SetDefaultCatalog :exec
				UPDATE catalogs SET is_default = 1, updated_at = datetime('now') WHERE id = ?
				"""
			bind: ["id"]
		}
	},
	{
		name: "SetSetupBootstrapToken"
		kind: "exec"
		paramMode: "struct"
		params: [
			{name: "BootstrapTokenHash", type: "sql.NullString"},
			{name: "BootstrapTokenExpiresAt", type: "sql.NullTime"},
		]
		raw: {
			sql: """
				-- name: SetSetupBootstrapToken :exec
				UPDATE setup_state
				SET bootstrap_token_hash = ?,
				    bootstrap_token_expires_at = ?,
				    updated_at = CURRENT_TIMESTAMP
				WHERE id = 1
				"""
			bind: ["BootstrapTokenHash", "BootstrapTokenExpiresAt"]
		}
	},
	{
		name: "SoftDeleteCatalogMetadata"
		kind: "exec"
		paramMode: "struct"
		params: [
			{name: "SecurableType", type: "string"},
			{name: "SecurableName", type: "string"},
		]
		raw: {
			sql: """
				-- name: SoftDeleteCatalogMetadata :exec
				UPDATE catalog_metadata SET deleted_at = datetime('now')
				WHERE securable_type = ? AND securable_name = ?
				"""
			bind: ["SecurableType", "SecurableName"]
		}
	},
	{
		name: "SoftDeleteCatalogMetadataByPattern"
		kind: "exec"
		paramMode: "struct"
		params: [
			{name: "SecurableType", type: "string"},
			{name: "SecurableName", type: "string"},
		]
		raw: {
			sql: """
				-- name: SoftDeleteCatalogMetadataByPattern :exec
				UPDATE catalog_metadata SET deleted_at = datetime('now')
				WHERE securable_type = ? AND securable_name LIKE ?
				"""
			bind: ["SecurableType", "SecurableName"]
		}
	},
	{
		name: "SoftDeleteExternalTable"
		kind: "exec"
		paramMode: "struct"
		params: [
			{name: "SchemaName", type: "string"},
			{name: "TableName", type: "string"},
		]
		raw: {
			sql: """
				-- name: SoftDeleteExternalTable :exec
				UPDATE external_tables SET deleted_at = datetime('now')
				WHERE schema_name = ? AND table_name = ? AND deleted_at IS NULL
				"""
			bind: ["SchemaName", "TableName"]
		}
	},
	{
		name: "SoftDeleteExternalTablesBySchema"
		kind: "exec"
		paramMode: "single"
		params: [
			{name: "schemaName", type: "string"},
		]
		raw: {
			sql: """
				-- name: SoftDeleteExternalTablesBySchema :exec
				UPDATE external_tables SET deleted_at = datetime('now')
				WHERE schema_name = ? AND deleted_at IS NULL
				"""
			bind: ["schemaName"]
		}
	},
	{
		name: "TouchWebSession"
		kind: "exec"
		paramMode: "struct"
		params: [
			{name: "IdleExpiresAt", type: "time.Time"},
			{name: "ID", type: "string"},
		]
		raw: {
			sql: """
				-- name: TouchWebSession :exec
				UPDATE web_sessions
				SET idle_expires_at = ?, last_seen_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP
				WHERE id = ?
				"""
			bind: ["IdleExpiresAt", "ID"]
		}
	},
	{
		name: "UnbindColumnMask"
		kind: "exec"
		paramMode: "struct"
		params: [
			{name: "ColumnMaskID", type: "string"},
			{name: "PrincipalID", type: "string"},
			{name: "PrincipalType", type: "string"},
		]
		raw: {
			sql: """
				-- name: UnbindColumnMask :exec
				DELETE FROM column_mask_bindings
				WHERE column_mask_id = ? AND principal_id = ? AND principal_type = ?
				"""
			bind: ["ColumnMaskID", "PrincipalID", "PrincipalType"]
		}
	},
	{
		name: "UnbindRowFilter"
		kind: "execresult"
		paramMode: "struct"
		params: [
			{name: "RowFilterID", type: "string"},
			{name: "PrincipalID", type: "string"},
			{name: "PrincipalType", type: "string"},
		]
		raw: {
			sql: """
				-- name: UnbindRowFilter :execresult
				DELETE FROM row_filter_bindings
				WHERE row_filter_id = ? AND principal_id = ? AND principal_type = ?
				"""
			bind: ["RowFilterID", "PrincipalID", "PrincipalType"]
		}
	},
	{
		name: "UpdateCatalog"
		kind: "one"
		paramMode: "struct"
		params: [
			{name: "Comment", type: "sql.NullString"},
			{name: "DataPath", type: "string"},
			{name: "Dsn", type: "string"},
			{name: "ID", type: "string"},
		]
		result: {
			row: "Catalog"
			fields: [
				{name: "ID", type: "string"},
				{name: "Name", type: "string"},
				{name: "MetastoreType", type: "string"},
				{name: "Dsn", type: "string"},
				{name: "DataPath", type: "string"},
				{name: "Status", type: "string"},
				{name: "StatusMessage", type: "sql.NullString"},
				{name: "IsDefault", type: "int64"},
				{name: "Comment", type: "sql.NullString"},
				{name: "CreatedAt", type: "string"},
				{name: "UpdatedAt", type: "string"},
			]
		}
		raw: {
			sql: """
				-- name: UpdateCatalog :one
				UPDATE catalogs
				SET comment = COALESCE(?, comment),
				    data_path = COALESCE(?, data_path),
				    dsn = COALESCE(?, dsn),
				    updated_at = datetime('now')
				WHERE id = ?
				RETURNING id, name, metastore_type, dsn, data_path, status, status_message, is_default, comment, created_at, updated_at
				"""
			bind: ["Comment", "DataPath", "Dsn", "ID"]
		}
	},
	{
		name: "UpdateCatalogStatus"
		kind: "exec"
		paramMode: "struct"
		params: [
			{name: "Status", type: "string"},
			{name: "StatusMessage", type: "sql.NullString"},
			{name: "ID", type: "string"},
		]
		raw: {
			sql: """
				-- name: UpdateCatalogStatus :exec
				UPDATE catalogs
				SET status = ?, status_message = ?, updated_at = datetime('now')
				WHERE id = ?
				"""
			bind: ["Status", "StatusMessage", "ID"]
		}
	},
	{
		name: "UpdateCell"
		kind: "one"
		paramMode: "struct"
		params: [
			{name: "Name", type: "sql.NullString"},
			{name: "Role", type: "string"},
			{name: "Disabled", type: "int64"},
			{name: "TestConfig", type: "string"},
			{name: "VisualSpec", type: "string"},
			{name: "Content", type: "string"},
			{name: "Position", type: "int64"},
			{name: "ID", type: "string"},
		]
		result: {
			row: "Cell"
			fields: [
				{name: "ID", type: "string"},
				{name: "NotebookID", type: "string"},
				{name: "CellType", type: "string"},
				{name: "Content", type: "string"},
				{name: "Position", type: "int64"},
				{name: "LastResult", type: "sql.NullString"},
				{name: "CreatedAt", type: "string"},
				{name: "UpdatedAt", type: "string"},
				{name: "Name", type: "sql.NullString"},
				{name: "Role", type: "string"},
				{name: "Disabled", type: "int64"},
				{name: "TestConfig", type: "string"},
				{name: "VisualSpec", type: "string"},
			]
		}
		raw: {
			sql: """
				-- name: UpdateCell :one
				UPDATE cells
				SET name = ?, role = ?, disabled = ?, test_config = ?, visual_spec = ?, content = ?, position = ?, updated_at = datetime('now')
				WHERE id = ?
				RETURNING id, notebook_id, cell_type, content, position, last_result, created_at, updated_at, name, role, disabled, test_config, visual_spec
				"""
			bind: ["Name", "Role", "Disabled", "TestConfig", "VisualSpec", "Content", "Position", "ID"]
		}
	},
	{
		name: "UpdateCellPosition"
		kind: "exec"
		paramMode: "struct"
		params: [
			{name: "Position", type: "int64"},
			{name: "ID", type: "string"},
		]
		raw: {
			sql: """
				-- name: UpdateCellPosition :exec
				UPDATE cells SET position = ?, updated_at = datetime('now') WHERE id = ?
				"""
			bind: ["Position", "ID"]
		}
	},
	{
		name: "UpdateCellResult"
		kind: "exec"
		paramMode: "struct"
		params: [
			{name: "LastResult", type: "sql.NullString"},
			{name: "ID", type: "string"},
		]
		raw: {
			sql: """
				-- name: UpdateCellResult :exec
				UPDATE cells SET last_result = ?, updated_at = datetime('now') WHERE id = ?
				"""
			bind: ["LastResult", "ID"]
		}
	},
	{
		name: "UpdateComputeEndpoint"
		kind: "exec"
		paramMode: "struct"
		params: [
			{name: "Url", type: "string"},
			{name: "Size", type: "string"},
			{name: "MaxMemoryGb", type: "sql.NullInt64"},
			{name: "MaxConcurrency", type: "sql.NullInt64"},
			{name: "MaxResultSizeMb", type: "sql.NullInt64"},
			{name: "SelectionPolicy", type: "string"},
			{name: "WorkloadClass", type: "string"},
			{name: "ReadinessStatus", type: "string"},
			{name: "RecommendedForLargeQueries", type: "int64"},
			{name: "IsDraining", type: "int64"},
			{name: "AuthToken", type: "string"},
			{name: "ID", type: "string"},
		]
		raw: {
			sql: """
				-- name: UpdateComputeEndpoint :exec
				UPDATE compute_endpoints
				SET url = ?,
				    size = ?,
				    max_memory_gb = ?,
				    max_concurrency = ?,
				    max_result_size_mb = ?,
				    selection_policy = ?,
				    workload_class = ?,
				    readiness_status = ?,
				    recommended_for_large_queries = ?,
				    is_draining = ?,
				    auth_token = ?,
				    updated_at = datetime('now')
				WHERE id = ?
				"""
			bind: ["Url", "Size", "MaxMemoryGb", "MaxConcurrency", "MaxResultSizeMb", "SelectionPolicy", "WorkloadClass", "ReadinessStatus", "RecommendedForLargeQueries", "IsDraining", "AuthToken", "ID"]
		}
	},
	{
		name: "UpdateComputeEndpointHealth"
		kind: "exec"
		paramMode: "struct"
		params: [
			{name: "LastHealthStatus", type: "sql.NullString"},
			{name: "ActiveQueries", type: "sql.NullInt64"},
			{name: "QueuedJobs", type: "sql.NullInt64"},
			{name: "RunningJobs", type: "sql.NullInt64"},
			{name: "CompletedJobs", type: "sql.NullInt64"},
			{name: "StoredJobs", type: "sql.NullInt64"},
			{name: "CleanedJobs", type: "sql.NullInt64"},
			{name: "QueryResultTtlSeconds", type: "sql.NullInt64"},
			{name: "ReadinessStatus", type: "string"},
			{name: "ID", type: "string"},
		]
		raw: {
			sql: """
				-- name: UpdateComputeEndpointHealth :exec
				UPDATE compute_endpoints
				SET last_health_status = ?,
				    last_health_checked_at = datetime('now'),
				    active_queries = ?,
				    queued_jobs = ?,
				    running_jobs = ?,
				    completed_jobs = ?,
				    stored_jobs = ?,
				    cleaned_jobs = ?,
				    query_result_ttl_seconds = ?,
				    readiness_status = ?,
				    updated_at = datetime('now')
				WHERE id = ?
				"""
			bind: ["LastHealthStatus", "ActiveQueries", "QueuedJobs", "RunningJobs", "CompletedJobs", "StoredJobs", "CleanedJobs", "QueryResultTtlSeconds", "ReadinessStatus", "ID"]
		}
	},
	{
		name: "UpdateComputeEndpointStatus"
		kind: "exec"
		paramMode: "struct"
		params: [
			{name: "Status", type: "string"},
			{name: "ID", type: "string"},
		]
		raw: {
			sql: """
				-- name: UpdateComputeEndpointStatus :exec
				UPDATE compute_endpoints
				SET status = ?,
				    updated_at = datetime('now')
				WHERE id = ?
				"""
			bind: ["Status", "ID"]
		}
	},
	{
		name: "UpdateDashboard"
		kind: "one"
		paramMode: "struct"
		params: [
			{name: "Name", type: "string"},
			{name: "Description", type: "string"},
			{name: "Owner", type: "string"},
			{name: "FolderID", type: "sql.NullString"},
			{name: "SemanticProjectName", type: "string"},
			{name: "SemanticModelName", type: "string"},
			{name: "ComputeMode", type: "string"},
			{name: "ComputeEndpointName", type: "string"},
			{name: "ComputeFallbackLocal", type: "int64"},
			{name: "ID", type: "string"},
		]
		result: {
			row: "Dashboard"
			fields: [
				{name: "ID", type: "string"},
				{name: "Name", type: "string"},
				{name: "Description", type: "string"},
				{name: "Owner", type: "string"},
				{name: "CreatedAt", type: "string"},
				{name: "UpdatedAt", type: "string"},
				{name: "FolderID", type: "sql.NullString"},
				{name: "SemanticProjectName", type: "string"},
				{name: "SemanticModelName", type: "string"},
				{name: "ComputeMode", type: "string"},
				{name: "ComputeEndpointName", type: "string"},
				{name: "ComputeFallbackLocal", type: "int64"},
			]
		}
		raw: {
			sql: """
				-- name: UpdateDashboard :one
				UPDATE dashboards
				SET name = ?,
				    description = ?,
				    owner = ?,
				    folder_id = ?,
				    semantic_project_name = ?,
				    semantic_model_name = ?,
				    compute_mode = ?,
				    compute_endpoint_name = ?,
				    compute_fallback_local = ?,
				    updated_at = datetime('now')
				WHERE id = ?
				RETURNING id, name, description, owner, created_at, updated_at, folder_id, semantic_project_name, semantic_model_name, compute_mode, compute_endpoint_name, compute_fallback_local
				"""
			bind: ["Name", "Description", "Owner", "FolderID", "SemanticProjectName", "SemanticModelName", "ComputeMode", "ComputeEndpointName", "ComputeFallbackLocal", "ID"]
		}
	},
	{
		name: "UpdateDashboardWidget"
		kind: "one"
		paramMode: "struct"
		params: [
			{name: "FilterOriginKey", type: "string"},
			{name: "PageName", type: "string"},
			{name: "Name", type: "string"},
			{name: "Description", type: "string"},
			{name: "SourceJson", type: "string"},
			{name: "VisualSpec", type: "string"},
			{name: "LayoutX", type: "int64"},
			{name: "LayoutY", type: "int64"},
			{name: "LayoutW", type: "int64"},
			{name: "LayoutH", type: "int64"},
			{name: "ID", type: "string"},
		]
		result: {
			row: "DashboardWidget"
			fields: [
				{name: "ID", type: "string"},
				{name: "DashboardID", type: "string"},
				{name: "Name", type: "string"},
				{name: "Description", type: "string"},
				{name: "SourceJson", type: "string"},
				{name: "VisualSpec", type: "string"},
				{name: "LayoutX", type: "int64"},
				{name: "LayoutY", type: "int64"},
				{name: "LayoutW", type: "int64"},
				{name: "LayoutH", type: "int64"},
				{name: "CreatedAt", type: "string"},
				{name: "UpdatedAt", type: "string"},
				{name: "FilterOriginKey", type: "string"},
				{name: "PageName", type: "string"},
			]
		}
		raw: {
			sql: """
				-- name: UpdateDashboardWidget :one
				UPDATE dashboard_widgets
				SET filter_origin_key = ?,
				    page_name = ?,
				    name = ?,
				    description = ?,
				    source_json = ?,
				    visual_spec = ?,
				    layout_x = ?,
				    layout_y = ?,
				    layout_w = ?,
				    layout_h = ?,
				    updated_at = datetime('now')
				WHERE id = ?
				RETURNING id, dashboard_id, name, description, source_json, visual_spec, layout_x, layout_y, layout_w, layout_h, created_at, updated_at, filter_origin_key, page_name
				"""
			bind: ["FilterOriginKey", "PageName", "Name", "Description", "SourceJson", "VisualSpec", "LayoutX", "LayoutY", "LayoutW", "LayoutH", "ID"]
		}
	},
	{
		name: "UpdateExternalLocation"
		kind: "exec"
		paramMode: "struct"
		params: [
			{name: "Url", type: "string"},
			{name: "CredentialName", type: "string"},
			{name: "Comment", type: "string"},
			{name: "Owner", type: "string"},
			{name: "ReadOnly", type: "int64"},
			{name: "ID", type: "string"},
		]
		raw: {
			sql: """
				-- name: UpdateExternalLocation :exec
				UPDATE external_locations
				SET url = COALESCE(?, url),
				    credential_name = COALESCE(?, credential_name),
				    comment = COALESCE(?, comment),
				    owner = COALESCE(?, owner),
				    read_only = COALESCE(?, read_only),
				    updated_at = datetime('now')
				WHERE id = ?
				"""
			bind: ["Url", "CredentialName", "Comment", "Owner", "ReadOnly", "ID"]
		}
	},
	{
		name: "UpdateExternalTable"
		kind: "exec"
		paramMode: "struct"
		params: [
			{name: "SetComment", type: "interface{}"},
			{name: "Comment", type: "string"},
			{name: "SetOwner", type: "interface{}"},
			{name: "Owner", type: "string"},
			{name: "SchemaName", type: "string"},
			{name: "TableName", type: "string"},
		]
		raw: {
			sql: """
				-- name: UpdateExternalTable :exec
				UPDATE external_tables
				SET comment = CASE WHEN ?1 = 1 THEN ?2 ELSE comment END,
				    owner = CASE WHEN ?3 = 1 THEN ?4 ELSE owner END,
				    updated_at = datetime('now')
				WHERE schema_name = ?5 AND table_name = ?6 AND deleted_at IS NULL
				"""
			bind: ["SetComment", "Comment", "SetOwner", "Owner", "SchemaName", "TableName"]
		}
	},
	{
		name: "UpdateGitRepoSyncStatus"
		kind: "exec"
		paramMode: "struct"
		params: [
			{name: "LastCommit", type: "sql.NullString"},
			{name: "LastSyncAt", type: "sql.NullString"},
			{name: "ID", type: "string"},
		]
		raw: {
			sql: """
				-- name: UpdateGitRepoSyncStatus :exec
				UPDATE git_repos
				SET last_commit = ?, last_sync_at = ?, updated_at = datetime('now')
				WHERE id = ?
				"""
			bind: ["LastCommit", "LastSyncAt", "ID"]
		}
	},
	{
		name: "UpdateGroup"
		kind: "one"
		paramMode: "struct"
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
		raw: {
			sql: """
				-- name: UpdateGroup :one
				UPDATE groups
				SET description = ?
				WHERE id = ?
				RETURNING id, name, description, created_at
				"""
			bind: ["Description", "ID"]
		}
	},
	{
		name: "UpdateMacro"
		kind: "exec"
		paramMode: "struct"
		params: [
			{name: "Body", type: "string"},
			{name: "Description", type: "string"},
			{name: "Parameters", type: "string"},
			{name: "Status", type: "string"},
			{name: "CatalogName", type: "string"},
			{name: "ProjectName", type: "string"},
			{name: "Visibility", type: "string"},
			{name: "Owner", type: "string"},
			{name: "Properties", type: "string"},
			{name: "Tags", type: "string"},
			{name: "Name", type: "string"},
		]
		raw: {
			sql: """
				-- name: UpdateMacro :exec
				UPDATE macros
				SET body = ?,
				    description = ?,
				    parameters = ?,
				    status = ?,
				    catalog_name = ?,
				    project_name = ?,
				    visibility = ?,
				    owner = ?,
				    properties = ?,
				    tags = ?,
				    updated_at = datetime('now')
				WHERE name = ?
				"""
			bind: ["Body", "Description", "Parameters", "Status", "CatalogName", "ProjectName", "Visibility", "Owner", "Properties", "Tags", "Name"]
		}
	},
	{
		name: "UpdateModel"
		kind: "exec"
		paramMode: "struct"
		params: [
			{name: "SqlBody", type: "string"},
			{name: "Materialization", type: "string"},
			{name: "Description", type: "string"},
			{name: "Tags", type: "string"},
			{name: "Config", type: "string"},
			{name: "Contract", type: "string"},
			{name: "FreshnessMaxLag", type: "sql.NullInt64"},
			{name: "FreshnessCron", type: "sql.NullString"},
			{name: "ID", type: "string"},
		]
		raw: {
			sql: """
				-- name: UpdateModel :exec
				UPDATE models
				SET sql_body = COALESCE(?, sql_body),
				    materialization = COALESCE(?, materialization),
				    description = COALESCE(?, description),
				    tags = COALESCE(?, tags),
				    config = COALESCE(?, config),
				    contract = COALESCE(?, contract),
				    freshness_max_lag = ?,
				    freshness_cron = ?,
				    updated_at = datetime('now')
				WHERE id = ?
				"""
			bind: ["SqlBody", "Materialization", "Description", "Tags", "Config", "Contract", "FreshnessMaxLag", "FreshnessCron", "ID"]
		}
	},
	{
		name: "UpdateModelDependencies"
		kind: "exec"
		paramMode: "struct"
		params: [
			{name: "DependsOn", type: "string"},
			{name: "ID", type: "string"},
		]
		raw: {
			sql: """
				-- name: UpdateModelDependencies :exec
				UPDATE models SET depends_on = ?, updated_at = datetime('now') WHERE id = ?
				"""
			bind: ["DependsOn", "ID"]
		}
	},
	{
		name: "UpdateModelRunBuild"
		kind: "exec"
		paramMode: "struct"
		params: [
			{name: "BuildID", type: "sql.NullString"},
			{name: "ID", type: "string"},
		]
		raw: {
			sql: """
				-- name: UpdateModelRunBuild :exec
				UPDATE model_runs SET build_id = ? WHERE id = ?
				"""
			bind: ["BuildID", "ID"]
		}
	},
	{
		name: "UpdateModelRunFinished"
		kind: "exec"
		paramMode: "struct"
		params: [
			{name: "Status", type: "string"},
			{name: "ErrorMessage", type: "sql.NullString"},
			{name: "ID", type: "string"},
		]
		raw: {
			sql: """
				-- name: UpdateModelRunFinished :exec
				UPDATE model_runs SET status = ?, finished_at = datetime('now'), error_message = ? WHERE id = ?
				"""
			bind: ["Status", "ErrorMessage", "ID"]
		}
	},
	{
		name: "UpdateModelRunStarted"
		kind: "exec"
		paramMode: "single"
		params: [
			{name: "id", type: "string"},
		]
		raw: {
			sql: """
				-- name: UpdateModelRunStarted :exec
				UPDATE model_runs SET status = 'RUNNING', started_at = datetime('now') WHERE id = ?
				"""
			bind: ["id"]
		}
	},
	{
		name: "UpdateModelRunStepFinished"
		kind: "exec"
		paramMode: "struct"
		params: [
			{name: "Status", type: "string"},
			{name: "RowsAffected", type: "sql.NullInt64"},
			{name: "ErrorMessage", type: "sql.NullString"},
			{name: "ID", type: "string"},
		]
		raw: {
			sql: """
				-- name: UpdateModelRunStepFinished :exec
				UPDATE model_run_steps SET status = ?, finished_at = datetime('now'), rows_affected = ?, error_message = ? WHERE id = ?
				"""
			bind: ["Status", "RowsAffected", "ErrorMessage", "ID"]
		}
	},
	{
		name: "UpdateModelRunStepStarted"
		kind: "exec"
		paramMode: "single"
		params: [
			{name: "id", type: "string"},
		]
		raw: {
			sql: """
				-- name: UpdateModelRunStepStarted :exec
				UPDATE model_run_steps SET status = 'RUNNING', started_at = datetime('now') WHERE id = ?
				"""
			bind: ["id"]
		}
	},
	{
		name: "UpdateNotebook"
		kind: "one"
		paramMode: "struct"
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
		raw: {
			sql: """
				-- name: UpdateNotebook :one
				UPDATE notebooks
				SET name = ?, description = ?, updated_at = datetime('now')
				WHERE id = ?
				RETURNING id, name, description, owner, created_at, updated_at, git_repo_id, git_path, folder_id, project_override_id, environment_override_id
				"""
			bind: ["Name", "Description", "ID"]
		}
	},
	{
		name: "UpdateNotebookJobState"
		kind: "exec"
		paramMode: "struct"
		params: [
			{name: "State", type: "string"},
			{name: "Result", type: "sql.NullString"},
			{name: "Error", type: "sql.NullString"},
			{name: "ID", type: "string"},
		]
		raw: {
			sql: """
				-- name: UpdateNotebookJobState :exec
				UPDATE notebook_jobs
				SET state = ?, result = ?, error = ?, updated_at = datetime('now')
				WHERE id = ?
				"""
			bind: ["State", "Result", "Error", "ID"]
		}
	},
	{
		name: "UpdatePipeline"
		kind: "exec"
		paramMode: "struct"
		params: [
			{name: "Description", type: "string"},
			{name: "ScheduleCron", type: "sql.NullString"},
			{name: "IsPaused", type: "int64"},
			{name: "ConcurrencyLimit", type: "int64"},
			{name: "FolderID", type: "sql.NullString"},
			{name: "ID", type: "string"},
		]
		raw: {
			sql: """
				-- name: UpdatePipeline :exec
				UPDATE pipelines
				SET description = COALESCE(?, description),
				    schedule_cron = COALESCE(?, schedule_cron),
				    is_paused = COALESCE(?, is_paused),
				    concurrency_limit = COALESCE(?, concurrency_limit),
				    folder_id = COALESCE(?, folder_id),
				    updated_at = datetime('now')
				WHERE id = ?
				"""
			bind: ["Description", "ScheduleCron", "IsPaused", "ConcurrencyLimit", "FolderID", "ID"]
		}
	},
	{
		name: "UpdatePipelineJobRunFinished"
		kind: "exec"
		paramMode: "struct"
		params: [
			{name: "Status", type: "string"},
			{name: "ErrorMessage", type: "sql.NullString"},
			{name: "ID", type: "string"},
		]
		raw: {
			sql: """
				-- name: UpdatePipelineJobRunFinished :exec
				UPDATE pipeline_job_runs SET status = ?, finished_at = datetime('now'), error_message = ? WHERE id = ?
				"""
			bind: ["Status", "ErrorMessage", "ID"]
		}
	},
	{
		name: "UpdatePipelineJobRunStarted"
		kind: "exec"
		paramMode: "single"
		params: [
			{name: "id", type: "string"},
		]
		raw: {
			sql: """
				-- name: UpdatePipelineJobRunStarted :exec
				UPDATE pipeline_job_runs SET status = 'RUNNING', started_at = datetime('now') WHERE id = ?
				"""
			bind: ["id"]
		}
	},
	{
		name: "UpdatePipelineJobRunStatus"
		kind: "exec"
		paramMode: "struct"
		params: [
			{name: "Status", type: "string"},
			{name: "ErrorMessage", type: "sql.NullString"},
			{name: "ID", type: "string"},
		]
		raw: {
			sql: """
				-- name: UpdatePipelineJobRunStatus :exec
				UPDATE pipeline_job_runs SET status = ?, error_message = ? WHERE id = ?
				"""
			bind: ["Status", "ErrorMessage", "ID"]
		}
	},
	{
		name: "UpdatePipelineRunFinished"
		kind: "exec"
		paramMode: "struct"
		params: [
			{name: "Status", type: "string"},
			{name: "ErrorMessage", type: "sql.NullString"},
			{name: "ID", type: "string"},
		]
		raw: {
			sql: """
				-- name: UpdatePipelineRunFinished :exec
				UPDATE pipeline_runs SET status = ?, finished_at = datetime('now'), error_message = ? WHERE id = ?
				"""
			bind: ["Status", "ErrorMessage", "ID"]
		}
	},
	{
		name: "UpdatePipelineRunStarted"
		kind: "exec"
		paramMode: "single"
		params: [
			{name: "id", type: "string"},
		]
		raw: {
			sql: """
				-- name: UpdatePipelineRunStarted :exec
				UPDATE pipeline_runs SET status = 'RUNNING', started_at = datetime('now') WHERE id = ?
				"""
			bind: ["id"]
		}
	},
	{
		name: "UpdatePipelineRunStatus"
		kind: "exec"
		paramMode: "struct"
		params: [
			{name: "Status", type: "string"},
			{name: "ErrorMessage", type: "sql.NullString"},
			{name: "ID", type: "string"},
		]
		raw: {
			sql: """
				-- name: UpdatePipelineRunStatus :exec
				UPDATE pipeline_runs SET status = ?, error_message = ? WHERE id = ?
				"""
			bind: ["Status", "ErrorMessage", "ID"]
		}
	},
	{
		name: "UpdateSemanticMetric"
		kind: "exec"
		paramMode: "struct"
		params: [
			{name: "Description", type: "string"},
			{name: "Label", type: "string"},
			{name: "MetricType", type: "string"},
			{name: "ExpressionMode", type: "string"},
			{name: "Expression", type: "string"},
			{name: "RelationshipNames", type: "string"},
			{name: "FilterSql", type: "string"},
			{name: "DefaultTimeGrain", type: "string"},
			{name: "Format", type: "string"},
			{name: "Owner", type: "string"},
			{name: "CertificationState", type: "string"},
			{name: "ID", type: "string"},
		]
		raw: {
			sql: """
				-- name: UpdateSemanticMetric :exec
				UPDATE semantic_metrics
				SET description = COALESCE(?, description),
				    label = COALESCE(?, label),
				    metric_type = COALESCE(?, metric_type),
				    expression_mode = COALESCE(?, expression_mode),
				    expression = COALESCE(?, expression),
				    relationship_names = COALESCE(?, relationship_names),
				    filter_sql = COALESCE(?, filter_sql),
				    default_time_grain = COALESCE(?, default_time_grain),
				    format = COALESCE(?, format),
				    owner = COALESCE(?, owner),
				    certification_state = COALESCE(?, certification_state),
				    updated_at = datetime('now')
				WHERE id = ?
				"""
			bind: ["Description", "Label", "MetricType", "ExpressionMode", "Expression", "RelationshipNames", "FilterSql", "DefaultTimeGrain", "Format", "Owner", "CertificationState", "ID"]
		}
	},
	{
		name: "UpdateSemanticModel"
		kind: "exec"
		paramMode: "struct"
		params: [
			{name: "Description", type: "string"},
			{name: "Owner", type: "string"},
			{name: "BaseModelRef", type: "string"},
			{name: "DefaultTimeDimension", type: "string"},
			{name: "Tags", type: "string"},
			{name: "ID", type: "string"},
		]
		raw: {
			sql: """
				-- name: UpdateSemanticModel :exec
				UPDATE semantic_models
				SET description = COALESCE(?, description),
				    owner = COALESCE(?, owner),
				    base_model_ref = COALESCE(?, base_model_ref),
				    default_time_dimension = COALESCE(?, default_time_dimension),
				    tags = COALESCE(?, tags),
				    updated_at = datetime('now')
				WHERE id = ?
				"""
			bind: ["Description", "Owner", "BaseModelRef", "DefaultTimeDimension", "Tags", "ID"]
		}
	},
	{
		name: "UpdateSemanticPreAggregation"
		kind: "exec"
		paramMode: "struct"
		params: [
			{name: "MetricSet", type: "string"},
			{name: "DimensionSet", type: "string"},
			{name: "Grain", type: "string"},
			{name: "TargetRelation", type: "string"},
			{name: "RefreshPolicy", type: "string"},
			{name: "ID", type: "string"},
		]
		raw: {
			sql: """
				-- name: UpdateSemanticPreAggregation :exec
				UPDATE semantic_pre_aggregations
				SET metric_set = COALESCE(?, metric_set),
				    dimension_set = COALESCE(?, dimension_set),
				    grain = COALESCE(?, grain),
				    target_relation = COALESCE(?, target_relation),
				    refresh_policy = COALESCE(?, refresh_policy),
				    updated_at = datetime('now')
				WHERE id = ?
				"""
			bind: ["MetricSet", "DimensionSet", "Grain", "TargetRelation", "RefreshPolicy", "ID"]
		}
	},
	{
		name: "UpdateSemanticRelationship"
		kind: "exec"
		paramMode: "struct"
		params: [
			{name: "RelationshipType", type: "string"},
			{name: "JoinSql", type: "string"},
			{name: "Cost", type: "int64"},
			{name: "MaxHops", type: "int64"},
			{name: "ID", type: "string"},
		]
		raw: {
			sql: """
				-- name: UpdateSemanticRelationship :exec
				UPDATE semantic_relationships
				SET relationship_type = COALESCE(?, relationship_type),
				    join_sql = COALESCE(?, join_sql),
				    cost = COALESCE(?, cost),
				    max_hops = COALESCE(?, max_hops),
				    updated_at = datetime('now')
				WHERE id = ?
				"""
			bind: ["RelationshipType", "JoinSql", "Cost", "MaxHops", "ID"]
		}
	},
	{
		name: "UpdateStorageCredential"
		kind: "exec"
		paramMode: "struct"
		params: [
			{name: "KeyIDEncrypted", type: "string"},
			{name: "SecretEncrypted", type: "string"},
			{name: "Endpoint", type: "string"},
			{name: "Region", type: "string"},
			{name: "UrlStyle", type: "string"},
			{name: "AzureAccountName", type: "string"},
			{name: "AzureAccountKeyEncrypted", type: "string"},
			{name: "AzureClientID", type: "string"},
			{name: "AzureTenantID", type: "string"},
			{name: "AzureClientSecretEncrypted", type: "string"},
			{name: "GcsKeyFilePath", type: "string"},
			{name: "Comment", type: "string"},
			{name: "ID", type: "string"},
		]
		raw: {
			sql: """
				-- name: UpdateStorageCredential :exec
				UPDATE storage_credentials
				SET key_id_encrypted = COALESCE(?, key_id_encrypted),
				    secret_encrypted = COALESCE(?, secret_encrypted),
				    endpoint = COALESCE(?, endpoint),
				    region = COALESCE(?, region),
				    url_style = COALESCE(?, url_style),
				    azure_account_name = COALESCE(?, azure_account_name),
				    azure_account_key_encrypted = COALESCE(?, azure_account_key_encrypted),
				    azure_client_id = COALESCE(?, azure_client_id),
				    azure_tenant_id = COALESCE(?, azure_tenant_id),
				    azure_client_secret_encrypted = COALESCE(?, azure_client_secret_encrypted),
				    gcs_key_file_path = COALESCE(?, gcs_key_file_path),
				    comment = COALESCE(?, comment),
				    updated_at = datetime('now')
				WHERE id = ?
				"""
			bind: ["KeyIDEncrypted", "SecretEncrypted", "Endpoint", "Region", "UrlStyle", "AzureAccountName", "AzureAccountKeyEncrypted", "AzureClientID", "AzureTenantID", "AzureClientSecretEncrypted", "GcsKeyFilePath", "Comment", "ID"]
		}
	},
	{
		name: "UpdateView"
		kind: "exec"
		paramMode: "struct"
		params: [
			{name: "Comment", type: "sql.NullString"},
			{name: "Properties", type: "sql.NullString"},
			{name: "ViewDefinition", type: "string"},
			{name: "SourceTables", type: "sql.NullString"},
			{name: "SchemaID", type: "string"},
			{name: "Name", type: "string"},
		]
		raw: {
			sql: """
				-- name: UpdateView :exec
				UPDATE views SET comment = ?, properties = ?, view_definition = ?, source_tables = ?, updated_at = datetime('now')
				WHERE schema_id = ? AND name = ?
				"""
			bind: ["Comment", "Properties", "ViewDefinition", "SourceTables", "SchemaID", "Name"]
		}
	},
	{
		name: "UpdateVolume"
		kind: "exec"
		paramMode: "struct"
		params: [
			{name: "Name", type: "string"},
			{name: "StorageLocation", type: "string"},
			{name: "Comment", type: "string"},
			{name: "Owner", type: "string"},
			{name: "ID", type: "string"},
		]
		raw: {
			sql: """
				-- name: UpdateVolume :exec
				UPDATE volumes
				SET name = COALESCE(?, name),
				    storage_location = COALESCE(?, storage_location),
				    comment = COALESCE(?, comment),
				    owner = COALESCE(?, owner),
				    updated_at = datetime('now')
				WHERE id = ?
				"""
			bind: ["Name", "StorageLocation", "Comment", "Owner", "ID"]
		}
	},
	{
		name: "UpdateWebauthnCredentialCounter"
		kind: "exec"
		paramMode: "struct"
		params: [
			{name: "SignCount", type: "int64"},
			{name: "CredentialID", type: "string"},
		]
		raw: {
			sql: """
				-- name: UpdateWebauthnCredentialCounter :exec
				UPDATE webauthn_credentials
				SET sign_count = ?, last_used_at = CURRENT_TIMESTAMP
				WHERE credential_id = ?
				"""
			bind: ["SignCount", "CredentialID"]
		}
	},
	{
		name: "UpsertAuthProviderConfig"
		kind: "exec"
		paramMode: "struct"
		params: [
			{name: "OidcEnabled", type: "int64"},
			{name: "OidcIssuerUrl", type: "sql.NullString"},
			{name: "OidcJwksUrl", type: "sql.NullString"},
			{name: "OidcAudience", type: "sql.NullString"},
			{name: "OidcClientID", type: "sql.NullString"},
			{name: "OidcClientSecretEnc", type: "sql.NullString"},
			{name: "OidcScopes", type: "sql.NullString"},
		]
		raw: {
			sql: """
				-- name: UpsertAuthProviderConfig :exec
				INSERT INTO auth_providers (
				  id, oidc_enabled, oidc_issuer_url, oidc_jwks_url, oidc_audience,
				  oidc_client_id, oidc_client_secret_enc, oidc_scopes, updated_at
				)
				VALUES (1, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
				ON CONFLICT(id) DO UPDATE SET
				  oidc_enabled = excluded.oidc_enabled,
				  oidc_issuer_url = excluded.oidc_issuer_url,
				  oidc_jwks_url = excluded.oidc_jwks_url,
				  oidc_audience = excluded.oidc_audience,
				  oidc_client_id = excluded.oidc_client_id,
				  oidc_client_secret_enc = excluded.oidc_client_secret_enc,
				  oidc_scopes = excluded.oidc_scopes,
				  updated_at = CURRENT_TIMESTAMP
				"""
			bind: ["OidcEnabled", "OidcIssuerUrl", "OidcJwksUrl", "OidcAudience", "OidcClientID", "OidcClientSecretEnc", "OidcScopes"]
		}
	},
	{
		name: "UpsertCatalogMetadata"
		kind: "exec"
		paramMode: "struct"
		params: [
			{name: "SecurableType", type: "string"},
			{name: "SecurableName", type: "string"},
			{name: "Comment", type: "sql.NullString"},
			{name: "Properties", type: "sql.NullString"},
			{name: "Owner", type: "sql.NullString"},
		]
		raw: {
			sql: """
				-- name: UpsertCatalogMetadata :exec
				INSERT INTO catalog_metadata (securable_type, securable_name, comment, properties, owner)
				VALUES (?, ?, ?, ?, ?)
				ON CONFLICT(securable_type, securable_name)
				DO UPDATE SET comment = COALESCE(excluded.comment, comment),
				              properties = COALESCE(excluded.properties, properties),
				              owner = COALESCE(excluded.owner, owner),
				              deleted_at = NULL,
				              updated_at = datetime('now')
				"""
			bind: ["SecurableType", "SecurableName", "Comment", "Properties", "Owner"]
		}
	},
	{
		name: "UpsertColumnMetadata"
		kind: "exec"
		paramMode: "struct"
		params: [
			{name: "TableSecurableName", type: "string"},
			{name: "ColumnName", type: "string"},
			{name: "Comment", type: "sql.NullString"},
			{name: "Properties", type: "sql.NullString"},
		]
		raw: {
			sql: """
				-- name: UpsertColumnMetadata :exec
				INSERT INTO column_metadata (table_securable_name, column_name, comment, properties)
				VALUES (?, ?, ?, ?)
				ON CONFLICT(table_securable_name, column_name)
				DO UPDATE SET comment = COALESCE(excluded.comment, comment),
				              properties = COALESCE(excluded.properties, properties),
				              updated_at = datetime('now')
				"""
			bind: ["TableSecurableName", "ColumnName", "Comment", "Properties"]
		}
	},
	{
		name: "UpsertLocalCredential"
		kind: "exec"
		paramMode: "struct"
		params: [
			{name: "PrincipalID", type: "string"},
			{name: "Username", type: "string"},
			{name: "PasswordHash", type: "string"},
			{name: "MustChangePassword", type: "int64"},
		]
		raw: {
			sql: """
				-- name: UpsertLocalCredential :exec
				INSERT INTO local_credentials (
				  principal_id, username, password_hash, password_changed_at, must_change_password, updated_at
				)
				VALUES (?, ?, ?, CURRENT_TIMESTAMP, ?, CURRENT_TIMESTAMP)
				ON CONFLICT(principal_id) DO UPDATE SET
				  username = excluded.username,
				  password_hash = excluded.password_hash,
				  password_changed_at = CURRENT_TIMESTAMP,
				  must_change_password = excluded.must_change_password,
				  updated_at = CURRENT_TIMESTAMP
				"""
			bind: ["PrincipalID", "Username", "PasswordHash", "MustChangePassword"]
		}
	},
	{
		name: "UpsertNotebookModelLink"
		kind: "exec"
		paramMode: "struct"
		params: [
			{name: "ID", type: "string"},
			{name: "NotebookID", type: "string"},
			{name: "ModelID", type: "string"},
			{name: "OutputCellID", type: "string"},
		]
		raw: {
			sql: """
				-- name: UpsertNotebookModelLink :exec
				INSERT INTO notebook_model_links (id, notebook_id, model_id, output_cell_id)
				VALUES (?, ?, ?, ?)
				ON CONFLICT(notebook_id) DO UPDATE SET
				    model_id = excluded.model_id,
				    output_cell_id = excluded.output_cell_id,
				    updated_at = datetime('now')
				"""
			bind: ["ID", "NotebookID", "ModelID", "OutputCellID"]
		}
	},
	{
		name: "UpsertTableStatistics"
		kind: "exec"
		paramMode: "struct"
		params: [
			{name: "TableSecurableName", type: "string"},
			{name: "RowCount", type: "sql.NullInt64"},
			{name: "SizeBytes", type: "sql.NullInt64"},
			{name: "ColumnCount", type: "sql.NullInt64"},
			{name: "ProfiledBy", type: "sql.NullString"},
		]
		raw: {
			sql: """
				-- name: UpsertTableStatistics :exec
				INSERT INTO table_statistics (table_securable_name, row_count, size_bytes, column_count, last_profiled_at, profiled_by)
				VALUES (?, ?, ?, ?, datetime('now'), ?)
				ON CONFLICT(table_securable_name)
				DO UPDATE SET row_count = excluded.row_count,
				              size_bytes = excluded.size_bytes,
				              column_count = excluded.column_count,
				              last_profiled_at = datetime('now'),
				              profiled_by = excluded.profiled_by
				"""
			bind: ["TableSecurableName", "RowCount", "SizeBytes", "ColumnCount", "ProfiledBy"]
		}
	},
]
