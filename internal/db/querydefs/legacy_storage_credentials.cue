package querydefs

queries: [
	{
		name: "CountStorageCredentials"
		kind: "one"
		result: {scalar: "int64"}
		select: {
			from: "storage_credentials"
			columns: [
				{expr: "COUNT(*)"},
			]
		}
	},
	{
		name: "CreateStorageCredential"
		kind: "one"
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
		insert: {
			into: "storage_credentials"
			columns: [
				"id",
				"name",
				"credential_type",
				"key_id_encrypted",
				"secret_encrypted",
				"endpoint",
				"region",
				"url_style",
				"azure_account_name",
				"azure_account_key_encrypted",
				"azure_client_id",
				"azure_tenant_id",
				"azure_client_secret_encrypted",
				"gcs_key_file_path",
				"comment",
				"owner",
			]
			values: [
				{param: "ID"},
				{param: "Name"},
				{param: "CredentialType"},
				{param: "KeyIDEncrypted"},
				{param: "SecretEncrypted"},
				{param: "Endpoint"},
				{param: "Region"},
				{param: "UrlStyle"},
				{param: "AzureAccountName"},
				{param: "AzureAccountKeyEncrypted"},
				{param: "AzureClientID"},
				{param: "AzureTenantID"},
				{param: "AzureClientSecretEncrypted"},
				{param: "GcsKeyFilePath"},
				{param: "Comment"},
				{param: "Owner"},
			]
			returningColumns: [
				{expr: "id"},
				{expr: "name"},
				{expr: "credential_type"},
				{expr: "key_id_encrypted"},
				{expr: "secret_encrypted"},
				{expr: "endpoint"},
				{expr: "region"},
				{expr: "url_style"},
				{expr: "comment"},
				{expr: "owner"},
				{expr: "created_at"},
				{expr: "updated_at"},
				{expr: "azure_account_name"},
				{expr: "azure_account_key_encrypted"},
				{expr: "azure_client_id"},
				{expr: "azure_tenant_id"},
				{expr: "azure_client_secret_encrypted"},
				{expr: "gcs_key_file_path"},
			]
		}
	},
	{
		name: "DeleteStorageCredential"
		kind: "exec"
		params: [
			{name: "id", type: "string"},
		]
		delete: {
			from: "storage_credentials"
			where: [
				{column: "id", op: "=", param: "id"},
			]
		}
	},
	{
		name: "GetStorageCredential"
		kind: "one"
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
		select: {
			from: "storage_credentials"
			columns: [
				{expr: "id"},
				{expr: "name"},
				{expr: "credential_type"},
				{expr: "key_id_encrypted"},
				{expr: "secret_encrypted"},
				{expr: "endpoint"},
				{expr: "region"},
				{expr: "url_style"},
				{expr: "comment"},
				{expr: "owner"},
				{expr: "created_at"},
				{expr: "updated_at"},
				{expr: "azure_account_name"},
				{expr: "azure_account_key_encrypted"},
				{expr: "azure_client_id"},
				{expr: "azure_tenant_id"},
				{expr: "azure_client_secret_encrypted"},
				{expr: "gcs_key_file_path"},
			]
			where: [
				{column: "id", op: "=", param: "id"},
			]
		}
	},
	{
		name: "GetStorageCredentialByName"
		kind: "one"
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
		select: {
			from: "storage_credentials"
			columns: [
				{expr: "id"},
				{expr: "name"},
				{expr: "credential_type"},
				{expr: "key_id_encrypted"},
				{expr: "secret_encrypted"},
				{expr: "endpoint"},
				{expr: "region"},
				{expr: "url_style"},
				{expr: "comment"},
				{expr: "owner"},
				{expr: "created_at"},
				{expr: "updated_at"},
				{expr: "azure_account_name"},
				{expr: "azure_account_key_encrypted"},
				{expr: "azure_client_id"},
				{expr: "azure_tenant_id"},
				{expr: "azure_client_secret_encrypted"},
				{expr: "gcs_key_file_path"},
			]
			where: [
				{column: "name", op: "=", param: "name"},
			]
		}
	},
	{
		name: "ListStorageCredentials"
		kind: "many"
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
		select: {
			from: "storage_credentials"
			columns: [
				{expr: "id"},
				{expr: "name"},
				{expr: "credential_type"},
				{expr: "key_id_encrypted"},
				{expr: "secret_encrypted"},
				{expr: "endpoint"},
				{expr: "region"},
				{expr: "url_style"},
				{expr: "comment"},
				{expr: "owner"},
				{expr: "created_at"},
				{expr: "updated_at"},
				{expr: "azure_account_name"},
				{expr: "azure_account_key_encrypted"},
				{expr: "azure_client_id"},
				{expr: "azure_tenant_id"},
				{expr: "azure_client_secret_encrypted"},
				{expr: "gcs_key_file_path"},
			]
			orderBy: [
				{expr: "name"},
			]
			limitParam: "Limit"
			offsetParam: "Offset"
		}
	},
	{
		name: "UpdateStorageCredential"
		kind: "exec"
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
		update: {
			table: "storage_credentials"
			set: [
				{column: "key_id_encrypted", value: {param: "KeyIDEncrypted"}, coalesceWith: true},
				{column: "secret_encrypted", value: {param: "SecretEncrypted"}, coalesceWith: true},
				{column: "endpoint", value: {param: "Endpoint"}, coalesceWith: true},
				{column: "region", value: {param: "Region"}, coalesceWith: true},
				{column: "url_style", value: {param: "UrlStyle"}, coalesceWith: true},
				{column: "azure_account_name", value: {param: "AzureAccountName"}, coalesceWith: true},
				{column: "azure_account_key_encrypted", value: {param: "AzureAccountKeyEncrypted"}, coalesceWith: true},
				{column: "azure_client_id", value: {param: "AzureClientID"}, coalesceWith: true},
				{column: "azure_tenant_id", value: {param: "AzureTenantID"}, coalesceWith: true},
				{column: "azure_client_secret_encrypted", value: {param: "AzureClientSecretEncrypted"}, coalesceWith: true},
				{column: "gcs_key_file_path", value: {param: "GcsKeyFilePath"}, coalesceWith: true},
				{column: "comment", value: {param: "Comment"}, coalesceWith: true},
				{column: "updated_at", value: {sql: "datetime('now')"}},
			]
			where: [
				{column: "id", op: "=", param: "ID"},
			]
		}
	},
]
