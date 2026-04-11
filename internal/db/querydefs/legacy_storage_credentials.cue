package querydefs

queries: [
	#CountAll & {
		name:   "CountStorageCredentials"
		_table: "storage_credentials"
	},
	#InsertReturningTable & {
		name:   "CreateStorageCredential"
		_table: "storage_credentials"
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
		insert: {
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
		}
	},
	#DeleteByID & {
		name:   "DeleteStorageCredential"
		_table: "storage_credentials"
	},
	#GetByID & {
		name:   "GetStorageCredential"
		_table: "storage_credentials"
	},
	#GetByStringField & {
		name:   "GetStorageCredentialByName"
		_table: "storage_credentials"
		_field: "name"
		_param: "name"
	},
	#ListPaginatedOrdered & {
		name:   "ListStorageCredentials"
		_table: "storage_credentials"
		_order: [
			{expr: "name"},
		]
	},
	#UpdateByIDTouch & {
		name:   "UpdateStorageCredential"
		_table: "storage_credentials"
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
		_set: [
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
		]
	},
]
