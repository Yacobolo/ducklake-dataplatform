package api

// Authored storage operations.

#storageTag: "Storage"

#wrappedStorageOperation: #genericOperationSpec & {
	wrapped: true
}

#credentialNamePathParameter: #pathStringParameter & {
	#name: "credential_name"
}

#locationNamePathParameter: #pathStringParameter & {
	#name: "location_name"
}

#storageCredentialPathParameters: [
	#credentialNamePathParameter,
]

#externalLocationPathParameters: [
	#locationNamePathParameter,
]

#createStorageCredentialAuthz: {
	mode: "privilege"
	checks: [
		{
			securable_type:     "catalog"
			privilege:          "CREATE_STORAGE_CREDENTIAL"
			securable_id_source: "catalog_sentinel"
		},
	]
}

#manageStorageCredentialAuthz: {
	mode: "privilege"
	checks: [
		{
			securable_type:     "storage_credential"
			privilege:          "CREATE_STORAGE_CREDENTIAL"
			securable_id_source: "runtime_resolved_object_id"
		},
	]
}

#createExternalLocationAuthz: {
	mode: "privilege"
	checks: [
		{
			securable_type:     "catalog"
			privilege:          "CREATE_EXTERNAL_LOCATION"
			securable_id_source: "catalog_sentinel"
		},
	]
}

#manageExternalLocationAuthz: {
	mode: "privilege"
	checks: [
		{
			securable_type:     "external_location"
			privilege:          "CREATE_EXTERNAL_LOCATION"
			securable_id_source: "runtime_resolved_object_id"
		},
	]
}

#storageOps: [
	#wrappedStorageOperation & {
		kind:         "response"
		method:       "get"
		op:           "listStorageCredentials"
		path:         "/storage-credentials"
		summary:      "List storage credentials"
		cli: {
			command: ["storage", "credentials", "list"]
		}
		returns:      "PaginatedStorageCredentials"
		error_family: "standard"
		params:       #paginationParameters
	},
	#wrappedStorageOperation & {
		kind:           "response"
		method:         "post"
		op:             "createStorageCredential"
		path:           "/storage-credentials"
		summary:        "Create storage credential"
		description:    "Creates a reusable storage credential that can be referenced by external locations and managed catalogs."
		cli: {
			command: ["storage", "credentials", "create"]
		}
		returns:        "StorageCredential"
		success_status: 201
		error_family:   "mutating"
		body_ref:       "CreateStorageCredentialRequest"
		body_description: "Request payload"
		authz_default:   false
		authz:           #createStorageCredentialAuthz
	},
	#wrappedStorageOperation & {
		kind:         "response"
		method:       "get"
		op:           "getStorageCredential"
		path:         "/storage-credentials/{credential_name}"
		summary:      "Get storage credential"
		cli: {
			command: ["storage", "credentials", "get"]
		}
		returns:      "StorageCredential"
		error_family: "resource"
		params:       #storageCredentialPathParameters
	},
	#wrappedStorageOperation & {
		kind:         "response"
		method:       "patch"
		op:           "updateStorageCredential"
		path:         "/storage-credentials/{credential_name}"
		summary:      "Update storage credential"
		cli: {
			command: ["storage", "credentials", "update"]
		}
		returns:      "StorageCredential"
		error_family: "resource"
		params:       #storageCredentialPathParameters
		body_ref:     "UpdateStorageCredentialRequest"
		body_description: "Request payload"
		authz_default:   false
		authz:           #manageStorageCredentialAuthz
	},
	#genericOperationSpec & {
		wrapped:       false
		kind:          "no_content"
		method:        "delete"
		op:            "deleteStorageCredential"
		path:          "/storage-credentials/{credential_name}"
		summary:       "Delete storage credential"
		cli: {
			command: ["storage", "credentials", "delete"]
		}
		error_family:  "resource"
		params:        #storageCredentialPathParameters
		authz_default: false
		authz:         #manageStorageCredentialAuthz
	},
	#wrappedStorageOperation & {
		kind:         "response"
		method:       "get"
		op:           "listExternalLocations"
		path:         "/external-locations"
		summary:      "List external locations"
		cli: {
			command: ["storage", "locations", "list"]
		}
		returns:      "PaginatedExternalLocations"
		error_family: "standard"
		params:       #paginationParameters
	},
	#wrappedStorageOperation & {
		kind:           "response"
		method:         "post"
		op:             "createExternalLocation"
		path:           "/external-locations"
		summary:        "Create external location"
		description:    "Creates a new external location and configures DuckDB with the associated credential. Catalog registrations are managed separately; creating a location does not attach or create a DuckLake catalog."
		cli: {
			command: ["storage", "locations", "create"]
		}
		returns:        "ExternalLocation"
		success_status: 201
		error_family:   "mutating"
		body_ref:       "CreateExternalLocationRequest"
		body_description: "Request payload"
		authz_default:   false
		authz:           #createExternalLocationAuthz
	},
	#wrappedStorageOperation & {
		kind:         "response"
		method:       "get"
		op:           "getExternalLocation"
		path:         "/external-locations/{location_name}"
		summary:      "Get external location"
		cli: {
			command: ["storage", "locations", "get"]
		}
		returns:      "ExternalLocation"
		error_family: "resource"
		params:       #externalLocationPathParameters
	},
	#wrappedStorageOperation & {
		kind:         "response"
		method:       "patch"
		op:           "updateExternalLocation"
		path:         "/external-locations/{location_name}"
		summary:      "Update external location"
		cli: {
			command: ["storage", "locations", "update"]
		}
		returns:      "ExternalLocation"
		error_family: "resource"
		params:       #externalLocationPathParameters
		body_ref:     "UpdateExternalLocationRequest"
		body_description: "Request payload"
		authz_default:   false
		authz:           #manageExternalLocationAuthz
	},
	#genericOperationSpec & {
		wrapped:       false
		kind:          "no_content"
		method:        "delete"
		op:            "deleteExternalLocation"
		path:          "/external-locations/{location_name}"
		summary:       "Delete external location"
		cli: {
			command: ["storage", "locations", "delete"]
		}
		error_family:  "resource"
		params:        #externalLocationPathParameters
		authz_default: false
		authz:         #manageExternalLocationAuthz
	},
]

endpoints_storage: [
	for op in #storageOps {
		(#endpointFromGenericOperation & {
			tag:  #storageTag
			spec: op
		}).endpoint
	},
]
