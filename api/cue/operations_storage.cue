package api

// Authored storage operations.

endpoints_storage: [
  {
    "method": "get",
    "path": "/storage-credentials",
    "operation_id": "listStorageCredentials",
    "summary": "List storage credentials",
    "tags": [
      "Storage"
    ],
    "parameters": [
      {
        "name": "max_results",
        "in": "query",
        "schema": {
          "type": "integer",
          "format": "int32"
        }
      },
      {
        "name": "page_token",
        "in": "query",
        "schema": {
          "type": "string"
        }
      }
    ],
    "responses": [
      {
        "status_code": 200,
        "description": "The request has succeeded.",
        "schema": {
          "ref": "PaginatedStorageCredentials"
        },
        "extensions": {
          "x-apigen-response-shape": {
            "body_type": "PaginatedStorageCredentials",
            "kind": "wrapped_json"
          }
        }
      },
      {
        "status_code": 400,
        "description": "The server could not understand the request due to invalid syntax.",
        "schema": {
          "ref": "Error"
        },
        "extensions": {
          "x-apigen-response-shape": {
            "body_type": "PaginatedStorageCredentials",
            "kind": "wrapped_json"
          }
        }
      },
      {
        "status_code": 401,
        "description": "Access is unauthorized.",
        "schema": {
          "ref": "Error"
        },
        "extensions": {
          "x-apigen-response-shape": {
            "body_type": "PaginatedStorageCredentials",
            "kind": "wrapped_json"
          }
        }
      },
      {
        "status_code": 429,
        "description": "Client error",
        "schema": {
          "ref": "Error"
        },
        "extensions": {
          "x-apigen-response-shape": {
            "body_type": "PaginatedStorageCredentials",
            "kind": "wrapped_json"
          }
        }
      },
      {
        "status_code": 500,
        "description": "Server error",
        "schema": {
          "ref": "Error"
        },
        "extensions": {
          "x-apigen-response-shape": {
            "body_type": "PaginatedStorageCredentials",
            "kind": "wrapped_json"
          }
        }
      }
    ],
    "extensions": {
      "security": [
        {
          "ApiKeyAuth": []
        },
        {
          "BearerAuth": []
        }
      ],
      "x-authz": {
        "mode": "authenticated"
      },
      "x-cli-command": "storage credentials list"
    }
  },
  {
    "method": "post",
    "path": "/storage-credentials",
    "operation_id": "createStorageCredential",
    "summary": "Create storage credential",
    "description": "Creates a reusable storage credential that can be referenced by external locations and managed catalogs.",
    "tags": [
      "Storage"
    ],
    "request_body": {
      "required": true,
      "description": "Request payload",
      "schema": {
        "ref": "CreateStorageCredentialRequest"
      }
    },
    "responses": [
      {
        "status_code": 201,
        "description": "The request has succeeded and a new resource has been created as a result.",
        "schema": {
          "ref": "StorageCredential"
        },
        "extensions": {
          "x-apigen-response-shape": {
            "body_type": "StorageCredential",
            "kind": "wrapped_json"
          }
        }
      },
      {
        "status_code": 400,
        "description": "The server could not understand the request due to invalid syntax.",
        "schema": {
          "ref": "Error"
        },
        "extensions": {
          "x-apigen-response-shape": {
            "body_type": "StorageCredential",
            "kind": "wrapped_json"
          }
        }
      },
      {
        "status_code": 401,
        "description": "Access is unauthorized.",
        "schema": {
          "ref": "Error"
        },
        "extensions": {
          "x-apigen-response-shape": {
            "body_type": "StorageCredential",
            "kind": "wrapped_json"
          }
        }
      },
      {
        "status_code": 403,
        "description": "Access is forbidden.",
        "schema": {
          "ref": "Error"
        },
        "extensions": {
          "x-apigen-response-shape": {
            "body_type": "StorageCredential",
            "kind": "wrapped_json"
          }
        }
      },
      {
        "status_code": 429,
        "description": "Client error",
        "schema": {
          "ref": "Error"
        },
        "extensions": {
          "x-apigen-response-shape": {
            "body_type": "StorageCredential",
            "kind": "wrapped_json"
          }
        }
      },
      {
        "status_code": 500,
        "description": "Server error",
        "schema": {
          "ref": "Error"
        },
        "extensions": {
          "x-apigen-response-shape": {
            "body_type": "StorageCredential",
            "kind": "wrapped_json"
          }
        }
      }
    ],
    "extensions": {
      "security": [
        {
          "ApiKeyAuth": []
        },
        {
          "BearerAuth": []
        }
      ],
      "x-authz": {
        "checks": [
          {
            "privilege": "CREATE_STORAGE_CREDENTIAL",
            "securable_id_source": "catalog_sentinel",
            "securable_type": "catalog"
          }
        ],
        "mode": "privilege"
      },
      "x-cli-command": "storage credentials create"
    }
  },
  {
    "method": "get",
    "path": "/storage-credentials/{credential_name}",
    "operation_id": "getStorageCredential",
    "summary": "Get storage credential",
    "tags": [
      "Storage"
    ],
    "parameters": [
      {
        "name": "credential_name",
        "in": "path",
        "required": true,
        "schema": {
          "type": "string"
        }
      }
    ],
    "responses": [
      {
        "status_code": 200,
        "description": "The request has succeeded.",
        "schema": {
          "ref": "StorageCredential"
        },
        "extensions": {
          "x-apigen-response-shape": {
            "body_type": "StorageCredential",
            "kind": "wrapped_json"
          }
        }
      },
      {
        "status_code": 400,
        "description": "The server could not understand the request due to invalid syntax.",
        "schema": {
          "ref": "Error"
        },
        "extensions": {
          "x-apigen-response-shape": {
            "body_type": "StorageCredential",
            "kind": "wrapped_json"
          }
        }
      },
      {
        "status_code": 401,
        "description": "Access is unauthorized.",
        "schema": {
          "ref": "Error"
        },
        "extensions": {
          "x-apigen-response-shape": {
            "body_type": "StorageCredential",
            "kind": "wrapped_json"
          }
        }
      },
      {
        "status_code": 403,
        "description": "Access is forbidden.",
        "schema": {
          "ref": "Error"
        },
        "extensions": {
          "x-apigen-response-shape": {
            "body_type": "StorageCredential",
            "kind": "wrapped_json"
          }
        }
      },
      {
        "status_code": 404,
        "description": "The server cannot find the requested resource.",
        "schema": {
          "ref": "Error"
        },
        "extensions": {
          "x-apigen-response-shape": {
            "body_type": "StorageCredential",
            "kind": "wrapped_json"
          }
        }
      },
      {
        "status_code": 429,
        "description": "Client error",
        "schema": {
          "ref": "Error"
        },
        "extensions": {
          "x-apigen-response-shape": {
            "body_type": "StorageCredential",
            "kind": "wrapped_json"
          }
        }
      },
      {
        "status_code": 500,
        "description": "Server error",
        "schema": {
          "ref": "Error"
        },
        "extensions": {
          "x-apigen-response-shape": {
            "body_type": "StorageCredential",
            "kind": "wrapped_json"
          }
        }
      }
    ],
    "extensions": {
      "security": [
        {
          "ApiKeyAuth": []
        },
        {
          "BearerAuth": []
        }
      ],
      "x-authz": {
        "mode": "authenticated"
      },
      "x-cli-command": "storage credentials get"
    }
  },
  {
    "method": "patch",
    "path": "/storage-credentials/{credential_name}",
    "operation_id": "updateStorageCredential",
    "summary": "Update storage credential",
    "tags": [
      "Storage"
    ],
    "parameters": [
      {
        "name": "credential_name",
        "in": "path",
        "required": true,
        "schema": {
          "type": "string"
        }
      }
    ],
    "request_body": {
      "required": true,
      "description": "Request payload",
      "schema": {
        "ref": "UpdateStorageCredentialRequest"
      }
    },
    "responses": [
      {
        "status_code": 200,
        "description": "The request has succeeded.",
        "schema": {
          "ref": "StorageCredential"
        },
        "extensions": {
          "x-apigen-response-shape": {
            "body_type": "StorageCredential",
            "kind": "wrapped_json"
          }
        }
      },
      {
        "status_code": 400,
        "description": "The server could not understand the request due to invalid syntax.",
        "schema": {
          "ref": "Error"
        },
        "extensions": {
          "x-apigen-response-shape": {
            "body_type": "StorageCredential",
            "kind": "wrapped_json"
          }
        }
      },
      {
        "status_code": 401,
        "description": "Access is unauthorized.",
        "schema": {
          "ref": "Error"
        },
        "extensions": {
          "x-apigen-response-shape": {
            "body_type": "StorageCredential",
            "kind": "wrapped_json"
          }
        }
      },
      {
        "status_code": 403,
        "description": "Access is forbidden.",
        "schema": {
          "ref": "Error"
        },
        "extensions": {
          "x-apigen-response-shape": {
            "body_type": "StorageCredential",
            "kind": "wrapped_json"
          }
        }
      },
      {
        "status_code": 404,
        "description": "The server cannot find the requested resource.",
        "schema": {
          "ref": "Error"
        },
        "extensions": {
          "x-apigen-response-shape": {
            "body_type": "StorageCredential",
            "kind": "wrapped_json"
          }
        }
      },
      {
        "status_code": 429,
        "description": "Client error",
        "schema": {
          "ref": "Error"
        },
        "extensions": {
          "x-apigen-response-shape": {
            "body_type": "StorageCredential",
            "kind": "wrapped_json"
          }
        }
      },
      {
        "status_code": 500,
        "description": "Server error",
        "schema": {
          "ref": "Error"
        },
        "extensions": {
          "x-apigen-response-shape": {
            "body_type": "StorageCredential",
            "kind": "wrapped_json"
          }
        }
      }
    ],
    "extensions": {
      "security": [
        {
          "ApiKeyAuth": []
        },
        {
          "BearerAuth": []
        }
      ],
      "x-authz": {
        "checks": [
          {
            "privilege": "CREATE_STORAGE_CREDENTIAL",
            "securable_id_source": "runtime_resolved_object_id",
            "securable_type": "storage_credential"
          }
        ],
        "mode": "privilege"
      },
      "x-cli-command": "storage credentials update"
    }
  },
  {
    "method": "delete",
    "path": "/storage-credentials/{credential_name}",
    "operation_id": "deleteStorageCredential",
    "summary": "Delete storage credential",
    "tags": [
      "Storage"
    ],
    "parameters": [
      {
        "name": "credential_name",
        "in": "path",
        "required": true,
        "schema": {
          "type": "string"
        }
      }
    ],
    "responses": [
      {
        "status_code": 204,
        "description": "There is no content to send for this request, but the headers may be useful."
      },
      {
        "status_code": 400,
        "description": "The server could not understand the request due to invalid syntax.",
        "schema": {
          "ref": "Error"
        }
      },
      {
        "status_code": 401,
        "description": "Access is unauthorized.",
        "schema": {
          "ref": "Error"
        }
      },
      {
        "status_code": 403,
        "description": "Access is forbidden.",
        "schema": {
          "ref": "Error"
        }
      },
      {
        "status_code": 404,
        "description": "The server cannot find the requested resource.",
        "schema": {
          "ref": "Error"
        }
      },
      {
        "status_code": 429,
        "description": "Client error",
        "schema": {
          "ref": "Error"
        }
      },
      {
        "status_code": 500,
        "description": "Server error",
        "schema": {
          "ref": "Error"
        }
      }
    ],
    "extensions": {
      "security": [
        {
          "ApiKeyAuth": []
        },
        {
          "BearerAuth": []
        }
      ],
      "x-authz": {
        "checks": [
          {
            "privilege": "CREATE_STORAGE_CREDENTIAL",
            "securable_id_source": "runtime_resolved_object_id",
            "securable_type": "storage_credential"
          }
        ],
        "mode": "privilege"
      },
      "x-cli-command": "storage credentials delete"
    }
  },
  {
    "method": "get",
    "path": "/external-locations",
    "operation_id": "listExternalLocations",
    "summary": "List external locations",
    "tags": [
      "Storage"
    ],
    "parameters": [
      {
        "name": "max_results",
        "in": "query",
        "schema": {
          "type": "integer",
          "format": "int32"
        }
      },
      {
        "name": "page_token",
        "in": "query",
        "schema": {
          "type": "string"
        }
      }
    ],
    "responses": [
      {
        "status_code": 200,
        "description": "The request has succeeded.",
        "schema": {
          "ref": "PaginatedExternalLocations"
        },
        "extensions": {
          "x-apigen-response-shape": {
            "body_type": "PaginatedExternalLocations",
            "kind": "wrapped_json"
          }
        }
      },
      {
        "status_code": 400,
        "description": "The server could not understand the request due to invalid syntax.",
        "schema": {
          "ref": "Error"
        },
        "extensions": {
          "x-apigen-response-shape": {
            "body_type": "PaginatedExternalLocations",
            "kind": "wrapped_json"
          }
        }
      },
      {
        "status_code": 401,
        "description": "Access is unauthorized.",
        "schema": {
          "ref": "Error"
        },
        "extensions": {
          "x-apigen-response-shape": {
            "body_type": "PaginatedExternalLocations",
            "kind": "wrapped_json"
          }
        }
      },
      {
        "status_code": 429,
        "description": "Client error",
        "schema": {
          "ref": "Error"
        },
        "extensions": {
          "x-apigen-response-shape": {
            "body_type": "PaginatedExternalLocations",
            "kind": "wrapped_json"
          }
        }
      },
      {
        "status_code": 500,
        "description": "Server error",
        "schema": {
          "ref": "Error"
        },
        "extensions": {
          "x-apigen-response-shape": {
            "body_type": "PaginatedExternalLocations",
            "kind": "wrapped_json"
          }
        }
      }
    ],
    "extensions": {
      "security": [
        {
          "ApiKeyAuth": []
        },
        {
          "BearerAuth": []
        }
      ],
      "x-authz": {
        "mode": "authenticated"
      },
      "x-cli-command": "storage locations list"
    }
  },
  {
    "method": "post",
    "path": "/external-locations",
    "operation_id": "createExternalLocation",
    "summary": "Create external location",
    "description": "Creates a new external location and configures DuckDB with the associated credential. Catalog registrations are managed separately; creating a location does not attach or create a DuckLake catalog.",
    "tags": [
      "Storage"
    ],
    "request_body": {
      "required": true,
      "description": "Request payload",
      "schema": {
        "ref": "CreateExternalLocationRequest"
      }
    },
    "responses": [
      {
        "status_code": 201,
        "description": "The request has succeeded and a new resource has been created as a result.",
        "schema": {
          "ref": "ExternalLocation"
        },
        "extensions": {
          "x-apigen-response-shape": {
            "body_type": "ExternalLocation",
            "kind": "wrapped_json"
          }
        }
      },
      {
        "status_code": 400,
        "description": "The server could not understand the request due to invalid syntax.",
        "schema": {
          "ref": "Error"
        },
        "extensions": {
          "x-apigen-response-shape": {
            "body_type": "ExternalLocation",
            "kind": "wrapped_json"
          }
        }
      },
      {
        "status_code": 401,
        "description": "Access is unauthorized.",
        "schema": {
          "ref": "Error"
        },
        "extensions": {
          "x-apigen-response-shape": {
            "body_type": "ExternalLocation",
            "kind": "wrapped_json"
          }
        }
      },
      {
        "status_code": 403,
        "description": "Access is forbidden.",
        "schema": {
          "ref": "Error"
        },
        "extensions": {
          "x-apigen-response-shape": {
            "body_type": "ExternalLocation",
            "kind": "wrapped_json"
          }
        }
      },
      {
        "status_code": 429,
        "description": "Client error",
        "schema": {
          "ref": "Error"
        },
        "extensions": {
          "x-apigen-response-shape": {
            "body_type": "ExternalLocation",
            "kind": "wrapped_json"
          }
        }
      },
      {
        "status_code": 500,
        "description": "Server error",
        "schema": {
          "ref": "Error"
        },
        "extensions": {
          "x-apigen-response-shape": {
            "body_type": "ExternalLocation",
            "kind": "wrapped_json"
          }
        }
      }
    ],
    "extensions": {
      "security": [
        {
          "ApiKeyAuth": []
        },
        {
          "BearerAuth": []
        }
      ],
      "x-authz": {
        "checks": [
          {
            "privilege": "CREATE_EXTERNAL_LOCATION",
            "securable_id_source": "catalog_sentinel",
            "securable_type": "catalog"
          }
        ],
        "mode": "privilege"
      },
      "x-cli-command": "storage locations create"
    }
  },
  {
    "method": "get",
    "path": "/external-locations/{location_name}",
    "operation_id": "getExternalLocation",
    "summary": "Get external location",
    "tags": [
      "Storage"
    ],
    "parameters": [
      {
        "name": "location_name",
        "in": "path",
        "required": true,
        "schema": {
          "type": "string"
        }
      }
    ],
    "responses": [
      {
        "status_code": 200,
        "description": "The request has succeeded.",
        "schema": {
          "ref": "ExternalLocation"
        },
        "extensions": {
          "x-apigen-response-shape": {
            "body_type": "ExternalLocation",
            "kind": "wrapped_json"
          }
        }
      },
      {
        "status_code": 400,
        "description": "The server could not understand the request due to invalid syntax.",
        "schema": {
          "ref": "Error"
        },
        "extensions": {
          "x-apigen-response-shape": {
            "body_type": "ExternalLocation",
            "kind": "wrapped_json"
          }
        }
      },
      {
        "status_code": 401,
        "description": "Access is unauthorized.",
        "schema": {
          "ref": "Error"
        },
        "extensions": {
          "x-apigen-response-shape": {
            "body_type": "ExternalLocation",
            "kind": "wrapped_json"
          }
        }
      },
      {
        "status_code": 403,
        "description": "Access is forbidden.",
        "schema": {
          "ref": "Error"
        },
        "extensions": {
          "x-apigen-response-shape": {
            "body_type": "ExternalLocation",
            "kind": "wrapped_json"
          }
        }
      },
      {
        "status_code": 404,
        "description": "The server cannot find the requested resource.",
        "schema": {
          "ref": "Error"
        },
        "extensions": {
          "x-apigen-response-shape": {
            "body_type": "ExternalLocation",
            "kind": "wrapped_json"
          }
        }
      },
      {
        "status_code": 429,
        "description": "Client error",
        "schema": {
          "ref": "Error"
        },
        "extensions": {
          "x-apigen-response-shape": {
            "body_type": "ExternalLocation",
            "kind": "wrapped_json"
          }
        }
      },
      {
        "status_code": 500,
        "description": "Server error",
        "schema": {
          "ref": "Error"
        },
        "extensions": {
          "x-apigen-response-shape": {
            "body_type": "ExternalLocation",
            "kind": "wrapped_json"
          }
        }
      }
    ],
    "extensions": {
      "security": [
        {
          "ApiKeyAuth": []
        },
        {
          "BearerAuth": []
        }
      ],
      "x-authz": {
        "mode": "authenticated"
      },
      "x-cli-command": "storage locations get"
    }
  },
  {
    "method": "patch",
    "path": "/external-locations/{location_name}",
    "operation_id": "updateExternalLocation",
    "summary": "Update external location",
    "tags": [
      "Storage"
    ],
    "parameters": [
      {
        "name": "location_name",
        "in": "path",
        "required": true,
        "schema": {
          "type": "string"
        }
      }
    ],
    "request_body": {
      "required": true,
      "description": "Request payload",
      "schema": {
        "ref": "UpdateExternalLocationRequest"
      }
    },
    "responses": [
      {
        "status_code": 200,
        "description": "The request has succeeded.",
        "schema": {
          "ref": "ExternalLocation"
        },
        "extensions": {
          "x-apigen-response-shape": {
            "body_type": "ExternalLocation",
            "kind": "wrapped_json"
          }
        }
      },
      {
        "status_code": 400,
        "description": "The server could not understand the request due to invalid syntax.",
        "schema": {
          "ref": "Error"
        },
        "extensions": {
          "x-apigen-response-shape": {
            "body_type": "ExternalLocation",
            "kind": "wrapped_json"
          }
        }
      },
      {
        "status_code": 401,
        "description": "Access is unauthorized.",
        "schema": {
          "ref": "Error"
        },
        "extensions": {
          "x-apigen-response-shape": {
            "body_type": "ExternalLocation",
            "kind": "wrapped_json"
          }
        }
      },
      {
        "status_code": 403,
        "description": "Access is forbidden.",
        "schema": {
          "ref": "Error"
        },
        "extensions": {
          "x-apigen-response-shape": {
            "body_type": "ExternalLocation",
            "kind": "wrapped_json"
          }
        }
      },
      {
        "status_code": 404,
        "description": "The server cannot find the requested resource.",
        "schema": {
          "ref": "Error"
        },
        "extensions": {
          "x-apigen-response-shape": {
            "body_type": "ExternalLocation",
            "kind": "wrapped_json"
          }
        }
      },
      {
        "status_code": 429,
        "description": "Client error",
        "schema": {
          "ref": "Error"
        },
        "extensions": {
          "x-apigen-response-shape": {
            "body_type": "ExternalLocation",
            "kind": "wrapped_json"
          }
        }
      },
      {
        "status_code": 500,
        "description": "Server error",
        "schema": {
          "ref": "Error"
        },
        "extensions": {
          "x-apigen-response-shape": {
            "body_type": "ExternalLocation",
            "kind": "wrapped_json"
          }
        }
      }
    ],
    "extensions": {
      "security": [
        {
          "ApiKeyAuth": []
        },
        {
          "BearerAuth": []
        }
      ],
      "x-authz": {
        "checks": [
          {
            "privilege": "CREATE_EXTERNAL_LOCATION",
            "securable_id_source": "runtime_resolved_object_id",
            "securable_type": "external_location"
          }
        ],
        "mode": "privilege"
      },
      "x-cli-command": "storage locations update"
    }
  },
  {
    "method": "delete",
    "path": "/external-locations/{location_name}",
    "operation_id": "deleteExternalLocation",
    "summary": "Delete external location",
    "tags": [
      "Storage"
    ],
    "parameters": [
      {
        "name": "location_name",
        "in": "path",
        "required": true,
        "schema": {
          "type": "string"
        }
      }
    ],
    "responses": [
      {
        "status_code": 204,
        "description": "There is no content to send for this request, but the headers may be useful."
      },
      {
        "status_code": 400,
        "description": "The server could not understand the request due to invalid syntax.",
        "schema": {
          "ref": "Error"
        }
      },
      {
        "status_code": 401,
        "description": "Access is unauthorized.",
        "schema": {
          "ref": "Error"
        }
      },
      {
        "status_code": 403,
        "description": "Access is forbidden.",
        "schema": {
          "ref": "Error"
        }
      },
      {
        "status_code": 404,
        "description": "The server cannot find the requested resource.",
        "schema": {
          "ref": "Error"
        }
      },
      {
        "status_code": 429,
        "description": "Client error",
        "schema": {
          "ref": "Error"
        }
      },
      {
        "status_code": 500,
        "description": "Server error",
        "schema": {
          "ref": "Error"
        }
      }
    ],
    "extensions": {
      "security": [
        {
          "ApiKeyAuth": []
        },
        {
          "BearerAuth": []
        }
      ],
      "x-authz": {
        "checks": [
          {
            "privilege": "CREATE_EXTERNAL_LOCATION",
            "securable_id_source": "runtime_resolved_object_id",
            "securable_type": "external_location"
          }
        ],
        "mode": "privilege"
      },
      "x-cli-command": "storage locations delete"
    }
  }
]

