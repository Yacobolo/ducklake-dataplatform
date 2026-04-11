package api

// Authored audit operations.

endpoints_audit: [
  {
    "method": "get",
    "path": "/audit-entries",
    "operation_id": "listAuditLogs",
    "summary": "List audit entries",
    "tags": [
      "Audit"
    ],
    "parameters": [
      {
        "name": "action",
        "in": "query",
        "schema": {
          "type": "string"
        }
      },
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
      },
      {
        "name": "principal_name",
        "in": "query",
        "schema": {
          "type": "string"
        }
      },
      {
        "name": "status",
        "in": "query",
        "schema": {
          "ref": "AuditDecisionStatus"
        }
      }
    ],
    "responses": [
      {
        "status_code": 200,
        "description": "The request has succeeded.",
        "schema": {
          "ref": "PaginatedAuditLogs"
        },
        "extensions": {
          "x-apigen-response-shape": {
            "body_type": "PaginatedAuditLogs",
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
            "body_type": "PaginatedAuditLogs",
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
            "body_type": "PaginatedAuditLogs",
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
            "body_type": "PaginatedAuditLogs",
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
            "body_type": "PaginatedAuditLogs",
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
            "body_type": "PaginatedAuditLogs",
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
        "mode": "admin_only"
      },
      "x-cli-command": "observability audit-logs list"
    }
  }
]

