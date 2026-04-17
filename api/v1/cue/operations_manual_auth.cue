package api

// Authored manual auth endpoints that belong in canonical OpenAPI but are
// filtered out of APIGen codegen via x-apigen-manual.

endpoints_generated: [
  {
    "method": "post"
    "path": "/auth/bootstrap/complete"
    "operation_id": "bootstrapComplete"
    "summary": "Bootstrap complete"
    "responses": [
      {
        "status_code": 201
        "description": "The request has succeeded and a new resource has been created as a result."
        "schema": {
          "ref": "AuthLoginResponse"
        }
      },
      {
        "status_code": 400
        "description": "The server could not understand the request due to invalid syntax."
        "schema": {
          "ref": "Error"
        }
      },
      {
        "status_code": 401
        "description": "Access is unauthorized."
        "schema": {
          "ref": "Error"
        }
      },
      {
        "status_code": 403
        "description": "Access is forbidden."
        "schema": {
          "ref": "Error"
        }
      },
      {
        "status_code": 409
        "description": "The request conflicts with the current state of the server."
        "schema": {
          "ref": "Error"
        }
      },
      {
        "status_code": 429
        "description": "Client error"
        "schema": {
          "ref": "Error"
        }
      },
      {
        "status_code": 500
        "description": "Server error"
        "schema": {
          "ref": "Error"
        }
      },
    ]
    "tags": [
      "Auth",
    ]
    "parameters": []
    "request_body": {
      "required": true
      "schema": {
        "ref": "BootstrapCompleteRequest"
      }
    }
    "extensions": {
      "security": [
        {},
      ]
      "x-authz": {
        "mode": "none"
      }
      "x-apigen-manual": true
    }
  },
  {
    "method": "post"
    "path": "/auth/bootstrap/tokens"
    "operation_id": "createBootstrapToken"
    "summary": "Create bootstrap token"
    "responses": [
      {
        "status_code": 201
        "description": "The request has succeeded and a new resource has been created as a result."
        "schema": {
          "ref": "BootstrapTokenResponse"
        }
      },
      {
        "status_code": 400
        "description": "The server could not understand the request due to invalid syntax."
        "schema": {
          "ref": "Error"
        }
      },
      {
        "status_code": 401
        "description": "Access is unauthorized."
        "schema": {
          "ref": "Error"
        }
      },
      {
        "status_code": 403
        "description": "Access is forbidden."
        "schema": {
          "ref": "Error"
        }
      },
      {
        "status_code": 429
        "description": "Client error"
        "schema": {
          "ref": "Error"
        }
      },
      {
        "status_code": 500
        "description": "Server error"
        "schema": {
          "ref": "Error"
        }
      },
    ]
    "tags": [
      "Auth",
    ]
    "parameters": []
    "request_body": {
      "required": true
      "schema": {
        "ref": "BootstrapTokenRequest"
      }
    }
    "extensions": {
      "x-authz": {
        "mode": "admin_only"
      }
      "x-apigen-manual": true
    }
  },
  {
    "method": "get"
    "path": "/auth/provider/oidc"
    "operation_id": "getOIDCProvider"
    "summary": "Get OIDC provider"
    "responses": [
      {
        "status_code": 200
        "description": "The request has succeeded."
        "schema": {
          "ref": "OIDCProviderResponse"
        }
      },
      {
        "status_code": 401
        "description": "Access is unauthorized."
        "schema": {
          "ref": "Error"
        }
      },
      {
        "status_code": 403
        "description": "Access is forbidden."
        "schema": {
          "ref": "Error"
        }
      },
      {
        "status_code": 429
        "description": "Client error"
        "schema": {
          "ref": "Error"
        }
      },
      {
        "status_code": 500
        "description": "Server error"
        "schema": {
          "ref": "Error"
        }
      },
    ]
    "tags": [
      "Auth",
    ]
    "parameters": []
    "extensions": {
      "x-authz": {
        "mode": "admin_only"
      }
      "x-apigen-manual": true
    }
  },
  {
    "method": "get"
    "path": "/auth/sessions/stats"
    "operation_id": "getWebSessionStats"
    "summary": "Get web session stats"
    "responses": [
      {
        "status_code": 200
        "description": "The request has succeeded."
        "schema": {
          "ref": "WebSessionStatsResponse"
        }
      },
      {
        "status_code": 401
        "description": "Access is unauthorized."
        "schema": {
          "ref": "Error"
        }
      },
      {
        "status_code": 403
        "description": "Access is forbidden."
        "schema": {
          "ref": "Error"
        }
      },
      {
        "status_code": 429
        "description": "Client error"
        "schema": {
          "ref": "Error"
        }
      },
      {
        "status_code": 500
        "description": "Server error"
        "schema": {
          "ref": "Error"
        }
      },
      {
        "status_code": 503
        "description": "Service unavailable."
        "schema": {
          "ref": "Error"
        }
      },
    ]
    "tags": [
      "Auth",
    ]
    "parameters": []
    "extensions": {
      "x-authz": {
        "mode": "admin_only"
      }
      "x-apigen-manual": true
    }
  },
  {
    "method": "post"
    "path": "/auth/local/login"
    "operation_id": "localLogin"
    "summary": "Local login"
    "responses": [
      {
        "status_code": 201
        "description": "The request has succeeded and a new resource has been created as a result."
        "schema": {
          "ref": "AuthLoginResponse"
        }
      },
      {
        "status_code": 400
        "description": "The server could not understand the request due to invalid syntax."
        "schema": {
          "ref": "Error"
        }
      },
      {
        "status_code": 401
        "description": "Access is unauthorized."
        "schema": {
          "ref": "Error"
        }
      },
      {
        "status_code": 403
        "description": "Access is forbidden."
        "schema": {
          "ref": "Error"
        }
      },
      {
        "status_code": 429
        "description": "Client error"
        "schema": {
          "ref": "Error"
        }
      },
      {
        "status_code": 500
        "description": "Server error"
        "schema": {
          "ref": "Error"
        }
      },
    ]
    "description": "Authenticates a local user with username and password and returns a bearer token for subsequent API calls."
    "tags": [
      "Auth",
    ]
    "parameters": []
    "request_body": {
      "required": true
      "schema": {
        "ref": "LocalLoginRequest"
      }
    }
    "extensions": {
      "security": [
        {},
      ]
      "x-authz": {
        "mode": "none"
      }
      "x-apigen-manual": true
    }
  },
  {
    "method": "post"
    "path": "/auth/sessions/revocations"
    "operation_id": "revokeAllWebSessions"
    "summary": "Revoke all web sessions"
    "responses": [
      {
        "status_code": 204
        "description": "There is no content to send for this request, but the headers may be useful. "
      },
      {
        "status_code": 400
        "description": "The server could not understand the request due to invalid syntax."
        "schema": {
          "ref": "Error"
        }
      },
      {
        "status_code": 401
        "description": "Access is unauthorized."
        "schema": {
          "ref": "Error"
        }
      },
      {
        "status_code": 403
        "description": "Access is forbidden."
        "schema": {
          "ref": "Error"
        }
      },
      {
        "status_code": 429
        "description": "Client error"
        "schema": {
          "ref": "Error"
        }
      },
      {
        "status_code": 500
        "description": "Server error"
        "schema": {
          "ref": "Error"
        }
      },
      {
        "status_code": 503
        "description": "Service unavailable."
        "schema": {
          "ref": "Error"
        }
      },
    ]
    "tags": [
      "Auth",
    ]
    "parameters": []
    "request_body": {
      "required": true
      "schema": {
        "ref": "RevokeWebSessionsRequest"
      }
    }
    "extensions": {
      "x-authz": {
        "mode": "admin_only"
      }
      "x-apigen-manual": true
    }
  },
  {
    "method": "put"
    "path": "/auth/provider/oidc"
    "operation_id": "upsertOIDCProvider"
    "summary": "Upsert OIDC provider"
    "responses": [
      {
        "status_code": 204
        "description": "There is no content to send for this request, but the headers may be useful. "
      },
      {
        "status_code": 400
        "description": "The server could not understand the request due to invalid syntax."
        "schema": {
          "ref": "Error"
        }
      },
      {
        "status_code": 401
        "description": "Access is unauthorized."
        "schema": {
          "ref": "Error"
        }
      },
      {
        "status_code": 403
        "description": "Access is forbidden."
        "schema": {
          "ref": "Error"
        }
      },
      {
        "status_code": 429
        "description": "Client error"
        "schema": {
          "ref": "Error"
        }
      },
      {
        "status_code": 500
        "description": "Server error"
        "schema": {
          "ref": "Error"
        }
      },
    ]
    "tags": [
      "Auth",
    ]
    "parameters": []
    "request_body": {
      "required": true
      "schema": {
        "ref": "OIDCProviderRequest"
      }
    }
    "extensions": {
      "x-authz": {
        "mode": "admin_only"
      }
      "x-apigen-manual": true
    }
  },
]
