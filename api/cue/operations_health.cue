package api

// Authored health operations.

endpoints_health: [
  {
    "method": "get",
    "path": "/healthz",
    "operation_id": "getHealth",
    "summary": "Get health",
    "tags": [
      "Health"
    ],
    "responses": [
      {
        "status_code": 200,
        "description": "The request has succeeded.",
        "schema": {
          "ref": "HealthResponse"
        }
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
        {}
      ],
      "x-authz": {
        "mode": "none"
      }
    }
  }
]
