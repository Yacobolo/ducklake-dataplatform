package api

// Authored semantic-layer operations.

endpoints_semantic: [
  {
    "method": "get",
    "path": "/semantic-sources/{source_schema}/{source_table}/freshness",
    "operation_id": "checkSourceFreshness",
    "summary": "Check source freshness",
    "tags": [
      "Semantic Layer"
    ],
    "parameters": [
      {
        "name": "max_lag_seconds",
        "in": "query",
        "schema": {
          "type": "integer",
          "format": "int64"
        }
      },
      {
        "name": "source_schema",
        "in": "path",
        "required": true,
        "schema": {
          "type": "string"
        }
      },
      {
        "name": "source_table",
        "in": "path",
        "required": true,
        "schema": {
          "type": "string"
        }
      },
      {
        "name": "timestamp_column",
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
          "ref": "SourceFreshnessStatus"
        },
        "extensions": {
          "x-apigen-response-shape": {
            "body_type": "SourceFreshnessStatus",
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
            "body_type": "SourceFreshnessStatus",
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
            "body_type": "SourceFreshnessStatus",
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
            "body_type": "SourceFreshnessStatus",
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
            "body_type": "SourceFreshnessStatus",
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
            "body_type": "SourceFreshnessStatus",
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
            "body_type": "SourceFreshnessStatus",
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
      "x-cli-command": "models freshness check-source-freshness"
    }
  },
  {
    "method": "get",
    "path": "/semantic-models",
    "operation_id": "listSemanticModels",
    "summary": "List semantic models",
    "tags": [
      "Semantic Layer"
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
          "ref": "PaginatedSemanticModels"
        },
        "extensions": {
          "x-apigen-response-shape": {
            "body_type": "PaginatedSemanticModels",
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
            "body_type": "PaginatedSemanticModels",
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
            "body_type": "PaginatedSemanticModels",
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
            "body_type": "PaginatedSemanticModels",
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
            "body_type": "PaginatedSemanticModels",
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
      "x-cli-command": "semantic semantic-models list"
    }
  },
  {
    "method": "post",
    "path": "/semantic-models",
    "operation_id": "createSemanticModel",
    "summary": "Create semantic model",
    "tags": [
      "Semantic Layer"
    ],
    "request_body": {
      "required": true,
      "description": "Request payload",
      "schema": {
        "ref": "CreateSemanticModelRequest"
      }
    },
    "responses": [
      {
        "status_code": 201,
        "description": "The request has succeeded and a new resource has been created as a result.",
        "schema": {
          "ref": "SemanticModel"
        },
        "extensions": {
          "x-apigen-response-shape": {
            "body_type": "SemanticModel",
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
            "body_type": "SemanticModel",
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
            "body_type": "SemanticModel",
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
            "body_type": "SemanticModel",
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
            "body_type": "SemanticModel",
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
            "body_type": "SemanticModel",
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
      "x-cli-command": "semantic semantic-models create"
    }
  },
  {
    "method": "get",
    "path": "/semantic-models/{semantic_model_id}",
    "operation_id": "getSemanticModel",
    "summary": "Get semantic model",
    "tags": [
      "Semantic Layer"
    ],
    "parameters": [
      {
        "name": "semantic_model_id",
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
          "ref": "SemanticModel"
        },
        "extensions": {
          "x-apigen-response-shape": {
            "body_type": "SemanticModel",
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
            "body_type": "SemanticModel",
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
            "body_type": "SemanticModel",
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
            "body_type": "SemanticModel",
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
            "body_type": "SemanticModel",
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
            "body_type": "SemanticModel",
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
            "body_type": "SemanticModel",
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
      "x-cli-command": "semantic semantic-models get"
    }
  },
  {
    "method": "patch",
    "path": "/semantic-models/{semantic_model_id}",
    "operation_id": "updateSemanticModel",
    "summary": "Update semantic model",
    "tags": [
      "Semantic Layer"
    ],
    "parameters": [
      {
        "name": "semantic_model_id",
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
        "ref": "UpdateSemanticModelRequest"
      }
    },
    "responses": [
      {
        "status_code": 200,
        "description": "The request has succeeded.",
        "schema": {
          "ref": "SemanticModel"
        },
        "extensions": {
          "x-apigen-response-shape": {
            "body_type": "SemanticModel",
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
            "body_type": "SemanticModel",
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
            "body_type": "SemanticModel",
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
            "body_type": "SemanticModel",
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
            "body_type": "SemanticModel",
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
            "body_type": "SemanticModel",
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
      "x-cli-command": "semantic semantic-models update"
    }
  },
  {
    "method": "delete",
    "path": "/semantic-models/{semantic_model_id}",
    "operation_id": "deleteSemanticModel",
    "summary": "Delete semantic model",
    "tags": [
      "Semantic Layer"
    ],
    "parameters": [
      {
        "name": "semantic_model_id",
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
        "mode": "authenticated"
      },
      "x-cli-command": "semantic semantic-models delete"
    }
  },
  {
    "method": "get",
    "path": "/semantic-models/{semantic_model_id}/metrics",
    "operation_id": "listSemanticMetrics",
    "summary": "List semantic metrics",
    "tags": [
      "Semantic Layer"
    ],
    "parameters": [
      {
        "name": "semantic_model_id",
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
          "ref": "SemanticMetricList"
        },
        "extensions": {
          "x-apigen-response-shape": {
            "body_type": "SemanticMetricList",
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
            "body_type": "SemanticMetricList",
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
            "body_type": "SemanticMetricList",
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
            "body_type": "SemanticMetricList",
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
            "body_type": "SemanticMetricList",
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
            "body_type": "SemanticMetricList",
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
            "body_type": "SemanticMetricList",
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
      "x-cli-command": "semantic metrics list"
    }
  },
  {
    "method": "get",
    "path": "/semantic-models/{semantic_model_id}/metrics/{metric_name}",
    "operation_id": "getSemanticMetric",
    "summary": "Get semantic metric",
    "tags": [
      "Semantic Layer"
    ],
    "parameters": [
      {
        "name": "metric_name",
        "in": "path",
        "required": true,
        "schema": {
          "type": "string"
        }
      },
      {
        "name": "semantic_model_id",
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
          "ref": "SemanticMetric"
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
        "mode": "authenticated"
      }
    }
  },
  {
    "method": "post",
    "path": "/semantic-models/{semantic_model_id}/metrics",
    "operation_id": "createSemanticMetric",
    "summary": "Create semantic metric",
    "tags": [
      "Semantic Layer"
    ],
    "parameters": [
      {
        "name": "semantic_model_id",
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
        "ref": "CreateSemanticMetricRequest"
      }
    },
    "responses": [
      {
        "status_code": 201,
        "description": "The request has succeeded and a new resource has been created as a result.",
        "schema": {
          "ref": "SemanticMetric"
        },
        "extensions": {
          "x-apigen-response-shape": {
            "body_type": "SemanticMetric",
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
            "body_type": "SemanticMetric",
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
            "body_type": "SemanticMetric",
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
            "body_type": "SemanticMetric",
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
            "body_type": "SemanticMetric",
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
            "body_type": "SemanticMetric",
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
      "x-cli-command": "semantic metrics create"
    }
  },
  {
    "method": "patch",
    "path": "/semantic-models/{semantic_model_id}/metrics/{metric_name}",
    "operation_id": "updateSemanticMetric",
    "summary": "Update semantic metric",
    "tags": [
      "Semantic Layer"
    ],
    "parameters": [
      {
        "name": "metric_name",
        "in": "path",
        "required": true,
        "schema": {
          "type": "string"
        }
      },
      {
        "name": "semantic_model_id",
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
        "ref": "UpdateSemanticMetricRequest"
      }
    },
    "responses": [
      {
        "status_code": 200,
        "description": "The request has succeeded.",
        "schema": {
          "ref": "SemanticMetric"
        },
        "extensions": {
          "x-apigen-response-shape": {
            "body_type": "SemanticMetric",
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
            "body_type": "SemanticMetric",
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
            "body_type": "SemanticMetric",
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
            "body_type": "SemanticMetric",
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
            "body_type": "SemanticMetric",
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
            "body_type": "SemanticMetric",
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
      "x-cli-command": "semantic metrics update"
    }
  },
  {
    "method": "delete",
    "path": "/semantic-models/{semantic_model_id}/metrics/{metric_name}",
    "operation_id": "deleteSemanticMetric",
    "summary": "Delete semantic metric",
    "tags": [
      "Semantic Layer"
    ],
    "parameters": [
      {
        "name": "metric_name",
        "in": "path",
        "required": true,
        "schema": {
          "type": "string"
        }
      },
      {
        "name": "semantic_model_id",
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
        "mode": "authenticated"
      },
      "x-cli-command": "semantic metrics delete"
    }
  },
  {
    "method": "get",
    "path": "/semantic-models/{semantic_model_id}/pre-aggregations",
    "operation_id": "listSemanticPreAggregations",
    "summary": "List semantic pre aggregations",
    "tags": [
      "Semantic Layer"
    ],
    "parameters": [
      {
        "name": "semantic_model_id",
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
          "ref": "SemanticPreAggregationList"
        },
        "extensions": {
          "x-apigen-response-shape": {
            "body_type": "SemanticPreAggregationList",
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
            "body_type": "SemanticPreAggregationList",
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
            "body_type": "SemanticPreAggregationList",
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
            "body_type": "SemanticPreAggregationList",
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
            "body_type": "SemanticPreAggregationList",
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
            "body_type": "SemanticPreAggregationList",
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
            "body_type": "SemanticPreAggregationList",
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
      "x-cli-command": "semantic pre-aggregations list"
    }
  },
  {
    "method": "get",
    "path": "/semantic-models/{semantic_model_id}/pre-aggregations/{pre_aggregation_name}",
    "operation_id": "getSemanticPreAggregation",
    "summary": "Get semantic pre aggregation",
    "tags": [
      "Semantic Layer"
    ],
    "parameters": [
      {
        "name": "pre_aggregation_name",
        "in": "path",
        "required": true,
        "schema": {
          "type": "string"
        }
      },
      {
        "name": "semantic_model_id",
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
          "ref": "SemanticPreAggregation"
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
        "mode": "authenticated"
      }
    }
  },
  {
    "method": "post",
    "path": "/semantic-models/{semantic_model_id}/pre-aggregations",
    "operation_id": "createSemanticPreAggregation",
    "summary": "Create semantic pre aggregation",
    "tags": [
      "Semantic Layer"
    ],
    "parameters": [
      {
        "name": "semantic_model_id",
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
        "ref": "CreateSemanticPreAggregationRequest"
      }
    },
    "responses": [
      {
        "status_code": 201,
        "description": "The request has succeeded and a new resource has been created as a result.",
        "schema": {
          "ref": "SemanticPreAggregation"
        },
        "extensions": {
          "x-apigen-response-shape": {
            "body_type": "SemanticPreAggregation",
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
            "body_type": "SemanticPreAggregation",
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
            "body_type": "SemanticPreAggregation",
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
            "body_type": "SemanticPreAggregation",
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
            "body_type": "SemanticPreAggregation",
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
            "body_type": "SemanticPreAggregation",
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
      "x-cli-command": "semantic pre-aggregations create"
    }
  },
  {
    "method": "patch",
    "path": "/semantic-models/{semantic_model_id}/pre-aggregations/{pre_aggregation_name}",
    "operation_id": "updateSemanticPreAggregation",
    "summary": "Update semantic pre aggregation",
    "tags": [
      "Semantic Layer"
    ],
    "parameters": [
      {
        "name": "pre_aggregation_name",
        "in": "path",
        "required": true,
        "schema": {
          "type": "string"
        }
      },
      {
        "name": "semantic_model_id",
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
        "ref": "UpdateSemanticPreAggregationRequest"
      }
    },
    "responses": [
      {
        "status_code": 200,
        "description": "The request has succeeded.",
        "schema": {
          "ref": "SemanticPreAggregation"
        },
        "extensions": {
          "x-apigen-response-shape": {
            "body_type": "SemanticPreAggregation",
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
            "body_type": "SemanticPreAggregation",
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
            "body_type": "SemanticPreAggregation",
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
            "body_type": "SemanticPreAggregation",
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
            "body_type": "SemanticPreAggregation",
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
            "body_type": "SemanticPreAggregation",
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
      "x-cli-command": "semantic pre-aggregations update"
    }
  },
  {
    "method": "delete",
    "path": "/semantic-models/{semantic_model_id}/pre-aggregations/{pre_aggregation_name}",
    "operation_id": "deleteSemanticPreAggregation",
    "summary": "Delete semantic pre aggregation",
    "tags": [
      "Semantic Layer"
    ],
    "parameters": [
      {
        "name": "pre_aggregation_name",
        "in": "path",
        "required": true,
        "schema": {
          "type": "string"
        }
      },
      {
        "name": "semantic_model_id",
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
        "mode": "authenticated"
      },
      "x-cli-command": "semantic pre-aggregations delete"
    }
  },
  {
    "method": "get",
    "path": "/semantic-models/{semantic_model_id}/relationships",
    "operation_id": "listSemanticModelRelationships",
    "summary": "List semantic relationships for a semantic model",
    "tags": [
      "Semantic Layer"
    ],
    "parameters": [
      {
        "name": "semantic_model_id",
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
          "ref": "SemanticRelationshipList"
        },
        "extensions": {
          "x-apigen-response-shape": {
            "body_type": "SemanticRelationshipList",
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
            "body_type": "SemanticRelationshipList",
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
            "body_type": "SemanticRelationshipList",
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
            "body_type": "SemanticRelationshipList",
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
            "body_type": "SemanticRelationshipList",
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
            "body_type": "SemanticRelationshipList",
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
            "body_type": "SemanticRelationshipList",
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
      "x-cli-command": "semantic semantic-relationships list"
    }
  },
  {
    "method": "get",
    "path": "/semantic-models/{semantic_model_id}/relationships/{relationship_name}",
    "operation_id": "getSemanticModelRelationship",
    "summary": "Get semantic relationship for a semantic model",
    "tags": [
      "Semantic Layer"
    ],
    "parameters": [
      {
        "name": "relationship_name",
        "in": "path",
        "required": true,
        "schema": {
          "type": "string"
        }
      },
      {
        "name": "semantic_model_id",
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
          "ref": "SemanticRelationship"
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
        "mode": "authenticated"
      }
    }
  },
  {
    "method": "post",
    "path": "/semantic-models/{semantic_model_id}/relationships",
    "operation_id": "createSemanticModelRelationship",
    "summary": "Create semantic relationship for a semantic model",
    "tags": [
      "Semantic Layer"
    ],
    "parameters": [
      {
        "name": "semantic_model_id",
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
        "ref": "CreateSemanticRelationshipRequest"
      }
    },
    "responses": [
      {
        "status_code": 201,
        "description": "The request has succeeded and a new resource has been created as a result.",
        "schema": {
          "ref": "SemanticRelationship"
        },
        "extensions": {
          "x-apigen-response-shape": {
            "body_type": "SemanticRelationship",
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
            "body_type": "SemanticRelationship",
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
            "body_type": "SemanticRelationship",
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
            "body_type": "SemanticRelationship",
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
            "body_type": "SemanticRelationship",
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
            "body_type": "SemanticRelationship",
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
      "x-cli-command": "semantic semantic-relationships create"
    }
  },
  {
    "method": "patch",
    "path": "/semantic-models/{semantic_model_id}/relationships/{relationship_name}",
    "operation_id": "updateSemanticModelRelationship",
    "summary": "Update semantic relationship for a semantic model",
    "tags": [
      "Semantic Layer"
    ],
    "parameters": [
      {
        "name": "relationship_name",
        "in": "path",
        "required": true,
        "schema": {
          "type": "string"
        }
      },
      {
        "name": "semantic_model_id",
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
        "ref": "UpdateSemanticRelationshipRequest"
      }
    },
    "responses": [
      {
        "status_code": 200,
        "description": "The request has succeeded.",
        "schema": {
          "ref": "SemanticRelationship"
        },
        "extensions": {
          "x-apigen-response-shape": {
            "body_type": "SemanticRelationship",
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
            "body_type": "SemanticRelationship",
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
            "body_type": "SemanticRelationship",
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
            "body_type": "SemanticRelationship",
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
            "body_type": "SemanticRelationship",
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
            "body_type": "SemanticRelationship",
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
      "x-cli-command": "semantic semantic-relationships update"
    }
  },
  {
    "method": "delete",
    "path": "/semantic-models/{semantic_model_id}/relationships/{relationship_name}",
    "operation_id": "deleteSemanticModelRelationship",
    "summary": "Delete semantic relationship for a semantic model",
    "tags": [
      "Semantic Layer"
    ],
    "parameters": [
      {
        "name": "relationship_name",
        "in": "path",
        "required": true,
        "schema": {
          "type": "string"
        }
      },
      {
        "name": "semantic_model_id",
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
        "mode": "authenticated"
      },
      "x-cli-command": "semantic semantic-relationships delete"
    }
  },
  {
    "method": "post",
    "path": "/semantic-models/{semantic_model_id}/query-explanations",
    "operation_id": "explainMetricQuery",
    "summary": "Explain metric query",
    "tags": [
      "Semantic Layer"
    ],
    "parameters": [
      {
        "name": "semantic_model_id",
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
        "ref": "MetricQueryRequest"
      }
    },
    "responses": [
      {
        "status_code": 200,
        "description": "The request has succeeded.",
        "schema": {
          "ref": "MetricQueryExplainResponse"
        },
        "extensions": {
          "x-apigen-response-shape": {
            "body_type": "MetricQueryExplainResponse",
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
            "body_type": "MetricQueryExplainResponse",
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
            "body_type": "MetricQueryExplainResponse",
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
            "body_type": "MetricQueryExplainResponse",
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
            "body_type": "MetricQueryExplainResponse",
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
            "body_type": "MetricQueryExplainResponse",
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
      "x-cli-command": "semantic explain"
    }
  },
  {
    "method": "post",
    "path": "/semantic-models/{semantic_model_id}/query-runs",
    "operation_id": "runMetricQuery",
    "summary": "Run metric query",
    "tags": [
      "Semantic Layer"
    ],
    "parameters": [
      {
        "name": "semantic_model_id",
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
        "ref": "MetricQueryRequest"
      }
    },
    "responses": [
      {
        "status_code": 200,
        "description": "The request has succeeded.",
        "schema": {
          "ref": "MetricQueryRunResponse"
        },
        "extensions": {
          "x-apigen-response-shape": {
            "body_type": "MetricQueryRunResponse",
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
            "body_type": "MetricQueryRunResponse",
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
            "body_type": "MetricQueryRunResponse",
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
            "body_type": "MetricQueryRunResponse",
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
            "body_type": "MetricQueryRunResponse",
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
            "body_type": "MetricQueryRunResponse",
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
      "x-cli-command": "semantic run"
    }
  },
  {
    "method": "get",
    "path": "/semantic-metrics/{metric_name}/freshness",
    "operation_id": "checkMetricFreshness",
    "summary": "Check metric freshness",
    "tags": [
      "Semantic Layer"
    ],
    "parameters": [
      {
        "name": "metric_name",
        "in": "path",
        "required": true,
        "schema": {
          "type": "string"
        }
      },
      {
        "name": "semantic_model_id",
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
          "ref": "MetricFreshnessStatus"
        },
        "extensions": {
          "x-apigen-response-shape": {
            "body_type": "MetricFreshnessStatus",
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
            "body_type": "MetricFreshnessStatus",
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
            "body_type": "MetricFreshnessStatus",
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
            "body_type": "MetricFreshnessStatus",
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
            "body_type": "MetricFreshnessStatus",
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
            "body_type": "MetricFreshnessStatus",
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
            "body_type": "MetricFreshnessStatus",
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
      "x-cli-command": "semantic freshness check-metric-freshness"
    }
  }
]

