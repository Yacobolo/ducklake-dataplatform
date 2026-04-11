package api

// Authored pipeline operations.

endpoints_pipelines: [
  {
    "method": "get",
    "path": "/pipelines",
    "operation_id": "listPipelines",
    "summary": "List pipelines",
    "tags": [
      "Pipelines"
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
          "ref": "PaginatedPipelines"
        },
        "extensions": {
          "x-apigen-response-shape": {
            "body_type": "PaginatedPipelines",
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
            "body_type": "PaginatedPipelines",
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
            "body_type": "PaginatedPipelines",
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
            "body_type": "PaginatedPipelines",
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
            "body_type": "PaginatedPipelines",
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
      "x-cli-command": "pipelines pipelines list"
    }
  },
  {
    "method": "post",
    "path": "/pipelines",
    "operation_id": "createPipeline",
    "summary": "Create pipeline",
    "tags": [
      "Pipelines"
    ],
    "request_body": {
      "required": true,
      "description": "Request payload",
      "schema": {
        "ref": "CreatePipelineRequest"
      }
    },
    "responses": [
      {
        "status_code": 201,
        "description": "The request has succeeded and a new resource has been created as a result.",
        "schema": {
          "ref": "Pipeline"
        },
        "extensions": {
          "x-apigen-response-shape": {
            "body_type": "Pipeline",
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
            "body_type": "Pipeline",
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
            "body_type": "Pipeline",
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
            "body_type": "Pipeline",
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
            "body_type": "Pipeline",
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
            "body_type": "Pipeline",
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
      "x-cli-command": "pipelines pipelines create"
    }
  },
  {
    "method": "get",
    "path": "/pipelines/{pipeline_name}",
    "operation_id": "getPipeline",
    "summary": "Get pipeline",
    "tags": [
      "Pipelines"
    ],
    "parameters": [
      {
        "name": "pipeline_name",
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
          "ref": "Pipeline"
        },
        "extensions": {
          "x-apigen-response-shape": {
            "body_type": "Pipeline",
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
            "body_type": "Pipeline",
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
            "body_type": "Pipeline",
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
            "body_type": "Pipeline",
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
            "body_type": "Pipeline",
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
            "body_type": "Pipeline",
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
            "body_type": "Pipeline",
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
      "x-cli-command": "pipelines pipelines get"
    }
  },
  {
    "method": "patch",
    "path": "/pipelines/{pipeline_name}",
    "operation_id": "updatePipeline",
    "summary": "Update pipeline",
    "tags": [
      "Pipelines"
    ],
    "parameters": [
      {
        "name": "pipeline_name",
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
        "ref": "UpdatePipelineRequest"
      }
    },
    "responses": [
      {
        "status_code": 200,
        "description": "The request has succeeded.",
        "schema": {
          "ref": "Pipeline"
        },
        "extensions": {
          "x-apigen-response-shape": {
            "body_type": "Pipeline",
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
            "body_type": "Pipeline",
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
            "body_type": "Pipeline",
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
            "body_type": "Pipeline",
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
            "body_type": "Pipeline",
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
            "body_type": "Pipeline",
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
      "x-cli-command": "pipelines pipelines update"
    }
  },
  {
    "method": "delete",
    "path": "/pipelines/{pipeline_name}",
    "operation_id": "deletePipeline",
    "summary": "Delete pipeline",
    "tags": [
      "Pipelines"
    ],
    "parameters": [
      {
        "name": "pipeline_name",
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
      "x-cli-command": "pipelines pipelines delete"
    }
  },
  {
    "method": "get",
    "path": "/pipelines/{pipeline_name}/jobs",
    "operation_id": "listPipelineJobs",
    "summary": "List pipeline jobs",
    "tags": [
      "Pipelines"
    ],
    "parameters": [
      {
        "name": "pipeline_name",
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
          "ref": "PipelineJobList"
        },
        "extensions": {
          "x-apigen-response-shape": {
            "body_type": "PipelineJobList",
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
            "body_type": "PipelineJobList",
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
            "body_type": "PipelineJobList",
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
            "body_type": "PipelineJobList",
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
            "body_type": "PipelineJobList",
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
            "body_type": "PipelineJobList",
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
            "body_type": "PipelineJobList",
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
      "x-cli-command": "pipelines jobs list"
    }
  },
  {
    "method": "post",
    "path": "/pipelines/{pipeline_name}/jobs",
    "operation_id": "createPipelineJob",
    "summary": "Create pipeline job",
    "tags": [
      "Pipelines"
    ],
    "parameters": [
      {
        "name": "pipeline_name",
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
        "ref": "CreatePipelineJobRequest"
      }
    },
    "responses": [
      {
        "status_code": 201,
        "description": "The request has succeeded and a new resource has been created as a result.",
        "schema": {
          "ref": "PipelineJob"
        },
        "extensions": {
          "x-apigen-response-shape": {
            "body_type": "PipelineJob",
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
            "body_type": "PipelineJob",
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
            "body_type": "PipelineJob",
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
            "body_type": "PipelineJob",
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
            "body_type": "PipelineJob",
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
            "body_type": "PipelineJob",
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
      "x-cli-command": "pipelines jobs create"
    }
  },
  {
    "method": "get",
    "path": "/pipelines/{pipeline_name}/jobs/{job_id}",
    "operation_id": "getPipelineJob",
    "summary": "Get pipeline job",
    "tags": [
      "Pipelines"
    ],
    "parameters": [
      {
        "name": "job_id",
        "in": "path",
        "required": true,
        "schema": {
          "type": "string"
        }
      },
      {
        "name": "pipeline_name",
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
          "ref": "PipelineJob"
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
    "method": "patch",
    "path": "/pipelines/{pipeline_name}/jobs/{job_id}",
    "operation_id": "updatePipelineJob",
    "summary": "Update pipeline job",
    "tags": [
      "Pipelines"
    ],
    "parameters": [
      {
        "name": "job_id",
        "in": "path",
        "required": true,
        "schema": {
          "type": "string"
        }
      },
      {
        "name": "pipeline_name",
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
        "ref": "UpdatePipelineJobRequest"
      }
    },
    "responses": [
      {
        "status_code": 200,
        "description": "The request has succeeded.",
        "schema": {
          "ref": "PipelineJob"
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
    "method": "delete",
    "path": "/pipelines/{pipeline_name}/jobs/{job_id}",
    "operation_id": "deletePipelineJob",
    "summary": "Delete pipeline job",
    "tags": [
      "Pipelines"
    ],
    "parameters": [
      {
        "name": "job_id",
        "in": "path",
        "required": true,
        "schema": {
          "type": "string"
        }
      },
      {
        "name": "pipeline_name",
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
      "x-cli-command": "pipelines jobs delete"
    }
  },
  {
    "method": "post",
    "path": "/pipelines/{pipeline_name}/runs",
    "operation_id": "triggerPipelineRun",
    "summary": "Trigger pipeline run",
    "tags": [
      "Pipelines"
    ],
    "parameters": [
      {
        "name": "pipeline_name",
        "in": "path",
        "required": true,
        "schema": {
          "type": "string"
        }
      }
    ],
    "request_body": {
      "description": "Request payload",
      "schema": {
        "ref": "TriggerPipelineRunRequest"
      }
    },
    "responses": [
      {
        "status_code": 200,
        "description": "The request has succeeded.",
        "schema": {
          "ref": "PipelineRun"
        },
        "extensions": {
          "x-apigen-response-shape": {
            "body_type": "PipelineRun",
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
            "body_type": "PipelineRun",
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
            "body_type": "PipelineRun",
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
            "body_type": "PipelineRun",
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
            "body_type": "PipelineRun",
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
            "body_type": "PipelineRun",
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
      "x-cli-command": "pipelines runs trigger"
    }
  },
  {
    "method": "get",
    "path": "/pipelines/{pipeline_name}/runs",
    "operation_id": "listPipelineRuns",
    "summary": "List pipeline runs",
    "tags": [
      "Pipelines"
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
      },
      {
        "name": "pipeline_name",
        "in": "path",
        "required": true,
        "schema": {
          "type": "string"
        }
      },
      {
        "name": "status",
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
          "ref": "PaginatedPipelineRuns"
        },
        "extensions": {
          "x-apigen-response-shape": {
            "body_type": "PaginatedPipelineRuns",
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
            "body_type": "PaginatedPipelineRuns",
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
            "body_type": "PaginatedPipelineRuns",
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
            "body_type": "PaginatedPipelineRuns",
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
            "body_type": "PaginatedPipelineRuns",
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
            "body_type": "PaginatedPipelineRuns",
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
            "body_type": "PaginatedPipelineRuns",
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
      "x-cli-command": "pipelines runs list"
    }
  },
  {
    "method": "get",
    "path": "/pipelines/runs/{run_id}",
    "operation_id": "getPipelineRun",
    "summary": "Get pipeline run",
    "tags": [
      "Pipelines"
    ],
    "parameters": [
      {
        "name": "run_id",
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
          "ref": "PipelineRun"
        },
        "extensions": {
          "x-apigen-response-shape": {
            "body_type": "PipelineRun",
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
            "body_type": "PipelineRun",
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
            "body_type": "PipelineRun",
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
            "body_type": "PipelineRun",
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
            "body_type": "PipelineRun",
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
            "body_type": "PipelineRun",
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
            "body_type": "PipelineRun",
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
      "x-cli-command": "pipelines runs get"
    }
  },
  {
    "method": "post",
    "path": "/pipelines/runs/{run_id}/cancellations",
    "operation_id": "cancelPipelineRun",
    "summary": "Cancel pipeline run",
    "tags": [
      "Pipelines"
    ],
    "parameters": [
      {
        "name": "run_id",
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
          "ref": "PipelineRun"
        },
        "extensions": {
          "x-apigen-response-shape": {
            "body_type": "PipelineRun",
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
            "body_type": "PipelineRun",
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
            "body_type": "PipelineRun",
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
            "body_type": "PipelineRun",
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
            "body_type": "PipelineRun",
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
            "body_type": "PipelineRun",
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
      "x-cli-command": "pipelines runs cancel"
    }
  },
  {
    "method": "get",
    "path": "/pipelines/runs/{run_id}/jobs",
    "operation_id": "listPipelineJobRuns",
    "summary": "List pipeline job runs",
    "tags": [
      "Pipelines"
    ],
    "parameters": [
      {
        "name": "run_id",
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
          "ref": "PipelineJobRunList"
        },
        "extensions": {
          "x-apigen-response-shape": {
            "body_type": "PipelineJobRunList",
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
            "body_type": "PipelineJobRunList",
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
            "body_type": "PipelineJobRunList",
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
            "body_type": "PipelineJobRunList",
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
            "body_type": "PipelineJobRunList",
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
            "body_type": "PipelineJobRunList",
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
            "body_type": "PipelineJobRunList",
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
      "x-cli-command": "pipelines runs list-job-runs"
    }
  }
]

