package api

// Authored identity operations.

endpoints_identity: [
  {
    "method": "get",
    "path": "/principals",
    "operation_id": "listPrincipals",
    "summary": "List principals",
    "tags": [
      "Identity"
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
          "ref": "PaginatedPrincipals"
        },
        "extensions": {
          "x-apigen-response-shape": {
            "body_type": "PaginatedPrincipals",
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
            "body_type": "PaginatedPrincipals",
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
            "body_type": "PaginatedPrincipals",
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
            "body_type": "PaginatedPrincipals",
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
            "body_type": "PaginatedPrincipals",
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
            "body_type": "PaginatedPrincipals",
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
      "x-cli-command": "security principals list"
    }
  },
  {
    "method": "post",
    "path": "/principals",
    "operation_id": "createPrincipal",
    "summary": "Create principal",
    "tags": [
      "Identity"
    ],
    "request_body": {
      "required": true,
      "description": "Request payload",
      "schema": {
        "ref": "CreatePrincipalRequest"
      }
    },
    "responses": [
      {
        "status_code": 201,
        "description": "The request has succeeded and a new resource has been created as a result.",
        "schema": {
          "ref": "Principal"
        },
        "extensions": {
          "x-apigen-response-shape": {
            "body_type": "Principal",
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
            "body_type": "Principal",
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
            "body_type": "Principal",
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
            "body_type": "Principal",
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
            "body_type": "Principal",
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
            "body_type": "Principal",
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
      "x-cli-command": "security principals create"
    }
  },
  {
    "method": "get",
    "path": "/principals/{principal_id}",
    "operation_id": "getPrincipal",
    "summary": "Get principal",
    "tags": [
      "Identity"
    ],
    "parameters": [
      {
        "name": "principal_id",
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
          "ref": "Principal"
        },
        "extensions": {
          "x-apigen-response-shape": {
            "body_type": "Principal",
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
            "body_type": "Principal",
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
            "body_type": "Principal",
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
            "body_type": "Principal",
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
            "body_type": "Principal",
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
            "body_type": "Principal",
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
            "body_type": "Principal",
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
      "x-cli-command": "security principals get"
    }
  },
  {
    "method": "delete",
    "path": "/principals/{principal_id}",
    "operation_id": "deletePrincipal",
    "summary": "Delete principal",
    "tags": [
      "Identity"
    ],
    "parameters": [
      {
        "name": "principal_id",
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
      "x-cli-command": "security principals delete"
    }
  },
  {
    "method": "patch",
    "path": "/principals/{principal_id}",
    "operation_id": "updatePrincipal",
    "summary": "Update principal",
    "tags": [
      "Identity"
    ],
    "parameters": [
      {
        "name": "principal_id",
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
        "ref": "UpdatePrincipalRequest"
      }
    },
    "responses": [
      {
        "status_code": 200,
        "description": "The request has succeeded.",
        "schema": {
          "ref": "Principal"
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
      },
      "x-cli-command": "security principals set-admin"
    }
  },
  {
    "method": "get",
    "path": "/groups",
    "operation_id": "listGroups",
    "summary": "List groups",
    "tags": [
      "Identity"
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
          "ref": "PaginatedGroups"
        },
        "extensions": {
          "x-apigen-response-shape": {
            "body_type": "PaginatedGroups",
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
            "body_type": "PaginatedGroups",
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
            "body_type": "PaginatedGroups",
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
            "body_type": "PaginatedGroups",
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
            "body_type": "PaginatedGroups",
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
            "body_type": "PaginatedGroups",
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
      "x-cli-command": "security groups list"
    }
  },
  {
    "method": "post",
    "path": "/groups",
    "operation_id": "createGroup",
    "summary": "Create group",
    "tags": [
      "Identity"
    ],
    "request_body": {
      "required": true,
      "description": "Request payload",
      "schema": {
        "ref": "CreateGroupRequest"
      }
    },
    "responses": [
      {
        "status_code": 201,
        "description": "The request has succeeded and a new resource has been created as a result.",
        "schema": {
          "ref": "Group"
        },
        "extensions": {
          "x-apigen-response-shape": {
            "body_type": "Group",
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
            "body_type": "Group",
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
            "body_type": "Group",
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
            "body_type": "Group",
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
            "body_type": "Group",
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
            "body_type": "Group",
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
      "x-cli-command": "security groups create"
    }
  },
  {
    "method": "get",
    "path": "/groups/{group_id}",
    "operation_id": "getGroup",
    "summary": "Get group",
    "tags": [
      "Identity"
    ],
    "parameters": [
      {
        "name": "group_id",
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
          "ref": "Group"
        },
        "extensions": {
          "x-apigen-response-shape": {
            "body_type": "Group",
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
            "body_type": "Group",
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
            "body_type": "Group",
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
            "body_type": "Group",
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
            "body_type": "Group",
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
            "body_type": "Group",
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
            "body_type": "Group",
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
      "x-cli-command": "security groups get"
    }
  },
  {
    "method": "patch",
    "path": "/groups/{group_id}",
    "operation_id": "updateGroup",
    "summary": "Update group",
    "tags": [
      "Identity"
    ],
    "parameters": [
      {
        "name": "group_id",
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
        "ref": "UpdateGroupRequest"
      }
    },
    "responses": [
      {
        "status_code": 200,
        "description": "The request has succeeded.",
        "schema": {
          "ref": "Group"
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
    "method": "delete",
    "path": "/groups/{group_id}",
    "operation_id": "deleteGroup",
    "summary": "Delete group",
    "tags": [
      "Identity"
    ],
    "parameters": [
      {
        "name": "group_id",
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
      "x-cli-command": "security groups delete"
    }
  },
  {
    "method": "get",
    "path": "/groups/{group_id}/members",
    "operation_id": "listGroupMembers",
    "summary": "List group members",
    "tags": [
      "Identity"
    ],
    "parameters": [
      {
        "name": "group_id",
        "in": "path",
        "required": true,
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
      }
    ],
    "responses": [
      {
        "status_code": 200,
        "description": "The request has succeeded.",
        "schema": {
          "ref": "PaginatedGroupMembers"
        },
        "extensions": {
          "x-apigen-response-shape": {
            "body_type": "PaginatedGroupMembers",
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
            "body_type": "PaginatedGroupMembers",
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
            "body_type": "PaginatedGroupMembers",
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
            "body_type": "PaginatedGroupMembers",
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
            "body_type": "PaginatedGroupMembers",
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
            "body_type": "PaginatedGroupMembers",
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
            "body_type": "PaginatedGroupMembers",
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
      "x-cli-command": "security members list"
    }
  },
  {
    "method": "post",
    "path": "/groups/{group_id}/members",
    "operation_id": "createGroupMember",
    "summary": "Create group member",
    "tags": [
      "Identity"
    ],
    "parameters": [
      {
        "name": "group_id",
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
        "ref": "CreateGroupMemberRequest"
      }
    },
    "responses": [
      {
        "status_code": 201,
        "description": "The request has succeeded and a new resource has been created as a result."
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
      "x-cli-command": "security members add"
    }
  },
  {
    "method": "delete",
    "path": "/groups/{group_id}/members/{member_type}/{member_id}",
    "operation_id": "deleteGroupMember",
    "summary": "Delete group member",
    "tags": [
      "Identity"
    ],
    "parameters": [
      {
        "name": "group_id",
        "in": "path",
        "required": true,
        "schema": {
          "type": "string"
        }
      },
      {
        "name": "member_id",
        "in": "path",
        "required": true,
        "schema": {
          "type": "string"
        }
      },
      {
        "name": "member_type",
        "in": "path",
        "required": true,
        "schema": {
          "ref": "PrincipalType"
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
      "x-cli-command": "security members remove"
    }
  },
  {
    "method": "get",
    "path": "/api-keys",
    "operation_id": "listAPIKeys",
    "summary": "List API keys",
    "tags": [
      "Identity"
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
        "name": "principal_id",
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
          "ref": "PaginatedAPIKeys"
        },
        "extensions": {
          "x-apigen-response-shape": {
            "body_type": "PaginatedAPIKeys",
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
            "body_type": "PaginatedAPIKeys",
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
            "body_type": "PaginatedAPIKeys",
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
            "body_type": "PaginatedAPIKeys",
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
            "body_type": "PaginatedAPIKeys",
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
      "x-cli-command": "security api-keys list"
    }
  },
  {
    "method": "post",
    "path": "/api-keys",
    "operation_id": "createAPIKey",
    "summary": "Create API key",
    "tags": [
      "Identity"
    ],
    "request_body": {
      "required": true,
      "description": "Request payload",
      "schema": {
        "ref": "CreateAPIKeyRequest"
      }
    },
    "responses": [
      {
        "status_code": 201,
        "description": "The request has succeeded and a new resource has been created as a result.",
        "schema": {
          "ref": "CreateAPIKeyResponse"
        },
        "extensions": {
          "x-apigen-response-shape": {
            "body_type": "CreateAPIKeyResponse",
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
            "body_type": "CreateAPIKeyResponse",
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
            "body_type": "CreateAPIKeyResponse",
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
            "body_type": "CreateAPIKeyResponse",
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
            "body_type": "CreateAPIKeyResponse",
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
            "body_type": "CreateAPIKeyResponse",
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
      "x-cli-command": "security api-keys create"
    }
  },
  {
    "method": "delete",
    "path": "/api-keys/{api_key_id}",
    "operation_id": "deleteAPIKey",
    "summary": "Delete API key",
    "tags": [
      "Identity"
    ],
    "parameters": [
      {
        "name": "api_key_id",
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
      "x-cli-command": "security api-keys delete"
    }
  },
  {
    "method": "post",
    "path": "/api-key-cleanup-runs",
    "operation_id": "cleanupExpiredAPIKeys",
    "summary": "Clean up expired API keys",
    "tags": [
      "Identity"
    ],
    "responses": [
      {
        "status_code": 200,
        "description": "The request has succeeded.",
        "schema": {
          "ref": "CleanupAPIKeysResponse"
        },
        "extensions": {
          "x-apigen-response-shape": {
            "body_type": "CleanupAPIKeysResponse",
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
            "body_type": "CleanupAPIKeysResponse",
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
            "body_type": "CleanupAPIKeysResponse",
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
            "body_type": "CleanupAPIKeysResponse",
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
            "body_type": "CleanupAPIKeysResponse",
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
            "body_type": "CleanupAPIKeysResponse",
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
      "x-cli-command": "security api-keys cleanup"
    }
  }
]

