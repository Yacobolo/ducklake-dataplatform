package api

// Authored core schemas.

schemas_core: {
  "APIKeyInfo": {
    "type": "object",
    "properties": {
      "created_at": {
        "schema": {
          "type": "string"
        }
      },
      "expires_at": {
        "schema": {
          "type": "string"
        }
      },
      "id": {
        "schema": {
          "type": "string"
        }
      },
      "key_prefix": {
        "schema": {
          "type": "string"
        }
      },
      "name": {
        "schema": {
          "type": "string"
        }
      },
      "principal_id": {
        "schema": {
          "type": "string"
        }
      }
    },
    "required": [
      "id",
      "principal_id",
      "name"
    ]
  },
  "AddWorkspaceMemberRequest": {
    "type": "object",
    "properties": {
      "principal_name": {
        "schema": {
          "type": "string"
        }
      },
      "role": {
        "schema": {
          "ref": "NotebookShareRole"
        }
      }
    },
    "required": [
      "principal_name"
    ]
  },
  "AuditDecisionStatus": {
    "type": "string",
    "enum": [
      "ALLOWED",
      "DENIED",
      "ERROR"
    ]
  },
  "AuditEntry": {
    "type": "object",
    "properties": {
      "action": {
        "schema": {
          "type": "string"
        }
      },
      "created_at": {
        "schema": {
          "type": "string"
        }
      },
      "duration_ms": {
        "schema": {
          "type": "integer",
          "format": "int64"
        }
      },
      "error_message": {
        "schema": {
          "type": "string"
        }
      },
      "id": {
        "schema": {
          "type": "string"
        }
      },
      "original_sql": {
        "schema": {
          "type": "string"
        }
      },
      "principal_name": {
        "schema": {
          "type": "string"
        }
      },
      "rewritten_sql": {
        "schema": {
          "type": "string"
        }
      },
      "statement_type": {
        "schema": {
          "type": "string"
        }
      },
      "status": {
        "schema": {
          "ref": "AuditDecisionStatus"
        }
      },
      "tables_accessed": {
        "schema": {
          "type": "array",
          "items": {
            "type": "string"
          }
        }
      }
    },
    "required": [
      "id"
    ]
  },
  "AuthLoginResponse": {
    "type": "object",
    "properties": {
      "principal": {
        "schema": {
          "ref": "AuthPrincipalSummary"
        }
      },
      "token": {
        "schema": {
          "type": "string"
        }
      }
    },
    "required": [
      "token",
      "principal"
    ]
  },
  "AuthPrincipalSummary": {
    "type": "object",
    "properties": {
      "id": {
        "schema": {
          "type": "string"
        }
      },
      "is_admin": {
        "schema": {
          "type": "boolean"
        }
      },
      "name": {
        "schema": {
          "type": "string"
        }
      }
    },
    "required": [
      "id",
      "name",
      "is_admin"
    ]
  },
  "BootstrapCompleteRequest": {
    "type": "object",
    "properties": {
      "bootstrap_token": {
        "schema": {
          "type": "string"
        }
      },
      "password": {
        "schema": {
          "type": "string"
        }
      },
      "principal_name": {
        "schema": {
          "type": "string"
        }
      },
      "username": {
        "schema": {
          "type": "string"
        }
      }
    },
    "required": [
      "username",
      "password"
    ]
  },
  "BootstrapTokenRequest": {
    "type": "object",
    "properties": {
      "ttl_seconds": {
        "schema": {
          "type": "integer",
          "format": "int64"
        }
      }
    }
  },
  "BootstrapTokenResponse": {
    "type": "object",
    "properties": {
      "bootstrap_token": {
        "schema": {
          "type": "string"
        }
      },
      "ttl_seconds": {
        "schema": {
          "type": "integer",
          "format": "int64"
        }
      }
    },
    "required": [
      "bootstrap_token",
      "ttl_seconds"
    ]
  },
  "Build": {
    "type": "object",
    "properties": {
      "commit_sha": {
        "schema": {
          "type": "string"
        }
      },
      "compile_diagnostics": {
        "schema": {
          "type": "string"
        }
      },
      "compile_manifest": {
        "schema": {
          "type": "string"
        }
      },
      "created_at": {
        "schema": {
          "type": "string"
        }
      },
      "environment_id": {
        "schema": {
          "type": "string"
        }
      },
      "environment_name": {
        "schema": {
          "type": "string"
        }
      },
      "git_ref": {
        "schema": {
          "type": "string"
        }
      },
      "id": {
        "schema": {
          "type": "string"
        }
      },
      "product_id": {
        "schema": {
          "type": "string"
        }
      },
      "project_id": {
        "schema": {
          "type": "string"
        }
      },
      "project_name": {
        "schema": {
          "type": "string"
        }
      },
      "selector": {
        "schema": {
          "type": "string"
        }
      },
      "source_model_run_id": {
        "schema": {
          "type": "string"
        }
      },
      "state": {
        "schema": {
          "ref": "BuildState"
        }
      },
      "target_catalog": {
        "schema": {
          "type": "string"
        }
      },
      "target_schema": {
        "schema": {
          "type": "string"
        }
      }
    },
    "required": [
      "git_ref",
      "target_catalog",
      "target_schema",
      "compile_manifest"
    ]
  },
  "BuildState": {
    "type": "string",
    "enum": [
      "draft",
      "ready",
      "released",
      "superseded"
    ]
  },
  "CancelQueryResponse": {
    "type": "object",
    "properties": {
      "query_id": {
        "schema": {
          "type": "string"
        }
      },
      "status": {
        "schema": {
          "ref": "QueryJobStatus"
        }
      }
    },
    "required": [
      "query_id",
      "status"
    ]
  },
  "CatalogHistoryEntry": {
    "type": "object",
    "properties": {
      "begin_snapshot_id": {
        "schema": {
          "type": "integer",
          "format": "int64"
        }
      },
      "column_name": {
        "schema": {
          "type": "string"
        }
      },
      "end_snapshot_id": {
        "schema": {
          "type": "integer",
          "format": "int64"
        }
      },
      "entity_type": {
        "schema": {
          "type": "string"
        }
      },
      "has_history": {
        "schema": {
          "type": "boolean"
        }
      },
      "is_active": {
        "schema": {
          "type": "boolean"
        }
      },
      "latest_snapshot_id": {
        "schema": {
          "type": "integer",
          "format": "int64"
        }
      },
      "object_id": {
        "schema": {
          "type": "string"
        }
      },
      "object_name": {
        "schema": {
          "type": "string"
        }
      },
      "schema_name": {
        "schema": {
          "type": "string"
        }
      },
      "table_name": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "CatalogHistoryResponse": {
    "type": "object",
    "properties": {
      "data": {
        "schema": {
          "type": "array",
          "items": {
            "ref": "CatalogHistoryEntry"
          }
        }
      }
    },
    "required": [
      "data"
    ]
  },
  "CatalogRegistration": {
    "type": "object",
    "properties": {
      "comment": {
        "schema": {
          "type": "string"
        }
      },
      "created_at": {
        "schema": {
          "type": "string"
        }
      },
      "data_path": {
        "schema": {
          "type": "string"
        }
      },
      "dsn": {
        "schema": {
          "type": "string"
        }
      },
      "id": {
        "schema": {
          "type": "string"
        }
      },
      "is_default": {
        "schema": {
          "type": "boolean"
        }
      },
      "metastore_type": {
        "schema": {
          "ref": "MetastoreType"
        }
      },
      "name": {
        "schema": {
          "type": "string"
        }
      },
      "status": {
        "schema": {
          "ref": "CatalogStatus"
        }
      },
      "system_managed": {
        "schema": {
          "type": "boolean"
        }
      },
      "updated_at": {
        "schema": {
          "type": "string"
        }
      }
    },
    "required": [
      "id",
      "name"
    ]
  },
  "CatalogRegistrationList": {
    "type": "object",
    "properties": {
      "catalogs": {
        "schema": {
          "type": "array",
          "items": {
            "ref": "CatalogRegistration"
          }
        }
      },
      "next_page_token": {
        "schema": {
          "type": "string"
        }
      },
      "total_count": {
        "schema": {
          "type": "integer",
          "format": "int32"
        }
      }
    },
    "required": [
      "catalogs"
    ]
  },
  "CatalogStatus": {
    "type": "string",
    "enum": [
      "ACTIVE",
      "ERROR",
      "DETACHED"
    ]
  },
  "CatalogVersionSummary": {
    "type": "object",
    "properties": {
      "catalog_name": {
        "schema": {
          "type": "string"
        }
      },
      "columns": {
        "schema": {
          "ref": "VersionedObjectSummary"
        }
      },
      "created_by": {
        "schema": {
          "type": "string"
        }
      },
      "data_path": {
        "schema": {
          "type": "string"
        }
      },
      "encrypted": {
        "schema": {
          "type": "boolean"
        }
      },
      "latest_snapshot_id": {
        "schema": {
          "type": "integer",
          "format": "int64"
        }
      },
      "schemas": {
        "schema": {
          "ref": "VersionedObjectSummary"
        }
      },
      "tables": {
        "schema": {
          "ref": "VersionedObjectSummary"
        }
      },
      "version": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "Cell": {
    "type": "object",
    "properties": {
      "cell_type": {
        "schema": {
          "ref": "CellCellType"
        }
      },
      "content": {
        "schema": {
          "type": "string"
        }
      },
      "created_at": {
        "schema": {
          "type": "string"
        }
      },
      "disabled": {
        "schema": {
          "type": "boolean"
        }
      },
      "id": {
        "schema": {
          "type": "string"
        }
      },
      "last_result": {
        "schema": {
          "type": "string"
        }
      },
      "name": {
        "schema": {
          "type": "string"
        }
      },
      "notebook_id": {
        "schema": {
          "type": "string"
        }
      },
      "position": {
        "schema": {
          "type": "integer",
          "format": "int32"
        }
      },
      "role": {
        "schema": {
          "ref": "CellRole"
        }
      },
      "test": {
        "schema": {
          "ref": "NotebookCellTestConfig"
        }
      },
      "updated_at": {
        "schema": {
          "type": "string"
        }
      },
      "visual_spec": {
        "schema": {
          "ref": "VisualSpec"
        }
      }
    }
  },
  "CellCellType": {
    "type": "string",
    "enum": [
      "sql",
      "markdown"
    ]
  },
  "CellExecutionResult": {
    "type": "object",
    "properties": {
      "cell_id": {
        "schema": {
          "type": "string"
        }
      },
      "columns": {
        "schema": {
          "type": "array",
          "items": {
            "ref": "TabularColumn"
          }
        }
      },
      "duration_ms": {
        "schema": {
          "type": "integer",
          "format": "int64"
        }
      },
      "error": {
        "schema": {
          "type": "string"
        }
      },
      "row_count": {
        "schema": {
          "type": "integer",
          "format": "int32"
        }
      },
      "rows": {
        "schema": {
          "type": "array",
          "items": {
            "ref": "Record"
          }
        }
      }
    }
  },
  "CellList": {
    "type": "object",
    "properties": {
      "data": {
        "schema": {
          "type": "array",
          "items": {
            "ref": "Cell"
          }
        }
      }
    },
    "required": [
      "data"
    ]
  },
  "CellRole": {
    "type": "string",
    "enum": [
      "transform",
      "output",
      "test",
      "markdown"
    ]
  },
  "CleanupAPIKeysResponse": {
    "type": "object",
    "properties": {
      "deleted_count": {
        "schema": {
          "type": "integer",
          "format": "int32"
        }
      }
    },
    "required": [
      "deleted_count"
    ]
  },
  "ColumnDetail": {
    "type": "object",
    "properties": {
      "comment": {
        "schema": {
          "type": "string"
        }
      },
      "name": {
        "schema": {
          "type": "string"
        }
      },
      "nullable": {
        "schema": {
          "type": "boolean"
        }
      },
      "position": {
        "schema": {
          "type": "integer",
          "format": "int32"
        }
      },
      "type": {
        "schema": {
          "type": "string"
        }
      }
    },
    "required": [
      "name",
      "type"
    ]
  },
  "ColumnLineageEdge": {
    "type": "object",
    "properties": {
      "function": {
        "schema": {
          "type": "string"
        }
      },
      "id": {
        "schema": {
          "type": "integer",
          "format": "int64"
        }
      },
      "lineage_edge_id": {
        "schema": {
          "type": "string"
        }
      },
      "source_column": {
        "schema": {
          "type": "string"
        }
      },
      "source_schema": {
        "schema": {
          "type": "string"
        }
      },
      "source_table": {
        "schema": {
          "type": "string"
        }
      },
      "target_column": {
        "schema": {
          "type": "string"
        }
      },
      "transform_type": {
        "schema": {
          "ref": "ColumnLineageEdgeTransformType"
        }
      }
    }
  },
  "ColumnLineageEdgeTransformType": {
    "type": "string",
    "enum": [
      "DIRECT",
      "EXPRESSION"
    ]
  },
  "ColumnMask": {
    "type": "object",
    "properties": {
      "column_name": {
        "schema": {
          "type": "string"
        }
      },
      "created_at": {
        "schema": {
          "type": "string"
        }
      },
      "description": {
        "schema": {
          "type": "string"
        }
      },
      "id": {
        "schema": {
          "type": "string"
        }
      },
      "mask_expression": {
        "schema": {
          "type": "string"
        }
      },
      "name": {
        "schema": {
          "type": "string"
        }
      },
      "table_id": {
        "schema": {
          "type": "string"
        }
      }
    },
    "required": [
      "id",
      "table_id",
      "name",
      "column_name",
      "mask_expression"
    ]
  },
  "ColumnMaskBinding": {
    "type": "object",
    "properties": {
      "column_mask_id": {
        "schema": {
          "type": "string"
        }
      },
      "id": {
        "schema": {
          "type": "string"
        }
      },
      "principal_id": {
        "schema": {
          "type": "string"
        }
      },
      "principal_type": {
        "schema": {
          "ref": "PrincipalType"
        }
      },
      "see_original": {
        "schema": {
          "type": "boolean"
        }
      }
    }
  },
  "ColumnMaskBindingRequest": {
    "type": "object",
    "properties": {
      "principal_id": {
        "schema": {
          "type": "string"
        }
      },
      "principal_type": {
        "schema": {
          "ref": "PrincipalType"
        }
      },
      "see_original": {
        "schema": {
          "type": "boolean"
        }
      }
    },
    "required": [
      "principal_id",
      "principal_type"
    ]
  },
  "ComputeAssignment": {
    "type": "object",
    "properties": {
      "created_at": {
        "schema": {
          "type": "string"
        }
      },
      "endpoint_id": {
        "schema": {
          "type": "string"
        }
      },
      "endpoint_name": {
        "schema": {
          "type": "string"
        }
      },
      "fallback_local": {
        "schema": {
          "type": "boolean"
        }
      },
      "id": {
        "schema": {
          "type": "string"
        }
      },
      "is_default": {
        "schema": {
          "type": "boolean"
        }
      },
      "principal_id": {
        "schema": {
          "type": "string"
        }
      },
      "principal_type": {
        "schema": {
          "ref": "ComputeAssignmentPrincipalType"
        }
      }
    }
  },
  "ComputeAssignmentPrincipalType": {
    "type": "string",
    "enum": [
      "user",
      "group"
    ]
  },
  "ComputeEndpoint": {
    "type": "object",
    "properties": {
      "created_at": {
        "schema": {
          "type": "string"
        }
      },
      "external_id": {
        "schema": {
          "type": "string"
        }
      },
      "id": {
        "schema": {
          "type": "string"
        }
      },
      "max_memory_gb": {
        "schema": {
          "type": "integer",
          "format": "int64"
        }
      },
      "name": {
        "schema": {
          "type": "string"
        }
      },
      "owner": {
        "schema": {
          "type": "string"
        }
      },
      "size": {
        "schema": {
          "ref": "ComputeEndpointSize"
        }
      },
      "status": {
        "schema": {
          "ref": "ComputeEndpointStatus"
        }
      },
      "type": {
        "schema": {
          "ref": "ComputeEndpointType"
        }
      },
      "updated_at": {
        "schema": {
          "type": "string"
        }
      },
      "url": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "ComputeEndpointHealth": {
    "type": "object",
    "properties": {
      "duckdb_version": {
        "schema": {
          "type": "string"
        }
      },
      "endpoint_name": {
        "schema": {
          "type": "string"
        }
      },
      "max_memory_gb": {
        "schema": {
          "type": "integer",
          "format": "int32"
        }
      },
      "memory_used_mb": {
        "schema": {
          "type": "integer",
          "format": "int32"
        }
      },
      "status": {
        "schema": {
          "type": "string"
        }
      },
      "uptime_seconds": {
        "schema": {
          "type": "integer",
          "format": "int32"
        }
      }
    }
  },
  "ComputeEndpointSize": {
    "type": "string",
    "enum": [
      "SMALL",
      "MEDIUM",
      "LARGE"
    ]
  },
  "ComputeEndpointStatus": {
    "type": "string",
    "enum": [
      "ACTIVE",
      "INACTIVE",
      "STARTING",
      "STOPPING",
      "ERROR"
    ]
  },
  "ComputeEndpointType": {
    "type": "string",
    "enum": [
      "LOCAL",
      "REMOTE"
    ]
  },
  "ComputeRoutingDefaults": {
    "type": "object",
    "properties": {
      "interactive_mode": {
        "schema": {
          "type": "string"
        }
      },
      "notebook_mode": {
        "schema": {
          "type": "string"
        }
      },
      "scheduled_mode": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "CreateAPIKeyRequest": {
    "type": "object",
    "properties": {
      "expires_at": {
        "schema": {
          "type": "string"
        }
      },
      "name": {
        "schema": {
          "type": "string"
        }
      },
      "principal_id": {
        "schema": {
          "type": "string"
        }
      }
    },
    "required": [
      "principal_id"
    ]
  },
  "CreateAPIKeyResponse": {
    "type": "object",
    "properties": {
      "created_at": {
        "schema": {
          "type": "string"
        }
      },
      "expires_at": {
        "schema": {
          "type": "string"
        }
      },
      "id": {
        "schema": {
          "type": "string"
        }
      },
      "key": {
        "schema": {
          "type": "string"
        }
      },
      "key_prefix": {
        "schema": {
          "type": "string"
        }
      },
      "name": {
        "schema": {
          "type": "string"
        }
      }
    },
    "required": [
      "id",
      "key"
    ]
  },
  "CreateBuildRequest": {
    "type": "object",
    "properties": {
      "commit_sha": {
        "schema": {
          "type": "string"
        }
      },
      "compile_diagnostics": {
        "schema": {
          "type": "string"
        }
      },
      "compile_manifest": {
        "schema": {
          "type": "string"
        }
      },
      "environment_name": {
        "schema": {
          "type": "string"
        }
      },
      "git_ref": {
        "schema": {
          "type": "string"
        }
      },
      "selector": {
        "schema": {
          "type": "string"
        }
      },
      "source_model_run_id": {
        "schema": {
          "type": "string"
        }
      },
      "target_catalog": {
        "schema": {
          "type": "string"
        }
      },
      "target_schema": {
        "schema": {
          "type": "string"
        }
      }
    },
    "required": [
      "environment_name",
      "git_ref",
      "target_catalog",
      "target_schema",
      "compile_manifest"
    ]
  },
  "CreateCatalogRequest": {
    "type": "object",
    "properties": {
      "comment": {
        "schema": {
          "type": "string"
        }
      },
      "data_path": {
        "schema": {
          "type": "string"
        }
      },
      "dsn": {
        "schema": {
          "type": "string"
        }
      },
      "metastore_type": {
        "schema": {
          "ref": "MetastoreType"
        }
      },
      "name": {
        "schema": {
          "type": "string"
        }
      }
    },
    "required": [
      "name"
    ]
  },
  "CreateCellRequest": {
    "type": "object",
    "properties": {
      "cell_type": {
        "schema": {
          "ref": "CellCellType"
        }
      },
      "content": {
        "schema": {
          "type": "string"
        }
      },
      "disabled": {
        "schema": {
          "type": "boolean"
        }
      },
      "name": {
        "schema": {
          "type": "string"
        }
      },
      "position": {
        "schema": {
          "type": "integer",
          "format": "int32"
        }
      },
      "role": {
        "schema": {
          "ref": "CellRole"
        }
      },
      "test": {
        "schema": {
          "ref": "NotebookCellTestConfig"
        }
      },
      "visual_spec": {
        "schema": {
          "ref": "VisualSpec"
        }
      }
    },
    "required": [
      "cell_type"
    ]
  },
  "CreateColumnMaskRequest": {
    "type": "object",
    "properties": {
      "column_name": {
        "schema": {
          "type": "string"
        }
      },
      "description": {
        "schema": {
          "type": "string"
        }
      },
      "mask_expression": {
        "schema": {
          "type": "string"
        }
      },
      "name": {
        "schema": {
          "type": "string"
        }
      },
      "table_id": {
        "schema": {
          "type": "string"
        }
      }
    },
    "required": [
      "name",
      "column_name",
      "mask_expression"
    ]
  },
  "CreateColumnRequest": {
    "type": "object",
    "properties": {
      "comment": {
        "schema": {
          "type": "string"
        }
      },
      "name": {
        "schema": {
          "type": "string"
        }
      },
      "nullable": {
        "schema": {
          "type": "boolean"
        }
      },
      "type": {
        "schema": {
          "type": "string"
        }
      }
    },
    "required": [
      "name",
      "type"
    ]
  },
  "CreateComputeAssignmentRequest": {
    "type": "object",
    "properties": {
      "fallback_local": {
        "schema": {
          "type": "boolean"
        }
      },
      "is_default": {
        "schema": {
          "type": "boolean"
        }
      },
      "principal_id": {
        "schema": {
          "type": "string"
        }
      },
      "principal_type": {
        "schema": {
          "ref": "ComputeAssignmentPrincipalType"
        }
      }
    },
    "required": [
      "principal_id",
      "principal_type"
    ]
  },
  "CreateComputeEndpointRequest": {
    "type": "object",
    "properties": {
      "auth_token": {
        "schema": {
          "type": "string"
        }
      },
      "max_memory_gb": {
        "schema": {
          "type": "integer",
          "format": "int64"
        }
      },
      "name": {
        "schema": {
          "type": "string"
        }
      },
      "size": {
        "schema": {
          "ref": "ComputeEndpointSize"
        }
      },
      "type": {
        "schema": {
          "ref": "ComputeEndpointType"
        }
      },
      "url": {
        "schema": {
          "type": "string"
        }
      }
    },
    "required": [
      "name",
      "type",
      "url"
    ]
  },
  "CreateDashboardRequest": {
    "type": "object",
    "properties": {
      "description": {
        "schema": {
          "type": "string"
        }
      },
      "folder_id": {
        "schema": {
          "type": "string"
        }
      },
      "name": {
        "schema": {
          "type": "string"
        }
      }
    },
    "required": [
      "name"
    ]
  },
  "CreateDashboardWidgetRequest": {
    "type": "object",
    "properties": {
      "description": {
        "schema": {
          "type": "string"
        }
      },
      "layout": {
        "schema": {
          "ref": "DashboardWidgetLayout"
        }
      },
      "name": {
        "schema": {
          "type": "string"
        }
      },
      "source": {
        "schema": {
          "ref": "DashboardWidgetSource"
        }
      },
      "visual_spec": {
        "schema": {
          "ref": "VisualSpec"
        }
      }
    },
    "required": [
      "name",
      "source",
      "layout"
    ]
  },
  "CreateEnvironmentRequest": {
    "type": "object",
    "properties": {
      "compute_endpoint": {
        "schema": {
          "type": "string"
        }
      },
      "defer_to_environment": {
        "schema": {
          "type": "string"
        }
      },
      "description": {
        "schema": {
          "type": "string"
        }
      },
      "kind": {
        "schema": {
          "ref": "EnvironmentKind"
        }
      },
      "name": {
        "schema": {
          "type": "string"
        }
      },
      "source_overrides": {
        "schema": {
          "ref": "Record"
        }
      },
      "target_catalog": {
        "schema": {
          "type": "string"
        }
      },
      "target_schema": {
        "schema": {
          "type": "string"
        }
      },
      "variables": {
        "schema": {
          "ref": "Record"
        }
      }
    },
    "required": [
      "name",
      "target_catalog",
      "target_schema"
    ]
  },
  "CreateExternalLocationRequest": {
    "type": "object",
    "properties": {
      "comment": {
        "schema": {
          "type": "string"
        }
      },
      "credential_name": {
        "schema": {
          "type": "string"
        }
      },
      "name": {
        "schema": {
          "type": "string"
        }
      },
      "read_only": {
        "schema": {
          "type": "boolean"
        }
      },
      "storage_type": {
        "schema": {
          "ref": "StorageType"
        }
      },
      "url": {
        "schema": {
          "type": "string"
        }
      }
    },
    "required": [
      "name",
      "url"
    ]
  },
  "CreateFolderRequest": {
    "type": "object",
    "properties": {
      "default_environment_id": {
        "schema": {
          "type": "string"
        }
      },
      "default_project_id": {
        "schema": {
          "type": "string"
        }
      },
      "git_repo_id": {
        "schema": {
          "type": "string"
        }
      },
      "git_root_path": {
        "schema": {
          "type": "string"
        }
      },
      "name": {
        "schema": {
          "type": "string"
        }
      },
      "parent_folder_id": {
        "schema": {
          "type": "string"
        }
      }
    },
    "required": [
      "name"
    ]
  },
  "CreateGitRepoRequest": {
    "type": "object",
    "properties": {
      "auth_token": {
        "schema": {
          "type": "string"
        }
      },
      "branch": {
        "schema": {
          "type": "string"
        }
      },
      "path": {
        "schema": {
          "type": "string"
        }
      },
      "url": {
        "schema": {
          "type": "string"
        }
      }
    },
    "required": [
      "url",
      "branch"
    ]
  },
  "CreateGrantRequest": {
    "type": "object",
    "properties": {
      "principal_id": {
        "schema": {
          "type": "string"
        }
      },
      "principal_type": {
        "schema": {
          "ref": "PrincipalType"
        }
      },
      "privilege": {
        "schema": {
          "type": "string"
        }
      },
      "securable_id": {
        "schema": {
          "type": "string"
        }
      },
      "securable_type": {
        "schema": {
          "type": "string"
        }
      }
    },
    "required": [
      "principal_id",
      "principal_type",
      "securable_type",
      "securable_id",
      "privilege"
    ]
  },
  "CreateGroupMemberRequest": {
    "type": "object",
    "properties": {
      "member_id": {
        "schema": {
          "type": "string"
        }
      },
      "member_type": {
        "schema": {
          "ref": "PrincipalType"
        }
      }
    },
    "required": [
      "member_type",
      "member_id"
    ]
  },
  "CreateGroupRequest": {
    "type": "object",
    "properties": {
      "description": {
        "schema": {
          "type": "string"
        }
      },
      "name": {
        "schema": {
          "type": "string"
        }
      }
    },
    "required": [
      "name"
    ]
  },
  "CreateMacroRequest": {
    "type": "object",
    "properties": {
      "body": {
        "schema": {
          "type": "string"
        }
      },
      "catalog_name": {
        "schema": {
          "type": "string"
        }
      },
      "description": {
        "schema": {
          "type": "string"
        }
      },
      "macro_type": {
        "schema": {
          "ref": "MacroType"
        }
      },
      "name": {
        "schema": {
          "type": "string"
        }
      },
      "owner": {
        "schema": {
          "type": "string"
        }
      },
      "parameters": {
        "schema": {
          "type": "array",
          "items": {
            "type": "string"
          }
        }
      },
      "project_name": {
        "schema": {
          "type": "string"
        }
      },
      "properties": {
        "schema": {
          "ref": "Record"
        }
      },
      "status": {
        "schema": {
          "ref": "MacroStatus"
        }
      },
      "tags": {
        "schema": {
          "type": "array",
          "items": {
            "type": "string"
          }
        }
      },
      "visibility": {
        "schema": {
          "ref": "MacroVisibility"
        }
      }
    },
    "required": [
      "name",
      "body"
    ]
  },
  "CreateModelRequest": {
    "type": "object",
    "properties": {
      "config": {
        "schema": {
          "ref": "ModelConfig"
        }
      },
      "contract": {
        "schema": {
          "ref": "ModelContract"
        }
      },
      "description": {
        "schema": {
          "type": "string"
        }
      },
      "freshness_policy": {
        "schema": {
          "ref": "FreshnessPolicy"
        }
      },
      "materialization": {
        "schema": {
          "ref": "ModelMaterialization"
        }
      },
      "name": {
        "schema": {
          "type": "string"
        }
      },
      "project_name": {
        "schema": {
          "type": "string"
        }
      },
      "sql": {
        "schema": {
          "type": "string"
        }
      },
      "tags": {
        "schema": {
          "type": "array",
          "items": {
            "type": "string"
          }
        }
      }
    },
    "required": [
      "project_name",
      "name",
      "sql"
    ]
  },
  "CreateModelTestRequest": {
    "type": "object",
    "properties": {
      "column": {
        "schema": {
          "type": "string"
        }
      },
      "config": {
        "schema": {
          "ref": "ModelTestConfig"
        }
      },
      "name": {
        "schema": {
          "type": "string"
        }
      },
      "test_type": {
        "schema": {
          "ref": "ModelTestTestType"
        }
      }
    },
    "required": [
      "name",
      "test_type"
    ]
  },
  "CreateNotebookRequest": {
    "type": "object",
    "properties": {
      "description": {
        "schema": {
          "type": "string"
        }
      },
      "folder_id": {
        "schema": {
          "type": "string"
        }
      },
      "name": {
        "schema": {
          "type": "string"
        }
      },
      "source": {
        "schema": {
          "type": "string"
        }
      }
    },
    "required": [
      "name"
    ]
  },
  "CreatePipelineJobRequest": {
    "type": "object",
    "properties": {
      "compute_endpoint_id": {
        "schema": {
          "type": "string"
        }
      },
      "depends_on": {
        "schema": {
          "type": "array",
          "items": {
            "type": "string"
          }
        }
      },
      "job_order": {
        "schema": {
          "type": "integer",
          "format": "int32"
        }
      },
      "job_type": {
        "schema": {
          "ref": "PipelineJobJobType"
        }
      },
      "model_selector": {
        "schema": {
          "type": "string"
        }
      },
      "name": {
        "schema": {
          "type": "string"
        }
      },
      "notebook_id": {
        "schema": {
          "type": "string"
        }
      },
      "retry_count": {
        "schema": {
          "type": "integer",
          "format": "int32"
        }
      },
      "timeout_seconds": {
        "schema": {
          "type": "integer",
          "format": "int64"
        }
      }
    },
    "required": [
      "name"
    ]
  },
  "CreatePipelineRequest": {
    "type": "object",
    "properties": {
      "concurrency_limit": {
        "schema": {
          "type": "integer",
          "format": "int32"
        }
      },
      "description": {
        "schema": {
          "type": "string"
        }
      },
      "folder_id": {
        "schema": {
          "type": "string"
        }
      },
      "is_paused": {
        "schema": {
          "type": "boolean"
        }
      },
      "name": {
        "schema": {
          "type": "string"
        }
      },
      "schedule_cron": {
        "schema": {
          "type": "string"
        }
      }
    },
    "required": [
      "name"
    ]
  },
  "CreatePrincipalRequest": {
    "type": "object",
    "properties": {
      "is_admin": {
        "schema": {
          "type": "boolean"
        }
      },
      "name": {
        "schema": {
          "type": "string"
        }
      },
      "type": {
        "schema": {
          "ref": "PrincipalType"
        }
      }
    },
    "required": [
      "name"
    ]
  },
  "CreateProjectRequest": {
    "type": "object",
    "properties": {
      "default_branch": {
        "schema": {
          "type": "string"
        }
      },
      "description": {
        "schema": {
          "type": "string"
        }
      },
      "kind": {
        "schema": {
          "ref": "ProjectKind"
        }
      },
      "name": {
        "schema": {
          "type": "string"
        }
      },
      "product_id": {
        "schema": {
          "type": "string"
        }
      }
    },
    "required": [
      "name"
    ]
  },
  "CreateRowFilterRequest": {
    "type": "object",
    "properties": {
      "description": {
        "schema": {
          "type": "string"
        }
      },
      "filter_sql": {
        "schema": {
          "type": "string"
        }
      },
      "name": {
        "schema": {
          "type": "string"
        }
      },
      "table_id": {
        "schema": {
          "type": "string"
        }
      }
    },
    "required": [
      "name",
      "filter_sql"
    ]
  },
  "CreateSavedResourceRequest": {
    "type": "object",
    "properties": {
      "display_name": {
        "schema": {
          "type": "string"
        }
      },
      "resource_key": {
        "schema": {
          "type": "string"
        }
      },
      "resource_path": {
        "schema": {
          "type": "string"
        }
      },
      "resource_type": {
        "schema": {
          "type": "string"
        }
      },
      "section": {
        "schema": {
          "type": "string"
        }
      }
    },
    "required": [
      "resource_type",
      "resource_key"
    ]
  },
  "CreateSchemaRequest": {
    "type": "object",
    "properties": {
      "comment": {
        "schema": {
          "type": "string"
        }
      },
      "location_name": {
        "schema": {
          "type": "string"
        }
      },
      "name": {
        "schema": {
          "type": "string"
        }
      },
      "properties": {
        "schema": {
          "ref": "Record"
        }
      }
    },
    "required": [
      "name"
    ]
  },
  "CreateSemanticMetricRequest": {
    "type": "object",
    "properties": {
      "certification_state": {
        "schema": {
          "ref": "CreateSemanticMetricRequestCertificationState"
        }
      },
      "default_time_grain": {
        "schema": {
          "type": "string"
        }
      },
      "description": {
        "schema": {
          "type": "string"
        }
      },
      "expression": {
        "schema": {
          "type": "string"
        }
      },
      "expression_mode": {
        "schema": {
          "ref": "SemanticMetricExpressionMode"
        }
      },
      "filter_sql": {
        "schema": {
          "type": "string"
        }
      },
      "format": {
        "schema": {
          "type": "string"
        }
      },
      "label": {
        "schema": {
          "type": "string"
        }
      },
      "metric_type": {
        "schema": {
          "ref": "SemanticMetricMetricType"
        }
      },
      "name": {
        "schema": {
          "type": "string"
        }
      },
      "relationship_names": {
        "schema": {
          "type": "array",
          "items": {
            "type": "string"
          }
        }
      }
    },
    "required": [
      "name",
      "metric_type",
      "expression"
    ]
  },
  "CreateSemanticMetricRequestCertificationState": {
    "type": "string",
    "enum": [
      "DRAFT",
      "CERTIFIED",
      "DEPRECATED"
    ]
  },
  "CreateSemanticModelRequest": {
    "type": "object",
    "properties": {
      "base_model_ref": {
        "schema": {
          "type": "string"
        }
      },
      "default_time_dimension": {
        "schema": {
          "type": "string"
        }
      },
      "description": {
        "schema": {
          "type": "string"
        }
      },
      "name": {
        "schema": {
          "type": "string"
        }
      },
      "tags": {
        "schema": {
          "type": "array",
          "items": {
            "type": "string"
          }
        }
      }
    },
    "required": [
      "name",
      "base_model_ref"
    ]
  },
  "CreateSemanticPreAggregationRequest": {
    "type": "object",
    "properties": {
      "dimension_set": {
        "schema": {
          "type": "array",
          "items": {
            "type": "string"
          }
        }
      },
      "grain": {
        "schema": {
          "type": "string"
        }
      },
      "metric_set": {
        "schema": {
          "type": "array",
          "items": {
            "type": "string"
          }
        }
      },
      "name": {
        "schema": {
          "type": "string"
        }
      },
      "refresh_policy": {
        "schema": {
          "type": "string"
        }
      },
      "target_relation": {
        "schema": {
          "type": "string"
        }
      }
    },
    "required": [
      "name",
      "target_relation"
    ]
  },
  "CreateSemanticRelationshipRequest": {
    "type": "object",
    "properties": {
      "cost": {
        "schema": {
          "type": "integer",
          "format": "int32"
        }
      },
      "from_semantic_id": {
        "schema": {
          "type": "string"
        }
      },
      "join_sql": {
        "schema": {
          "type": "string"
        }
      },
      "max_hops": {
        "schema": {
          "type": "integer",
          "format": "int32"
        }
      },
      "name": {
        "schema": {
          "type": "string"
        }
      },
      "relationship_type": {
        "schema": {
          "ref": "SemanticRelationshipRelationshipType"
        }
      },
      "to_semantic_id": {
        "schema": {
          "type": "string"
        }
      }
    },
    "required": [
      "name",
      "from_semantic_id",
      "to_semantic_id",
      "relationship_type",
      "join_sql"
    ]
  },
  "CreateStorageCredentialRequest": {
    "type": "object",
    "properties": {
      "comment": {
        "schema": {
          "type": "string"
        }
      },
      "credential_type": {
        "schema": {
          "ref": "StorageCredentialType"
        }
      },
      "endpoint": {
        "schema": {
          "type": "string"
        }
      },
      "key_id": {
        "schema": {
          "type": "string"
        }
      },
      "name": {
        "schema": {
          "type": "string"
        }
      },
      "region": {
        "schema": {
          "type": "string"
        }
      },
      "secret": {
        "schema": {
          "type": "string"
        }
      },
      "url_style": {
        "schema": {
          "ref": "URLStyle"
        }
      }
    },
    "required": [
      "name"
    ]
  },
  "CreateTableRequest": {
    "type": "object",
    "properties": {
      "columns": {
        "schema": {
          "type": "array",
          "items": {
            "ref": "CreateColumnRequest"
          }
        }
      },
      "comment": {
        "schema": {
          "type": "string"
        }
      },
      "name": {
        "schema": {
          "type": "string"
        }
      }
    },
    "required": [
      "name"
    ]
  },
  "CreateTagAssignmentRequest": {
    "type": "object",
    "properties": {
      "column_name": {
        "schema": {
          "type": "string"
        }
      },
      "securable_id": {
        "schema": {
          "type": "string"
        }
      },
      "securable_type": {
        "schema": {
          "ref": "TagAssignmentSecurableType"
        }
      }
    },
    "required": [
      "securable_type",
      "securable_id"
    ]
  },
  "CreateTagRequest": {
    "type": "object",
    "properties": {
      "key": {
        "schema": {
          "type": "string"
        }
      },
      "value": {
        "schema": {
          "type": "string"
        }
      }
    },
    "required": [
      "key"
    ]
  },
  "CreateViewRequest": {
    "type": "object",
    "properties": {
      "comment": {
        "schema": {
          "type": "string"
        }
      },
      "name": {
        "schema": {
          "type": "string"
        }
      },
      "view_definition": {
        "schema": {
          "type": "string"
        }
      }
    },
    "required": [
      "name",
      "view_definition"
    ]
  },
  "CreateVolumeRequest": {
    "type": "object",
    "properties": {
      "comment": {
        "schema": {
          "type": "string"
        }
      },
      "name": {
        "schema": {
          "type": "string"
        }
      },
      "storage_location": {
        "schema": {
          "type": "string"
        }
      },
      "volume_type": {
        "schema": {
          "type": "string"
        }
      }
    },
    "required": [
      "name"
    ]
  },
  "CreateWorkspaceRequest": {
    "type": "object",
    "properties": {
      "default_environment_id": {
        "schema": {
          "type": "string"
        }
      },
      "default_project_id": {
        "schema": {
          "type": "string"
        }
      },
      "git_repo_id": {
        "schema": {
          "type": "string"
        }
      },
      "git_root_path": {
        "schema": {
          "type": "string"
        }
      },
      "kind": {
        "schema": {
          "ref": "WorkspaceKind"
        }
      },
      "name": {
        "schema": {
          "type": "string"
        }
      },
      "owner_principal": {
        "schema": {
          "type": "string"
        }
      },
      "owner_team_id": {
        "schema": {
          "type": "string"
        }
      }
    },
    "required": [
      "name"
    ]
  },
  "Dashboard": {
    "type": "object",
    "properties": {
      "created_at": {
        "schema": {
          "type": "string"
        }
      },
      "description": {
        "schema": {
          "type": "string"
        }
      },
      "folder_id": {
        "schema": {
          "type": "string"
        }
      },
      "id": {
        "schema": {
          "type": "string"
        }
      },
      "name": {
        "schema": {
          "type": "string"
        }
      },
      "owner": {
        "schema": {
          "type": "string"
        }
      },
      "updated_at": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "DashboardDetail": {
    "type": "object",
    "properties": {
      "dashboard": {
        "schema": {
          "ref": "Dashboard"
        }
      },
      "widgets": {
        "schema": {
          "type": "array",
          "items": {
            "ref": "DashboardWidget"
          }
        }
      }
    }
  },
  "DashboardNotebookCellSource": {
    "type": "object",
    "properties": {
      "cell_id": {
        "schema": {
          "type": "string"
        }
      },
      "notebook_id": {
        "schema": {
          "type": "string"
        }
      }
    },
    "required": [
      "notebook_id",
      "cell_id"
    ]
  },
  "DashboardSQLQuerySource": {
    "type": "object",
    "properties": {
      "catalog": {
        "schema": {
          "type": "string"
        }
      },
      "schema": {
        "schema": {
          "type": "string"
        }
      },
      "sql": {
        "schema": {
          "type": "string"
        }
      }
    },
    "required": [
      "sql"
    ]
  },
  "DashboardSemanticQuerySource": {
    "type": "object",
    "properties": {
      "dimensions": {
        "schema": {
          "type": "array",
          "items": {
            "type": "string"
          }
        }
      },
      "filters": {
        "schema": {
          "type": "array",
          "items": {
            "type": "string"
          }
        }
      },
      "limit": {
        "schema": {
          "type": "integer",
          "format": "int32"
        }
      },
      "metrics": {
        "schema": {
          "type": "array",
          "items": {
            "type": "string"
          }
        }
      },
      "order_by": {
        "schema": {
          "type": "array",
          "items": {
            "type": "string"
          }
        }
      },
      "relationship_names": {
        "schema": {
          "type": "array",
          "items": {
            "type": "string"
          }
        }
      },
      "semantic_model_id": {
        "schema": {
          "type": "string"
        }
      },
      "time_grain": {
        "schema": {
          "type": "string"
        }
      }
    },
    "required": [
      "semantic_model_id",
      "metrics"
    ]
  },
  "DashboardWidget": {
    "type": "object",
    "properties": {
      "created_at": {
        "schema": {
          "type": "string"
        }
      },
      "dashboard_id": {
        "schema": {
          "type": "string"
        }
      },
      "description": {
        "schema": {
          "type": "string"
        }
      },
      "id": {
        "schema": {
          "type": "string"
        }
      },
      "layout": {
        "schema": {
          "ref": "DashboardWidgetLayout"
        }
      },
      "name": {
        "schema": {
          "type": "string"
        }
      },
      "source": {
        "schema": {
          "ref": "DashboardWidgetSource"
        }
      },
      "updated_at": {
        "schema": {
          "type": "string"
        }
      },
      "visual_spec": {
        "schema": {
          "ref": "VisualSpec"
        }
      }
    }
  },
  "DashboardWidgetLayout": {
    "type": "object",
    "properties": {
      "h": {
        "schema": {
          "type": "integer",
          "format": "int32"
        }
      },
      "w": {
        "schema": {
          "type": "integer",
          "format": "int32"
        }
      },
      "x": {
        "schema": {
          "type": "integer",
          "format": "int32"
        }
      },
      "y": {
        "schema": {
          "type": "integer",
          "format": "int32"
        }
      }
    },
    "required": [
      "x",
      "y",
      "w",
      "h"
    ]
  },
  "DashboardWidgetSource": {
    "type": "object",
    "properties": {
      "kind": {
        "schema": {
          "ref": "DashboardWidgetSourceKind"
        }
      },
      "notebook_cell": {
        "schema": {
          "ref": "DashboardNotebookCellSource"
        }
      },
      "semantic_query": {
        "schema": {
          "ref": "DashboardSemanticQuerySource"
        }
      },
      "sql_query": {
        "schema": {
          "ref": "DashboardSQLQuerySource"
        }
      }
    },
    "required": [
      "kind"
    ]
  },
  "DashboardWidgetSourceKind": {
    "type": "string",
    "enum": [
      "sql_query",
      "notebook_cell",
      "semantic_query"
    ]
  },
  "DuplicateNotebookRequest": {
    "type": "object",
    "properties": {
      "folder_id": {
        "schema": {
          "type": "string"
        }
      },
      "git_path": {
        "schema": {
          "type": "string"
        }
      },
      "name": {
        "schema": {
          "type": "string"
        }
      }
    },
    "required": [
      "folder_id"
    ]
  },
  "Environment": {
    "type": "object",
    "properties": {
      "compute_endpoint": {
        "schema": {
          "type": "string"
        }
      },
      "created_at": {
        "schema": {
          "type": "string"
        }
      },
      "defer_to_environment": {
        "schema": {
          "type": "string"
        }
      },
      "description": {
        "schema": {
          "type": "string"
        }
      },
      "id": {
        "schema": {
          "type": "string"
        }
      },
      "kind": {
        "schema": {
          "ref": "EnvironmentKind"
        }
      },
      "name": {
        "schema": {
          "type": "string"
        }
      },
      "project_id": {
        "schema": {
          "type": "string"
        }
      },
      "project_name": {
        "schema": {
          "type": "string"
        }
      },
      "source_overrides": {
        "schema": {
          "ref": "Record"
        }
      },
      "target_catalog": {
        "schema": {
          "type": "string"
        }
      },
      "target_schema": {
        "schema": {
          "type": "string"
        }
      },
      "updated_at": {
        "schema": {
          "type": "string"
        }
      },
      "variables": {
        "schema": {
          "ref": "Record"
        }
      }
    },
    "required": [
      "name",
      "kind",
      "target_catalog",
      "target_schema"
    ]
  },
  "EnvironmentKind": {
    "type": "string",
    "enum": [
      "development",
      "staging",
      "production"
    ]
  },
  "Error": {
    "type": "object",
    "properties": {
      "code": {
        "schema": {
          "type": "integer",
          "format": "int32"
        }
      },
      "details": {
        "schema": {
          "ref": "Record"
        }
      },
      "message": {
        "schema": {
          "type": "string"
        }
      }
    },
    "required": [
      "code",
      "message"
    ]
  },
  "ExternalLocation": {
    "type": "object",
    "properties": {
      "comment": {
        "schema": {
          "type": "string"
        }
      },
      "created_at": {
        "schema": {
          "type": "string"
        }
      },
      "credential_name": {
        "schema": {
          "type": "string"
        }
      },
      "id": {
        "schema": {
          "type": "string"
        }
      },
      "name": {
        "schema": {
          "type": "string"
        }
      },
      "owner": {
        "schema": {
          "type": "string"
        }
      },
      "read_only": {
        "schema": {
          "type": "boolean"
        }
      },
      "storage_type": {
        "schema": {
          "ref": "StorageType"
        }
      },
      "updated_at": {
        "schema": {
          "type": "string"
        }
      },
      "url": {
        "schema": {
          "type": "string"
        }
      }
    },
    "required": [
      "id",
      "name",
      "url"
    ]
  },
  "Folder": {
    "type": "object",
    "properties": {
      "created_at": {
        "schema": {
          "type": "string"
        }
      },
      "default_environment_id": {
        "schema": {
          "type": "string"
        }
      },
      "default_project_id": {
        "schema": {
          "type": "string"
        }
      },
      "depth": {
        "schema": {
          "type": "integer",
          "format": "int32"
        }
      },
      "git_repo_id": {
        "schema": {
          "type": "string"
        }
      },
      "git_root_path": {
        "schema": {
          "type": "string"
        }
      },
      "id": {
        "schema": {
          "type": "string"
        }
      },
      "name": {
        "schema": {
          "type": "string"
        }
      },
      "owner": {
        "schema": {
          "type": "string"
        }
      },
      "parent_folder_id": {
        "schema": {
          "type": "string"
        }
      },
      "path": {
        "schema": {
          "type": "string"
        }
      },
      "system_role": {
        "schema": {
          "type": "string"
        }
      },
      "updated_at": {
        "schema": {
          "type": "string"
        }
      },
      "workspace_id": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "FolderContentItem": {
    "type": "object",
    "properties": {
      "folder_id": {
        "schema": {
          "type": "string"
        }
      },
      "git_repo_id": {
        "schema": {
          "type": "string"
        }
      },
      "id": {
        "schema": {
          "type": "string"
        }
      },
      "kind": {
        "schema": {
          "type": "string"
        }
      },
      "name": {
        "schema": {
          "type": "string"
        }
      },
      "owner": {
        "schema": {
          "type": "string"
        }
      },
      "project_bound": {
        "schema": {
          "type": "boolean"
        }
      },
      "project_name": {
        "schema": {
          "type": "string"
        }
      },
      "scope": {
        "schema": {
          "type": "string"
        }
      },
      "shared": {
        "schema": {
          "type": "boolean"
        }
      },
      "updated_at": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "FolderPath": {
    "type": "object",
    "properties": {
      "data": {
        "schema": {
          "type": "array",
          "items": {
            "ref": "Folder"
          }
        }
      }
    },
    "required": [
      "data"
    ]
  },
  "FolderShare": {
    "type": "object",
    "properties": {
      "principal_name": {
        "schema": {
          "type": "string"
        }
      },
      "role": {
        "schema": {
          "ref": "NotebookShareRole"
        }
      }
    }
  },
  "FreshnessPolicy": {
    "type": "object",
    "properties": {
      "cron_schedule": {
        "schema": {
          "type": "string"
        }
      },
      "max_lag_seconds": {
        "schema": {
          "type": "integer",
          "format": "int64"
        }
      }
    }
  },
  "FreshnessStatus": {
    "type": "object",
    "properties": {
      "is_fresh": {
        "schema": {
          "type": "boolean"
        }
      },
      "last_run_at": {
        "schema": {
          "type": "string"
        }
      },
      "max_lag_seconds": {
        "schema": {
          "type": "integer",
          "format": "int64"
        }
      },
      "stale_since": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "GitRepo": {
    "type": "object",
    "properties": {
      "branch": {
        "schema": {
          "type": "string"
        }
      },
      "created_at": {
        "schema": {
          "type": "string"
        }
      },
      "id": {
        "schema": {
          "type": "string"
        }
      },
      "last_commit": {
        "schema": {
          "type": "string"
        }
      },
      "last_sync_at": {
        "schema": {
          "type": "string"
        }
      },
      "owner": {
        "schema": {
          "type": "string"
        }
      },
      "path": {
        "schema": {
          "type": "string"
        }
      },
      "updated_at": {
        "schema": {
          "type": "string"
        }
      },
      "url": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "GitSyncResult": {
    "type": "object",
    "properties": {
      "commit_sha": {
        "schema": {
          "type": "string"
        }
      },
      "notebooks_created": {
        "schema": {
          "type": "integer",
          "format": "int32"
        }
      },
      "notebooks_deleted": {
        "schema": {
          "type": "integer",
          "format": "int32"
        }
      },
      "notebooks_updated": {
        "schema": {
          "type": "integer",
          "format": "int32"
        }
      }
    }
  },
  "Group": {
    "type": "object",
    "properties": {
      "created_at": {
        "schema": {
          "type": "string"
        }
      },
      "description": {
        "schema": {
          "type": "string"
        }
      },
      "id": {
        "schema": {
          "type": "string"
        }
      },
      "name": {
        "schema": {
          "type": "string"
        }
      }
    },
    "required": [
      "id",
      "name"
    ]
  },
  "GroupMember": {
    "type": "object",
    "properties": {
      "group_id": {
        "schema": {
          "type": "string"
        }
      },
      "member_id": {
        "schema": {
          "type": "string"
        }
      },
      "member_type": {
        "schema": {
          "ref": "PrincipalType"
        }
      }
    },
    "required": [
      "group_id",
      "member_type",
      "member_id"
    ]
  },
  "HealthResponse": {
    "type": "object",
    "properties": {
      "status": {
        "schema": {
          "type": "string"
        }
      }
    },
    "required": [
      "status"
    ]
  },
  "LineageEdge": {
    "type": "object",
    "properties": {
      "created_at": {
        "schema": {
          "type": "string"
        }
      },
      "edge_type": {
        "schema": {
          "type": "string"
        }
      },
      "id": {
        "schema": {
          "type": "string"
        }
      },
      "principal_name": {
        "schema": {
          "type": "string"
        }
      },
      "source_schema": {
        "schema": {
          "type": "string"
        }
      },
      "source_table": {
        "schema": {
          "type": "string"
        }
      },
      "target_schema": {
        "schema": {
          "type": "string"
        }
      },
      "target_table": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "LineageNode": {
    "type": "object",
    "properties": {
      "downstream": {
        "schema": {
          "type": "array",
          "items": {
            "ref": "LineageEdge"
          }
        }
      },
      "table_name": {
        "schema": {
          "type": "string"
        }
      },
      "upstream": {
        "schema": {
          "type": "array",
          "items": {
            "ref": "LineageEdge"
          }
        }
      }
    }
  },
  "LocalLoginRequest": {
    "type": "object",
    "properties": {
      "password": {
        "schema": {
          "type": "string"
        }
      },
      "username": {
        "schema": {
          "type": "string"
        }
      }
    },
    "required": [
      "username",
      "password"
    ]
  },
  "Macro": {
    "type": "object",
    "properties": {
      "body": {
        "schema": {
          "type": "string"
        }
      },
      "catalog_name": {
        "schema": {
          "type": "string"
        }
      },
      "created_at": {
        "schema": {
          "type": "string"
        }
      },
      "created_by": {
        "schema": {
          "type": "string"
        }
      },
      "description": {
        "schema": {
          "type": "string"
        }
      },
      "id": {
        "schema": {
          "type": "string"
        }
      },
      "macro_type": {
        "schema": {
          "ref": "MacroType"
        }
      },
      "name": {
        "schema": {
          "type": "string"
        }
      },
      "owner": {
        "schema": {
          "type": "string"
        }
      },
      "parameters": {
        "schema": {
          "type": "array",
          "items": {
            "type": "string"
          }
        }
      },
      "project_name": {
        "schema": {
          "type": "string"
        }
      },
      "properties": {
        "schema": {
          "ref": "Record"
        }
      },
      "status": {
        "schema": {
          "ref": "MacroStatus"
        }
      },
      "tags": {
        "schema": {
          "type": "array",
          "items": {
            "type": "string"
          }
        }
      },
      "updated_at": {
        "schema": {
          "type": "string"
        }
      },
      "visibility": {
        "schema": {
          "ref": "MacroVisibility"
        }
      }
    }
  },
  "MacroImpactList": {
    "type": "object",
    "properties": {
      "data": {
        "schema": {
          "type": "array",
          "items": {
            "ref": "MacroImpactModel"
          }
        }
      },
      "next_page_token": {
        "schema": {
          "type": "string"
        }
      }
    },
    "required": [
      "data"
    ]
  },
  "MacroImpactModel": {
    "type": "object",
    "properties": {
      "last_seen_at": {
        "schema": {
          "type": "string"
        }
      },
      "model_name": {
        "schema": {
          "type": "string"
        }
      },
      "target_schema": {
        "schema": {
          "type": "string"
        }
      },
      "target_table": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "MacroRevision": {
    "type": "object",
    "properties": {
      "body": {
        "schema": {
          "type": "string"
        }
      },
      "content_hash": {
        "schema": {
          "type": "string"
        }
      },
      "created_at": {
        "schema": {
          "type": "string"
        }
      },
      "created_by": {
        "schema": {
          "type": "string"
        }
      },
      "description": {
        "schema": {
          "type": "string"
        }
      },
      "id": {
        "schema": {
          "type": "string"
        }
      },
      "macro_name": {
        "schema": {
          "type": "string"
        }
      },
      "parameters": {
        "schema": {
          "type": "array",
          "items": {
            "type": "string"
          }
        }
      },
      "status": {
        "schema": {
          "ref": "MacroStatus"
        }
      },
      "version": {
        "schema": {
          "type": "integer",
          "format": "int32"
        }
      }
    }
  },
  "MacroRevisionDiff": {
    "type": "object",
    "properties": {
      "body_changed": {
        "schema": {
          "type": "boolean"
        }
      },
      "changed": {
        "schema": {
          "type": "boolean"
        }
      },
      "description_changed": {
        "schema": {
          "type": "boolean"
        }
      },
      "from_body": {
        "schema": {
          "type": "string"
        }
      },
      "from_content_hash": {
        "schema": {
          "type": "string"
        }
      },
      "from_description": {
        "schema": {
          "type": "string"
        }
      },
      "from_parameters": {
        "schema": {
          "type": "array",
          "items": {
            "type": "string"
          }
        }
      },
      "from_status": {
        "schema": {
          "ref": "MacroStatus"
        }
      },
      "from_version": {
        "schema": {
          "type": "integer",
          "format": "int32"
        }
      },
      "impact_changed": {
        "schema": {
          "type": "boolean"
        }
      },
      "impacted_models_added": {
        "schema": {
          "type": "array",
          "items": {
            "ref": "MacroImpactModel"
          }
        }
      },
      "impacted_models_removed": {
        "schema": {
          "type": "array",
          "items": {
            "ref": "MacroImpactModel"
          }
        }
      },
      "impacted_models_unchanged": {
        "schema": {
          "type": "array",
          "items": {
            "ref": "MacroImpactModel"
          }
        }
      },
      "macro_name": {
        "schema": {
          "type": "string"
        }
      },
      "parameters_changed": {
        "schema": {
          "type": "boolean"
        }
      },
      "status_changed": {
        "schema": {
          "type": "boolean"
        }
      },
      "to_body": {
        "schema": {
          "type": "string"
        }
      },
      "to_content_hash": {
        "schema": {
          "type": "string"
        }
      },
      "to_description": {
        "schema": {
          "type": "string"
        }
      },
      "to_parameters": {
        "schema": {
          "type": "array",
          "items": {
            "type": "string"
          }
        }
      },
      "to_status": {
        "schema": {
          "ref": "MacroStatus"
        }
      },
      "to_version": {
        "schema": {
          "type": "integer",
          "format": "int32"
        }
      }
    }
  },
  "MacroRevisionList": {
    "type": "object",
    "properties": {
      "data": {
        "schema": {
          "type": "array",
          "items": {
            "ref": "MacroRevision"
          }
        }
      }
    },
    "required": [
      "data"
    ]
  },
  "MacroStatus": {
    "type": "string",
    "enum": [
      "ACTIVE",
      "DEPRECATED"
    ]
  },
  "MacroType": {
    "type": "string",
    "enum": [
      "SCALAR",
      "TABLE"
    ]
  },
  "MacroVisibility": {
    "type": "string",
    "enum": [
      "project",
      "catalog_global",
      "system"
    ]
  },
  "ManifestColumn": {
    "type": "object",
    "properties": {
      "name": {
        "schema": {
          "type": "string"
        }
      },
      "type": {
        "schema": {
          "type": "string"
        }
      }
    },
    "required": [
      "name",
      "type"
    ]
  },
  "ManifestResponse": {
    "type": "object",
    "properties": {
      "column_masks": {
        "schema": {
          "ref": "Record"
        }
      },
      "columns": {
        "schema": {
          "type": "array",
          "items": {
            "ref": "ManifestColumn"
          }
        }
      },
      "expires_at": {
        "schema": {
          "type": "string"
        }
      },
      "files": {
        "schema": {
          "type": "array",
          "items": {
            "type": "string"
          }
        }
      },
      "row_filters": {
        "schema": {
          "type": "array",
          "items": {
            "type": "string"
          }
        }
      },
      "schema": {
        "schema": {
          "type": "string"
        }
      },
      "table": {
        "schema": {
          "type": "string"
        }
      }
    },
    "required": [
      "table"
    ]
  },
  "MetastoreSummary": {
    "type": "object",
    "properties": {
      "catalog_name": {
        "schema": {
          "type": "string"
        }
      },
      "data_path": {
        "schema": {
          "type": "string"
        }
      },
      "metastore_type": {
        "schema": {
          "type": "string"
        }
      },
      "schema_count": {
        "schema": {
          "type": "integer",
          "format": "int32"
        }
      },
      "storage_backend": {
        "schema": {
          "type": "string"
        }
      },
      "table_count": {
        "schema": {
          "type": "integer",
          "format": "int32"
        }
      }
    },
    "required": [
      "catalog_name"
    ]
  },
  "MetastoreType": {
    "type": "string",
    "enum": [
      "sqlite",
      "postgres"
    ]
  },
  "MetricFreshnessStatus": {
    "type": "object",
    "properties": {
      "checked_at": {
        "schema": {
          "type": "string"
        }
      },
      "freshness_basis": {
        "schema": {
          "type": "array",
          "items": {
            "type": "string"
          }
        }
      },
      "freshness_status": {
        "schema": {
          "type": "string"
        }
      },
      "metric_name": {
        "schema": {
          "type": "string"
        }
      },
      "selected_pre_aggregation": {
        "schema": {
          "type": "string"
        }
      },
      "semantic_model_id": {
        "schema": {
          "type": "string"
        }
      },
      "semantic_model_name": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "MetricQueryExplainResponse": {
    "type": "object",
    "properties": {
      "plan": {
        "schema": {
          "ref": "MetricQueryPlan"
        }
      }
    }
  },
  "MetricQueryJoinStep": {
    "type": "object",
    "properties": {
      "from_model": {
        "schema": {
          "type": "string"
        }
      },
      "join_sql": {
        "schema": {
          "type": "string"
        }
      },
      "relationship_name": {
        "schema": {
          "type": "string"
        }
      },
      "relationship_type": {
        "schema": {
          "type": "string"
        }
      },
      "to_model": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "MetricQueryPlan": {
    "type": "object",
    "properties": {
      "base_model_name": {
        "schema": {
          "type": "string"
        }
      },
      "base_relation": {
        "schema": {
          "type": "string"
        }
      },
      "dimensions": {
        "schema": {
          "type": "array",
          "items": {
            "type": "string"
          }
        }
      },
      "freshness_basis": {
        "schema": {
          "type": "array",
          "items": {
            "type": "string"
          }
        }
      },
      "freshness_status": {
        "schema": {
          "type": "string"
        }
      },
      "generated_sql": {
        "schema": {
          "type": "string"
        }
      },
      "join_path": {
        "schema": {
          "type": "array",
          "items": {
            "ref": "MetricQueryJoinStep"
          }
        }
      },
      "metrics": {
        "schema": {
          "type": "array",
          "items": {
            "type": "string"
          }
        }
      },
      "selected_pre_aggregation": {
        "schema": {
          "type": "string"
        }
      },
      "time_grain": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "MetricQueryRequest": {
    "type": "object",
    "properties": {
      "dimensions": {
        "schema": {
          "type": "array",
          "items": {
            "type": "string"
          }
        }
      },
      "filters": {
        "schema": {
          "type": "array",
          "items": {
            "type": "string"
          }
        }
      },
      "limit": {
        "schema": {
          "type": "integer",
          "format": "int32"
        }
      },
      "metrics": {
        "schema": {
          "type": "array",
          "items": {
            "type": "string"
          }
        }
      },
      "order_by": {
        "schema": {
          "type": "array",
          "items": {
            "type": "string"
          }
        }
      },
      "relationship_names": {
        "schema": {
          "type": "array",
          "items": {
            "type": "string"
          }
        }
      },
      "time_grain": {
        "schema": {
          "type": "string"
        }
      }
    },
    "required": [
      "metrics"
    ]
  },
  "MetricQueryRunResponse": {
    "type": "object",
    "properties": {
      "plan": {
        "schema": {
          "ref": "MetricQueryPlan"
        }
      },
      "result": {
        "schema": {
          "ref": "QueryResult"
        }
      }
    }
  },
  "Model": {
    "type": "object",
    "properties": {
      "config": {
        "schema": {
          "ref": "ModelConfig"
        }
      },
      "contract": {
        "schema": {
          "ref": "ModelContract"
        }
      },
      "created_at": {
        "schema": {
          "type": "string"
        }
      },
      "created_by": {
        "schema": {
          "type": "string"
        }
      },
      "depends_on": {
        "schema": {
          "type": "array",
          "items": {
            "type": "string"
          }
        }
      },
      "description": {
        "schema": {
          "type": "string"
        }
      },
      "freshness_policy": {
        "schema": {
          "ref": "FreshnessPolicy"
        }
      },
      "id": {
        "schema": {
          "type": "string"
        }
      },
      "materialization": {
        "schema": {
          "ref": "ModelMaterialization"
        }
      },
      "name": {
        "schema": {
          "type": "string"
        }
      },
      "owner": {
        "schema": {
          "type": "string"
        }
      },
      "project_name": {
        "schema": {
          "type": "string"
        }
      },
      "sql": {
        "schema": {
          "type": "string"
        }
      },
      "tags": {
        "schema": {
          "type": "array",
          "items": {
            "type": "string"
          }
        }
      },
      "updated_at": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "ModelConfig": {
    "type": "object",
    "properties": {
      "incremental_strategy": {
        "schema": {
          "type": "string"
        }
      },
      "on_schema_change": {
        "schema": {
          "ref": "ModelConfigOnSchemaChange"
        }
      },
      "unique_key": {
        "schema": {
          "type": "array",
          "items": {
            "type": "string"
          }
        }
      }
    }
  },
  "ModelConfigOnSchemaChange": {
    "type": "string",
    "enum": [
      "ignore",
      "fail"
    ]
  },
  "ModelContract": {
    "type": "object",
    "properties": {
      "columns": {
        "schema": {
          "type": "array",
          "items": {
            "ref": "ModelContractColumn"
          }
        }
      },
      "enforce": {
        "schema": {
          "type": "boolean"
        }
      }
    }
  },
  "ModelContractColumn": {
    "type": "object",
    "properties": {
      "name": {
        "schema": {
          "type": "string"
        }
      },
      "nullable": {
        "schema": {
          "type": "boolean"
        }
      },
      "type": {
        "schema": {
          "type": "string"
        }
      }
    },
    "required": [
      "name",
      "type"
    ]
  },
  "ModelDAG": {
    "type": "object",
    "properties": {
      "tiers": {
        "schema": {
          "type": "array",
          "items": {
            "ref": "ModelDAGTier"
          }
        }
      }
    }
  },
  "ModelDAGNode": {
    "type": "object",
    "properties": {
      "depends_on": {
        "schema": {
          "type": "array",
          "items": {
            "type": "string"
          }
        }
      },
      "materialization": {
        "schema": {
          "ref": "ModelMaterialization"
        }
      },
      "model_name": {
        "schema": {
          "type": "string"
        }
      },
      "project_name": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "ModelDAGTier": {
    "type": "object",
    "properties": {
      "nodes": {
        "schema": {
          "type": "array",
          "items": {
            "ref": "ModelDAGNode"
          }
        }
      },
      "tier": {
        "schema": {
          "type": "integer",
          "format": "int32"
        }
      }
    }
  },
  "ModelMaterialization": {
    "type": "string",
    "enum": [
      "VIEW",
      "TABLE",
      "INCREMENTAL",
      "EPHEMERAL",
      "SEED",
      "SNAPSHOT"
    ]
  },
  "ModelRun": {
    "type": "object",
    "properties": {
      "build_id": {
        "schema": {
          "type": "string"
        }
      },
      "compile_diagnostics": {
        "schema": {
          "ref": "ModelRunCompileDiagnostics"
        }
      },
      "compile_manifest": {
        "schema": {
          "type": "string"
        }
      },
      "created_at": {
        "schema": {
          "type": "string"
        }
      },
      "environment_name": {
        "schema": {
          "type": "string"
        }
      },
      "error_message": {
        "schema": {
          "type": "string"
        }
      },
      "finished_at": {
        "schema": {
          "type": "string"
        }
      },
      "full_refresh": {
        "schema": {
          "type": "boolean"
        }
      },
      "id": {
        "schema": {
          "type": "string"
        }
      },
      "model_names": {
        "schema": {
          "type": "array",
          "items": {
            "type": "string"
          }
        }
      },
      "project_name": {
        "schema": {
          "type": "string"
        }
      },
      "started_at": {
        "schema": {
          "type": "string"
        }
      },
      "status": {
        "schema": {
          "type": "string"
        }
      },
      "trigger_type": {
        "schema": {
          "type": "string"
        }
      },
      "triggered_by": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "ModelRunCompileDiagnostics": {
    "type": "object",
    "properties": {
      "errors": {
        "schema": {
          "type": "array",
          "items": {
            "type": "string"
          }
        }
      },
      "warnings": {
        "schema": {
          "type": "array",
          "items": {
            "type": "string"
          }
        }
      }
    }
  },
  "ModelRunStep": {
    "type": "object",
    "properties": {
      "compiled_hash": {
        "schema": {
          "type": "string"
        }
      },
      "compiled_sql": {
        "schema": {
          "type": "string"
        }
      },
      "created_at": {
        "schema": {
          "type": "string"
        }
      },
      "depends_on": {
        "schema": {
          "type": "array",
          "items": {
            "type": "string"
          }
        }
      },
      "error_message": {
        "schema": {
          "type": "string"
        }
      },
      "finished_at": {
        "schema": {
          "type": "string"
        }
      },
      "id": {
        "schema": {
          "type": "string"
        }
      },
      "macros_used": {
        "schema": {
          "type": "array",
          "items": {
            "type": "string"
          }
        }
      },
      "model_name": {
        "schema": {
          "type": "string"
        }
      },
      "rows_affected": {
        "schema": {
          "type": "integer",
          "format": "int64"
        }
      },
      "run_id": {
        "schema": {
          "type": "string"
        }
      },
      "started_at": {
        "schema": {
          "type": "string"
        }
      },
      "status": {
        "schema": {
          "type": "string"
        }
      },
      "vars_used": {
        "schema": {
          "type": "array",
          "items": {
            "type": "string"
          }
        }
      }
    }
  },
  "ModelRunStepList": {
    "type": "object",
    "properties": {
      "data": {
        "schema": {
          "type": "array",
          "items": {
            "ref": "ModelRunStep"
          }
        }
      }
    },
    "required": [
      "data"
    ]
  },
  "ModelTest": {
    "type": "object",
    "properties": {
      "column": {
        "schema": {
          "type": "string"
        }
      },
      "config": {
        "schema": {
          "ref": "ModelTestConfig"
        }
      },
      "created_at": {
        "schema": {
          "type": "string"
        }
      },
      "id": {
        "schema": {
          "type": "string"
        }
      },
      "model_id": {
        "schema": {
          "type": "string"
        }
      },
      "name": {
        "schema": {
          "type": "string"
        }
      },
      "test_type": {
        "schema": {
          "ref": "ModelTestTestType"
        }
      }
    }
  },
  "ModelTestConfig": {
    "type": "object",
    "properties": {
      "custom_sql": {
        "schema": {
          "type": "string"
        }
      },
      "to_column": {
        "schema": {
          "type": "string"
        }
      },
      "to_model": {
        "schema": {
          "type": "string"
        }
      },
      "values": {
        "schema": {
          "type": "array",
          "items": {
            "type": "string"
          }
        }
      }
    }
  },
  "ModelTestList": {
    "type": "object",
    "properties": {
      "data": {
        "schema": {
          "type": "array",
          "items": {
            "ref": "ModelTest"
          }
        }
      }
    },
    "required": [
      "data"
    ]
  },
  "ModelTestResult": {
    "type": "object",
    "properties": {
      "created_at": {
        "schema": {
          "type": "string"
        }
      },
      "error_message": {
        "schema": {
          "type": "string"
        }
      },
      "id": {
        "schema": {
          "type": "string"
        }
      },
      "rows_returned": {
        "schema": {
          "type": "integer",
          "format": "int64"
        }
      },
      "run_step_id": {
        "schema": {
          "type": "string"
        }
      },
      "status": {
        "schema": {
          "ref": "ModelTestResultStatus"
        }
      },
      "test_id": {
        "schema": {
          "type": "string"
        }
      },
      "test_name": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "ModelTestResultList": {
    "type": "object",
    "properties": {
      "data": {
        "schema": {
          "type": "array",
          "items": {
            "ref": "ModelTestResult"
          }
        }
      }
    },
    "required": [
      "data"
    ]
  },
  "ModelTestResultStatus": {
    "type": "string",
    "enum": [
      "PASS",
      "FAIL",
      "ERROR"
    ]
  },
  "ModelTestTestType": {
    "type": "string",
    "enum": [
      "not_null",
      "unique",
      "accepted_values",
      "relationships",
      "custom_sql"
    ]
  },
  "MoveFolderRequest": {
    "type": "object",
    "properties": {
      "confirm_context_change": {
        "schema": {
          "type": "boolean"
        }
      },
      "confirm_leave_git": {
        "schema": {
          "type": "boolean"
        }
      },
      "parent_folder_id": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "MoveNotebookRequest": {
    "type": "object",
    "properties": {
      "confirm_context_change": {
        "schema": {
          "type": "boolean"
        }
      },
      "confirm_leave_git": {
        "schema": {
          "type": "boolean"
        }
      },
      "folder_id": {
        "schema": {
          "type": "string"
        }
      },
      "git_path": {
        "schema": {
          "type": "string"
        }
      }
    },
    "required": [
      "folder_id"
    ]
  },
  "Notebook": {
    "type": "object",
    "properties": {
      "created_at": {
        "schema": {
          "type": "string"
        }
      },
      "description": {
        "schema": {
          "type": "string"
        }
      },
      "environment_override_id": {
        "schema": {
          "type": "string"
        }
      },
      "folder_id": {
        "schema": {
          "type": "string"
        }
      },
      "git_path": {
        "schema": {
          "type": "string"
        }
      },
      "git_repo_id": {
        "schema": {
          "type": "string"
        }
      },
      "id": {
        "schema": {
          "type": "string"
        }
      },
      "name": {
        "schema": {
          "type": "string"
        }
      },
      "owner": {
        "schema": {
          "type": "string"
        }
      },
      "project_override_id": {
        "schema": {
          "type": "string"
        }
      },
      "updated_at": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "NotebookCellTestConfig": {
    "type": "object",
    "properties": {
      "severity": {
        "schema": {
          "ref": "NotebookTestSeverity"
        }
      }
    }
  },
  "NotebookContext": {
    "type": "object",
    "properties": {
      "effective_environment_id": {
        "schema": {
          "type": "string"
        }
      },
      "effective_git_repo_id": {
        "schema": {
          "type": "string"
        }
      },
      "effective_git_root_path": {
        "schema": {
          "type": "string"
        }
      },
      "effective_project_id": {
        "schema": {
          "type": "string"
        }
      },
      "environment_source_id": {
        "schema": {
          "type": "string"
        }
      },
      "folder_id": {
        "schema": {
          "type": "string"
        }
      },
      "git_source_folder_id": {
        "schema": {
          "type": "string"
        }
      },
      "notebook_id": {
        "schema": {
          "type": "string"
        }
      },
      "project_source_folder_id": {
        "schema": {
          "type": "string"
        }
      },
      "workspace_id": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "NotebookDetail": {
    "type": "object",
    "properties": {
      "cells": {
        "schema": {
          "type": "array",
          "items": {
            "ref": "Cell"
          }
        }
      },
      "context": {
        "schema": {
          "ref": "NotebookContext"
        }
      },
      "notebook": {
        "schema": {
          "ref": "Notebook"
        }
      },
      "publish_model": {
        "schema": {
          "ref": "NotebookPublishModel"
        }
      },
      "shares": {
        "schema": {
          "type": "array",
          "items": {
            "ref": "NotebookShare"
          }
        }
      }
    }
  },
  "NotebookJob": {
    "type": "object",
    "properties": {
      "created_at": {
        "schema": {
          "type": "string"
        }
      },
      "error": {
        "schema": {
          "type": "string"
        }
      },
      "id": {
        "schema": {
          "type": "string"
        }
      },
      "notebook_id": {
        "schema": {
          "type": "string"
        }
      },
      "result": {
        "schema": {
          "type": "string"
        }
      },
      "session_id": {
        "schema": {
          "type": "string"
        }
      },
      "state": {
        "schema": {
          "ref": "NotebookJobState"
        }
      },
      "updated_at": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "NotebookJobState": {
    "type": "string",
    "enum": [
      "pending",
      "running",
      "complete",
      "failed"
    ]
  },
  "NotebookPublishModel": {
    "type": "object",
    "properties": {
      "materialization": {
        "schema": {
          "ref": "ModelMaterialization"
        }
      },
      "name": {
        "schema": {
          "type": "string"
        }
      },
      "output_cell_id": {
        "schema": {
          "type": "string"
        }
      },
      "project_name": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "NotebookSession": {
    "type": "object",
    "properties": {
      "created_at": {
        "schema": {
          "type": "string"
        }
      },
      "id": {
        "schema": {
          "type": "string"
        }
      },
      "last_used_at": {
        "schema": {
          "type": "string"
        }
      },
      "notebook_id": {
        "schema": {
          "type": "string"
        }
      },
      "principal": {
        "schema": {
          "type": "string"
        }
      },
      "state": {
        "schema": {
          "ref": "NotebookSessionState"
        }
      }
    }
  },
  "NotebookSessionState": {
    "type": "string",
    "enum": [
      "active",
      "closed"
    ]
  },
  "NotebookShare": {
    "type": "object",
    "properties": {
      "principal_name": {
        "schema": {
          "type": "string"
        }
      },
      "role": {
        "schema": {
          "ref": "NotebookShareRole"
        }
      }
    }
  },
  "NotebookShareRole": {
    "type": "string",
    "enum": [
      "viewer",
      "editor",
      "manager"
    ]
  },
  "NotebookTestSeverity": {
    "type": "string",
    "enum": [
      "error",
      "warn"
    ]
  },
  "OIDCProviderRequest": {
    "type": "object",
    "properties": {
      "audience": {
        "schema": {
          "type": "string"
        }
      },
      "client_id": {
        "schema": {
          "type": "string"
        }
      },
      "client_secret": {
        "schema": {
          "type": "string"
        }
      },
      "enabled": {
        "schema": {
          "type": "boolean"
        }
      },
      "issuer_url": {
        "schema": {
          "type": "string"
        }
      },
      "jwks_url": {
        "schema": {
          "type": "string"
        }
      },
      "scopes": {
        "schema": {
          "type": "string"
        }
      }
    },
    "required": [
      "enabled"
    ]
  },
  "OIDCProviderResponse": {
    "type": "object",
    "properties": {
      "audience": {
        "schema": {
          "type": "string"
        }
      },
      "client_id": {
        "schema": {
          "type": "string"
        }
      },
      "enabled": {
        "schema": {
          "type": "boolean"
        }
      },
      "issuer_url": {
        "schema": {
          "type": "string"
        }
      },
      "jwks_url": {
        "schema": {
          "type": "string"
        }
      },
      "scopes": {
        "schema": {
          "type": "string"
        }
      },
      "secret_stored": {
        "schema": {
          "type": "boolean"
        }
      },
      "updated_at": {
        "schema": {
          "type": "string"
        }
      }
    },
    "required": [
      "enabled",
      "secret_stored"
    ]
  },
  "PaginatedAPIKeys": {
    "type": "object",
    "properties": {
      "data": {
        "schema": {
          "type": "array",
          "items": {
            "ref": "APIKeyInfo"
          }
        }
      },
      "next_page_token": {
        "schema": {
          "type": "string"
        }
      }
    },
    "required": [
      "data"
    ]
  },
  "PaginatedAuditLogs": {
    "type": "object",
    "properties": {
      "data": {
        "schema": {
          "type": "array",
          "items": {
            "ref": "AuditEntry"
          }
        }
      },
      "next_page_token": {
        "schema": {
          "type": "string"
        }
      }
    },
    "required": [
      "data"
    ]
  },
  "PaginatedBuilds": {
    "type": "object",
    "properties": {
      "data": {
        "schema": {
          "type": "array",
          "items": {
            "ref": "Build"
          }
        }
      },
      "next_page_token": {
        "schema": {
          "type": "string"
        }
      }
    },
    "required": [
      "data"
    ]
  },
  "PaginatedColumnDetails": {
    "type": "object",
    "properties": {
      "data": {
        "schema": {
          "type": "array",
          "items": {
            "ref": "ColumnDetail"
          }
        }
      },
      "next_page_token": {
        "schema": {
          "type": "string"
        }
      }
    },
    "required": [
      "data"
    ]
  },
  "PaginatedColumnLineageEdges": {
    "type": "object",
    "properties": {
      "data": {
        "schema": {
          "type": "array",
          "items": {
            "ref": "ColumnLineageEdge"
          }
        }
      },
      "next_page_token": {
        "schema": {
          "type": "string"
        }
      }
    },
    "required": [
      "data"
    ]
  },
  "PaginatedColumnMaskBindings": {
    "type": "object",
    "properties": {
      "data": {
        "schema": {
          "type": "array",
          "items": {
            "ref": "ColumnMaskBinding"
          }
        }
      },
      "next_page_token": {
        "schema": {
          "type": "string"
        }
      }
    },
    "required": [
      "data"
    ]
  },
  "PaginatedColumnMasks": {
    "type": "object",
    "properties": {
      "data": {
        "schema": {
          "type": "array",
          "items": {
            "ref": "ColumnMask"
          }
        }
      },
      "next_page_token": {
        "schema": {
          "type": "string"
        }
      }
    },
    "required": [
      "data"
    ]
  },
  "PaginatedComputeAssignments": {
    "type": "object",
    "properties": {
      "data": {
        "schema": {
          "type": "array",
          "items": {
            "ref": "ComputeAssignment"
          }
        }
      },
      "next_page_token": {
        "schema": {
          "type": "string"
        }
      }
    },
    "required": [
      "data"
    ]
  },
  "PaginatedComputeEndpoints": {
    "type": "object",
    "properties": {
      "data": {
        "schema": {
          "type": "array",
          "items": {
            "ref": "ComputeEndpoint"
          }
        }
      },
      "next_page_token": {
        "schema": {
          "type": "string"
        }
      }
    },
    "required": [
      "data"
    ]
  },
  "PaginatedDashboards": {
    "type": "object",
    "properties": {
      "data": {
        "schema": {
          "type": "array",
          "items": {
            "ref": "Dashboard"
          }
        }
      },
      "next_page_token": {
        "schema": {
          "type": "string"
        }
      }
    },
    "required": [
      "data"
    ]
  },
  "PaginatedEnvironments": {
    "type": "object",
    "properties": {
      "data": {
        "schema": {
          "type": "array",
          "items": {
            "ref": "Environment"
          }
        }
      },
      "next_page_token": {
        "schema": {
          "type": "string"
        }
      }
    },
    "required": [
      "data"
    ]
  },
  "PaginatedExternalLocations": {
    "type": "object",
    "properties": {
      "data": {
        "schema": {
          "type": "array",
          "items": {
            "ref": "ExternalLocation"
          }
        }
      },
      "next_page_token": {
        "schema": {
          "type": "string"
        }
      }
    },
    "required": [
      "data"
    ]
  },
  "PaginatedFolderContents": {
    "type": "object",
    "properties": {
      "data": {
        "schema": {
          "type": "array",
          "items": {
            "ref": "FolderContentItem"
          }
        }
      },
      "next_page_token": {
        "schema": {
          "type": "string"
        }
      }
    },
    "required": [
      "data"
    ]
  },
  "PaginatedFolders": {
    "type": "object",
    "properties": {
      "data": {
        "schema": {
          "type": "array",
          "items": {
            "ref": "Folder"
          }
        }
      },
      "next_page_token": {
        "schema": {
          "type": "string"
        }
      }
    },
    "required": [
      "data"
    ]
  },
  "PaginatedGitRepos": {
    "type": "object",
    "properties": {
      "data": {
        "schema": {
          "type": "array",
          "items": {
            "ref": "GitRepo"
          }
        }
      },
      "next_page_token": {
        "schema": {
          "type": "string"
        }
      }
    },
    "required": [
      "data"
    ]
  },
  "PaginatedGrants": {
    "type": "object",
    "properties": {
      "data": {
        "schema": {
          "type": "array",
          "items": {
            "ref": "PrivilegeGrant"
          }
        }
      },
      "next_page_token": {
        "schema": {
          "type": "string"
        }
      }
    },
    "required": [
      "data"
    ]
  },
  "PaginatedGroupMembers": {
    "type": "object",
    "properties": {
      "data": {
        "schema": {
          "type": "array",
          "items": {
            "ref": "GroupMember"
          }
        }
      },
      "next_page_token": {
        "schema": {
          "type": "string"
        }
      }
    },
    "required": [
      "data"
    ]
  },
  "PaginatedGroups": {
    "type": "object",
    "properties": {
      "data": {
        "schema": {
          "type": "array",
          "items": {
            "ref": "Group"
          }
        }
      },
      "next_page_token": {
        "schema": {
          "type": "string"
        }
      }
    },
    "required": [
      "data"
    ]
  },
  "PaginatedLineageEdges": {
    "type": "object",
    "properties": {
      "data": {
        "schema": {
          "type": "array",
          "items": {
            "ref": "LineageEdge"
          }
        }
      },
      "next_page_token": {
        "schema": {
          "type": "string"
        }
      }
    },
    "required": [
      "data"
    ]
  },
  "PaginatedMacros": {
    "type": "object",
    "properties": {
      "data": {
        "schema": {
          "type": "array",
          "items": {
            "ref": "Macro"
          }
        }
      },
      "next_page_token": {
        "schema": {
          "type": "string"
        }
      }
    },
    "required": [
      "data"
    ]
  },
  "PaginatedModelRuns": {
    "type": "object",
    "properties": {
      "data": {
        "schema": {
          "type": "array",
          "items": {
            "ref": "ModelRun"
          }
        }
      },
      "next_page_token": {
        "schema": {
          "type": "string"
        }
      }
    },
    "required": [
      "data"
    ]
  },
  "PaginatedModels": {
    "type": "object",
    "properties": {
      "data": {
        "schema": {
          "type": "array",
          "items": {
            "ref": "Model"
          }
        }
      },
      "next_page_token": {
        "schema": {
          "type": "string"
        }
      }
    },
    "required": [
      "data"
    ]
  },
  "PaginatedNotebookJobs": {
    "type": "object",
    "properties": {
      "data": {
        "schema": {
          "type": "array",
          "items": {
            "ref": "NotebookJob"
          }
        }
      },
      "next_page_token": {
        "schema": {
          "type": "string"
        }
      }
    },
    "required": [
      "data"
    ]
  },
  "PaginatedNotebooks": {
    "type": "object",
    "properties": {
      "data": {
        "schema": {
          "type": "array",
          "items": {
            "ref": "Notebook"
          }
        }
      },
      "next_page_token": {
        "schema": {
          "type": "string"
        }
      }
    },
    "required": [
      "data"
    ]
  },
  "PaginatedPipelineRuns": {
    "type": "object",
    "properties": {
      "data": {
        "schema": {
          "type": "array",
          "items": {
            "ref": "PipelineRun"
          }
        }
      },
      "next_page_token": {
        "schema": {
          "type": "string"
        }
      }
    },
    "required": [
      "data"
    ]
  },
  "PaginatedPipelines": {
    "type": "object",
    "properties": {
      "data": {
        "schema": {
          "type": "array",
          "items": {
            "ref": "Pipeline"
          }
        }
      },
      "next_page_token": {
        "schema": {
          "type": "string"
        }
      }
    },
    "required": [
      "data"
    ]
  },
  "PaginatedPrincipals": {
    "type": "object",
    "properties": {
      "data": {
        "schema": {
          "type": "array",
          "items": {
            "ref": "Principal"
          }
        }
      },
      "next_page_token": {
        "schema": {
          "type": "string"
        }
      }
    },
    "required": [
      "data"
    ]
  },
  "PaginatedProjects": {
    "type": "object",
    "properties": {
      "data": {
        "schema": {
          "type": "array",
          "items": {
            "ref": "Project"
          }
        }
      },
      "next_page_token": {
        "schema": {
          "type": "string"
        }
      }
    },
    "required": [
      "data"
    ]
  },
  "PaginatedQueryHistoryEntries": {
    "type": "object",
    "properties": {
      "data": {
        "schema": {
          "type": "array",
          "items": {
            "ref": "QueryHistoryEntry"
          }
        }
      },
      "next_page_token": {
        "schema": {
          "type": "string"
        }
      }
    },
    "required": [
      "data"
    ]
  },
  "PaginatedQueryJobs": {
    "type": "object",
    "properties": {
      "data": {
        "schema": {
          "type": "array",
          "items": {
            "ref": "QueryJob"
          }
        }
      },
      "next_page_token": {
        "schema": {
          "type": "string"
        }
      }
    },
    "required": [
      "data"
    ]
  },
  "PaginatedRecentResources": {
    "type": "object",
    "properties": {
      "data": {
        "schema": {
          "type": "array",
          "items": {
            "ref": "RecentResource"
          }
        }
      },
      "next_page_token": {
        "schema": {
          "type": "string"
        }
      }
    },
    "required": [
      "data"
    ]
  },
  "PaginatedRowFilterBindings": {
    "type": "object",
    "properties": {
      "data": {
        "schema": {
          "type": "array",
          "items": {
            "ref": "RowFilterBinding"
          }
        }
      },
      "next_page_token": {
        "schema": {
          "type": "string"
        }
      }
    },
    "required": [
      "data"
    ]
  },
  "PaginatedRowFilters": {
    "type": "object",
    "properties": {
      "data": {
        "schema": {
          "type": "array",
          "items": {
            "ref": "RowFilter"
          }
        }
      },
      "next_page_token": {
        "schema": {
          "type": "string"
        }
      }
    },
    "required": [
      "data"
    ]
  },
  "PaginatedSavedResources": {
    "type": "object",
    "properties": {
      "data": {
        "schema": {
          "type": "array",
          "items": {
            "ref": "SavedResource"
          }
        }
      },
      "next_page_token": {
        "schema": {
          "type": "string"
        }
      }
    },
    "required": [
      "data"
    ]
  },
  "PaginatedSchemaDetails": {
    "type": "object",
    "properties": {
      "data": {
        "schema": {
          "type": "array",
          "items": {
            "ref": "SchemaDetail"
          }
        }
      },
      "next_page_token": {
        "schema": {
          "type": "string"
        }
      }
    },
    "required": [
      "data"
    ]
  },
  "PaginatedSearchResults": {
    "type": "object",
    "properties": {
      "data": {
        "schema": {
          "type": "array",
          "items": {
            "ref": "SearchResult"
          }
        }
      },
      "next_page_token": {
        "schema": {
          "type": "string"
        }
      }
    },
    "required": [
      "data"
    ]
  },
  "PaginatedSemanticModels": {
    "type": "object",
    "properties": {
      "data": {
        "schema": {
          "type": "array",
          "items": {
            "ref": "SemanticModel"
          }
        }
      },
      "next_page_token": {
        "schema": {
          "type": "string"
        }
      }
    },
    "required": [
      "data"
    ]
  },
  "PaginatedStorageCredentials": {
    "type": "object",
    "properties": {
      "data": {
        "schema": {
          "type": "array",
          "items": {
            "ref": "StorageCredential"
          }
        }
      },
      "next_page_token": {
        "schema": {
          "type": "string"
        }
      }
    },
    "required": [
      "data"
    ]
  },
  "PaginatedTableDetails": {
    "type": "object",
    "properties": {
      "data": {
        "schema": {
          "type": "array",
          "items": {
            "ref": "TableDetail"
          }
        }
      },
      "next_page_token": {
        "schema": {
          "type": "string"
        }
      }
    },
    "required": [
      "data"
    ]
  },
  "PaginatedTagAssignments": {
    "type": "object",
    "properties": {
      "data": {
        "schema": {
          "type": "array",
          "items": {
            "ref": "TagAssignment"
          }
        }
      },
      "next_page_token": {
        "schema": {
          "type": "string"
        }
      }
    },
    "required": [
      "data"
    ]
  },
  "PaginatedTags": {
    "type": "object",
    "properties": {
      "data": {
        "schema": {
          "type": "array",
          "items": {
            "ref": "Tag"
          }
        }
      },
      "next_page_token": {
        "schema": {
          "type": "string"
        }
      }
    },
    "required": [
      "data"
    ]
  },
  "PaginatedViewDetails": {
    "type": "object",
    "properties": {
      "data": {
        "schema": {
          "type": "array",
          "items": {
            "ref": "ViewDetail"
          }
        }
      },
      "next_page_token": {
        "schema": {
          "type": "string"
        }
      }
    },
    "required": [
      "data"
    ]
  },
  "PaginatedVolumes": {
    "type": "object",
    "properties": {
      "data": {
        "schema": {
          "type": "array",
          "items": {
            "ref": "VolumeDetail"
          }
        }
      },
      "next_page_token": {
        "schema": {
          "type": "string"
        }
      }
    },
    "required": [
      "data"
    ]
  },
  "PaginatedWorkspaces": {
    "type": "object",
    "properties": {
      "data": {
        "schema": {
          "type": "array",
          "items": {
            "ref": "Workspace"
          }
        }
      },
      "next_page_token": {
        "schema": {
          "type": "string"
        }
      }
    },
    "required": [
      "data"
    ]
  },
  "Pipeline": {
    "type": "object",
    "properties": {
      "concurrency_limit": {
        "schema": {
          "type": "integer",
          "format": "int32"
        }
      },
      "created_at": {
        "schema": {
          "type": "string"
        }
      },
      "created_by": {
        "schema": {
          "type": "string"
        }
      },
      "description": {
        "schema": {
          "type": "string"
        }
      },
      "folder_id": {
        "schema": {
          "type": "string"
        }
      },
      "id": {
        "schema": {
          "type": "string"
        }
      },
      "is_paused": {
        "schema": {
          "type": "boolean"
        }
      },
      "name": {
        "schema": {
          "type": "string"
        }
      },
      "schedule_cron": {
        "schema": {
          "type": "string"
        }
      },
      "updated_at": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "PipelineJob": {
    "type": "object",
    "properties": {
      "compute_endpoint_id": {
        "schema": {
          "type": "string"
        }
      },
      "created_at": {
        "schema": {
          "type": "string"
        }
      },
      "depends_on": {
        "schema": {
          "type": "array",
          "items": {
            "type": "string"
          }
        }
      },
      "id": {
        "schema": {
          "type": "string"
        }
      },
      "job_order": {
        "schema": {
          "type": "integer",
          "format": "int32"
        }
      },
      "job_type": {
        "schema": {
          "ref": "PipelineJobJobType"
        }
      },
      "model_selector": {
        "schema": {
          "type": "string"
        }
      },
      "name": {
        "schema": {
          "type": "string"
        }
      },
      "notebook_id": {
        "schema": {
          "type": "string"
        }
      },
      "pipeline_id": {
        "schema": {
          "type": "string"
        }
      },
      "retry_count": {
        "schema": {
          "type": "integer",
          "format": "int32"
        }
      },
      "timeout_seconds": {
        "schema": {
          "type": "integer",
          "format": "int64"
        }
      }
    }
  },
  "PipelineJobJobType": {
    "type": "string",
    "enum": [
      "NOTEBOOK",
      "MODEL_RUN"
    ]
  },
  "PipelineJobList": {
    "type": "object",
    "properties": {
      "data": {
        "schema": {
          "type": "array",
          "items": {
            "ref": "PipelineJob"
          }
        }
      }
    },
    "required": [
      "data"
    ]
  },
  "PipelineJobRun": {
    "type": "object",
    "properties": {
      "created_at": {
        "schema": {
          "type": "string"
        }
      },
      "error_message": {
        "schema": {
          "type": "string"
        }
      },
      "finished_at": {
        "schema": {
          "type": "string"
        }
      },
      "id": {
        "schema": {
          "type": "string"
        }
      },
      "job_id": {
        "schema": {
          "type": "string"
        }
      },
      "job_name": {
        "schema": {
          "type": "string"
        }
      },
      "retry_attempt": {
        "schema": {
          "type": "integer",
          "format": "int32"
        }
      },
      "run_id": {
        "schema": {
          "type": "string"
        }
      },
      "started_at": {
        "schema": {
          "type": "string"
        }
      },
      "status": {
        "schema": {
          "ref": "PipelineJobRunStatus"
        }
      }
    }
  },
  "PipelineJobRunList": {
    "type": "object",
    "properties": {
      "data": {
        "schema": {
          "type": "array",
          "items": {
            "ref": "PipelineJobRun"
          }
        }
      }
    },
    "required": [
      "data"
    ]
  },
  "PipelineJobRunStatus": {
    "type": "string",
    "enum": [
      "PENDING",
      "RUNNING",
      "SUCCESS",
      "FAILED",
      "SKIPPED",
      "CANCELLED"
    ]
  },
  "PipelineRun": {
    "type": "object",
    "properties": {
      "created_at": {
        "schema": {
          "type": "string"
        }
      },
      "error_message": {
        "schema": {
          "type": "string"
        }
      },
      "finished_at": {
        "schema": {
          "type": "string"
        }
      },
      "git_commit_hash": {
        "schema": {
          "type": "string"
        }
      },
      "id": {
        "schema": {
          "type": "string"
        }
      },
      "parameters": {
        "schema": {
          "ref": "Record"
        }
      },
      "pipeline_id": {
        "schema": {
          "type": "string"
        }
      },
      "started_at": {
        "schema": {
          "type": "string"
        }
      },
      "status": {
        "schema": {
          "ref": "PipelineRunStatus"
        }
      },
      "trigger_type": {
        "schema": {
          "ref": "PipelineRunTriggerType"
        }
      },
      "triggered_by": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "PipelineRunStatus": {
    "type": "string",
    "enum": [
      "PENDING",
      "RUNNING",
      "SUCCESS",
      "FAILED",
      "CANCELLED"
    ]
  },
  "PipelineRunTriggerType": {
    "type": "string",
    "enum": [
      "MANUAL",
      "SCHEDULED"
    ]
  },
  "Principal": {
    "type": "object",
    "properties": {
      "created_at": {
        "schema": {
          "type": "string"
        }
      },
      "id": {
        "schema": {
          "type": "string"
        }
      },
      "is_admin": {
        "schema": {
          "type": "boolean"
        }
      },
      "name": {
        "schema": {
          "type": "string"
        }
      },
      "type": {
        "schema": {
          "ref": "PrincipalType"
        }
      }
    },
    "required": [
      "id",
      "name",
      "type",
      "is_admin"
    ]
  },
  "PrincipalType": {
    "type": "string",
    "enum": [
      "user",
      "group"
    ]
  },
  "PrivilegeGrant": {
    "type": "object",
    "properties": {
      "granted_at": {
        "schema": {
          "type": "string"
        }
      },
      "granted_by": {
        "schema": {
          "type": "string"
        }
      },
      "id": {
        "schema": {
          "type": "string"
        }
      },
      "principal_id": {
        "schema": {
          "type": "string"
        }
      },
      "principal_type": {
        "schema": {
          "ref": "PrincipalType"
        }
      },
      "privilege": {
        "schema": {
          "type": "string"
        }
      },
      "securable_id": {
        "schema": {
          "type": "string"
        }
      },
      "securable_type": {
        "schema": {
          "type": "string"
        }
      }
    },
    "required": [
      "id",
      "principal_id",
      "principal_type",
      "securable_type",
      "securable_id",
      "privilege"
    ]
  },
  "Project": {
    "type": "object",
    "properties": {
      "created_at": {
        "schema": {
          "type": "string"
        }
      },
      "default_branch": {
        "schema": {
          "type": "string"
        }
      },
      "description": {
        "schema": {
          "type": "string"
        }
      },
      "id": {
        "schema": {
          "type": "string"
        }
      },
      "kind": {
        "schema": {
          "ref": "ProjectKind"
        }
      },
      "name": {
        "schema": {
          "type": "string"
        }
      },
      "owner_principal": {
        "schema": {
          "type": "string"
        }
      },
      "owner_team_id": {
        "schema": {
          "type": "string"
        }
      },
      "product_id": {
        "schema": {
          "type": "string"
        }
      },
      "updated_at": {
        "schema": {
          "type": "string"
        }
      },
      "workspace_id": {
        "schema": {
          "type": "string"
        }
      }
    },
    "required": [
      "workspace_id",
      "name",
      "kind"
    ]
  },
  "ProjectKind": {
    "type": "string",
    "enum": [
      "personal",
      "shared",
      "library"
    ]
  },
  "PromoteNotebookRequest": {
    "type": "object",
    "properties": {
      "cell_index": {
        "schema": {
          "type": "integer",
          "format": "int32"
        }
      },
      "materialization": {
        "schema": {
          "ref": "ModelMaterialization"
        }
      },
      "name": {
        "schema": {
          "type": "string"
        }
      },
      "project_name": {
        "schema": {
          "type": "string"
        }
      }
    },
    "required": [
      "cell_index",
      "project_name",
      "name"
    ]
  },
  "PurgeLineageRequest": {
    "type": "object",
    "properties": {
      "older_than_days": {
        "schema": {
          "type": "integer",
          "format": "int32"
        }
      }
    },
    "required": [
      "older_than_days"
    ]
  },
  "PurgeLineageResponse": {
    "type": "object",
    "properties": {
      "deleted_count": {
        "schema": {
          "type": "integer",
          "format": "int64"
        }
      }
    }
  },
  "QueryHistoryEntry": {
    "type": "object",
    "properties": {
      "created_at": {
        "schema": {
          "type": "string"
        }
      },
      "duration_ms": {
        "schema": {
          "type": "integer",
          "format": "int64"
        }
      },
      "error_message": {
        "schema": {
          "type": "string"
        }
      },
      "id": {
        "schema": {
          "type": "string"
        }
      },
      "original_sql": {
        "schema": {
          "type": "string"
        }
      },
      "principal_name": {
        "schema": {
          "type": "string"
        }
      },
      "rewritten_sql": {
        "schema": {
          "type": "string"
        }
      },
      "rows_returned": {
        "schema": {
          "type": "integer",
          "format": "int64"
        }
      },
      "statement_type": {
        "schema": {
          "type": "string"
        }
      },
      "status": {
        "schema": {
          "ref": "AuditDecisionStatus"
        }
      },
      "tables_accessed": {
        "schema": {
          "type": "array",
          "items": {
            "type": "string"
          }
        }
      }
    },
    "required": [
      "id"
    ]
  },
  "QueryJob": {
    "type": "object",
    "properties": {
      "completed_at": {
        "schema": {
          "type": "string"
        }
      },
      "created_at": {
        "schema": {
          "type": "string"
        }
      },
      "error": {
        "schema": {
          "type": "string"
        }
      },
      "query_id": {
        "schema": {
          "type": "string"
        }
      },
      "request_id": {
        "schema": {
          "type": "string"
        }
      },
      "row_count": {
        "schema": {
          "type": "integer",
          "format": "int64"
        }
      },
      "started_at": {
        "schema": {
          "type": "string"
        }
      },
      "status": {
        "schema": {
          "ref": "QueryJobStatus"
        }
      }
    },
    "required": [
      "query_id",
      "status",
      "row_count"
    ]
  },
  "QueryJobStatus": {
    "type": "string",
    "enum": [
      "QUEUED",
      "RUNNING",
      "SUCCEEDED",
      "FAILED",
      "CANCELED"
    ]
  },
  "QueryRequest": {
    "type": "object",
    "properties": {
      "sql": {
        "schema": {
          "type": "string"
        }
      }
    },
    "required": [
      "sql"
    ]
  },
  "QueryResult": {
    "type": "object",
    "properties": {
      "columns": {
        "schema": {
          "type": "array",
          "items": {
            "ref": "TabularColumn"
          }
        }
      },
      "next_page_token": {
        "schema": {
          "type": "string"
        }
      },
      "row_count": {
        "schema": {
          "type": "integer",
          "format": "int64"
        }
      },
      "rows": {
        "schema": {
          "type": "array",
          "items": {
            "ref": "Record"
          }
        }
      }
    },
    "required": [
      "columns",
      "rows"
    ]
  },
  "RecentResource": {
    "type": "object",
    "properties": {
      "accessed_at": {
        "schema": {
          "type": "string"
        }
      },
      "display_name": {
        "schema": {
          "type": "string"
        }
      },
      "href": {
        "schema": {
          "type": "string"
        }
      },
      "resource_key": {
        "schema": {
          "type": "string"
        }
      },
      "resource_path": {
        "schema": {
          "type": "string"
        }
      },
      "resource_type": {
        "schema": {
          "type": "string"
        }
      },
      "section": {
        "schema": {
          "type": "string"
        }
      }
    },
    "required": [
      "resource_type",
      "resource_key",
      "display_name"
    ]
  },
  "Record": {
    "type": "object"
  },
  "ReorderCellsRequest": {
    "type": "object",
    "properties": {
      "cell_ids": {
        "schema": {
          "type": "array",
          "items": {
            "type": "string"
          }
        }
      }
    },
    "required": [
      "cell_ids"
    ]
  },
  "ResolvedDashboardDetail": {
    "type": "object",
    "properties": {
      "dashboard": {
        "schema": {
          "ref": "Dashboard"
        }
      },
      "widgets": {
        "schema": {
          "type": "array",
          "items": {
            "ref": "ResolvedDashboardWidget"
          }
        }
      }
    }
  },
  "ResolvedDashboardWidget": {
    "type": "object",
    "properties": {
      "columns": {
        "schema": {
          "type": "array",
          "items": {
            "type": "string"
          }
        }
      },
      "generated_sql": {
        "schema": {
          "type": "string"
        }
      },
      "row_count": {
        "schema": {
          "type": "integer",
          "format": "int64"
        }
      },
      "rows": {
        "schema": {
          "type": "array",
          "items": {
            "type": "array",
            "items": {
              "type": "string"
            }
          }
        }
      },
      "widget": {
        "schema": {
          "ref": "DashboardWidget"
        }
      }
    },
    "required": [
      "columns"
    ]
  },
  "RevokeWebSessionsRequest": {
    "type": "object",
    "properties": {
      "principal_id": {
        "schema": {
          "type": "string"
        }
      }
    },
    "required": [
      "principal_id"
    ]
  },
  "RowFilter": {
    "type": "object",
    "properties": {
      "created_at": {
        "schema": {
          "type": "string"
        }
      },
      "description": {
        "schema": {
          "type": "string"
        }
      },
      "filter_sql": {
        "schema": {
          "type": "string"
        }
      },
      "id": {
        "schema": {
          "type": "string"
        }
      },
      "name": {
        "schema": {
          "type": "string"
        }
      },
      "table_id": {
        "schema": {
          "type": "string"
        }
      }
    },
    "required": [
      "id",
      "table_id",
      "name",
      "filter_sql"
    ]
  },
  "RowFilterBinding": {
    "type": "object",
    "properties": {
      "id": {
        "schema": {
          "type": "string"
        }
      },
      "principal_id": {
        "schema": {
          "type": "string"
        }
      },
      "principal_type": {
        "schema": {
          "ref": "PrincipalType"
        }
      },
      "row_filter_id": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "RowFilterBindingRequest": {
    "type": "object",
    "properties": {
      "principal_id": {
        "schema": {
          "type": "string"
        }
      },
      "principal_type": {
        "schema": {
          "ref": "PrincipalType"
        }
      }
    },
    "required": [
      "principal_id",
      "principal_type"
    ]
  },
  "RunAllResult": {
    "type": "object",
    "properties": {
      "notebook_id": {
        "schema": {
          "type": "string"
        }
      },
      "results": {
        "schema": {
          "type": "array",
          "items": {
            "ref": "CellExecutionResult"
          }
        }
      },
      "total_duration_ms": {
        "schema": {
          "type": "integer",
          "format": "int64"
        }
      }
    }
  },
  "SavedResource": {
    "type": "object",
    "properties": {
      "display_name": {
        "schema": {
          "type": "string"
        }
      },
      "href": {
        "schema": {
          "type": "string"
        }
      },
      "last_accessed_at": {
        "schema": {
          "type": "string"
        }
      },
      "resource_key": {
        "schema": {
          "type": "string"
        }
      },
      "resource_path": {
        "schema": {
          "type": "string"
        }
      },
      "resource_type": {
        "schema": {
          "type": "string"
        }
      },
      "saved_at": {
        "schema": {
          "type": "string"
        }
      },
      "section": {
        "schema": {
          "type": "string"
        }
      }
    },
    "required": [
      "resource_type",
      "resource_key",
      "display_name"
    ]
  },
  "SchemaDetail": {
    "type": "object",
    "properties": {
      "catalog_name": {
        "schema": {
          "type": "string"
        }
      },
      "comment": {
        "schema": {
          "type": "string"
        }
      },
      "created_at": {
        "schema": {
          "type": "string"
        }
      },
      "name": {
        "schema": {
          "type": "string"
        }
      },
      "owner": {
        "schema": {
          "type": "string"
        }
      },
      "properties": {
        "schema": {
          "ref": "Record"
        }
      },
      "schema_id": {
        "schema": {
          "type": "string"
        }
      },
      "tags": {
        "schema": {
          "type": "array",
          "items": {
            "ref": "Tag"
          }
        }
      },
      "updated_at": {
        "schema": {
          "type": "string"
        }
      }
    },
    "required": [
      "schema_id",
      "name",
      "catalog_name"
    ]
  },
  "SearchResult": {
    "type": "object",
    "properties": {
      "comment": {
        "schema": {
          "type": "string"
        }
      },
      "match_field": {
        "schema": {
          "type": "string"
        }
      },
      "name": {
        "schema": {
          "type": "string"
        }
      },
      "schema_name": {
        "schema": {
          "type": "string"
        }
      },
      "table_name": {
        "schema": {
          "type": "string"
        }
      },
      "type": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "SemanticMetric": {
    "type": "object",
    "properties": {
      "certification_state": {
        "schema": {
          "ref": "CreateSemanticMetricRequestCertificationState"
        }
      },
      "created_at": {
        "schema": {
          "type": "string"
        }
      },
      "created_by": {
        "schema": {
          "type": "string"
        }
      },
      "default_time_grain": {
        "schema": {
          "type": "string"
        }
      },
      "description": {
        "schema": {
          "type": "string"
        }
      },
      "expression": {
        "schema": {
          "type": "string"
        }
      },
      "expression_mode": {
        "schema": {
          "ref": "SemanticMetricExpressionMode"
        }
      },
      "filter_sql": {
        "schema": {
          "type": "string"
        }
      },
      "format": {
        "schema": {
          "type": "string"
        }
      },
      "id": {
        "schema": {
          "type": "string"
        }
      },
      "label": {
        "schema": {
          "type": "string"
        }
      },
      "metric_type": {
        "schema": {
          "ref": "SemanticMetricMetricType"
        }
      },
      "name": {
        "schema": {
          "type": "string"
        }
      },
      "owner": {
        "schema": {
          "type": "string"
        }
      },
      "relationship_names": {
        "schema": {
          "type": "array",
          "items": {
            "type": "string"
          }
        }
      },
      "semantic_model_id": {
        "schema": {
          "type": "string"
        }
      },
      "updated_at": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "SemanticMetricExpressionMode": {
    "type": "string",
    "enum": [
      "DSL",
      "SQL"
    ]
  },
  "SemanticMetricList": {
    "type": "object",
    "properties": {
      "data": {
        "schema": {
          "type": "array",
          "items": {
            "ref": "SemanticMetric"
          }
        }
      }
    },
    "required": [
      "data"
    ]
  },
  "SemanticMetricMetricType": {
    "type": "string",
    "enum": [
      "SUM",
      "COUNT",
      "COUNT_DISTINCT",
      "AVG",
      "MIN",
      "MAX",
      "RATIO"
    ]
  },
  "SemanticModel": {
    "type": "object",
    "properties": {
      "base_model_ref": {
        "schema": {
          "type": "string"
        }
      },
      "created_at": {
        "schema": {
          "type": "string"
        }
      },
      "created_by": {
        "schema": {
          "type": "string"
        }
      },
      "default_time_dimension": {
        "schema": {
          "type": "string"
        }
      },
      "description": {
        "schema": {
          "type": "string"
        }
      },
      "id": {
        "schema": {
          "type": "string"
        }
      },
      "name": {
        "schema": {
          "type": "string"
        }
      },
      "owner": {
        "schema": {
          "type": "string"
        }
      },
      "tags": {
        "schema": {
          "type": "array",
          "items": {
            "type": "string"
          }
        }
      },
      "updated_at": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "SemanticPreAggregation": {
    "type": "object",
    "properties": {
      "created_at": {
        "schema": {
          "type": "string"
        }
      },
      "created_by": {
        "schema": {
          "type": "string"
        }
      },
      "dimension_set": {
        "schema": {
          "type": "array",
          "items": {
            "type": "string"
          }
        }
      },
      "grain": {
        "schema": {
          "type": "string"
        }
      },
      "id": {
        "schema": {
          "type": "string"
        }
      },
      "metric_set": {
        "schema": {
          "type": "array",
          "items": {
            "type": "string"
          }
        }
      },
      "name": {
        "schema": {
          "type": "string"
        }
      },
      "refresh_policy": {
        "schema": {
          "type": "string"
        }
      },
      "semantic_model_id": {
        "schema": {
          "type": "string"
        }
      },
      "target_relation": {
        "schema": {
          "type": "string"
        }
      },
      "updated_at": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "SemanticPreAggregationList": {
    "type": "object",
    "properties": {
      "data": {
        "schema": {
          "type": "array",
          "items": {
            "ref": "SemanticPreAggregation"
          }
        }
      }
    },
    "required": [
      "data"
    ]
  },
  "SemanticRelationship": {
    "type": "object",
    "properties": {
      "cost": {
        "schema": {
          "type": "integer",
          "format": "int32"
        }
      },
      "created_at": {
        "schema": {
          "type": "string"
        }
      },
      "created_by": {
        "schema": {
          "type": "string"
        }
      },
      "from_semantic_id": {
        "schema": {
          "type": "string"
        }
      },
      "id": {
        "schema": {
          "type": "string"
        }
      },
      "join_sql": {
        "schema": {
          "type": "string"
        }
      },
      "max_hops": {
        "schema": {
          "type": "integer",
          "format": "int32"
        }
      },
      "name": {
        "schema": {
          "type": "string"
        }
      },
      "relationship_type": {
        "schema": {
          "ref": "SemanticRelationshipRelationshipType"
        }
      },
      "to_semantic_id": {
        "schema": {
          "type": "string"
        }
      },
      "updated_at": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "SemanticRelationshipList": {
    "type": "object",
    "properties": {
      "data": {
        "schema": {
          "type": "array",
          "items": {
            "ref": "SemanticRelationship"
          }
        }
      }
    },
    "required": [
      "data"
    ]
  },
  "SemanticRelationshipRelationshipType": {
    "type": "string",
    "enum": [
      "ONE_TO_ONE",
      "ONE_TO_MANY",
      "MANY_TO_ONE",
      "MANY_TO_MANY"
    ]
  },
  "SetDefaultCatalogRequest": {
    "type": "object"
  },
  "ShareFolderRequest": {
    "type": "object",
    "properties": {
      "principal_name": {
        "schema": {
          "type": "string"
        }
      },
      "role": {
        "schema": {
          "ref": "NotebookShareRole"
        }
      }
    },
    "required": [
      "principal_name"
    ]
  },
  "ShareNotebookRequest": {
    "type": "object",
    "properties": {
      "principal_name": {
        "schema": {
          "type": "string"
        }
      },
      "role": {
        "schema": {
          "ref": "NotebookShareRole"
        }
      }
    },
    "required": [
      "principal_name"
    ]
  },
  "SourceFreshnessStatus": {
    "type": "object",
    "properties": {
      "is_fresh": {
        "schema": {
          "type": "boolean"
        }
      },
      "last_loaded_at": {
        "schema": {
          "type": "string"
        }
      },
      "max_lag_seconds": {
        "schema": {
          "type": "integer",
          "format": "int64"
        }
      },
      "source_schema": {
        "schema": {
          "type": "string"
        }
      },
      "source_table": {
        "schema": {
          "type": "string"
        }
      },
      "stale_since": {
        "schema": {
          "type": "string"
        }
      },
      "timestamp_column": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "StorageCredential": {
    "type": "object",
    "properties": {
      "comment": {
        "schema": {
          "type": "string"
        }
      },
      "created_at": {
        "schema": {
          "type": "string"
        }
      },
      "credential_type": {
        "schema": {
          "ref": "StorageCredentialType"
        }
      },
      "endpoint": {
        "schema": {
          "type": "string"
        }
      },
      "id": {
        "schema": {
          "type": "string"
        }
      },
      "name": {
        "schema": {
          "type": "string"
        }
      },
      "owner": {
        "schema": {
          "type": "string"
        }
      },
      "region": {
        "schema": {
          "type": "string"
        }
      },
      "updated_at": {
        "schema": {
          "type": "string"
        }
      },
      "url_style": {
        "schema": {
          "ref": "URLStyle"
        }
      }
    },
    "required": [
      "id",
      "name"
    ]
  },
  "StorageCredentialType": {
    "type": "string",
    "enum": [
      "S3",
      "AZURE",
      "GCS"
    ]
  },
  "StorageType": {
    "type": "string",
    "enum": [
      "S3",
      "AZURE",
      "GCS"
    ]
  },
  "SubmitQueryRequest": {
    "type": "object",
    "properties": {
      "request_id": {
        "schema": {
          "type": "string"
        }
      },
      "sql": {
        "schema": {
          "type": "string"
        }
      }
    },
    "required": [
      "sql"
    ]
  },
  "SubmitQueryResponse": {
    "type": "object",
    "properties": {
      "query_id": {
        "schema": {
          "type": "string"
        }
      },
      "status": {
        "schema": {
          "ref": "QueryJobStatus"
        }
      }
    },
    "required": [
      "query_id",
      "status"
    ]
  },
  "TableDetail": {
    "type": "object",
    "properties": {
      "catalog_name": {
        "schema": {
          "type": "string"
        }
      },
      "columns": {
        "schema": {
          "type": "array",
          "items": {
            "ref": "ColumnDetail"
          }
        }
      },
      "comment": {
        "schema": {
          "type": "string"
        }
      },
      "created_at": {
        "schema": {
          "type": "string"
        }
      },
      "name": {
        "schema": {
          "type": "string"
        }
      },
      "owner": {
        "schema": {
          "type": "string"
        }
      },
      "properties": {
        "schema": {
          "ref": "Record"
        }
      },
      "schema_name": {
        "schema": {
          "type": "string"
        }
      },
      "statistics": {
        "schema": {
          "ref": "TableStatistics"
        }
      },
      "table_id": {
        "schema": {
          "type": "string"
        }
      },
      "table_type": {
        "schema": {
          "type": "string"
        }
      },
      "tags": {
        "schema": {
          "type": "array",
          "items": {
            "ref": "Tag"
          }
        }
      },
      "updated_at": {
        "schema": {
          "type": "string"
        }
      }
    },
    "required": [
      "table_id",
      "name",
      "schema_name",
      "catalog_name"
    ]
  },
  "TableStatistics": {
    "type": "object",
    "properties": {
      "column_count": {
        "schema": {
          "type": "integer",
          "format": "int32"
        }
      },
      "last_profiled_at": {
        "schema": {
          "type": "string"
        }
      },
      "profiled_by": {
        "schema": {
          "type": "string"
        }
      },
      "row_count": {
        "schema": {
          "type": "integer",
          "format": "int64"
        }
      },
      "size_bytes": {
        "schema": {
          "type": "integer",
          "format": "int64"
        }
      }
    }
  },
  "TabularColumn": {
    "type": "object",
    "properties": {
      "name": {
        "schema": {
          "type": "string"
        }
      }
    },
    "required": [
      "name"
    ]
  },
  "Tag": {
    "type": "object",
    "properties": {
      "created_at": {
        "schema": {
          "type": "string"
        }
      },
      "created_by": {
        "schema": {
          "type": "string"
        }
      },
      "id": {
        "schema": {
          "type": "string"
        }
      },
      "key": {
        "schema": {
          "type": "string"
        }
      },
      "value": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "TagAssignment": {
    "type": "object",
    "properties": {
      "assigned_at": {
        "schema": {
          "type": "string"
        }
      },
      "assigned_by": {
        "schema": {
          "type": "string"
        }
      },
      "column_name": {
        "schema": {
          "type": "string"
        }
      },
      "id": {
        "schema": {
          "type": "string"
        }
      },
      "securable_id": {
        "schema": {
          "type": "string"
        }
      },
      "securable_type": {
        "schema": {
          "ref": "TagAssignmentSecurableType"
        }
      },
      "tag_id": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "TagAssignmentSecurableType": {
    "type": "string",
    "enum": [
      "schema",
      "table",
      "column",
      "macro"
    ]
  },
  "TriggerModelRunRequest": {
    "type": "object",
    "properties": {
      "environment_name": {
        "schema": {
          "type": "string"
        }
      },
      "full_refresh": {
        "schema": {
          "type": "boolean"
        }
      },
      "model_names": {
        "schema": {
          "type": "array",
          "items": {
            "type": "string"
          }
        }
      },
      "project_name": {
        "schema": {
          "type": "string"
        }
      }
    },
    "required": [
      "project_name"
    ]
  },
  "TriggerPipelineRunRequest": {
    "type": "object",
    "properties": {
      "parameters": {
        "schema": {
          "ref": "Record"
        }
      }
    }
  },
  "URLStyle": {
    "type": "string",
    "enum": [
      "path",
      "vhost"
    ]
  },
  "UpdateCatalogRegistrationRequest": {
    "type": "object",
    "properties": {
      "comment": {
        "schema": {
          "type": "string"
        }
      },
      "data_path": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "UpdateCellRequest": {
    "type": "object",
    "properties": {
      "content": {
        "schema": {
          "type": "string"
        }
      },
      "disabled": {
        "schema": {
          "type": "boolean"
        }
      },
      "name": {
        "schema": {
          "type": "string"
        }
      },
      "position": {
        "schema": {
          "type": "integer",
          "format": "int32"
        }
      },
      "role": {
        "schema": {
          "ref": "CellRole"
        }
      },
      "test": {
        "schema": {
          "ref": "NotebookCellTestConfig"
        }
      },
      "visual_spec": {
        "schema": {
          "ref": "VisualSpec"
        }
      }
    }
  },
  "UpdateColumnMaskRequest": {
    "type": "object",
    "properties": {
      "column_name": {
        "schema": {
          "type": "string"
        }
      },
      "description": {
        "schema": {
          "type": "string"
        }
      },
      "mask_expression": {
        "schema": {
          "type": "string"
        }
      },
      "name": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "UpdateColumnRequest": {
    "type": "object",
    "properties": {
      "comment": {
        "schema": {
          "type": "string"
        }
      },
      "nullable": {
        "schema": {
          "type": "boolean"
        }
      }
    }
  },
  "UpdateComputeEndpointRequest": {
    "type": "object",
    "properties": {
      "auth_token": {
        "schema": {
          "type": "string"
        }
      },
      "max_memory_gb": {
        "schema": {
          "type": "integer",
          "format": "int64"
        }
      },
      "size": {
        "schema": {
          "ref": "ComputeEndpointSize"
        }
      },
      "status": {
        "schema": {
          "ref": "ComputeEndpointStatus"
        }
      },
      "url": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "UpdateDashboardRequest": {
    "type": "object",
    "properties": {
      "description": {
        "schema": {
          "type": "string"
        }
      },
      "folder_id": {
        "schema": {
          "type": "string"
        }
      },
      "name": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "UpdateDashboardWidgetRequest": {
    "type": "object",
    "properties": {
      "description": {
        "schema": {
          "type": "string"
        }
      },
      "layout": {
        "schema": {
          "ref": "DashboardWidgetLayout"
        }
      },
      "name": {
        "schema": {
          "type": "string"
        }
      },
      "source": {
        "schema": {
          "ref": "DashboardWidgetSource"
        }
      },
      "visual_spec": {
        "schema": {
          "ref": "VisualSpec"
        }
      }
    }
  },
  "UpdateExternalLocationRequest": {
    "type": "object",
    "properties": {
      "comment": {
        "schema": {
          "type": "string"
        }
      },
      "credential_name": {
        "schema": {
          "type": "string"
        }
      },
      "read_only": {
        "schema": {
          "type": "boolean"
        }
      },
      "url": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "UpdateFolderRequest": {
    "type": "object",
    "properties": {
      "default_environment_id": {
        "schema": {
          "type": "string"
        }
      },
      "default_project_id": {
        "schema": {
          "type": "string"
        }
      },
      "git_repo_id": {
        "schema": {
          "type": "string"
        }
      },
      "git_root_path": {
        "schema": {
          "type": "string"
        }
      },
      "name": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "UpdateGroupRequest": {
    "type": "object",
    "properties": {
      "description": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "UpdateMacroRequest": {
    "type": "object",
    "properties": {
      "body": {
        "schema": {
          "type": "string"
        }
      },
      "catalog_name": {
        "schema": {
          "type": "string"
        }
      },
      "description": {
        "schema": {
          "type": "string"
        }
      },
      "owner": {
        "schema": {
          "type": "string"
        }
      },
      "parameters": {
        "schema": {
          "type": "array",
          "items": {
            "type": "string"
          }
        }
      },
      "project_name": {
        "schema": {
          "type": "string"
        }
      },
      "properties": {
        "schema": {
          "ref": "Record"
        }
      },
      "status": {
        "schema": {
          "ref": "MacroStatus"
        }
      },
      "tags": {
        "schema": {
          "type": "array",
          "items": {
            "type": "string"
          }
        }
      },
      "visibility": {
        "schema": {
          "ref": "MacroVisibility"
        }
      }
    }
  },
  "UpdateModelRequest": {
    "type": "object",
    "properties": {
      "config": {
        "schema": {
          "ref": "ModelConfig"
        }
      },
      "contract": {
        "schema": {
          "ref": "ModelContract"
        }
      },
      "description": {
        "schema": {
          "type": "string"
        }
      },
      "freshness_policy": {
        "schema": {
          "ref": "FreshnessPolicy"
        }
      },
      "materialization": {
        "schema": {
          "ref": "ModelMaterialization"
        }
      },
      "sql": {
        "schema": {
          "type": "string"
        }
      },
      "tags": {
        "schema": {
          "type": "array",
          "items": {
            "type": "string"
          }
        }
      }
    }
  },
  "UpdateNotebookRequest": {
    "type": "object",
    "properties": {
      "description": {
        "schema": {
          "type": "string"
        }
      },
      "environment_override_id": {
        "schema": {
          "type": "string"
        }
      },
      "name": {
        "schema": {
          "type": "string"
        }
      },
      "project_override_id": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "UpdatePipelineJobRequest": {
    "type": "object",
    "properties": {
      "compute_endpoint_id": {
        "schema": {
          "type": "string"
        }
      },
      "depends_on": {
        "schema": {
          "type": "array",
          "items": {
            "type": "string"
          }
        }
      },
      "job_order": {
        "schema": {
          "type": "integer",
          "format": "int32"
        }
      },
      "job_type": {
        "schema": {
          "ref": "PipelineJobJobType"
        }
      },
      "model_selector": {
        "schema": {
          "type": "string"
        }
      },
      "name": {
        "schema": {
          "type": "string"
        }
      },
      "notebook_id": {
        "schema": {
          "type": "string"
        }
      },
      "retry_count": {
        "schema": {
          "type": "integer",
          "format": "int32"
        }
      },
      "timeout_seconds": {
        "schema": {
          "type": "integer",
          "format": "int64"
        }
      }
    }
  },
  "UpdatePipelineRequest": {
    "type": "object",
    "properties": {
      "concurrency_limit": {
        "schema": {
          "type": "integer",
          "format": "int32"
        }
      },
      "description": {
        "schema": {
          "type": "string"
        }
      },
      "folder_id": {
        "schema": {
          "type": "string"
        }
      },
      "is_paused": {
        "schema": {
          "type": "boolean"
        }
      },
      "schedule_cron": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "UpdatePrincipalRequest": {
    "type": "object",
    "properties": {
      "is_admin": {
        "schema": {
          "type": "boolean"
        }
      }
    }
  },
  "UpdateRowFilterRequest": {
    "type": "object",
    "properties": {
      "description": {
        "schema": {
          "type": "string"
        }
      },
      "filter_sql": {
        "schema": {
          "type": "string"
        }
      },
      "name": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "UpdateSchemaRequest": {
    "type": "object",
    "properties": {
      "comment": {
        "schema": {
          "type": "string"
        }
      },
      "properties": {
        "schema": {
          "ref": "Record"
        }
      }
    }
  },
  "UpdateSemanticMetricRequest": {
    "type": "object",
    "properties": {
      "certification_state": {
        "schema": {
          "ref": "CreateSemanticMetricRequestCertificationState"
        }
      },
      "default_time_grain": {
        "schema": {
          "type": "string"
        }
      },
      "description": {
        "schema": {
          "type": "string"
        }
      },
      "expression": {
        "schema": {
          "type": "string"
        }
      },
      "expression_mode": {
        "schema": {
          "ref": "SemanticMetricExpressionMode"
        }
      },
      "filter_sql": {
        "schema": {
          "type": "string"
        }
      },
      "format": {
        "schema": {
          "type": "string"
        }
      },
      "label": {
        "schema": {
          "type": "string"
        }
      },
      "metric_type": {
        "schema": {
          "ref": "SemanticMetricMetricType"
        }
      },
      "owner": {
        "schema": {
          "type": "string"
        }
      },
      "relationship_names": {
        "schema": {
          "type": "array",
          "items": {
            "type": "string"
          }
        }
      }
    }
  },
  "UpdateSemanticModelRequest": {
    "type": "object",
    "properties": {
      "base_model_ref": {
        "schema": {
          "type": "string"
        }
      },
      "default_time_dimension": {
        "schema": {
          "type": "string"
        }
      },
      "description": {
        "schema": {
          "type": "string"
        }
      },
      "owner": {
        "schema": {
          "type": "string"
        }
      },
      "tags": {
        "schema": {
          "type": "array",
          "items": {
            "type": "string"
          }
        }
      }
    }
  },
  "UpdateSemanticPreAggregationRequest": {
    "type": "object",
    "properties": {
      "dimension_set": {
        "schema": {
          "type": "array",
          "items": {
            "type": "string"
          }
        }
      },
      "grain": {
        "schema": {
          "type": "string"
        }
      },
      "metric_set": {
        "schema": {
          "type": "array",
          "items": {
            "type": "string"
          }
        }
      },
      "refresh_policy": {
        "schema": {
          "type": "string"
        }
      },
      "target_relation": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "UpdateSemanticRelationshipRequest": {
    "type": "object",
    "properties": {
      "cost": {
        "schema": {
          "type": "integer",
          "format": "int32"
        }
      },
      "join_sql": {
        "schema": {
          "type": "string"
        }
      },
      "max_hops": {
        "schema": {
          "type": "integer",
          "format": "int32"
        }
      },
      "relationship_type": {
        "schema": {
          "ref": "SemanticRelationshipRelationshipType"
        }
      }
    }
  },
  "UpdateStorageCredentialRequest": {
    "type": "object",
    "properties": {
      "comment": {
        "schema": {
          "type": "string"
        }
      },
      "endpoint": {
        "schema": {
          "type": "string"
        }
      },
      "key_id": {
        "schema": {
          "type": "string"
        }
      },
      "region": {
        "schema": {
          "type": "string"
        }
      },
      "secret": {
        "schema": {
          "type": "string"
        }
      },
      "url_style": {
        "schema": {
          "ref": "URLStyle"
        }
      }
    }
  },
  "UpdateTableRequest": {
    "type": "object",
    "properties": {
      "comment": {
        "schema": {
          "type": "string"
        }
      },
      "owner": {
        "schema": {
          "type": "string"
        }
      },
      "properties": {
        "schema": {
          "ref": "Record"
        }
      }
    }
  },
  "UpdateTagRequest": {
    "type": "object",
    "properties": {
      "key": {
        "schema": {
          "type": "string"
        }
      },
      "value": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "UpdateViewRequest": {
    "type": "object",
    "properties": {
      "comment": {
        "schema": {
          "type": "string"
        }
      },
      "view_definition": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "UpdateVolumeRequest": {
    "type": "object",
    "properties": {
      "comment": {
        "schema": {
          "type": "string"
        }
      },
      "new_name": {
        "schema": {
          "type": "string"
        }
      },
      "storage_location": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "UpdateWorkspaceRequest": {
    "type": "object",
    "properties": {
      "default_environment_id": {
        "schema": {
          "type": "string"
        }
      },
      "default_project_id": {
        "schema": {
          "type": "string"
        }
      },
      "git_repo_id": {
        "schema": {
          "type": "string"
        }
      },
      "git_root_path": {
        "schema": {
          "type": "string"
        }
      },
      "name": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "VersionedObjectSummary": {
    "type": "object",
    "properties": {
      "active_count": {
        "schema": {
          "type": "integer",
          "format": "int64"
        }
      },
      "has_history": {
        "schema": {
          "type": "boolean"
        }
      },
      "historical_count": {
        "schema": {
          "type": "integer",
          "format": "int64"
        }
      },
      "latest_snapshot_id": {
        "schema": {
          "type": "integer",
          "format": "int64"
        }
      },
      "total_count": {
        "schema": {
          "type": "integer",
          "format": "int64"
        }
      }
    }
  },
  "ViewDetail": {
    "type": "object",
    "properties": {
      "catalog_name": {
        "schema": {
          "type": "string"
        }
      },
      "comment": {
        "schema": {
          "type": "string"
        }
      },
      "created_at": {
        "schema": {
          "type": "string"
        }
      },
      "id": {
        "schema": {
          "type": "string"
        }
      },
      "name": {
        "schema": {
          "type": "string"
        }
      },
      "owner": {
        "schema": {
          "type": "string"
        }
      },
      "schema_id": {
        "schema": {
          "type": "string"
        }
      },
      "schema_name": {
        "schema": {
          "type": "string"
        }
      },
      "source_tables": {
        "schema": {
          "type": "array",
          "items": {
            "type": "string"
          }
        }
      },
      "updated_at": {
        "schema": {
          "type": "string"
        }
      },
      "view_definition": {
        "schema": {
          "type": "string"
        }
      }
    },
    "required": [
      "id",
      "schema_name",
      "catalog_name",
      "name"
    ]
  },
  "VisualChartType": {
    "type": "string",
    "enum": [
      "bar",
      "line",
      "area",
      "pie",
      "doughnut",
      "scatter",
      "stacked_bar"
    ]
  },
  "VisualEncodings": {
    "type": "object",
    "properties": {
      "label": {
        "schema": {
          "ref": "VisualFieldBinding"
        }
      },
      "secondary": {
        "schema": {
          "ref": "VisualFieldBinding"
        }
      },
      "series": {
        "schema": {
          "ref": "VisualFieldBinding"
        }
      },
      "value": {
        "schema": {
          "ref": "VisualFieldBinding"
        }
      },
      "x": {
        "schema": {
          "ref": "VisualFieldBinding"
        }
      },
      "y": {
        "schema": {
          "ref": "VisualFieldBinding"
        }
      }
    }
  },
  "VisualFieldBinding": {
    "type": "object",
    "properties": {
      "field": {
        "schema": {
          "type": "string"
        }
      }
    },
    "required": [
      "field"
    ]
  },
  "VisualOutputKind": {
    "type": "string",
    "enum": [
      "table",
      "metric",
      "chart"
    ]
  },
  "VisualSpec": {
    "type": "object",
    "properties": {
      "chart_type": {
        "schema": {
          "ref": "VisualChartType"
        }
      },
      "color_palette": {
        "schema": {
          "type": "string"
        }
      },
      "encodings": {
        "schema": {
          "ref": "VisualEncodings"
        }
      },
      "kind": {
        "schema": {
          "ref": "VisualOutputKind"
        }
      },
      "legend": {
        "schema": {
          "type": "boolean"
        }
      },
      "stacked": {
        "schema": {
          "type": "boolean"
        }
      },
      "subtitle": {
        "schema": {
          "type": "string"
        }
      },
      "title": {
        "schema": {
          "type": "string"
        }
      }
    },
    "required": [
      "kind"
    ]
  },
  "VolumeDetail": {
    "type": "object",
    "properties": {
      "catalog_name": {
        "schema": {
          "type": "string"
        }
      },
      "comment": {
        "schema": {
          "type": "string"
        }
      },
      "created_at": {
        "schema": {
          "type": "string"
        }
      },
      "id": {
        "schema": {
          "type": "string"
        }
      },
      "name": {
        "schema": {
          "type": "string"
        }
      },
      "owner": {
        "schema": {
          "type": "string"
        }
      },
      "schema_name": {
        "schema": {
          "type": "string"
        }
      },
      "storage_location": {
        "schema": {
          "type": "string"
        }
      },
      "updated_at": {
        "schema": {
          "type": "string"
        }
      },
      "volume_type": {
        "schema": {
          "type": "string"
        }
      }
    },
    "required": [
      "id",
      "name",
      "schema_name",
      "catalog_name"
    ]
  },
  "WebSessionStatsResponse": {
    "type": "object",
    "properties": {
      "absolute_ttl_seconds": {
        "schema": {
          "type": "integer",
          "format": "int64"
        }
      },
      "active_sessions": {
        "schema": {
          "type": "integer",
          "format": "int64"
        }
      },
      "created_total": {
        "schema": {
          "type": "integer",
          "format": "int64"
        }
      },
      "idle_ttl_seconds": {
        "schema": {
          "type": "integer",
          "format": "int64"
        }
      },
      "reaped_total": {
        "schema": {
          "type": "integer",
          "format": "int64"
        }
      },
      "resolve_failed_total": {
        "schema": {
          "type": "integer",
          "format": "int64"
        }
      },
      "resolved_total": {
        "schema": {
          "type": "integer",
          "format": "int64"
        }
      },
      "revoked_all_total": {
        "schema": {
          "type": "integer",
          "format": "int64"
        }
      },
      "revoked_total": {
        "schema": {
          "type": "integer",
          "format": "int64"
        }
      }
    },
    "required": [
      "created_total",
      "resolved_total",
      "resolve_failed_total",
      "revoked_total",
      "revoked_all_total",
      "reaped_total",
      "active_sessions",
      "idle_ttl_seconds",
      "absolute_ttl_seconds"
    ]
  },
  "Workspace": {
    "type": "object",
    "properties": {
      "created_at": {
        "schema": {
          "type": "string"
        }
      },
      "default_environment_id": {
        "schema": {
          "type": "string"
        }
      },
      "default_project_id": {
        "schema": {
          "type": "string"
        }
      },
      "git_repo_id": {
        "schema": {
          "type": "string"
        }
      },
      "git_root_path": {
        "schema": {
          "type": "string"
        }
      },
      "id": {
        "schema": {
          "type": "string"
        }
      },
      "kind": {
        "schema": {
          "ref": "WorkspaceKind"
        }
      },
      "name": {
        "schema": {
          "type": "string"
        }
      },
      "owner_principal": {
        "schema": {
          "type": "string"
        }
      },
      "owner_team_id": {
        "schema": {
          "type": "string"
        }
      },
      "updated_at": {
        "schema": {
          "type": "string"
        }
      }
    },
    "required": [
      "name",
      "kind"
    ]
  },
  "WorkspaceKind": {
    "type": "string",
    "enum": [
      "personal",
      "shared",
      "library"
    ]
  },
  "WorkspaceMember": {
    "type": "object",
    "properties": {
      "created_at": {
        "schema": {
          "type": "string"
        }
      },
      "principal_name": {
        "schema": {
          "type": "string"
        }
      },
      "role": {
        "schema": {
          "ref": "NotebookShareRole"
        }
      },
      "updated_at": {
        "schema": {
          "type": "string"
        }
      },
      "workspace_id": {
        "schema": {
          "type": "string"
        }
      }
    },
    "required": [
      "workspace_id",
      "principal_name",
      "role"
    ]
  }
}

