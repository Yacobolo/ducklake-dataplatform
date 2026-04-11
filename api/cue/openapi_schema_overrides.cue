package api

openapi_schema_overrides: {
  "APIKeyInfo": {
    "title": "API key metadata.",
    "description": "Represents a stored API key without returning the full secret value.",
    "required": [
      "id",
      "principal_id",
      "name"
    ],
    "property_order": [
      "id",
      "principal_id",
      "name",
      "key_prefix",
      "expires_at",
      "created_at"
    ],
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
      "name": {
        "schema": {
          "type": "string"
        }
      },
      "key_prefix": {
        "schema": {
          "type": "string"
        }
      },
      "expires_at": {
        "schema": {
          "type": "string",
          "format": "date-time"
        }
      },
      "created_at": {
        "schema": {
          "type": "string",
          "format": "date-time"
        }
      }
    }
  },
  "AddWorkspaceMemberRequest": {
    "required": [
      "principal_name"
    ],
    "property_order": [
      "principal_name",
      "role"
    ],
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
  "Asset": {
    "required": [],
    "property_order": [
      "id",
      "asset_key",
      "asset_type",
      "owner",
      "description",
      "tags",
      "freshness_policy",
      "materialization_policy",
      "auto_materialize_policy",
      "io_profile",
      "is_active",
      "created_by",
      "created_at",
      "updated_at"
    ],
    "properties": {
      "id": {
        "schema": {
          "type": "string"
        }
      },
      "asset_key": {
        "schema": {
          "type": "string"
        }
      },
      "asset_type": {
        "schema": {
          "ref": "AssetType"
        }
      },
      "owner": {
        "schema": {
          "type": "string"
        }
      },
      "description": {
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
      "freshness_policy": {
        "schema": {
          "ref": "AssetFreshnessPolicy"
        }
      },
      "materialization_policy": {
        "schema": {
          "ref": "AssetMaterializationPolicy"
        }
      },
      "auto_materialize_policy": {
        "schema": {
          "ref": "AssetAutoMaterializePolicy"
        }
      },
      "io_profile": {
        "schema": {
          "type": "string"
        }
      },
      "is_active": {
        "schema": {
          "type": "boolean"
        }
      },
      "created_by": {
        "schema": {
          "type": "string"
        }
      },
      "created_at": {
        "schema": {
          "type": "string",
          "format": "date-time"
        }
      },
      "updated_at": {
        "schema": {
          "type": "string",
          "format": "date-time"
        }
      }
    }
  },
  "AssetAutoMaterializePolicy": {
    "required": [],
    "property_order": [
      "mode",
      "min_interval_seconds",
      "require_all_upstreams",
      "on_freshness_breach",
      "on_upstream_materialized",
      "respect_downtime_windows",
      "downtime_windows_cron_expr"
    ],
    "properties": {
      "mode": {
        "schema": {
          "type": "string"
        }
      },
      "min_interval_seconds": {
        "schema": {
          "type": "integer",
          "format": "int64"
        }
      },
      "require_all_upstreams": {
        "schema": {
          "type": "boolean"
        }
      },
      "on_freshness_breach": {
        "schema": {
          "type": "boolean"
        }
      },
      "on_upstream_materialized": {
        "schema": {
          "type": "boolean"
        }
      },
      "respect_downtime_windows": {
        "schema": {
          "type": "boolean"
        }
      },
      "downtime_windows_cron_expr": {
        "schema": {
          "type": "array",
          "items": {
            "type": "string"
          }
        }
      }
    }
  },
  "AssetBackfillDetails": {
    "required": [],
    "property_order": [
      "request",
      "slices"
    ],
    "properties": {
      "request": {
        "schema": {
          "ref": "BackfillRequest"
        }
      },
      "slices": {
        "schema": {
          "type": "array",
          "items": {
            "ref": "BackfillSlice"
          }
        }
      }
    }
  },
  "AssetCheck": {
    "required": [],
    "property_order": [
      "id",
      "asset_id",
      "name",
      "check_type",
      "severity",
      "enabled",
      "created_at",
      "updated_at"
    ],
    "properties": {
      "id": {
        "schema": {
          "type": "string"
        }
      },
      "asset_id": {
        "schema": {
          "type": "string"
        }
      },
      "name": {
        "schema": {
          "type": "string"
        }
      },
      "check_type": {
        "schema": {
          "type": "string"
        }
      },
      "severity": {
        "schema": {
          "ref": "AssetCheckSeverity"
        }
      },
      "enabled": {
        "schema": {
          "type": "boolean"
        }
      },
      "created_at": {
        "schema": {
          "type": "string",
          "format": "date-time"
        }
      },
      "updated_at": {
        "schema": {
          "type": "string",
          "format": "date-time"
        }
      }
    }
  },
  "AssetCheckInput": {
    "required": [
      "name",
      "check_type"
    ],
    "property_order": [
      "name",
      "check_type",
      "severity",
      "enabled",
      "config_json"
    ],
    "properties": {
      "name": {
        "schema": {
          "type": "string"
        }
      },
      "check_type": {
        "schema": {
          "type": "string"
        }
      },
      "severity": {
        "schema": {
          "ref": "AssetCheckSeverity"
        }
      },
      "enabled": {
        "schema": {
          "type": "boolean"
        }
      },
      "config_json": {
        "schema": {
          "type": "object",
          "additional_properties": {
            "any": true
          }
        }
      }
    }
  },
  "AssetCheckList": {
    "required": [
      "data"
    ],
    "property_order": [
      "data"
    ],
    "properties": {
      "data": {
        "schema": {
          "type": "array",
          "items": {
            "ref": "AssetCheck"
          }
        }
      }
    }
  },
  "AssetCheckResult": {
    "required": [],
    "property_order": [
      "id",
      "check_id",
      "run_id",
      "partition_key",
      "status",
      "message",
      "metrics_json",
      "created_at"
    ],
    "properties": {
      "id": {
        "schema": {
          "type": "string"
        }
      },
      "check_id": {
        "schema": {
          "type": "string"
        }
      },
      "run_id": {
        "schema": {
          "type": "string"
        }
      },
      "partition_key": {
        "schema": {
          "type": "string"
        }
      },
      "status": {
        "schema": {
          "type": "string"
        }
      },
      "message": {
        "schema": {
          "type": "string"
        }
      },
      "metrics_json": {
        "schema": {
          "type": "object",
          "additional_properties": {
            "any": true
          }
        }
      },
      "created_at": {
        "schema": {
          "type": "string",
          "format": "date-time"
        }
      }
    }
  },
  "AssetCheckSeverity": {
    "required": []
  },
  "AssetFreshnessBlocker": {
    "required": [],
    "property_order": [
      "asset",
      "dependency_type"
    ],
    "properties": {
      "asset": {
        "schema": {
          "ref": "AssetFreshnessStatus"
        }
      },
      "dependency_type": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "AssetFreshnessBlockersResponse": {
    "required": [],
    "property_order": [
      "asset",
      "blockers"
    ],
    "properties": {
      "asset": {
        "schema": {
          "ref": "AssetFreshnessStatus"
        }
      },
      "blockers": {
        "schema": {
          "type": "array",
          "items": {
            "ref": "AssetFreshnessBlocker"
          }
        }
      }
    }
  },
  "AssetFreshnessEdge": {
    "required": [],
    "property_order": [
      "from_asset_key",
      "to_asset_key",
      "dependency_type"
    ],
    "properties": {
      "from_asset_key": {
        "schema": {
          "type": "string"
        }
      },
      "to_asset_key": {
        "schema": {
          "type": "string"
        }
      },
      "dependency_type": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "AssetFreshnessExplanation": {
    "required": [],
    "property_order": [
      "asset",
      "nodes",
      "edges"
    ],
    "properties": {
      "asset": {
        "schema": {
          "ref": "AssetFreshnessStatus"
        }
      },
      "nodes": {
        "schema": {
          "type": "array",
          "items": {
            "ref": "AssetFreshnessStatus"
          }
        }
      },
      "edges": {
        "schema": {
          "type": "array",
          "items": {
            "ref": "AssetFreshnessEdge"
          }
        }
      }
    }
  },
  "AssetFreshnessPolicy": {
    "required": [],
    "property_order": [
      "max_lag_seconds",
      "cron_schedule"
    ],
    "properties": {
      "max_lag_seconds": {
        "schema": {
          "type": "integer",
          "format": "int64"
        }
      },
      "cron_schedule": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "AssetFreshnessReconcileResponse": {
    "required": [],
    "property_order": [
      "asset",
      "targets"
    ],
    "properties": {
      "asset": {
        "schema": {
          "ref": "AssetFreshnessStatus"
        }
      },
      "targets": {
        "schema": {
          "type": "array",
          "items": {
            "ref": "AssetFreshnessReconcileTarget"
          }
        }
      }
    }
  },
  "AssetFreshnessReconcileTarget": {
    "required": [],
    "property_order": [
      "asset_id",
      "asset_key",
      "asset_type",
      "freshness_status",
      "event_id"
    ],
    "properties": {
      "asset_id": {
        "schema": {
          "type": "string"
        }
      },
      "asset_key": {
        "schema": {
          "type": "string"
        }
      },
      "asset_type": {
        "schema": {
          "ref": "AssetType"
        }
      },
      "freshness_status": {
        "schema": {
          "type": "string"
        }
      },
      "event_id": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "AssetFreshnessRequirement": {
    "required": [],
    "property_order": [
      "asset",
      "dependency_type"
    ],
    "properties": {
      "asset": {
        "schema": {
          "ref": "AssetFreshnessStatus"
        }
      },
      "dependency_type": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "AssetFreshnessRequirementsResponse": {
    "required": [],
    "property_order": [
      "asset",
      "requirements"
    ],
    "properties": {
      "asset": {
        "schema": {
          "ref": "AssetFreshnessStatus"
        }
      },
      "requirements": {
        "schema": {
          "type": "array",
          "items": {
            "ref": "AssetFreshnessRequirement"
          }
        }
      }
    }
  },
  "AssetFreshnessStatus": {
    "required": [],
    "property_order": [
      "asset_id",
      "asset_key",
      "asset_type",
      "freshness_status",
      "effective_max_lag_seconds",
      "last_materialized_at",
      "stale_since",
      "reason",
      "basis"
    ],
    "properties": {
      "asset_id": {
        "schema": {
          "type": "string"
        }
      },
      "asset_key": {
        "schema": {
          "type": "string"
        }
      },
      "asset_type": {
        "schema": {
          "ref": "AssetType"
        }
      },
      "freshness_status": {
        "schema": {
          "type": "string"
        }
      },
      "effective_max_lag_seconds": {
        "schema": {
          "type": "integer",
          "format": "int64"
        }
      },
      "last_materialized_at": {
        "schema": {
          "type": "string",
          "format": "date-time"
        }
      },
      "stale_since": {
        "schema": {
          "type": "string",
          "format": "date-time"
        }
      },
      "reason": {
        "schema": {
          "type": "string"
        }
      },
      "basis": {
        "schema": {
          "type": "array",
          "items": {
            "type": "string"
          }
        }
      }
    }
  },
  "AssetGraph": {
    "required": [],
    "property_order": [
      "asset_key",
      "upstream_asset_keys",
      "downstream_asset_keys"
    ],
    "properties": {
      "asset_key": {
        "schema": {
          "type": "string"
        }
      },
      "upstream_asset_keys": {
        "schema": {
          "type": "array",
          "items": {
            "type": "string"
          }
        }
      },
      "downstream_asset_keys": {
        "schema": {
          "type": "array",
          "items": {
            "type": "string"
          }
        }
      }
    }
  },
  "AssetMaterialization": {
    "required": [],
    "property_order": [
      "id",
      "asset_id",
      "run_id",
      "partition_key",
      "row_count",
      "schema_hash",
      "materialized_at",
      "created_at"
    ],
    "properties": {
      "id": {
        "schema": {
          "type": "string"
        }
      },
      "asset_id": {
        "schema": {
          "type": "string"
        }
      },
      "run_id": {
        "schema": {
          "type": "string"
        }
      },
      "partition_key": {
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
      "schema_hash": {
        "schema": {
          "type": "string"
        }
      },
      "materialized_at": {
        "schema": {
          "type": "string",
          "format": "date-time"
        }
      },
      "created_at": {
        "schema": {
          "type": "string",
          "format": "date-time"
        }
      }
    }
  },
  "AssetMaterializationPolicy": {
    "required": [],
    "property_order": [
      "mode",
      "allow_concurrent"
    ],
    "properties": {
      "mode": {
        "schema": {
          "type": "string"
        }
      },
      "allow_concurrent": {
        "schema": {
          "type": "boolean"
        }
      }
    }
  },
  "AssetPartition": {
    "required": [],
    "property_order": [
      "id",
      "asset_id",
      "partition_key",
      "status",
      "created_at",
      "updated_at"
    ],
    "properties": {
      "id": {
        "schema": {
          "type": "string"
        }
      },
      "asset_id": {
        "schema": {
          "type": "string"
        }
      },
      "partition_key": {
        "schema": {
          "type": "string"
        }
      },
      "status": {
        "schema": {
          "type": "string"
        }
      },
      "created_at": {
        "schema": {
          "type": "string",
          "format": "date-time"
        }
      },
      "updated_at": {
        "schema": {
          "type": "string",
          "format": "date-time"
        }
      }
    }
  },
  "AssetRun": {
    "required": [],
    "property_order": [
      "id",
      "asset_id",
      "run_group_id",
      "partition_key",
      "partition_from",
      "partition_to",
      "status",
      "trigger_type",
      "triggered_by",
      "attempt_count",
      "max_attempts",
      "started_at",
      "finished_at",
      "error_message",
      "created_at",
      "updated_at"
    ],
    "properties": {
      "id": {
        "schema": {
          "type": "string"
        }
      },
      "asset_id": {
        "schema": {
          "type": "string"
        }
      },
      "run_group_id": {
        "schema": {
          "type": "string"
        }
      },
      "partition_key": {
        "schema": {
          "type": "string"
        }
      },
      "partition_from": {
        "schema": {
          "type": "string"
        }
      },
      "partition_to": {
        "schema": {
          "type": "string"
        }
      },
      "status": {
        "schema": {
          "ref": "AssetRunStatus"
        }
      },
      "trigger_type": {
        "schema": {
          "ref": "AssetTriggerType"
        }
      },
      "triggered_by": {
        "schema": {
          "type": "string"
        }
      },
      "attempt_count": {
        "schema": {
          "type": "integer",
          "format": "int32"
        }
      },
      "max_attempts": {
        "schema": {
          "type": "integer",
          "format": "int32"
        }
      },
      "started_at": {
        "schema": {
          "type": "string",
          "format": "date-time"
        }
      },
      "finished_at": {
        "schema": {
          "type": "string",
          "format": "date-time"
        }
      },
      "error_message": {
        "schema": {
          "type": "string"
        }
      },
      "created_at": {
        "schema": {
          "type": "string",
          "format": "date-time"
        }
      },
      "updated_at": {
        "schema": {
          "type": "string",
          "format": "date-time"
        }
      }
    }
  },
  "AssetRunStatus": {
    "required": []
  },
  "AssetTriggerResponse": {
    "required": [],
    "property_order": [
      "event_id",
      "status"
    ],
    "properties": {
      "event_id": {
        "schema": {
          "type": "string"
        }
      },
      "status": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "AssetTriggerType": {
    "required": []
  },
  "AssetType": {
    "required": []
  },
  "AuditDecisionStatus": {
    "required": []
  },
  "AuditEntry": {
    "required": [
      "id"
    ],
    "property_order": [
      "id",
      "principal_name",
      "action",
      "statement_type",
      "original_sql",
      "rewritten_sql",
      "tables_accessed",
      "status",
      "error_message",
      "duration_ms",
      "created_at"
    ],
    "properties": {
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
      "action": {
        "schema": {
          "type": "string"
        }
      },
      "statement_type": {
        "schema": {
          "type": "string"
        }
      },
      "original_sql": {
        "schema": {
          "type": "string"
        }
      },
      "rewritten_sql": {
        "schema": {
          "type": "string"
        }
      },
      "tables_accessed": {
        "schema": {
          "type": "array",
          "items": {
            "type": "string"
          }
        }
      },
      "status": {
        "schema": {
          "ref": "AuditDecisionStatus"
        }
      },
      "error_message": {
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
      "created_at": {
        "schema": {
          "type": "string",
          "format": "date-time"
        }
      }
    }
  },
  "AuthLoginResponse": {
    "required": [
      "token",
      "principal"
    ],
    "property_order": [
      "token",
      "principal"
    ],
    "properties": {
      "token": {
        "schema": {
          "type": "string"
        }
      },
      "principal": {
        "schema": {
          "ref": "AuthPrincipalSummary"
        }
      }
    }
  },
  "AuthPrincipalSummary": {
    "required": [
      "id",
      "name",
      "is_admin"
    ],
    "property_order": [
      "id",
      "name",
      "is_admin"
    ],
    "properties": {
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
      "is_admin": {
        "schema": {
          "type": "boolean"
        }
      }
    }
  },
  "BackfillRequest": {
    "required": [],
    "property_order": [
      "id",
      "asset_id",
      "partition_from",
      "partition_to",
      "status",
      "requested_by",
      "max_parallelism",
      "created_at",
      "started_at",
      "finished_at",
      "error_message"
    ],
    "properties": {
      "id": {
        "schema": {
          "type": "string"
        }
      },
      "asset_id": {
        "schema": {
          "type": "string"
        }
      },
      "partition_from": {
        "schema": {
          "type": "string"
        }
      },
      "partition_to": {
        "schema": {
          "type": "string"
        }
      },
      "status": {
        "schema": {
          "type": "string"
        }
      },
      "requested_by": {
        "schema": {
          "type": "string"
        }
      },
      "max_parallelism": {
        "schema": {
          "type": "integer",
          "format": "int32"
        }
      },
      "created_at": {
        "schema": {
          "type": "string",
          "format": "date-time"
        }
      },
      "started_at": {
        "schema": {
          "type": "string",
          "format": "date-time"
        }
      },
      "finished_at": {
        "schema": {
          "type": "string",
          "format": "date-time"
        }
      },
      "error_message": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "BackfillSlice": {
    "required": [],
    "property_order": [
      "id",
      "request_id",
      "asset_id",
      "partition_key",
      "status",
      "run_id",
      "created_at",
      "started_at",
      "finished_at",
      "error_message",
      "attempt_count",
      "max_attempts"
    ],
    "properties": {
      "id": {
        "schema": {
          "type": "string"
        }
      },
      "request_id": {
        "schema": {
          "type": "string"
        }
      },
      "asset_id": {
        "schema": {
          "type": "string"
        }
      },
      "partition_key": {
        "schema": {
          "type": "string"
        }
      },
      "status": {
        "schema": {
          "type": "string"
        }
      },
      "run_id": {
        "schema": {
          "type": "string"
        }
      },
      "created_at": {
        "schema": {
          "type": "string",
          "format": "date-time"
        }
      },
      "started_at": {
        "schema": {
          "type": "string",
          "format": "date-time"
        }
      },
      "finished_at": {
        "schema": {
          "type": "string",
          "format": "date-time"
        }
      },
      "error_message": {
        "schema": {
          "type": "string"
        }
      },
      "attempt_count": {
        "schema": {
          "type": "integer",
          "format": "int32"
        }
      },
      "max_attempts": {
        "schema": {
          "type": "integer",
          "format": "int32"
        }
      }
    }
  },
  "BootstrapCompleteRequest": {
    "required": [
      "username",
      "password"
    ],
    "property_order": [
      "username",
      "password",
      "principal_name",
      "bootstrap_token"
    ],
    "properties": {
      "username": {
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
      "bootstrap_token": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "BootstrapTokenRequest": {
    "required": [],
    "property_order": [
      "ttl_seconds"
    ],
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
    "required": [
      "bootstrap_token",
      "ttl_seconds"
    ],
    "property_order": [
      "bootstrap_token",
      "ttl_seconds"
    ],
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
    }
  },
  "Build": {
    "required": [
      "git_ref",
      "target_catalog",
      "target_schema",
      "compile_manifest"
    ],
    "property_order": [
      "id",
      "project_id",
      "project_name",
      "product_id",
      "environment_id",
      "environment_name",
      "state",
      "git_ref",
      "commit_sha",
      "selector",
      "target_catalog",
      "target_schema",
      "source_model_run_id",
      "compile_manifest",
      "compile_diagnostics",
      "created_at"
    ],
    "properties": {
      "id": {
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
      "product_id": {
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
      "state": {
        "schema": {
          "ref": "BuildState"
        }
      },
      "git_ref": {
        "schema": {
          "type": "string"
        }
      },
      "commit_sha": {
        "schema": {
          "type": "string"
        }
      },
      "selector": {
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
      },
      "source_model_run_id": {
        "schema": {
          "type": "string"
        }
      },
      "compile_manifest": {
        "schema": {
          "type": "string"
        }
      },
      "compile_diagnostics": {
        "schema": {
          "type": "string"
        }
      },
      "created_at": {
        "schema": {
          "type": "string",
          "format": "date-time"
        }
      }
    }
  },
  "BuildState": {
    "required": []
  },
  "CancelQueryResponse": {
    "required": [
      "query_id",
      "status"
    ],
    "property_order": [
      "query_id",
      "status"
    ],
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
    }
  },
  "CatalogHistoryEntry": {
    "required": [],
    "property_order": [
      "entity_type",
      "schema_name",
      "table_name",
      "column_name",
      "object_name",
      "object_id",
      "begin_snapshot_id",
      "end_snapshot_id",
      "latest_snapshot_id",
      "is_active",
      "has_history"
    ],
    "properties": {
      "entity_type": {
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
      "column_name": {
        "schema": {
          "type": "string"
        }
      },
      "object_name": {
        "schema": {
          "type": "string"
        }
      },
      "object_id": {
        "schema": {
          "type": "string"
        }
      },
      "begin_snapshot_id": {
        "schema": {
          "type": "integer",
          "format": "int64"
        }
      },
      "end_snapshot_id": {
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
      "is_active": {
        "schema": {
          "type": "boolean"
        }
      },
      "has_history": {
        "schema": {
          "type": "boolean"
        }
      }
    }
  },
  "CatalogHistoryResponse": {
    "required": [
      "data"
    ],
    "property_order": [
      "data"
    ],
    "properties": {
      "data": {
        "schema": {
          "type": "array",
          "items": {
            "ref": "CatalogHistoryEntry"
          }
        }
      }
    }
  },
  "CatalogInfo": {
    "required": [
      "name"
    ],
    "property_order": [
      "name",
      "comment",
      "created_at",
      "updated_at",
      "system_managed"
    ],
    "properties": {
      "name": {
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
          "type": "string",
          "format": "date-time"
        }
      },
      "updated_at": {
        "schema": {
          "type": "string",
          "format": "date-time"
        }
      },
      "system_managed": {
        "schema": {
          "type": "boolean"
        }
      }
    }
  },
  "CatalogRegistration": {
    "required": [
      "id",
      "name"
    ],
    "property_order": [
      "id",
      "name",
      "metastore_type",
      "dsn",
      "data_path",
      "status",
      "is_default",
      "comment",
      "created_at",
      "updated_at",
      "system_managed"
    ],
    "properties": {
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
      "metastore_type": {
        "schema": {
          "ref": "MetastoreType"
        }
      },
      "dsn": {
        "schema": {
          "type": "string"
        }
      },
      "data_path": {
        "schema": {
          "type": "string"
        }
      },
      "status": {
        "schema": {
          "ref": "CatalogStatus"
        }
      },
      "is_default": {
        "schema": {
          "type": "boolean"
        }
      },
      "comment": {
        "schema": {
          "type": "string"
        }
      },
      "created_at": {
        "schema": {
          "type": "string",
          "format": "date-time"
        }
      },
      "updated_at": {
        "schema": {
          "type": "string",
          "format": "date-time"
        }
      },
      "system_managed": {
        "schema": {
          "type": "boolean"
        }
      }
    }
  },
  "CatalogRegistrationList": {
    "required": [
      "catalogs"
    ],
    "property_order": [
      "catalogs",
      "next_page_token",
      "total_count"
    ],
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
    }
  },
  "CatalogStatus": {
    "required": []
  },
  "CatalogVersionSummary": {
    "required": [],
    "property_order": [
      "catalog_name",
      "version",
      "created_by",
      "encrypted",
      "data_path",
      "latest_snapshot_id",
      "schemas",
      "tables",
      "columns"
    ],
    "properties": {
      "catalog_name": {
        "schema": {
          "type": "string"
        }
      },
      "version": {
        "schema": {
          "type": "string"
        }
      },
      "created_by": {
        "schema": {
          "type": "string"
        }
      },
      "encrypted": {
        "schema": {
          "type": "boolean"
        }
      },
      "data_path": {
        "schema": {
          "type": "string"
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
      "columns": {
        "schema": {
          "ref": "VersionedObjectSummary"
        }
      }
    }
  },
  "Cell": {
    "required": [],
    "property_order": [
      "id",
      "notebook_id",
      "cell_type",
      "name",
      "role",
      "disabled",
      "test",
      "visual_spec",
      "content",
      "position",
      "last_result",
      "created_at",
      "updated_at"
    ],
    "properties": {
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
      "cell_type": {
        "schema": {
          "ref": "CellCellType"
        }
      },
      "name": {
        "schema": {
          "type": "string"
        }
      },
      "role": {
        "schema": {
          "ref": "CellRole"
        }
      },
      "disabled": {
        "schema": {
          "type": "boolean"
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
      },
      "content": {
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
      "last_result": {
        "schema": {
          "type": "string"
        }
      },
      "created_at": {
        "schema": {
          "type": "string",
          "format": "date-time"
        }
      },
      "updated_at": {
        "schema": {
          "type": "string",
          "format": "date-time"
        }
      }
    }
  },
  "CellCellType": {
    "required": []
  },
  "CellExecutionResult": {
    "required": [],
    "property_order": [
      "cell_id",
      "columns",
      "rows",
      "row_count",
      "error",
      "duration_ms"
    ],
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
      "rows": {
        "schema": {
          "type": "array",
          "items": {
            "type": "object",
            "additional_properties": {
              "any": true
            }
          }
        }
      },
      "row_count": {
        "schema": {
          "type": "integer",
          "format": "int32"
        }
      },
      "error": {
        "schema": {
          "type": "string"
        }
      },
      "duration_ms": {
        "schema": {
          "type": "integer",
          "format": "int64"
        }
      }
    }
  },
  "CellList": {
    "required": [
      "data"
    ],
    "property_order": [
      "data"
    ],
    "properties": {
      "data": {
        "schema": {
          "type": "array",
          "items": {
            "ref": "Cell"
          }
        }
      }
    }
  },
  "CellRole": {
    "required": []
  },
  "CleanupAPIKeysResponse": {
    "required": [
      "deleted_count"
    ],
    "property_order": [
      "deleted_count"
    ],
    "properties": {
      "deleted_count": {
        "schema": {
          "type": "integer",
          "format": "int32"
        }
      }
    }
  },
  "ColumnDetail": {
    "required": [
      "name",
      "type"
    ],
    "property_order": [
      "name",
      "type",
      "position",
      "nullable",
      "comment"
    ],
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
      },
      "position": {
        "schema": {
          "type": "integer",
          "format": "int32"
        }
      },
      "nullable": {
        "schema": {
          "type": "boolean"
        }
      },
      "comment": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "ColumnLineageEdge": {
    "required": [],
    "property_order": [
      "id",
      "lineage_edge_id",
      "target_column",
      "source_schema",
      "source_table",
      "source_column",
      "transform_type",
      "function"
    ],
    "properties": {
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
      "target_column": {
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
      "source_column": {
        "schema": {
          "type": "string"
        }
      },
      "transform_type": {
        "schema": {
          "ref": "ColumnLineageEdgeTransformType"
        }
      },
      "function": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "ColumnLineageEdgeTransformType": {
    "required": []
  },
  "ColumnMask": {
    "required": [
      "id",
      "table_id",
      "name",
      "column_name",
      "mask_expression"
    ],
    "property_order": [
      "id",
      "table_id",
      "name",
      "column_name",
      "mask_expression",
      "description",
      "created_at"
    ],
    "properties": {
      "id": {
        "schema": {
          "type": "string"
        }
      },
      "table_id": {
        "schema": {
          "type": "string"
        }
      },
      "name": {
        "schema": {
          "type": "string"
        }
      },
      "column_name": {
        "schema": {
          "type": "string"
        }
      },
      "mask_expression": {
        "schema": {
          "type": "string"
        }
      },
      "description": {
        "schema": {
          "type": "string"
        }
      },
      "created_at": {
        "schema": {
          "type": "string",
          "format": "date-time"
        }
      }
    }
  },
  "ColumnMaskBinding": {
    "required": [],
    "property_order": [
      "id",
      "column_mask_id",
      "principal_id",
      "principal_type",
      "see_original"
    ],
    "properties": {
      "id": {
        "schema": {
          "type": "string"
        }
      },
      "column_mask_id": {
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
    "required": [
      "principal_id",
      "principal_type"
    ],
    "property_order": [
      "principal_id",
      "principal_type",
      "see_original"
    ],
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
    }
  },
  "CommitIngestionRequest": {
    "required": [
      "s3_keys"
    ],
    "property_order": [
      "s3_keys",
      "options"
    ],
    "properties": {
      "s3_keys": {
        "schema": {
          "type": "array",
          "items": {
            "type": "string"
          }
        }
      },
      "options": {
        "schema": {
          "ref": "IngestionOptions"
        }
      }
    }
  },
  "ComputeAssignment": {
    "required": [],
    "property_order": [
      "id",
      "endpoint_id",
      "endpoint_name",
      "principal_id",
      "principal_type",
      "fallback_local",
      "is_default",
      "created_at"
    ],
    "properties": {
      "id": {
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
      "principal_id": {
        "schema": {
          "type": "string"
        }
      },
      "principal_type": {
        "schema": {
          "ref": "ComputeAssignmentPrincipalType"
        }
      },
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
      "created_at": {
        "schema": {
          "type": "string",
          "format": "date-time"
        }
      }
    }
  },
  "ComputeAssignmentPrincipalType": {
    "required": []
  },
  "ComputeEndpoint": {
    "required": [],
    "property_order": [
      "id",
      "name",
      "type",
      "size",
      "status",
      "url",
      "external_id",
      "max_memory_gb",
      "owner",
      "created_at",
      "updated_at"
    ],
    "properties": {
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
      "type": {
        "schema": {
          "ref": "ComputeEndpointType"
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
      },
      "external_id": {
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
      "owner": {
        "schema": {
          "type": "string"
        }
      },
      "created_at": {
        "schema": {
          "type": "string",
          "format": "date-time"
        }
      },
      "updated_at": {
        "schema": {
          "type": "string",
          "format": "date-time"
        }
      }
    }
  },
  "ComputeEndpointHealth": {
    "required": [],
    "property_order": [
      "endpoint_name",
      "status",
      "memory_used_mb",
      "max_memory_gb",
      "uptime_seconds",
      "duckdb_version"
    ],
    "properties": {
      "endpoint_name": {
        "schema": {
          "type": "string"
        }
      },
      "status": {
        "schema": {
          "type": "string"
        }
      },
      "memory_used_mb": {
        "schema": {
          "type": "integer",
          "format": "int32"
        }
      },
      "max_memory_gb": {
        "schema": {
          "type": "integer",
          "format": "int32"
        }
      },
      "uptime_seconds": {
        "schema": {
          "type": "integer",
          "format": "int32"
        }
      },
      "duckdb_version": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "ComputeEndpointSize": {
    "required": []
  },
  "ComputeEndpointStatus": {
    "required": []
  },
  "ComputeEndpointType": {
    "required": []
  },
  "ComputeRoutingDefaults": {
    "required": [],
    "property_order": [
      "interactive_mode",
      "scheduled_mode",
      "notebook_mode"
    ],
    "properties": {
      "interactive_mode": {
        "schema": {
          "type": "string"
        }
      },
      "scheduled_mode": {
        "schema": {
          "type": "string"
        }
      },
      "notebook_mode": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "CreateAPIKeyRequest": {
    "required": [
      "principal_id"
    ],
    "property_order": [
      "principal_id",
      "name",
      "expires_at"
    ],
    "properties": {
      "principal_id": {
        "schema": {
          "type": "string"
        }
      },
      "name": {
        "schema": {
          "type": "string"
        }
      },
      "expires_at": {
        "schema": {
          "type": "string",
          "format": "date-time"
        }
      }
    }
  },
  "CreateAPIKeyResponse": {
    "required": [
      "id",
      "key"
    ],
    "property_order": [
      "id",
      "key",
      "name",
      "key_prefix",
      "expires_at",
      "created_at"
    ],
    "properties": {
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
      "name": {
        "schema": {
          "type": "string"
        }
      },
      "key_prefix": {
        "schema": {
          "type": "string"
        }
      },
      "expires_at": {
        "schema": {
          "type": "string",
          "format": "date-time"
        }
      },
      "created_at": {
        "schema": {
          "type": "string",
          "format": "date-time"
        }
      }
    }
  },
  "CreateAssetBackfillRequest": {
    "required": [
      "partition_from",
      "partition_to"
    ],
    "property_order": [
      "partition_from",
      "partition_to",
      "max_parallelism"
    ],
    "properties": {
      "partition_from": {
        "schema": {
          "type": "string"
        }
      },
      "partition_to": {
        "schema": {
          "type": "string"
        }
      },
      "max_parallelism": {
        "schema": {
          "type": "integer",
          "format": "int32"
        }
      }
    }
  },
  "CreateAssetBackfillResponse": {
    "required": [],
    "property_order": [
      "request",
      "slices"
    ],
    "properties": {
      "request": {
        "schema": {
          "ref": "BackfillRequest"
        }
      },
      "slices": {
        "schema": {
          "type": "array",
          "items": {
            "ref": "BackfillSlice"
          }
        }
      }
    }
  },
  "CreateAssetRequest": {
    "required": [
      "asset_key",
      "asset_type",
      "product_slug",
      "owner"
    ],
    "property_order": [
      "asset_key",
      "asset_type",
      "product_slug",
      "owner",
      "description",
      "tags",
      "freshness_policy",
      "materialization_policy",
      "auto_materialize_policy",
      "io_profile",
      "is_active",
      "upstream_asset_keys",
      "checks"
    ],
    "properties": {
      "asset_key": {
        "schema": {
          "type": "string"
        }
      },
      "asset_type": {
        "schema": {
          "ref": "AssetType"
        }
      },
      "product_slug": {
        "schema": {
          "type": "string"
        }
      },
      "owner": {
        "schema": {
          "type": "string"
        }
      },
      "description": {
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
      "freshness_policy": {
        "schema": {
          "ref": "AssetFreshnessPolicy"
        }
      },
      "materialization_policy": {
        "schema": {
          "ref": "AssetMaterializationPolicy"
        }
      },
      "auto_materialize_policy": {
        "schema": {
          "ref": "AssetAutoMaterializePolicy"
        }
      },
      "io_profile": {
        "schema": {
          "type": "string"
        }
      },
      "is_active": {
        "schema": {
          "type": "boolean"
        }
      },
      "upstream_asset_keys": {
        "schema": {
          "type": "array",
          "items": {
            "type": "string"
          }
        }
      },
      "checks": {
        "schema": {
          "type": "array",
          "items": {
            "ref": "AssetCheckInput"
          }
        }
      }
    }
  },
  "CreateBuildRequest": {
    "required": [
      "environment_name",
      "git_ref",
      "target_catalog",
      "target_schema",
      "compile_manifest"
    ],
    "property_order": [
      "environment_name",
      "git_ref",
      "commit_sha",
      "selector",
      "target_catalog",
      "target_schema",
      "source_model_run_id",
      "compile_manifest",
      "compile_diagnostics"
    ],
    "properties": {
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
      "commit_sha": {
        "schema": {
          "type": "string"
        }
      },
      "selector": {
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
      },
      "source_model_run_id": {
        "schema": {
          "type": "string"
        }
      },
      "compile_manifest": {
        "schema": {
          "type": "string"
        }
      },
      "compile_diagnostics": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "CreateCatalogRequest": {
    "required": [
      "name"
    ],
    "property_order": [
      "name",
      "metastore_type",
      "dsn",
      "data_path",
      "comment"
    ],
    "properties": {
      "name": {
        "schema": {
          "type": "string"
        }
      },
      "metastore_type": {
        "schema": {
          "ref": "MetastoreType"
        }
      },
      "dsn": {
        "schema": {
          "type": "string"
        }
      },
      "data_path": {
        "schema": {
          "type": "string"
        }
      },
      "comment": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "CreateCellRequest": {
    "required": [
      "cell_type"
    ],
    "property_order": [
      "cell_type",
      "name",
      "role",
      "disabled",
      "test",
      "visual_spec",
      "content",
      "position"
    ],
    "properties": {
      "cell_type": {
        "schema": {
          "ref": "CellCellType"
        }
      },
      "name": {
        "schema": {
          "type": "string"
        }
      },
      "role": {
        "schema": {
          "ref": "CellRole"
        }
      },
      "disabled": {
        "schema": {
          "type": "boolean"
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
      },
      "content": {
        "schema": {
          "type": "string"
        }
      },
      "position": {
        "schema": {
          "type": "integer",
          "format": "int32"
        }
      }
    }
  },
  "CreateColumnMaskRequest": {
    "required": [
      "name",
      "column_name",
      "mask_expression"
    ],
    "property_order": [
      "table_id",
      "name",
      "column_name",
      "mask_expression",
      "description"
    ],
    "properties": {
      "table_id": {
        "schema": {
          "type": "string"
        }
      },
      "name": {
        "schema": {
          "type": "string"
        }
      },
      "column_name": {
        "schema": {
          "type": "string"
        }
      },
      "mask_expression": {
        "schema": {
          "type": "string"
        }
      },
      "description": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "CreateColumnRequest": {
    "required": [
      "name",
      "type"
    ],
    "property_order": [
      "name",
      "type",
      "nullable",
      "comment"
    ],
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
      },
      "nullable": {
        "schema": {
          "type": "boolean"
        }
      },
      "comment": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "CreateComputeAssignmentRequest": {
    "required": [
      "principal_id",
      "principal_type"
    ],
    "property_order": [
      "principal_id",
      "principal_type",
      "fallback_local",
      "is_default"
    ],
    "properties": {
      "principal_id": {
        "schema": {
          "type": "string"
        }
      },
      "principal_type": {
        "schema": {
          "ref": "ComputeAssignmentPrincipalType"
        }
      },
      "fallback_local": {
        "schema": {
          "type": "boolean"
        }
      },
      "is_default": {
        "schema": {
          "type": "boolean"
        }
      }
    }
  },
  "CreateComputeEndpointRequest": {
    "required": [
      "name",
      "type",
      "url"
    ],
    "property_order": [
      "name",
      "type",
      "url",
      "auth_token",
      "max_memory_gb",
      "size"
    ],
    "properties": {
      "name": {
        "schema": {
          "type": "string"
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
        },
        "description": "Endpoint URI. REMOTE endpoints must use grpc:// or grpcs://; LOCAL endpoints use local routing URLs."
      },
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
      }
    }
  },
  "CreateDashboardRequest": {
    "required": [
      "name"
    ],
    "property_order": [
      "name",
      "description",
      "folder_id"
    ],
    "properties": {
      "name": {
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
      }
    }
  },
  "CreateDashboardWidgetRequest": {
    "required": [
      "name",
      "source",
      "layout"
    ],
    "property_order": [
      "name",
      "description",
      "source",
      "visual_spec",
      "layout"
    ],
    "properties": {
      "name": {
        "schema": {
          "type": "string"
        }
      },
      "description": {
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
      },
      "layout": {
        "schema": {
          "ref": "DashboardWidgetLayout"
        }
      }
    }
  },
  "CreateDataProductRequest": {
    "required": [
      "slug",
      "name",
      "domain_name",
      "team_name",
      "steward_principal",
      "contact_channel"
    ],
    "property_order": [
      "slug",
      "name",
      "description",
      "domain_name",
      "team_name",
      "steward_principal",
      "contact_channel",
      "visibility",
      "consumer_audience",
      "docs_url",
      "access_request_path",
      "business_definitions",
      "contract",
      "slo",
      "producing_build_id",
      "primary_asset_key",
      "semantic_model_refs",
      "created_by"
    ],
    "properties": {
      "slug": {
        "schema": {
          "type": "string"
        }
      },
      "name": {
        "schema": {
          "type": "string"
        }
      },
      "description": {
        "schema": {
          "type": "string"
        }
      },
      "domain_name": {
        "schema": {
          "type": "string"
        }
      },
      "team_name": {
        "schema": {
          "type": "string"
        }
      },
      "steward_principal": {
        "schema": {
          "type": "string"
        }
      },
      "contact_channel": {
        "schema": {
          "type": "string"
        }
      },
      "visibility": {
        "schema": {
          "type": "string"
        }
      },
      "consumer_audience": {
        "schema": {
          "type": "string"
        }
      },
      "docs_url": {
        "schema": {
          "type": "string"
        }
      },
      "access_request_path": {
        "schema": {
          "type": "string"
        }
      },
      "business_definitions": {
        "schema": {
          "type": "object",
          "additional_properties": {
            "schema": {
              "type": "string"
            }
          }
        }
      },
      "contract": {
        "schema": {
          "ref": "ProductContract"
        }
      },
      "slo": {
        "schema": {
          "ref": "ProductSLO"
        }
      },
      "producing_build_id": {
        "schema": {
          "type": "string"
        }
      },
      "primary_asset_key": {
        "schema": {
          "type": "string"
        }
      },
      "semantic_model_refs": {
        "schema": {
          "type": "array",
          "items": {
            "type": "string"
          }
        }
      },
      "created_by": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "CreateDataProductVersionRequest": {
    "required": [],
    "property_order": [
      "compatibility_level",
      "contract",
      "slo",
      "docs_url",
      "access_request_path",
      "producing_build_id",
      "output_asset_keys",
      "semantic_model_refs",
      "created_by"
    ],
    "properties": {
      "compatibility_level": {
        "schema": {
          "type": "string"
        }
      },
      "contract": {
        "schema": {
          "ref": "ProductContract"
        }
      },
      "slo": {
        "schema": {
          "ref": "ProductSLO"
        }
      },
      "docs_url": {
        "schema": {
          "type": "string"
        }
      },
      "access_request_path": {
        "schema": {
          "type": "string"
        }
      },
      "producing_build_id": {
        "schema": {
          "type": "string"
        }
      },
      "output_asset_keys": {
        "schema": {
          "type": "array",
          "items": {
            "type": "string"
          }
        }
      },
      "semantic_model_refs": {
        "schema": {
          "type": "array",
          "items": {
            "type": "string"
          }
        }
      },
      "created_by": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "CreateEnvironmentRequest": {
    "required": [
      "name",
      "target_catalog",
      "target_schema"
    ],
    "property_order": [
      "name",
      "kind",
      "description",
      "target_catalog",
      "target_schema",
      "compute_endpoint",
      "defer_to_environment",
      "variables",
      "source_overrides"
    ],
    "properties": {
      "name": {
        "schema": {
          "type": "string"
        }
      },
      "kind": {
        "schema": {
          "ref": "EnvironmentKind"
        }
      },
      "description": {
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
      },
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
      "variables": {
        "schema": {
          "type": "object",
          "additional_properties": {
            "schema": {
              "type": "string"
            }
          }
        }
      },
      "source_overrides": {
        "schema": {
          "type": "object",
          "additional_properties": {
            "schema": {
              "type": "string"
            }
          }
        }
      }
    }
  },
  "CreateExternalLocationRequest": {
    "required": [
      "name",
      "url"
    ],
    "property_order": [
      "name",
      "url",
      "credential_name",
      "storage_type",
      "comment",
      "read_only"
    ],
    "properties": {
      "name": {
        "schema": {
          "type": "string"
        }
      },
      "url": {
        "schema": {
          "type": "string"
        }
      },
      "credential_name": {
        "schema": {
          "type": "string"
        }
      },
      "storage_type": {
        "schema": {
          "ref": "StorageType"
        }
      },
      "comment": {
        "schema": {
          "type": "string"
        }
      },
      "read_only": {
        "schema": {
          "type": "boolean"
        }
      }
    }
  },
  "CreateFolderRequest": {
    "required": [
      "name"
    ],
    "property_order": [
      "name",
      "parent_folder_id",
      "git_repo_id",
      "git_root_path",
      "default_project_id",
      "default_environment_id"
    ],
    "properties": {
      "name": {
        "schema": {
          "type": "string"
        }
      },
      "parent_folder_id": {
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
      "default_project_id": {
        "schema": {
          "type": "string"
        }
      },
      "default_environment_id": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "CreateGitRepoRequest": {
    "required": [
      "url",
      "branch"
    ],
    "property_order": [
      "url",
      "branch",
      "path",
      "auth_token"
    ],
    "properties": {
      "url": {
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
      "auth_token": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "CreateGrantRequest": {
    "required": [
      "principal_id",
      "principal_type",
      "securable_type",
      "securable_id",
      "privilege"
    ],
    "property_order": [
      "principal_id",
      "principal_type",
      "securable_type",
      "securable_id",
      "privilege"
    ],
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
      "securable_type": {
        "schema": {
          "type": "string"
        }
      },
      "securable_id": {
        "schema": {
          "type": "string"
        }
      },
      "privilege": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "CreateGroupMemberRequest": {
    "required": [
      "member_type",
      "member_id"
    ],
    "property_order": [
      "member_type",
      "member_id"
    ],
    "properties": {
      "member_type": {
        "schema": {
          "ref": "PrincipalType"
        }
      },
      "member_id": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "CreateGroupRequest": {
    "required": [
      "name"
    ],
    "property_order": [
      "name",
      "description"
    ],
    "properties": {
      "name": {
        "schema": {
          "type": "string"
        }
      },
      "description": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "CreateMacroRequest": {
    "required": [
      "name",
      "body"
    ],
    "property_order": [
      "name",
      "body",
      "macro_type",
      "parameters",
      "description",
      "catalog_name",
      "project_name",
      "visibility",
      "owner",
      "properties",
      "tags",
      "status"
    ],
    "properties": {
      "name": {
        "schema": {
          "type": "string"
        }
      },
      "body": {
        "schema": {
          "type": "string"
        }
      },
      "macro_type": {
        "schema": {
          "ref": "MacroType"
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
      "description": {
        "schema": {
          "type": "string"
        }
      },
      "catalog_name": {
        "schema": {
          "type": "string"
        }
      },
      "project_name": {
        "schema": {
          "type": "string"
        }
      },
      "visibility": {
        "schema": {
          "ref": "MacroVisibility"
        }
      },
      "owner": {
        "schema": {
          "type": "string"
        }
      },
      "properties": {
        "schema": {
          "type": "object",
          "additional_properties": {
            "schema": {
              "type": "string"
            }
          }
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
      "status": {
        "schema": {
          "ref": "MacroStatus"
        }
      }
    }
  },
  "CreateModelRequest": {
    "required": [
      "project_name",
      "name",
      "sql"
    ],
    "property_order": [
      "project_name",
      "name",
      "sql",
      "materialization",
      "description",
      "tags",
      "config",
      "contract",
      "freshness_policy"
    ],
    "properties": {
      "project_name": {
        "schema": {
          "type": "string"
        }
      },
      "name": {
        "schema": {
          "type": "string"
        }
      },
      "sql": {
        "schema": {
          "type": "string"
        }
      },
      "materialization": {
        "schema": {
          "ref": "ModelMaterialization"
        }
      },
      "description": {
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
      "freshness_policy": {
        "schema": {
          "ref": "FreshnessPolicy"
        }
      }
    }
  },
  "CreateModelTestRequest": {
    "required": [
      "name",
      "test_type"
    ],
    "property_order": [
      "name",
      "test_type",
      "column",
      "config"
    ],
    "properties": {
      "name": {
        "schema": {
          "type": "string"
        }
      },
      "test_type": {
        "schema": {
          "ref": "ModelTestTestType"
        }
      },
      "column": {
        "schema": {
          "type": "string"
        }
      },
      "config": {
        "schema": {
          "ref": "ModelTestConfig"
        }
      }
    }
  },
  "CreateNotebookRequest": {
    "required": [
      "name"
    ],
    "property_order": [
      "name",
      "description",
      "source",
      "folder_id"
    ],
    "properties": {
      "name": {
        "schema": {
          "type": "string"
        }
      },
      "description": {
        "schema": {
          "type": "string"
        }
      },
      "source": {
        "schema": {
          "type": "string"
        }
      },
      "folder_id": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "CreatePipelineJobRequest": {
    "required": [
      "name"
    ],
    "property_order": [
      "name",
      "notebook_id",
      "compute_endpoint_id",
      "depends_on",
      "timeout_seconds",
      "retry_count",
      "job_order",
      "job_type",
      "model_selector"
    ],
    "properties": {
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
      "timeout_seconds": {
        "schema": {
          "type": "integer",
          "format": "int64"
        }
      },
      "retry_count": {
        "schema": {
          "type": "integer",
          "format": "int32"
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
      }
    }
  },
  "CreatePipelineRequest": {
    "required": [
      "name"
    ],
    "property_order": [
      "name",
      "description",
      "schedule_cron",
      "is_paused",
      "concurrency_limit",
      "folder_id"
    ],
    "properties": {
      "name": {
        "schema": {
          "type": "string"
        }
      },
      "description": {
        "schema": {
          "type": "string"
        }
      },
      "schedule_cron": {
        "schema": {
          "type": "string"
        }
      },
      "is_paused": {
        "schema": {
          "type": "boolean"
        }
      },
      "concurrency_limit": {
        "schema": {
          "type": "integer",
          "format": "int32"
        }
      },
      "folder_id": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "CreatePrincipalRequest": {
    "required": [
      "name"
    ],
    "property_order": [
      "name",
      "type",
      "is_admin"
    ],
    "properties": {
      "name": {
        "schema": {
          "type": "string"
        }
      },
      "type": {
        "schema": {
          "ref": "PrincipalType"
        }
      },
      "is_admin": {
        "schema": {
          "type": "boolean"
        }
      }
    }
  },
  "CreateProductDependencyRequest": {
    "required": [
      "depends_on_slug"
    ],
    "property_order": [
      "depends_on_slug"
    ],
    "properties": {
      "depends_on_slug": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "CreateProductDomainRequest": {
    "required": [
      "name"
    ],
    "property_order": [
      "name",
      "description"
    ],
    "properties": {
      "name": {
        "schema": {
          "type": "string"
        }
      },
      "description": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "CreateProductSubscriptionRequest": {
    "required": [
      "principal_name",
      "event_type"
    ],
    "property_order": [
      "principal_name",
      "event_type",
      "channel"
    ],
    "properties": {
      "principal_name": {
        "schema": {
          "type": "string"
        }
      },
      "event_type": {
        "schema": {
          "type": "string"
        }
      },
      "channel": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "CreateProductTeamRequest": {
    "required": [
      "name"
    ],
    "property_order": [
      "name",
      "contact_channel"
    ],
    "properties": {
      "name": {
        "schema": {
          "type": "string"
        }
      },
      "contact_channel": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "CreateProjectRequest": {
    "required": [
      "name"
    ],
    "property_order": [
      "name",
      "kind",
      "description",
      "product_id",
      "default_branch"
    ],
    "properties": {
      "name": {
        "schema": {
          "type": "string"
        }
      },
      "kind": {
        "schema": {
          "ref": "ProjectKind"
        }
      },
      "description": {
        "schema": {
          "type": "string"
        }
      },
      "product_id": {
        "schema": {
          "type": "string"
        }
      },
      "default_branch": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "CreateRowFilterRequest": {
    "required": [
      "name",
      "filter_sql"
    ],
    "property_order": [
      "table_id",
      "name",
      "filter_sql",
      "description"
    ],
    "properties": {
      "table_id": {
        "schema": {
          "type": "string"
        }
      },
      "name": {
        "schema": {
          "type": "string"
        }
      },
      "filter_sql": {
        "schema": {
          "type": "string"
        }
      },
      "description": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "CreateSavedResourceRequest": {
    "required": [
      "resource_type",
      "resource_key"
    ],
    "property_order": [
      "resource_type",
      "resource_key",
      "display_name",
      "resource_path",
      "section"
    ],
    "properties": {
      "resource_type": {
        "schema": {
          "type": "string"
        }
      },
      "resource_key": {
        "schema": {
          "type": "string"
        }
      },
      "display_name": {
        "schema": {
          "type": "string"
        }
      },
      "resource_path": {
        "schema": {
          "type": "string"
        }
      },
      "section": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "CreateSchemaRequest": {
    "required": [
      "name"
    ],
    "property_order": [
      "name",
      "comment",
      "location_name",
      "properties"
    ],
    "properties": {
      "name": {
        "schema": {
          "type": "string"
        }
      },
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
      "properties": {
        "schema": {
          "type": "object",
          "additional_properties": {
            "schema": {
              "type": "string"
            }
          }
        }
      }
    }
  },
  "CreateSemanticMetricRequest": {
    "required": [
      "name",
      "metric_type",
      "expression"
    ],
    "property_order": [
      "name",
      "description",
      "label",
      "metric_type",
      "expression_mode",
      "expression",
      "relationship_names",
      "filter_sql",
      "default_time_grain",
      "format",
      "certification_state"
    ],
    "properties": {
      "name": {
        "schema": {
          "type": "string"
        }
      },
      "description": {
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
      "expression_mode": {
        "schema": {
          "ref": "SemanticMetricExpressionMode"
        }
      },
      "expression": {
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
      "filter_sql": {
        "schema": {
          "type": "string"
        }
      },
      "default_time_grain": {
        "schema": {
          "type": "string"
        }
      },
      "format": {
        "schema": {
          "type": "string"
        }
      },
      "certification_state": {
        "schema": {
          "ref": "CreateSemanticMetricRequestCertificationState"
        }
      }
    }
  },
  "CreateSemanticMetricRequestCertificationState": {
    "required": []
  },
  "CreateSemanticModelRequest": {
    "required": [
      "name",
      "base_model_ref"
    ],
    "property_order": [
      "name",
      "description",
      "base_model_ref",
      "default_time_dimension",
      "tags"
    ],
    "properties": {
      "name": {
        "schema": {
          "type": "string"
        }
      },
      "description": {
        "schema": {
          "type": "string"
        }
      },
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
  "CreateSemanticPreAggregationRequest": {
    "required": [
      "name",
      "target_relation"
    ],
    "property_order": [
      "name",
      "metric_set",
      "dimension_set",
      "grain",
      "target_relation",
      "refresh_policy"
    ],
    "properties": {
      "name": {
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
      "target_relation": {
        "schema": {
          "type": "string"
        }
      },
      "refresh_policy": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "CreateSemanticRelationshipRequest": {
    "required": [
      "name",
      "from_semantic_id",
      "to_semantic_id",
      "relationship_type",
      "join_sql"
    ],
    "property_order": [
      "name",
      "from_semantic_id",
      "to_semantic_id",
      "relationship_type",
      "join_sql",
      "cost",
      "max_hops"
    ],
    "properties": {
      "name": {
        "schema": {
          "type": "string"
        }
      },
      "from_semantic_id": {
        "schema": {
          "type": "string"
        }
      },
      "to_semantic_id": {
        "schema": {
          "type": "string"
        }
      },
      "relationship_type": {
        "schema": {
          "ref": "SemanticRelationshipRelationshipType"
        }
      },
      "join_sql": {
        "schema": {
          "type": "string"
        }
      },
      "cost": {
        "schema": {
          "type": "integer",
          "format": "int32"
        }
      },
      "max_hops": {
        "schema": {
          "type": "integer",
          "format": "int32"
        }
      }
    }
  },
  "CreateStorageCredentialRequest": {
    "required": [
      "name"
    ],
    "property_order": [
      "name",
      "credential_type",
      "key_id",
      "secret",
      "endpoint",
      "region",
      "url_style",
      "comment"
    ],
    "properties": {
      "name": {
        "schema": {
          "type": "string"
        }
      },
      "credential_type": {
        "schema": {
          "ref": "StorageCredentialType"
        }
      },
      "key_id": {
        "schema": {
          "type": "string"
        }
      },
      "secret": {
        "schema": {
          "type": "string"
        }
      },
      "endpoint": {
        "schema": {
          "type": "string"
        }
      },
      "region": {
        "schema": {
          "type": "string"
        }
      },
      "url_style": {
        "schema": {
          "ref": "URLStyle"
        }
      },
      "comment": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "CreateTableRequest": {
    "required": [
      "name"
    ],
    "property_order": [
      "name",
      "columns",
      "comment"
    ],
    "properties": {
      "name": {
        "schema": {
          "type": "string"
        }
      },
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
      }
    }
  },
  "CreateTagAssignmentRequest": {
    "required": [
      "securable_type",
      "securable_id"
    ],
    "property_order": [
      "securable_type",
      "securable_id",
      "column_name"
    ],
    "properties": {
      "securable_type": {
        "schema": {
          "ref": "TagAssignmentSecurableType"
        }
      },
      "securable_id": {
        "schema": {
          "type": "string"
        }
      },
      "column_name": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "CreateTagRequest": {
    "required": [
      "key"
    ],
    "property_order": [
      "key",
      "value"
    ],
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
  "CreateViewRequest": {
    "required": [
      "name",
      "view_definition"
    ],
    "property_order": [
      "name",
      "view_definition",
      "comment"
    ],
    "properties": {
      "name": {
        "schema": {
          "type": "string"
        }
      },
      "view_definition": {
        "schema": {
          "type": "string"
        }
      },
      "comment": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "CreateVolumeRequest": {
    "required": [
      "name"
    ],
    "property_order": [
      "name",
      "volume_type",
      "storage_location",
      "comment"
    ],
    "properties": {
      "name": {
        "schema": {
          "type": "string"
        }
      },
      "volume_type": {
        "schema": {
          "type": "string"
        }
      },
      "storage_location": {
        "schema": {
          "type": "string"
        }
      },
      "comment": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "CreateWorkspaceRequest": {
    "required": [
      "name"
    ],
    "property_order": [
      "name",
      "kind",
      "owner_team_id",
      "owner_principal",
      "default_project_id",
      "default_environment_id",
      "git_repo_id",
      "git_root_path"
    ],
    "properties": {
      "name": {
        "schema": {
          "type": "string"
        }
      },
      "kind": {
        "schema": {
          "ref": "WorkspaceKind"
        }
      },
      "owner_team_id": {
        "schema": {
          "type": "string"
        }
      },
      "owner_principal": {
        "schema": {
          "type": "string"
        }
      },
      "default_project_id": {
        "schema": {
          "type": "string"
        }
      },
      "default_environment_id": {
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
      }
    }
  },
  "Dashboard": {
    "required": [],
    "property_order": [
      "id",
      "name",
      "description",
      "owner",
      "folder_id",
      "created_at",
      "updated_at"
    ],
    "properties": {
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
      "folder_id": {
        "schema": {
          "type": "string"
        }
      },
      "created_at": {
        "schema": {
          "type": "string",
          "format": "date-time"
        }
      },
      "updated_at": {
        "schema": {
          "type": "string",
          "format": "date-time"
        }
      }
    }
  },
  "DashboardDetail": {
    "required": [],
    "property_order": [
      "dashboard",
      "widgets"
    ],
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
    "required": [
      "notebook_id",
      "cell_id"
    ],
    "property_order": [
      "notebook_id",
      "cell_id"
    ],
    "properties": {
      "notebook_id": {
        "schema": {
          "type": "string"
        }
      },
      "cell_id": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "DashboardNotebookCellSourceUpdate": {
    "required": [],
    "property_order": [
      "notebook_id",
      "cell_id"
    ],
    "properties": {
      "notebook_id": {
        "schema": {
          "type": "string"
        }
      },
      "cell_id": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "DashboardSQLQuerySource": {
    "required": [
      "sql"
    ],
    "property_order": [
      "sql",
      "catalog",
      "schema"
    ],
    "properties": {
      "sql": {
        "schema": {
          "type": "string"
        }
      },
      "catalog": {
        "schema": {
          "type": "string"
        }
      },
      "schema": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "DashboardSQLQuerySourceUpdate": {
    "required": [],
    "property_order": [
      "sql",
      "catalog",
      "schema"
    ],
    "properties": {
      "sql": {
        "schema": {
          "type": "string"
        }
      },
      "catalog": {
        "schema": {
          "type": "string"
        }
      },
      "schema": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "DashboardSemanticQuerySource": {
    "required": [
      "semantic_model_id",
      "metrics"
    ],
    "property_order": [
      "semantic_model_id",
      "metrics",
      "relationship_names",
      "dimensions",
      "filters",
      "order_by",
      "limit",
      "time_grain"
    ],
    "properties": {
      "semantic_model_id": {
        "schema": {
          "type": "string"
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
      "relationship_names": {
        "schema": {
          "type": "array",
          "items": {
            "type": "string"
          }
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
      "filters": {
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
      "limit": {
        "schema": {
          "type": "integer",
          "format": "int32"
        }
      },
      "time_grain": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "DashboardSemanticQuerySourceUpdate": {
    "required": [],
    "property_order": [
      "semantic_model_id",
      "metrics",
      "relationship_names",
      "dimensions",
      "filters",
      "order_by",
      "limit",
      "time_grain"
    ],
    "properties": {
      "semantic_model_id": {
        "schema": {
          "type": "string"
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
      "relationship_names": {
        "schema": {
          "type": "array",
          "items": {
            "type": "string"
          }
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
      "filters": {
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
      "limit": {
        "schema": {
          "type": "integer",
          "format": "int32"
        }
      },
      "time_grain": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "DashboardWidget": {
    "required": [],
    "property_order": [
      "id",
      "dashboard_id",
      "name",
      "description",
      "source",
      "visual_spec",
      "layout",
      "created_at",
      "updated_at"
    ],
    "properties": {
      "id": {
        "schema": {
          "type": "string"
        }
      },
      "dashboard_id": {
        "schema": {
          "type": "string"
        }
      },
      "name": {
        "schema": {
          "type": "string"
        }
      },
      "description": {
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
      },
      "layout": {
        "schema": {
          "ref": "DashboardWidgetLayout"
        }
      },
      "created_at": {
        "schema": {
          "type": "string",
          "format": "date-time"
        }
      },
      "updated_at": {
        "schema": {
          "type": "string",
          "format": "date-time"
        }
      }
    }
  },
  "DashboardWidgetLayout": {
    "required": [
      "x",
      "y",
      "w",
      "h"
    ],
    "property_order": [
      "x",
      "y",
      "w",
      "h"
    ],
    "properties": {
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
      },
      "w": {
        "schema": {
          "type": "integer",
          "format": "int32"
        }
      },
      "h": {
        "schema": {
          "type": "integer",
          "format": "int32"
        }
      }
    }
  },
  "DashboardWidgetLayoutUpdate": {
    "required": [],
    "property_order": [
      "x",
      "y",
      "w",
      "h"
    ],
    "properties": {
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
      },
      "w": {
        "schema": {
          "type": "integer",
          "format": "int32"
        }
      },
      "h": {
        "schema": {
          "type": "integer",
          "format": "int32"
        }
      }
    }
  },
  "DashboardWidgetSource": {
    "required": [
      "kind"
    ],
    "property_order": [
      "kind",
      "sql_query",
      "notebook_cell",
      "semantic_query"
    ],
    "properties": {
      "kind": {
        "schema": {
          "ref": "DashboardWidgetSourceKind"
        }
      },
      "sql_query": {
        "schema": {
          "ref": "DashboardSQLQuerySource"
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
      }
    }
  },
  "DashboardWidgetSourceKind": {
    "required": []
  },
  "DashboardWidgetSourceUpdate": {
    "required": [],
    "property_order": [
      "kind",
      "sql_query",
      "notebook_cell",
      "semantic_query"
    ],
    "properties": {
      "kind": {
        "schema": {
          "ref": "DashboardWidgetSourceKind"
        }
      },
      "sql_query": {
        "schema": {
          "ref": "DashboardSQLQuerySourceUpdate"
        }
      },
      "notebook_cell": {
        "schema": {
          "ref": "DashboardNotebookCellSourceUpdate"
        }
      },
      "semantic_query": {
        "schema": {
          "ref": "DashboardSemanticQuerySourceUpdate"
        }
      }
    }
  },
  "DataProduct": {
    "required": [
      "id",
      "slug",
      "name",
      "description",
      "domain_id",
      "owner_team_id",
      "steward_principal",
      "contact_channel"
    ],
    "property_order": [
      "id",
      "slug",
      "name",
      "description",
      "domain_id",
      "owner_team_id",
      "steward_principal",
      "contact_channel",
      "visibility",
      "consumer_audience",
      "docs_url",
      "access_request_path",
      "business_definitions",
      "contract",
      "slo",
      "publication_intent",
      "created_by",
      "created_at",
      "updated_at"
    ],
    "properties": {
      "id": {
        "schema": {
          "type": "string"
        }
      },
      "slug": {
        "schema": {
          "type": "string"
        }
      },
      "name": {
        "schema": {
          "type": "string"
        }
      },
      "description": {
        "schema": {
          "type": "string"
        }
      },
      "domain_id": {
        "schema": {
          "type": "string"
        }
      },
      "owner_team_id": {
        "schema": {
          "type": "string"
        }
      },
      "steward_principal": {
        "schema": {
          "type": "string"
        }
      },
      "contact_channel": {
        "schema": {
          "type": "string"
        }
      },
      "visibility": {
        "schema": {
          "type": "string"
        }
      },
      "consumer_audience": {
        "schema": {
          "type": "string"
        }
      },
      "docs_url": {
        "schema": {
          "type": "string"
        }
      },
      "access_request_path": {
        "schema": {
          "type": "string"
        }
      },
      "business_definitions": {
        "schema": {
          "type": "object",
          "additional_properties": {
            "schema": {
              "type": "string"
            }
          }
        }
      },
      "contract": {
        "schema": {
          "ref": "ProductContract"
        }
      },
      "slo": {
        "schema": {
          "ref": "ProductSLO"
        }
      },
      "publication_intent": {
        "schema": {
          "type": "string"
        }
      },
      "created_by": {
        "schema": {
          "type": "string"
        }
      },
      "created_at": {
        "schema": {
          "type": "string",
          "format": "date-time"
        }
      },
      "updated_at": {
        "schema": {
          "type": "string",
          "format": "date-time"
        }
      }
    }
  },
  "DataProductDetail": {
    "required": [
      "product",
      "domain",
      "owner_team",
      "versions",
      "outputs",
      "semantic_entrypoints",
      "dependencies",
      "subscriptions",
      "events"
    ],
    "property_order": [
      "product",
      "domain",
      "owner_team",
      "versions",
      "status",
      "outputs",
      "semantic_entrypoints",
      "dependencies",
      "subscriptions",
      "events"
    ],
    "properties": {
      "product": {
        "schema": {
          "ref": "DataProduct"
        }
      },
      "domain": {
        "schema": {
          "ref": "ProductDomain"
        }
      },
      "owner_team": {
        "schema": {
          "ref": "ProductTeam"
        }
      },
      "versions": {
        "schema": {
          "type": "array",
          "items": {
            "ref": "DataProductVersion"
          }
        }
      },
      "status": {
        "schema": {
          "ref": "DataProductStatus"
        }
      },
      "outputs": {
        "schema": {
          "type": "array",
          "items": {
            "ref": "ProductOutput"
          }
        }
      },
      "semantic_entrypoints": {
        "schema": {
          "type": "array",
          "items": {
            "ref": "ProductSemanticEntrypoint"
          }
        }
      },
      "dependencies": {
        "schema": {
          "type": "array",
          "items": {
            "ref": "DataProductListItem"
          }
        }
      },
      "subscriptions": {
        "schema": {
          "type": "array",
          "items": {
            "ref": "ProductSubscription"
          }
        }
      },
      "events": {
        "schema": {
          "type": "array",
          "items": {
            "ref": "ProductEvent"
          }
        }
      }
    }
  },
  "DataProductListItem": {
    "required": [
      "product",
      "domain",
      "owner_team"
    ],
    "property_order": [
      "product",
      "domain",
      "owner_team",
      "latest_version",
      "status",
      "primary_output"
    ],
    "properties": {
      "product": {
        "schema": {
          "ref": "DataProduct"
        }
      },
      "domain": {
        "schema": {
          "ref": "ProductDomain"
        }
      },
      "owner_team": {
        "schema": {
          "ref": "ProductTeam"
        }
      },
      "latest_version": {
        "schema": {
          "ref": "DataProductVersion"
        }
      },
      "status": {
        "schema": {
          "ref": "DataProductStatus"
        }
      },
      "primary_output": {
        "schema": {
          "ref": "ProductOutput"
        }
      }
    }
  },
  "DataProductStatus": {
    "required": [
      "product_id",
      "publication_state",
      "certification_state",
      "freshness_status",
      "quality_status",
      "failing_checks_count"
    ],
    "property_order": [
      "product_id",
      "publication_state",
      "certification_state",
      "freshness_status",
      "quality_status",
      "last_successful_update_at",
      "failing_checks_count",
      "lineage_coverage",
      "adoption_metrics",
      "open_warnings",
      "replacement_product_id",
      "updated_at"
    ],
    "properties": {
      "product_id": {
        "schema": {
          "type": "string"
        }
      },
      "publication_state": {
        "schema": {
          "type": "string"
        }
      },
      "certification_state": {
        "schema": {
          "type": "string"
        }
      },
      "freshness_status": {
        "schema": {
          "type": "string"
        }
      },
      "quality_status": {
        "schema": {
          "type": "string"
        }
      },
      "last_successful_update_at": {
        "schema": {
          "type": "string",
          "format": "date-time"
        }
      },
      "failing_checks_count": {
        "schema": {
          "type": "integer",
          "format": "int64"
        }
      },
      "lineage_coverage": {
        "schema": {
          "type": "number",
          "format": "double"
        }
      },
      "adoption_metrics": {
        "schema": {
          "type": "object",
          "additional_properties": {
            "any": true
          }
        }
      },
      "open_warnings": {
        "schema": {
          "type": "array",
          "items": {
            "type": "string"
          }
        }
      },
      "replacement_product_id": {
        "schema": {
          "type": "string"
        }
      },
      "updated_at": {
        "schema": {
          "type": "string",
          "format": "date-time"
        }
      }
    }
  },
  "DataProductVersion": {
    "required": [
      "id",
      "product_id",
      "version",
      "release_state",
      "compatibility_level"
    ],
    "property_order": [
      "id",
      "product_id",
      "producing_build_id",
      "version",
      "release_state",
      "compatibility_level",
      "contract",
      "slo",
      "docs_url",
      "access_request_path",
      "created_by",
      "created_at"
    ],
    "properties": {
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
      "producing_build_id": {
        "schema": {
          "type": "string"
        }
      },
      "version": {
        "schema": {
          "type": "integer",
          "format": "int32"
        }
      },
      "release_state": {
        "schema": {
          "type": "string"
        }
      },
      "compatibility_level": {
        "schema": {
          "type": "string"
        }
      },
      "contract": {
        "schema": {
          "ref": "ProductContract"
        }
      },
      "slo": {
        "schema": {
          "ref": "ProductSLO"
        }
      },
      "docs_url": {
        "schema": {
          "type": "string"
        }
      },
      "access_request_path": {
        "schema": {
          "type": "string"
        }
      },
      "created_by": {
        "schema": {
          "type": "string"
        }
      },
      "created_at": {
        "schema": {
          "type": "string",
          "format": "date-time"
        }
      }
    }
  },
  "DataProductVersionDetail": {
    "required": [
      "product",
      "domain",
      "owner_team",
      "version",
      "outputs",
      "semantic_entrypoints",
      "dependencies",
      "events"
    ],
    "property_order": [
      "product",
      "domain",
      "owner_team",
      "version",
      "status",
      "outputs",
      "semantic_entrypoints",
      "dependencies",
      "events"
    ],
    "properties": {
      "product": {
        "schema": {
          "ref": "DataProduct"
        }
      },
      "domain": {
        "schema": {
          "ref": "ProductDomain"
        }
      },
      "owner_team": {
        "schema": {
          "ref": "ProductTeam"
        }
      },
      "version": {
        "schema": {
          "ref": "DataProductVersion"
        }
      },
      "status": {
        "schema": {
          "ref": "DataProductStatus"
        }
      },
      "outputs": {
        "schema": {
          "type": "array",
          "items": {
            "ref": "ProductOutput"
          }
        }
      },
      "semantic_entrypoints": {
        "schema": {
          "type": "array",
          "items": {
            "ref": "ProductSemanticEntrypoint"
          }
        }
      },
      "dependencies": {
        "schema": {
          "type": "array",
          "items": {
            "ref": "DataProductListItem"
          }
        }
      },
      "events": {
        "schema": {
          "type": "array",
          "items": {
            "ref": "ProductEvent"
          }
        }
      }
    }
  },
  "DataProductVersionList": {
    "required": [
      "data"
    ],
    "property_order": [
      "data"
    ],
    "properties": {
      "data": {
        "schema": {
          "type": "array",
          "items": {
            "ref": "DataProductVersion"
          }
        }
      }
    }
  },
  "DeprecateProductVersionRequest": {
    "required": [],
    "property_order": [
      "replacement_slug"
    ],
    "properties": {
      "replacement_slug": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "DuplicateNotebookRequest": {
    "required": [
      "folder_id"
    ],
    "property_order": [
      "folder_id",
      "name",
      "git_path"
    ],
    "properties": {
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
      "git_path": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "Environment": {
    "required": [
      "name",
      "kind",
      "target_catalog",
      "target_schema"
    ],
    "property_order": [
      "id",
      "project_id",
      "project_name",
      "name",
      "kind",
      "description",
      "target_catalog",
      "target_schema",
      "compute_endpoint",
      "defer_to_environment",
      "variables",
      "source_overrides",
      "created_at",
      "updated_at"
    ],
    "properties": {
      "id": {
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
      "name": {
        "schema": {
          "type": "string"
        }
      },
      "kind": {
        "schema": {
          "ref": "EnvironmentKind"
        }
      },
      "description": {
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
      },
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
      "variables": {
        "schema": {
          "type": "object",
          "additional_properties": {
            "schema": {
              "type": "string"
            }
          }
        }
      },
      "source_overrides": {
        "schema": {
          "type": "object",
          "additional_properties": {
            "schema": {
              "type": "string"
            }
          }
        }
      },
      "created_at": {
        "schema": {
          "type": "string",
          "format": "date-time"
        }
      },
      "updated_at": {
        "schema": {
          "type": "string",
          "format": "date-time"
        }
      }
    }
  },
  "EnvironmentKind": {
    "required": []
  },
  "Error": {
    "title": "Standard API error response.",
    "description": "Errors use a shared schema across the API so clients can handle failure responses consistently.",
    "required": [
      "code",
      "message"
    ],
    "property_order": [
      "code",
      "message",
      "details"
    ],
    "properties": {
      "code": {
        "schema": {
          "type": "integer",
          "format": "int32"
        }
      },
      "message": {
        "schema": {
          "type": "string"
        }
      },
      "details": {
        "schema": {
          "type": "object",
          "additional_properties": {
            "schema": {
              "type": "string"
            }
          }
        }
      }
    }
  },
  "ExternalLocation": {
    "required": [
      "id",
      "name",
      "url"
    ],
    "property_order": [
      "id",
      "name",
      "url",
      "credential_name",
      "storage_type",
      "comment",
      "owner",
      "read_only",
      "created_at",
      "updated_at"
    ],
    "properties": {
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
      "url": {
        "schema": {
          "type": "string"
        }
      },
      "credential_name": {
        "schema": {
          "type": "string"
        }
      },
      "storage_type": {
        "schema": {
          "ref": "StorageType"
        }
      },
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
      "read_only": {
        "schema": {
          "type": "boolean"
        }
      },
      "created_at": {
        "schema": {
          "type": "string",
          "format": "date-time"
        }
      },
      "updated_at": {
        "schema": {
          "type": "string",
          "format": "date-time"
        }
      }
    }
  },
  "Folder": {
    "required": [],
    "property_order": [
      "id",
      "workspace_id",
      "name",
      "owner",
      "parent_folder_id",
      "path",
      "depth",
      "system_role",
      "git_repo_id",
      "git_root_path",
      "default_project_id",
      "default_environment_id",
      "created_at",
      "updated_at"
    ],
    "properties": {
      "id": {
        "schema": {
          "type": "string"
        }
      },
      "workspace_id": {
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
      "depth": {
        "schema": {
          "type": "integer",
          "format": "int32"
        }
      },
      "system_role": {
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
      "default_project_id": {
        "schema": {
          "type": "string"
        }
      },
      "default_environment_id": {
        "schema": {
          "type": "string"
        }
      },
      "created_at": {
        "schema": {
          "type": "string",
          "format": "date-time"
        }
      },
      "updated_at": {
        "schema": {
          "type": "string",
          "format": "date-time"
        }
      }
    }
  },
  "FolderContentItem": {
    "required": [],
    "property_order": [
      "kind",
      "scope",
      "id",
      "name",
      "owner",
      "folder_id",
      "project_name",
      "updated_at",
      "git_repo_id",
      "shared",
      "project_bound"
    ],
    "properties": {
      "kind": {
        "schema": {
          "type": "string"
        }
      },
      "scope": {
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
      "folder_id": {
        "schema": {
          "type": "string"
        }
      },
      "project_name": {
        "schema": {
          "type": "string"
        }
      },
      "updated_at": {
        "schema": {
          "type": "string",
          "format": "date-time"
        }
      },
      "git_repo_id": {
        "schema": {
          "type": "string"
        }
      },
      "shared": {
        "schema": {
          "type": "boolean"
        }
      },
      "project_bound": {
        "schema": {
          "type": "boolean"
        }
      }
    }
  },
  "FolderPath": {
    "required": [
      "data"
    ],
    "property_order": [
      "data"
    ],
    "properties": {
      "data": {
        "schema": {
          "type": "array",
          "items": {
            "ref": "Folder"
          }
        }
      }
    }
  },
  "FolderShare": {
    "required": [],
    "property_order": [
      "principal_name",
      "role"
    ],
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
    "required": [],
    "property_order": [
      "max_lag_seconds",
      "cron_schedule"
    ],
    "properties": {
      "max_lag_seconds": {
        "schema": {
          "type": "integer",
          "format": "int64"
        }
      },
      "cron_schedule": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "FreshnessStatus": {
    "required": [],
    "property_order": [
      "is_fresh",
      "last_run_at",
      "max_lag_seconds",
      "stale_since"
    ],
    "properties": {
      "is_fresh": {
        "schema": {
          "type": "boolean"
        }
      },
      "last_run_at": {
        "schema": {
          "type": "string",
          "format": "date-time"
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
          "type": "string",
          "format": "date-time"
        }
      }
    }
  },
  "GitRepo": {
    "required": [],
    "property_order": [
      "id",
      "url",
      "branch",
      "path",
      "owner",
      "last_sync_at",
      "last_commit",
      "created_at",
      "updated_at"
    ],
    "properties": {
      "id": {
        "schema": {
          "type": "string"
        }
      },
      "url": {
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
      "owner": {
        "schema": {
          "type": "string"
        }
      },
      "last_sync_at": {
        "schema": {
          "type": "string",
          "format": "date-time"
        }
      },
      "last_commit": {
        "schema": {
          "type": "string"
        }
      },
      "created_at": {
        "schema": {
          "type": "string",
          "format": "date-time"
        }
      },
      "updated_at": {
        "schema": {
          "type": "string",
          "format": "date-time"
        }
      }
    }
  },
  "GitSyncResult": {
    "required": [],
    "property_order": [
      "notebooks_created",
      "notebooks_updated",
      "notebooks_deleted",
      "commit_sha"
    ],
    "properties": {
      "notebooks_created": {
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
      },
      "notebooks_deleted": {
        "schema": {
          "type": "integer",
          "format": "int32"
        }
      },
      "commit_sha": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "Group": {
    "required": [
      "id",
      "name"
    ],
    "property_order": [
      "id",
      "name",
      "description",
      "created_at"
    ],
    "properties": {
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
      "description": {
        "schema": {
          "type": "string"
        }
      },
      "created_at": {
        "schema": {
          "type": "string",
          "format": "date-time"
        }
      }
    }
  },
  "GroupMember": {
    "required": [
      "group_id",
      "member_type",
      "member_id"
    ],
    "property_order": [
      "group_id",
      "member_type",
      "member_id"
    ],
    "properties": {
      "group_id": {
        "schema": {
          "type": "string"
        }
      },
      "member_type": {
        "schema": {
          "ref": "PrincipalType"
        }
      },
      "member_id": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "HealthResponse": {
    "title": "Service health status.",
    "required": [
      "status"
    ],
    "property_order": [
      "status"
    ],
    "properties": {
      "status": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "IngestionOptions": {
    "required": [],
    "property_order": [
      "allow_missing_columns",
      "ignore_extra_columns"
    ],
    "properties": {
      "allow_missing_columns": {
        "schema": {
          "type": "boolean"
        }
      },
      "ignore_extra_columns": {
        "schema": {
          "type": "boolean"
        }
      }
    }
  },
  "IngestionResult": {
    "required": [
      "files_registered",
      "files_skipped",
      "schema",
      "table"
    ],
    "property_order": [
      "files_registered",
      "files_skipped",
      "schema",
      "table"
    ],
    "properties": {
      "files_registered": {
        "schema": {
          "type": "integer",
          "format": "int64"
        }
      },
      "files_skipped": {
        "schema": {
          "type": "integer",
          "format": "int64"
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
    }
  },
  "LineageEdge": {
    "required": [],
    "property_order": [
      "id",
      "source_table",
      "target_table",
      "source_schema",
      "target_schema",
      "edge_type",
      "principal_name",
      "created_at"
    ],
    "properties": {
      "id": {
        "schema": {
          "type": "string"
        }
      },
      "source_table": {
        "schema": {
          "type": "string"
        }
      },
      "target_table": {
        "schema": {
          "type": "string"
        }
      },
      "source_schema": {
        "schema": {
          "type": "string"
        }
      },
      "target_schema": {
        "schema": {
          "type": "string"
        }
      },
      "edge_type": {
        "schema": {
          "type": "string"
        }
      },
      "principal_name": {
        "schema": {
          "type": "string"
        }
      },
      "created_at": {
        "schema": {
          "type": "string",
          "format": "date-time"
        }
      }
    }
  },
  "LoadExternalRequest": {
    "required": [
      "paths"
    ],
    "property_order": [
      "paths",
      "options"
    ],
    "properties": {
      "paths": {
        "schema": {
          "type": "array",
          "items": {
            "type": "string"
          }
        }
      },
      "options": {
        "schema": {
          "ref": "IngestionOptions"
        }
      }
    }
  },
  "LocalLoginRequest": {
    "required": [
      "username",
      "password"
    ],
    "property_order": [
      "username",
      "password"
    ],
    "properties": {
      "username": {
        "schema": {
          "type": "string"
        }
      },
      "password": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "Macro": {
    "required": [],
    "property_order": [
      "id",
      "name",
      "macro_type",
      "parameters",
      "body",
      "description",
      "catalog_name",
      "project_name",
      "visibility",
      "owner",
      "properties",
      "tags",
      "status",
      "created_by",
      "created_at",
      "updated_at"
    ],
    "properties": {
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
      "macro_type": {
        "schema": {
          "ref": "MacroType"
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
      "body": {
        "schema": {
          "type": "string"
        }
      },
      "description": {
        "schema": {
          "type": "string"
        }
      },
      "catalog_name": {
        "schema": {
          "type": "string"
        }
      },
      "project_name": {
        "schema": {
          "type": "string"
        }
      },
      "visibility": {
        "schema": {
          "ref": "MacroVisibility"
        }
      },
      "owner": {
        "schema": {
          "type": "string"
        }
      },
      "properties": {
        "schema": {
          "type": "object",
          "additional_properties": {
            "schema": {
              "type": "string"
            }
          }
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
      "status": {
        "schema": {
          "ref": "MacroStatus"
        }
      },
      "created_by": {
        "schema": {
          "type": "string"
        }
      },
      "created_at": {
        "schema": {
          "type": "string",
          "format": "date-time"
        }
      },
      "updated_at": {
        "schema": {
          "type": "string",
          "format": "date-time"
        }
      }
    }
  },
  "MacroImpactList": {
    "required": [
      "data"
    ],
    "property_order": [
      "data",
      "next_page_token"
    ],
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
    }
  },
  "MacroImpactModel": {
    "required": [],
    "property_order": [
      "target_table",
      "target_schema",
      "model_name",
      "last_seen_at"
    ],
    "properties": {
      "target_table": {
        "schema": {
          "type": "string"
        }
      },
      "target_schema": {
        "schema": {
          "type": "string"
        }
      },
      "model_name": {
        "schema": {
          "type": "string"
        }
      },
      "last_seen_at": {
        "schema": {
          "type": "string",
          "format": "date-time"
        }
      }
    }
  },
  "MacroRevision": {
    "required": [],
    "property_order": [
      "id",
      "macro_name",
      "version",
      "content_hash",
      "parameters",
      "body",
      "description",
      "status",
      "created_by",
      "created_at"
    ],
    "properties": {
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
      "version": {
        "schema": {
          "type": "integer",
          "format": "int32"
        }
      },
      "content_hash": {
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
      "body": {
        "schema": {
          "type": "string"
        }
      },
      "description": {
        "schema": {
          "type": "string"
        }
      },
      "status": {
        "schema": {
          "ref": "MacroStatus"
        }
      },
      "created_by": {
        "schema": {
          "type": "string"
        }
      },
      "created_at": {
        "schema": {
          "type": "string",
          "format": "date-time"
        }
      }
    }
  },
  "MacroRevisionDiff": {
    "required": [],
    "property_order": [
      "macro_name",
      "from_version",
      "to_version",
      "from_content_hash",
      "to_content_hash",
      "changed",
      "parameters_changed",
      "body_changed",
      "description_changed",
      "status_changed",
      "from_parameters",
      "to_parameters",
      "from_body",
      "to_body",
      "from_description",
      "to_description",
      "from_status",
      "to_status",
      "impact_changed",
      "impacted_models_added",
      "impacted_models_removed",
      "impacted_models_unchanged"
    ],
    "properties": {
      "macro_name": {
        "schema": {
          "type": "string"
        }
      },
      "from_version": {
        "schema": {
          "type": "integer",
          "format": "int32"
        }
      },
      "to_version": {
        "schema": {
          "type": "integer",
          "format": "int32"
        }
      },
      "from_content_hash": {
        "schema": {
          "type": "string"
        }
      },
      "to_content_hash": {
        "schema": {
          "type": "string"
        }
      },
      "changed": {
        "schema": {
          "type": "boolean"
        }
      },
      "parameters_changed": {
        "schema": {
          "type": "boolean"
        }
      },
      "body_changed": {
        "schema": {
          "type": "boolean"
        }
      },
      "description_changed": {
        "schema": {
          "type": "boolean"
        }
      },
      "status_changed": {
        "schema": {
          "type": "boolean"
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
      "to_parameters": {
        "schema": {
          "type": "array",
          "items": {
            "type": "string"
          }
        }
      },
      "from_body": {
        "schema": {
          "type": "string"
        }
      },
      "to_body": {
        "schema": {
          "type": "string"
        }
      },
      "from_description": {
        "schema": {
          "type": "string"
        }
      },
      "to_description": {
        "schema": {
          "type": "string"
        }
      },
      "from_status": {
        "schema": {
          "ref": "MacroStatus"
        }
      },
      "to_status": {
        "schema": {
          "ref": "MacroStatus"
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
      }
    }
  },
  "MacroRevisionList": {
    "required": [
      "data"
    ],
    "property_order": [
      "data"
    ],
    "properties": {
      "data": {
        "schema": {
          "type": "array",
          "items": {
            "ref": "MacroRevision"
          }
        }
      }
    }
  },
  "MacroStatus": {
    "required": []
  },
  "MacroType": {
    "required": []
  },
  "MacroVisibility": {
    "required": []
  },
  "ManifestColumn": {
    "required": [
      "name",
      "type"
    ],
    "property_order": [
      "name",
      "type"
    ],
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
    }
  },
  "ManifestResponse": {
    "required": [
      "table"
    ],
    "property_order": [
      "table",
      "schema",
      "columns",
      "files",
      "row_filters",
      "column_masks",
      "expires_at"
    ],
    "properties": {
      "table": {
        "schema": {
          "type": "string"
        }
      },
      "schema": {
        "schema": {
          "type": "string"
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
      "column_masks": {
        "schema": {
          "type": "object",
          "additional_properties": {
            "schema": {
              "type": "string"
            }
          }
        }
      },
      "expires_at": {
        "schema": {
          "type": "string",
          "format": "date-time"
        }
      }
    }
  },
  "MetastoreSummary": {
    "required": [
      "catalog_name"
    ],
    "property_order": [
      "catalog_name",
      "metastore_type",
      "storage_backend",
      "data_path",
      "schema_count",
      "table_count"
    ],
    "properties": {
      "catalog_name": {
        "schema": {
          "type": "string"
        }
      },
      "metastore_type": {
        "schema": {
          "type": "string"
        }
      },
      "storage_backend": {
        "schema": {
          "type": "string"
        }
      },
      "data_path": {
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
      "table_count": {
        "schema": {
          "type": "integer",
          "format": "int32"
        }
      }
    }
  },
  "MetastoreType": {
    "required": []
  },
  "MetricFreshnessStatus": {
    "required": [],
    "property_order": [
      "metric_name",
      "semantic_model_id",
      "semantic_model_name",
      "freshness_status",
      "freshness_basis",
      "selected_pre_aggregation",
      "checked_at"
    ],
    "properties": {
      "metric_name": {
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
      },
      "freshness_status": {
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
      "selected_pre_aggregation": {
        "schema": {
          "type": "string"
        }
      },
      "checked_at": {
        "schema": {
          "type": "string",
          "format": "date-time"
        }
      }
    }
  },
  "MetricQueryExplainResponse": {
    "required": [],
    "property_order": [
      "plan"
    ],
    "properties": {
      "plan": {
        "schema": {
          "ref": "MetricQueryPlan"
        }
      }
    }
  },
  "MetricQueryJoinStep": {
    "required": [],
    "property_order": [
      "relationship_name",
      "from_model",
      "to_model",
      "relationship_type",
      "join_sql"
    ],
    "properties": {
      "relationship_name": {
        "schema": {
          "type": "string"
        }
      },
      "from_model": {
        "schema": {
          "type": "string"
        }
      },
      "to_model": {
        "schema": {
          "type": "string"
        }
      },
      "relationship_type": {
        "schema": {
          "type": "string"
        }
      },
      "join_sql": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "MetricQueryPlan": {
    "required": [],
    "property_order": [
      "base_model_name",
      "base_relation",
      "metrics",
      "dimensions",
      "time_grain",
      "join_path",
      "selected_pre_aggregation",
      "generated_sql",
      "freshness_status",
      "freshness_basis"
    ],
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
      "metrics": {
        "schema": {
          "type": "array",
          "items": {
            "type": "string"
          }
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
      "time_grain": {
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
      "selected_pre_aggregation": {
        "schema": {
          "type": "string"
        }
      },
      "generated_sql": {
        "schema": {
          "type": "string"
        }
      },
      "freshness_status": {
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
      }
    }
  },
  "MetricQueryRequest": {
    "required": [
      "metrics"
    ],
    "property_order": [
      "metrics",
      "relationship_names",
      "dimensions",
      "filters",
      "order_by",
      "limit",
      "time_grain"
    ],
    "properties": {
      "metrics": {
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
      "order_by": {
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
      "time_grain": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "MetricQueryRunResponse": {
    "required": [],
    "property_order": [
      "plan",
      "result"
    ],
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
    "required": [],
    "property_order": [
      "id",
      "project_name",
      "name",
      "sql",
      "materialization",
      "description",
      "owner",
      "depends_on",
      "tags",
      "config",
      "contract",
      "freshness_policy",
      "created_by",
      "created_at",
      "updated_at"
    ],
    "properties": {
      "id": {
        "schema": {
          "type": "string"
        }
      },
      "project_name": {
        "schema": {
          "type": "string"
        }
      },
      "name": {
        "schema": {
          "type": "string"
        }
      },
      "sql": {
        "schema": {
          "type": "string"
        }
      },
      "materialization": {
        "schema": {
          "ref": "ModelMaterialization"
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
      "depends_on": {
        "schema": {
          "type": "array",
          "items": {
            "type": "string"
          }
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
      "freshness_policy": {
        "schema": {
          "ref": "FreshnessPolicy"
        }
      },
      "created_by": {
        "schema": {
          "type": "string"
        }
      },
      "created_at": {
        "schema": {
          "type": "string",
          "format": "date-time"
        }
      },
      "updated_at": {
        "schema": {
          "type": "string",
          "format": "date-time"
        }
      }
    }
  },
  "ModelConfig": {
    "required": [],
    "property_order": [
      "unique_key",
      "incremental_strategy",
      "on_schema_change"
    ],
    "properties": {
      "unique_key": {
        "schema": {
          "type": "array",
          "items": {
            "type": "string"
          }
        }
      },
      "incremental_strategy": {
        "schema": {
          "type": "string"
        }
      },
      "on_schema_change": {
        "schema": {
          "ref": "ModelConfigOnSchemaChange"
        }
      }
    }
  },
  "ModelConfigOnSchemaChange": {
    "required": []
  },
  "ModelContract": {
    "required": [],
    "property_order": [
      "enforce",
      "columns"
    ],
    "properties": {
      "enforce": {
        "schema": {
          "type": "boolean"
        }
      },
      "columns": {
        "schema": {
          "type": "array",
          "items": {
            "ref": "ModelContractColumn"
          }
        }
      }
    }
  },
  "ModelContractColumn": {
    "required": [
      "name",
      "type"
    ],
    "property_order": [
      "name",
      "type",
      "nullable"
    ],
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
      },
      "nullable": {
        "schema": {
          "type": "boolean"
        }
      }
    }
  },
  "ModelDAG": {
    "required": [],
    "property_order": [
      "tiers"
    ],
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
    "required": [],
    "property_order": [
      "project_name",
      "model_name",
      "materialization",
      "depends_on"
    ],
    "properties": {
      "project_name": {
        "schema": {
          "type": "string"
        }
      },
      "model_name": {
        "schema": {
          "type": "string"
        }
      },
      "materialization": {
        "schema": {
          "ref": "ModelMaterialization"
        }
      },
      "depends_on": {
        "schema": {
          "type": "array",
          "items": {
            "type": "string"
          }
        }
      }
    }
  },
  "ModelDAGTier": {
    "required": [],
    "property_order": [
      "tier",
      "nodes"
    ],
    "properties": {
      "tier": {
        "schema": {
          "type": "integer",
          "format": "int32"
        }
      },
      "nodes": {
        "schema": {
          "type": "array",
          "items": {
            "ref": "ModelDAGNode"
          }
        }
      }
    }
  },
  "ModelMaterialization": {
    "required": []
  },
  "ModelRun": {
    "required": [],
    "property_order": [
      "id",
      "status",
      "trigger_type",
      "triggered_by",
      "project_name",
      "environment_name",
      "build_id",
      "model_names",
      "full_refresh",
      "compile_manifest",
      "compile_diagnostics",
      "started_at",
      "finished_at",
      "error_message",
      "created_at"
    ],
    "properties": {
      "id": {
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
      },
      "project_name": {
        "schema": {
          "type": "string"
        }
      },
      "environment_name": {
        "schema": {
          "type": "string"
        }
      },
      "build_id": {
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
      "full_refresh": {
        "schema": {
          "type": "boolean"
        }
      },
      "compile_manifest": {
        "schema": {
          "type": "string"
        }
      },
      "compile_diagnostics": {
        "schema": {
          "ref": "ModelRunCompileDiagnostics"
        }
      },
      "started_at": {
        "schema": {
          "type": "string",
          "format": "date-time"
        }
      },
      "finished_at": {
        "schema": {
          "type": "string",
          "format": "date-time"
        }
      },
      "error_message": {
        "schema": {
          "type": "string"
        }
      },
      "created_at": {
        "schema": {
          "type": "string",
          "format": "date-time"
        }
      }
    }
  },
  "ModelRunCompileDiagnostics": {
    "required": [],
    "property_order": [
      "warnings",
      "errors"
    ],
    "properties": {
      "warnings": {
        "schema": {
          "type": "array",
          "items": {
            "type": "string"
          }
        }
      },
      "errors": {
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
    "required": [],
    "property_order": [
      "id",
      "run_id",
      "model_name",
      "compiled_sql",
      "compiled_hash",
      "depends_on",
      "vars_used",
      "macros_used",
      "status",
      "rows_affected",
      "started_at",
      "finished_at",
      "error_message",
      "created_at"
    ],
    "properties": {
      "id": {
        "schema": {
          "type": "string"
        }
      },
      "run_id": {
        "schema": {
          "type": "string"
        }
      },
      "model_name": {
        "schema": {
          "type": "string"
        }
      },
      "compiled_sql": {
        "schema": {
          "type": "string"
        }
      },
      "compiled_hash": {
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
      "vars_used": {
        "schema": {
          "type": "array",
          "items": {
            "type": "string"
          }
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
      "status": {
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
      "started_at": {
        "schema": {
          "type": "string",
          "format": "date-time"
        }
      },
      "finished_at": {
        "schema": {
          "type": "string",
          "format": "date-time"
        }
      },
      "error_message": {
        "schema": {
          "type": "string"
        }
      },
      "created_at": {
        "schema": {
          "type": "string",
          "format": "date-time"
        }
      }
    }
  },
  "ModelRunStepList": {
    "required": [
      "data"
    ],
    "property_order": [
      "data"
    ],
    "properties": {
      "data": {
        "schema": {
          "type": "array",
          "items": {
            "ref": "ModelRunStep"
          }
        }
      }
    }
  },
  "ModelTest": {
    "required": [],
    "property_order": [
      "id",
      "model_id",
      "name",
      "test_type",
      "column",
      "config",
      "created_at"
    ],
    "properties": {
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
      },
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
          "type": "string",
          "format": "date-time"
        }
      }
    }
  },
  "ModelTestConfig": {
    "required": [],
    "property_order": [
      "values",
      "to_model",
      "to_column",
      "custom_sql"
    ],
    "properties": {
      "values": {
        "schema": {
          "type": "array",
          "items": {
            "type": "string"
          }
        }
      },
      "to_model": {
        "schema": {
          "type": "string"
        }
      },
      "to_column": {
        "schema": {
          "type": "string"
        }
      },
      "custom_sql": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "ModelTestList": {
    "required": [
      "data"
    ],
    "property_order": [
      "data"
    ],
    "properties": {
      "data": {
        "schema": {
          "type": "array",
          "items": {
            "ref": "ModelTest"
          }
        }
      }
    }
  },
  "ModelTestResult": {
    "required": [],
    "property_order": [
      "id",
      "run_step_id",
      "test_id",
      "test_name",
      "status",
      "rows_returned",
      "error_message",
      "created_at"
    ],
    "properties": {
      "id": {
        "schema": {
          "type": "string"
        }
      },
      "run_step_id": {
        "schema": {
          "type": "string"
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
      },
      "status": {
        "schema": {
          "ref": "ModelTestResultStatus"
        }
      },
      "rows_returned": {
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
      "created_at": {
        "schema": {
          "type": "string",
          "format": "date-time"
        }
      }
    }
  },
  "ModelTestResultList": {
    "required": [
      "data"
    ],
    "property_order": [
      "data"
    ],
    "properties": {
      "data": {
        "schema": {
          "type": "array",
          "items": {
            "ref": "ModelTestResult"
          }
        }
      }
    }
  },
  "ModelTestResultStatus": {
    "required": []
  },
  "ModelTestTestType": {
    "required": []
  },
  "MoveFolderRequest": {
    "required": [],
    "property_order": [
      "parent_folder_id",
      "confirm_leave_git",
      "confirm_context_change"
    ],
    "properties": {
      "parent_folder_id": {
        "schema": {
          "type": "string"
        }
      },
      "confirm_leave_git": {
        "schema": {
          "type": "boolean"
        }
      },
      "confirm_context_change": {
        "schema": {
          "type": "boolean"
        }
      }
    }
  },
  "MoveNotebookRequest": {
    "required": [
      "folder_id"
    ],
    "property_order": [
      "folder_id",
      "git_path",
      "confirm_leave_git",
      "confirm_context_change"
    ],
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
      "confirm_leave_git": {
        "schema": {
          "type": "boolean"
        }
      },
      "confirm_context_change": {
        "schema": {
          "type": "boolean"
        }
      }
    }
  },
  "Notebook": {
    "required": [],
    "property_order": [
      "id",
      "folder_id",
      "name",
      "description",
      "owner",
      "git_repo_id",
      "git_path",
      "project_override_id",
      "environment_override_id",
      "created_at",
      "updated_at"
    ],
    "properties": {
      "id": {
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
      "git_repo_id": {
        "schema": {
          "type": "string"
        }
      },
      "git_path": {
        "schema": {
          "type": "string"
        }
      },
      "project_override_id": {
        "schema": {
          "type": "string"
        }
      },
      "environment_override_id": {
        "schema": {
          "type": "string"
        }
      },
      "created_at": {
        "schema": {
          "type": "string",
          "format": "date-time"
        }
      },
      "updated_at": {
        "schema": {
          "type": "string",
          "format": "date-time"
        }
      }
    }
  },
  "NotebookCellTestConfig": {
    "required": [],
    "property_order": [
      "severity"
    ],
    "properties": {
      "severity": {
        "schema": {
          "ref": "NotebookTestSeverity"
        }
      }
    }
  },
  "NotebookContext": {
    "required": [],
    "property_order": [
      "notebook_id",
      "folder_id",
      "workspace_id",
      "effective_project_id",
      "effective_environment_id",
      "effective_git_repo_id",
      "effective_git_root_path",
      "project_source_folder_id",
      "environment_source_id",
      "git_source_folder_id"
    ],
    "properties": {
      "notebook_id": {
        "schema": {
          "type": "string"
        }
      },
      "folder_id": {
        "schema": {
          "type": "string"
        }
      },
      "workspace_id": {
        "schema": {
          "type": "string"
        }
      },
      "effective_project_id": {
        "schema": {
          "type": "string"
        }
      },
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
      "project_source_folder_id": {
        "schema": {
          "type": "string"
        }
      },
      "environment_source_id": {
        "schema": {
          "type": "string"
        }
      },
      "git_source_folder_id": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "NotebookDetail": {
    "required": [],
    "property_order": [
      "notebook",
      "cells",
      "context",
      "shares",
      "publish_model"
    ],
    "properties": {
      "notebook": {
        "schema": {
          "ref": "Notebook"
        }
      },
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
      "shares": {
        "schema": {
          "type": "array",
          "items": {
            "ref": "NotebookShare"
          }
        }
      },
      "publish_model": {
        "schema": {
          "ref": "NotebookPublishModel"
        }
      }
    }
  },
  "NotebookJob": {
    "required": [],
    "property_order": [
      "id",
      "notebook_id",
      "session_id",
      "state",
      "result",
      "error",
      "created_at",
      "updated_at"
    ],
    "properties": {
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
      "result": {
        "schema": {
          "type": "string"
        }
      },
      "error": {
        "schema": {
          "type": "string"
        }
      },
      "created_at": {
        "schema": {
          "type": "string",
          "format": "date-time"
        }
      },
      "updated_at": {
        "schema": {
          "type": "string",
          "format": "date-time"
        }
      }
    }
  },
  "NotebookJobState": {
    "required": []
  },
  "NotebookPublishModel": {
    "required": [],
    "property_order": [
      "project_name",
      "name",
      "materialization",
      "output_cell_id"
    ],
    "properties": {
      "project_name": {
        "schema": {
          "type": "string"
        }
      },
      "name": {
        "schema": {
          "type": "string"
        }
      },
      "materialization": {
        "schema": {
          "ref": "ModelMaterialization"
        }
      },
      "output_cell_id": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "NotebookSession": {
    "required": [],
    "property_order": [
      "id",
      "notebook_id",
      "principal",
      "state",
      "created_at",
      "last_used_at"
    ],
    "properties": {
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
      "principal": {
        "schema": {
          "type": "string"
        }
      },
      "state": {
        "schema": {
          "ref": "NotebookSessionState"
        }
      },
      "created_at": {
        "schema": {
          "type": "string",
          "format": "date-time"
        }
      },
      "last_used_at": {
        "schema": {
          "type": "string",
          "format": "date-time"
        }
      }
    }
  },
  "NotebookSessionState": {
    "required": []
  },
  "NotebookShare": {
    "required": [],
    "property_order": [
      "principal_name",
      "role"
    ],
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
    "required": []
  },
  "NotebookTestSeverity": {
    "required": []
  },
  "OIDCProviderRequest": {
    "required": [
      "enabled"
    ],
    "property_order": [
      "enabled",
      "issuer_url",
      "jwks_url",
      "audience",
      "client_id",
      "client_secret",
      "scopes"
    ],
    "properties": {
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
      "scopes": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "OIDCProviderResponse": {
    "required": [
      "enabled",
      "secret_stored"
    ],
    "property_order": [
      "enabled",
      "issuer_url",
      "jwks_url",
      "audience",
      "client_id",
      "scopes",
      "updated_at",
      "secret_stored"
    ],
    "properties": {
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
      "scopes": {
        "schema": {
          "type": "string"
        }
      },
      "updated_at": {
        "schema": {
          "type": "string",
          "format": "date-time"
        }
      },
      "secret_stored": {
        "schema": {
          "type": "boolean"
        }
      }
    }
  },
  "OrphanResource": {
    "required": [
      "resource_type",
      "resource_id",
      "resource_name"
    ],
    "property_order": [
      "resource_type",
      "resource_id",
      "resource_name"
    ],
    "properties": {
      "resource_type": {
        "schema": {
          "type": "string"
        }
      },
      "resource_id": {
        "schema": {
          "type": "string"
        }
      },
      "resource_name": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "PaginatedAPIKeys": {
    "required": [
      "data"
    ],
    "property_order": [
      "data",
      "next_page_token"
    ],
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
    }
  },
  "PaginatedAssetCheckResults": {
    "required": [
      "data"
    ],
    "property_order": [
      "data",
      "next_page_token"
    ],
    "properties": {
      "data": {
        "schema": {
          "type": "array",
          "items": {
            "ref": "AssetCheckResult"
          }
        }
      },
      "next_page_token": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "PaginatedAssetMaterializations": {
    "required": [
      "data"
    ],
    "property_order": [
      "data",
      "next_page_token"
    ],
    "properties": {
      "data": {
        "schema": {
          "type": "array",
          "items": {
            "ref": "AssetMaterialization"
          }
        }
      },
      "next_page_token": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "PaginatedAssetPartitions": {
    "required": [
      "data"
    ],
    "property_order": [
      "data",
      "next_page_token"
    ],
    "properties": {
      "data": {
        "schema": {
          "type": "array",
          "items": {
            "ref": "AssetPartition"
          }
        }
      },
      "next_page_token": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "PaginatedAssetRuns": {
    "required": [
      "data"
    ],
    "property_order": [
      "data",
      "next_page_token"
    ],
    "properties": {
      "data": {
        "schema": {
          "type": "array",
          "items": {
            "ref": "AssetRun"
          }
        }
      },
      "next_page_token": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "PaginatedAssets": {
    "required": [
      "data"
    ],
    "property_order": [
      "data",
      "next_page_token"
    ],
    "properties": {
      "data": {
        "schema": {
          "type": "array",
          "items": {
            "ref": "Asset"
          }
        }
      },
      "next_page_token": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "PaginatedAuditLogs": {
    "required": [
      "data"
    ],
    "property_order": [
      "data",
      "next_page_token"
    ],
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
    }
  },
  "PaginatedBackfillRequests": {
    "required": [
      "data"
    ],
    "property_order": [
      "data",
      "next_page_token"
    ],
    "properties": {
      "data": {
        "schema": {
          "type": "array",
          "items": {
            "ref": "BackfillRequest"
          }
        }
      },
      "next_page_token": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "PaginatedBuilds": {
    "required": [
      "data"
    ],
    "property_order": [
      "data",
      "next_page_token"
    ],
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
    }
  },
  "PaginatedColumnDetails": {
    "required": [
      "data"
    ],
    "property_order": [
      "data",
      "next_page_token"
    ],
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
    }
  },
  "PaginatedColumnMaskBindings": {
    "required": [
      "data"
    ],
    "property_order": [
      "data",
      "next_page_token"
    ],
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
    }
  },
  "PaginatedColumnMasks": {
    "required": [
      "data"
    ],
    "property_order": [
      "data",
      "next_page_token"
    ],
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
    }
  },
  "PaginatedComputeAssignments": {
    "required": [
      "data"
    ],
    "property_order": [
      "data",
      "next_page_token"
    ],
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
    }
  },
  "PaginatedComputeEndpoints": {
    "required": [
      "data"
    ],
    "property_order": [
      "data",
      "next_page_token"
    ],
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
    }
  },
  "PaginatedDashboards": {
    "required": [
      "data"
    ],
    "property_order": [
      "data",
      "next_page_token"
    ],
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
    }
  },
  "PaginatedDataProducts": {
    "required": [
      "data"
    ],
    "property_order": [
      "data",
      "next_page_token"
    ],
    "properties": {
      "data": {
        "schema": {
          "type": "array",
          "items": {
            "ref": "DataProductListItem"
          }
        }
      },
      "next_page_token": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "PaginatedEnvironments": {
    "required": [
      "data"
    ],
    "property_order": [
      "data",
      "next_page_token"
    ],
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
    }
  },
  "PaginatedExternalLocations": {
    "required": [
      "data"
    ],
    "property_order": [
      "data",
      "next_page_token"
    ],
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
    }
  },
  "PaginatedFolderContents": {
    "required": [
      "data"
    ],
    "property_order": [
      "data",
      "next_page_token"
    ],
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
    }
  },
  "PaginatedFolders": {
    "required": [
      "data"
    ],
    "property_order": [
      "data",
      "next_page_token"
    ],
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
    }
  },
  "PaginatedGitRepos": {
    "required": [
      "data"
    ],
    "property_order": [
      "data",
      "next_page_token"
    ],
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
    }
  },
  "PaginatedGrants": {
    "required": [
      "data"
    ],
    "property_order": [
      "data",
      "next_page_token"
    ],
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
    }
  },
  "PaginatedGroupMembers": {
    "required": [
      "data"
    ],
    "property_order": [
      "data",
      "next_page_token"
    ],
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
    }
  },
  "PaginatedGroups": {
    "required": [
      "data"
    ],
    "property_order": [
      "data",
      "next_page_token"
    ],
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
    }
  },
  "PaginatedMacros": {
    "required": [
      "data"
    ],
    "property_order": [
      "data",
      "next_page_token"
    ],
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
    }
  },
  "PaginatedModelRuns": {
    "required": [
      "data"
    ],
    "property_order": [
      "data",
      "next_page_token"
    ],
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
    }
  },
  "PaginatedModels": {
    "required": [
      "data"
    ],
    "property_order": [
      "data",
      "next_page_token"
    ],
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
    }
  },
  "PaginatedNotebookJobs": {
    "required": [
      "data"
    ],
    "property_order": [
      "data",
      "next_page_token"
    ],
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
    }
  },
  "PaginatedNotebooks": {
    "required": [
      "data"
    ],
    "property_order": [
      "data",
      "next_page_token"
    ],
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
    }
  },
  "PaginatedPipelineRuns": {
    "required": [
      "data"
    ],
    "property_order": [
      "data",
      "next_page_token"
    ],
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
    }
  },
  "PaginatedPipelines": {
    "required": [
      "data"
    ],
    "property_order": [
      "data",
      "next_page_token"
    ],
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
    }
  },
  "PaginatedPrincipals": {
    "required": [
      "data"
    ],
    "property_order": [
      "data",
      "next_page_token"
    ],
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
    }
  },
  "PaginatedProductDomains": {
    "required": [
      "data"
    ],
    "property_order": [
      "data",
      "next_page_token"
    ],
    "properties": {
      "data": {
        "schema": {
          "type": "array",
          "items": {
            "ref": "ProductDomain"
          }
        }
      },
      "next_page_token": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "PaginatedProductTeams": {
    "required": [
      "data"
    ],
    "property_order": [
      "data",
      "next_page_token"
    ],
    "properties": {
      "data": {
        "schema": {
          "type": "array",
          "items": {
            "ref": "ProductTeam"
          }
        }
      },
      "next_page_token": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "PaginatedProjects": {
    "required": [
      "data"
    ],
    "property_order": [
      "data",
      "next_page_token"
    ],
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
    }
  },
  "PaginatedQueryHistoryEntries": {
    "required": [
      "data"
    ],
    "property_order": [
      "data",
      "next_page_token"
    ],
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
    }
  },
  "PaginatedQueryJobs": {
    "required": [
      "data"
    ],
    "property_order": [
      "data",
      "next_page_token"
    ],
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
    }
  },
  "PaginatedRecentResources": {
    "required": [
      "data"
    ],
    "property_order": [
      "data",
      "next_page_token"
    ],
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
    }
  },
  "PaginatedRowFilterBindings": {
    "required": [
      "data"
    ],
    "property_order": [
      "data",
      "next_page_token"
    ],
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
    }
  },
  "PaginatedRowFilters": {
    "required": [
      "data"
    ],
    "property_order": [
      "data",
      "next_page_token"
    ],
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
    }
  },
  "PaginatedSavedResources": {
    "required": [
      "data"
    ],
    "property_order": [
      "data",
      "next_page_token"
    ],
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
    }
  },
  "PaginatedSchemaDetails": {
    "required": [
      "data"
    ],
    "property_order": [
      "data",
      "next_page_token"
    ],
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
    }
  },
  "PaginatedSearchResults": {
    "required": [
      "data"
    ],
    "property_order": [
      "data",
      "next_page_token"
    ],
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
    }
  },
  "PaginatedSemanticModels": {
    "required": [
      "data"
    ],
    "property_order": [
      "data",
      "next_page_token"
    ],
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
    }
  },
  "PaginatedSemanticRelationships": {
    "required": [
      "data"
    ],
    "property_order": [
      "data",
      "next_page_token"
    ],
    "properties": {
      "data": {
        "schema": {
          "type": "array",
          "items": {
            "ref": "SemanticRelationship"
          }
        }
      },
      "next_page_token": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "PaginatedStorageCredentials": {
    "required": [
      "data"
    ],
    "property_order": [
      "data",
      "next_page_token"
    ],
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
    }
  },
  "PaginatedTableDetails": {
    "required": [
      "data"
    ],
    "property_order": [
      "data",
      "next_page_token"
    ],
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
    }
  },
  "PaginatedTagAssignments": {
    "required": [
      "data"
    ],
    "property_order": [
      "data",
      "next_page_token"
    ],
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
    }
  },
  "PaginatedTags": {
    "required": [
      "data"
    ],
    "property_order": [
      "data",
      "next_page_token"
    ],
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
    }
  },
  "PaginatedViewDetails": {
    "required": [
      "data"
    ],
    "property_order": [
      "data",
      "next_page_token"
    ],
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
    }
  },
  "PaginatedVolumes": {
    "required": [
      "data"
    ],
    "property_order": [
      "data",
      "next_page_token"
    ],
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
    }
  },
  "PaginatedWorkspaces": {
    "required": [
      "data"
    ],
    "property_order": [
      "data",
      "next_page_token"
    ],
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
    }
  },
  "Pipeline": {
    "required": [],
    "property_order": [
      "id",
      "name",
      "description",
      "schedule_cron",
      "is_paused",
      "concurrency_limit",
      "created_by",
      "folder_id",
      "created_at",
      "updated_at"
    ],
    "properties": {
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
      "description": {
        "schema": {
          "type": "string"
        }
      },
      "schedule_cron": {
        "schema": {
          "type": "string"
        }
      },
      "is_paused": {
        "schema": {
          "type": "boolean"
        }
      },
      "concurrency_limit": {
        "schema": {
          "type": "integer",
          "format": "int32"
        }
      },
      "created_by": {
        "schema": {
          "type": "string"
        }
      },
      "folder_id": {
        "schema": {
          "type": "string"
        }
      },
      "created_at": {
        "schema": {
          "type": "string",
          "format": "date-time"
        }
      },
      "updated_at": {
        "schema": {
          "type": "string",
          "format": "date-time"
        }
      }
    }
  },
  "PipelineJob": {
    "required": [],
    "property_order": [
      "id",
      "pipeline_id",
      "name",
      "notebook_id",
      "compute_endpoint_id",
      "depends_on",
      "timeout_seconds",
      "retry_count",
      "job_order",
      "job_type",
      "model_selector",
      "created_at"
    ],
    "properties": {
      "id": {
        "schema": {
          "type": "string"
        }
      },
      "pipeline_id": {
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
      "timeout_seconds": {
        "schema": {
          "type": "integer",
          "format": "int64"
        }
      },
      "retry_count": {
        "schema": {
          "type": "integer",
          "format": "int32"
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
      "created_at": {
        "schema": {
          "type": "string",
          "format": "date-time"
        }
      }
    }
  },
  "PipelineJobJobType": {
    "required": []
  },
  "PipelineJobList": {
    "required": [
      "data"
    ],
    "property_order": [
      "data"
    ],
    "properties": {
      "data": {
        "schema": {
          "type": "array",
          "items": {
            "ref": "PipelineJob"
          }
        }
      }
    }
  },
  "PipelineJobRun": {
    "required": [],
    "property_order": [
      "id",
      "run_id",
      "job_id",
      "job_name",
      "status",
      "started_at",
      "finished_at",
      "error_message",
      "retry_attempt",
      "created_at"
    ],
    "properties": {
      "id": {
        "schema": {
          "type": "string"
        }
      },
      "run_id": {
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
      "status": {
        "schema": {
          "ref": "PipelineJobRunStatus"
        }
      },
      "started_at": {
        "schema": {
          "type": "string",
          "format": "date-time"
        }
      },
      "finished_at": {
        "schema": {
          "type": "string",
          "format": "date-time"
        }
      },
      "error_message": {
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
      "created_at": {
        "schema": {
          "type": "string",
          "format": "date-time"
        }
      }
    }
  },
  "PipelineJobRunList": {
    "required": [
      "data"
    ],
    "property_order": [
      "data"
    ],
    "properties": {
      "data": {
        "schema": {
          "type": "array",
          "items": {
            "ref": "PipelineJobRun"
          }
        }
      }
    }
  },
  "PipelineJobRunStatus": {
    "required": []
  },
  "PipelineRun": {
    "required": [],
    "property_order": [
      "id",
      "pipeline_id",
      "status",
      "trigger_type",
      "triggered_by",
      "parameters",
      "git_commit_hash",
      "started_at",
      "finished_at",
      "error_message",
      "created_at"
    ],
    "properties": {
      "id": {
        "schema": {
          "type": "string"
        }
      },
      "pipeline_id": {
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
      },
      "parameters": {
        "schema": {
          "type": "object",
          "additional_properties": {
            "schema": {
              "type": "string"
            }
          }
        }
      },
      "git_commit_hash": {
        "schema": {
          "type": "string"
        }
      },
      "started_at": {
        "schema": {
          "type": "string",
          "format": "date-time"
        }
      },
      "finished_at": {
        "schema": {
          "type": "string",
          "format": "date-time"
        }
      },
      "error_message": {
        "schema": {
          "type": "string"
        }
      },
      "created_at": {
        "schema": {
          "type": "string",
          "format": "date-time"
        }
      }
    }
  },
  "PipelineRunStatus": {
    "required": []
  },
  "PipelineRunTriggerType": {
    "required": []
  },
  "Principal": {
    "title": "Authenticated principal.",
    "required": [
      "id",
      "name",
      "type",
      "is_admin"
    ],
    "property_order": [
      "id",
      "name",
      "type",
      "is_admin",
      "created_at"
    ],
    "properties": {
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
      "type": {
        "schema": {
          "ref": "PrincipalType"
        }
      },
      "is_admin": {
        "schema": {
          "type": "boolean"
        }
      },
      "created_at": {
        "schema": {
          "type": "string",
          "format": "date-time"
        }
      }
    }
  },
  "PrincipalType": {
    "required": []
  },
  "PrivilegeGrant": {
    "required": [
      "id",
      "principal_id",
      "principal_type",
      "securable_type",
      "securable_id",
      "privilege"
    ],
    "property_order": [
      "id",
      "principal_id",
      "principal_type",
      "securable_type",
      "securable_id",
      "privilege",
      "granted_by",
      "granted_at"
    ],
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
      "securable_type": {
        "schema": {
          "type": "string"
        }
      },
      "securable_id": {
        "schema": {
          "type": "string"
        }
      },
      "privilege": {
        "schema": {
          "type": "string"
        }
      },
      "granted_by": {
        "schema": {
          "type": "string"
        }
      },
      "granted_at": {
        "schema": {
          "type": "string",
          "format": "date-time"
        }
      }
    }
  },
  "ProductAdoptionSummary": {
    "required": [
      "product_id",
      "product_slug",
      "product_name",
      "domain_name",
      "team_name",
      "subscriber_count",
      "downstream_product_count",
      "output_count",
      "semantic_entrypoint_count",
      "adoption_score"
    ],
    "property_order": [
      "product_id",
      "product_slug",
      "product_name",
      "domain_name",
      "team_name",
      "subscriber_count",
      "downstream_product_count",
      "output_count",
      "semantic_entrypoint_count",
      "adoption_score"
    ],
    "properties": {
      "product_id": {
        "schema": {
          "type": "string"
        }
      },
      "product_slug": {
        "schema": {
          "type": "string"
        }
      },
      "product_name": {
        "schema": {
          "type": "string"
        }
      },
      "domain_name": {
        "schema": {
          "type": "string"
        }
      },
      "team_name": {
        "schema": {
          "type": "string"
        }
      },
      "subscriber_count": {
        "schema": {
          "type": "integer",
          "format": "int64"
        }
      },
      "downstream_product_count": {
        "schema": {
          "type": "integer",
          "format": "int64"
        }
      },
      "output_count": {
        "schema": {
          "type": "integer",
          "format": "int64"
        }
      },
      "semantic_entrypoint_count": {
        "schema": {
          "type": "integer",
          "format": "int64"
        }
      },
      "adoption_score": {
        "schema": {
          "type": "integer",
          "format": "int64"
        }
      }
    }
  },
  "ProductContract": {
    "required": [],
    "property_order": [
      "data_grain",
      "primary_keys",
      "join_keys",
      "dimensions",
      "measures",
      "retention_window",
      "update_cadence",
      "quality_expectations",
      "breaking_change_policy",
      "sample_queries"
    ],
    "properties": {
      "data_grain": {
        "schema": {
          "type": "string"
        }
      },
      "primary_keys": {
        "schema": {
          "type": "array",
          "items": {
            "type": "string"
          }
        }
      },
      "join_keys": {
        "schema": {
          "type": "array",
          "items": {
            "type": "string"
          }
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
      "measures": {
        "schema": {
          "type": "array",
          "items": {
            "type": "string"
          }
        }
      },
      "retention_window": {
        "schema": {
          "type": "string"
        }
      },
      "update_cadence": {
        "schema": {
          "type": "string"
        }
      },
      "quality_expectations": {
        "schema": {
          "type": "array",
          "items": {
            "type": "string"
          }
        }
      },
      "breaking_change_policy": {
        "schema": {
          "type": "string"
        }
      },
      "sample_queries": {
        "schema": {
          "type": "array",
          "items": {
            "type": "string"
          }
        }
      }
    }
  },
  "ProductDependencyList": {
    "required": [
      "data"
    ],
    "property_order": [
      "data"
    ],
    "properties": {
      "data": {
        "schema": {
          "type": "array",
          "items": {
            "ref": "DataProductListItem"
          }
        }
      }
    }
  },
  "ProductDomain": {
    "required": [
      "id",
      "name",
      "description"
    ],
    "property_order": [
      "id",
      "name",
      "description",
      "created_at",
      "updated_at"
    ],
    "properties": {
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
      "description": {
        "schema": {
          "type": "string"
        }
      },
      "created_at": {
        "schema": {
          "type": "string",
          "format": "date-time"
        }
      },
      "updated_at": {
        "schema": {
          "type": "string",
          "format": "date-time"
        }
      }
    }
  },
  "ProductEvent": {
    "required": [
      "id",
      "product_id",
      "event_type",
      "title",
      "description"
    ],
    "property_order": [
      "id",
      "product_id",
      "event_type",
      "title",
      "description",
      "metadata",
      "created_at"
    ],
    "properties": {
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
      "event_type": {
        "schema": {
          "type": "string"
        }
      },
      "title": {
        "schema": {
          "type": "string"
        }
      },
      "description": {
        "schema": {
          "type": "string"
        }
      },
      "metadata": {
        "schema": {
          "type": "object",
          "additional_properties": {
            "any": true
          }
        }
      },
      "created_at": {
        "schema": {
          "type": "string",
          "format": "date-time"
        }
      }
    }
  },
  "ProductEventList": {
    "required": [
      "data"
    ],
    "property_order": [
      "data",
      "next_page_token"
    ],
    "properties": {
      "data": {
        "schema": {
          "type": "array",
          "items": {
            "ref": "ProductEvent"
          }
        }
      },
      "next_page_token": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "ProductOutput": {
    "required": [
      "id",
      "product_version_id",
      "asset_id",
      "asset_key",
      "asset_type",
      "is_primary"
    ],
    "property_order": [
      "id",
      "product_version_id",
      "asset_id",
      "asset_key",
      "asset_type",
      "is_primary",
      "created_at"
    ],
    "properties": {
      "id": {
        "schema": {
          "type": "string"
        }
      },
      "product_version_id": {
        "schema": {
          "type": "string"
        }
      },
      "asset_id": {
        "schema": {
          "type": "string"
        }
      },
      "asset_key": {
        "schema": {
          "type": "string"
        }
      },
      "asset_type": {
        "schema": {
          "type": "string"
        }
      },
      "is_primary": {
        "schema": {
          "type": "boolean"
        }
      },
      "created_at": {
        "schema": {
          "type": "string",
          "format": "date-time"
        }
      }
    }
  },
  "ProductOutputList": {
    "required": [
      "data"
    ],
    "property_order": [
      "data"
    ],
    "properties": {
      "data": {
        "schema": {
          "type": "array",
          "items": {
            "ref": "ProductOutput"
          }
        }
      }
    }
  },
  "ProductPortfolioGroup": {
    "required": [
      "name",
      "product_count",
      "published_count",
      "certified_count",
      "average_completeness_pct"
    ],
    "property_order": [
      "name",
      "product_count",
      "published_count",
      "certified_count",
      "average_completeness_pct"
    ],
    "properties": {
      "name": {
        "schema": {
          "type": "string"
        }
      },
      "product_count": {
        "schema": {
          "type": "integer",
          "format": "int64"
        }
      },
      "published_count": {
        "schema": {
          "type": "integer",
          "format": "int64"
        }
      },
      "certified_count": {
        "schema": {
          "type": "integer",
          "format": "int64"
        }
      },
      "average_completeness_pct": {
        "schema": {
          "type": "integer",
          "format": "int32"
        }
      }
    }
  },
  "ProductPortfolioReport": {
    "required": [
      "top_used",
      "least_adopted",
      "high_blast_radius",
      "domain_scorecards",
      "team_scorecards",
      "orphan_assets",
      "orphan_semantic_models"
    ],
    "property_order": [
      "top_used",
      "least_adopted",
      "high_blast_radius",
      "domain_scorecards",
      "team_scorecards",
      "orphan_assets",
      "orphan_semantic_models"
    ],
    "properties": {
      "top_used": {
        "schema": {
          "type": "array",
          "items": {
            "ref": "ProductAdoptionSummary"
          }
        }
      },
      "least_adopted": {
        "schema": {
          "type": "array",
          "items": {
            "ref": "ProductAdoptionSummary"
          }
        }
      },
      "high_blast_radius": {
        "schema": {
          "type": "array",
          "items": {
            "ref": "ProductAdoptionSummary"
          }
        }
      },
      "domain_scorecards": {
        "schema": {
          "type": "array",
          "items": {
            "ref": "ProductPortfolioGroup"
          }
        }
      },
      "team_scorecards": {
        "schema": {
          "type": "array",
          "items": {
            "ref": "ProductPortfolioGroup"
          }
        }
      },
      "orphan_assets": {
        "schema": {
          "type": "array",
          "items": {
            "ref": "OrphanResource"
          }
        }
      },
      "orphan_semantic_models": {
        "schema": {
          "type": "array",
          "items": {
            "ref": "OrphanResource"
          }
        }
      }
    }
  },
  "ProductSLO": {
    "required": [],
    "property_order": [
      "freshness_slo",
      "latency_slo"
    ],
    "properties": {
      "freshness_slo": {
        "schema": {
          "type": "string"
        }
      },
      "latency_slo": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "ProductScorecard": {
    "required": [
      "product_id",
      "product_slug",
      "product_name",
      "domain_name",
      "team_name",
      "publication_state",
      "certification_state",
      "has_owner",
      "has_contract",
      "has_slo",
      "has_docs_or_access_path",
      "has_primary_output",
      "has_warnings",
      "completeness_percent"
    ],
    "property_order": [
      "product_id",
      "product_slug",
      "product_name",
      "domain_name",
      "team_name",
      "publication_state",
      "certification_state",
      "has_owner",
      "has_contract",
      "has_slo",
      "has_docs_or_access_path",
      "has_primary_output",
      "has_warnings",
      "completeness_percent"
    ],
    "properties": {
      "product_id": {
        "schema": {
          "type": "string"
        }
      },
      "product_slug": {
        "schema": {
          "type": "string"
        }
      },
      "product_name": {
        "schema": {
          "type": "string"
        }
      },
      "domain_name": {
        "schema": {
          "type": "string"
        }
      },
      "team_name": {
        "schema": {
          "type": "string"
        }
      },
      "publication_state": {
        "schema": {
          "type": "string"
        }
      },
      "certification_state": {
        "schema": {
          "type": "string"
        }
      },
      "has_owner": {
        "schema": {
          "type": "boolean"
        }
      },
      "has_contract": {
        "schema": {
          "type": "boolean"
        }
      },
      "has_slo": {
        "schema": {
          "type": "boolean"
        }
      },
      "has_docs_or_access_path": {
        "schema": {
          "type": "boolean"
        }
      },
      "has_primary_output": {
        "schema": {
          "type": "boolean"
        }
      },
      "has_warnings": {
        "schema": {
          "type": "boolean"
        }
      },
      "completeness_percent": {
        "schema": {
          "type": "integer",
          "format": "int32"
        }
      }
    }
  },
  "ProductScorecardList": {
    "required": [
      "data"
    ],
    "property_order": [
      "data",
      "next_page_token"
    ],
    "properties": {
      "data": {
        "schema": {
          "type": "array",
          "items": {
            "ref": "ProductScorecard"
          }
        }
      },
      "next_page_token": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "ProductSemanticEntrypoint": {
    "required": [
      "id",
      "product_version_id",
      "semantic_model_id",
      "model_name"
    ],
    "property_order": [
      "id",
      "product_version_id",
      "semantic_model_id",
      "model_name",
      "created_at"
    ],
    "properties": {
      "id": {
        "schema": {
          "type": "string"
        }
      },
      "product_version_id": {
        "schema": {
          "type": "string"
        }
      },
      "semantic_model_id": {
        "schema": {
          "type": "string"
        }
      },
      "model_name": {
        "schema": {
          "type": "string"
        }
      },
      "created_at": {
        "schema": {
          "type": "string",
          "format": "date-time"
        }
      }
    }
  },
  "ProductSemanticEntrypointList": {
    "required": [
      "data"
    ],
    "property_order": [
      "data"
    ],
    "properties": {
      "data": {
        "schema": {
          "type": "array",
          "items": {
            "ref": "ProductSemanticEntrypoint"
          }
        }
      }
    }
  },
  "ProductSubscription": {
    "required": [
      "id",
      "product_id",
      "principal_name",
      "event_type",
      "channel"
    ],
    "property_order": [
      "id",
      "product_id",
      "principal_name",
      "event_type",
      "channel",
      "created_at"
    ],
    "properties": {
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
      "principal_name": {
        "schema": {
          "type": "string"
        }
      },
      "event_type": {
        "schema": {
          "type": "string"
        }
      },
      "channel": {
        "schema": {
          "type": "string"
        }
      },
      "created_at": {
        "schema": {
          "type": "string",
          "format": "date-time"
        }
      }
    }
  },
  "ProductSubscriptionList": {
    "required": [
      "data"
    ],
    "property_order": [
      "data"
    ],
    "properties": {
      "data": {
        "schema": {
          "type": "array",
          "items": {
            "ref": "ProductSubscription"
          }
        }
      }
    }
  },
  "ProductTeam": {
    "required": [
      "id",
      "domain_id",
      "name",
      "contact_channel"
    ],
    "property_order": [
      "id",
      "domain_id",
      "name",
      "contact_channel",
      "created_at",
      "updated_at"
    ],
    "properties": {
      "id": {
        "schema": {
          "type": "string"
        }
      },
      "domain_id": {
        "schema": {
          "type": "string"
        }
      },
      "name": {
        "schema": {
          "type": "string"
        }
      },
      "contact_channel": {
        "schema": {
          "type": "string"
        }
      },
      "created_at": {
        "schema": {
          "type": "string",
          "format": "date-time"
        }
      },
      "updated_at": {
        "schema": {
          "type": "string",
          "format": "date-time"
        }
      }
    }
  },
  "Project": {
    "required": [
      "workspace_id",
      "name",
      "kind"
    ],
    "property_order": [
      "id",
      "workspace_id",
      "name",
      "kind",
      "description",
      "owner_team_id",
      "owner_principal",
      "product_id",
      "default_branch",
      "created_at",
      "updated_at"
    ],
    "properties": {
      "id": {
        "schema": {
          "type": "string"
        }
      },
      "workspace_id": {
        "schema": {
          "type": "string"
        }
      },
      "name": {
        "schema": {
          "type": "string"
        }
      },
      "kind": {
        "schema": {
          "ref": "ProjectKind"
        }
      },
      "description": {
        "schema": {
          "type": "string"
        }
      },
      "owner_team_id": {
        "schema": {
          "type": "string"
        }
      },
      "owner_principal": {
        "schema": {
          "type": "string"
        }
      },
      "product_id": {
        "schema": {
          "type": "string"
        }
      },
      "default_branch": {
        "schema": {
          "type": "string"
        }
      },
      "created_at": {
        "schema": {
          "type": "string",
          "format": "date-time"
        }
      },
      "updated_at": {
        "schema": {
          "type": "string",
          "format": "date-time"
        }
      }
    }
  },
  "ProjectKind": {
    "required": []
  },
  "PromoteNotebookRequest": {
    "required": [
      "cell_index",
      "project_name",
      "name"
    ],
    "property_order": [
      "cell_index",
      "project_name",
      "name",
      "materialization"
    ],
    "properties": {
      "cell_index": {
        "schema": {
          "type": "integer",
          "format": "int32"
        }
      },
      "project_name": {
        "schema": {
          "type": "string"
        }
      },
      "name": {
        "schema": {
          "type": "string"
        }
      },
      "materialization": {
        "schema": {
          "ref": "ModelMaterialization"
        }
      }
    }
  },
  "QueryHistoryEntry": {
    "required": [
      "id"
    ],
    "property_order": [
      "id",
      "principal_name",
      "original_sql",
      "rewritten_sql",
      "statement_type",
      "tables_accessed",
      "status",
      "error_message",
      "duration_ms",
      "rows_returned",
      "created_at"
    ],
    "properties": {
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
      "original_sql": {
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
      "tables_accessed": {
        "schema": {
          "type": "array",
          "items": {
            "type": "string"
          }
        }
      },
      "status": {
        "schema": {
          "ref": "AuditDecisionStatus"
        }
      },
      "error_message": {
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
      "rows_returned": {
        "schema": {
          "type": "integer",
          "format": "int64"
        }
      },
      "created_at": {
        "schema": {
          "type": "string",
          "format": "date-time"
        }
      }
    }
  },
  "QueryJob": {
    "required": [
      "query_id",
      "status",
      "row_count"
    ],
    "property_order": [
      "query_id",
      "status",
      "row_count",
      "request_id",
      "error",
      "created_at",
      "started_at",
      "completed_at"
    ],
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
      },
      "row_count": {
        "schema": {
          "type": "integer",
          "format": "int64"
        }
      },
      "request_id": {
        "schema": {
          "type": "string"
        }
      },
      "error": {
        "schema": {
          "type": "string"
        }
      },
      "created_at": {
        "schema": {
          "type": "string",
          "format": "date-time"
        }
      },
      "started_at": {
        "schema": {
          "type": "string",
          "format": "date-time"
        }
      },
      "completed_at": {
        "schema": {
          "type": "string",
          "format": "date-time"
        }
      }
    }
  },
  "QueryJobStatus": {
    "required": []
  },
  "QueryRequest": {
    "title": "Synchronous SQL query request.",
    "description": "Submits a SQL statement for immediate execution and returns a tabular result when the request completes.",
    "required": [
      "sql"
    ],
    "property_order": [
      "sql"
    ],
    "properties": {
      "sql": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "QueryResult": {
    "title": "Tabular SQL query result.",
    "description": "Contains result-set columns, row data, and an optional continuation token when additional rows are available.",
    "required": [
      "columns",
      "rows"
    ],
    "property_order": [
      "columns",
      "rows",
      "row_count",
      "next_page_token"
    ],
    "properties": {
      "columns": {
        "schema": {
          "type": "array",
          "items": {
            "ref": "TabularColumn"
          }
        }
      },
      "rows": {
        "schema": {
          "type": "array",
          "items": {
            "type": "object",
            "additional_properties": {
              "any": true
            }
          }
        }
      },
      "row_count": {
        "schema": {
          "type": "integer",
          "format": "int64"
        }
      },
      "next_page_token": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "RecentResource": {
    "required": [
      "resource_type",
      "resource_key",
      "display_name"
    ],
    "property_order": [
      "resource_type",
      "resource_key",
      "display_name",
      "resource_path",
      "href",
      "section",
      "accessed_at"
    ],
    "properties": {
      "resource_type": {
        "schema": {
          "type": "string"
        }
      },
      "resource_key": {
        "schema": {
          "type": "string"
        }
      },
      "display_name": {
        "schema": {
          "type": "string"
        }
      },
      "resource_path": {
        "schema": {
          "type": "string"
        }
      },
      "href": {
        "schema": {
          "type": "string"
        }
      },
      "section": {
        "schema": {
          "type": "string"
        }
      },
      "accessed_at": {
        "schema": {
          "type": "string",
          "format": "date-time"
        }
      }
    }
  },
  "ReorderCellsRequest": {
    "required": [
      "cell_ids"
    ],
    "property_order": [
      "cell_ids"
    ],
    "properties": {
      "cell_ids": {
        "schema": {
          "type": "array",
          "items": {
            "type": "string"
          }
        }
      }
    }
  },
  "ResolvedDashboardDetail": {
    "required": [],
    "property_order": [
      "dashboard",
      "widgets"
    ],
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
    "required": [
      "columns"
    ],
    "property_order": [
      "widget",
      "columns",
      "rows",
      "row_count",
      "generated_sql"
    ],
    "properties": {
      "widget": {
        "schema": {
          "ref": "DashboardWidget"
        }
      },
      "columns": {
        "schema": {
          "type": "array",
          "items": {
            "type": "string"
          }
        }
      },
      "rows": {
        "schema": {
          "type": "array",
          "items": {
            "type": "array",
            "items": {}
          }
        }
      },
      "row_count": {
        "schema": {
          "type": "integer",
          "format": "int64"
        }
      },
      "generated_sql": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "RevokeWebSessionsRequest": {
    "required": [
      "principal_id"
    ],
    "property_order": [
      "principal_id"
    ],
    "properties": {
      "principal_id": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "RowFilter": {
    "required": [
      "id",
      "table_id",
      "name",
      "filter_sql"
    ],
    "property_order": [
      "id",
      "table_id",
      "name",
      "filter_sql",
      "description",
      "created_at"
    ],
    "properties": {
      "id": {
        "schema": {
          "type": "string"
        }
      },
      "table_id": {
        "schema": {
          "type": "string"
        }
      },
      "name": {
        "schema": {
          "type": "string"
        }
      },
      "filter_sql": {
        "schema": {
          "type": "string"
        }
      },
      "description": {
        "schema": {
          "type": "string"
        }
      },
      "created_at": {
        "schema": {
          "type": "string",
          "format": "date-time"
        }
      }
    }
  },
  "RowFilterBinding": {
    "required": [],
    "property_order": [
      "id",
      "row_filter_id",
      "principal_id",
      "principal_type"
    ],
    "properties": {
      "id": {
        "schema": {
          "type": "string"
        }
      },
      "row_filter_id": {
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
      }
    }
  },
  "RowFilterBindingRequest": {
    "required": [
      "principal_id",
      "principal_type"
    ],
    "property_order": [
      "principal_id",
      "principal_type"
    ],
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
    }
  },
  "RunAllResult": {
    "required": [],
    "property_order": [
      "notebook_id",
      "results",
      "total_duration_ms"
    ],
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
    "required": [
      "resource_type",
      "resource_key",
      "display_name"
    ],
    "property_order": [
      "resource_type",
      "resource_key",
      "display_name",
      "resource_path",
      "href",
      "section",
      "saved_at",
      "last_accessed_at"
    ],
    "properties": {
      "resource_type": {
        "schema": {
          "type": "string"
        }
      },
      "resource_key": {
        "schema": {
          "type": "string"
        }
      },
      "display_name": {
        "schema": {
          "type": "string"
        }
      },
      "resource_path": {
        "schema": {
          "type": "string"
        }
      },
      "href": {
        "schema": {
          "type": "string"
        }
      },
      "section": {
        "schema": {
          "type": "string"
        }
      },
      "saved_at": {
        "schema": {
          "type": "string",
          "format": "date-time"
        }
      },
      "last_accessed_at": {
        "schema": {
          "type": "string",
          "format": "date-time"
        }
      }
    }
  },
  "SchemaDetail": {
    "required": [
      "schema_id",
      "name",
      "catalog_name"
    ],
    "property_order": [
      "schema_id",
      "name",
      "catalog_name",
      "comment",
      "properties",
      "tags",
      "owner",
      "created_at",
      "updated_at"
    ],
    "properties": {
      "schema_id": {
        "schema": {
          "type": "string"
        }
      },
      "name": {
        "schema": {
          "type": "string"
        }
      },
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
      "properties": {
        "schema": {
          "type": "object",
          "additional_properties": {
            "schema": {
              "type": "string"
            }
          }
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
      "owner": {
        "schema": {
          "type": "string"
        }
      },
      "created_at": {
        "schema": {
          "type": "string",
          "format": "date-time"
        }
      },
      "updated_at": {
        "schema": {
          "type": "string",
          "format": "date-time"
        }
      }
    }
  },
  "SearchResult": {
    "required": [],
    "property_order": [
      "type",
      "name",
      "schema_name",
      "table_name",
      "comment",
      "match_field"
    ],
    "properties": {
      "type": {
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
      "comment": {
        "schema": {
          "type": "string"
        }
      },
      "match_field": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "SemanticMetric": {
    "required": [],
    "property_order": [
      "id",
      "semantic_model_id",
      "name",
      "description",
      "label",
      "metric_type",
      "expression_mode",
      "expression",
      "relationship_names",
      "filter_sql",
      "default_time_grain",
      "format",
      "owner",
      "certification_state",
      "created_by",
      "created_at",
      "updated_at"
    ],
    "properties": {
      "id": {
        "schema": {
          "type": "string"
        }
      },
      "semantic_model_id": {
        "schema": {
          "type": "string"
        }
      },
      "name": {
        "schema": {
          "type": "string"
        }
      },
      "description": {
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
      "expression_mode": {
        "schema": {
          "ref": "SemanticMetricExpressionMode"
        }
      },
      "expression": {
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
      "filter_sql": {
        "schema": {
          "type": "string"
        }
      },
      "default_time_grain": {
        "schema": {
          "type": "string"
        }
      },
      "format": {
        "schema": {
          "type": "string"
        }
      },
      "owner": {
        "schema": {
          "type": "string"
        }
      },
      "certification_state": {
        "schema": {
          "ref": "CreateSemanticMetricRequestCertificationState"
        }
      },
      "created_by": {
        "schema": {
          "type": "string"
        }
      },
      "created_at": {
        "schema": {
          "type": "string",
          "format": "date-time"
        }
      },
      "updated_at": {
        "schema": {
          "type": "string",
          "format": "date-time"
        }
      }
    }
  },
  "SemanticMetricExpressionMode": {
    "required": []
  },
  "SemanticMetricList": {
    "required": [
      "data"
    ],
    "property_order": [
      "data"
    ],
    "properties": {
      "data": {
        "schema": {
          "type": "array",
          "items": {
            "ref": "SemanticMetric"
          }
        }
      }
    }
  },
  "SemanticMetricMetricType": {
    "required": []
  },
  "SemanticModel": {
    "required": [],
    "property_order": [
      "id",
      "name",
      "description",
      "owner",
      "base_model_ref",
      "default_time_dimension",
      "tags",
      "created_by",
      "created_at",
      "updated_at"
    ],
    "properties": {
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
      "tags": {
        "schema": {
          "type": "array",
          "items": {
            "type": "string"
          }
        }
      },
      "created_by": {
        "schema": {
          "type": "string"
        }
      },
      "created_at": {
        "schema": {
          "type": "string",
          "format": "date-time"
        }
      },
      "updated_at": {
        "schema": {
          "type": "string",
          "format": "date-time"
        }
      }
    }
  },
  "SemanticPreAggregation": {
    "required": [],
    "property_order": [
      "id",
      "semantic_model_id",
      "name",
      "metric_set",
      "dimension_set",
      "grain",
      "target_relation",
      "refresh_policy",
      "created_by",
      "created_at",
      "updated_at"
    ],
    "properties": {
      "id": {
        "schema": {
          "type": "string"
        }
      },
      "semantic_model_id": {
        "schema": {
          "type": "string"
        }
      },
      "name": {
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
      "target_relation": {
        "schema": {
          "type": "string"
        }
      },
      "refresh_policy": {
        "schema": {
          "type": "string"
        }
      },
      "created_by": {
        "schema": {
          "type": "string"
        }
      },
      "created_at": {
        "schema": {
          "type": "string",
          "format": "date-time"
        }
      },
      "updated_at": {
        "schema": {
          "type": "string",
          "format": "date-time"
        }
      }
    }
  },
  "SemanticPreAggregationList": {
    "required": [
      "data"
    ],
    "property_order": [
      "data"
    ],
    "properties": {
      "data": {
        "schema": {
          "type": "array",
          "items": {
            "ref": "SemanticPreAggregation"
          }
        }
      }
    }
  },
  "SemanticRelationship": {
    "required": [],
    "property_order": [
      "id",
      "name",
      "from_semantic_id",
      "to_semantic_id",
      "relationship_type",
      "join_sql",
      "cost",
      "max_hops",
      "created_by",
      "created_at",
      "updated_at"
    ],
    "properties": {
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
      "from_semantic_id": {
        "schema": {
          "type": "string"
        }
      },
      "to_semantic_id": {
        "schema": {
          "type": "string"
        }
      },
      "relationship_type": {
        "schema": {
          "ref": "SemanticRelationshipRelationshipType"
        }
      },
      "join_sql": {
        "schema": {
          "type": "string"
        }
      },
      "cost": {
        "schema": {
          "type": "integer",
          "format": "int32"
        }
      },
      "max_hops": {
        "schema": {
          "type": "integer",
          "format": "int32"
        }
      },
      "created_by": {
        "schema": {
          "type": "string"
        }
      },
      "created_at": {
        "schema": {
          "type": "string",
          "format": "date-time"
        }
      },
      "updated_at": {
        "schema": {
          "type": "string",
          "format": "date-time"
        }
      }
    }
  },
  "SemanticRelationshipList": {
    "required": [
      "data"
    ],
    "property_order": [
      "data"
    ],
    "properties": {
      "data": {
        "schema": {
          "type": "array",
          "items": {
            "ref": "SemanticRelationship"
          }
        }
      }
    }
  },
  "SemanticRelationshipRelationshipType": {
    "required": []
  },
  "SetDefaultCatalogRequest": {
    "required": []
  },
  "ShareFolderRequest": {
    "required": [
      "principal_name"
    ],
    "property_order": [
      "principal_name",
      "role"
    ],
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
  "ShareNotebookRequest": {
    "required": [
      "principal_name"
    ],
    "property_order": [
      "principal_name",
      "role"
    ],
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
  "SourceFreshnessStatus": {
    "required": [],
    "property_order": [
      "is_fresh",
      "source_schema",
      "source_table",
      "timestamp_column",
      "last_loaded_at",
      "max_lag_seconds",
      "stale_since"
    ],
    "properties": {
      "is_fresh": {
        "schema": {
          "type": "boolean"
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
      "timestamp_column": {
        "schema": {
          "type": "string"
        }
      },
      "last_loaded_at": {
        "schema": {
          "type": "string",
          "format": "date-time"
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
          "type": "string",
          "format": "date-time"
        }
      }
    }
  },
  "StorageCredential": {
    "required": [
      "id",
      "name"
    ],
    "property_order": [
      "id",
      "name",
      "credential_type",
      "endpoint",
      "region",
      "url_style",
      "comment",
      "owner",
      "created_at",
      "updated_at"
    ],
    "properties": {
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
      "region": {
        "schema": {
          "type": "string"
        }
      },
      "url_style": {
        "schema": {
          "ref": "URLStyle"
        }
      },
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
      "created_at": {
        "schema": {
          "type": "string",
          "format": "date-time"
        }
      },
      "updated_at": {
        "schema": {
          "type": "string",
          "format": "date-time"
        }
      }
    }
  },
  "StorageCredentialType": {
    "required": []
  },
  "StorageType": {
    "required": []
  },
  "SubmitQueryRequest": {
    "required": [
      "sql"
    ],
    "property_order": [
      "sql",
      "request_id"
    ],
    "properties": {
      "sql": {
        "schema": {
          "type": "string"
        }
      },
      "request_id": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "SubmitQueryResponse": {
    "required": [
      "query_id",
      "status"
    ],
    "property_order": [
      "query_id",
      "status"
    ],
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
    }
  },
  "TableDetail": {
    "required": [
      "table_id",
      "name",
      "schema_name",
      "catalog_name"
    ],
    "property_order": [
      "table_id",
      "name",
      "schema_name",
      "catalog_name",
      "table_type",
      "columns",
      "comment",
      "properties",
      "owner",
      "tags",
      "statistics",
      "created_at",
      "updated_at"
    ],
    "properties": {
      "table_id": {
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
      "catalog_name": {
        "schema": {
          "type": "string"
        }
      },
      "table_type": {
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
      "properties": {
        "schema": {
          "type": "object",
          "additional_properties": {
            "schema": {
              "type": "string"
            }
          }
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
            "ref": "Tag"
          }
        }
      },
      "statistics": {
        "schema": {
          "ref": "TableStatistics"
        }
      },
      "created_at": {
        "schema": {
          "type": "string",
          "format": "date-time"
        }
      },
      "updated_at": {
        "schema": {
          "type": "string",
          "format": "date-time"
        }
      }
    }
  },
  "TableStatistics": {
    "required": [],
    "property_order": [
      "row_count",
      "size_bytes",
      "column_count",
      "last_profiled_at",
      "profiled_by"
    ],
    "properties": {
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
      },
      "column_count": {
        "schema": {
          "type": "integer",
          "format": "int32"
        }
      },
      "last_profiled_at": {
        "schema": {
          "type": "string",
          "format": "date-time"
        }
      },
      "profiled_by": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "TabularColumn": {
    "title": "Metadata for a result-set column.",
    "required": [
      "name"
    ],
    "property_order": [
      "name"
    ],
    "properties": {
      "name": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "Tag": {
    "required": [],
    "property_order": [
      "id",
      "key",
      "value",
      "created_by",
      "created_at"
    ],
    "properties": {
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
      },
      "created_by": {
        "schema": {
          "type": "string"
        }
      },
      "created_at": {
        "schema": {
          "type": "string",
          "format": "date-time"
        }
      }
    }
  },
  "TagAssignment": {
    "required": [],
    "property_order": [
      "id",
      "tag_id",
      "securable_type",
      "securable_id",
      "column_name",
      "assigned_by",
      "assigned_at"
    ],
    "properties": {
      "id": {
        "schema": {
          "type": "string"
        }
      },
      "tag_id": {
        "schema": {
          "type": "string"
        }
      },
      "securable_type": {
        "schema": {
          "ref": "TagAssignmentSecurableType"
        }
      },
      "securable_id": {
        "schema": {
          "type": "string"
        }
      },
      "column_name": {
        "schema": {
          "type": "string"
        }
      },
      "assigned_by": {
        "schema": {
          "type": "string"
        }
      },
      "assigned_at": {
        "schema": {
          "type": "string",
          "format": "date-time"
        }
      }
    }
  },
  "TagAssignmentSecurableType": {
    "required": []
  },
  "TriggerAssetMaterializationRequest": {
    "required": [],
    "property_order": [
      "partition_key",
      "idempotency_key",
      "payload"
    ],
    "properties": {
      "partition_key": {
        "schema": {
          "type": "string"
        }
      },
      "idempotency_key": {
        "schema": {
          "type": "string"
        }
      },
      "payload": {
        "schema": {
          "type": "object",
          "additional_properties": {
            "any": true
          }
        }
      }
    }
  },
  "TriggerModelRunRequest": {
    "required": [
      "project_name"
    ],
    "property_order": [
      "project_name",
      "environment_name",
      "model_names",
      "full_refresh"
    ],
    "properties": {
      "project_name": {
        "schema": {
          "type": "string"
        }
      },
      "environment_name": {
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
      "full_refresh": {
        "schema": {
          "type": "boolean"
        }
      }
    }
  },
  "TriggerPipelineRunRequest": {
    "required": [],
    "property_order": [
      "parameters"
    ],
    "properties": {
      "parameters": {
        "schema": {
          "type": "object",
          "additional_properties": {
            "schema": {
              "type": "string"
            }
          }
        }
      }
    }
  },
  "URLStyle": {
    "required": []
  },
  "UpdateAssetRequest": {
    "required": [],
    "property_order": [
      "asset_type",
      "product_slug",
      "owner",
      "description",
      "tags",
      "freshness_policy",
      "materialization_policy",
      "auto_materialize_policy",
      "io_profile",
      "is_active",
      "upstream_asset_keys",
      "checks"
    ],
    "properties": {
      "asset_type": {
        "schema": {
          "ref": "AssetType"
        }
      },
      "product_slug": {
        "schema": {
          "type": "string"
        }
      },
      "owner": {
        "schema": {
          "type": "string"
        }
      },
      "description": {
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
      "freshness_policy": {
        "schema": {
          "ref": "AssetFreshnessPolicy"
        }
      },
      "materialization_policy": {
        "schema": {
          "ref": "AssetMaterializationPolicy"
        }
      },
      "auto_materialize_policy": {
        "schema": {
          "ref": "AssetAutoMaterializePolicy"
        }
      },
      "io_profile": {
        "schema": {
          "type": "string"
        }
      },
      "is_active": {
        "schema": {
          "type": "boolean"
        }
      },
      "upstream_asset_keys": {
        "schema": {
          "type": "array",
          "items": {
            "type": "string"
          }
        }
      },
      "checks": {
        "schema": {
          "type": "array",
          "items": {
            "ref": "AssetCheckInput"
          }
        }
      }
    }
  },
  "UpdateCatalogRegistrationRequest": {
    "required": [],
    "property_order": [
      "data_path",
      "comment"
    ],
    "properties": {
      "data_path": {
        "schema": {
          "type": "string"
        }
      },
      "comment": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "UpdateCellRequest": {
    "required": [],
    "property_order": [
      "name",
      "role",
      "disabled",
      "test",
      "visual_spec",
      "content",
      "position"
    ],
    "properties": {
      "name": {
        "schema": {
          "type": "string"
        }
      },
      "role": {
        "schema": {
          "ref": "CellRole"
        }
      },
      "disabled": {
        "schema": {
          "type": "boolean"
        }
      },
      "test": {
        "schema": {
          "ref": "NotebookCellTestConfig"
        }
      },
      "visual_spec": {
        "schema": {
          "ref": "VisualSpecUpdate"
        }
      },
      "content": {
        "schema": {
          "type": "string"
        }
      },
      "position": {
        "schema": {
          "type": "integer",
          "format": "int32"
        }
      }
    }
  },
  "UpdateColumnMaskRequest": {
    "required": [],
    "property_order": [
      "name",
      "column_name",
      "mask_expression",
      "description"
    ],
    "properties": {
      "name": {
        "schema": {
          "type": "string"
        }
      },
      "column_name": {
        "schema": {
          "type": "string"
        }
      },
      "mask_expression": {
        "schema": {
          "type": "string"
        }
      },
      "description": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "UpdateColumnRequest": {
    "required": [],
    "property_order": [
      "comment",
      "nullable"
    ],
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
    "required": [],
    "property_order": [
      "auth_token",
      "max_memory_gb",
      "size",
      "status",
      "url"
    ],
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
    "required": [],
    "property_order": [
      "name",
      "description",
      "folder_id"
    ],
    "properties": {
      "name": {
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
      }
    }
  },
  "UpdateDashboardWidgetRequest": {
    "required": [],
    "property_order": [
      "name",
      "description",
      "source",
      "visual_spec",
      "layout"
    ],
    "properties": {
      "name": {
        "schema": {
          "type": "string"
        }
      },
      "description": {
        "schema": {
          "type": "string"
        }
      },
      "source": {
        "schema": {
          "ref": "DashboardWidgetSourceUpdate"
        }
      },
      "visual_spec": {
        "schema": {
          "ref": "VisualSpecUpdate"
        }
      },
      "layout": {
        "schema": {
          "ref": "DashboardWidgetLayoutUpdate"
        }
      }
    }
  },
  "UpdateDataProductRequest": {
    "required": [],
    "property_order": [
      "name",
      "description",
      "domain_name",
      "team_name",
      "steward_principal",
      "contact_channel",
      "visibility",
      "consumer_audience",
      "docs_url",
      "access_request_path",
      "business_definitions",
      "contract",
      "slo",
      "publication_intent"
    ],
    "properties": {
      "name": {
        "schema": {
          "type": "string"
        }
      },
      "description": {
        "schema": {
          "type": "string"
        }
      },
      "domain_name": {
        "schema": {
          "type": "string"
        }
      },
      "team_name": {
        "schema": {
          "type": "string"
        }
      },
      "steward_principal": {
        "schema": {
          "type": "string"
        }
      },
      "contact_channel": {
        "schema": {
          "type": "string"
        }
      },
      "visibility": {
        "schema": {
          "type": "string"
        }
      },
      "consumer_audience": {
        "schema": {
          "type": "string"
        }
      },
      "docs_url": {
        "schema": {
          "type": "string"
        }
      },
      "access_request_path": {
        "schema": {
          "type": "string"
        }
      },
      "business_definitions": {
        "schema": {
          "type": "object",
          "additional_properties": {
            "schema": {
              "type": "string"
            }
          }
        }
      },
      "contract": {
        "schema": {
          "ref": "ProductContract"
        }
      },
      "slo": {
        "schema": {
          "ref": "ProductSLO"
        }
      },
      "publication_intent": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "UpdateExternalLocationRequest": {
    "required": [],
    "property_order": [
      "url",
      "credential_name",
      "comment",
      "read_only"
    ],
    "properties": {
      "url": {
        "schema": {
          "type": "string"
        }
      },
      "credential_name": {
        "schema": {
          "type": "string"
        }
      },
      "comment": {
        "schema": {
          "type": "string"
        }
      },
      "read_only": {
        "schema": {
          "type": "boolean"
        }
      }
    }
  },
  "UpdateFolderRequest": {
    "required": [],
    "property_order": [
      "name",
      "git_repo_id",
      "git_root_path",
      "default_project_id",
      "default_environment_id"
    ],
    "properties": {
      "name": {
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
      "default_project_id": {
        "schema": {
          "type": "string"
        }
      },
      "default_environment_id": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "UpdateGroupRequest": {
    "required": [],
    "property_order": [
      "description"
    ],
    "properties": {
      "description": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "UpdateMacroRequest": {
    "required": [],
    "property_order": [
      "body",
      "description",
      "parameters",
      "status",
      "catalog_name",
      "project_name",
      "visibility",
      "owner",
      "properties",
      "tags"
    ],
    "properties": {
      "body": {
        "schema": {
          "type": "string"
        }
      },
      "description": {
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
      "catalog_name": {
        "schema": {
          "type": "string"
        }
      },
      "project_name": {
        "schema": {
          "type": "string"
        }
      },
      "visibility": {
        "schema": {
          "ref": "MacroVisibility"
        }
      },
      "owner": {
        "schema": {
          "type": "string"
        }
      },
      "properties": {
        "schema": {
          "type": "object",
          "additional_properties": {
            "schema": {
              "type": "string"
            }
          }
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
  "UpdateModelRequest": {
    "required": [],
    "property_order": [
      "sql",
      "materialization",
      "description",
      "tags",
      "config",
      "contract",
      "freshness_policy"
    ],
    "properties": {
      "sql": {
        "schema": {
          "type": "string"
        }
      },
      "materialization": {
        "schema": {
          "ref": "ModelMaterialization"
        }
      },
      "description": {
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
      "freshness_policy": {
        "schema": {
          "ref": "FreshnessPolicy"
        }
      }
    }
  },
  "UpdateNotebookRequest": {
    "required": [],
    "property_order": [
      "name",
      "description",
      "project_override_id",
      "environment_override_id"
    ],
    "properties": {
      "name": {
        "schema": {
          "type": "string"
        }
      },
      "description": {
        "schema": {
          "type": "string"
        }
      },
      "project_override_id": {
        "schema": {
          "type": "string"
        }
      },
      "environment_override_id": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "UpdatePipelineJobRequest": {
    "required": [],
    "property_order": [
      "name",
      "notebook_id",
      "compute_endpoint_id",
      "depends_on",
      "timeout_seconds",
      "retry_count",
      "job_order",
      "job_type",
      "model_selector"
    ],
    "properties": {
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
      "timeout_seconds": {
        "schema": {
          "type": "integer",
          "format": "int64"
        }
      },
      "retry_count": {
        "schema": {
          "type": "integer",
          "format": "int32"
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
      }
    }
  },
  "UpdatePipelineRequest": {
    "required": [],
    "property_order": [
      "description",
      "schedule_cron",
      "is_paused",
      "concurrency_limit",
      "folder_id"
    ],
    "properties": {
      "description": {
        "schema": {
          "type": "string"
        }
      },
      "schedule_cron": {
        "schema": {
          "type": "string"
        }
      },
      "is_paused": {
        "schema": {
          "type": "boolean"
        }
      },
      "concurrency_limit": {
        "schema": {
          "type": "integer",
          "format": "int32"
        }
      },
      "folder_id": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "UpdatePrincipalRequest": {
    "required": [],
    "property_order": [
      "is_admin"
    ],
    "properties": {
      "is_admin": {
        "schema": {
          "type": "boolean"
        }
      }
    }
  },
  "UpdateProductDomainRequest": {
    "required": [],
    "property_order": [
      "description"
    ],
    "properties": {
      "description": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "UpdateProductTeamRequest": {
    "required": [],
    "property_order": [
      "contact_channel"
    ],
    "properties": {
      "contact_channel": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "UpdateRowFilterRequest": {
    "required": [],
    "property_order": [
      "name",
      "filter_sql",
      "description"
    ],
    "properties": {
      "name": {
        "schema": {
          "type": "string"
        }
      },
      "filter_sql": {
        "schema": {
          "type": "string"
        }
      },
      "description": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "UpdateSchemaRequest": {
    "required": [],
    "property_order": [
      "comment",
      "properties"
    ],
    "properties": {
      "comment": {
        "schema": {
          "type": "string"
        }
      },
      "properties": {
        "schema": {
          "type": "object",
          "additional_properties": {
            "schema": {
              "type": "string"
            }
          }
        }
      }
    }
  },
  "UpdateSemanticMetricRequest": {
    "required": [],
    "property_order": [
      "description",
      "label",
      "metric_type",
      "expression_mode",
      "expression",
      "relationship_names",
      "filter_sql",
      "default_time_grain",
      "format",
      "owner",
      "certification_state"
    ],
    "properties": {
      "description": {
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
      "expression_mode": {
        "schema": {
          "ref": "SemanticMetricExpressionMode"
        }
      },
      "expression": {
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
      "filter_sql": {
        "schema": {
          "type": "string"
        }
      },
      "default_time_grain": {
        "schema": {
          "type": "string"
        }
      },
      "format": {
        "schema": {
          "type": "string"
        }
      },
      "owner": {
        "schema": {
          "type": "string"
        }
      },
      "certification_state": {
        "schema": {
          "ref": "CreateSemanticMetricRequestCertificationState"
        }
      }
    }
  },
  "UpdateSemanticModelRequest": {
    "required": [],
    "property_order": [
      "description",
      "owner",
      "base_model_ref",
      "default_time_dimension",
      "tags"
    ],
    "properties": {
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
    "required": [],
    "property_order": [
      "metric_set",
      "dimension_set",
      "grain",
      "target_relation",
      "refresh_policy"
    ],
    "properties": {
      "metric_set": {
        "schema": {
          "type": "array",
          "items": {
            "type": "string"
          }
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
      "target_relation": {
        "schema": {
          "type": "string"
        }
      },
      "refresh_policy": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "UpdateSemanticRelationshipRequest": {
    "required": [],
    "property_order": [
      "relationship_type",
      "join_sql",
      "cost",
      "max_hops"
    ],
    "properties": {
      "relationship_type": {
        "schema": {
          "ref": "SemanticRelationshipRelationshipType"
        }
      },
      "join_sql": {
        "schema": {
          "type": "string"
        }
      },
      "cost": {
        "schema": {
          "type": "integer",
          "format": "int32"
        }
      },
      "max_hops": {
        "schema": {
          "type": "integer",
          "format": "int32"
        }
      }
    }
  },
  "UpdateStorageCredentialRequest": {
    "required": [],
    "property_order": [
      "key_id",
      "secret",
      "endpoint",
      "region",
      "url_style",
      "comment"
    ],
    "properties": {
      "key_id": {
        "schema": {
          "type": "string"
        }
      },
      "secret": {
        "schema": {
          "type": "string"
        }
      },
      "endpoint": {
        "schema": {
          "type": "string"
        }
      },
      "region": {
        "schema": {
          "type": "string"
        }
      },
      "url_style": {
        "schema": {
          "ref": "URLStyle"
        }
      },
      "comment": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "UpdateTableRequest": {
    "required": [],
    "property_order": [
      "comment",
      "properties",
      "owner"
    ],
    "properties": {
      "comment": {
        "schema": {
          "type": "string"
        }
      },
      "properties": {
        "schema": {
          "type": "object",
          "additional_properties": {
            "schema": {
              "type": "string"
            }
          }
        }
      },
      "owner": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "UpdateTagRequest": {
    "required": [],
    "property_order": [
      "key",
      "value"
    ],
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
    "required": [],
    "property_order": [
      "comment",
      "view_definition"
    ],
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
    "required": [],
    "property_order": [
      "comment",
      "new_name",
      "storage_location"
    ],
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
    "required": [],
    "property_order": [
      "name",
      "default_project_id",
      "default_environment_id",
      "git_repo_id",
      "git_root_path"
    ],
    "properties": {
      "name": {
        "schema": {
          "type": "string"
        }
      },
      "default_project_id": {
        "schema": {
          "type": "string"
        }
      },
      "default_environment_id": {
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
      }
    }
  },
  "UploadUrlRequest": {
    "required": [],
    "property_order": [
      "filename"
    ],
    "properties": {
      "filename": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "UploadUrlResponse": {
    "required": [
      "upload_url",
      "s3_key",
      "expires_at"
    ],
    "property_order": [
      "upload_url",
      "s3_key",
      "expires_at"
    ],
    "properties": {
      "upload_url": {
        "schema": {
          "type": "string"
        }
      },
      "s3_key": {
        "schema": {
          "type": "string"
        }
      },
      "expires_at": {
        "schema": {
          "type": "string",
          "format": "date-time"
        }
      }
    }
  },
  "VersionedObjectSummary": {
    "required": [],
    "property_order": [
      "total_count",
      "active_count",
      "historical_count",
      "has_history",
      "latest_snapshot_id"
    ],
    "properties": {
      "total_count": {
        "schema": {
          "type": "integer",
          "format": "int64"
        }
      },
      "active_count": {
        "schema": {
          "type": "integer",
          "format": "int64"
        }
      },
      "historical_count": {
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
      "latest_snapshot_id": {
        "schema": {
          "type": "integer",
          "format": "int64"
        }
      }
    }
  },
  "ViewDetail": {
    "required": [
      "id",
      "schema_name",
      "catalog_name",
      "name"
    ],
    "property_order": [
      "id",
      "schema_id",
      "schema_name",
      "catalog_name",
      "name",
      "view_definition",
      "comment",
      "owner",
      "source_tables",
      "created_at",
      "updated_at"
    ],
    "properties": {
      "id": {
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
      "catalog_name": {
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
      },
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
      "source_tables": {
        "schema": {
          "type": "array",
          "items": {
            "type": "string"
          }
        }
      },
      "created_at": {
        "schema": {
          "type": "string",
          "format": "date-time"
        }
      },
      "updated_at": {
        "schema": {
          "type": "string",
          "format": "date-time"
        }
      }
    }
  },
  "VisualChartType": {
    "required": []
  },
  "VisualEncodings": {
    "required": [],
    "property_order": [
      "x",
      "y",
      "series",
      "label",
      "value",
      "secondary"
    ],
    "properties": {
      "x": {
        "schema": {
          "ref": "VisualFieldBinding"
        }
      },
      "y": {
        "schema": {
          "ref": "VisualFieldBinding"
        }
      },
      "series": {
        "schema": {
          "ref": "VisualFieldBinding"
        }
      },
      "label": {
        "schema": {
          "ref": "VisualFieldBinding"
        }
      },
      "value": {
        "schema": {
          "ref": "VisualFieldBinding"
        }
      },
      "secondary": {
        "schema": {
          "ref": "VisualFieldBinding"
        }
      }
    }
  },
  "VisualEncodingsUpdate": {
    "required": [],
    "property_order": [
      "x",
      "y",
      "series",
      "label",
      "value",
      "secondary"
    ],
    "properties": {
      "x": {
        "schema": {
          "ref": "VisualFieldBindingUpdate"
        }
      },
      "y": {
        "schema": {
          "ref": "VisualFieldBindingUpdate"
        }
      },
      "series": {
        "schema": {
          "ref": "VisualFieldBindingUpdate"
        }
      },
      "label": {
        "schema": {
          "ref": "VisualFieldBindingUpdate"
        }
      },
      "value": {
        "schema": {
          "ref": "VisualFieldBindingUpdate"
        }
      },
      "secondary": {
        "schema": {
          "ref": "VisualFieldBindingUpdate"
        }
      }
    }
  },
  "VisualFieldBinding": {
    "required": [
      "field"
    ],
    "property_order": [
      "field"
    ],
    "properties": {
      "field": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "VisualFieldBindingUpdate": {
    "required": [],
    "property_order": [
      "field"
    ],
    "properties": {
      "field": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "VisualOutputKind": {
    "required": []
  },
  "VisualSpec": {
    "required": [
      "kind"
    ],
    "property_order": [
      "kind",
      "chart_type",
      "encodings",
      "title",
      "subtitle",
      "legend",
      "stacked",
      "color_palette"
    ],
    "properties": {
      "kind": {
        "schema": {
          "ref": "VisualOutputKind"
        }
      },
      "chart_type": {
        "schema": {
          "ref": "VisualChartType"
        }
      },
      "encodings": {
        "schema": {
          "ref": "VisualEncodings"
        }
      },
      "title": {
        "schema": {
          "type": "string"
        }
      },
      "subtitle": {
        "schema": {
          "type": "string"
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
      "color_palette": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "VisualSpecUpdate": {
    "required": [],
    "property_order": [
      "kind",
      "chart_type",
      "encodings",
      "title",
      "subtitle",
      "legend",
      "stacked",
      "color_palette"
    ],
    "properties": {
      "kind": {
        "schema": {
          "ref": "VisualOutputKind"
        }
      },
      "chart_type": {
        "schema": {
          "ref": "VisualChartType"
        }
      },
      "encodings": {
        "schema": {
          "ref": "VisualEncodingsUpdate"
        }
      },
      "title": {
        "schema": {
          "type": "string"
        }
      },
      "subtitle": {
        "schema": {
          "type": "string"
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
      "color_palette": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "VolumeDetail": {
    "required": [
      "id",
      "name",
      "schema_name",
      "catalog_name"
    ],
    "property_order": [
      "id",
      "name",
      "schema_name",
      "catalog_name",
      "volume_type",
      "storage_location",
      "comment",
      "owner",
      "created_at",
      "updated_at"
    ],
    "properties": {
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
      "schema_name": {
        "schema": {
          "type": "string"
        }
      },
      "catalog_name": {
        "schema": {
          "type": "string"
        }
      },
      "volume_type": {
        "schema": {
          "type": "string"
        }
      },
      "storage_location": {
        "schema": {
          "type": "string"
        }
      },
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
      "created_at": {
        "schema": {
          "type": "string",
          "format": "date-time"
        }
      },
      "updated_at": {
        "schema": {
          "type": "string",
          "format": "date-time"
        }
      }
    }
  },
  "WebSessionStatsResponse": {
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
    ],
    "property_order": [
      "created_total",
      "resolved_total",
      "resolve_failed_total",
      "revoked_total",
      "revoked_all_total",
      "reaped_total",
      "active_sessions",
      "idle_ttl_seconds",
      "absolute_ttl_seconds"
    ],
    "properties": {
      "created_total": {
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
      "resolve_failed_total": {
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
      },
      "revoked_all_total": {
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
      "active_sessions": {
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
      "absolute_ttl_seconds": {
        "schema": {
          "type": "integer",
          "format": "int64"
        }
      }
    }
  },
  "Workspace": {
    "required": [
      "name",
      "kind"
    ],
    "property_order": [
      "id",
      "name",
      "kind",
      "owner_team_id",
      "owner_principal",
      "default_project_id",
      "default_environment_id",
      "git_repo_id",
      "git_root_path",
      "created_at",
      "updated_at"
    ],
    "properties": {
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
      "kind": {
        "schema": {
          "ref": "WorkspaceKind"
        }
      },
      "owner_team_id": {
        "schema": {
          "type": "string"
        }
      },
      "owner_principal": {
        "schema": {
          "type": "string"
        }
      },
      "default_project_id": {
        "schema": {
          "type": "string"
        }
      },
      "default_environment_id": {
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
      "created_at": {
        "schema": {
          "type": "string",
          "format": "date-time"
        }
      },
      "updated_at": {
        "schema": {
          "type": "string",
          "format": "date-time"
        }
      }
    }
  },
  "WorkspaceKind": {
    "required": []
  },
  "WorkspaceMember": {
    "required": [
      "workspace_id",
      "principal_name",
      "role"
    ],
    "property_order": [
      "workspace_id",
      "principal_name",
      "role",
      "created_at",
      "updated_at"
    ],
    "properties": {
      "workspace_id": {
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
      "created_at": {
        "schema": {
          "type": "string",
          "format": "date-time"
        }
      },
      "updated_at": {
        "schema": {
          "type": "string",
          "format": "date-time"
        }
      }
    }
  }
}
