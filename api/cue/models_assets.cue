package api

// Authored asset schemas.

schemas_assets: {
  "Asset": {
    "type": "object",
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
      "auto_materialize_policy": {
        "schema": {
          "ref": "AssetAutoMaterializePolicy"
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
      "freshness_policy": {
        "schema": {
          "ref": "AssetFreshnessPolicy"
        }
      },
      "id": {
        "schema": {
          "type": "string"
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
      "materialization_policy": {
        "schema": {
          "ref": "AssetMaterializationPolicy"
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
  "AssetAutoMaterializePolicy": {
    "type": "object",
    "properties": {
      "downtime_windows_cron_expr": {
        "schema": {
          "type": "array",
          "items": {
            "type": "string"
          }
        }
      },
      "min_interval_seconds": {
        "schema": {
          "type": "integer",
          "format": "int64"
        }
      },
      "mode": {
        "schema": {
          "type": "string"
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
      "require_all_upstreams": {
        "schema": {
          "type": "boolean"
        }
      },
      "respect_downtime_windows": {
        "schema": {
          "type": "boolean"
        }
      }
    }
  },
  "AssetBackfillDetails": {
    "type": "object",
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
    "type": "object",
    "properties": {
      "asset_id": {
        "schema": {
          "type": "string"
        }
      },
      "check_type": {
        "schema": {
          "type": "string"
        }
      },
      "created_at": {
        "schema": {
          "type": "string"
        }
      },
      "enabled": {
        "schema": {
          "type": "boolean"
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
      "severity": {
        "schema": {
          "ref": "AssetCheckSeverity"
        }
      },
      "updated_at": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "AssetCheckInput": {
    "type": "object",
    "properties": {
      "check_type": {
        "schema": {
          "type": "string"
        }
      },
      "config_json": {
        "schema": {
          "ref": "Record"
        }
      },
      "enabled": {
        "schema": {
          "type": "boolean"
        }
      },
      "name": {
        "schema": {
          "type": "string"
        }
      },
      "severity": {
        "schema": {
          "ref": "AssetCheckSeverity"
        }
      }
    },
    "required": [
      "name",
      "check_type"
    ]
  },
  "AssetCheckList": {
    "type": "object",
    "properties": {
      "data": {
        "schema": {
          "type": "array",
          "items": {
            "ref": "AssetCheck"
          }
        }
      }
    },
    "required": [
      "data"
    ]
  },
  "AssetCheckResult": {
    "type": "object",
    "properties": {
      "check_id": {
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
      "message": {
        "schema": {
          "type": "string"
        }
      },
      "metrics_json": {
        "schema": {
          "ref": "Record"
        }
      },
      "partition_key": {
        "schema": {
          "type": "string"
        }
      },
      "run_id": {
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
  "AssetCheckSeverity": {
    "type": "string",
    "enum": [
      "ERROR",
      "WARN"
    ]
  },
  "AssetFreshnessBlocker": {
    "type": "object",
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
    "type": "object",
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
    "type": "object",
    "properties": {
      "dependency_type": {
        "schema": {
          "type": "string"
        }
      },
      "from_asset_key": {
        "schema": {
          "type": "string"
        }
      },
      "to_asset_key": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "AssetFreshnessExplanation": {
    "type": "object",
    "properties": {
      "asset": {
        "schema": {
          "ref": "AssetFreshnessStatus"
        }
      },
      "edges": {
        "schema": {
          "type": "array",
          "items": {
            "ref": "AssetFreshnessEdge"
          }
        }
      },
      "nodes": {
        "schema": {
          "type": "array",
          "items": {
            "ref": "AssetFreshnessStatus"
          }
        }
      }
    }
  },
  "AssetFreshnessPolicy": {
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
  "AssetFreshnessReconcileResponse": {
    "type": "object",
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
    "type": "object",
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
      "event_id": {
        "schema": {
          "type": "string"
        }
      },
      "freshness_status": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "AssetFreshnessRequirement": {
    "type": "object",
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
    "type": "object",
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
    "type": "object",
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
      "basis": {
        "schema": {
          "type": "array",
          "items": {
            "type": "string"
          }
        }
      },
      "effective_max_lag_seconds": {
        "schema": {
          "type": "integer",
          "format": "int64"
        }
      },
      "freshness_status": {
        "schema": {
          "type": "string"
        }
      },
      "last_materialized_at": {
        "schema": {
          "type": "string"
        }
      },
      "reason": {
        "schema": {
          "type": "string"
        }
      },
      "stale_since": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "AssetGraph": {
    "type": "object",
    "properties": {
      "asset_key": {
        "schema": {
          "type": "string"
        }
      },
      "downstream_asset_keys": {
        "schema": {
          "type": "array",
          "items": {
            "type": "string"
          }
        }
      },
      "upstream_asset_keys": {
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
    "type": "object",
    "properties": {
      "asset_id": {
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
      "materialized_at": {
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
      "run_id": {
        "schema": {
          "type": "string"
        }
      },
      "schema_hash": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "AssetMaterializationPolicy": {
    "type": "object",
    "properties": {
      "allow_concurrent": {
        "schema": {
          "type": "boolean"
        }
      },
      "mode": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "AssetPartition": {
    "type": "object",
    "properties": {
      "asset_id": {
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
      "updated_at": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "AssetRun": {
    "type": "object",
    "properties": {
      "asset_id": {
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
      "max_attempts": {
        "schema": {
          "type": "integer",
          "format": "int32"
        }
      },
      "partition_from": {
        "schema": {
          "type": "string"
        }
      },
      "partition_key": {
        "schema": {
          "type": "string"
        }
      },
      "partition_to": {
        "schema": {
          "type": "string"
        }
      },
      "run_group_id": {
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
      "updated_at": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "AssetRunStatus": {
    "type": "string",
    "enum": [
      "QUEUED",
      "PLANNING",
      "RUNNING",
      "RETRYING",
      "SUCCESS",
      "FAILED",
      "CANCELLED",
      "SKIPPED",
      "STALE"
    ]
  },
  "AssetTriggerResponse": {
    "type": "object",
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
    "type": "string",
    "enum": [
      "MANUAL",
      "SCHEDULED",
      "UPSTREAM_UPDATE",
      "FRESHNESS_BREACH",
      "API_EVENT",
      "BACKFILL",
      "RECONCILER",
      "PIPELINE"
    ]
  },
  "AssetType": {
    "type": "string",
    "enum": [
      "TABLE",
      "VIEW",
      "MODEL",
      "NOTEBOOK",
      "OUTPUT",
      "DASHBOARD",
      "SEMANTIC_MODEL",
      "METRIC",
      "SEMANTIC_PRE_AGGREGATION",
      "NOTEBOOK_OUTPUT"
    ]
  },
  "BackfillRequest": {
    "type": "object",
    "properties": {
      "asset_id": {
        "schema": {
          "type": "string"
        }
      },
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
      "max_parallelism": {
        "schema": {
          "type": "integer",
          "format": "int32"
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
      "requested_by": {
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
      }
    }
  },
  "BackfillSlice": {
    "type": "object",
    "properties": {
      "asset_id": {
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
      "max_attempts": {
        "schema": {
          "type": "integer",
          "format": "int32"
        }
      },
      "partition_key": {
        "schema": {
          "type": "string"
        }
      },
      "request_id": {
        "schema": {
          "type": "string"
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
      }
    }
  },
  "CreateAssetBackfillRequest": {
    "type": "object",
    "properties": {
      "max_parallelism": {
        "schema": {
          "type": "integer",
          "format": "int32"
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
      }
    },
    "required": [
      "partition_from",
      "partition_to"
    ]
  },
  "CreateAssetBackfillResponse": {
    "type": "object",
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
    "type": "object",
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
      "auto_materialize_policy": {
        "schema": {
          "ref": "AssetAutoMaterializePolicy"
        }
      },
      "checks": {
        "schema": {
          "type": "array",
          "items": {
            "ref": "AssetCheckInput"
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
          "ref": "AssetFreshnessPolicy"
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
      "materialization_policy": {
        "schema": {
          "ref": "AssetMaterializationPolicy"
        }
      },
      "owner": {
        "schema": {
          "type": "string"
        }
      },
      "product_slug": {
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
      "upstream_asset_keys": {
        "schema": {
          "type": "array",
          "items": {
            "type": "string"
          }
        }
      }
    },
    "required": [
      "asset_key",
      "asset_type",
      "product_slug",
      "owner"
    ]
  },
  "PaginatedAssetCheckResults": {
    "type": "object",
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
    },
    "required": [
      "data"
    ]
  },
  "PaginatedAssetMaterializations": {
    "type": "object",
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
    },
    "required": [
      "data"
    ]
  },
  "PaginatedAssetPartitions": {
    "type": "object",
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
    },
    "required": [
      "data"
    ]
  },
  "PaginatedAssetRuns": {
    "type": "object",
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
    },
    "required": [
      "data"
    ]
  },
  "PaginatedAssets": {
    "type": "object",
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
    },
    "required": [
      "data"
    ]
  },
  "PaginatedBackfillRequests": {
    "type": "object",
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
    },
    "required": [
      "data"
    ]
  },
  "TriggerAssetMaterializationRequest": {
    "type": "object",
    "properties": {
      "idempotency_key": {
        "schema": {
          "type": "string"
        }
      },
      "partition_key": {
        "schema": {
          "type": "string"
        }
      },
      "payload": {
        "schema": {
          "ref": "Record"
        }
      }
    }
  },
  "UpdateAssetRequest": {
    "type": "object",
    "properties": {
      "asset_type": {
        "schema": {
          "ref": "AssetType"
        }
      },
      "auto_materialize_policy": {
        "schema": {
          "ref": "AssetAutoMaterializePolicy"
        }
      },
      "checks": {
        "schema": {
          "type": "array",
          "items": {
            "ref": "AssetCheckInput"
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
          "ref": "AssetFreshnessPolicy"
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
      "materialization_policy": {
        "schema": {
          "ref": "AssetMaterializationPolicy"
        }
      },
      "owner": {
        "schema": {
          "type": "string"
        }
      },
      "product_slug": {
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
      "upstream_asset_keys": {
        "schema": {
          "type": "array",
          "items": {
            "type": "string"
          }
        }
      }
    },
    "required": [
      "asset_type",
      "product_slug",
      "owner"
    ]
  }
}

