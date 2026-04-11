package api

openapi_schema_overrides: {
  "Asset": {
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
    ]
  },
  "AssetAutoMaterializePolicy": {
    "property_order": [
      "mode",
      "min_interval_seconds",
      "require_all_upstreams",
      "on_freshness_breach",
      "on_upstream_materialized",
      "respect_downtime_windows",
      "downtime_windows_cron_expr"
    ]
  },
  "AssetCheck": {
    "property_order": [
      "id",
      "asset_id",
      "name",
      "check_type",
      "severity",
      "enabled",
      "created_at",
      "updated_at"
    ]
  },
  "AssetCheckInput": {
    "property_order": [
      "name",
      "check_type",
      "severity",
      "enabled",
      "config_json"
    ],
    "properties": {
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
  "AssetCheckResult": {
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
      "metrics_json": {
        "schema": {
          "type": "object",
          "additional_properties": {
            "any": true
          }
        }
      }
    }
  },
  "AssetFreshnessEdge": {
    "property_order": [
      "from_asset_key",
      "to_asset_key",
      "dependency_type"
    ]
  },
  "AssetFreshnessExplanation": {
    "property_order": [
      "asset",
      "nodes",
      "edges"
    ]
  },
  "AssetFreshnessPolicy": {
    "property_order": [
      "max_lag_seconds",
      "cron_schedule"
    ]
  },
  "AssetFreshnessReconcileTarget": {
    "property_order": [
      "asset_id",
      "asset_key",
      "asset_type",
      "freshness_status",
      "event_id"
    ]
  },
  "AssetFreshnessStatus": {
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
      }
    }
  },
  "AssetGraph": {
    "property_order": [
      "asset_key",
      "upstream_asset_keys",
      "downstream_asset_keys"
    ]
  },
  "AssetMaterialization": {
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
      "materialized_at": {
        "schema": {
          "type": "string",
          "format": "date-time"
        }
      }
    }
  },
  "AssetMaterializationPolicy": {
    "property_order": [
      "mode",
      "allow_concurrent"
    ]
  },
  "AssetPartition": {
    "property_order": [
      "id",
      "asset_id",
      "partition_key",
      "status",
      "created_at",
      "updated_at"
    ]
  },
  "AssetRun": {
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
      "finished_at": {
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
      }
    }
  },
  "AuditEntry": {
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
    ]
  },
  "BackfillRequest": {
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
      "finished_at": {
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
      }
    }
  },
  "BackfillSlice": {
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
      "finished_at": {
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
      }
    }
  },
  "Build": {
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
    ]
  },
  "CatalogHistoryEntry": {
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
    ]
  },
  "CatalogRegistration": {
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
    ]
  },
  "CatalogVersionSummary": {
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
    ]
  },
  "Cell": {
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
    ]
  },
  "CellExecutionResult": {
    "property_order": [
      "cell_id",
      "columns",
      "rows",
      "row_count",
      "error",
      "duration_ms"
    ],
    "properties": {
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
      }
    }
  },
  "ColumnDetail": {
    "property_order": [
      "name",
      "type",
      "position",
      "nullable",
      "comment"
    ]
  },
  "ColumnLineageEdge": {
    "property_order": [
      "id",
      "lineage_edge_id",
      "target_column",
      "source_schema",
      "source_table",
      "source_column",
      "transform_type",
      "function"
    ]
  },
  "ColumnMask": {
    "property_order": [
      "id",
      "table_id",
      "name",
      "column_name",
      "mask_expression",
      "description",
      "created_at"
    ]
  },
  "ColumnMaskBinding": {
    "property_order": [
      "id",
      "column_mask_id",
      "principal_id",
      "principal_type",
      "see_original"
    ]
  },
  "CommitIngestionRequest": {
    "property_order": [
      "s3_keys",
      "options"
    ]
  },
  "ComputeAssignment": {
    "property_order": [
      "id",
      "endpoint_id",
      "endpoint_name",
      "principal_id",
      "principal_type",
      "fallback_local",
      "is_default",
      "created_at"
    ]
  },
  "ComputeEndpoint": {
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
    ]
  },
  "ComputeEndpointHealth": {
    "property_order": [
      "endpoint_name",
      "status",
      "memory_used_mb",
      "max_memory_gb",
      "uptime_seconds",
      "duckdb_version"
    ]
  },
  "ComputeRoutingDefaults": {
    "property_order": [
      "interactive_mode",
      "scheduled_mode",
      "notebook_mode"
    ]
  },
  "CreateAssetBackfillRequest": {
    "property_order": [
      "partition_from",
      "partition_to",
      "max_parallelism"
    ]
  },
  "CreateAssetRequest": {
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
    ]
  },
  "CreateBuildRequest": {
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
    ]
  },
  "CreateCatalogRequest": {
    "property_order": [
      "name",
      "metastore_type",
      "dsn",
      "data_path",
      "comment"
    ]
  },
  "CreateCellRequest": {
    "property_order": [
      "cell_type",
      "name",
      "role",
      "disabled",
      "test",
      "visual_spec",
      "content",
      "position"
    ]
  },
  "CreateColumnMaskRequest": {
    "property_order": [
      "table_id",
      "name",
      "column_name",
      "mask_expression",
      "description"
    ]
  },
  "CreateColumnRequest": {
    "property_order": [
      "name",
      "type",
      "nullable",
      "comment"
    ]
  },
  "CreateComputeAssignmentRequest": {
    "property_order": [
      "principal_id",
      "principal_type",
      "fallback_local",
      "is_default"
    ]
  },
  "CreateComputeEndpointRequest": {
    "property_order": [
      "name",
      "type",
      "url",
      "auth_token",
      "max_memory_gb",
      "size"
    ],
    "properties": {
      "url": {
        "description": "Endpoint URI. REMOTE endpoints must use grpc:// or grpcs://; LOCAL endpoints use local routing URLs."
      }
    }
  },
  "CreateDashboardRequest": {
    "property_order": [
      "name",
      "description",
      "folder_id"
    ]
  },
  "CreateDashboardWidgetRequest": {
    "property_order": [
      "name",
      "description",
      "source",
      "visual_spec",
      "layout"
    ]
  },
  "CreateDataProductRequest": {
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
      "business_definitions": {
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
  "CreateDataProductVersionRequest": {
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
    ]
  },
  "CreateEnvironmentRequest": {
    "properties": {
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
      "variables": {
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
    "property_order": [
      "name",
      "url",
      "credential_name",
      "storage_type",
      "comment",
      "read_only"
    ]
  },
  "CreateGitRepoRequest": {
    "property_order": [
      "url",
      "branch",
      "path",
      "auth_token"
    ]
  },
  "CreateGrantRequest": {
    "property_order": [
      "principal_id",
      "principal_type",
      "securable_type",
      "securable_id",
      "privilege"
    ]
  },
  "CreateGroupMemberRequest": {
    "property_order": [
      "member_type",
      "member_id"
    ]
  },
  "CreateGroupRequest": {
    "property_order": [
      "name",
      "description"
    ]
  },
  "CreateMacroRequest": {
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
  "CreateModelRequest": {
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
    ]
  },
  "CreateModelTestRequest": {
    "property_order": [
      "name",
      "test_type",
      "column",
      "config"
    ]
  },
  "CreateNotebookRequest": {
    "property_order": [
      "name",
      "description",
      "source",
      "folder_id"
    ]
  },
  "CreatePipelineJobRequest": {
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
    ]
  },
  "CreatePipelineRequest": {
    "property_order": [
      "name",
      "description",
      "schedule_cron",
      "is_paused",
      "concurrency_limit",
      "folder_id"
    ]
  },
  "CreateProductDomainRequest": {
    "property_order": [
      "name",
      "description"
    ]
  },
  "CreateProductSubscriptionRequest": {
    "property_order": [
      "principal_name",
      "event_type",
      "channel"
    ]
  },
  "CreateProductTeamRequest": {
    "property_order": [
      "name",
      "contact_channel"
    ]
  },
  "CreateRowFilterRequest": {
    "property_order": [
      "table_id",
      "name",
      "filter_sql",
      "description"
    ]
  },
  "CreateSavedResourceRequest": {
    "property_order": [
      "resource_type",
      "resource_key",
      "display_name",
      "resource_path",
      "section"
    ]
  },
  "CreateSchemaRequest": {
    "property_order": [
      "name",
      "comment",
      "location_name",
      "properties"
    ],
    "properties": {
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
    ]
  },
  "CreateSemanticModelRequest": {
    "property_order": [
      "name",
      "description",
      "base_model_ref",
      "default_time_dimension",
      "tags"
    ]
  },
  "CreateSemanticPreAggregationRequest": {
    "property_order": [
      "name",
      "metric_set",
      "dimension_set",
      "grain",
      "target_relation",
      "refresh_policy"
    ]
  },
  "CreateSemanticRelationshipRequest": {
    "property_order": [
      "name",
      "from_semantic_id",
      "to_semantic_id",
      "relationship_type",
      "join_sql",
      "cost",
      "max_hops"
    ]
  },
  "CreateStorageCredentialRequest": {
    "property_order": [
      "name",
      "credential_type",
      "key_id",
      "secret",
      "endpoint",
      "region",
      "url_style",
      "comment"
    ]
  },
  "CreateTableRequest": {
    "property_order": [
      "name",
      "columns",
      "comment"
    ]
  },
  "CreateTagAssignmentRequest": {
    "property_order": [
      "securable_type",
      "securable_id",
      "column_name"
    ]
  },
  "CreateViewRequest": {
    "property_order": [
      "name",
      "view_definition",
      "comment"
    ]
  },
  "CreateVolumeRequest": {
    "property_order": [
      "name",
      "volume_type",
      "storage_location",
      "comment"
    ]
  },
  "CreateWorkspaceRequest": {
    "property_order": [
      "name",
      "kind",
      "owner_team_id",
      "owner_principal",
      "default_project_id",
      "default_environment_id",
      "git_repo_id",
      "git_root_path"
    ]
  },
  "Dashboard": {
    "property_order": [
      "id",
      "name",
      "description",
      "owner",
      "folder_id",
      "created_at",
      "updated_at"
    ]
  },
  "DashboardNotebookCellSource": {
    "property_order": [
      "notebook_id",
      "cell_id"
    ]
  },
  "DashboardSQLQuerySource": {
    "property_order": [
      "sql",
      "catalog",
      "schema"
    ]
  },
  "DashboardSemanticQuerySource": {
    "property_order": [
      "semantic_model_id",
      "metrics",
      "relationship_names",
      "dimensions",
      "filters",
      "order_by",
      "limit",
      "time_grain"
    ]
  },
  "DashboardWidget": {
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
    ]
  },
  "DashboardWidgetLayout": {
    "property_order": [
      "x",
      "y",
      "w",
      "h"
    ]
  },
  "DashboardWidgetSource": {
    "property_order": [
      "kind",
      "sql_query",
      "notebook_cell",
      "semantic_query"
    ]
  },
  "DataProduct": {
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
      "business_definitions": {
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
  "DataProductDetail": {
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
    ]
  },
  "DataProductListItem": {
    "property_order": [
      "product",
      "domain",
      "owner_team",
      "latest_version",
      "status",
      "primary_output"
    ]
  },
  "DataProductStatus": {
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
      "adoption_metrics": {
        "schema": {
          "type": "object",
          "additional_properties": {
            "any": true
          }
        }
      },
      "last_successful_update_at": {
        "schema": {
          "type": "string",
          "format": "date-time"
        }
      },
      "lineage_coverage": {
        "schema": {
          "type": "number",
          "format": "double"
        }
      }
    }
  },
  "DataProductVersion": {
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
    ]
  },
  "DataProductVersionDetail": {
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
    ]
  },
  "DuplicateNotebookRequest": {
    "property_order": [
      "folder_id",
      "name",
      "git_path"
    ]
  },
  "Environment": {
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
      "variables": {
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
  "Error": {
    "title": "Standard API error response.",
    "description": "Errors use a shared schema across the API so clients can handle failure responses consistently.",
    "property_order": [
      "code",
      "message",
      "details"
    ],
    "properties": {
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
    ]
  },
  "Folder": {
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
    ]
  },
  "FolderContentItem": {
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
    ]
  },
  "FreshnessPolicy": {
    "property_order": [
      "max_lag_seconds",
      "cron_schedule"
    ]
  },
  "FreshnessStatus": {
    "properties": {
      "last_run_at": {
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
      }
    }
  },
  "GitRepo": {
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
      "last_sync_at": {
        "schema": {
          "type": "string",
          "format": "date-time"
        }
      }
    }
  },
  "GitSyncResult": {
    "property_order": [
      "notebooks_created",
      "notebooks_updated",
      "notebooks_deleted",
      "commit_sha"
    ]
  },
  "Group": {
    "property_order": [
      "id",
      "name",
      "description",
      "created_at"
    ]
  },
  "GroupMember": {
    "property_order": [
      "group_id",
      "member_type",
      "member_id"
    ]
  },
  "LoadExternalRequest": {
    "property_order": [
      "paths",
      "options"
    ]
  },
  "LocalLoginRequest": {
    "property_order": [
      "username",
      "password"
    ]
  },
  "Macro": {
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
  "MacroImpactModel": {
    "property_order": [
      "target_table",
      "target_schema",
      "model_name",
      "last_seen_at"
    ],
    "properties": {
      "last_seen_at": {
        "schema": {
          "type": "string",
          "format": "date-time"
        }
      }
    }
  },
  "MacroRevision": {
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
    ]
  },
  "MacroRevisionDiff": {
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
    ]
  },
  "ManifestResponse": {
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
      "column_masks": {
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
  "MetastoreSummary": {
    "property_order": [
      "catalog_name",
      "metastore_type",
      "storage_backend",
      "data_path",
      "schema_count",
      "table_count"
    ]
  },
  "MetricFreshnessStatus": {
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
      "checked_at": {
        "schema": {
          "type": "string",
          "format": "date-time"
        }
      }
    }
  },
  "MetricQueryJoinStep": {
    "property_order": [
      "relationship_name",
      "from_model",
      "to_model",
      "relationship_type",
      "join_sql"
    ]
  },
  "MetricQueryPlan": {
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
    ]
  },
  "MetricQueryRequest": {
    "property_order": [
      "metrics",
      "relationship_names",
      "dimensions",
      "filters",
      "order_by",
      "limit",
      "time_grain"
    ]
  },
  "Model": {
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
    ]
  },
  "ModelConfig": {
    "property_order": [
      "unique_key",
      "incremental_strategy",
      "on_schema_change"
    ]
  },
  "ModelContract": {
    "property_order": [
      "enforce",
      "columns"
    ]
  },
  "ModelContractColumn": {
    "property_order": [
      "name",
      "type",
      "nullable"
    ]
  },
  "ModelDAGNode": {
    "property_order": [
      "project_name",
      "model_name",
      "materialization",
      "depends_on"
    ]
  },
  "ModelDAGTier": {
    "property_order": [
      "tier",
      "nodes"
    ]
  },
  "ModelRun": {
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
      "finished_at": {
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
      }
    }
  },
  "ModelRunCompileDiagnostics": {
    "property_order": [
      "warnings",
      "errors"
    ]
  },
  "ModelRunStep": {
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
      "finished_at": {
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
      }
    }
  },
  "ModelTest": {
    "property_order": [
      "id",
      "model_id",
      "name",
      "test_type",
      "column",
      "config",
      "created_at"
    ]
  },
  "ModelTestConfig": {
    "property_order": [
      "values",
      "to_model",
      "to_column",
      "custom_sql"
    ]
  },
  "ModelTestResult": {
    "property_order": [
      "id",
      "run_step_id",
      "test_id",
      "test_name",
      "status",
      "rows_returned",
      "error_message",
      "created_at"
    ]
  },
  "MoveNotebookRequest": {
    "property_order": [
      "folder_id",
      "git_path",
      "confirm_leave_git",
      "confirm_context_change"
    ]
  },
  "Notebook": {
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
    ]
  },
  "NotebookContext": {
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
    ]
  },
  "NotebookDetail": {
    "property_order": [
      "notebook",
      "cells",
      "context",
      "shares",
      "publish_model"
    ]
  },
  "NotebookJob": {
    "property_order": [
      "id",
      "notebook_id",
      "session_id",
      "state",
      "result",
      "error",
      "created_at",
      "updated_at"
    ]
  },
  "NotebookPublishModel": {
    "property_order": [
      "project_name",
      "name",
      "materialization",
      "output_cell_id"
    ]
  },
  "NotebookSession": {
    "property_order": [
      "id",
      "notebook_id",
      "principal",
      "state",
      "created_at",
      "last_used_at"
    ],
    "properties": {
      "last_used_at": {
        "schema": {
          "type": "string",
          "format": "date-time"
        }
      }
    }
  },
  "OIDCProviderRequest": {
    "property_order": [
      "enabled",
      "issuer_url",
      "jwks_url",
      "audience",
      "client_id",
      "client_secret",
      "scopes"
    ]
  },
  "OIDCProviderResponse": {
    "property_order": [
      "enabled",
      "issuer_url",
      "jwks_url",
      "audience",
      "client_id",
      "scopes",
      "updated_at",
      "secret_stored"
    ]
  },
  "OrphanResource": {
    "property_order": [
      "resource_type",
      "resource_id",
      "resource_name"
    ]
  },
  "Pipeline": {
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
    ]
  },
  "PipelineJob": {
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
    ]
  },
  "PipelineJobRun": {
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
      "finished_at": {
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
      }
    }
  },
  "PipelineRun": {
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
      "finished_at": {
        "schema": {
          "type": "string",
          "format": "date-time"
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
      "started_at": {
        "schema": {
          "type": "string",
          "format": "date-time"
        }
      }
    }
  },
  "PrivilegeGrant": {
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
      "granted_at": {
        "schema": {
          "type": "string",
          "format": "date-time"
        }
      }
    }
  },
  "ProductAdoptionSummary": {
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
    ]
  },
  "ProductContract": {
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
    ]
  },
  "ProductDomain": {
    "property_order": [
      "id",
      "name",
      "description",
      "created_at",
      "updated_at"
    ]
  },
  "ProductEvent": {
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
      "metadata": {
        "schema": {
          "type": "object",
          "additional_properties": {
            "any": true
          }
        }
      }
    }
  },
  "ProductOutput": {
    "property_order": [
      "id",
      "product_version_id",
      "asset_id",
      "asset_key",
      "asset_type",
      "is_primary",
      "created_at"
    ]
  },
  "ProductPortfolioGroup": {
    "property_order": [
      "name",
      "product_count",
      "published_count",
      "certified_count",
      "average_completeness_pct"
    ]
  },
  "ProductPortfolioReport": {
    "property_order": [
      "top_used",
      "least_adopted",
      "high_blast_radius",
      "domain_scorecards",
      "team_scorecards",
      "orphan_assets",
      "orphan_semantic_models"
    ]
  },
  "ProductScorecard": {
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
    ]
  },
  "ProductSemanticEntrypoint": {
    "property_order": [
      "id",
      "product_version_id",
      "semantic_model_id",
      "model_name",
      "created_at"
    ]
  },
  "ProductSubscription": {
    "property_order": [
      "id",
      "product_id",
      "principal_name",
      "event_type",
      "channel",
      "created_at"
    ]
  },
  "ProductTeam": {
    "property_order": [
      "id",
      "domain_id",
      "name",
      "contact_channel",
      "created_at",
      "updated_at"
    ]
  },
  "Project": {
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
    ]
  },
  "PromoteNotebookRequest": {
    "property_order": [
      "cell_index",
      "project_name",
      "name",
      "materialization"
    ]
  },
  "QueryHistoryEntry": {
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
    ]
  },
  "QueryJob": {
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
      "completed_at": {
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
      }
    }
  },
  "QueryRequest": {
    "title": "Synchronous SQL query request.",
    "description": "Submits a SQL statement for immediate execution and returns a tabular result when the request completes."
  },
  "QueryResult": {
    "title": "Tabular SQL query result.",
    "description": "Contains result-set columns, row data, and an optional continuation token when additional rows are available.",
    "property_order": [
      "columns",
      "rows",
      "row_count",
      "next_page_token"
    ],
    "properties": {
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
      }
    }
  },
  "RecentResource": {
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
      "accessed_at": {
        "schema": {
          "type": "string",
          "format": "date-time"
        }
      }
    }
  },
  "ResolvedDashboardWidget": {
    "property_order": [
      "widget",
      "columns",
      "rows",
      "row_count",
      "generated_sql"
    ],
    "properties": {
      "rows": {
        "schema": {
          "type": "array",
          "items": {
            "type": "array",
            "items": {}
          }
        }
      }
    }
  },
  "RowFilter": {
    "property_order": [
      "id",
      "table_id",
      "name",
      "filter_sql",
      "description",
      "created_at"
    ]
  },
  "RowFilterBinding": {
    "property_order": [
      "id",
      "row_filter_id",
      "principal_id",
      "principal_type"
    ]
  },
  "SavedResource": {
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
      "last_accessed_at": {
        "schema": {
          "type": "string",
          "format": "date-time"
        }
      },
      "saved_at": {
        "schema": {
          "type": "string",
          "format": "date-time"
        }
      }
    }
  },
  "SchemaDetail": {
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
  "SearchResult": {
    "property_order": [
      "type",
      "name",
      "schema_name",
      "table_name",
      "comment",
      "match_field"
    ]
  },
  "SemanticMetric": {
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
    ]
  },
  "SemanticModel": {
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
    ]
  },
  "SemanticPreAggregation": {
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
    ]
  },
  "SemanticRelationship": {
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
    ]
  },
  "SourceFreshnessStatus": {
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
      "last_loaded_at": {
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
      }
    }
  },
  "StorageCredential": {
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
    ]
  },
  "SubmitQueryRequest": {
    "property_order": [
      "sql",
      "request_id"
    ]
  },
  "TableDetail": {
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
  "Tag": {
    "property_order": [
      "id",
      "key",
      "value",
      "created_by",
      "created_at"
    ]
  },
  "TagAssignment": {
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
      "assigned_at": {
        "schema": {
          "type": "string",
          "format": "date-time"
        }
      }
    }
  },
  "TriggerAssetMaterializationRequest": {
    "property_order": [
      "partition_key",
      "idempotency_key",
      "payload"
    ],
    "properties": {
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
    "property_order": [
      "project_name",
      "environment_name",
      "model_names",
      "full_refresh"
    ]
  },
  "TriggerPipelineRunRequest": {
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
    ]
  },
  "UpdateCatalogRegistrationRequest": {
    "property_order": [
      "data_path",
      "comment"
    ]
  },
  "UpdateCellRequest": {
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
      "visual_spec": {
        "schema": {
          "ref": "VisualSpecUpdate"
        }
      }
    }
  },
  "UpdateColumnMaskRequest": {
    "property_order": [
      "name",
      "column_name",
      "mask_expression",
      "description"
    ]
  },
  "UpdateDashboardRequest": {
    "property_order": [
      "name",
      "description",
      "folder_id"
    ]
  },
  "UpdateDashboardWidgetRequest": {
    "property_order": [
      "name",
      "description",
      "source",
      "visual_spec",
      "layout"
    ],
    "properties": {
      "layout": {
        "schema": {
          "ref": "DashboardWidgetLayoutUpdate"
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
      "business_definitions": {
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
  "UpdateExternalLocationRequest": {
    "property_order": [
      "url",
      "credential_name",
      "comment",
      "read_only"
    ]
  },
  "UpdateFolderRequest": {
    "property_order": [
      "name",
      "git_repo_id",
      "git_root_path",
      "default_project_id",
      "default_environment_id"
    ]
  },
  "UpdateMacroRequest": {
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
  "UpdateModelRequest": {
    "property_order": [
      "sql",
      "materialization",
      "description",
      "tags",
      "config",
      "contract",
      "freshness_policy"
    ]
  },
  "UpdateNotebookRequest": {
    "property_order": [
      "name",
      "description",
      "project_override_id",
      "environment_override_id"
    ]
  },
  "UpdatePipelineJobRequest": {
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
    ]
  },
  "UpdatePipelineRequest": {
    "property_order": [
      "description",
      "schedule_cron",
      "is_paused",
      "concurrency_limit",
      "folder_id"
    ]
  },
  "UpdateRowFilterRequest": {
    "property_order": [
      "name",
      "filter_sql",
      "description"
    ]
  },
  "UpdateSchemaRequest": {
    "properties": {
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
    ]
  },
  "UpdateSemanticModelRequest": {
    "property_order": [
      "description",
      "owner",
      "base_model_ref",
      "default_time_dimension",
      "tags"
    ]
  },
  "UpdateSemanticPreAggregationRequest": {
    "property_order": [
      "metric_set",
      "dimension_set",
      "grain",
      "target_relation",
      "refresh_policy"
    ]
  },
  "UpdateSemanticRelationshipRequest": {
    "property_order": [
      "relationship_type",
      "join_sql",
      "cost",
      "max_hops"
    ]
  },
  "UpdateStorageCredentialRequest": {
    "property_order": [
      "key_id",
      "secret",
      "endpoint",
      "region",
      "url_style",
      "comment"
    ]
  },
  "UpdateTableRequest": {
    "property_order": [
      "comment",
      "properties",
      "owner"
    ],
    "properties": {
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
  "UpdateWorkspaceRequest": {
    "property_order": [
      "name",
      "default_project_id",
      "default_environment_id",
      "git_repo_id",
      "git_root_path"
    ]
  },
  "UploadUrlResponse": {
    "property_order": [
      "upload_url",
      "s3_key",
      "expires_at"
    ]
  },
  "VersionedObjectSummary": {
    "property_order": [
      "total_count",
      "active_count",
      "historical_count",
      "has_history",
      "latest_snapshot_id"
    ]
  },
  "ViewDetail": {
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
    ]
  },
  "VisualEncodings": {
    "property_order": [
      "x",
      "y",
      "series",
      "label",
      "value",
      "secondary"
    ]
  },
  "VisualSpec": {
    "property_order": [
      "kind",
      "chart_type",
      "encodings",
      "title",
      "subtitle",
      "legend",
      "stacked",
      "color_palette"
    ]
  },
  "VolumeDetail": {
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
    ]
  },
  "Workspace": {
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
    ]
  },
  "WorkspaceMember": {
    "property_order": [
      "workspace_id",
      "principal_name",
      "role",
      "created_at",
      "updated_at"
    ]
  }
}
