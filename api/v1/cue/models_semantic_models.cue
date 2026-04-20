package api

// Authored semantic and model schemas.

schemas_semantic_models: {
  CreateMacroRequest: #objectSchema & {
    #fields: {
      name: #nameProperty,
      body: #stringProperty,
      macro_type: #refProperty & {#ref: "MacroType"},
      parameters: #stringArrayProperty,
      description: #descriptionProperty,
      catalog_name: #stringProperty,
      project_name: #stringProperty,
      owner: #ownerProperty,
      properties: #stringMapProperty,
      tags: #stringArrayProperty,
      status: #refProperty & {#ref: "MacroStatus"},
      visibility: #refProperty & {#ref: "MacroVisibility"}
    },
    #required: [
      "name",
      "body"
    ]
  },
  CreateProjectMacroRequest: #objectSchema & {
    #fields: {
      name: #nameProperty,
      body: #stringProperty,
      macro_type: #refProperty & {#ref: "MacroType"},
      parameters: #stringArrayProperty,
      description: #descriptionProperty,
      catalog_name: #stringProperty,
      owner: #ownerProperty,
      properties: #stringMapProperty,
      tags: #stringArrayProperty,
      status: #refProperty & {#ref: "MacroStatus"}
    },
    #required: [
      "name",
      "body"
    ]
  },
  CreateProjectModelRequest: #objectSchema & {
    #fields: {
      name: #nameProperty,
      sql: #stringProperty,
      materialization: #refProperty & {#ref: "ModelMaterialization"},
      description: #descriptionProperty,
      tags: #stringArrayProperty,
      config: #refProperty & {#ref: "ModelConfig"},
      contract: #refProperty & {#ref: "ModelContract"},
      freshness_policy: #refProperty & {#ref: "FreshnessPolicy"}
    },
    #required: [
      "name",
      "sql"
    ]
  },
  CreateModelRequest: #objectSchema & {
    example: {
      project_name:    "revenue"
      name:            "fct_daily_revenue"
      sql:             "select order_date, sum(net_revenue) as daily_revenue from stg_orders group by 1"
      materialization: "TABLE"
      description:     "Daily revenue fact model."
      tags:            ["finance", "gold"]
      config: {
        unique_key:           ["order_date"]
        incremental_strategy: "merge"
        on_schema_change:     "fail"
      }
      contract: {
        enforce: true
        columns: [
          {
            name:     "order_date"
            type:     "DATE"
            nullable: false
          },
          {
            name:     "daily_revenue"
            type:     "DOUBLE"
            nullable: false
          },
        ]
      }
      freshness_policy: {
        max_lag_seconds: 21600
        cron_schedule:   "0 */6 * * *"
      }
    }
    #fields: {
      project_name: #stringProperty,
      name: #nameProperty,
      sql: #stringProperty,
      materialization: #refProperty & {#ref: "ModelMaterialization"},
      description: #descriptionProperty,
      tags: #stringArrayProperty,
      config: #refProperty & {#ref: "ModelConfig"},
      contract: #refProperty & {#ref: "ModelContract"},
      freshness_policy: #refProperty & {#ref: "FreshnessPolicy"}
    },
    #required: [
      "project_name",
      "name",
      "sql"
    ]
  },
  CreateModelTestRequest: #objectSchema & {
    #fields: {
      name: #nameProperty,
      test_type: #refProperty & {#ref: "ModelTestTestType"},
      column: #stringProperty,
      config: #refProperty & {#ref: "ModelTestConfig"},
    },
    #required: [
      "name",
      "test_type"
    ]
  },
  CreateSemanticMetricRequest: #objectSchema & {
    example: {
      name:                "monthly_recurring_revenue"
      description:         "Monthly recurring revenue for active subscriptions."
      label:               "MRR"
      metric_type:         "SUM"
      expression_mode:     "SQL"
      expression:          "subscription_mrr"
      relationship_names:  ["account_to_subscription"]
      filter_sql:          "subscription_status = 'ACTIVE'"
      default_time_grain:  "month"
      format:              "currency_usd"
      certification_state: "CERTIFIED"
    }
    #fields: {
      name: #nameProperty,
      description: #descriptionProperty,
      label: #stringProperty,
      metric_type: #refProperty & {#ref: "SemanticMetricMetricType"},
      expression_mode: #refProperty & {#ref: "SemanticMetricExpressionMode"},
      expression: #stringProperty,
      relationship_names: #stringArrayProperty,
      filter_sql: #stringProperty,
      default_time_grain: #stringProperty,
      format: #stringProperty,
      certification_state: #refProperty & {#ref: "CreateSemanticMetricRequestCertificationState"}
    },
    #required: [
      "name",
      "metric_type",
      "expression"
    ]
  },
  CreateSemanticMetricRequestCertificationState: #enumSchema & {
    #values: [
      "DRAFT",
      "CERTIFIED",
      "DEPRECATED"
    ]
  },
  CreateSemanticModelRequest: #objectSchema & {
    example: {
      name:                   "customer_360"
      description:            "Semantic model for customer lifecycle and commercial performance."
      base_relation_ref:         "revenue.fct_customer_360"
      default_time_dimension: "snapshot_date"
      tags:                   ["growth", "finance"]
    }
    #fields: {
      name: #nameProperty,
      description: #descriptionProperty,
      base_relation_ref: #stringProperty,
      default_time_dimension: #stringProperty,
      tags: #stringArrayProperty
    },
    #required: [
      "name",
      "base_relation_ref"
    ]
  },
  CreateSemanticPreAggregationRequest: #objectSchema & {
    #fields: {
      name: #nameProperty,
      metric_set: #stringArrayProperty,
      dimension_set: #stringArrayProperty,
      grain: #stringProperty,
      refresh_policy: #stringProperty,
      target_relation: #stringProperty
    },
    #required: [
      "name",
      "target_relation"
    ]
  },
  CreateSemanticRelationshipRequest: #objectSchema & {
    #fields: {
      name: #nameProperty,
      from_semantic_id: #stringProperty,
      to_semantic_id: #stringProperty,
      relationship_type: #refProperty & {#ref: "SemanticRelationshipRelationshipType"},
      join_sql: #stringProperty,
      cost: #int32Property,
      max_hops: #int32Property,
    },
    #required: [
      "name",
      "from_semantic_id",
      "to_semantic_id",
      "relationship_type",
      "join_sql"
    ]
  },
  Macro: #objectSchema & {
    example: {
      id:           "macro_01hzysafe_divide"
      name:         "safe_divide"
      macro_type:   "SCALAR"
      parameters:   ["numerator", "denominator"]
      body:         "case when {{ denominator }} = 0 then null else {{ numerator }} / {{ denominator }} end"
      description:  "Safely divides two expressions and returns null on zero denominator."
      catalog_name: "analytics"
      project_name: "revenue"
      visibility:   "project"
      owner:        "team-analytics"
      properties: {
        package: "finance"
      }
      tags:       ["macro", "utility"]
      status:     "ACTIVE"
      created_by: "alice@example.com"
      created_at: "2026-03-01T08:00:00Z"
      updated_at: "2026-04-13T08:00:00Z"
    }
    #fields: {
      id: #idProperty,
      name: #nameProperty,
      macro_type: #refProperty & {#ref: "MacroType"},
      parameters: #stringArrayProperty,
      body: #stringProperty,
      description: #descriptionProperty,
      catalog_name: #stringProperty,
      project_name: #stringProperty,
      visibility: #refProperty & {#ref: "MacroVisibility"},
      owner: #ownerProperty,
      properties: #stringMapProperty,
      tags: #stringArrayProperty,
      status: #refProperty & {#ref: "MacroStatus"},
      created_by: #stringProperty,
      created_at: #createdAtProperty,
      updated_at: #updatedAtProperty,
    }
  },
  MacroImpactList: #objectSchema & {
    #fields: {
      data: #arrayRefProperty & {#ref: "MacroImpactModel"},
      next_page_token: #stringProperty
    },
    #required: [
      "data"
    ]
  },
  MacroImpactModel: #objectSchema & {
    #fields: {
      target_table: #stringProperty,
      target_schema: #stringProperty,
      model_name: #stringProperty,
      last_seen_at: #dateTimeProperty
    }
  },
  MacroRevision: #objectSchema & {
    #fields: {
      id: #idProperty,
      macro_name: #stringProperty,
      version: #int32Property,
      content_hash: #stringProperty,
      parameters: #stringArrayProperty,
      body: #stringProperty,
      description: #descriptionProperty,
      status: #refProperty & {#ref: "MacroStatus"},
      created_by: #stringProperty,
      created_at: #createdAtProperty
    }
  },
  MacroRevisionDiff: #objectSchema & {
    #fields: {
      macro_name: #stringProperty,
      from_version: #int32Property,
      to_version: #int32Property,
      from_content_hash: #stringProperty,
      to_content_hash: #stringProperty,
      changed: #boolProperty,
      parameters_changed: #boolProperty,
      body_changed: #boolProperty,
      description_changed: #boolProperty,
      status_changed: #boolProperty,
      from_parameters: #stringArrayProperty,
      to_parameters: #stringArrayProperty,
      from_body: #stringProperty,
      to_body: #stringProperty,
      from_description: #stringProperty,
      to_description: #stringProperty,
      from_status: #refProperty & {#ref: "MacroStatus"},
      impact_changed: #boolProperty,
      impacted_models_added: #arrayRefProperty & {#ref: "MacroImpactModel"},
      impacted_models_removed: #arrayRefProperty & {#ref: "MacroImpactModel"},
      impacted_models_unchanged: #arrayRefProperty & {#ref: "MacroImpactModel"},
      to_status: #refProperty & {#ref: "MacroStatus"}
    }
  },
  MacroRevisionList: #objectSchema & {
    #fields: {
      data: #arrayRefProperty & {#ref: "MacroRevision"}
    },
    #required: [
      "data"
    ]
  },
  MacroStatus: #enumSchema & {
    #values: [
      "ACTIVE",
      "DEPRECATED"
    ]
  },
  MacroType: #enumSchema & {
    #values: [
      "SCALAR",
      "TABLE"
    ]
  },
  MacroVisibility: #enumSchema & {
    #values: [
      "project"
    ]
  },
  Model: #objectSchema & {
    example: {
      id:             "mdl_01hzydailyrevenue"
      project_name:   "revenue"
      name:           "fct_daily_revenue"
      sql:            "select order_date, sum(net_revenue) as daily_revenue from stg_orders group by 1"
      materialization:"TABLE"
      description:    "Daily revenue fact model."
      owner:          "team-analytics"
      depends_on:     ["stg_orders"]
      tags:           ["finance", "gold"]
      config: {
        unique_key:           ["order_date"]
        incremental_strategy: "merge"
        on_schema_change:     "fail"
      }
      contract: {
        enforce: true
        columns: [
          {
            name:     "order_date"
            type:     "DATE"
            nullable: false
          },
          {
            name:     "daily_revenue"
            type:     "DOUBLE"
            nullable: false
          },
        ]
      }
      freshness_policy: {
        max_lag_seconds: 21600
        cron_schedule:   "0 */6 * * *"
      }
      created_by: "alice@example.com"
      created_at: "2026-03-01T08:00:00Z"
      updated_at: "2026-04-13T08:00:00Z"
    }
    #fields: {
      id: #idProperty,
      project_name: #stringProperty,
      name: #nameProperty,
      sql: #stringProperty,
      materialization: #refProperty & {#ref: "ModelMaterialization"},
      description: #descriptionProperty,
      owner: #ownerProperty,
      depends_on: #stringArrayProperty,
      tags: #stringArrayProperty,
      config: #refProperty & {#ref: "ModelConfig"},
      contract: #refProperty & {#ref: "ModelContract"},
      freshness_policy: #refProperty & {#ref: "FreshnessPolicy"},
      created_by: #stringProperty,
      created_at: #createdAtProperty,
      updated_at: #updatedAtProperty
    }
  },
  ModelConfig: #objectSchema & {
    #fields: {
      unique_key: #stringArrayProperty,
      incremental_strategy: #stringProperty,
      on_schema_change: #refProperty & {#ref: "ModelConfigOnSchemaChange"}
    }
  },
  ModelConfigOnSchemaChange: #enumSchema & {
    #values: [
      "ignore",
      "fail"
    ]
  },
  ModelContract: #objectSchema & {
    #fields: {
      enforce: #boolProperty,
      columns: #arrayRefProperty & {#ref: "ModelContractColumn"}
    }
  },
  ModelContractColumn: #objectSchema & {
    #fields: {
      name: #nameProperty,
      type: #stringProperty,
      nullable: #boolProperty
    },
    #required: [
      "name",
      "type"
    ]
  },
  ModelDAG: #objectSchema & {
    #fields: {
      tiers: #arrayRefProperty & {#ref: "ModelDAGTier"}
    }
  },
  ModelDAGNode: #objectSchema & {
    #fields: {
      project_name: #stringProperty,
      model_name: #stringProperty,
      materialization: #refProperty & {#ref: "ModelMaterialization"},
      depends_on: #stringArrayProperty
    }
  },
  ModelDAGTier: #objectSchema & {
    #fields: {
      tier: #int32Property,
      nodes: #arrayRefProperty & {#ref: "ModelDAGNode"}
    }
  },
  ModelMaterialization: #enumSchema & {
    #values: [
      "VIEW",
      "TABLE",
      "INCREMENTAL",
      "EPHEMERAL",
      "SEED",
      "SNAPSHOT"
    ]
  },
  ModelRun: #objectSchema & {
    example: {
      id:                  "run_01hzymodel"
      status:              "SUCCESS"
      trigger_type:        "manual"
      triggered_by:        "alice@example.com"
      project_name:        "revenue"
      environment_name:    "prod"
      build_id:            "build_01hzyprod123"
      model_names:         ["fct_daily_revenue", "dim_customer"]
      full_refresh:        false
      compile_manifest:    "{...manifest json...}"
      compile_diagnostics: {
        warnings: []
        errors:   []
      }
      started_at:    "2026-04-13T07:00:00Z"
      finished_at:   "2026-04-13T07:05:00Z"
      error_message: ""
      created_at:    "2026-04-13T07:00:00Z"
    }
    #fields: {
      id: #idProperty,
      status: #statusProperty,
      trigger_type: #stringProperty,
      triggered_by: #stringProperty,
      project_name: #stringProperty,
      environment_name: #stringProperty,
      build_id: #stringProperty,
      model_names: #stringArrayProperty,
      full_refresh: #boolProperty,
      compile_manifest: #stringProperty,
      compile_diagnostics: #refProperty & {#ref: "ModelRunCompileDiagnostics"},
      started_at: #dateTimeProperty,
      finished_at: #dateTimeProperty,
      error_message: #stringProperty,
      created_at: #createdAtProperty,
    }
  },
  ModelRunCompileDiagnostics: #objectSchema & {
    #fields: {
      items: #arrayRefProperty & {#ref: "CompileDiagnostic"},
      warnings: #stringArrayProperty,
      errors: #stringArrayProperty
    }
  },
  ModelRunStep: #objectSchema & {
    #fields: {
      id: #idProperty,
      run_id: #stringProperty,
      model_name: #stringProperty,
      compiled_sql: #stringProperty,
      compiled_hash: #stringProperty,
      depends_on: #stringArrayProperty,
      vars_used: #stringArrayProperty,
      macros_used: #stringArrayProperty,
      status: #statusProperty,
      rows_affected: #int64Property,
      started_at: #dateTimeProperty,
      finished_at: #dateTimeProperty,
      error_message: #stringProperty,
      created_at: #createdAtProperty
    }
  },
  ModelRunStepList: #objectSchema & {
    #fields: {
      data: #arrayRefProperty & {#ref: "ModelRunStep"}
    },
    #required: [
      "data"
    ]
  },
  ModelTest: #objectSchema & {
    #fields: {
      id: #idProperty,
      model_id: #stringProperty,
      name: #nameProperty,
      test_type: #refProperty & {#ref: "ModelTestTestType"},
      column: #stringProperty,
      config: #refProperty & {#ref: "ModelTestConfig"},
      created_at: #createdAtProperty
    }
  },
  ModelTestConfig: #objectSchema & {
    #fields: {
      values: #stringArrayProperty,
      to_model: #stringProperty,
      to_column: #stringProperty,
      custom_sql: #stringProperty
    }
  },
  ModelTestList: #objectSchema & {
    #fields: {
      data: #arrayRefProperty & {#ref: "ModelTest"}
    },
    #required: [
      "data"
    ]
  },
  ModelTestResult: #objectSchema & {
    #fields: {
      id: #idProperty,
      run_step_id: #stringProperty,
      test_id: #stringProperty,
      test_name: #stringProperty,
      status: #refProperty & {#ref: "ModelTestResultStatus"},
      rows_returned: #int64Property,
      error_message: #stringProperty,
      created_at: #createdAtProperty
    }
  },
  ModelTestResultList: #objectSchema & {
    #fields: {
      data: #arrayRefProperty & {#ref: "ModelTestResult"}
    },
    #required: [
      "data"
    ]
  },
  ModelTestResultStatus: #enumSchema & {
    #values: [
      "PASS",
      "FAIL",
      "ERROR"
    ]
  },
  ModelTestTestType: #enumSchema & {
    #values: [
      "not_null",
      "unique",
      "accepted_values",
      "relationships",
      "custom_sql"
    ]
  },
  SemanticMetric: #objectSchema & {
    example: {
      id:                  "metric_01hzymrr"
      semantic_model_id:   "sem_01hzycust360"
      name:                "monthly_recurring_revenue"
      description:         "Monthly recurring revenue for active subscriptions."
      label:               "MRR"
      metric_type:         "SUM"
      expression_mode:     "SQL"
      expression:          "subscription_mrr"
      relationship_names:  ["account_to_subscription"]
      filter_sql:          "subscription_status = 'ACTIVE'"
      default_time_grain:  "month"
      format:              "currency_usd"
      owner:               "team-analytics"
      certification_state: "CERTIFIED"
      created_by:          "alice@example.com"
      created_at:          "2026-04-13T08:00:00Z"
      updated_at:          "2026-04-13T08:00:00Z"
    }
    #fields: {
      id: #idProperty,
      semantic_model_id: #stringProperty,
      name: #nameProperty,
      description: #descriptionProperty,
      label: #stringProperty,
      metric_type: #refProperty & {#ref: "SemanticMetricMetricType"},
      expression_mode: #refProperty & {#ref: "SemanticMetricExpressionMode"},
      expression: #stringProperty,
      relationship_names: #stringArrayProperty,
      filter_sql: #stringProperty,
      default_time_grain: #stringProperty,
      format: #stringProperty,
      owner: #ownerProperty,
      certification_state: #refProperty & {#ref: "CreateSemanticMetricRequestCertificationState"},
      created_by: #stringProperty,
      created_at: #createdAtProperty,
      updated_at: #updatedAtProperty
    }
  },
  SemanticMetricExpressionMode: #enumSchema & {
    #values: [
      "DSL",
      "SQL"
    ]
  },
  SemanticMetricList: #objectSchema & {
    #fields: {
      data: #arrayRefProperty & {#ref: "SemanticMetric"}
    },
    #required: [
      "data"
    ]
  },
  SemanticMetricMetricType: #enumSchema & {
    #values: [
      "SUM",
      "COUNT",
      "COUNT_DISTINCT",
      "AVG",
      "MIN",
      "MAX",
      "RATIO"
    ]
  },
  SemanticModel: #objectSchema & {
    example: {
      id:                     "sem_01hzycust360"
      workspace_id:           "ws_01hzworkspace"
      name:                   "customer_360"
      description:            "Semantic model for customer lifecycle and commercial performance."
      owner:                  "team-analytics"
      base_relation_ref:      "revenue.fct_customer_360"
      default_time_dimension: "snapshot_date"
      tags:                   ["growth", "finance"]
      created_by:             "alice@example.com"
      created_at:             "2026-04-13T08:00:00Z"
      updated_at:             "2026-04-13T08:00:00Z"
    }
    #fields: {
      id: #idProperty,
      workspace_id: #stringProperty,
      name: #nameProperty,
      description: #descriptionProperty,
      owner: #ownerProperty,
      base_relation_ref: #stringProperty,
      default_time_dimension: #stringProperty,
      tags: #stringArrayProperty,
      created_by: #stringProperty,
      created_at: #createdAtProperty,
      updated_at: #updatedAtProperty
    }
  },
  SemanticPreAggregation: #objectSchema & {
    #fields: {
      id: #idProperty,
      semantic_model_id: #stringProperty,
      name: #nameProperty,
      metric_set: #stringArrayProperty,
      dimension_set: #stringArrayProperty,
      grain: #stringProperty,
      target_relation: #stringProperty,
      refresh_policy: #stringProperty,
      created_by: #stringProperty,
      created_at: #createdAtProperty,
      updated_at: #updatedAtProperty
    }
  },
  SemanticPreAggregationList: #objectSchema & {
    #fields: {
      data: #arrayRefProperty & {#ref: "SemanticPreAggregation"}
    },
    #required: [
      "data"
    ]
  },
  SemanticRelationship: #objectSchema & {
    #fields: {
      id: #idProperty,
      name: #nameProperty,
      from_semantic_id: #stringProperty,
      to_semantic_id: #stringProperty,
      relationship_type: #refProperty & {#ref: "SemanticRelationshipRelationshipType"},
      join_sql: #stringProperty,
      cost: #int32Property,
      max_hops: #int32Property,
      created_by: #stringProperty,
      created_at: #createdAtProperty,
      updated_at: #updatedAtProperty
    }
  },
  SemanticRelationshipList: #objectSchema & {
    #fields: {
      data: #arrayRefProperty & {#ref: "SemanticRelationship"}
    },
    #required: [
      "data"
    ]
  },
  SemanticRelationshipRelationshipType: #enumSchema & {
    #values: [
      "ONE_TO_ONE",
      "ONE_TO_MANY",
      "MANY_TO_ONE",
      "MANY_TO_MANY"
    ]
  },
  UpdateMacroRequest: #objectSchema & {
    #fields: {
      body: #stringProperty,
      description: #descriptionProperty,
      parameters: #stringArrayProperty,
      status: #refProperty & {#ref: "MacroStatus"},
      catalog_name: #stringProperty,
      project_name: #stringProperty,
      visibility: #refProperty & {#ref: "MacroVisibility"},
      owner: #ownerProperty,
      properties: #stringMapProperty,
      tags: #stringArrayProperty
    }
  },
  UpdateProjectMacroRequest: #objectSchema & {
    #fields: {
      body: #stringProperty,
      description: #descriptionProperty,
      parameters: #stringArrayProperty,
      status: #refProperty & {#ref: "MacroStatus"},
      catalog_name: #stringProperty,
      owner: #ownerProperty,
      properties: #stringMapProperty,
      tags: #stringArrayProperty
    }
  },
  UpdateModelRequest: #objectSchema & {
    #fields: {
      sql: #stringProperty,
      materialization: #refProperty & {#ref: "ModelMaterialization"},
      description: #descriptionProperty,
      tags: #stringArrayProperty,
      config: #refProperty & {#ref: "ModelConfig"},
      contract: #refProperty & {#ref: "ModelContract"},
      freshness_policy: #refProperty & {#ref: "FreshnessPolicy"}
    }
  },
  UpdateSemanticMetricRequest: #objectSchema & {
    #fields: {
      description: #descriptionProperty,
      label: #stringProperty,
      metric_type: #refProperty & {#ref: "SemanticMetricMetricType"},
      expression_mode: #refProperty & {#ref: "SemanticMetricExpressionMode"},
      expression: #stringProperty,
      relationship_names: #stringArrayProperty,
      filter_sql: #stringProperty,
      default_time_grain: #stringProperty,
      format: #stringProperty,
      owner: #ownerProperty,
      certification_state: #refProperty & {#ref: "CreateSemanticMetricRequestCertificationState"}
    }
  },
  UpdateSemanticModelRequest: #objectSchema & {
    #fields: {
      description: #descriptionProperty,
      owner: #ownerProperty,
      base_relation_ref: #stringProperty,
      default_time_dimension: #stringProperty,
      tags: #stringArrayProperty
    }
  },
  UpdateSemanticPreAggregationRequest: #objectSchema & {
    #fields: {
      metric_set: #stringArrayProperty,
      dimension_set: #stringArrayProperty,
      grain: #stringProperty,
      refresh_policy: #stringProperty,
      target_relation: #stringProperty
    }
  },
  UpdateSemanticRelationshipRequest: #objectSchema & {
    #fields: {
      relationship_type: #refProperty & {#ref: "SemanticRelationshipRelationshipType"},
      join_sql: #stringProperty,
      cost: #int32Property,
      max_hops: #int32Property,
    }
  },
  VersionedObjectSummary: #objectSchema & {
    #fields: {
      total_count: #int64Property,
      active_count: #int64Property,
      historical_count: #int64Property,
      has_history: #boolProperty,
      latest_snapshot_id: #int64Property
    }
  },
}
