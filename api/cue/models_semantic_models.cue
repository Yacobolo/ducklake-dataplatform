package api

// Authored semantic and model schemas.

schemas_semantic_models: {
  CreateMacroRequest: #objectSchema & {
    #fields: {
      body: #stringProperty,
      catalog_name: #stringProperty,
      description: #descriptionProperty,
      macro_type: #refProperty & {#ref: "MacroType"},
      name: #nameProperty,
      owner: #ownerProperty,
      parameters: #stringArrayProperty,
      project_name: #stringProperty,
      properties: #refProperty & {#ref: "Record"},
      status: #refProperty & {#ref: "MacroStatus"},
      tags: #stringArrayProperty,
      visibility: #refProperty & {#ref: "MacroVisibility"}
    },
    #required: [
      "name",
      "body"
    ]
  },
  CreateModelRequest: #objectSchema & {
    #fields: {
      config: #refProperty & {#ref: "ModelConfig"},
      contract: #refProperty & {#ref: "ModelContract"},
      description: #descriptionProperty,
      freshness_policy: #refProperty & {#ref: "FreshnessPolicy"},
      materialization: #refProperty & {#ref: "ModelMaterialization"},
      name: #nameProperty,
      project_name: #stringProperty,
      sql: #stringProperty,
      tags: #stringArrayProperty
    },
    #required: [
      "project_name",
      "name",
      "sql"
    ]
  },
  CreateModelTestRequest: #objectSchema & {
    #fields: {
      column: #stringProperty,
      config: #refProperty & {#ref: "ModelTestConfig"},
      name: #nameProperty,
      test_type: #refProperty & {#ref: "ModelTestTestType"}
    },
    #required: [
      "name",
      "test_type"
    ]
  },
  CreateSemanticMetricRequest: #objectSchema & {
    #fields: {
      certification_state: #refProperty & {#ref: "CreateSemanticMetricRequestCertificationState"},
      default_time_grain: #stringProperty,
      description: #descriptionProperty,
      expression: #stringProperty,
      expression_mode: #refProperty & {#ref: "SemanticMetricExpressionMode"},
      filter_sql: #stringProperty,
      format: #stringProperty,
      label: #stringProperty,
      metric_type: #refProperty & {#ref: "SemanticMetricMetricType"},
      name: #nameProperty,
      relationship_names: #stringArrayProperty
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
    #fields: {
      base_model_ref: #stringProperty,
      default_time_dimension: #stringProperty,
      description: #descriptionProperty,
      name: #nameProperty,
      tags: #stringArrayProperty
    },
    #required: [
      "name",
      "base_model_ref"
    ]
  },
  CreateSemanticPreAggregationRequest: #objectSchema & {
    #fields: {
      dimension_set: #stringArrayProperty,
      grain: #stringProperty,
      metric_set: #stringArrayProperty,
      name: #nameProperty,
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
      cost: #int32Property,
      from_semantic_id: #stringProperty,
      join_sql: #stringProperty,
      max_hops: #int32Property,
      name: #nameProperty,
      relationship_type: #refProperty & {#ref: "SemanticRelationshipRelationshipType"},
      to_semantic_id: #stringProperty
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
    #fields: {
      body: #stringProperty,
      catalog_name: #stringProperty,
      created_at: #createdAtProperty,
      created_by: #stringProperty,
      description: #descriptionProperty,
      id: #idProperty,
      macro_type: #refProperty & {#ref: "MacroType"},
      name: #nameProperty,
      owner: #ownerProperty,
      parameters: #stringArrayProperty,
      project_name: #stringProperty,
      properties: #refProperty & {#ref: "Record"},
      status: #refProperty & {#ref: "MacroStatus"},
      tags: #stringArrayProperty,
      updated_at: #updatedAtProperty,
      visibility: #refProperty & {#ref: "MacroVisibility"}
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
      last_seen_at: #stringProperty,
      model_name: #stringProperty,
      target_schema: #stringProperty,
      target_table: #stringProperty
    }
  },
  MacroRevision: #objectSchema & {
    #fields: {
      body: #stringProperty,
      content_hash: #stringProperty,
      created_at: #createdAtProperty,
      created_by: #stringProperty,
      description: #descriptionProperty,
      id: #idProperty,
      macro_name: #stringProperty,
      parameters: #stringArrayProperty,
      status: #refProperty & {#ref: "MacroStatus"},
      version: #int32Property
    }
  },
  MacroRevisionDiff: #objectSchema & {
    #fields: {
      body_changed: #boolProperty,
      changed: #boolProperty,
      description_changed: #boolProperty,
      from_body: #stringProperty,
      from_content_hash: #stringProperty,
      from_description: #stringProperty,
      from_parameters: #stringArrayProperty,
      from_status: #refProperty & {#ref: "MacroStatus"},
      from_version: #int32Property,
      impact_changed: #boolProperty,
      impacted_models_added: #arrayRefProperty & {#ref: "MacroImpactModel"},
      impacted_models_removed: #arrayRefProperty & {#ref: "MacroImpactModel"},
      impacted_models_unchanged: #arrayRefProperty & {#ref: "MacroImpactModel"},
      macro_name: #stringProperty,
      parameters_changed: #boolProperty,
      status_changed: #boolProperty,
      to_body: #stringProperty,
      to_content_hash: #stringProperty,
      to_description: #stringProperty,
      to_parameters: #stringArrayProperty,
      to_status: #refProperty & {#ref: "MacroStatus"},
      to_version: #int32Property
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
      "project",
      "catalog_global",
      "system"
    ]
  },
  Model: #objectSchema & {
    #fields: {
      config: #refProperty & {#ref: "ModelConfig"},
      contract: #refProperty & {#ref: "ModelContract"},
      created_at: #createdAtProperty,
      created_by: #stringProperty,
      depends_on: #stringArrayProperty,
      description: #descriptionProperty,
      freshness_policy: #refProperty & {#ref: "FreshnessPolicy"},
      id: #idProperty,
      materialization: #refProperty & {#ref: "ModelMaterialization"},
      name: #nameProperty,
      owner: #ownerProperty,
      project_name: #stringProperty,
      sql: #stringProperty,
      tags: #stringArrayProperty,
      updated_at: #updatedAtProperty
    }
  },
  ModelConfig: #objectSchema & {
    #fields: {
      incremental_strategy: #stringProperty,
      on_schema_change: #refProperty & {#ref: "ModelConfigOnSchemaChange"},
      unique_key: #stringArrayProperty
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
      columns: #arrayRefProperty & {#ref: "ModelContractColumn"},
      enforce: #boolProperty
    }
  },
  ModelContractColumn: #objectSchema & {
    #fields: {
      name: #nameProperty,
      nullable: #boolProperty,
      type: #stringProperty
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
      depends_on: #stringArrayProperty,
      materialization: #refProperty & {#ref: "ModelMaterialization"},
      model_name: #stringProperty,
      project_name: #stringProperty
    }
  },
  ModelDAGTier: #objectSchema & {
    #fields: {
      nodes: #arrayRefProperty & {#ref: "ModelDAGNode"},
      tier: #int32Property
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
    #fields: {
      build_id: #stringProperty,
      compile_diagnostics: #refProperty & {#ref: "ModelRunCompileDiagnostics"},
      compile_manifest: #stringProperty,
      created_at: #createdAtProperty,
      environment_name: #stringProperty,
      error_message: #stringProperty,
      finished_at: #stringProperty,
      full_refresh: #boolProperty,
      id: #idProperty,
      model_names: #stringArrayProperty,
      project_name: #stringProperty,
      started_at: #stringProperty,
      status: #statusProperty,
      trigger_type: #stringProperty,
      triggered_by: #stringProperty
    }
  },
  ModelRunCompileDiagnostics: #objectSchema & {
    #fields: {
      errors: #stringArrayProperty,
      warnings: #stringArrayProperty
    }
  },
  ModelRunStep: #objectSchema & {
    #fields: {
      compiled_hash: #stringProperty,
      compiled_sql: #stringProperty,
      created_at: #createdAtProperty,
      depends_on: #stringArrayProperty,
      error_message: #stringProperty,
      finished_at: #stringProperty,
      id: #idProperty,
      macros_used: #stringArrayProperty,
      model_name: #stringProperty,
      rows_affected: #int64Property,
      run_id: #stringProperty,
      started_at: #stringProperty,
      status: #statusProperty,
      vars_used: #stringArrayProperty
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
      column: #stringProperty,
      config: #refProperty & {#ref: "ModelTestConfig"},
      created_at: #createdAtProperty,
      id: #idProperty,
      model_id: #stringProperty,
      name: #nameProperty,
      test_type: #refProperty & {#ref: "ModelTestTestType"}
    }
  },
  ModelTestConfig: #objectSchema & {
    #fields: {
      custom_sql: #stringProperty,
      to_column: #stringProperty,
      to_model: #stringProperty,
      values: #stringArrayProperty
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
      created_at: #createdAtProperty,
      error_message: #stringProperty,
      id: #idProperty,
      rows_returned: #int64Property,
      run_step_id: #stringProperty,
      status: #refProperty & {#ref: "ModelTestResultStatus"},
      test_id: #stringProperty,
      test_name: #stringProperty
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
    #fields: {
      certification_state: #refProperty & {#ref: "CreateSemanticMetricRequestCertificationState"},
      created_at: #createdAtProperty,
      created_by: #stringProperty,
      default_time_grain: #stringProperty,
      description: #descriptionProperty,
      expression: #stringProperty,
      expression_mode: #refProperty & {#ref: "SemanticMetricExpressionMode"},
      filter_sql: #stringProperty,
      format: #stringProperty,
      id: #idProperty,
      label: #stringProperty,
      metric_type: #refProperty & {#ref: "SemanticMetricMetricType"},
      name: #nameProperty,
      owner: #ownerProperty,
      relationship_names: #stringArrayProperty,
      semantic_model_id: #stringProperty,
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
    #fields: {
      base_model_ref: #stringProperty,
      created_at: #createdAtProperty,
      created_by: #stringProperty,
      default_time_dimension: #stringProperty,
      description: #descriptionProperty,
      id: #idProperty,
      name: #nameProperty,
      owner: #ownerProperty,
      tags: #stringArrayProperty,
      updated_at: #updatedAtProperty
    }
  },
  SemanticPreAggregation: #objectSchema & {
    #fields: {
      created_at: #createdAtProperty,
      created_by: #stringProperty,
      dimension_set: #stringArrayProperty,
      grain: #stringProperty,
      id: #idProperty,
      metric_set: #stringArrayProperty,
      name: #nameProperty,
      refresh_policy: #stringProperty,
      semantic_model_id: #stringProperty,
      target_relation: #stringProperty,
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
      cost: #int32Property,
      created_at: #createdAtProperty,
      created_by: #stringProperty,
      from_semantic_id: #stringProperty,
      id: #idProperty,
      join_sql: #stringProperty,
      max_hops: #int32Property,
      name: #nameProperty,
      relationship_type: #refProperty & {#ref: "SemanticRelationshipRelationshipType"},
      to_semantic_id: #stringProperty,
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
      catalog_name: #stringProperty,
      description: #descriptionProperty,
      owner: #ownerProperty,
      parameters: #stringArrayProperty,
      project_name: #stringProperty,
      properties: #refProperty & {#ref: "Record"},
      status: #refProperty & {#ref: "MacroStatus"},
      tags: #stringArrayProperty,
      visibility: #refProperty & {#ref: "MacroVisibility"}
    }
  },
  UpdateModelRequest: #objectSchema & {
    #fields: {
      config: #refProperty & {#ref: "ModelConfig"},
      contract: #refProperty & {#ref: "ModelContract"},
      description: #descriptionProperty,
      freshness_policy: #refProperty & {#ref: "FreshnessPolicy"},
      materialization: #refProperty & {#ref: "ModelMaterialization"},
      sql: #stringProperty,
      tags: #stringArrayProperty
    }
  },
  UpdateSemanticMetricRequest: #objectSchema & {
    #fields: {
      certification_state: #refProperty & {#ref: "CreateSemanticMetricRequestCertificationState"},
      default_time_grain: #stringProperty,
      description: #descriptionProperty,
      expression: #stringProperty,
      expression_mode: #refProperty & {#ref: "SemanticMetricExpressionMode"},
      filter_sql: #stringProperty,
      format: #stringProperty,
      label: #stringProperty,
      metric_type: #refProperty & {#ref: "SemanticMetricMetricType"},
      owner: #ownerProperty,
      relationship_names: #stringArrayProperty
    }
  },
  UpdateSemanticModelRequest: #objectSchema & {
    #fields: {
      base_model_ref: #stringProperty,
      default_time_dimension: #stringProperty,
      description: #descriptionProperty,
      owner: #ownerProperty,
      tags: #stringArrayProperty
    }
  },
  UpdateSemanticPreAggregationRequest: #objectSchema & {
    #fields: {
      dimension_set: #stringArrayProperty,
      grain: #stringProperty,
      metric_set: #stringArrayProperty,
      refresh_policy: #stringProperty,
      target_relation: #stringProperty
    }
  },
  UpdateSemanticRelationshipRequest: #objectSchema & {
    #fields: {
      cost: #int32Property,
      join_sql: #stringProperty,
      max_hops: #int32Property,
      relationship_type: #refProperty & {#ref: "SemanticRelationshipRelationshipType"}
    }
  },
  VersionedObjectSummary: #objectSchema & {
    #fields: {
      active_count: #int64Property,
      has_history: #boolProperty,
      historical_count: #int64Property,
      latest_snapshot_id: #int64Property,
      total_count: #int64Property
    }
  },
}
