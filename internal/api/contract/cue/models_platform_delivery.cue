package api

// Authored platform delivery schemas.

schemas_platform_delivery: {
  Build: #objectSchema & {
    #fields: {
      id: #idProperty,
      project_id: #stringProperty,
      project_name: #stringProperty,
      product_id: #stringProperty,
      environment_id: #stringProperty,
      environment_name: #stringProperty,
      state: #refProperty & {#ref: "BuildState"},
      git_ref: #stringProperty,
      commit_sha: #stringProperty,
      selector: #stringProperty,
      target_catalog: #stringProperty,
      target_schema: #stringProperty,
      source_model_run_id: #stringProperty,
      compile_manifest: #stringProperty,
      compile_diagnostics: #stringProperty,
      created_at: #createdAtProperty,
    },
    #required: [
      "git_ref",
      "target_catalog",
      "target_schema",
      "compile_manifest"
    ]
  },
  BuildState: #enumSchema & {
    #values: [
      "draft",
      "ready",
      "released",
      "superseded"
    ]
  },
  CreateBuildRequest: #objectSchema & {
    #fields: {
      environment_name: #stringProperty,
      git_ref: #stringProperty,
      commit_sha: #stringProperty,
      selector: #stringProperty,
      target_catalog: #stringProperty,
      target_schema: #stringProperty,
      source_model_run_id: #stringProperty,
      compile_manifest: #stringProperty,
      compile_diagnostics: #stringProperty
    },
    #required: [
      "environment_name",
      "git_ref",
      "target_catalog",
      "target_schema",
      "compile_manifest"
    ]
  },
  CreateExternalLocationRequest: #objectSchema & {
    #fields: {
      name: #nameProperty,
      url: #stringProperty,
      credential_name: #stringProperty,
      storage_type: #refProperty & {#ref: "StorageType"},
      comment: #commentProperty,
      read_only: #boolProperty
    },
    #required: [
      "name",
      "url"
    ]
  },
  CreateGitRepoRequest: #objectSchema & {
    #fields: {
      url: #stringProperty,
      branch: #stringProperty,
      path: #stringProperty,
      auth_token: #stringProperty
    },
    #required: [
      "url",
      "branch"
    ]
  },
  CreatePipelineJobRequest: #objectSchema & {
    #fields: {
      name: #nameProperty,
      notebook_id: #stringProperty,
      compute_endpoint_id: #stringProperty,
      depends_on: #stringArrayProperty,
      timeout_seconds: #int64Property,
      retry_count: #int32Property,
      job_order: #int32Property,
      job_type: #refProperty & {#ref: "PipelineJobJobType"},
      model_selector: #stringProperty
    },
    #required: [
      "name"
    ]
  },
  CreatePipelineRequest: #objectSchema & {
    #fields: {
      name: #nameProperty,
      description: #descriptionProperty,
      schedule_cron: #stringProperty,
      is_paused: #boolProperty,
      concurrency_limit: #int32Property,
      folder_id: #stringProperty
    },
    #required: [
      "name"
    ]
  },
  Environment: #objectSchema & {
    #fields: {
      id: #idProperty,
      project_id: #stringProperty,
      project_name: #stringProperty,
      name: #nameProperty,
      kind: #refProperty & {#ref: "EnvironmentKind"},
      description: #descriptionProperty,
      target_catalog: #stringProperty,
      target_schema: #stringProperty,
      compute_endpoint: #stringProperty,
      defer_to_environment: #stringProperty,
      variables: #stringMapProperty,
      source_overrides: #stringMapProperty,
      created_at: #createdAtProperty,
      updated_at: #updatedAtProperty,
    },
    #required: [
      "name",
      "kind",
      "target_catalog",
      "target_schema"
    ]
  },
  EnvironmentKind: #enumSchema & {
    #values: [
      "development",
      "staging",
      "production"
    ]
  },
  ExternalLocation: #objectSchema & {
    #fields: {
      id: #idProperty,
      name: #nameProperty,
      url: #stringProperty,
      credential_name: #stringProperty,
      storage_type: #refProperty & {#ref: "StorageType"},
      comment: #commentProperty,
      owner: #ownerProperty,
      read_only: #boolProperty,
      created_at: #createdAtProperty,
      updated_at: #updatedAtProperty,
    },
    #required: [
      "id",
      "name",
      "url"
    ]
  },
  GitRepo: #objectSchema & {
    #fields: {
      id: #idProperty,
      url: #stringProperty,
      branch: #stringProperty,
      path: #stringProperty,
      owner: #ownerProperty,
      last_sync_at: #dateTimeProperty,
      last_commit: #stringProperty,
      created_at: #createdAtProperty,
      updated_at: #updatedAtProperty,
    }
  },
  Pipeline: #objectSchema & {
    #fields: {
      id: #idProperty,
      name: #nameProperty,
      description: #descriptionProperty,
      schedule_cron: #stringProperty,
      is_paused: #boolProperty,
      concurrency_limit: #int32Property,
      created_by: #stringProperty,
      folder_id: #stringProperty,
      created_at: #createdAtProperty,
      updated_at: #updatedAtProperty
    }
  },
  PipelineJob: #objectSchema & {
    #fields: {
      id: #idProperty,
      pipeline_id: #stringProperty,
      name: #nameProperty,
      notebook_id: #stringProperty,
      compute_endpoint_id: #stringProperty,
      depends_on: #stringArrayProperty,
      timeout_seconds: #int64Property,
      retry_count: #int32Property,
      job_order: #int32Property,
      job_type: #refProperty & {#ref: "PipelineJobJobType"},
      model_selector: #stringProperty,
      created_at: #createdAtProperty
    }
  },
  PipelineJobJobType: #enumSchema & {
    #values: [
      "NOTEBOOK",
      "MODEL_RUN"
    ]
  },
  PipelineJobList: #objectSchema & {
    #fields: {
      data: #arrayRefProperty & {#ref: "PipelineJob"}
    },
    #required: [
      "data"
    ]
  },
  PipelineJobRun: #objectSchema & {
    #fields: {
      id: #idProperty,
      run_id: #stringProperty,
      job_id: #stringProperty,
      job_name: #stringProperty,
      status: #refProperty & {#ref: "PipelineJobRunStatus"},
      started_at: #dateTimeProperty,
      finished_at: #dateTimeProperty,
      error_message: #stringProperty,
      retry_attempt: #int32Property,
      created_at: #createdAtProperty
    }
  },
  PipelineJobRunList: #objectSchema & {
    #fields: {
      data: #arrayRefProperty & {#ref: "PipelineJobRun"}
    },
    #required: [
      "data"
    ]
  },
  PipelineJobRunStatus: #enumSchema & {
    #values: [
      "PENDING",
      "RUNNING",
      "SUCCESS",
      "FAILED",
      "SKIPPED",
      "CANCELLED"
    ]
  },
  PipelineRun: #objectSchema & {
    #fields: {
      id: #idProperty,
      pipeline_id: #stringProperty,
      status: #refProperty & {#ref: "PipelineRunStatus"},
      trigger_type: #refProperty & {#ref: "PipelineRunTriggerType"},
      triggered_by: #stringProperty,
      parameters: #stringMapProperty,
      git_commit_hash: #stringProperty,
      started_at: #dateTimeProperty,
      finished_at: #dateTimeProperty,
      error_message: #stringProperty,
      created_at: #createdAtProperty
    }
  },
  PipelineRunStatus: #enumSchema & {
    #values: [
      "PENDING",
      "RUNNING",
      "SUCCESS",
      "FAILED",
      "CANCELLED"
    ]
  },
  PipelineRunTriggerType: #enumSchema & {
    #values: [
      "MANUAL",
      "SCHEDULED"
    ]
  },
  TriggerPipelineRunRequest: #objectSchema & {
    #fields: {
      parameters: #stringMapProperty
    }
  },
  UpdateExternalLocationRequest: #objectSchema & {
    #fields: {
      url: #stringProperty,
      credential_name: #stringProperty,
      comment: #commentProperty,
      read_only: #boolProperty
    }
  },
  UpdatePipelineJobRequest: #objectSchema & {
    #fields: {
      name: #nameProperty,
      notebook_id: #stringProperty,
      compute_endpoint_id: #stringProperty,
      depends_on: #stringArrayProperty,
      timeout_seconds: #int64Property,
      retry_count: #int32Property,
      job_order: #int32Property,
      job_type: #refProperty & {#ref: "PipelineJobJobType"},
      model_selector: #stringProperty
    }
  },
  UpdatePipelineRequest: #objectSchema & {
    #fields: {
      description: #descriptionProperty,
      schedule_cron: #stringProperty,
      is_paused: #boolProperty,
      concurrency_limit: #int32Property,
      folder_id: #stringProperty
    }
  },
}
