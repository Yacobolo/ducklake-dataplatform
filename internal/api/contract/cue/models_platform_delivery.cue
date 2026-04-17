package api

// Authored platform delivery schemas.

schemas_platform_delivery: {
  Build: #objectSchema & {
    example: {
      id:                  "build_01hzyprod123"
      project_id:          "prj_revenue"
      project_name:        "revenue"
      product_id:          "prd_01hzycust360"
      environment_id:      "env_prod"
      environment_name:    "prod"
      state:               "released"
      git_ref:             "refs/heads/main"
      commit_sha:          "8f3d9e2a"
      selector:            "tag:daily"
      target_catalog:      "analytics"
      target_schema:       "mart"
      source_model_run_id: "run_01hzymodel"
      resolved_release_id: "rel_01hzylib"
      compile_manifest:    "{...manifest json...}"
      compile_diagnostics: {
        items: []
      }
      state_snapshot: {
        version: 1
      }
      created_at:          "2026-04-13T07:00:00Z"
    }
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
      resolved_release_id: #stringProperty,
      compile_manifest: #stringProperty,
      compile_diagnostics: #refProperty & {#ref: "ModelRunCompileDiagnostics"},
      state_snapshot: #refProperty & {#ref: "BuildStateSnapshot"},
      created_at: #createdAtProperty,
    },
    #required: [
      "git_ref",
      "target_catalog",
      "target_schema",
      "compile_manifest"
    ]
  },
  Compilation: #objectSchema & {
    #fields: {
      id: #idProperty,
      project_id: #stringProperty,
      project_name: #stringProperty,
      environment_id: #stringProperty,
      environment_name: #stringProperty,
      git_ref: #stringProperty,
      commit_sha: #stringProperty,
      selector: #stringProperty,
      target_catalog: #stringProperty,
      target_schema: #stringProperty,
      resolved_release_id: #stringProperty,
      compile_manifest: #stringProperty,
      compile_diagnostics: #refProperty & {#ref: "ModelRunCompileDiagnostics"},
      state_snapshot: #refProperty & {#ref: "BuildStateSnapshot"},
      created_at: #createdAtProperty,
    },
    #required: [
      "id",
      "project_id",
      "environment_id",
      "git_ref",
      "target_catalog",
      "target_schema",
      "compile_manifest"
    ]
  },
  CreateCompilationRequest: #objectSchema & {
    #fields: {
      git_ref: #stringProperty,
      commit_sha: #stringProperty,
      selector: #stringProperty,
      target_catalog: #stringProperty,
      target_schema: #stringProperty
    },
    #required: [
      "git_ref"
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
    example: {
      environment_name:    "prod"
      git_ref:             "refs/heads/main"
      commit_sha:          "8f3d9e2a"
      selector:            "tag:daily"
      target_catalog:      "analytics"
      target_schema:       "mart"
      source_model_run_id: "run_01hzymodel"
      compile_manifest:    "{...manifest json...}"
      compile_diagnostics: "{...diagnostics json...}"
      state_snapshot:      "{...snapshot json...}"
    }
    #fields: {
      environment_name: #stringProperty,
      git_ref: #stringProperty,
      commit_sha: #stringProperty,
      selector: #stringProperty,
      target_catalog: #stringProperty,
      target_schema: #stringProperty,
      source_model_run_id: #stringProperty,
      compile_manifest: #stringProperty,
      compile_diagnostics: #stringProperty,
      state_snapshot: #stringProperty
    },
    #required: [
      "environment_name",
      "git_ref",
      "target_catalog",
      "target_schema",
      "compile_manifest"
    ]
  },
  CreateBuildRunRequest: #objectSchema & {
    #fields: {},
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
      run_as_principal: #stringProperty,
      admission_mode: #refProperty & {#ref: "PipelineAdmissionMode"},
      max_run_duration_seconds: #int64Property,
      notification_webhooks: #arrayRefProperty & {#ref: "PipelineNotificationWebhook"},
      default_retry_count: #int32Property,
      default_timeout_seconds: #int64Property,
      default_compute_endpoint_id: #stringProperty,
      folder_id: #stringProperty
    },
    #required: [
      "name"
    ]
  },
  Environment: #objectSchema & {
    example: {
      id:               "env_prod"
      project_id:       "prj_revenue"
      project_name:     "revenue"
      name:             "prod"
      kind:             "production"
      description:      "Production environment for revenue reporting."
      target_catalog:   "analytics"
      target_schema:    "mart"
      compute_endpoint: "analytics-prod"
      defer_to_environment: ""
      variables: {
        dbt_target: "prod"
      }
      source_overrides: {
        raw_schema: "landing"
      }
      created_at: "2026-03-01T08:00:00Z"
      updated_at: "2026-04-13T08:00:00Z"
    }
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
  ProjectRelease: #objectSchema & {
    #fields: {
      id: #idProperty,
      project_id: #stringProperty,
      project_name: #stringProperty,
      version: #stringProperty,
      resolved_build_id: #stringProperty,
      resolved_compilation_id: #stringProperty,
      created_at: #createdAtProperty,
    },
    #required: [
      "id",
      "project_id",
      "version"
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
    example: {
      id:           "repo_revops"
      url:          "https://github.com/example/revenue-analytics.git"
      branch:       "main"
      path:         "analytics/revenue"
      owner:        "team-analytics"
      last_sync_at: "2026-04-13T08:45:00Z"
      last_commit:  "8f3d9e2a"
      created_at:   "2026-03-01T08:00:00Z"
      updated_at:   "2026-04-13T08:45:00Z"
    }
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
    example: {
      id:                "pipe_01hzydailyrev"
      name:              "daily-revenue-refresh"
      description:       "Refreshes revenue notebooks and downstream models every morning."
      schedule_cron:     "0 6 * * *"
      is_paused:         false
      concurrency_limit: 1
      run_as_principal:  "service:finance-pipeline"
      admission_mode:    "QUEUE"
      max_run_duration_seconds: 1800
      notification_webhooks: [
        {
          url: "https://hooks.example.com/pipelines"
          events: ["FAILED", "SLA_BREACHED"]
        }
      ]
      default_retry_count: 2
      default_timeout_seconds: 900
      default_compute_endpoint_id: "cmp_prod"
      created_by:        "alice@example.com"
      folder_id:         "fld_01hzyfinance"
      created_at:        "2026-04-01T08:00:00Z"
      updated_at:        "2026-04-13T08:00:00Z"
    }
    #fields: {
      id: #idProperty,
      name: #nameProperty,
      description: #descriptionProperty,
      schedule_cron: #stringProperty,
      is_paused: #boolProperty,
      concurrency_limit: #int32Property,
      run_as_principal: #stringProperty,
      admission_mode: #refProperty & {#ref: "PipelineAdmissionMode"},
      max_run_duration_seconds: #int64Property,
      notification_webhooks: #arrayRefProperty & {#ref: "PipelineNotificationWebhook"},
      default_retry_count: #int32Property,
      default_timeout_seconds: #int64Property,
      default_compute_endpoint_id: #stringProperty,
      created_by: #stringProperty,
      folder_id: #stringProperty,
      created_at: #createdAtProperty,
      updated_at: #updatedAtProperty
    }
  },
  PipelineAdmissionMode: #enumSchema & {
    #values: [
      "REJECT",
      "QUEUE"
    ]
  },
  PipelineNotificationWebhook: #objectSchema & {
    #fields: {
      url: #stringProperty,
      events: #stringArrayProperty
    },
    #required: ["url"]
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
      effective_compute_endpoint_id: #stringProperty,
      attempt_count: #int32Property,
      last_error_code: #stringProperty,
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
    example: {
      id:           "pirun_01hzydailyrev"
      pipeline_id:  "pipe_01hzydailyrev"
      status:       "SUCCESS"
      trigger_type: "SCHEDULED"
      triggered_by: "system:scheduler"
      effective_principal: "service:finance-pipeline"
      queued_at: "2026-04-13T05:59:00Z"
      queue_started_at: "2026-04-13T06:00:00Z"
      repaired_from_run_id: "pirun_01hzyprev"
      started_at:   "2026-04-13T06:00:00Z"
      finished_at:  "2026-04-13T06:14:00Z"
      provenance: {
        trigger_type: "SCHEDULED"
        triggered_by: "system:scheduler"
        effective_principal: "service:finance-pipeline"
        pipeline_definition_version: "2026-04-13T05:55:00Z"
      }
      error_message:""
      created_at:   "2026-04-13T06:00:00Z"
      updated_at:   "2026-04-13T06:14:00Z"
    }
    #fields: {
      id: #idProperty,
      pipeline_id: #stringProperty,
      status: #refProperty & {#ref: "PipelineRunStatus"},
      trigger_type: #refProperty & {#ref: "PipelineRunTriggerType"},
      triggered_by: #stringProperty,
      effective_principal: #stringProperty,
      parameters: #stringMapProperty,
      git_commit_hash: #stringProperty,
      queued_at: #dateTimeProperty,
      queue_started_at: #dateTimeProperty,
      started_at: #dateTimeProperty,
      finished_at: #dateTimeProperty,
      repaired_from_run_id: #stringProperty,
      provenance: #refProperty & {#ref: "PipelineRunProvenance"},
      error_message: #stringProperty,
      created_at: #createdAtProperty
    }
  },
  PipelineRunProvenance: #objectSchema & {
    #fields: {
      trigger_type: #stringProperty,
      triggered_by: #stringProperty,
      effective_principal: #stringProperty,
      pipeline_definition_version: #stringProperty,
      notebooks: #arrayRefProperty & {#ref: "PipelineNotebookProvenance"},
      models: #arrayRefProperty & {#ref: "PipelineModelProvenance"}
    }
  },
  PipelineNotebookProvenance: #objectSchema & {
    #fields: {
      notebook_id: #stringProperty,
      git_repo_id: #stringProperty,
      git_commit_sha: #stringProperty,
      last_updated_at: #dateTimeProperty
    },
    #required: ["notebook_id"]
  },
  PipelineModelProvenance: #objectSchema & {
    #fields: {
      selector: #stringProperty,
      model_id: #stringProperty,
      last_updated_at: #dateTimeProperty
    },
    #required: ["selector"]
  },
  PipelineRunEvent: #objectSchema & {
    #fields: {
      id: #idProperty,
      run_id: #stringProperty,
      job_run_id: #stringProperty,
      event_type: #stringProperty,
      message: #stringProperty,
      error_code: #stringProperty,
      metadata: #anyMapProperty,
      created_at: #createdAtProperty
    }
  },
  PipelineRunEventList: #objectSchema & {
    #fields: {
      data: #arrayRefProperty & {#ref: "PipelineRunEvent"}
    },
    #required: ["data"]
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
  RepairPipelineRunRequest: #objectSchema & {
    #fields: {
      mode: #stringProperty,
      from_job_id: #stringProperty
    },
    #required: ["mode"]
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
      run_as_principal: #stringProperty,
      admission_mode: #refProperty & {#ref: "PipelineAdmissionMode"},
      max_run_duration_seconds: #int64Property,
      notification_webhooks: #arrayRefProperty & {#ref: "PipelineNotificationWebhook"},
      default_retry_count: #int32Property,
      default_timeout_seconds: #int64Property,
      default_compute_endpoint_id: #stringProperty,
      folder_id: #stringProperty
    }
  },
}
