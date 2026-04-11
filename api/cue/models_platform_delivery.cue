package api

// Authored platform delivery schemas.

schemas_platform_delivery: {
  "Build": #objectSchema & {
    #fields: {
      "commit_sha": #stringProperty,
      "compile_diagnostics": #stringProperty,
      "compile_manifest": #stringProperty,
      "created_at": #createdAtProperty,
      "environment_id": #stringProperty,
      "environment_name": #stringProperty,
      "git_ref": #stringProperty,
      "id": #idProperty,
      "product_id": #stringProperty,
      "project_id": #stringProperty,
      "project_name": #stringProperty,
      "selector": #stringProperty,
      "source_model_run_id": #stringProperty,
      "state": #refProperty & {#ref: "BuildState"},
      "target_catalog": #stringProperty,
      "target_schema": #stringProperty
    },
    #required: [
      "git_ref",
      "target_catalog",
      "target_schema",
      "compile_manifest"
    ]
  },
  "BuildState": #enumSchema & {
    #values: [
      "draft",
      "ready",
      "released",
      "superseded"
    ]
  },
  "CreateBuildRequest": #objectSchema & {
    #fields: {
      "commit_sha": #stringProperty,
      "compile_diagnostics": #stringProperty,
      "compile_manifest": #stringProperty,
      "environment_name": #stringProperty,
      "git_ref": #stringProperty,
      "selector": #stringProperty,
      "source_model_run_id": #stringProperty,
      "target_catalog": #stringProperty,
      "target_schema": #stringProperty
    },
    #required: [
      "environment_name",
      "git_ref",
      "target_catalog",
      "target_schema",
      "compile_manifest"
    ]
  },
  "CreateExternalLocationRequest": #objectSchema & {
    #fields: {
      "comment": #commentProperty,
      "credential_name": #stringProperty,
      "name": #nameProperty,
      "read_only": #boolProperty,
      "storage_type": #refProperty & {#ref: "StorageType"},
      "url": #stringProperty
    },
    #required: [
      "name",
      "url"
    ]
  },
  "CreateGitRepoRequest": #objectSchema & {
    #fields: {
      "auth_token": #stringProperty,
      "branch": #stringProperty,
      "path": #stringProperty,
      "url": #stringProperty
    },
    #required: [
      "url",
      "branch"
    ]
  },
  "CreatePipelineJobRequest": #objectSchema & {
    #fields: {
      "compute_endpoint_id": #stringProperty,
      "depends_on": #stringArrayProperty,
      "job_order": #int32Property,
      "job_type": #refProperty & {#ref: "PipelineJobJobType"},
      "model_selector": #stringProperty,
      "name": #nameProperty,
      "notebook_id": #stringProperty,
      "retry_count": #int32Property,
      "timeout_seconds": #int64Property
    },
    #required: [
      "name"
    ]
  },
  "CreatePipelineRequest": #objectSchema & {
    #fields: {
      "concurrency_limit": #int32Property,
      "description": #descriptionProperty,
      "folder_id": #stringProperty,
      "is_paused": #boolProperty,
      "name": #nameProperty,
      "schedule_cron": #stringProperty
    },
    #required: [
      "name"
    ]
  },
  "Environment": #objectSchema & {
    #fields: {
      "compute_endpoint": #stringProperty,
      "created_at": #createdAtProperty,
      "defer_to_environment": #stringProperty,
      "description": #descriptionProperty,
      "id": #idProperty,
      "kind": #refProperty & {#ref: "EnvironmentKind"},
      "name": #nameProperty,
      "project_id": #stringProperty,
      "project_name": #stringProperty,
      "source_overrides": #refProperty & {#ref: "Record"},
      "target_catalog": #stringProperty,
      "target_schema": #stringProperty,
      "updated_at": #updatedAtProperty,
      "variables": #refProperty & {#ref: "Record"}
    },
    #required: [
      "name",
      "kind",
      "target_catalog",
      "target_schema"
    ]
  },
  "EnvironmentKind": #enumSchema & {
    #values: [
      "development",
      "staging",
      "production"
    ]
  },
  "ExternalLocation": #objectSchema & {
    #fields: {
      "comment": #commentProperty,
      "created_at": #createdAtProperty,
      "credential_name": #stringProperty,
      "id": #idProperty,
      "name": #nameProperty,
      "owner": #ownerProperty,
      "read_only": #boolProperty,
      "storage_type": #refProperty & {#ref: "StorageType"},
      "updated_at": #updatedAtProperty,
      "url": #stringProperty
    },
    #required: [
      "id",
      "name",
      "url"
    ]
  },
  "GitRepo": #objectSchema & {
    #fields: {
      "branch": #stringProperty,
      "created_at": #createdAtProperty,
      "id": #idProperty,
      "last_commit": #stringProperty,
      "last_sync_at": #stringProperty,
      "owner": #ownerProperty,
      "path": #stringProperty,
      "updated_at": #updatedAtProperty,
      "url": #stringProperty
    }
  },
  "Pipeline": #objectSchema & {
    #fields: {
      "concurrency_limit": #int32Property,
      "created_at": #createdAtProperty,
      "created_by": #stringProperty,
      "description": #descriptionProperty,
      "folder_id": #stringProperty,
      "id": #idProperty,
      "is_paused": #boolProperty,
      "name": #nameProperty,
      "schedule_cron": #stringProperty,
      "updated_at": #updatedAtProperty
    }
  },
  "PipelineJob": #objectSchema & {
    #fields: {
      "compute_endpoint_id": #stringProperty,
      "created_at": #createdAtProperty,
      "depends_on": #stringArrayProperty,
      "id": #idProperty,
      "job_order": #int32Property,
      "job_type": #refProperty & {#ref: "PipelineJobJobType"},
      "model_selector": #stringProperty,
      "name": #nameProperty,
      "notebook_id": #stringProperty,
      "pipeline_id": #stringProperty,
      "retry_count": #int32Property,
      "timeout_seconds": #int64Property
    }
  },
  "PipelineJobJobType": #enumSchema & {
    #values: [
      "NOTEBOOK",
      "MODEL_RUN"
    ]
  },
  "PipelineJobList": #objectSchema & {
    #fields: {
      "data": #arrayRefProperty & {#ref: "PipelineJob"}
    },
    #required: [
      "data"
    ]
  },
  "PipelineJobRun": #objectSchema & {
    #fields: {
      "created_at": #createdAtProperty,
      "error_message": #stringProperty,
      "finished_at": #stringProperty,
      "id": #idProperty,
      "job_id": #stringProperty,
      "job_name": #stringProperty,
      "retry_attempt": #int32Property,
      "run_id": #stringProperty,
      "started_at": #stringProperty,
      "status": #refProperty & {#ref: "PipelineJobRunStatus"}
    }
  },
  "PipelineJobRunList": #objectSchema & {
    #fields: {
      "data": #arrayRefProperty & {#ref: "PipelineJobRun"}
    },
    #required: [
      "data"
    ]
  },
  "PipelineJobRunStatus": #enumSchema & {
    #values: [
      "PENDING",
      "RUNNING",
      "SUCCESS",
      "FAILED",
      "SKIPPED",
      "CANCELLED"
    ]
  },
  "PipelineRun": #objectSchema & {
    #fields: {
      "created_at": #createdAtProperty,
      "error_message": #stringProperty,
      "finished_at": #stringProperty,
      "git_commit_hash": #stringProperty,
      "id": #idProperty,
      "parameters": #refProperty & {#ref: "Record"},
      "pipeline_id": #stringProperty,
      "started_at": #stringProperty,
      "status": #refProperty & {#ref: "PipelineRunStatus"},
      "trigger_type": #refProperty & {#ref: "PipelineRunTriggerType"},
      "triggered_by": #stringProperty
    }
  },
  "PipelineRunStatus": #enumSchema & {
    #values: [
      "PENDING",
      "RUNNING",
      "SUCCESS",
      "FAILED",
      "CANCELLED"
    ]
  },
  "PipelineRunTriggerType": #enumSchema & {
    #values: [
      "MANUAL",
      "SCHEDULED"
    ]
  },
  "TriggerPipelineRunRequest": #objectSchema & {
    #fields: {
      "parameters": #refProperty & {#ref: "Record"}
    }
  },
  "UpdateExternalLocationRequest": #objectSchema & {
    #fields: {
      "comment": #commentProperty,
      "credential_name": #stringProperty,
      "read_only": #boolProperty,
      "url": #stringProperty
    }
  },
  "UpdatePipelineJobRequest": #objectSchema & {
    #fields: {
      "compute_endpoint_id": #stringProperty,
      "depends_on": #stringArrayProperty,
      "job_order": #int32Property,
      "job_type": #refProperty & {#ref: "PipelineJobJobType"},
      "model_selector": #stringProperty,
      "name": #nameProperty,
      "notebook_id": #stringProperty,
      "retry_count": #int32Property,
      "timeout_seconds": #int64Property
    }
  },
  "UpdatePipelineRequest": #objectSchema & {
    #fields: {
      "concurrency_limit": #int32Property,
      "description": #descriptionProperty,
      "folder_id": #stringProperty,
      "is_paused": #boolProperty,
      "schedule_cron": #stringProperty
    }
  },
}
