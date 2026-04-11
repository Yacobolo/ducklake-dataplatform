package api

// Authored catalog and compute schemas.

schemas_catalog_compute: {
  CatalogHistoryEntry: #objectSchema & {
    #fields: {
      begin_snapshot_id: #int64Property,
      column_name: #stringProperty,
      end_snapshot_id: #int64Property,
      entity_type: #stringProperty,
      has_history: #boolProperty,
      is_active: #boolProperty,
      latest_snapshot_id: #int64Property,
      object_id: #stringProperty,
      object_name: #stringProperty,
      schema_name: #stringProperty,
      table_name: #stringProperty
    }
  },
  CatalogHistoryResponse: #objectSchema & {
    #fields: {
      data: #arrayRefProperty & {#ref: "CatalogHistoryEntry"}
    },
    #required: [
      "data"
    ]
  },
  CatalogRegistration: #objectSchema & {
    #fields: {
      comment: #commentProperty,
      created_at: #createdAtProperty,
      data_path: #stringProperty,
      dsn: #stringProperty,
      id: #idProperty,
      is_default: #boolProperty,
      metastore_type: #refProperty & {#ref: "MetastoreType"},
      name: #nameProperty,
      status: #refProperty & {#ref: "CatalogStatus"},
      system_managed: #boolProperty,
      updated_at: #updatedAtProperty
    },
    #required: [
      "id",
      "name"
    ]
  },
  CatalogRegistrationList: #objectSchema & {
    #fields: {
      catalogs: #arrayRefProperty & {#ref: "CatalogRegistration"},
      next_page_token: #stringProperty,
      total_count: #int32Property
    },
    #required: [
      "catalogs"
    ]
  },
  CatalogStatus: #enumSchema & {
    #values: [
      "ACTIVE",
      "ERROR",
      "DETACHED"
    ]
  },
  CatalogVersionSummary: #objectSchema & {
    #fields: {
      catalog_name: #stringProperty,
      columns: #refProperty & {#ref: "VersionedObjectSummary"},
      created_by: #stringProperty,
      data_path: #stringProperty,
      encrypted: #boolProperty,
      latest_snapshot_id: #int64Property,
      schemas: #refProperty & {#ref: "VersionedObjectSummary"},
      tables: #refProperty & {#ref: "VersionedObjectSummary"},
      version: #stringProperty
    }
  },
  ColumnDetail: #objectSchema & {
    #fields: {
      comment: #commentProperty,
      name: #nameProperty,
      nullable: #boolProperty,
      position: #int32Property,
      type: #stringProperty
    },
    #required: [
      "name",
      "type"
    ]
  },
  ColumnLineageEdge: #objectSchema & {
    #fields: {
      function: #stringProperty,
      id: #int64Property,
      lineage_edge_id: #stringProperty,
      source_column: #stringProperty,
      source_schema: #stringProperty,
      source_table: #stringProperty,
      target_column: #stringProperty,
      transform_type: #refProperty & {#ref: "ColumnLineageEdgeTransformType"}
    }
  },
  ColumnLineageEdgeTransformType: #enumSchema & {
    #values: [
      "DIRECT",
      "EXPRESSION"
    ]
  },
  ColumnMask: #objectSchema & {
    #fields: {
      column_name: #stringProperty,
      created_at: #createdAtProperty,
      description: #descriptionProperty,
      id: #idProperty,
      mask_expression: #stringProperty,
      name: #nameProperty,
      table_id: #stringProperty
    },
    #required: [
      "id",
      "table_id",
      "name",
      "column_name",
      "mask_expression"
    ]
  },
  ColumnMaskBinding: #objectSchema & {
    #fields: {
      column_mask_id: #stringProperty,
      id: #idProperty,
      principal_id: #principalIDProperty,
      principal_type: #refProperty & {#ref: "PrincipalType"},
      see_original: #boolProperty
    }
  },
  ColumnMaskBindingRequest: #objectSchema & {
    #fields: {
      principal_id: #principalIDProperty,
      principal_type: #refProperty & {#ref: "PrincipalType"},
      see_original: #boolProperty
    },
    #required: [
      "principal_id",
      "principal_type"
    ]
  },
  ComputeAssignment: #objectSchema & {
    #fields: {
      created_at: #createdAtProperty,
      endpoint_id: #stringProperty,
      endpoint_name: #stringProperty,
      fallback_local: #boolProperty,
      id: #idProperty,
      is_default: #boolProperty,
      principal_id: #principalIDProperty,
      principal_type: #refProperty & {#ref: "ComputeAssignmentPrincipalType"}
    }
  },
  ComputeAssignmentPrincipalType: #enumSchema & {
    #values: [
      "user",
      "group"
    ]
  },
  ComputeEndpoint: #objectSchema & {
    #fields: {
      created_at: #createdAtProperty,
      external_id: #stringProperty,
      id: #idProperty,
      max_memory_gb: #int64Property,
      name: #nameProperty,
      owner: #ownerProperty,
      size: #refProperty & {#ref: "ComputeEndpointSize"},
      status: #refProperty & {#ref: "ComputeEndpointStatus"},
      type: #refProperty & {#ref: "ComputeEndpointType"},
      updated_at: #updatedAtProperty,
      url: #stringProperty
    }
  },
  ComputeEndpointHealth: #objectSchema & {
    #fields: {
      duckdb_version: #stringProperty,
      endpoint_name: #stringProperty,
      max_memory_gb: #int32Property,
      memory_used_mb: #int32Property,
      status: #statusProperty,
      uptime_seconds: #int32Property
    }
  },
  ComputeEndpointSize: #enumSchema & {
    #values: [
      "SMALL",
      "MEDIUM",
      "LARGE"
    ]
  },
  ComputeEndpointStatus: #enumSchema & {
    #values: [
      "ACTIVE",
      "INACTIVE",
      "STARTING",
      "STOPPING",
      "ERROR"
    ]
  },
  ComputeEndpointType: #enumSchema & {
    #values: [
      "LOCAL",
      "REMOTE"
    ]
  },
  ComputeRoutingDefaults: #objectSchema & {
    #fields: {
      interactive_mode: #stringProperty,
      notebook_mode: #stringProperty,
      scheduled_mode: #stringProperty
    }
  },
  CreateCatalogRequest: #objectSchema & {
    #fields: {
      comment: #commentProperty,
      data_path: #stringProperty,
      dsn: #stringProperty,
      metastore_type: #refProperty & {#ref: "MetastoreType"},
      name: #nameProperty
    },
    #required: [
      "name"
    ]
  },
  CreateColumnMaskRequest: #objectSchema & {
    #fields: {
      column_name: #stringProperty,
      description: #descriptionProperty,
      mask_expression: #stringProperty,
      name: #nameProperty,
      table_id: #stringProperty
    },
    #required: [
      "name",
      "column_name",
      "mask_expression"
    ]
  },
  CreateColumnRequest: #objectSchema & {
    #fields: {
      comment: #commentProperty,
      name: #nameProperty,
      nullable: #boolProperty,
      type: #stringProperty
    },
    #required: [
      "name",
      "type"
    ]
  },
  CreateComputeAssignmentRequest: #objectSchema & {
    #fields: {
      fallback_local: #boolProperty,
      is_default: #boolProperty,
      principal_id: #principalIDProperty,
      principal_type: #refProperty & {#ref: "ComputeAssignmentPrincipalType"}
    },
    #required: [
      "principal_id",
      "principal_type"
    ]
  },
  CreateComputeEndpointRequest: #objectSchema & {
    #fields: {
      auth_token: #stringProperty,
      max_memory_gb: #int64Property,
      name: #nameProperty,
      size: #refProperty & {#ref: "ComputeEndpointSize"},
      type: #refProperty & {#ref: "ComputeEndpointType"},
      url: #stringProperty
    },
    #required: [
      "name",
      "type",
      "url"
    ]
  },
  CreateSchemaRequest: #objectSchema & {
    #fields: {
      comment: #commentProperty,
      location_name: #stringProperty,
      name: #nameProperty,
      properties: #refProperty & {#ref: "Record"}
    },
    #required: [
      "name"
    ]
  },
  CreateStorageCredentialRequest: #objectSchema & {
    #fields: {
      comment: #commentProperty,
      credential_type: #refProperty & {#ref: "StorageCredentialType"},
      endpoint: #stringProperty,
      key_id: #stringProperty,
      name: #nameProperty,
      region: #stringProperty,
      secret: #stringProperty,
      url_style: #refProperty & {#ref: "URLStyle"}
    },
    #required: [
      "name"
    ]
  },
  CreateTableRequest: #objectSchema & {
    #fields: {
      columns: #arrayRefProperty & {#ref: "CreateColumnRequest"},
      comment: #commentProperty,
      name: #nameProperty
    },
    #required: [
      "name"
    ]
  },
  CreateViewRequest: #objectSchema & {
    #fields: {
      comment: #commentProperty,
      name: #nameProperty,
      view_definition: #stringProperty
    },
    #required: [
      "name",
      "view_definition"
    ]
  },
  CreateVolumeRequest: #objectSchema & {
    #fields: {
      comment: #commentProperty,
      name: #nameProperty,
      storage_location: #stringProperty,
      volume_type: #stringProperty
    },
    #required: [
      "name"
    ]
  },
  SchemaDetail: #objectSchema & {
    #fields: {
      catalog_name: #stringProperty,
      comment: #commentProperty,
      created_at: #createdAtProperty,
      name: #nameProperty,
      owner: #ownerProperty,
      properties: #refProperty & {#ref: "Record"},
      schema_id: #stringProperty,
      tags: #arrayRefProperty & {#ref: "Tag"},
      updated_at: #updatedAtProperty
    },
    #required: [
      "schema_id",
      "name",
      "catalog_name"
    ]
  },
  StorageCredential: #objectSchema & {
    #fields: {
      comment: #commentProperty,
      created_at: #createdAtProperty,
      credential_type: #refProperty & {#ref: "StorageCredentialType"},
      endpoint: #stringProperty,
      id: #idProperty,
      name: #nameProperty,
      owner: #ownerProperty,
      region: #stringProperty,
      updated_at: #updatedAtProperty,
      url_style: #refProperty & {#ref: "URLStyle"}
    },
    #required: [
      "id",
      "name"
    ]
  },
  StorageCredentialType: #enumSchema & {
    #values: [
      "S3",
      "AZURE",
      "GCS"
    ]
  },
  TableDetail: #objectSchema & {
    #fields: {
      catalog_name: #stringProperty,
      columns: #arrayRefProperty & {#ref: "ColumnDetail"},
      comment: #commentProperty,
      created_at: #createdAtProperty,
      name: #nameProperty,
      owner: #ownerProperty,
      properties: #refProperty & {#ref: "Record"},
      schema_name: #stringProperty,
      statistics: #refProperty & {#ref: "TableStatistics"},
      table_id: #stringProperty,
      table_type: #stringProperty,
      tags: #arrayRefProperty & {#ref: "Tag"},
      updated_at: #updatedAtProperty
    },
    #required: [
      "table_id",
      "name",
      "schema_name",
      "catalog_name"
    ]
  },
  UpdateColumnMaskRequest: #objectSchema & {
    #fields: {
      column_name: #stringProperty,
      description: #descriptionProperty,
      mask_expression: #stringProperty,
      name: #nameProperty
    }
  },
  UpdateColumnRequest: #objectSchema & {
    #fields: {
      comment: #commentProperty,
      nullable: #boolProperty
    }
  },
  UpdateComputeEndpointRequest: #objectSchema & {
    #fields: {
      auth_token: #stringProperty,
      max_memory_gb: #int64Property,
      size: #refProperty & {#ref: "ComputeEndpointSize"},
      status: #refProperty & {#ref: "ComputeEndpointStatus"},
      url: #stringProperty
    }
  },
  UpdateSchemaRequest: #objectSchema & {
    #fields: {
      comment: #commentProperty,
      properties: #refProperty & {#ref: "Record"}
    }
  },
  UpdateStorageCredentialRequest: #objectSchema & {
    #fields: {
      comment: #commentProperty,
      endpoint: #stringProperty,
      key_id: #stringProperty,
      region: #stringProperty,
      secret: #stringProperty,
      url_style: #refProperty & {#ref: "URLStyle"}
    }
  },
  UpdateTableRequest: #objectSchema & {
    #fields: {
      comment: #commentProperty,
      owner: #ownerProperty,
      properties: #refProperty & {#ref: "Record"}
    }
  },
  UpdateViewRequest: #objectSchema & {
    #fields: {
      comment: #commentProperty,
      view_definition: #stringProperty
    }
  },
  UpdateVolumeRequest: #objectSchema & {
    #fields: {
      comment: #commentProperty,
      new_name: #stringProperty,
      storage_location: #stringProperty
    }
  },
  ViewDetail: #objectSchema & {
    #fields: {
      catalog_name: #stringProperty,
      comment: #commentProperty,
      created_at: #createdAtProperty,
      id: #idProperty,
      name: #nameProperty,
      owner: #ownerProperty,
      schema_id: #stringProperty,
      schema_name: #stringProperty,
      source_tables: #stringArrayProperty,
      updated_at: #updatedAtProperty,
      view_definition: #stringProperty
    },
    #required: [
      "id",
      "schema_name",
      "catalog_name",
      "name"
    ]
  },
  VolumeDetail: #objectSchema & {
    #fields: {
      catalog_name: #stringProperty,
      comment: #commentProperty,
      created_at: #createdAtProperty,
      id: #idProperty,
      name: #nameProperty,
      owner: #ownerProperty,
      schema_name: #stringProperty,
      storage_location: #stringProperty,
      updated_at: #updatedAtProperty,
      volume_type: #stringProperty
    },
    #required: [
      "id",
      "name",
      "schema_name",
      "catalog_name"
    ]
  },
}
