package api

// Authored catalog and compute schemas.

schemas_catalog_compute: {
  CatalogHistoryEntry: #objectSchema & {
    #fields: {
      entity_type: #stringProperty,
      schema_name: #stringProperty,
      table_name: #stringProperty,
      column_name: #stringProperty,
      object_name: #stringProperty,
      object_id: #stringProperty,
      begin_snapshot_id: #int64Property,
      end_snapshot_id: #int64Property,
      latest_snapshot_id: #int64Property,
      is_active: #boolProperty,
      has_history: #boolProperty
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
      id: #idProperty,
      name: #nameProperty,
      metastore_type: #refProperty & {#ref: "MetastoreType"},
      dsn: #stringProperty,
      data_path: #stringProperty,
      status: #refProperty & {#ref: "CatalogStatus"},
      is_default: #boolProperty,
      comment: #commentProperty,
      created_at: #createdAtProperty,
      updated_at: #updatedAtProperty,
      system_managed: #boolProperty,
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
      version: #stringProperty,
      created_by: #stringProperty,
      encrypted: #boolProperty,
      data_path: #stringProperty,
      latest_snapshot_id: #int64Property,
      schemas: #refProperty & {#ref: "VersionedObjectSummary"},
      tables: #refProperty & {#ref: "VersionedObjectSummary"},
      columns: #refProperty & {#ref: "VersionedObjectSummary"}
    }
  },
  ColumnDetail: #objectSchema & {
    #fields: {
      name: #nameProperty,
      type: #stringProperty,
      position: #int32Property,
      nullable: #boolProperty,
      comment: #commentProperty
    },
    #required: [
      "name",
      "type"
    ]
  },
  ColumnLineageEdge: #objectSchema & {
    #fields: {
      id: #int64Property,
      lineage_edge_id: #stringProperty,
      source_column: #stringProperty,
      source_schema: #stringProperty,
      source_table: #stringProperty,
      target_column: #stringProperty,
      transform_type: #refProperty & {#ref: "ColumnLineageEdgeTransformType"},
      function: #stringProperty
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
      id: #idProperty,
      table_id: #stringProperty,
      mask_expression: #stringProperty,
      name: #nameProperty,
      column_name: #stringProperty,
      description: #descriptionProperty,
      created_at: #createdAtProperty
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
      id: #idProperty,
      column_mask_id: #stringProperty,
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
      id: #idProperty,
      endpoint_id: #stringProperty,
      endpoint_name: #stringProperty,
      principal_id: #principalIDProperty,
      principal_type: #refProperty & {#ref: "ComputeAssignmentPrincipalType"},
      fallback_local: #boolProperty,
      is_default: #boolProperty,
      created_at: #createdAtProperty
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
      id: #idProperty,
      name: #nameProperty,
      type: #refProperty & {#ref: "ComputeEndpointType"},
      size: #refProperty & {#ref: "ComputeEndpointSize"},
      status: #refProperty & {#ref: "ComputeEndpointStatus"},
      url: #stringProperty,
      external_id: #stringProperty,
      max_memory_gb: #int64Property,
      owner: #ownerProperty,
      created_at: #createdAtProperty,
      updated_at: #updatedAtProperty
    }
  },
  ComputeEndpointHealth: #objectSchema & {
    #fields: {
      duckdb_version: #stringProperty,
      endpoint_name: #stringProperty,
      status: #statusProperty,
      memory_used_mb: #int32Property,
      max_memory_gb: #int32Property,
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
      scheduled_mode: #stringProperty,
      notebook_mode: #stringProperty
    }
  },
  CreateCatalogRequest: #objectSchema & {
    #fields: {
      name: #nameProperty,
      metastore_type: #refProperty & {#ref: "MetastoreType"},
      dsn: #stringProperty,
      data_path: #stringProperty,
      comment: #commentProperty
    },
    #required: [
      "name"
    ]
  },
  CreateColumnMaskRequest: #objectSchema & {
    #fields: {
      table_id: #stringProperty,
      name: #nameProperty,
      column_name: #stringProperty,
      mask_expression: #stringProperty,
      description: #descriptionProperty
    },
    #required: [
      "name",
      "column_name",
      "mask_expression"
    ]
  },
  CreateColumnRequest: #objectSchema & {
    #fields: {
      name: #nameProperty,
      type: #stringProperty,
      nullable: #boolProperty,
      comment: #commentProperty
    },
    #required: [
      "name",
      "type"
    ]
  },
  CreateComputeAssignmentRequest: #objectSchema & {
    #fields: {
      principal_id: #principalIDProperty,
      principal_type: #refProperty & {#ref: "ComputeAssignmentPrincipalType"},
      fallback_local: #boolProperty,
      is_default: #boolProperty
    },
    #required: [
      "principal_id",
      "principal_type"
    ]
  },
  CreateComputeEndpointRequest: #objectSchema & {
    #fields: {
      name: #nameProperty,
      type: #refProperty & {#ref: "ComputeEndpointType"},
      url: {
        description: "Endpoint URI. REMOTE endpoints must use grpc:// or grpcs://; LOCAL endpoints use local routing URLs."
        schema: {
          type: "string"
        }
      },
      auth_token: #stringProperty,
      max_memory_gb: #int64Property,
      size: #refProperty & {#ref: "ComputeEndpointSize"}
    },
    #required: [
      "name",
      "type",
      "url"
    ]
  },
  CreateSchemaRequest: #objectSchema & {
    #fields: {
      name: #nameProperty,
      comment: #commentProperty,
      location_name: #stringProperty,
      properties: #stringMapProperty
    },
    #required: [
      "name"
    ]
  },
  CreateStorageCredentialRequest: #objectSchema & {
    #fields: {
      name: #nameProperty,
      credential_type: #refProperty & {#ref: "StorageCredentialType"},
      key_id: #stringProperty,
      secret: #stringProperty,
      endpoint: #stringProperty,
      region: #stringProperty,
      url_style: #refProperty & {#ref: "URLStyle"},
      comment: #commentProperty
    },
    #required: [
      "name"
    ]
  },
  CreateTableRequest: #objectSchema & {
    #fields: {
      name: #nameProperty,
      columns: #arrayRefProperty & {#ref: "CreateColumnRequest"},
      comment: #commentProperty
    },
    #required: [
      "name"
    ]
  },
  CreateViewRequest: #objectSchema & {
    #fields: {
      name: #nameProperty,
      view_definition: #stringProperty,
      comment: #commentProperty
    },
    #required: [
      "name",
      "view_definition"
    ]
  },
  CreateVolumeRequest: #objectSchema & {
    #fields: {
      name: #nameProperty,
      volume_type: #stringProperty,
      storage_location: #stringProperty,
      comment: #commentProperty
    },
    #required: [
      "name"
    ]
  },
  SchemaDetail: #objectSchema & {
    #fields: {
      schema_id: #stringProperty,
      name: #nameProperty,
      catalog_name: #stringProperty,
      comment: #commentProperty,
      properties: #stringMapProperty,
      tags: #arrayRefProperty & {#ref: "Tag"},
      owner: #ownerProperty,
      created_at: #createdAtProperty,
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
      id: #idProperty,
      name: #nameProperty,
      credential_type: #refProperty & {#ref: "StorageCredentialType"},
      endpoint: #stringProperty,
      region: #stringProperty,
      url_style: #refProperty & {#ref: "URLStyle"},
      comment: #commentProperty,
      owner: #ownerProperty,
      created_at: #createdAtProperty,
      updated_at: #updatedAtProperty
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
      table_id: #stringProperty,
      name: #nameProperty,
      schema_name: #stringProperty,
      catalog_name: #stringProperty,
      table_type: #stringProperty,
      columns: #arrayRefProperty & {#ref: "ColumnDetail"},
      comment: #commentProperty,
      properties: #stringMapProperty,
      tags: #arrayRefProperty & {#ref: "Tag"},
      owner: #ownerProperty,
      statistics: #refProperty & {#ref: "TableStatistics"},
      created_at: #createdAtProperty,
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
      name: #nameProperty,
      column_name: #stringProperty,
      mask_expression: #stringProperty,
      description: #descriptionProperty
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
      properties: #stringMapProperty
    }
  },
  UpdateStorageCredentialRequest: #objectSchema & {
    #fields: {
      key_id: #stringProperty,
      secret: #stringProperty,
      endpoint: #stringProperty,
      region: #stringProperty,
      url_style: #refProperty & {#ref: "URLStyle"},
      comment: #commentProperty
    }
  },
  UpdateTableRequest: #objectSchema & {
    #fields: {
      comment: #commentProperty,
      properties: #stringMapProperty,
      owner: #ownerProperty
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
      id: #idProperty,
      schema_id: #stringProperty,
      schema_name: #stringProperty,
      catalog_name: #stringProperty,
      name: #nameProperty,
      source_tables: #stringArrayProperty,
      view_definition: #stringProperty,
      comment: #commentProperty,
      owner: #ownerProperty,
      created_at: #createdAtProperty,
      updated_at: #updatedAtProperty
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
      id: #idProperty,
      name: #nameProperty,
      schema_name: #stringProperty,
      catalog_name: #stringProperty,
      storage_location: #stringProperty,
      volume_type: #stringProperty,
      comment: #commentProperty,
      owner: #ownerProperty,
      created_at: #createdAtProperty,
      updated_at: #updatedAtProperty
    },
    #required: [
      "id",
      "name",
      "schema_name",
      "catalog_name"
    ]
  },
}
