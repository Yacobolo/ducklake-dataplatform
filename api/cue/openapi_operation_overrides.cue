package api

openapi_operation_overrides: {
  "listAPIKeys": {
    "parameter_order": [
      "principal_id",
      "max_results",
      "page_token"
    ],
    "parameters": {
      "principal_id": {
        "schema": {
          "type": "string"
        },
        "explode": false
      },
      "max_results": {
        "schema": {
          "type": "integer",
          "format": "int32"
        },
        "explode": false
      },
      "page_token": {
        "schema": {
          "type": "string"
        },
        "explode": false
      }
    }
  },
  "deleteAPIKey": {
    "parameter_order": [
      "api_key_id"
    ],
    "parameters": {
      "api_key_id": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "listAssets": {
    "parameter_order": [
      "max_results",
      "page_token"
    ],
    "parameters": {
      "max_results": {
        "schema": {
          "type": "integer",
          "format": "int32"
        },
        "explode": false
      },
      "page_token": {
        "schema": {
          "type": "string"
        },
        "explode": false
      }
    }
  },
  "getAsset": {
    "parameter_order": [
      "asset_key"
    ],
    "parameters": {
      "asset_key": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "updateAsset": {
    "parameter_order": [
      "asset_key"
    ],
    "parameters": {
      "asset_key": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "deleteAsset": {
    "parameter_order": [
      "asset_key"
    ],
    "parameters": {
      "asset_key": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "listAssetBackfills": {
    "parameter_order": [
      "asset_key",
      "status",
      "max_results",
      "page_token"
    ],
    "parameters": {
      "asset_key": {
        "schema": {
          "type": "string"
        }
      },
      "status": {
        "schema": {
          "ref": "AssetRunStatus"
        },
        "explode": false
      },
      "max_results": {
        "schema": {
          "type": "integer",
          "format": "int32"
        },
        "explode": false
      },
      "page_token": {
        "schema": {
          "type": "string"
        },
        "explode": false
      }
    }
  },
  "createAssetBackfill": {
    "parameter_order": [
      "asset_key"
    ],
    "parameters": {
      "asset_key": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "getAssetBackfill": {
    "parameter_order": [
      "asset_key",
      "backfill_id"
    ],
    "parameters": {
      "asset_key": {
        "schema": {
          "type": "string"
        }
      },
      "backfill_id": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "listAssetChecks": {
    "parameter_order": [
      "asset_key"
    ],
    "parameters": {
      "asset_key": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "listAssetCheckResults": {
    "parameter_order": [
      "asset_key",
      "max_results",
      "page_token"
    ],
    "parameters": {
      "asset_key": {
        "schema": {
          "type": "string"
        }
      },
      "max_results": {
        "schema": {
          "type": "integer",
          "format": "int32"
        },
        "explode": false
      },
      "page_token": {
        "schema": {
          "type": "string"
        },
        "explode": false
      }
    }
  },
  "getAssetFreshness": {
    "parameter_order": [
      "asset_key"
    ],
    "parameters": {
      "asset_key": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "reconcileAssetFreshness": {
    "parameter_order": [
      "asset_key"
    ],
    "parameters": {
      "asset_key": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "listAssetFreshnessBlockers": {
    "parameter_order": [
      "asset_key"
    ],
    "parameters": {
      "asset_key": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "explainAssetFreshness": {
    "parameter_order": [
      "asset_key"
    ],
    "parameters": {
      "asset_key": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "listAssetFreshnessRequirements": {
    "parameter_order": [
      "asset_key"
    ],
    "parameters": {
      "asset_key": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "getAssetGraph": {
    "parameter_order": [
      "asset_key"
    ],
    "parameters": {
      "asset_key": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "triggerAssetMaterialization": {
    "parameter_order": [
      "asset_key"
    ],
    "parameters": {
      "asset_key": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "listAssetMaterializations": {
    "parameter_order": [
      "asset_key",
      "max_results",
      "page_token"
    ],
    "parameters": {
      "asset_key": {
        "schema": {
          "type": "string"
        }
      },
      "max_results": {
        "schema": {
          "type": "integer",
          "format": "int32"
        },
        "explode": false
      },
      "page_token": {
        "schema": {
          "type": "string"
        },
        "explode": false
      }
    }
  },
  "listAssetPartitions": {
    "parameter_order": [
      "asset_key",
      "max_results",
      "page_token"
    ],
    "parameters": {
      "asset_key": {
        "schema": {
          "type": "string"
        }
      },
      "max_results": {
        "schema": {
          "type": "integer",
          "format": "int32"
        },
        "explode": false
      },
      "page_token": {
        "schema": {
          "type": "string"
        },
        "explode": false
      }
    }
  },
  "listAssetRuns": {
    "parameter_order": [
      "asset_key",
      "status",
      "max_results",
      "page_token"
    ],
    "parameters": {
      "asset_key": {
        "schema": {
          "type": "string"
        }
      },
      "status": {
        "schema": {
          "ref": "AssetRunStatus"
        },
        "explode": false
      },
      "max_results": {
        "schema": {
          "type": "integer",
          "format": "int32"
        },
        "explode": false
      },
      "page_token": {
        "schema": {
          "type": "string"
        },
        "explode": false
      }
    }
  },
  "listAuditLogs": {
    "parameter_order": [
      "principal_name",
      "action",
      "status",
      "max_results",
      "page_token"
    ],
    "parameters": {
      "principal_name": {
        "schema": {
          "type": "string"
        },
        "explode": false
      },
      "action": {
        "schema": {
          "type": "string"
        },
        "explode": false
      },
      "status": {
        "schema": {
          "ref": "AuditDecisionStatus"
        },
        "explode": false
      },
      "max_results": {
        "schema": {
          "type": "integer",
          "format": "int32"
        },
        "explode": false
      },
      "page_token": {
        "schema": {
          "type": "string"
        },
        "explode": false
      }
    }
  },
  "bootstrapComplete": {
    "security": [
      {}
    ]
  },
  "localLogin": {
    "security": [
      {}
    ]
  },
  "listCatalogs": {
    "parameter_order": [
      "max_results",
      "page_token"
    ],
    "parameters": {
      "max_results": {
        "schema": {
          "type": "integer",
          "format": "int32"
        },
        "explode": false
      },
      "page_token": {
        "schema": {
          "type": "string"
        },
        "explode": false
      }
    }
  },
  "searchCatalog": {
    "parameter_order": [
      "query",
      "type",
      "catalog",
      "max_results",
      "page_token"
    ],
    "parameters": {
      "query": {
        "schema": {
          "type": "string"
        },
        "explode": false
      },
      "type": {
        "schema": {
          "type": "string"
        },
        "explode": false
      },
      "catalog": {
        "schema": {
          "type": "string"
        },
        "explode": false
      },
      "max_results": {
        "schema": {
          "type": "integer",
          "format": "int32"
        },
        "explode": false
      },
      "page_token": {
        "schema": {
          "type": "string"
        },
        "explode": false
      }
    }
  },
  "getCatalog": {
    "parameter_order": [
      "catalog_name"
    ],
    "parameters": {
      "catalog_name": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "updateCatalogRegistration": {
    "parameter_order": [
      "catalog_name"
    ],
    "parameters": {
      "catalog_name": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "deleteCatalogRegistration": {
    "parameter_order": [
      "catalog_name"
    ],
    "parameters": {
      "catalog_name": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "setDefaultCatalog": {
    "parameter_order": [
      "catalog_name"
    ],
    "parameters": {
      "catalog_name": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "listCatalogHistory": {
    "parameter_order": [
      "catalog_name",
      "entity_type",
      "schema_name",
      "table_name",
      "limit"
    ],
    "parameters": {
      "catalog_name": {
        "schema": {
          "type": "string"
        }
      },
      "entity_type": {
        "schema": {
          "type": "string"
        },
        "explode": false
      },
      "schema_name": {
        "schema": {
          "type": "string"
        },
        "explode": false
      },
      "table_name": {
        "schema": {
          "type": "string"
        },
        "explode": false
      },
      "limit": {
        "schema": {
          "type": "integer",
          "format": "int32"
        },
        "explode": false
      }
    }
  },
  "getMetastoreSummary": {
    "parameter_order": [
      "catalog_name"
    ],
    "parameters": {
      "catalog_name": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "listSchemas": {
    "parameter_order": [
      "catalog_name",
      "max_results",
      "page_token"
    ],
    "parameters": {
      "catalog_name": {
        "schema": {
          "type": "string"
        }
      },
      "max_results": {
        "schema": {
          "type": "integer",
          "format": "int32"
        },
        "explode": false
      },
      "page_token": {
        "schema": {
          "type": "string"
        },
        "explode": false
      }
    }
  },
  "createSchema": {
    "parameter_order": [
      "catalog_name"
    ],
    "parameters": {
      "catalog_name": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "getSchema": {
    "parameter_order": [
      "catalog_name",
      "schema_name"
    ],
    "parameters": {
      "catalog_name": {
        "schema": {
          "type": "string"
        }
      },
      "schema_name": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "updateSchema": {
    "parameter_order": [
      "catalog_name",
      "schema_name"
    ],
    "parameters": {
      "catalog_name": {
        "schema": {
          "type": "string"
        }
      },
      "schema_name": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "deleteSchema": {
    "parameter_order": [
      "catalog_name",
      "schema_name",
      "force"
    ],
    "parameters": {
      "catalog_name": {
        "schema": {
          "type": "string"
        }
      },
      "schema_name": {
        "schema": {
          "type": "string"
        }
      },
      "force": {
        "schema": {
          "type": "boolean"
        },
        "explode": false
      }
    }
  },
  "listTables": {
    "parameter_order": [
      "catalog_name",
      "schema_name",
      "max_results",
      "page_token"
    ],
    "parameters": {
      "catalog_name": {
        "schema": {
          "type": "string"
        }
      },
      "schema_name": {
        "schema": {
          "type": "string"
        }
      },
      "max_results": {
        "schema": {
          "type": "integer",
          "format": "int32"
        },
        "explode": false
      },
      "page_token": {
        "schema": {
          "type": "string"
        },
        "explode": false
      }
    }
  },
  "createTable": {
    "parameter_order": [
      "catalog_name",
      "schema_name"
    ],
    "parameters": {
      "catalog_name": {
        "schema": {
          "type": "string"
        }
      },
      "schema_name": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "getTable": {
    "parameter_order": [
      "catalog_name",
      "schema_name",
      "table_name"
    ],
    "parameters": {
      "catalog_name": {
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
      }
    }
  },
  "updateTable": {
    "parameter_order": [
      "catalog_name",
      "schema_name",
      "table_name"
    ],
    "parameters": {
      "catalog_name": {
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
      }
    }
  },
  "deleteTable": {
    "parameter_order": [
      "catalog_name",
      "schema_name",
      "table_name"
    ],
    "parameters": {
      "catalog_name": {
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
      }
    }
  },
  "listTableColumns": {
    "parameter_order": [
      "catalog_name",
      "schema_name",
      "table_name",
      "max_results",
      "page_token"
    ],
    "parameters": {
      "catalog_name": {
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
      "max_results": {
        "schema": {
          "type": "integer",
          "format": "int32"
        },
        "explode": false
      },
      "page_token": {
        "schema": {
          "type": "string"
        },
        "explode": false
      }
    }
  },
  "updateColumn": {
    "parameter_order": [
      "catalog_name",
      "schema_name",
      "table_name",
      "column_name"
    ],
    "parameters": {
      "catalog_name": {
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
      }
    }
  },
  "commitTableIngestion": {
    "parameter_order": [
      "catalog_name",
      "schema_name",
      "table_name"
    ],
    "parameters": {
      "catalog_name": {
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
      }
    }
  },
  "loadTableExternalFiles": {
    "parameter_order": [
      "catalog_name",
      "schema_name",
      "table_name"
    ],
    "parameters": {
      "catalog_name": {
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
      }
    }
  },
  "createManifest": {
    "parameter_order": [
      "catalog_name",
      "schema_name",
      "table_name"
    ],
    "parameters": {
      "catalog_name": {
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
      }
    }
  },
  "profileTable": {
    "parameter_order": [
      "catalog_name",
      "schema_name",
      "table_name"
    ],
    "parameters": {
      "catalog_name": {
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
      }
    }
  },
  "createUploadUrl": {
    "parameter_order": [
      "catalog_name",
      "schema_name",
      "table_name"
    ],
    "parameters": {
      "catalog_name": {
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
      }
    }
  },
  "listViews": {
    "parameter_order": [
      "catalog_name",
      "schema_name",
      "max_results",
      "page_token"
    ],
    "parameters": {
      "catalog_name": {
        "schema": {
          "type": "string"
        }
      },
      "schema_name": {
        "schema": {
          "type": "string"
        }
      },
      "max_results": {
        "schema": {
          "type": "integer",
          "format": "int32"
        },
        "explode": false
      },
      "page_token": {
        "schema": {
          "type": "string"
        },
        "explode": false
      }
    }
  },
  "createView": {
    "parameter_order": [
      "catalog_name",
      "schema_name"
    ],
    "parameters": {
      "catalog_name": {
        "schema": {
          "type": "string"
        }
      },
      "schema_name": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "getView": {
    "parameter_order": [
      "catalog_name",
      "schema_name",
      "view_name"
    ],
    "parameters": {
      "catalog_name": {
        "schema": {
          "type": "string"
        }
      },
      "schema_name": {
        "schema": {
          "type": "string"
        }
      },
      "view_name": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "updateView": {
    "parameter_order": [
      "catalog_name",
      "schema_name",
      "view_name"
    ],
    "parameters": {
      "catalog_name": {
        "schema": {
          "type": "string"
        }
      },
      "schema_name": {
        "schema": {
          "type": "string"
        }
      },
      "view_name": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "deleteView": {
    "parameter_order": [
      "catalog_name",
      "schema_name",
      "view_name"
    ],
    "parameters": {
      "catalog_name": {
        "schema": {
          "type": "string"
        }
      },
      "schema_name": {
        "schema": {
          "type": "string"
        }
      },
      "view_name": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "listVolumes": {
    "parameter_order": [
      "catalog_name",
      "schema_name",
      "max_results",
      "page_token"
    ],
    "parameters": {
      "catalog_name": {
        "schema": {
          "type": "string"
        }
      },
      "schema_name": {
        "schema": {
          "type": "string"
        }
      },
      "max_results": {
        "schema": {
          "type": "integer",
          "format": "int32"
        },
        "explode": false
      },
      "page_token": {
        "schema": {
          "type": "string"
        },
        "explode": false
      }
    }
  },
  "createVolume": {
    "parameter_order": [
      "catalog_name",
      "schema_name"
    ],
    "parameters": {
      "catalog_name": {
        "schema": {
          "type": "string"
        }
      },
      "schema_name": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "getVolume": {
    "parameter_order": [
      "catalog_name",
      "schema_name",
      "volume_name"
    ],
    "parameters": {
      "catalog_name": {
        "schema": {
          "type": "string"
        }
      },
      "schema_name": {
        "schema": {
          "type": "string"
        }
      },
      "volume_name": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "updateVolume": {
    "parameter_order": [
      "catalog_name",
      "schema_name",
      "volume_name"
    ],
    "parameters": {
      "catalog_name": {
        "schema": {
          "type": "string"
        }
      },
      "schema_name": {
        "schema": {
          "type": "string"
        }
      },
      "volume_name": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "deleteVolume": {
    "parameter_order": [
      "catalog_name",
      "schema_name",
      "volume_name"
    ],
    "parameters": {
      "catalog_name": {
        "schema": {
          "type": "string"
        }
      },
      "schema_name": {
        "schema": {
          "type": "string"
        }
      },
      "volume_name": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "getCatalogVersionSummary": {
    "parameter_order": [
      "catalog_name"
    ],
    "parameters": {
      "catalog_name": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "listClassifications": {
    "parameter_order": [
      "max_results",
      "page_token"
    ],
    "parameters": {
      "max_results": {
        "schema": {
          "type": "integer",
          "format": "int32"
        },
        "explode": false
      },
      "page_token": {
        "schema": {
          "type": "string"
        },
        "explode": false
      }
    }
  },
  "listColumnMasks": {
    "parameter_order": [
      "table_id",
      "max_results",
      "page_token"
    ],
    "parameters": {
      "table_id": {
        "schema": {
          "type": "string"
        },
        "explode": false
      },
      "max_results": {
        "schema": {
          "type": "integer",
          "format": "int32"
        },
        "explode": false
      },
      "page_token": {
        "schema": {
          "type": "string"
        },
        "explode": false
      }
    }
  },
  "getColumnMask": {
    "parameter_order": [
      "column_mask_id"
    ],
    "parameters": {
      "column_mask_id": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "updateColumnMask": {
    "parameter_order": [
      "column_mask_id"
    ],
    "parameters": {
      "column_mask_id": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "deleteColumnMask": {
    "parameter_order": [
      "column_mask_id"
    ],
    "parameters": {
      "column_mask_id": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "listColumnMaskBindings": {
    "parameter_order": [
      "column_mask_id",
      "max_results",
      "page_token"
    ],
    "parameters": {
      "column_mask_id": {
        "schema": {
          "type": "string"
        }
      },
      "max_results": {
        "schema": {
          "type": "integer",
          "format": "int32"
        },
        "explode": false
      },
      "page_token": {
        "schema": {
          "type": "string"
        },
        "explode": false
      }
    }
  },
  "bindColumnMask": {
    "parameter_order": [
      "column_mask_id"
    ],
    "parameters": {
      "column_mask_id": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "unbindColumnMask": {
    "parameter_order": [
      "column_mask_id",
      "principal_type",
      "principal_id"
    ],
    "parameters": {
      "column_mask_id": {
        "schema": {
          "type": "string"
        }
      },
      "principal_type": {
        "schema": {
          "ref": "PrincipalType"
        }
      },
      "principal_id": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "listComputeEndpoints": {
    "parameter_order": [
      "max_results",
      "page_token"
    ],
    "parameters": {
      "max_results": {
        "schema": {
          "type": "integer",
          "format": "int32"
        },
        "explode": false
      },
      "page_token": {
        "schema": {
          "type": "string"
        },
        "explode": false
      }
    }
  },
  "createComputeEndpoint": {
    "responses": {
      "400": {
        "any_of": [
          {
            "ref": "Error"
          },
          {
            "ref": "Error"
          }
        ]
      }
    }
  },
  "getComputeEndpoint": {
    "parameter_order": [
      "endpoint_name"
    ],
    "parameters": {
      "endpoint_name": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "updateComputeEndpoint": {
    "parameter_order": [
      "endpoint_name"
    ],
    "parameters": {
      "endpoint_name": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "deleteComputeEndpoint": {
    "parameter_order": [
      "endpoint_name"
    ],
    "parameters": {
      "endpoint_name": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "listComputeAssignments": {
    "parameter_order": [
      "endpoint_name",
      "max_results",
      "page_token"
    ],
    "parameters": {
      "endpoint_name": {
        "schema": {
          "type": "string"
        }
      },
      "max_results": {
        "schema": {
          "type": "integer",
          "format": "int32"
        },
        "explode": false
      },
      "page_token": {
        "schema": {
          "type": "string"
        },
        "explode": false
      }
    }
  },
  "createComputeAssignment": {
    "parameter_order": [
      "endpoint_name"
    ],
    "parameters": {
      "endpoint_name": {
        "schema": {
          "type": "string"
        }
      }
    },
    "responses": {
      "400": {
        "any_of": [
          {
            "ref": "Error"
          },
          {
            "ref": "Error"
          }
        ]
      }
    }
  },
  "deleteComputeAssignment": {
    "parameter_order": [
      "endpoint_name",
      "assignment_id"
    ],
    "parameters": {
      "endpoint_name": {
        "schema": {
          "type": "string"
        }
      },
      "assignment_id": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "getComputeEndpointHealth": {
    "parameter_order": [
      "endpoint_name"
    ],
    "parameters": {
      "endpoint_name": {
        "schema": {
          "type": "string"
        }
      }
    },
    "responses": {
      "400": {
        "any_of": [
          {
            "ref": "Error"
          },
          {
            "ref": "Error"
          }
        ]
      }
    }
  },
  "updateComputeRoutingDefaults": {
    "responses": {
      "400": {
        "any_of": [
          {
            "ref": "Error"
          },
          {
            "ref": "Error"
          }
        ]
      }
    }
  },
  "listDashboards": {
    "parameter_order": [
      "owner",
      "max_results",
      "page_token"
    ],
    "parameters": {
      "owner": {
        "schema": {
          "type": "string"
        },
        "explode": false
      },
      "max_results": {
        "schema": {
          "type": "integer",
          "format": "int32"
        },
        "explode": false
      },
      "page_token": {
        "schema": {
          "type": "string"
        },
        "explode": false
      }
    }
  },
  "getDashboard": {
    "parameter_order": [
      "dashboard_id"
    ],
    "parameters": {
      "dashboard_id": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "updateDashboard": {
    "parameter_order": [
      "dashboard_id"
    ],
    "parameters": {
      "dashboard_id": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "deleteDashboard": {
    "parameter_order": [
      "dashboard_id"
    ],
    "parameters": {
      "dashboard_id": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "getRenderedDashboard": {
    "parameter_order": [
      "dashboard_id"
    ],
    "parameters": {
      "dashboard_id": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "listDashboardWidgets": {
    "parameter_order": [
      "dashboard_id"
    ],
    "parameters": {
      "dashboard_id": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "createDashboardWidget": {
    "parameter_order": [
      "dashboard_id"
    ],
    "parameters": {
      "dashboard_id": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "getDashboardWidget": {
    "parameter_order": [
      "dashboard_id",
      "widget_id"
    ],
    "parameters": {
      "dashboard_id": {
        "schema": {
          "type": "string"
        }
      },
      "widget_id": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "updateDashboardWidget": {
    "parameter_order": [
      "dashboard_id",
      "widget_id"
    ],
    "parameters": {
      "dashboard_id": {
        "schema": {
          "type": "string"
        }
      },
      "widget_id": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "deleteDashboardWidget": {
    "parameter_order": [
      "dashboard_id",
      "widget_id"
    ],
    "parameters": {
      "dashboard_id": {
        "schema": {
          "type": "string"
        }
      },
      "widget_id": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "listDataProducts": {
    "parameter_order": [
      "q",
      "domain",
      "team",
      "publication_state",
      "certification_state",
      "freshness_state",
      "max_results",
      "page_token"
    ],
    "parameters": {
      "q": {
        "schema": {
          "type": "string"
        },
        "explode": false
      },
      "domain": {
        "schema": {
          "type": "string"
        },
        "explode": false
      },
      "team": {
        "schema": {
          "type": "string"
        },
        "explode": false
      },
      "publication_state": {
        "schema": {
          "type": "string"
        },
        "explode": false
      },
      "certification_state": {
        "schema": {
          "type": "string"
        },
        "explode": false
      },
      "freshness_state": {
        "schema": {
          "type": "string"
        },
        "explode": false
      },
      "max_results": {
        "schema": {
          "type": "integer",
          "format": "int32"
        },
        "explode": false
      },
      "page_token": {
        "schema": {
          "type": "string"
        },
        "explode": false
      }
    }
  },
  "createDataProduct": {
    "responses": {
      "400": {
        "any_of": [
          {
            "ref": "Error"
          },
          {
            "ref": "Error"
          }
        ]
      }
    }
  },
  "listProductScorecards": {
    "parameter_order": [
      "max_results",
      "page_token"
    ],
    "parameters": {
      "max_results": {
        "schema": {
          "type": "integer",
          "format": "int32"
        },
        "explode": false
      },
      "page_token": {
        "schema": {
          "type": "string"
        },
        "explode": false
      }
    }
  },
  "getDataProduct": {
    "parameter_order": [
      "product_slug"
    ],
    "parameters": {
      "product_slug": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "updateDataProduct": {
    "parameter_order": [
      "product_slug"
    ],
    "parameters": {
      "product_slug": {
        "schema": {
          "type": "string"
        }
      }
    },
    "responses": {
      "400": {
        "any_of": [
          {
            "ref": "Error"
          },
          {
            "ref": "Error"
          }
        ]
      }
    }
  },
  "deleteDataProduct": {
    "parameter_order": [
      "product_slug"
    ],
    "parameters": {
      "product_slug": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "listDataProductDependencies": {
    "parameter_order": [
      "product_slug"
    ],
    "parameters": {
      "product_slug": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "createDataProductDependency": {
    "parameter_order": [
      "product_slug"
    ],
    "parameters": {
      "product_slug": {
        "schema": {
          "type": "string"
        }
      }
    },
    "responses": {
      "400": {
        "any_of": [
          {
            "ref": "Error"
          },
          {
            "ref": "Error"
          }
        ]
      }
    }
  },
  "listDataProductEvents": {
    "parameter_order": [
      "product_slug",
      "max_results",
      "page_token"
    ],
    "parameters": {
      "product_slug": {
        "schema": {
          "type": "string"
        }
      },
      "max_results": {
        "schema": {
          "type": "integer",
          "format": "int32"
        },
        "explode": false
      },
      "page_token": {
        "schema": {
          "type": "string"
        },
        "explode": false
      }
    }
  },
  "listDataProductOutputs": {
    "parameter_order": [
      "product_slug"
    ],
    "parameters": {
      "product_slug": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "listDataProductSemanticEntrypoints": {
    "parameter_order": [
      "product_slug"
    ],
    "parameters": {
      "product_slug": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "getDataProductStatus": {
    "parameter_order": [
      "product_slug"
    ],
    "parameters": {
      "product_slug": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "listDataProductSubscriptions": {
    "parameter_order": [
      "product_slug"
    ],
    "parameters": {
      "product_slug": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "createDataProductSubscription": {
    "parameter_order": [
      "product_slug"
    ],
    "parameters": {
      "product_slug": {
        "schema": {
          "type": "string"
        }
      }
    },
    "responses": {
      "400": {
        "any_of": [
          {
            "ref": "Error"
          },
          {
            "ref": "Error"
          }
        ]
      }
    }
  },
  "listDataProductVersions": {
    "parameter_order": [
      "product_slug"
    ],
    "parameters": {
      "product_slug": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "createDataProductVersion": {
    "parameter_order": [
      "product_slug"
    ],
    "parameters": {
      "product_slug": {
        "schema": {
          "type": "string"
        }
      }
    },
    "responses": {
      "400": {
        "any_of": [
          {
            "ref": "Error"
          },
          {
            "ref": "Error"
          }
        ]
      }
    }
  },
  "getDataProductVersion": {
    "parameter_order": [
      "product_slug",
      "version"
    ],
    "parameters": {
      "product_slug": {
        "schema": {
          "type": "string"
        }
      },
      "version": {
        "schema": {
          "type": "integer",
          "format": "int32"
        }
      }
    }
  },
  "deleteDataProductVersion": {
    "parameter_order": [
      "product_slug",
      "version"
    ],
    "parameters": {
      "product_slug": {
        "schema": {
          "type": "string"
        }
      },
      "version": {
        "schema": {
          "type": "integer",
          "format": "int32"
        }
      }
    },
    "responses": {
      "400": {
        "any_of": [
          {
            "ref": "Error"
          },
          {
            "ref": "Error"
          }
        ]
      }
    }
  },
  "deprecateDataProductVersion": {
    "parameter_order": [
      "product_slug",
      "version"
    ],
    "parameters": {
      "product_slug": {
        "schema": {
          "type": "string"
        }
      },
      "version": {
        "schema": {
          "type": "integer",
          "format": "int32"
        }
      }
    },
    "responses": {
      "400": {
        "any_of": [
          {
            "ref": "Error"
          },
          {
            "ref": "Error"
          }
        ]
      }
    }
  },
  "publishDataProductVersion": {
    "parameter_order": [
      "product_slug",
      "version"
    ],
    "parameters": {
      "product_slug": {
        "schema": {
          "type": "string"
        }
      },
      "version": {
        "schema": {
          "type": "integer",
          "format": "int32"
        }
      }
    },
    "responses": {
      "400": {
        "any_of": [
          {
            "ref": "Error"
          },
          {
            "ref": "Error"
          }
        ]
      }
    }
  },
  "retireDataProductVersion": {
    "parameter_order": [
      "product_slug",
      "version"
    ],
    "parameters": {
      "product_slug": {
        "schema": {
          "type": "string"
        }
      },
      "version": {
        "schema": {
          "type": "integer",
          "format": "int32"
        }
      }
    },
    "responses": {
      "400": {
        "any_of": [
          {
            "ref": "Error"
          },
          {
            "ref": "Error"
          }
        ]
      }
    }
  },
  "listExternalLocations": {
    "parameter_order": [
      "max_results",
      "page_token"
    ],
    "parameters": {
      "max_results": {
        "schema": {
          "type": "integer",
          "format": "int32"
        },
        "explode": false
      },
      "page_token": {
        "schema": {
          "type": "string"
        },
        "explode": false
      }
    }
  },
  "getExternalLocation": {
    "parameter_order": [
      "location_name"
    ],
    "parameters": {
      "location_name": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "updateExternalLocation": {
    "parameter_order": [
      "location_name"
    ],
    "parameters": {
      "location_name": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "deleteExternalLocation": {
    "parameter_order": [
      "location_name"
    ],
    "parameters": {
      "location_name": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "listRootFolderContents": {
    "parameter_order": [
      "kind",
      "max_results",
      "page_token"
    ],
    "parameters": {
      "kind": {
        "schema": {
          "type": "string"
        },
        "explode": false
      },
      "max_results": {
        "schema": {
          "type": "integer",
          "format": "int32"
        },
        "explode": false
      },
      "page_token": {
        "schema": {
          "type": "string"
        },
        "explode": false
      }
    }
  },
  "searchRootFolderContents": {
    "parameter_order": [
      "q",
      "kind",
      "owner",
      "max_results",
      "page_token"
    ],
    "parameters": {
      "q": {
        "schema": {
          "type": "string"
        },
        "explode": false
      },
      "kind": {
        "schema": {
          "type": "string"
        },
        "explode": false
      },
      "owner": {
        "schema": {
          "type": "string"
        },
        "explode": false
      },
      "max_results": {
        "schema": {
          "type": "integer",
          "format": "int32"
        },
        "explode": false
      },
      "page_token": {
        "schema": {
          "type": "string"
        },
        "explode": false
      }
    }
  },
  "getFolder": {
    "parameter_order": [
      "folder_id"
    ],
    "parameters": {
      "folder_id": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "updateFolder": {
    "parameter_order": [
      "folder_id"
    ],
    "parameters": {
      "folder_id": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "deleteFolder": {
    "parameter_order": [
      "folder_id"
    ],
    "parameters": {
      "folder_id": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "listFolderContents": {
    "parameter_order": [
      "folder_id",
      "kind",
      "max_results",
      "page_token"
    ],
    "parameters": {
      "folder_id": {
        "schema": {
          "type": "string"
        }
      },
      "kind": {
        "schema": {
          "type": "string"
        },
        "explode": false
      },
      "max_results": {
        "schema": {
          "type": "integer",
          "format": "int32"
        },
        "explode": false
      },
      "page_token": {
        "schema": {
          "type": "string"
        },
        "explode": false
      }
    }
  },
  "moveFolder": {
    "parameter_order": [
      "folder_id"
    ],
    "parameters": {
      "folder_id": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "getFolderPath": {
    "parameter_order": [
      "folder_id"
    ],
    "parameters": {
      "folder_id": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "searchFolderContents": {
    "parameter_order": [
      "folder_id",
      "q",
      "kind",
      "owner",
      "max_results",
      "page_token"
    ],
    "parameters": {
      "folder_id": {
        "schema": {
          "type": "string"
        }
      },
      "q": {
        "schema": {
          "type": "string"
        },
        "explode": false
      },
      "kind": {
        "schema": {
          "type": "string"
        },
        "explode": false
      },
      "owner": {
        "schema": {
          "type": "string"
        },
        "explode": false
      },
      "max_results": {
        "schema": {
          "type": "integer",
          "format": "int32"
        },
        "explode": false
      },
      "page_token": {
        "schema": {
          "type": "string"
        },
        "explode": false
      }
    }
  },
  "listFolderShares": {
    "parameter_order": [
      "folder_id"
    ],
    "parameters": {
      "folder_id": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "shareFolder": {
    "parameter_order": [
      "folder_id"
    ],
    "parameters": {
      "folder_id": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "unshareFolder": {
    "parameter_order": [
      "folder_id",
      "principal_name"
    ],
    "parameters": {
      "folder_id": {
        "schema": {
          "type": "string"
        }
      },
      "principal_name": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "listGitRepos": {
    "parameter_order": [
      "max_results",
      "page_token"
    ],
    "parameters": {
      "max_results": {
        "schema": {
          "type": "integer",
          "format": "int32"
        },
        "explode": false
      },
      "page_token": {
        "schema": {
          "type": "string"
        },
        "explode": false
      }
    }
  },
  "getGitRepo": {
    "parameter_order": [
      "git_repo_id"
    ],
    "parameters": {
      "git_repo_id": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "deleteGitRepo": {
    "parameter_order": [
      "git_repo_id"
    ],
    "parameters": {
      "git_repo_id": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "syncGitRepo": {
    "parameter_order": [
      "git_repo_id"
    ],
    "parameters": {
      "git_repo_id": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "listGrants": {
    "parameter_order": [
      "principal_id",
      "principal_type",
      "securable_type",
      "securable_id",
      "max_results",
      "page_token"
    ],
    "parameters": {
      "principal_id": {
        "schema": {
          "type": "string"
        },
        "explode": false
      },
      "principal_type": {
        "schema": {
          "ref": "PrincipalType"
        },
        "explode": false
      },
      "securable_type": {
        "schema": {
          "type": "string"
        },
        "explode": false
      },
      "securable_id": {
        "schema": {
          "type": "string"
        },
        "explode": false
      },
      "max_results": {
        "schema": {
          "type": "integer",
          "format": "int32"
        },
        "explode": false
      },
      "page_token": {
        "schema": {
          "type": "string"
        },
        "explode": false
      }
    }
  },
  "createGrant": {
    "responses": {
      "400": {
        "any_of": [
          {
            "ref": "Error"
          },
          {
            "ref": "Error"
          }
        ]
      }
    }
  },
  "deleteGrant": {
    "parameter_order": [
      "grant_id"
    ],
    "parameters": {
      "grant_id": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "listGroups": {
    "parameter_order": [
      "max_results",
      "page_token"
    ],
    "parameters": {
      "max_results": {
        "schema": {
          "type": "integer",
          "format": "int32"
        },
        "explode": false
      },
      "page_token": {
        "schema": {
          "type": "string"
        },
        "explode": false
      }
    }
  },
  "getGroup": {
    "parameter_order": [
      "group_id"
    ],
    "parameters": {
      "group_id": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "updateGroup": {
    "parameter_order": [
      "group_id"
    ],
    "parameters": {
      "group_id": {
        "schema": {
          "type": "string"
        }
      }
    },
    "responses": {
      "400": {
        "any_of": [
          {
            "ref": "Error"
          },
          {
            "ref": "Error"
          }
        ]
      }
    }
  },
  "deleteGroup": {
    "parameter_order": [
      "group_id"
    ],
    "parameters": {
      "group_id": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "listGroupMembers": {
    "parameter_order": [
      "group_id",
      "max_results",
      "page_token"
    ],
    "parameters": {
      "group_id": {
        "schema": {
          "type": "string"
        }
      },
      "max_results": {
        "schema": {
          "type": "integer",
          "format": "int32"
        },
        "explode": false
      },
      "page_token": {
        "schema": {
          "type": "string"
        },
        "explode": false
      }
    }
  },
  "createGroupMember": {
    "parameter_order": [
      "group_id"
    ],
    "parameters": {
      "group_id": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "deleteGroupMember": {
    "parameter_order": [
      "group_id",
      "member_type",
      "member_id"
    ],
    "parameters": {
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
  "getHealth": {
    "security": [
      {}
    ]
  },
  "getColumnLineage": {
    "parameter_order": [
      "schema_name",
      "table_name",
      "max_results",
      "page_token"
    ],
    "parameters": {
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
      "max_results": {
        "schema": {
          "type": "integer",
          "format": "int32"
        },
        "explode": false
      },
      "page_token": {
        "schema": {
          "type": "string"
        },
        "explode": false
      }
    }
  },
  "getColumnImpact": {
    "parameter_order": [
      "schema_name",
      "table_name",
      "column_name",
      "max_results",
      "page_token"
    ],
    "parameters": {
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
      "max_results": {
        "schema": {
          "type": "integer",
          "format": "int32"
        },
        "explode": false
      },
      "page_token": {
        "schema": {
          "type": "string"
        },
        "explode": false
      }
    }
  },
  "deleteLineageEdge": {
    "parameter_order": [
      "edge_id"
    ],
    "parameters": {
      "edge_id": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "getTableLineage": {
    "parameter_order": [
      "schema_name",
      "table_name",
      "max_results",
      "page_token"
    ],
    "parameters": {
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
      "max_results": {
        "schema": {
          "type": "integer",
          "format": "int32"
        },
        "explode": false
      },
      "page_token": {
        "schema": {
          "type": "string"
        },
        "explode": false
      }
    }
  },
  "getDownstreamLineage": {
    "parameter_order": [
      "schema_name",
      "table_name",
      "max_results",
      "page_token"
    ],
    "parameters": {
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
      "max_results": {
        "schema": {
          "type": "integer",
          "format": "int32"
        },
        "explode": false
      },
      "page_token": {
        "schema": {
          "type": "string"
        },
        "explode": false
      }
    }
  },
  "getUpstreamLineage": {
    "parameter_order": [
      "schema_name",
      "table_name",
      "max_results",
      "page_token"
    ],
    "parameters": {
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
      "max_results": {
        "schema": {
          "type": "integer",
          "format": "int32"
        },
        "explode": false
      },
      "page_token": {
        "schema": {
          "type": "string"
        },
        "explode": false
      }
    }
  },
  "listMacros": {
    "parameter_order": [
      "max_results",
      "page_token"
    ],
    "parameters": {
      "max_results": {
        "schema": {
          "type": "integer",
          "format": "int32"
        },
        "explode": false
      },
      "page_token": {
        "schema": {
          "type": "string"
        },
        "explode": false
      }
    }
  },
  "getMacro": {
    "parameter_order": [
      "macro_name"
    ],
    "parameters": {
      "macro_name": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "updateMacro": {
    "parameter_order": [
      "macro_name"
    ],
    "parameters": {
      "macro_name": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "deleteMacro": {
    "parameter_order": [
      "macro_name"
    ],
    "parameters": {
      "macro_name": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "getMacroImpact": {
    "parameter_order": [
      "macro_name",
      "max_results",
      "page_token"
    ],
    "parameters": {
      "macro_name": {
        "schema": {
          "type": "string"
        }
      },
      "max_results": {
        "schema": {
          "type": "integer",
          "format": "int32"
        },
        "explode": false
      },
      "page_token": {
        "schema": {
          "type": "string"
        },
        "explode": false
      }
    }
  },
  "diffMacroRevisions": {
    "parameter_order": [
      "macro_name",
      "from_version",
      "to_version"
    ],
    "parameters": {
      "macro_name": {
        "schema": {
          "type": "string"
        }
      },
      "from_version": {
        "schema": {
          "type": "integer",
          "format": "int32"
        },
        "explode": false
      },
      "to_version": {
        "schema": {
          "type": "integer",
          "format": "int32"
        },
        "explode": false
      }
    }
  },
  "listMacroRevisions": {
    "parameter_order": [
      "macro_name"
    ],
    "parameters": {
      "macro_name": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "listRecentResources": {
    "parameter_order": [
      "max_results",
      "page_token"
    ],
    "parameters": {
      "max_results": {
        "schema": {
          "type": "integer",
          "format": "int32"
        },
        "explode": false
      },
      "page_token": {
        "schema": {
          "type": "string"
        },
        "explode": false
      }
    }
  },
  "listSavedResources": {
    "parameter_order": [
      "max_results",
      "page_token"
    ],
    "parameters": {
      "max_results": {
        "schema": {
          "type": "integer",
          "format": "int32"
        },
        "explode": false
      },
      "page_token": {
        "schema": {
          "type": "string"
        },
        "explode": false
      }
    }
  },
  "deleteSavedResource": {
    "parameter_order": [
      "resource_type",
      "resource_key"
    ],
    "parameters": {
      "resource_type": {
        "schema": {
          "type": "string"
        }
      },
      "resource_key": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "listModelRuns": {
    "parameter_order": [
      "status",
      "max_results",
      "page_token"
    ],
    "parameters": {
      "status": {
        "schema": {
          "type": "string"
        },
        "explode": false
      },
      "max_results": {
        "schema": {
          "type": "integer",
          "format": "int32"
        },
        "explode": false
      },
      "page_token": {
        "schema": {
          "type": "string"
        },
        "explode": false
      }
    }
  },
  "getModelRun": {
    "parameter_order": [
      "run_id"
    ],
    "parameters": {
      "run_id": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "cancelModelRun": {
    "parameter_order": [
      "run_id"
    ],
    "parameters": {
      "run_id": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "listModelRunSteps": {
    "parameter_order": [
      "run_id"
    ],
    "parameters": {
      "run_id": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "listModelTestResults": {
    "parameter_order": [
      "run_id",
      "step_id"
    ],
    "parameters": {
      "run_id": {
        "schema": {
          "type": "string"
        }
      },
      "step_id": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "listModels": {
    "parameter_order": [
      "project_name",
      "max_results",
      "page_token"
    ],
    "parameters": {
      "project_name": {
        "schema": {
          "type": "string"
        },
        "explode": false
      },
      "max_results": {
        "schema": {
          "type": "integer",
          "format": "int32"
        },
        "explode": false
      },
      "page_token": {
        "schema": {
          "type": "string"
        },
        "explode": false
      }
    }
  },
  "getModelDAG": {
    "parameter_order": [
      "project_name"
    ],
    "parameters": {
      "project_name": {
        "schema": {
          "type": "string"
        },
        "explode": false
      }
    }
  },
  "getModel": {
    "parameter_order": [
      "project_name",
      "model_name"
    ],
    "parameters": {
      "project_name": {
        "schema": {
          "type": "string"
        }
      },
      "model_name": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "updateModel": {
    "parameter_order": [
      "project_name",
      "model_name"
    ],
    "parameters": {
      "project_name": {
        "schema": {
          "type": "string"
        }
      },
      "model_name": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "deleteModel": {
    "parameter_order": [
      "project_name",
      "model_name"
    ],
    "parameters": {
      "project_name": {
        "schema": {
          "type": "string"
        }
      },
      "model_name": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "checkModelFreshness": {
    "parameter_order": [
      "project_name",
      "model_name"
    ],
    "parameters": {
      "project_name": {
        "schema": {
          "type": "string"
        }
      },
      "model_name": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "createModelTest": {
    "parameter_order": [
      "project_name",
      "model_name"
    ],
    "parameters": {
      "project_name": {
        "schema": {
          "type": "string"
        }
      },
      "model_name": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "listModelTests": {
    "parameter_order": [
      "project_name",
      "model_name"
    ],
    "parameters": {
      "project_name": {
        "schema": {
          "type": "string"
        }
      },
      "model_name": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "deleteModelTest": {
    "parameter_order": [
      "project_name",
      "model_name",
      "test_id"
    ],
    "parameters": {
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
      "test_id": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "listNotebooks": {
    "parameter_order": [
      "owner",
      "max_results",
      "page_token"
    ],
    "parameters": {
      "owner": {
        "schema": {
          "type": "string"
        },
        "explode": false
      },
      "max_results": {
        "schema": {
          "type": "integer",
          "format": "int32"
        },
        "explode": false
      },
      "page_token": {
        "schema": {
          "type": "string"
        },
        "explode": false
      }
    }
  },
  "getNotebook": {
    "parameter_order": [
      "notebook_id"
    ],
    "parameters": {
      "notebook_id": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "updateNotebook": {
    "parameter_order": [
      "notebook_id"
    ],
    "parameters": {
      "notebook_id": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "deleteNotebook": {
    "parameter_order": [
      "notebook_id"
    ],
    "parameters": {
      "notebook_id": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "createCell": {
    "parameter_order": [
      "notebook_id"
    ],
    "parameters": {
      "notebook_id": {
        "schema": {
          "type": "string"
        }
      }
    },
    "responses": {
      "400": {
        "any_of": [
          {
            "ref": "Error"
          },
          {
            "ref": "Error"
          }
        ]
      }
    }
  },
  "reorderCells": {
    "parameter_order": [
      "notebook_id"
    ],
    "parameters": {
      "notebook_id": {
        "schema": {
          "type": "string"
        }
      }
    },
    "responses": {
      "400": {
        "any_of": [
          {
            "ref": "Error"
          },
          {
            "ref": "Error"
          }
        ]
      }
    }
  },
  "updateCell": {
    "parameter_order": [
      "notebook_id",
      "cell_id"
    ],
    "parameters": {
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
    },
    "responses": {
      "400": {
        "any_of": [
          {
            "ref": "Error"
          },
          {
            "ref": "Error"
          }
        ]
      }
    }
  },
  "deleteCell": {
    "parameter_order": [
      "notebook_id",
      "cell_id"
    ],
    "parameters": {
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
  "duplicateNotebook": {
    "parameter_order": [
      "notebook_id"
    ],
    "parameters": {
      "notebook_id": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "listNotebookJobs": {
    "parameter_order": [
      "notebook_id",
      "max_results",
      "page_token"
    ],
    "parameters": {
      "notebook_id": {
        "schema": {
          "type": "string"
        }
      },
      "max_results": {
        "schema": {
          "type": "integer",
          "format": "int32"
        },
        "explode": false
      },
      "page_token": {
        "schema": {
          "type": "string"
        },
        "explode": false
      }
    }
  },
  "getNotebookJob": {
    "parameter_order": [
      "notebook_id",
      "job_id"
    ],
    "parameters": {
      "notebook_id": {
        "schema": {
          "type": "string"
        }
      },
      "job_id": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "promoteNotebookToModel": {
    "parameter_order": [
      "notebook_id"
    ],
    "parameters": {
      "notebook_id": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "unpublishNotebookModel": {
    "parameter_order": [
      "notebook_id"
    ],
    "parameters": {
      "notebook_id": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "moveNotebook": {
    "parameter_order": [
      "notebook_id"
    ],
    "parameters": {
      "notebook_id": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "createNotebookSession": {
    "parameter_order": [
      "notebook_id"
    ],
    "parameters": {
      "notebook_id": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "closeNotebookSession": {
    "parameter_order": [
      "notebook_id",
      "session_id"
    ],
    "parameters": {
      "notebook_id": {
        "schema": {
          "type": "string"
        }
      },
      "session_id": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "runAllCells": {
    "parameter_order": [
      "notebook_id",
      "session_id"
    ],
    "parameters": {
      "notebook_id": {
        "schema": {
          "type": "string"
        }
      },
      "session_id": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "executeCell": {
    "parameter_order": [
      "notebook_id",
      "session_id",
      "cell_id"
    ],
    "parameters": {
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
      "cell_id": {
        "schema": {
          "type": "string"
        }
      }
    },
    "responses": {
      "400": {
        "any_of": [
          {
            "ref": "Error"
          },
          {
            "ref": "Error"
          }
        ]
      }
    }
  },
  "runAllCellsAsync": {
    "parameter_order": [
      "notebook_id",
      "session_id"
    ],
    "parameters": {
      "notebook_id": {
        "schema": {
          "type": "string"
        }
      },
      "session_id": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "listNotebookShares": {
    "parameter_order": [
      "notebook_id"
    ],
    "parameters": {
      "notebook_id": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "shareNotebook": {
    "parameter_order": [
      "notebook_id"
    ],
    "parameters": {
      "notebook_id": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "unshareNotebook": {
    "parameter_order": [
      "notebook_id",
      "principal_name"
    ],
    "parameters": {
      "notebook_id": {
        "schema": {
          "type": "string"
        }
      },
      "principal_name": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "listPipelines": {
    "parameter_order": [
      "max_results",
      "page_token"
    ],
    "parameters": {
      "max_results": {
        "schema": {
          "type": "integer",
          "format": "int32"
        },
        "explode": false
      },
      "page_token": {
        "schema": {
          "type": "string"
        },
        "explode": false
      }
    }
  },
  "getPipelineRun": {
    "parameter_order": [
      "run_id"
    ],
    "parameters": {
      "run_id": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "cancelPipelineRun": {
    "parameter_order": [
      "run_id"
    ],
    "parameters": {
      "run_id": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "listPipelineJobRuns": {
    "parameter_order": [
      "run_id"
    ],
    "parameters": {
      "run_id": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "getPipeline": {
    "parameter_order": [
      "pipeline_name"
    ],
    "parameters": {
      "pipeline_name": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "updatePipeline": {
    "parameter_order": [
      "pipeline_name"
    ],
    "parameters": {
      "pipeline_name": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "deletePipeline": {
    "parameter_order": [
      "pipeline_name"
    ],
    "parameters": {
      "pipeline_name": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "listPipelineJobs": {
    "parameter_order": [
      "pipeline_name"
    ],
    "parameters": {
      "pipeline_name": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "createPipelineJob": {
    "parameter_order": [
      "pipeline_name"
    ],
    "parameters": {
      "pipeline_name": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "getPipelineJob": {
    "parameter_order": [
      "pipeline_name",
      "job_id"
    ],
    "parameters": {
      "pipeline_name": {
        "schema": {
          "type": "string"
        }
      },
      "job_id": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "updatePipelineJob": {
    "parameter_order": [
      "pipeline_name",
      "job_id"
    ],
    "parameters": {
      "pipeline_name": {
        "schema": {
          "type": "string"
        }
      },
      "job_id": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "deletePipelineJob": {
    "parameter_order": [
      "pipeline_name",
      "job_id"
    ],
    "parameters": {
      "pipeline_name": {
        "schema": {
          "type": "string"
        }
      },
      "job_id": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "triggerPipelineRun": {
    "parameter_order": [
      "pipeline_name"
    ],
    "parameters": {
      "pipeline_name": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "listPipelineRuns": {
    "parameter_order": [
      "pipeline_name",
      "max_results",
      "page_token",
      "status"
    ],
    "parameters": {
      "pipeline_name": {
        "schema": {
          "type": "string"
        }
      },
      "max_results": {
        "schema": {
          "type": "integer",
          "format": "int32"
        },
        "explode": false
      },
      "page_token": {
        "schema": {
          "type": "string"
        },
        "explode": false
      },
      "status": {
        "schema": {
          "type": "string"
        },
        "explode": false
      }
    }
  },
  "listPrincipals": {
    "parameter_order": [
      "max_results",
      "page_token"
    ],
    "parameters": {
      "max_results": {
        "schema": {
          "type": "integer",
          "format": "int32"
        },
        "explode": false
      },
      "page_token": {
        "schema": {
          "type": "string"
        },
        "explode": false
      }
    }
  },
  "getPrincipal": {
    "parameter_order": [
      "principal_id"
    ],
    "parameters": {
      "principal_id": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "deletePrincipal": {
    "parameter_order": [
      "principal_id"
    ],
    "parameters": {
      "principal_id": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "updatePrincipal": {
    "parameter_order": [
      "principal_id"
    ],
    "parameters": {
      "principal_id": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "listProductDomains": {
    "parameter_order": [
      "max_results",
      "page_token"
    ],
    "parameters": {
      "max_results": {
        "schema": {
          "type": "integer",
          "format": "int32"
        },
        "explode": false
      },
      "page_token": {
        "schema": {
          "type": "string"
        },
        "explode": false
      }
    }
  },
  "getProductDomain": {
    "parameter_order": [
      "domain_name"
    ],
    "parameters": {
      "domain_name": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "updateProductDomain": {
    "parameter_order": [
      "domain_name"
    ],
    "parameters": {
      "domain_name": {
        "schema": {
          "type": "string"
        }
      }
    },
    "responses": {
      "400": {
        "any_of": [
          {
            "ref": "Error"
          },
          {
            "ref": "Error"
          }
        ]
      }
    }
  },
  "deleteProductDomain": {
    "parameter_order": [
      "domain_name"
    ],
    "parameters": {
      "domain_name": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "listProductTeams": {
    "parameter_order": [
      "domain_name",
      "max_results",
      "page_token"
    ],
    "parameters": {
      "domain_name": {
        "schema": {
          "type": "string"
        }
      },
      "max_results": {
        "schema": {
          "type": "integer",
          "format": "int32"
        },
        "explode": false
      },
      "page_token": {
        "schema": {
          "type": "string"
        },
        "explode": false
      }
    }
  },
  "createProductTeam": {
    "parameter_order": [
      "domain_name"
    ],
    "parameters": {
      "domain_name": {
        "schema": {
          "type": "string"
        }
      }
    },
    "responses": {
      "400": {
        "any_of": [
          {
            "ref": "Error"
          },
          {
            "ref": "Error"
          }
        ]
      }
    }
  },
  "getProductTeam": {
    "parameter_order": [
      "domain_name",
      "team_name"
    ],
    "parameters": {
      "domain_name": {
        "schema": {
          "type": "string"
        }
      },
      "team_name": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "updateProductTeam": {
    "parameter_order": [
      "domain_name",
      "team_name"
    ],
    "parameters": {
      "domain_name": {
        "schema": {
          "type": "string"
        }
      },
      "team_name": {
        "schema": {
          "type": "string"
        }
      }
    },
    "responses": {
      "400": {
        "any_of": [
          {
            "ref": "Error"
          },
          {
            "ref": "Error"
          }
        ]
      }
    }
  },
  "deleteProductTeam": {
    "parameter_order": [
      "domain_name",
      "team_name"
    ],
    "parameters": {
      "domain_name": {
        "schema": {
          "type": "string"
        }
      },
      "team_name": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "getProject": {
    "parameter_order": [
      "project_id"
    ],
    "parameters": {
      "project_id": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "listProjectBuilds": {
    "parameter_order": [
      "project_id",
      "max_results",
      "page_token"
    ],
    "parameters": {
      "project_id": {
        "schema": {
          "type": "string"
        }
      },
      "max_results": {
        "schema": {
          "type": "integer",
          "format": "int32"
        },
        "explode": false
      },
      "page_token": {
        "schema": {
          "type": "string"
        },
        "explode": false
      }
    }
  },
  "createProjectBuild": {
    "parameter_order": [
      "project_id"
    ],
    "parameters": {
      "project_id": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "listProjectEnvironments": {
    "parameter_order": [
      "project_id",
      "max_results",
      "page_token"
    ],
    "parameters": {
      "project_id": {
        "schema": {
          "type": "string"
        }
      },
      "max_results": {
        "schema": {
          "type": "integer",
          "format": "int32"
        },
        "explode": false
      },
      "page_token": {
        "schema": {
          "type": "string"
        },
        "explode": false
      }
    }
  },
  "createProjectEnvironment": {
    "parameter_order": [
      "project_id"
    ],
    "parameters": {
      "project_id": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "listQueries": {
    "parameter_order": [
      "status",
      "max_results",
      "page_token"
    ],
    "parameters": {
      "status": {
        "schema": {
          "ref": "QueryJobStatus"
        },
        "explode": false
      },
      "max_results": {
        "schema": {
          "type": "integer",
          "format": "int32"
        },
        "explode": false
      },
      "page_token": {
        "schema": {
          "type": "string"
        },
        "explode": false
      }
    }
  },
  "listQueryHistory": {
    "parameter_order": [
      "principal_name",
      "status",
      "from",
      "to",
      "max_results",
      "page_token"
    ],
    "parameters": {
      "principal_name": {
        "schema": {
          "type": "string"
        },
        "explode": false
      },
      "status": {
        "schema": {
          "ref": "AuditDecisionStatus"
        },
        "explode": false
      },
      "from": {
        "schema": {
          "type": "string",
          "format": "date-time"
        },
        "explode": false
      },
      "to": {
        "schema": {
          "type": "string",
          "format": "date-time"
        },
        "explode": false
      },
      "max_results": {
        "schema": {
          "type": "integer",
          "format": "int32"
        },
        "explode": false
      },
      "page_token": {
        "schema": {
          "type": "string"
        },
        "explode": false
      }
    }
  },
  "getQuery": {
    "parameter_order": [
      "query_id"
    ],
    "parameters": {
      "query_id": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "deleteQuery": {
    "parameter_order": [
      "query_id"
    ],
    "parameters": {
      "query_id": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "cancelQuery": {
    "parameter_order": [
      "query_id"
    ],
    "parameters": {
      "query_id": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "getQueryResults": {
    "parameter_order": [
      "query_id",
      "max_results",
      "page_token"
    ],
    "parameters": {
      "query_id": {
        "schema": {
          "type": "string"
        }
      },
      "max_results": {
        "schema": {
          "type": "integer",
          "format": "int32"
        },
        "explode": false
      },
      "page_token": {
        "schema": {
          "type": "string"
        },
        "explode": false
      }
    }
  },
  "listRowFilters": {
    "parameter_order": [
      "table_id",
      "max_results",
      "page_token"
    ],
    "parameters": {
      "table_id": {
        "schema": {
          "type": "string"
        },
        "explode": false
      },
      "max_results": {
        "schema": {
          "type": "integer",
          "format": "int32"
        },
        "explode": false
      },
      "page_token": {
        "schema": {
          "type": "string"
        },
        "explode": false
      }
    }
  },
  "getRowFilter": {
    "parameter_order": [
      "row_filter_id"
    ],
    "parameters": {
      "row_filter_id": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "updateRowFilter": {
    "parameter_order": [
      "row_filter_id"
    ],
    "parameters": {
      "row_filter_id": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "deleteRowFilter": {
    "parameter_order": [
      "row_filter_id"
    ],
    "parameters": {
      "row_filter_id": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "listRowFilterBindings": {
    "parameter_order": [
      "row_filter_id",
      "max_results",
      "page_token"
    ],
    "parameters": {
      "row_filter_id": {
        "schema": {
          "type": "string"
        }
      },
      "max_results": {
        "schema": {
          "type": "integer",
          "format": "int32"
        },
        "explode": false
      },
      "page_token": {
        "schema": {
          "type": "string"
        },
        "explode": false
      }
    }
  },
  "bindRowFilter": {
    "parameter_order": [
      "row_filter_id"
    ],
    "parameters": {
      "row_filter_id": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "unbindRowFilter": {
    "parameter_order": [
      "row_filter_id",
      "principal_type",
      "principal_id"
    ],
    "parameters": {
      "row_filter_id": {
        "schema": {
          "type": "string"
        }
      },
      "principal_type": {
        "schema": {
          "ref": "PrincipalType"
        }
      },
      "principal_id": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "checkMetricFreshness": {
    "parameter_order": [
      "metric_name",
      "semantic_model_id"
    ],
    "parameters": {
      "metric_name": {
        "schema": {
          "type": "string"
        }
      },
      "semantic_model_id": {
        "schema": {
          "type": "string"
        },
        "explode": false
      }
    }
  },
  "listSemanticModels": {
    "parameter_order": [
      "max_results",
      "page_token"
    ],
    "parameters": {
      "max_results": {
        "schema": {
          "type": "integer",
          "format": "int32"
        },
        "explode": false
      },
      "page_token": {
        "schema": {
          "type": "string"
        },
        "explode": false
      }
    }
  },
  "getSemanticModel": {
    "parameter_order": [
      "semantic_model_id"
    ],
    "parameters": {
      "semantic_model_id": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "updateSemanticModel": {
    "parameter_order": [
      "semantic_model_id"
    ],
    "parameters": {
      "semantic_model_id": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "deleteSemanticModel": {
    "parameter_order": [
      "semantic_model_id"
    ],
    "parameters": {
      "semantic_model_id": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "listSemanticMetrics": {
    "parameter_order": [
      "semantic_model_id"
    ],
    "parameters": {
      "semantic_model_id": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "createSemanticMetric": {
    "parameter_order": [
      "semantic_model_id"
    ],
    "parameters": {
      "semantic_model_id": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "getSemanticMetric": {
    "parameter_order": [
      "semantic_model_id",
      "metric_name"
    ],
    "parameters": {
      "semantic_model_id": {
        "schema": {
          "type": "string"
        }
      },
      "metric_name": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "updateSemanticMetric": {
    "parameter_order": [
      "semantic_model_id",
      "metric_name"
    ],
    "parameters": {
      "semantic_model_id": {
        "schema": {
          "type": "string"
        }
      },
      "metric_name": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "deleteSemanticMetric": {
    "parameter_order": [
      "semantic_model_id",
      "metric_name"
    ],
    "parameters": {
      "semantic_model_id": {
        "schema": {
          "type": "string"
        }
      },
      "metric_name": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "listSemanticPreAggregations": {
    "parameter_order": [
      "semantic_model_id"
    ],
    "parameters": {
      "semantic_model_id": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "createSemanticPreAggregation": {
    "parameter_order": [
      "semantic_model_id"
    ],
    "parameters": {
      "semantic_model_id": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "getSemanticPreAggregation": {
    "parameter_order": [
      "semantic_model_id",
      "pre_aggregation_name"
    ],
    "parameters": {
      "semantic_model_id": {
        "schema": {
          "type": "string"
        }
      },
      "pre_aggregation_name": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "updateSemanticPreAggregation": {
    "parameter_order": [
      "semantic_model_id",
      "pre_aggregation_name"
    ],
    "parameters": {
      "semantic_model_id": {
        "schema": {
          "type": "string"
        }
      },
      "pre_aggregation_name": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "deleteSemanticPreAggregation": {
    "parameter_order": [
      "semantic_model_id",
      "pre_aggregation_name"
    ],
    "parameters": {
      "semantic_model_id": {
        "schema": {
          "type": "string"
        }
      },
      "pre_aggregation_name": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "explainMetricQuery": {
    "parameter_order": [
      "semantic_model_id"
    ],
    "parameters": {
      "semantic_model_id": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "runMetricQuery": {
    "parameter_order": [
      "semantic_model_id"
    ],
    "parameters": {
      "semantic_model_id": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "listSemanticModelRelationships": {
    "parameter_order": [
      "semantic_model_id"
    ],
    "parameters": {
      "semantic_model_id": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "createSemanticModelRelationship": {
    "parameter_order": [
      "semantic_model_id"
    ],
    "parameters": {
      "semantic_model_id": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "getSemanticModelRelationship": {
    "parameter_order": [
      "semantic_model_id",
      "relationship_name"
    ],
    "parameters": {
      "semantic_model_id": {
        "schema": {
          "type": "string"
        }
      },
      "relationship_name": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "updateSemanticModelRelationship": {
    "parameter_order": [
      "semantic_model_id",
      "relationship_name"
    ],
    "parameters": {
      "semantic_model_id": {
        "schema": {
          "type": "string"
        }
      },
      "relationship_name": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "deleteSemanticModelRelationship": {
    "parameter_order": [
      "semantic_model_id",
      "relationship_name"
    ],
    "parameters": {
      "semantic_model_id": {
        "schema": {
          "type": "string"
        }
      },
      "relationship_name": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "checkSourceFreshness": {
    "parameter_order": [
      "source_schema",
      "source_table",
      "timestamp_column",
      "max_lag_seconds"
    ],
    "parameters": {
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
        },
        "explode": false
      },
      "max_lag_seconds": {
        "schema": {
          "type": "integer",
          "format": "int64"
        },
        "explode": false
      }
    }
  },
  "listStorageCredentials": {
    "parameter_order": [
      "max_results",
      "page_token"
    ],
    "parameters": {
      "max_results": {
        "schema": {
          "type": "integer",
          "format": "int32"
        },
        "explode": false
      },
      "page_token": {
        "schema": {
          "type": "string"
        },
        "explode": false
      }
    }
  },
  "getStorageCredential": {
    "parameter_order": [
      "credential_name"
    ],
    "parameters": {
      "credential_name": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "updateStorageCredential": {
    "parameter_order": [
      "credential_name"
    ],
    "parameters": {
      "credential_name": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "deleteStorageCredential": {
    "parameter_order": [
      "credential_name"
    ],
    "parameters": {
      "credential_name": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "listTags": {
    "parameter_order": [
      "max_results",
      "page_token"
    ],
    "parameters": {
      "max_results": {
        "schema": {
          "type": "integer",
          "format": "int32"
        },
        "explode": false
      },
      "page_token": {
        "schema": {
          "type": "string"
        },
        "explode": false
      }
    }
  },
  "getTag": {
    "parameter_order": [
      "tag_id"
    ],
    "parameters": {
      "tag_id": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "updateTag": {
    "parameter_order": [
      "tag_id"
    ],
    "parameters": {
      "tag_id": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "deleteTag": {
    "parameter_order": [
      "tag_id"
    ],
    "parameters": {
      "tag_id": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "listTagAssignments": {
    "parameter_order": [
      "tag_id",
      "max_results",
      "page_token"
    ],
    "parameters": {
      "tag_id": {
        "schema": {
          "type": "string"
        }
      },
      "max_results": {
        "schema": {
          "type": "integer",
          "format": "int32"
        },
        "explode": false
      },
      "page_token": {
        "schema": {
          "type": "string"
        },
        "explode": false
      }
    }
  },
  "createTagAssignment": {
    "parameter_order": [
      "tag_id"
    ],
    "parameters": {
      "tag_id": {
        "schema": {
          "type": "string"
        }
      }
    },
    "responses": {
      "400": {
        "any_of": [
          {
            "ref": "Error"
          },
          {
            "ref": "Error"
          }
        ]
      }
    }
  },
  "deleteTagAssignment": {
    "parameter_order": [
      "tag_id",
      "assignment_id"
    ],
    "parameters": {
      "tag_id": {
        "schema": {
          "type": "string"
        }
      },
      "assignment_id": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "listWorkspaces": {
    "parameter_order": [
      "max_results",
      "page_token"
    ],
    "parameters": {
      "max_results": {
        "schema": {
          "type": "integer",
          "format": "int32"
        },
        "explode": false
      },
      "page_token": {
        "schema": {
          "type": "string"
        },
        "explode": false
      }
    }
  },
  "getWorkspace": {
    "parameter_order": [
      "workspace_id"
    ],
    "parameters": {
      "workspace_id": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "updateWorkspace": {
    "parameter_order": [
      "workspace_id"
    ],
    "parameters": {
      "workspace_id": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "deleteWorkspace": {
    "parameter_order": [
      "workspace_id"
    ],
    "parameters": {
      "workspace_id": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "listFolders": {
    "parameter_order": [
      "workspace_id",
      "max_results",
      "page_token"
    ],
    "parameters": {
      "workspace_id": {
        "schema": {
          "type": "string"
        }
      },
      "max_results": {
        "schema": {
          "type": "integer",
          "format": "int32"
        },
        "explode": false
      },
      "page_token": {
        "schema": {
          "type": "string"
        },
        "explode": false
      }
    }
  },
  "createFolder": {
    "parameter_order": [
      "workspace_id"
    ],
    "parameters": {
      "workspace_id": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "listWorkspaceMembers": {
    "parameter_order": [
      "workspace_id"
    ],
    "parameters": {
      "workspace_id": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "addWorkspaceMember": {
    "parameter_order": [
      "workspace_id"
    ],
    "parameters": {
      "workspace_id": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "removeWorkspaceMember": {
    "parameter_order": [
      "workspace_id",
      "principal_name"
    ],
    "parameters": {
      "workspace_id": {
        "schema": {
          "type": "string"
        }
      },
      "principal_name": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "listWorkspaceProjects": {
    "parameter_order": [
      "workspace_id",
      "max_results",
      "page_token"
    ],
    "parameters": {
      "workspace_id": {
        "schema": {
          "type": "string"
        }
      },
      "max_results": {
        "schema": {
          "type": "integer",
          "format": "int32"
        },
        "explode": false
      },
      "page_token": {
        "schema": {
          "type": "string"
        },
        "explode": false
      }
    }
  },
  "createWorkspaceProject": {
    "parameter_order": [
      "workspace_id"
    ],
    "parameters": {
      "workspace_id": {
        "schema": {
          "type": "string"
        }
      }
    }
  }
}
