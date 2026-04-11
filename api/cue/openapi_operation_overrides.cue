package api

openapi_operation_overrides: {
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
  }
}
