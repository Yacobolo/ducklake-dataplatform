package api

openapi_operation_overrides: {
  "listCatalogs": {
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
    "parameters": {
      "catalog_name": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "updateCatalogRegistration": {
    "parameters": {
      "catalog_name": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "deleteCatalogRegistration": {
    "parameters": {
      "catalog_name": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "setDefaultCatalog": {
    "parameters": {
      "catalog_name": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "listCatalogHistory": {
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
    "parameters": {
      "catalog_name": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "listSchemas": {
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
    "parameters": {
      "catalog_name": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "getSchema": {
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
    "parameters": {
      "catalog_name": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "listClassifications": {
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
    "parameters": {
      "column_mask_id": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "updateColumnMask": {
    "parameters": {
      "column_mask_id": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "deleteColumnMask": {
    "parameters": {
      "column_mask_id": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "listColumnMaskBindings": {
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
    "parameters": {
      "column_mask_id": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "unbindColumnMask": {
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
    "parameters": {
      "product_slug": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "updateDataProduct": {
    "parameters": {
      "product_slug": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "deleteDataProduct": {
    "parameters": {
      "product_slug": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "listDataProductDependencies": {
    "parameters": {
      "product_slug": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "createDataProductDependency": {
    "parameters": {
      "product_slug": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "listDataProductEvents": {
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
    "parameters": {
      "product_slug": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "listDataProductSemanticEntrypoints": {
    "parameters": {
      "product_slug": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "getDataProductStatus": {
    "parameters": {
      "product_slug": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "listDataProductSubscriptions": {
    "parameters": {
      "product_slug": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "createDataProductSubscription": {
    "parameters": {
      "product_slug": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "listDataProductVersions": {
    "parameters": {
      "product_slug": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "createDataProductVersion": {
    "parameters": {
      "product_slug": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "getDataProductVersion": {
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
    "parameters": {
      "git_repo_id": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "deleteGitRepo": {
    "parameters": {
      "git_repo_id": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "syncGitRepo": {
    "parameters": {
      "git_repo_id": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "listGrants": {
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
    "parameters": {
      "grant_id": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "listMacros": {
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
    "parameters": {
      "macro_name": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "updateMacro": {
    "parameters": {
      "macro_name": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "deleteMacro": {
    "parameters": {
      "macro_name": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "getMacroImpact": {
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
    "parameters": {
      "macro_name": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "listModelRuns": {
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
    "parameters": {
      "run_id": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "cancelModelRun": {
    "parameters": {
      "run_id": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "listModelRunSteps": {
    "parameters": {
      "run_id": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "listModelTestResults": {
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
    "parameters": {
      "notebook_id": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "unpublishNotebookModel": {
    "parameters": {
      "notebook_id": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "listProductDomains": {
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
    "parameters": {
      "domain_name": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "updateProductDomain": {
    "parameters": {
      "domain_name": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "deleteProductDomain": {
    "parameters": {
      "domain_name": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "listProductTeams": {
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
    "parameters": {
      "domain_name": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "getProductTeam": {
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
    "parameters": {
      "row_filter_id": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "updateRowFilter": {
    "parameters": {
      "row_filter_id": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "deleteRowFilter": {
    "parameters": {
      "row_filter_id": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "listRowFilterBindings": {
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
    "parameters": {
      "row_filter_id": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "unbindRowFilter": {
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
    "parameters": {
      "tag_id": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "updateTag": {
    "parameters": {
      "tag_id": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "deleteTag": {
    "parameters": {
      "tag_id": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "listTagAssignments": {
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
    "parameters": {
      "tag_id": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "deleteTagAssignment": {
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
