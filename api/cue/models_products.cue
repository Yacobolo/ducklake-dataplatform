package api

// Authored product schemas.

schemas_products: {
  "CreateDataProductRequest": {
    "type": "object",
    "properties": {
      "access_request_path": {
        "schema": {
          "type": "string"
        }
      },
      "business_definitions": {
        "schema": {
          "ref": "Record"
        }
      },
      "consumer_audience": {
        "schema": {
          "type": "string"
        }
      },
      "contact_channel": {
        "schema": {
          "type": "string"
        }
      },
      "contract": {
        "schema": {
          "ref": "ProductContract"
        }
      },
      "created_by": {
        "schema": {
          "type": "string"
        }
      },
      "description": {
        "schema": {
          "type": "string"
        }
      },
      "docs_url": {
        "schema": {
          "type": "string"
        }
      },
      "domain_name": {
        "schema": {
          "type": "string"
        }
      },
      "name": {
        "schema": {
          "type": "string"
        }
      },
      "primary_asset_key": {
        "schema": {
          "type": "string"
        }
      },
      "producing_build_id": {
        "schema": {
          "type": "string"
        }
      },
      "semantic_model_refs": {
        "schema": {
          "type": "array",
          "items": {
            "type": "string"
          }
        }
      },
      "slo": {
        "schema": {
          "ref": "ProductSLO"
        }
      },
      "slug": {
        "schema": {
          "type": "string"
        }
      },
      "steward_principal": {
        "schema": {
          "type": "string"
        }
      },
      "team_name": {
        "schema": {
          "type": "string"
        }
      },
      "visibility": {
        "schema": {
          "type": "string"
        }
      }
    },
    "required": [
      "slug",
      "name",
      "domain_name",
      "team_name",
      "steward_principal",
      "contact_channel"
    ]
  },
  "CreateDataProductVersionRequest": {
    "type": "object",
    "properties": {
      "access_request_path": {
        "schema": {
          "type": "string"
        }
      },
      "compatibility_level": {
        "schema": {
          "type": "string"
        }
      },
      "contract": {
        "schema": {
          "ref": "ProductContract"
        }
      },
      "created_by": {
        "schema": {
          "type": "string"
        }
      },
      "docs_url": {
        "schema": {
          "type": "string"
        }
      },
      "output_asset_keys": {
        "schema": {
          "type": "array",
          "items": {
            "type": "string"
          }
        }
      },
      "producing_build_id": {
        "schema": {
          "type": "string"
        }
      },
      "semantic_model_refs": {
        "schema": {
          "type": "array",
          "items": {
            "type": "string"
          }
        }
      },
      "slo": {
        "schema": {
          "ref": "ProductSLO"
        }
      }
    }
  },
  "CreateProductDependencyRequest": {
    "type": "object",
    "properties": {
      "depends_on_slug": {
        "schema": {
          "type": "string"
        }
      }
    },
    "required": [
      "depends_on_slug"
    ]
  },
  "CreateProductDomainRequest": {
    "type": "object",
    "properties": {
      "description": {
        "schema": {
          "type": "string"
        }
      },
      "name": {
        "schema": {
          "type": "string"
        }
      }
    },
    "required": [
      "name"
    ]
  },
  "CreateProductSubscriptionRequest": {
    "type": "object",
    "properties": {
      "channel": {
        "schema": {
          "type": "string"
        }
      },
      "event_type": {
        "schema": {
          "type": "string"
        }
      },
      "principal_name": {
        "schema": {
          "type": "string"
        }
      }
    },
    "required": [
      "principal_name",
      "event_type"
    ]
  },
  "CreateProductTeamRequest": {
    "type": "object",
    "properties": {
      "contact_channel": {
        "schema": {
          "type": "string"
        }
      },
      "name": {
        "schema": {
          "type": "string"
        }
      }
    },
    "required": [
      "name"
    ]
  },
  "DataProduct": {
    "type": "object",
    "properties": {
      "access_request_path": {
        "schema": {
          "type": "string"
        }
      },
      "business_definitions": {
        "schema": {
          "ref": "Record"
        }
      },
      "consumer_audience": {
        "schema": {
          "type": "string"
        }
      },
      "contact_channel": {
        "schema": {
          "type": "string"
        }
      },
      "contract": {
        "schema": {
          "ref": "ProductContract"
        }
      },
      "created_at": {
        "schema": {
          "type": "string"
        }
      },
      "created_by": {
        "schema": {
          "type": "string"
        }
      },
      "description": {
        "schema": {
          "type": "string"
        }
      },
      "docs_url": {
        "schema": {
          "type": "string"
        }
      },
      "domain_id": {
        "schema": {
          "type": "string"
        }
      },
      "id": {
        "schema": {
          "type": "string"
        }
      },
      "name": {
        "schema": {
          "type": "string"
        }
      },
      "owner_team_id": {
        "schema": {
          "type": "string"
        }
      },
      "publication_intent": {
        "schema": {
          "type": "string"
        }
      },
      "slo": {
        "schema": {
          "ref": "ProductSLO"
        }
      },
      "slug": {
        "schema": {
          "type": "string"
        }
      },
      "steward_principal": {
        "schema": {
          "type": "string"
        }
      },
      "updated_at": {
        "schema": {
          "type": "string"
        }
      },
      "visibility": {
        "schema": {
          "type": "string"
        }
      }
    },
    "required": [
      "id",
      "slug",
      "name",
      "description",
      "domain_id",
      "owner_team_id",
      "steward_principal",
      "contact_channel"
    ]
  },
  "DataProductDetail": {
    "type": "object",
    "properties": {
      "dependencies": {
        "schema": {
          "type": "array",
          "items": {
            "ref": "DataProductListItem"
          }
        }
      },
      "domain": {
        "schema": {
          "ref": "ProductDomain"
        }
      },
      "events": {
        "schema": {
          "type": "array",
          "items": {
            "ref": "ProductEvent"
          }
        }
      },
      "outputs": {
        "schema": {
          "type": "array",
          "items": {
            "ref": "ProductOutput"
          }
        }
      },
      "owner_team": {
        "schema": {
          "ref": "ProductTeam"
        }
      },
      "product": {
        "schema": {
          "ref": "DataProduct"
        }
      },
      "semantic_entrypoints": {
        "schema": {
          "type": "array",
          "items": {
            "ref": "ProductSemanticEntrypoint"
          }
        }
      },
      "status": {
        "schema": {
          "ref": "DataProductStatus"
        }
      },
      "subscriptions": {
        "schema": {
          "type": "array",
          "items": {
            "ref": "ProductSubscription"
          }
        }
      },
      "versions": {
        "schema": {
          "type": "array",
          "items": {
            "ref": "DataProductVersion"
          }
        }
      }
    },
    "required": [
      "product",
      "domain",
      "owner_team",
      "versions",
      "outputs",
      "semantic_entrypoints",
      "dependencies",
      "subscriptions",
      "events"
    ]
  },
  "DataProductListItem": {
    "type": "object",
    "properties": {
      "domain": {
        "schema": {
          "ref": "ProductDomain"
        }
      },
      "latest_version": {
        "schema": {
          "ref": "DataProductVersion"
        }
      },
      "owner_team": {
        "schema": {
          "ref": "ProductTeam"
        }
      },
      "primary_output": {
        "schema": {
          "ref": "ProductOutput"
        }
      },
      "product": {
        "schema": {
          "ref": "DataProduct"
        }
      },
      "status": {
        "schema": {
          "ref": "DataProductStatus"
        }
      }
    },
    "required": [
      "product",
      "domain",
      "owner_team"
    ]
  },
  "DataProductStatus": {
    "type": "object",
    "properties": {
      "adoption_metrics": {
        "schema": {
          "ref": "Record"
        }
      },
      "certification_state": {
        "schema": {
          "type": "string"
        }
      },
      "failing_checks_count": {
        "schema": {
          "type": "integer",
          "format": "int64"
        }
      },
      "freshness_status": {
        "schema": {
          "type": "string"
        }
      },
      "last_successful_update_at": {
        "schema": {
          "type": "string"
        }
      },
      "lineage_coverage": {
        "schema": {
          "type": "number"
        }
      },
      "open_warnings": {
        "schema": {
          "type": "array",
          "items": {
            "type": "string"
          }
        }
      },
      "product_id": {
        "schema": {
          "type": "string"
        }
      },
      "publication_state": {
        "schema": {
          "type": "string"
        }
      },
      "quality_status": {
        "schema": {
          "type": "string"
        }
      },
      "replacement_product_id": {
        "schema": {
          "type": "string"
        }
      },
      "updated_at": {
        "schema": {
          "type": "string"
        }
      }
    },
    "required": [
      "product_id",
      "publication_state",
      "certification_state",
      "freshness_status",
      "quality_status",
      "failing_checks_count"
    ]
  },
  "DataProductVersion": {
    "type": "object",
    "properties": {
      "access_request_path": {
        "schema": {
          "type": "string"
        }
      },
      "compatibility_level": {
        "schema": {
          "type": "string"
        }
      },
      "contract": {
        "schema": {
          "ref": "ProductContract"
        }
      },
      "created_at": {
        "schema": {
          "type": "string"
        }
      },
      "created_by": {
        "schema": {
          "type": "string"
        }
      },
      "docs_url": {
        "schema": {
          "type": "string"
        }
      },
      "id": {
        "schema": {
          "type": "string"
        }
      },
      "producing_build_id": {
        "schema": {
          "type": "string"
        }
      },
      "product_id": {
        "schema": {
          "type": "string"
        }
      },
      "release_state": {
        "schema": {
          "type": "string"
        }
      },
      "slo": {
        "schema": {
          "ref": "ProductSLO"
        }
      },
      "version": {
        "schema": {
          "type": "integer",
          "format": "int32"
        }
      }
    },
    "required": [
      "id",
      "product_id",
      "version",
      "release_state",
      "compatibility_level"
    ]
  },
  "DataProductVersionDetail": {
    "type": "object",
    "properties": {
      "dependencies": {
        "schema": {
          "type": "array",
          "items": {
            "ref": "DataProductListItem"
          }
        }
      },
      "domain": {
        "schema": {
          "ref": "ProductDomain"
        }
      },
      "events": {
        "schema": {
          "type": "array",
          "items": {
            "ref": "ProductEvent"
          }
        }
      },
      "outputs": {
        "schema": {
          "type": "array",
          "items": {
            "ref": "ProductOutput"
          }
        }
      },
      "owner_team": {
        "schema": {
          "ref": "ProductTeam"
        }
      },
      "product": {
        "schema": {
          "ref": "DataProduct"
        }
      },
      "semantic_entrypoints": {
        "schema": {
          "type": "array",
          "items": {
            "ref": "ProductSemanticEntrypoint"
          }
        }
      },
      "status": {
        "schema": {
          "ref": "DataProductStatus"
        }
      },
      "version": {
        "schema": {
          "ref": "DataProductVersion"
        }
      }
    },
    "required": [
      "product",
      "domain",
      "owner_team",
      "version",
      "outputs",
      "semantic_entrypoints",
      "dependencies",
      "events"
    ]
  },
  "DataProductVersionList": {
    "type": "object",
    "properties": {
      "data": {
        "schema": {
          "type": "array",
          "items": {
            "ref": "DataProductVersion"
          }
        }
      }
    },
    "required": [
      "data"
    ]
  },
  "DeprecateProductVersionRequest": {
    "type": "object",
    "properties": {
      "replacement_slug": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "OrphanResource": {
    "type": "object",
    "properties": {
      "resource_id": {
        "schema": {
          "type": "string"
        }
      },
      "resource_name": {
        "schema": {
          "type": "string"
        }
      },
      "resource_type": {
        "schema": {
          "type": "string"
        }
      }
    },
    "required": [
      "resource_type",
      "resource_id",
      "resource_name"
    ]
  },
  "PaginatedDataProducts": {
    "type": "object",
    "properties": {
      "data": {
        "schema": {
          "type": "array",
          "items": {
            "ref": "DataProductListItem"
          }
        }
      },
      "next_page_token": {
        "schema": {
          "type": "string"
        }
      }
    },
    "required": [
      "data"
    ]
  },
  "PaginatedProductDomains": {
    "type": "object",
    "properties": {
      "data": {
        "schema": {
          "type": "array",
          "items": {
            "ref": "ProductDomain"
          }
        }
      },
      "next_page_token": {
        "schema": {
          "type": "string"
        }
      }
    },
    "required": [
      "data"
    ]
  },
  "PaginatedProductTeams": {
    "type": "object",
    "properties": {
      "data": {
        "schema": {
          "type": "array",
          "items": {
            "ref": "ProductTeam"
          }
        }
      },
      "next_page_token": {
        "schema": {
          "type": "string"
        }
      }
    },
    "required": [
      "data"
    ]
  },
  "ProductAdoptionSummary": {
    "type": "object",
    "properties": {
      "adoption_score": {
        "schema": {
          "type": "integer",
          "format": "int64"
        }
      },
      "domain_name": {
        "schema": {
          "type": "string"
        }
      },
      "downstream_product_count": {
        "schema": {
          "type": "integer",
          "format": "int64"
        }
      },
      "output_count": {
        "schema": {
          "type": "integer",
          "format": "int64"
        }
      },
      "product_id": {
        "schema": {
          "type": "string"
        }
      },
      "product_name": {
        "schema": {
          "type": "string"
        }
      },
      "product_slug": {
        "schema": {
          "type": "string"
        }
      },
      "semantic_entrypoint_count": {
        "schema": {
          "type": "integer",
          "format": "int64"
        }
      },
      "subscriber_count": {
        "schema": {
          "type": "integer",
          "format": "int64"
        }
      },
      "team_name": {
        "schema": {
          "type": "string"
        }
      }
    },
    "required": [
      "product_id",
      "product_slug",
      "product_name",
      "domain_name",
      "team_name",
      "subscriber_count",
      "downstream_product_count",
      "output_count",
      "semantic_entrypoint_count",
      "adoption_score"
    ]
  },
  "ProductContract": {
    "type": "object",
    "properties": {
      "breaking_change_policy": {
        "schema": {
          "type": "string"
        }
      },
      "data_grain": {
        "schema": {
          "type": "string"
        }
      },
      "dimensions": {
        "schema": {
          "type": "array",
          "items": {
            "type": "string"
          }
        }
      },
      "join_keys": {
        "schema": {
          "type": "array",
          "items": {
            "type": "string"
          }
        }
      },
      "measures": {
        "schema": {
          "type": "array",
          "items": {
            "type": "string"
          }
        }
      },
      "primary_keys": {
        "schema": {
          "type": "array",
          "items": {
            "type": "string"
          }
        }
      },
      "quality_expectations": {
        "schema": {
          "type": "array",
          "items": {
            "type": "string"
          }
        }
      },
      "retention_window": {
        "schema": {
          "type": "string"
        }
      },
      "sample_queries": {
        "schema": {
          "type": "array",
          "items": {
            "type": "string"
          }
        }
      },
      "update_cadence": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "ProductDependencyList": {
    "type": "object",
    "properties": {
      "data": {
        "schema": {
          "type": "array",
          "items": {
            "ref": "DataProductListItem"
          }
        }
      }
    },
    "required": [
      "data"
    ]
  },
  "ProductDomain": {
    "type": "object",
    "properties": {
      "created_at": {
        "schema": {
          "type": "string"
        }
      },
      "description": {
        "schema": {
          "type": "string"
        }
      },
      "id": {
        "schema": {
          "type": "string"
        }
      },
      "name": {
        "schema": {
          "type": "string"
        }
      },
      "updated_at": {
        "schema": {
          "type": "string"
        }
      }
    },
    "required": [
      "id",
      "name",
      "description"
    ]
  },
  "ProductEvent": {
    "type": "object",
    "properties": {
      "created_at": {
        "schema": {
          "type": "string"
        }
      },
      "description": {
        "schema": {
          "type": "string"
        }
      },
      "event_type": {
        "schema": {
          "type": "string"
        }
      },
      "id": {
        "schema": {
          "type": "string"
        }
      },
      "metadata": {
        "schema": {
          "ref": "Record"
        }
      },
      "product_id": {
        "schema": {
          "type": "string"
        }
      },
      "title": {
        "schema": {
          "type": "string"
        }
      }
    },
    "required": [
      "id",
      "product_id",
      "event_type",
      "title",
      "description"
    ]
  },
  "ProductEventList": {
    "type": "object",
    "properties": {
      "data": {
        "schema": {
          "type": "array",
          "items": {
            "ref": "ProductEvent"
          }
        }
      },
      "next_page_token": {
        "schema": {
          "type": "string"
        }
      }
    },
    "required": [
      "data"
    ]
  },
  "ProductOutput": {
    "type": "object",
    "properties": {
      "asset_id": {
        "schema": {
          "type": "string"
        }
      },
      "asset_key": {
        "schema": {
          "type": "string"
        }
      },
      "asset_type": {
        "schema": {
          "type": "string"
        }
      },
      "created_at": {
        "schema": {
          "type": "string"
        }
      },
      "id": {
        "schema": {
          "type": "string"
        }
      },
      "is_primary": {
        "schema": {
          "type": "boolean"
        }
      },
      "product_version_id": {
        "schema": {
          "type": "string"
        }
      }
    },
    "required": [
      "id",
      "product_version_id",
      "asset_id",
      "asset_key",
      "asset_type",
      "is_primary"
    ]
  },
  "ProductOutputList": {
    "type": "object",
    "properties": {
      "data": {
        "schema": {
          "type": "array",
          "items": {
            "ref": "ProductOutput"
          }
        }
      }
    },
    "required": [
      "data"
    ]
  },
  "ProductPortfolioGroup": {
    "type": "object",
    "properties": {
      "average_completeness_pct": {
        "schema": {
          "type": "integer",
          "format": "int32"
        }
      },
      "certified_count": {
        "schema": {
          "type": "integer",
          "format": "int64"
        }
      },
      "name": {
        "schema": {
          "type": "string"
        }
      },
      "product_count": {
        "schema": {
          "type": "integer",
          "format": "int64"
        }
      },
      "published_count": {
        "schema": {
          "type": "integer",
          "format": "int64"
        }
      }
    },
    "required": [
      "name",
      "product_count",
      "published_count",
      "certified_count",
      "average_completeness_pct"
    ]
  },
  "ProductPortfolioReport": {
    "type": "object",
    "properties": {
      "domain_scorecards": {
        "schema": {
          "type": "array",
          "items": {
            "ref": "ProductPortfolioGroup"
          }
        }
      },
      "high_blast_radius": {
        "schema": {
          "type": "array",
          "items": {
            "ref": "ProductAdoptionSummary"
          }
        }
      },
      "least_adopted": {
        "schema": {
          "type": "array",
          "items": {
            "ref": "ProductAdoptionSummary"
          }
        }
      },
      "orphan_assets": {
        "schema": {
          "type": "array",
          "items": {
            "ref": "OrphanResource"
          }
        }
      },
      "orphan_semantic_models": {
        "schema": {
          "type": "array",
          "items": {
            "ref": "OrphanResource"
          }
        }
      },
      "team_scorecards": {
        "schema": {
          "type": "array",
          "items": {
            "ref": "ProductPortfolioGroup"
          }
        }
      },
      "top_used": {
        "schema": {
          "type": "array",
          "items": {
            "ref": "ProductAdoptionSummary"
          }
        }
      }
    },
    "required": [
      "top_used",
      "least_adopted",
      "high_blast_radius",
      "domain_scorecards",
      "team_scorecards",
      "orphan_assets",
      "orphan_semantic_models"
    ]
  },
  "ProductSLO": {
    "type": "object",
    "properties": {
      "freshness_slo": {
        "schema": {
          "type": "string"
        }
      },
      "latency_slo": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "ProductScorecard": {
    "type": "object",
    "properties": {
      "certification_state": {
        "schema": {
          "type": "string"
        }
      },
      "completeness_percent": {
        "schema": {
          "type": "integer",
          "format": "int32"
        }
      },
      "domain_name": {
        "schema": {
          "type": "string"
        }
      },
      "has_contract": {
        "schema": {
          "type": "boolean"
        }
      },
      "has_docs_or_access_path": {
        "schema": {
          "type": "boolean"
        }
      },
      "has_owner": {
        "schema": {
          "type": "boolean"
        }
      },
      "has_primary_output": {
        "schema": {
          "type": "boolean"
        }
      },
      "has_slo": {
        "schema": {
          "type": "boolean"
        }
      },
      "has_warnings": {
        "schema": {
          "type": "boolean"
        }
      },
      "product_id": {
        "schema": {
          "type": "string"
        }
      },
      "product_name": {
        "schema": {
          "type": "string"
        }
      },
      "product_slug": {
        "schema": {
          "type": "string"
        }
      },
      "publication_state": {
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
    "required": [
      "product_id",
      "product_slug",
      "product_name",
      "domain_name",
      "team_name",
      "publication_state",
      "certification_state",
      "has_owner",
      "has_contract",
      "has_slo",
      "has_docs_or_access_path",
      "has_primary_output",
      "has_warnings",
      "completeness_percent"
    ]
  },
  "ProductScorecardList": {
    "type": "object",
    "properties": {
      "data": {
        "schema": {
          "type": "array",
          "items": {
            "ref": "ProductScorecard"
          }
        }
      },
      "next_page_token": {
        "schema": {
          "type": "string"
        }
      }
    },
    "required": [
      "data"
    ]
  },
  "ProductSemanticEntrypoint": {
    "type": "object",
    "properties": {
      "created_at": {
        "schema": {
          "type": "string"
        }
      },
      "id": {
        "schema": {
          "type": "string"
        }
      },
      "model_name": {
        "schema": {
          "type": "string"
        }
      },
      "product_version_id": {
        "schema": {
          "type": "string"
        }
      },
      "semantic_model_id": {
        "schema": {
          "type": "string"
        }
      }
    },
    "required": [
      "id",
      "product_version_id",
      "semantic_model_id",
      "model_name"
    ]
  },
  "ProductSemanticEntrypointList": {
    "type": "object",
    "properties": {
      "data": {
        "schema": {
          "type": "array",
          "items": {
            "ref": "ProductSemanticEntrypoint"
          }
        }
      }
    },
    "required": [
      "data"
    ]
  },
  "ProductSubscription": {
    "type": "object",
    "properties": {
      "channel": {
        "schema": {
          "type": "string"
        }
      },
      "created_at": {
        "schema": {
          "type": "string"
        }
      },
      "event_type": {
        "schema": {
          "type": "string"
        }
      },
      "id": {
        "schema": {
          "type": "string"
        }
      },
      "principal_name": {
        "schema": {
          "type": "string"
        }
      },
      "product_id": {
        "schema": {
          "type": "string"
        }
      }
    },
    "required": [
      "id",
      "product_id",
      "principal_name",
      "event_type",
      "channel"
    ]
  },
  "ProductSubscriptionList": {
    "type": "object",
    "properties": {
      "data": {
        "schema": {
          "type": "array",
          "items": {
            "ref": "ProductSubscription"
          }
        }
      }
    },
    "required": [
      "data"
    ]
  },
  "ProductTeam": {
    "type": "object",
    "properties": {
      "contact_channel": {
        "schema": {
          "type": "string"
        }
      },
      "created_at": {
        "schema": {
          "type": "string"
        }
      },
      "domain_id": {
        "schema": {
          "type": "string"
        }
      },
      "id": {
        "schema": {
          "type": "string"
        }
      },
      "name": {
        "schema": {
          "type": "string"
        }
      },
      "updated_at": {
        "schema": {
          "type": "string"
        }
      }
    },
    "required": [
      "id",
      "domain_id",
      "name",
      "contact_channel"
    ]
  },
  "UpdateDataProductRequest": {
    "type": "object",
    "properties": {
      "access_request_path": {
        "schema": {
          "type": "string"
        }
      },
      "business_definitions": {
        "schema": {
          "ref": "Record"
        }
      },
      "consumer_audience": {
        "schema": {
          "type": "string"
        }
      },
      "contact_channel": {
        "schema": {
          "type": "string"
        }
      },
      "contract": {
        "schema": {
          "ref": "ProductContract"
        }
      },
      "description": {
        "schema": {
          "type": "string"
        }
      },
      "docs_url": {
        "schema": {
          "type": "string"
        }
      },
      "domain_name": {
        "schema": {
          "type": "string"
        }
      },
      "name": {
        "schema": {
          "type": "string"
        }
      },
      "publication_intent": {
        "schema": {
          "type": "string"
        }
      },
      "slo": {
        "schema": {
          "ref": "ProductSLO"
        }
      },
      "steward_principal": {
        "schema": {
          "type": "string"
        }
      },
      "team_name": {
        "schema": {
          "type": "string"
        }
      },
      "visibility": {
        "schema": {
          "type": "string"
        }
      }
    },
    "required": [
      "name",
      "domain_name",
      "team_name",
      "steward_principal",
      "contact_channel"
    ]
  },
  "UpdateProductDomainRequest": {
    "type": "object",
    "properties": {
      "description": {
        "schema": {
          "type": "string"
        }
      }
    }
  },
  "UpdateProductTeamRequest": {
    "type": "object",
    "properties": {
      "contact_channel": {
        "schema": {
          "type": "string"
        }
      }
    }
  }
}

