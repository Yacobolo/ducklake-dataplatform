package api

// Authored product schemas.

schemas_products: {
  CreateDataProductRequest: #objectSchema & {
    example: {
      slug:                "customer-360"
      name:                "Customer 360"
      description:         "Unified customer profile product for downstream activation and analytics."
      domain_name:         "growth"
      team_name:           "customer-platform"
      steward_principal:   "alice@example.com"
      contact_channel:     "#customer-data"
      visibility:          "internal"
      consumer_audience:   "sales, marketing, support"
      docs_url:            "https://docs.example.com/products/customer-360"
      access_request_path: "https://jira.example.com/servicedesk/customer-360"
      business_definitions: {
        customer: "A billing-account level customer record."
        mrr:      "Monthly recurring revenue attributed to the customer."
      }
      contract: {
        data_grain:             "one row per active customer per day"
        primary_keys:           ["customer_id", "snapshot_date"]
        dimensions:             ["customer_id", "segment", "region"]
        measures:               ["mrr", "active_subscriptions"]
        quality_expectations:   ["customer_id is never null", "mrr is non-negative"]
        retention_window:       "730d"
        update_cadence:         "daily by 06:00 UTC"
        breaking_change_policy: "90 day notice"
        sample_queries:         ["select customer_id, mrr from mart.customer_360 where snapshot_date = current_date"]
      }
      slo: {
        freshness_slo: "6h"
        latency_slo:   "p95 < 3s"
      }
      producing_build_id: "build_01hzyprod123"
      primary_asset_key:  "mart.customer_360"
      semantic_model_refs:["semantic.customer_360"]
      created_by:         "alice@example.com"
    }
    #fields: {
      slug: #stringProperty,
      name: #nameProperty,
      description: #descriptionProperty,
      domain_name: #stringProperty,
      team_name: #stringProperty,
      steward_principal: #stringProperty,
      contact_channel: #stringProperty,
      visibility: #stringProperty,
      consumer_audience: #stringProperty,
      docs_url: #stringProperty,
      access_request_path: #stringProperty,
      business_definitions: #stringMapProperty,
      contract: #refProperty & {#ref: "ProductContract"},
      slo: #refProperty & {#ref: "ProductSLO"},
      producing_build_id: #stringProperty,
      primary_asset_key: #stringProperty,
      semantic_model_refs: #stringArrayProperty,
      created_by: #stringProperty
    },
    #required: [
      "slug",
      "name",
      "domain_name",
      "team_name",
      "steward_principal",
      "contact_channel"
    ]
  },
  CreateDataProductVersionRequest: #objectSchema & {
    #fields: {
      compatibility_level: #stringProperty,
      contract: #refProperty & {#ref: "ProductContract"},
      slo: #refProperty & {#ref: "ProductSLO"},
      docs_url: #stringProperty,
      access_request_path: #stringProperty,
      producing_build_id: #stringProperty,
      output_asset_keys: #stringArrayProperty,
      semantic_model_refs: #stringArrayProperty,
      created_by: #stringProperty
    }
  },
  CreateProductDependencyRequest: #objectSchema & {
    #fields: {
      depends_on_slug: #stringProperty
    },
    #required: [
      "depends_on_slug"
    ]
  },
  CreateProductDomainRequest: #objectSchema & {
    example: {
      name:        "growth"
      description: "Data products supporting acquisition, activation, and retention use cases."
    }
    #fields: {
      name: #nameProperty,
      description: #descriptionProperty
    },
    #required: [
      "name"
    ]
  },
  CreateProductSubscriptionRequest: #objectSchema & {
    #fields: {
      principal_name: #principalNameProperty,
      event_type: #stringProperty,
      channel: #stringProperty
    },
    #required: [
      "principal_name",
      "event_type"
    ]
  },
  CreateProductTeamRequest: #objectSchema & {
    example: {
      name:            "customer-platform"
      contact_channel: "#customer-data"
    }
    #fields: {
      name: #nameProperty,
      contact_channel: #stringProperty
    },
    #required: [
      "name"
    ]
  },
  DataProduct: #objectSchema & {
    example: {
      id:                  "prd_01hzycust360"
      slug:                "customer-360"
      name:                "Customer 360"
      description:         "Unified customer profile product for downstream activation and analytics."
      domain_id:           "dom_growth"
      owner_team_id:       "team_customer_platform"
      steward_principal:   "alice@example.com"
      contact_channel:     "#customer-data"
      visibility:          "internal"
      consumer_audience:   "sales, marketing, support"
      docs_url:            "https://docs.example.com/products/customer-360"
      access_request_path: "https://jira.example.com/servicedesk/customer-360"
      business_definitions: {
        customer: "A billing-account level customer record."
      }
      contract: {
        data_grain:   "one row per active customer per day"
        primary_keys: ["customer_id", "snapshot_date"]
        dimensions:   ["customer_id", "segment", "region"]
        measures:     ["mrr", "active_subscriptions"]
      }
      created_by:         "alice@example.com"
      publication_intent: "published"
      slo: {
        freshness_slo: "6h"
        latency_slo:   "p95 < 3s"
      }
      created_at: "2026-04-12T10:00:00Z"
      updated_at: "2026-04-13T09:00:00Z"
    }
    #fields: {
      id: #idProperty,
      slug: #stringProperty,
      name: #nameProperty,
      description: #descriptionProperty,
      domain_id: #stringProperty,
      owner_team_id: #stringProperty,
      steward_principal: #stringProperty,
      contact_channel: #stringProperty,
      visibility: #stringProperty,
      consumer_audience: #stringProperty,
      docs_url: #stringProperty,
      access_request_path: #stringProperty,
      business_definitions: #stringMapProperty,
      contract: #refProperty & {#ref: "ProductContract"},
      created_by: #stringProperty,
      publication_intent: #stringProperty,
      slo: #refProperty & {#ref: "ProductSLO"},
      created_at: #createdAtProperty,
      updated_at: #updatedAtProperty,
    },
    #required: [
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
  DataProductDetail: #objectSchema & {
    #fields: {
      product: #refProperty & {#ref: "DataProduct"},
      domain: #refProperty & {#ref: "ProductDomain"},
      owner_team: #refProperty & {#ref: "ProductTeam"},
      versions: #arrayRefProperty & {#ref: "DataProductVersion"},
      status: #refProperty & {#ref: "DataProductStatus"},
      outputs: #arrayRefProperty & {#ref: "ProductOutput"},
      semantic_entrypoints: #arrayRefProperty & {#ref: "ProductSemanticEntrypoint"},
      dependencies: #arrayRefProperty & {#ref: "DataProductListItem"},
      subscriptions: #arrayRefProperty & {#ref: "ProductSubscription"},
      events: #arrayRefProperty & {#ref: "ProductEvent"}
    },
    #required: [
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
  DataProductListItem: #objectSchema & {
    #fields: {
      product: #refProperty & {#ref: "DataProduct"},
      domain: #refProperty & {#ref: "ProductDomain"},
      owner_team: #refProperty & {#ref: "ProductTeam"},
      latest_version: #refProperty & {#ref: "DataProductVersion"},
      status: #refProperty & {#ref: "DataProductStatus"},
      primary_output: #refProperty & {#ref: "ProductOutput"}
    },
    #required: [
      "product",
      "domain",
      "owner_team"
    ]
  },
  DataProductStatus: #objectSchema & {
    example: {
      product_id:                 "prd_01hzycust360"
      publication_state:          "published"
      certification_state:        "certified"
      freshness_status:           "healthy"
      quality_status:             "passing"
      last_successful_update_at:  "2026-04-13T08:30:00Z"
      failing_checks_count:       0
      lineage_coverage:           0.96
      adoption_metrics: {
        monthly_active_consumers: 47
        weekly_queries:           1820
      }
      open_warnings: []
      replacement_product_id: ""
      updated_at: "2026-04-13T08:31:00Z"
    }
    #fields: {
      product_id: #stringProperty,
      publication_state: #stringProperty,
      certification_state: #stringProperty,
      freshness_status: #stringProperty,
      quality_status: #stringProperty,
      last_successful_update_at: #dateTimeProperty,
      failing_checks_count: #int64Property,
      lineage_coverage: #doubleProperty,
      adoption_metrics: #anyMapProperty,
      open_warnings: #stringArrayProperty,
      replacement_product_id: #stringProperty,
      updated_at: #updatedAtProperty
    },
    #required: [
      "product_id",
      "publication_state",
      "certification_state",
      "freshness_status",
      "quality_status",
      "failing_checks_count"
    ]
  },
  DataProductVersion: #objectSchema & {
    #fields: {
      id: #idProperty,
      product_id: #stringProperty,
      producing_build_id: #stringProperty,
      release_state: #stringProperty,
      compatibility_level: #stringProperty,
      contract: #refProperty & {#ref: "ProductContract"},
      slo: #refProperty & {#ref: "ProductSLO"},
      docs_url: #stringProperty,
      access_request_path: #stringProperty,
      created_by: #stringProperty,
      created_at: #createdAtProperty,
      version: #int32Property
    },
    #required: [
      "id",
      "product_id",
      "version",
      "release_state",
      "compatibility_level"
    ]
  },
  DataProductVersionDetail: #objectSchema & {
    #fields: {
      product: #refProperty & {#ref: "DataProduct"},
      domain: #refProperty & {#ref: "ProductDomain"},
      owner_team: #refProperty & {#ref: "ProductTeam"},
      version: #refProperty & {#ref: "DataProductVersion"},
      status: #refProperty & {#ref: "DataProductStatus"},
      outputs: #arrayRefProperty & {#ref: "ProductOutput"},
      semantic_entrypoints: #arrayRefProperty & {#ref: "ProductSemanticEntrypoint"},
      dependencies: #arrayRefProperty & {#ref: "DataProductListItem"},
      events: #arrayRefProperty & {#ref: "ProductEvent"}
    },
    #required: [
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
  DataProductVersionList: #objectSchema & {
    #fields: {
      data: #arrayRefProperty & {#ref: "DataProductVersion"}
    },
    #required: [
      "data"
    ]
  },
  DeprecateProductVersionRequest: #objectSchema & {
    #fields: {
      replacement_slug: #stringProperty
    }
  },
  OrphanResource: #objectSchema & {
    #fields: {
      resource_type: #stringProperty,
      resource_id: #stringProperty,
      resource_name: #stringProperty
    },
    #required: [
      "resource_type",
      "resource_id",
      "resource_name"
    ]
  },
  ProductAdoptionSummary: #objectSchema & {
    #fields: {
      adoption_score: #int64Property,
      domain_name: #stringProperty,
      downstream_product_count: #int64Property,
      output_count: #int64Property,
      product_id: #stringProperty,
      product_name: #stringProperty,
      product_slug: #stringProperty,
      semantic_entrypoint_count: #int64Property,
      subscriber_count: #int64Property,
      team_name: #stringProperty
    },
    #required: [
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
  ProductContract: #objectSchema & {
    example: {
      data_grain:             "one row per active customer per day"
      primary_keys:           ["customer_id", "snapshot_date"]
      join_keys:              ["customer_id", "account_id"]
      dimensions:             ["customer_id", "segment", "region"]
      measures:               ["mrr", "active_subscriptions"]
      quality_expectations:   ["customer_id is never null", "mrr is non-negative"]
      retention_window:       "730d"
      update_cadence:         "daily by 06:00 UTC"
      breaking_change_policy: "90 day notice"
      sample_queries:         ["select customer_id, mrr from mart.customer_360 where snapshot_date = current_date"]
    }
    #fields: {
      data_grain: #stringProperty,
      primary_keys: #stringArrayProperty,
      join_keys: #stringArrayProperty,
      dimensions: #stringArrayProperty,
      measures: #stringArrayProperty,
      quality_expectations: #stringArrayProperty,
      retention_window: #stringProperty,
      update_cadence: #stringProperty,
      breaking_change_policy: #stringProperty,
      sample_queries: #stringArrayProperty,
    }
  },
  ProductDependencyList: #objectSchema & {
    #fields: {
      data: #arrayRefProperty & {#ref: "DataProductListItem"}
    },
    #required: [
      "data"
    ]
  },
  ProductDomain: #objectSchema & {
    example: {
      id:          "dom_growth"
      name:        "growth"
      description: "Data products supporting acquisition, activation, and retention use cases."
      created_at:  "2026-03-01T09:00:00Z"
      updated_at:  "2026-04-01T09:00:00Z"
    }
    #fields: {
      id: #idProperty,
      name: #nameProperty,
      description: #descriptionProperty,
      created_at: #createdAtProperty,
      updated_at: #updatedAtProperty
    },
    #required: [
      "id",
      "name",
      "description"
    ]
  },
  ProductEvent: #objectSchema & {
    example: {
      id:          "evt_01hzylaunch"
      product_id:  "prd_01hzycust360"
      event_type:  "release_published"
      title:       "Customer 360 v3 published"
      description: "Version 3 added churn propensity and lifecycle segmentation."
      metadata: {
        version:       3
        release_notes: "https://docs.example.com/products/customer-360/releases/v3"
      }
      created_at: "2026-04-13T09:00:00Z"
    }
    #fields: {
      id: #idProperty,
      product_id: #stringProperty,
      event_type: #stringProperty,
      title: #stringProperty,
      description: #descriptionProperty,
      metadata: #anyMapProperty,
      created_at: #createdAtProperty
    },
    #required: [
      "id",
      "product_id",
      "event_type",
      "title",
      "description"
    ]
  },
  ProductEventList: #objectSchema & {
    #fields: {
      data: #arrayRefProperty & {#ref: "ProductEvent"},
      next_page_token: #stringProperty
    },
    #required: [
      "data"
    ]
  },
  ProductOutput: #objectSchema & {
    example: {
      id:                 "out_01hzyasset"
      product_version_id: "ver_01hzycust360_v3"
      asset_id:           "asset_01hzycust360"
      asset_key:          "mart.customer_360"
      asset_type:         "table"
      is_primary:         true
      created_at:         "2026-04-13T08:55:00Z"
    }
    #fields: {
      id: #idProperty,
      product_version_id: #stringProperty,
      asset_id: #stringProperty,
      asset_key: #stringProperty,
      asset_type: #stringProperty,
      is_primary: #boolProperty,
      created_at: #createdAtProperty
    },
    #required: [
      "id",
      "product_version_id",
      "asset_id",
      "asset_key",
      "asset_type",
      "is_primary"
    ]
  },
  ProductOutputList: #objectSchema & {
    #fields: {
      data: #arrayRefProperty & {#ref: "ProductOutput"}
    },
    #required: [
      "data"
    ]
  },
  ProductPortfolioGroup: #objectSchema & {
    #fields: {
      name: #nameProperty,
      product_count: #int64Property,
      published_count: #int64Property,
      certified_count: #int64Property,
      average_completeness_pct: #int32Property
    },
    #required: [
      "name",
      "product_count",
      "published_count",
      "certified_count",
      "average_completeness_pct"
    ]
  },
  ProductPortfolioReport: #objectSchema & {
    #fields: {
      top_used: #arrayRefProperty & {#ref: "ProductAdoptionSummary"},
      least_adopted: #arrayRefProperty & {#ref: "ProductAdoptionSummary"},
      high_blast_radius: #arrayRefProperty & {#ref: "ProductAdoptionSummary"},
      domain_scorecards: #arrayRefProperty & {#ref: "ProductPortfolioGroup"},
      team_scorecards: #arrayRefProperty & {#ref: "ProductPortfolioGroup"},
      orphan_assets: #arrayRefProperty & {#ref: "OrphanResource"},
      orphan_semantic_models: #arrayRefProperty & {#ref: "OrphanResource"}
    },
    #required: [
      "top_used",
      "least_adopted",
      "high_blast_radius",
      "domain_scorecards",
      "team_scorecards",
      "orphan_assets",
      "orphan_semantic_models"
    ]
  },
  ProductSLO: #objectSchema & {
    example: {
      freshness_slo: "6h"
      latency_slo:   "p95 < 3s"
    }
    #fields: {
      freshness_slo: #stringProperty,
      latency_slo: #stringProperty
    }
  },
  ProductScorecard: #objectSchema & {
    #fields: {
      product_id: #stringProperty,
      product_slug: #stringProperty,
      product_name: #stringProperty,
      domain_name: #stringProperty,
      team_name: #stringProperty,
      publication_state: #stringProperty,
      certification_state: #stringProperty,
      has_owner: #boolProperty,
      has_contract: #boolProperty,
      has_slo: #boolProperty,
      has_docs_or_access_path: #boolProperty,
      has_primary_output: #boolProperty,
      has_warnings: #boolProperty,
      completeness_percent: #int32Property
    },
    #required: [
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
  ProductScorecardList: #objectSchema & {
    #fields: {
      data: #arrayRefProperty & {#ref: "ProductScorecard"},
      next_page_token: #stringProperty
    },
    #required: [
      "data"
    ]
  },
  ProductSemanticEntrypoint: #objectSchema & {
    example: {
      id:                 "sementry_01hzycust360"
      product_version_id: "ver_01hzycust360_v3"
      semantic_model_id:  "sem_01hzymetrics"
      model_name:         "customer_360"
      created_at:         "2026-04-13T08:56:00Z"
    }
    #fields: {
      id: #idProperty,
      product_version_id: #stringProperty,
      semantic_model_id: #stringProperty,
      model_name: #stringProperty,
      created_at: #createdAtProperty
    },
    #required: [
      "id",
      "product_version_id",
      "semantic_model_id",
      "model_name"
    ]
  },
  ProductSemanticEntrypointList: #objectSchema & {
    #fields: {
      data: #arrayRefProperty & {#ref: "ProductSemanticEntrypoint"}
    },
    #required: [
      "data"
    ]
  },
  ProductSubscription: #objectSchema & {
    #fields: {
      id: #idProperty,
      product_id: #stringProperty,
      principal_name: #principalNameProperty,
      event_type: #stringProperty,
      channel: #stringProperty,
      created_at: #createdAtProperty
    },
    #required: [
      "id",
      "product_id",
      "principal_name",
      "event_type",
      "channel"
    ]
  },
  ProductSubscriptionList: #objectSchema & {
    #fields: {
      data: #arrayRefProperty & {#ref: "ProductSubscription"}
    },
    #required: [
      "data"
    ]
  },
  ProductTeam: #objectSchema & {
    example: {
      id:              "team_customer_platform"
      domain_id:       "dom_growth"
      name:            "customer-platform"
      contact_channel: "#customer-data"
      created_at:      "2026-03-01T09:00:00Z"
      updated_at:      "2026-04-01T09:00:00Z"
    }
    #fields: {
      id: #idProperty,
      domain_id: #stringProperty,
      name: #nameProperty,
      contact_channel: #stringProperty,
      created_at: #createdAtProperty,
      updated_at: #updatedAtProperty
    },
    #required: [
      "id",
      "domain_id",
      "name",
      "contact_channel"
    ]
  },
  UpdateDataProductRequest: #objectSchema & {
    example: {
      name:                "Customer 360"
      description:         "Unified customer profile product for analytics and activation."
      domain_name:         "growth"
      team_name:           "customer-platform"
      steward_principal:   "alice@example.com"
      contact_channel:     "#customer-data"
      visibility:          "internal"
      consumer_audience:   "sales, marketing, support"
      docs_url:            "https://docs.example.com/products/customer-360"
      access_request_path: "https://jira.example.com/servicedesk/customer-360"
      business_definitions: {
        customer: "A billing-account level customer record."
      }
      contract: {
        data_grain:   "one row per active customer per day"
        primary_keys: ["customer_id", "snapshot_date"]
        measures:     ["mrr", "active_subscriptions"]
      }
      slo: {
        freshness_slo: "6h"
        latency_slo:   "p95 < 3s"
      }
      publication_intent: "published"
    }
    #fields: {
      name: #nameProperty,
      description: #descriptionProperty,
      domain_name: #stringProperty,
      team_name: #stringProperty,
      steward_principal: #stringProperty,
      contact_channel: #stringProperty,
      visibility: #stringProperty,
      consumer_audience: #stringProperty,
      docs_url: #stringProperty,
      access_request_path: #stringProperty,
      business_definitions: #stringMapProperty,
      contract: #refProperty & {#ref: "ProductContract"},
      slo: #refProperty & {#ref: "ProductSLO"},
      publication_intent: #stringProperty
    }
  },
  UpdateProductDomainRequest: #objectSchema & {
    #fields: {
      description: #descriptionProperty
    }
  },
  UpdateProductTeamRequest: #objectSchema & {
    #fields: {
      contact_channel: #stringProperty
    }
  }
}
