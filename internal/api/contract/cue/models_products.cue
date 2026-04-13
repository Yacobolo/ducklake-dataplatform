package api

// Authored product schemas.

schemas_products: {
  CreateDataProductRequest: #objectSchema & {
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
    #fields: {
      name: #nameProperty,
      contact_channel: #stringProperty
    },
    #required: [
      "name"
    ]
  },
  DataProduct: #objectSchema & {
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
