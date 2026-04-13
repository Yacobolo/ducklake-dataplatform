package api

// Authored product schemas.

schemas_products: {
  CreateDataProductRequest: #objectSchema & {
    #fields: {
      access_request_path: #stringProperty,
      business_definitions: #refProperty & {#ref: "Record"},
      consumer_audience: #stringProperty,
      contact_channel: #stringProperty,
      contract: #refProperty & {#ref: "ProductContract"},
      created_by: #stringProperty,
      description: #descriptionProperty,
      docs_url: #stringProperty,
      domain_name: #stringProperty,
      name: #nameProperty,
      primary_asset_key: #stringProperty,
      producing_build_id: #stringProperty,
      semantic_model_refs: #stringArrayProperty,
      slo: #refProperty & {#ref: "ProductSLO"},
      slug: #stringProperty,
      steward_principal: #stringProperty,
      team_name: #stringProperty,
      visibility: #stringProperty
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
      access_request_path: #stringProperty,
      compatibility_level: #stringProperty,
      contract: #refProperty & {#ref: "ProductContract"},
      created_by: #stringProperty,
      docs_url: #stringProperty,
      output_asset_keys: #stringArrayProperty,
      producing_build_id: #stringProperty,
      semantic_model_refs: #stringArrayProperty,
      slo: #refProperty & {#ref: "ProductSLO"}
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
      description: #descriptionProperty,
      name: #nameProperty
    },
    #required: [
      "name"
    ]
  },
  CreateProductSubscriptionRequest: #objectSchema & {
    #fields: {
      channel: #stringProperty,
      event_type: #stringProperty,
      principal_name: #principalNameProperty
    },
    #required: [
      "principal_name",
      "event_type"
    ]
  },
  CreateProductTeamRequest: #objectSchema & {
    #fields: {
      contact_channel: #stringProperty,
      name: #nameProperty
    },
    #required: [
      "name"
    ]
  },
  DataProduct: #objectSchema & {
    #fields: {
      access_request_path: #stringProperty,
      business_definitions: #refProperty & {#ref: "Record"},
      consumer_audience: #stringProperty,
      contact_channel: #stringProperty,
      contract: #refProperty & {#ref: "ProductContract"},
      created_at: #createdAtProperty,
      created_by: #stringProperty,
      description: #descriptionProperty,
      docs_url: #stringProperty,
      domain_id: #stringProperty,
      id: #idProperty,
      name: #nameProperty,
      owner_team_id: #stringProperty,
      publication_intent: #stringProperty,
      slo: #refProperty & {#ref: "ProductSLO"},
      slug: #stringProperty,
      steward_principal: #stringProperty,
      updated_at: #updatedAtProperty,
      visibility: #stringProperty
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
      dependencies: #arrayRefProperty & {#ref: "DataProductListItem"},
      domain: #refProperty & {#ref: "ProductDomain"},
      events: #arrayRefProperty & {#ref: "ProductEvent"},
      outputs: #arrayRefProperty & {#ref: "ProductOutput"},
      owner_team: #refProperty & {#ref: "ProductTeam"},
      product: #refProperty & {#ref: "DataProduct"},
      semantic_entrypoints: #arrayRefProperty & {#ref: "ProductSemanticEntrypoint"},
      status: #refProperty & {#ref: "DataProductStatus"},
      subscriptions: #arrayRefProperty & {#ref: "ProductSubscription"},
      versions: #arrayRefProperty & {#ref: "DataProductVersion"}
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
      domain: #refProperty & {#ref: "ProductDomain"},
      latest_version: #refProperty & {#ref: "DataProductVersion"},
      owner_team: #refProperty & {#ref: "ProductTeam"},
      primary_output: #refProperty & {#ref: "ProductOutput"},
      product: #refProperty & {#ref: "DataProduct"},
      status: #refProperty & {#ref: "DataProductStatus"}
    },
    #required: [
      "product",
      "domain",
      "owner_team"
    ]
  },
  DataProductStatus: #objectSchema & {
    #fields: {
      adoption_metrics: #refProperty & {#ref: "Record"},
      certification_state: #stringProperty,
      failing_checks_count: #int64Property,
      freshness_status: #stringProperty,
      last_successful_update_at: #stringProperty,
      lineage_coverage: #numberProperty,
      open_warnings: #stringArrayProperty,
      product_id: #stringProperty,
      publication_state: #stringProperty,
      quality_status: #stringProperty,
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
      access_request_path: #stringProperty,
      compatibility_level: #stringProperty,
      contract: #refProperty & {#ref: "ProductContract"},
      created_at: #createdAtProperty,
      created_by: #stringProperty,
      docs_url: #stringProperty,
      id: #idProperty,
      producing_build_id: #stringProperty,
      product_id: #stringProperty,
      release_state: #stringProperty,
      slo: #refProperty & {#ref: "ProductSLO"},
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
      dependencies: #arrayRefProperty & {#ref: "DataProductListItem"},
      domain: #refProperty & {#ref: "ProductDomain"},
      events: #arrayRefProperty & {#ref: "ProductEvent"},
      outputs: #arrayRefProperty & {#ref: "ProductOutput"},
      owner_team: #refProperty & {#ref: "ProductTeam"},
      product: #refProperty & {#ref: "DataProduct"},
      semantic_entrypoints: #arrayRefProperty & {#ref: "ProductSemanticEntrypoint"},
      status: #refProperty & {#ref: "DataProductStatus"},
      version: #refProperty & {#ref: "DataProductVersion"}
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
      resource_id: #stringProperty,
      resource_name: #stringProperty,
      resource_type: #stringProperty
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
      breaking_change_policy: #stringProperty,
      data_grain: #stringProperty,
      dimensions: #stringArrayProperty,
      join_keys: #stringArrayProperty,
      measures: #stringArrayProperty,
      primary_keys: #stringArrayProperty,
      quality_expectations: #stringArrayProperty,
      retention_window: #stringProperty,
      sample_queries: #stringArrayProperty,
      update_cadence: #stringProperty
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
      created_at: #createdAtProperty,
      description: #descriptionProperty,
      id: #idProperty,
      name: #nameProperty,
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
      created_at: #createdAtProperty,
      description: #descriptionProperty,
      event_type: #stringProperty,
      id: #idProperty,
      metadata: #refProperty & {#ref: "Record"},
      product_id: #stringProperty,
      title: #stringProperty
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
      asset_id: #stringProperty,
      asset_key: #stringProperty,
      asset_type: #stringProperty,
      created_at: #createdAtProperty,
      id: #idProperty,
      is_primary: #boolProperty,
      product_version_id: #stringProperty
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
      average_completeness_pct: #int32Property,
      certified_count: #int64Property,
      name: #nameProperty,
      product_count: #int64Property,
      published_count: #int64Property
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
      domain_scorecards: #arrayRefProperty & {#ref: "ProductPortfolioGroup"},
      high_blast_radius: #arrayRefProperty & {#ref: "ProductAdoptionSummary"},
      least_adopted: #arrayRefProperty & {#ref: "ProductAdoptionSummary"},
      orphan_assets: #arrayRefProperty & {#ref: "OrphanResource"},
      orphan_semantic_models: #arrayRefProperty & {#ref: "OrphanResource"},
      team_scorecards: #arrayRefProperty & {#ref: "ProductPortfolioGroup"},
      top_used: #arrayRefProperty & {#ref: "ProductAdoptionSummary"}
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
      certification_state: #stringProperty,
      completeness_percent: #int32Property,
      domain_name: #stringProperty,
      has_contract: #boolProperty,
      has_docs_or_access_path: #boolProperty,
      has_owner: #boolProperty,
      has_primary_output: #boolProperty,
      has_slo: #boolProperty,
      has_warnings: #boolProperty,
      product_id: #stringProperty,
      product_name: #stringProperty,
      product_slug: #stringProperty,
      publication_state: #stringProperty,
      team_name: #stringProperty
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
      created_at: #createdAtProperty,
      id: #idProperty,
      model_name: #stringProperty,
      product_version_id: #stringProperty,
      semantic_model_id: #stringProperty
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
      channel: #stringProperty,
      created_at: #createdAtProperty,
      event_type: #stringProperty,
      id: #idProperty,
      principal_name: #principalNameProperty,
      product_id: #stringProperty
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
      contact_channel: #stringProperty,
      created_at: #createdAtProperty,
      domain_id: #stringProperty,
      id: #idProperty,
      name: #nameProperty,
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
      access_request_path: #stringProperty,
      business_definitions: #refProperty & {#ref: "Record"},
      consumer_audience: #stringProperty,
      contact_channel: #stringProperty,
      contract: #refProperty & {#ref: "ProductContract"},
      description: #descriptionProperty,
      docs_url: #stringProperty,
      domain_name: #stringProperty,
      name: #nameProperty,
      publication_intent: #stringProperty,
      slo: #refProperty & {#ref: "ProductSLO"},
      steward_principal: #stringProperty,
      team_name: #stringProperty,
      visibility: #stringProperty
    },
    #required: [
      "name",
      "domain_name",
      "team_name",
      "steward_principal",
      "contact_channel"
    ]
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
