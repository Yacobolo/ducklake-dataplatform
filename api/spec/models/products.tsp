namespace Duck;

model ProductDomain {
  id: string;
  name: string;
  description: string;
  created_at?: utcDateTime;
  updated_at?: utcDateTime;
}

model CreateProductDomainRequest {
  name: string;
  description?: string;
}

model UpdateProductDomainRequest {
  description?: string;
}

model PaginatedProductDomains {
  data: ProductDomain[];
  next_page_token?: string;
}

model ProductTeam {
  id: string;
  domain_id: string;
  name: string;
  contact_channel: string;
  created_at?: utcDateTime;
  updated_at?: utcDateTime;
}

model CreateProductTeamRequest {
  name: string;
  contact_channel?: string;
}

model UpdateProductTeamRequest {
  contact_channel?: string;
}

model PaginatedProductTeams {
  data: ProductTeam[];
  next_page_token?: string;
}

model ProductContract {
  data_grain?: string;
  primary_keys?: string[];
  join_keys?: string[];
  dimensions?: string[];
  measures?: string[];
  retention_window?: string;
  update_cadence?: string;
  quality_expectations?: string[];
  breaking_change_policy?: string;
  sample_queries?: string[];
}

model ProductSLO {
  freshness_slo?: string;
  latency_slo?: string;
}

model DataProduct {
  id: string;
  slug: string;
  name: string;
  description: string;
  domain_id: string;
  owner_team_id: string;
  steward_principal: string;
  contact_channel: string;
  visibility?: string;
  consumer_audience?: string;
  docs_url?: string;
  access_request_path?: string;
  business_definitions?: MetadataMap;
  contract?: ProductContract;
  slo?: ProductSLO;
  publication_intent?: string;
  created_by?: string;
  created_at?: utcDateTime;
  updated_at?: utcDateTime;
}

model CreateDataProductRequest {
  slug: string;
  name: string;
  description?: string;
  domain_name: string;
  team_name: string;
  steward_principal: string;
  contact_channel: string;
  visibility?: string;
  consumer_audience?: string;
  docs_url?: string;
  access_request_path?: string;
  business_definitions?: MetadataMap;
  contract?: ProductContract;
  slo?: ProductSLO;
  producing_build_id?: string;
  primary_asset_key?: string;
  semantic_model_refs?: string[];
  created_by?: string;
}

model UpdateDataProductRequest {
  name: string;
  description?: string;
  domain_name: string;
  team_name: string;
  steward_principal: string;
  contact_channel: string;
  visibility?: string;
  consumer_audience?: string;
  docs_url?: string;
  access_request_path?: string;
  business_definitions?: MetadataMap;
  contract?: ProductContract;
  slo?: ProductSLO;
  publication_intent?: string;
}

model DataProductVersion {
  id: string;
  product_id: string;
  producing_build_id?: string;
  version: int32;
  release_state: string;
  compatibility_level: string;
  contract?: ProductContract;
  slo?: ProductSLO;
  docs_url?: string;
  access_request_path?: string;
  created_by?: string;
  created_at?: utcDateTime;
}

model CreateDataProductVersionRequest {
  compatibility_level?: string;
  contract?: ProductContract;
  slo?: ProductSLO;
  docs_url?: string;
  access_request_path?: string;
  producing_build_id?: string;
  output_asset_keys?: string[];
  semantic_model_refs?: string[];
  created_by?: string;
}

model DataProductStatus {
  product_id: string;
  publication_state: string;
  certification_state: string;
  freshness_status: string;
  quality_status: string;
  last_successful_update_at?: utcDateTime;
  failing_checks_count: int64;
  lineage_coverage?: float64;
  adoption_metrics?: JsonObject;
  open_warnings?: string[];
  replacement_product_id?: string;
  updated_at?: utcDateTime;
}

model ProductOutput {
  id: string;
  product_version_id: string;
  asset_id: string;
  asset_key: string;
  asset_type: string;
  is_primary: boolean;
  created_at?: utcDateTime;
}

model ProductSemanticEntrypoint {
  id: string;
  product_version_id: string;
  semantic_model_id: string;
  model_name: string;
  created_at?: utcDateTime;
}

model ProductSubscription {
  id: string;
  product_id: string;
  principal_name: string;
  event_type: string;
  channel: string;
  created_at?: utcDateTime;
}

model ProductEvent {
  id: string;
  product_id: string;
  event_type: string;
  title: string;
  description: string;
  metadata?: JsonObject;
  created_at?: utcDateTime;
}

model ProductScorecard {
  product_id: string;
  product_slug: string;
  product_name: string;
  domain_name: string;
  team_name: string;
  publication_state: string;
  certification_state: string;
  has_owner: boolean;
  has_contract: boolean;
  has_slo: boolean;
  has_docs_or_access_path: boolean;
  has_primary_output: boolean;
  has_warnings: boolean;
  completeness_percent: int32;
}

model ProductAdoptionSummary {
  product_id: string;
  product_slug: string;
  product_name: string;
  domain_name: string;
  team_name: string;
  subscriber_count: int64;
  downstream_product_count: int64;
  output_count: int64;
  semantic_entrypoint_count: int64;
  adoption_score: int64;
}

model ProductPortfolioGroup {
  name: string;
  product_count: int64;
  published_count: int64;
  certified_count: int64;
  average_completeness_pct: int32;
}

model OrphanResource {
  resource_type: string;
  resource_id: string;
  resource_name: string;
}

model ProductPortfolioReport {
  top_used: ProductAdoptionSummary[];
  least_adopted: ProductAdoptionSummary[];
  high_blast_radius: ProductAdoptionSummary[];
  domain_scorecards: ProductPortfolioGroup[];
  team_scorecards: ProductPortfolioGroup[];
  orphan_assets: OrphanResource[];
  orphan_semantic_models: OrphanResource[];
}

model DataProductListItem {
  product: DataProduct;
  domain: ProductDomain;
  owner_team: ProductTeam;
  latest_version?: DataProductVersion;
  status?: DataProductStatus;
  primary_output?: ProductOutput;
}

model DataProductDetail {
  product: DataProduct;
  domain: ProductDomain;
  owner_team: ProductTeam;
  versions: DataProductVersion[];
  status?: DataProductStatus;
  outputs: ProductOutput[];
  semantic_entrypoints: ProductSemanticEntrypoint[];
  dependencies: DataProductListItem[];
  subscriptions: ProductSubscription[];
  events: ProductEvent[];
}

model DataProductVersionDetail {
  product: DataProduct;
  domain: ProductDomain;
  owner_team: ProductTeam;
  version: DataProductVersion;
  status?: DataProductStatus;
  outputs: ProductOutput[];
  semantic_entrypoints: ProductSemanticEntrypoint[];
  dependencies: DataProductListItem[];
  events: ProductEvent[];
}

model PaginatedDataProducts {
  data: DataProductListItem[];
  next_page_token?: string;
}

model DataProductVersionList {
  data: DataProductVersion[];
}

model ProductOutputList {
  data: ProductOutput[];
}

model ProductSemanticEntrypointList {
  data: ProductSemanticEntrypoint[];
}

model ProductDependencyList {
  data: DataProductListItem[];
}

model ProductSubscriptionList {
  data: ProductSubscription[];
}

model ProductEventList {
  data: ProductEvent[];
  next_page_token?: string;
}

model ProductScorecardList {
  data: ProductScorecard[];
  next_page_token?: string;
}

model CreateProductDependencyRequest {
  depends_on_slug: string;
}

model CreateProductSubscriptionRequest {
  principal_name: string;
  event_type: string;
  channel?: string;
}

model DeprecateProductVersionRequest {
  replacement_slug?: string;
}
