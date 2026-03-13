package domain

import (
	"strings"
	"time"
)

// Product control-plane constants.
const (
	ProductPublicationIntentDraft     = "DRAFT"
	ProductPublicationIntentPublished = "PUBLISHED"

	ProductReleaseStateDraft      = "DRAFT"
	ProductReleaseStatePublished  = "PUBLISHED"
	ProductReleaseStateDeprecated = "DEPRECATED"
	ProductReleaseStateRetired    = "RETIRED"

	ProductCompatibilityBackwardCompatible = "BACKWARD_COMPATIBLE"
	ProductCompatibilityBreaking           = "BREAKING"
)

// Domain is a normalized data domain for product ownership.
type Domain struct {
	ID          string    `json:"id"`
	Name        string    `json:"name"`
	Description string    `json:"description"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
}

// CreateDomainRequest creates a normalized product domain.
type CreateDomainRequest struct {
	Name        string `json:"name"`
	Description string `json:"description"`
}

// Validate checks the create-domain request.
func (r CreateDomainRequest) Validate() error {
	if strings.TrimSpace(r.Name) == "" {
		return ErrValidation("domain name is required")
	}
	return nil
}

// UpdateDomainRequest updates editable domain metadata.
type UpdateDomainRequest struct {
	Description string `json:"description"`
}

// Team is a normalized owner team within a domain.
type Team struct {
	ID             string    `json:"id"`
	DomainID       string    `json:"domain_id"`
	Name           string    `json:"name"`
	ContactChannel string    `json:"contact_channel"`
	CreatedAt      time.Time `json:"created_at"`
	UpdatedAt      time.Time `json:"updated_at"`
}

// CreateTeamRequest creates a normalized owner team.
type CreateTeamRequest struct {
	DomainID       string `json:"domain_id"`
	Name           string `json:"name"`
	ContactChannel string `json:"contact_channel"`
}

// Validate checks the create-team request.
func (r CreateTeamRequest) Validate() error {
	if strings.TrimSpace(r.DomainID) == "" {
		return ErrValidation("domain_id is required")
	}
	if strings.TrimSpace(r.Name) == "" {
		return ErrValidation("team name is required")
	}
	return nil
}

// UpdateTeamRequest updates editable team metadata.
type UpdateTeamRequest struct {
	ContactChannel string `json:"contact_channel"`
}

// ProductContract captures the consumer-facing product contract.
type ProductContract struct {
	DataGrain            string   `json:"data_grain,omitempty"`
	PrimaryKeys          []string `json:"primary_keys,omitempty"`
	JoinKeys             []string `json:"join_keys,omitempty"`
	Dimensions           []string `json:"dimensions,omitempty"`
	Measures             []string `json:"measures,omitempty"`
	RetentionWindow      string   `json:"retention_window,omitempty"`
	UpdateCadence        string   `json:"update_cadence,omitempty"`
	QualityExpectations  []string `json:"quality_expectations,omitempty"`
	BreakingChangePolicy string   `json:"breaking_change_policy,omitempty"`
	SampleQueries        []string `json:"sample_queries,omitempty"`
}

// ProductSLO captures authored product SLO targets.
type ProductSLO struct {
	FreshnessSLO string `json:"freshness_slo,omitempty"`
	LatencySLO   string `json:"latency_slo,omitempty"`
}

// DataProductFilter scopes product discovery queries.
type DataProductFilter struct {
	Query              *string
	DomainName         *string
	TeamName           *string
	PublicationState   *string
	CertificationState *string
	FreshnessState     *string
	Page               PageRequest
}

// DataProduct is the authored top-level product specification.
type DataProduct struct {
	ID                  string            `json:"id"`
	Slug                string            `json:"slug"`
	Name                string            `json:"name"`
	Description         string            `json:"description"`
	DomainID            string            `json:"domain_id"`
	OwnerTeamID         string            `json:"owner_team_id"`
	StewardPrincipal    string            `json:"steward_principal"`
	ContactChannel      string            `json:"contact_channel"`
	Visibility          string            `json:"visibility"`
	ConsumerAudience    string            `json:"consumer_audience"`
	DocsURL             string            `json:"docs_url"`
	AccessRequestPath   string            `json:"access_request_path"`
	BusinessDefinitions map[string]string `json:"business_definitions"`
	Contract            ProductContract   `json:"contract"`
	SLO                 ProductSLO        `json:"slo"`
	PublicationIntent   string            `json:"publication_intent"`
	CreatedBy           string            `json:"created_by"`
	CreatedAt           time.Time         `json:"created_at"`
	UpdatedAt           time.Time         `json:"updated_at"`
}

// CreateDataProductRequest creates a new draft product.
type CreateDataProductRequest struct {
	Slug                string            `json:"slug"`
	Name                string            `json:"name"`
	Description         string            `json:"description"`
	DomainName          string            `json:"domain_name"`
	TeamName            string            `json:"team_name"`
	StewardPrincipal    string            `json:"steward_principal"`
	ContactChannel      string            `json:"contact_channel"`
	Visibility          string            `json:"visibility"`
	ConsumerAudience    string            `json:"consumer_audience"`
	DocsURL             string            `json:"docs_url"`
	AccessRequestPath   string            `json:"access_request_path"`
	BusinessDefinitions map[string]string `json:"business_definitions"`
	Contract            ProductContract   `json:"contract"`
	SLO                 ProductSLO        `json:"slo"`
	ProducingBuildID    *string           `json:"producing_build_id,omitempty"`
	PrimaryAssetKey     *string           `json:"primary_asset_key"`
	SemanticModelRefs   []string          `json:"semantic_model_refs"`
	CreatedBy           string            `json:"created_by"`
}

// Validate checks the create-product request.
func (r CreateDataProductRequest) Validate() error {
	if strings.TrimSpace(r.Slug) == "" {
		return ErrValidation("slug is required")
	}
	if strings.TrimSpace(r.Name) == "" {
		return ErrValidation("name is required")
	}
	if strings.TrimSpace(r.DomainName) == "" {
		return ErrValidation("domain_name is required")
	}
	if strings.TrimSpace(r.TeamName) == "" {
		return ErrValidation("team_name is required")
	}
	if strings.TrimSpace(r.StewardPrincipal) == "" {
		return ErrValidation("steward_principal is required")
	}
	if strings.TrimSpace(r.ContactChannel) == "" {
		return ErrValidation("contact_channel is required")
	}
	if r.ProducingBuildID != nil && strings.TrimSpace(*r.ProducingBuildID) == "" {
		return ErrValidation("producing_build_id cannot be empty")
	}
	return nil
}

// UpdateDataProductRequest updates authored top-level product metadata.
type UpdateDataProductRequest struct {
	Name                string            `json:"name"`
	Description         string            `json:"description"`
	DomainName          string            `json:"domain_name"`
	TeamName            string            `json:"team_name"`
	StewardPrincipal    string            `json:"steward_principal"`
	ContactChannel      string            `json:"contact_channel"`
	Visibility          string            `json:"visibility"`
	ConsumerAudience    string            `json:"consumer_audience"`
	DocsURL             string            `json:"docs_url"`
	AccessRequestPath   string            `json:"access_request_path"`
	BusinessDefinitions map[string]string `json:"business_definitions"`
	Contract            ProductContract   `json:"contract"`
	SLO                 ProductSLO        `json:"slo"`
	PublicationIntent   string            `json:"publication_intent"`
}

// Validate checks the update-product request.
func (r UpdateDataProductRequest) Validate() error {
	if strings.TrimSpace(r.Name) == "" {
		return ErrValidation("name is required")
	}
	if strings.TrimSpace(r.DomainName) == "" {
		return ErrValidation("domain_name is required")
	}
	if strings.TrimSpace(r.TeamName) == "" {
		return ErrValidation("team_name is required")
	}
	if strings.TrimSpace(r.StewardPrincipal) == "" {
		return ErrValidation("steward_principal is required")
	}
	if strings.TrimSpace(r.ContactChannel) == "" {
		return ErrValidation("contact_channel is required")
	}
	return nil
}

// DataProductVersion is an immutable product release snapshot.
type DataProductVersion struct {
	ID                 string          `json:"id"`
	ProductID          string          `json:"product_id"`
	ProducingBuildID   *string         `json:"producing_build_id,omitempty"`
	Version            int             `json:"version"`
	ReleaseState       string          `json:"release_state"`
	CompatibilityLevel string          `json:"compatibility_level"`
	Contract           ProductContract `json:"contract"`
	SLO                ProductSLO      `json:"slo"`
	DocsURL            string          `json:"docs_url"`
	AccessRequestPath  string          `json:"access_request_path"`
	CreatedBy          string          `json:"created_by"`
	CreatedAt          time.Time       `json:"created_at"`
}

// CreateDataProductVersionRequest creates a new draft version.
type CreateDataProductVersionRequest struct {
	CompatibilityLevel string          `json:"compatibility_level"`
	Contract           ProductContract `json:"contract"`
	SLO                ProductSLO      `json:"slo"`
	DocsURL            string          `json:"docs_url"`
	AccessRequestPath  string          `json:"access_request_path"`
	ProducingBuildID   *string         `json:"producing_build_id,omitempty"`
	OutputAssetKeys    []string        `json:"output_asset_keys"`
	SemanticModelRefs  []string        `json:"semantic_model_refs"`
	CreatedBy          string          `json:"created_by"`
}

// Validate checks the create-version request.
func (r CreateDataProductVersionRequest) Validate() error {
	switch strings.TrimSpace(r.CompatibilityLevel) {
	case "", ProductCompatibilityBackwardCompatible, ProductCompatibilityBreaking:
	default:
		return ErrValidation("compatibility_level must be BACKWARD_COMPATIBLE or BREAKING")
	}
	if r.ProducingBuildID != nil && strings.TrimSpace(*r.ProducingBuildID) == "" {
		return ErrValidation("producing_build_id cannot be empty")
	}
	return nil
}

// DataProductStatus is the computed runtime status for a product.
type DataProductStatus struct {
	ProductID              string         `json:"product_id"`
	PublicationState       string         `json:"publication_state"`
	CertificationState     string         `json:"certification_state"`
	FreshnessStatus        string         `json:"freshness_status"`
	QualityStatus          string         `json:"quality_status"`
	LastSuccessfulUpdateAt *time.Time     `json:"last_successful_update_at"`
	FailingChecksCount     int64          `json:"failing_checks_count"`
	LineageCoverage        *float64       `json:"lineage_coverage"`
	AdoptionMetrics        map[string]any `json:"adoption_metrics"`
	OpenWarnings           []string       `json:"open_warnings"`
	ReplacementProductID   *string        `json:"replacement_product_id"`
	UpdatedAt              time.Time      `json:"updated_at"`
}

// ProductOutput links a product version to a runtime asset output.
type ProductOutput struct {
	ID               string    `json:"id"`
	ProductVersionID string    `json:"product_version_id"`
	AssetID          string    `json:"asset_id"`
	AssetKey         string    `json:"asset_key"`
	AssetType        string    `json:"asset_type"`
	IsPrimary        bool      `json:"is_primary"`
	CreatedAt        time.Time `json:"created_at"`
}

// ProductSemanticEntrypoint links a product version to a semantic model.
type ProductSemanticEntrypoint struct {
	ID               string    `json:"id"`
	ProductVersionID string    `json:"product_version_id"`
	SemanticModelID  string    `json:"semantic_model_id"`
	ProjectName      string    `json:"project_name"`
	ModelName        string    `json:"model_name"`
	CreatedAt        time.Time `json:"created_at"`
}

// ProductDependency records a product-to-product dependency.
type ProductDependency struct {
	ID                 string    `json:"id"`
	ProductID          string    `json:"product_id"`
	DependsOnProductID string    `json:"depends_on_product_id"`
	CreatedAt          time.Time `json:"created_at"`
}

// ProductSubscription records a consumer subscription to product events.
type ProductSubscription struct {
	ID            string    `json:"id"`
	ProductID     string    `json:"product_id"`
	PrincipalName string    `json:"principal_name"`
	EventType     string    `json:"event_type"`
	Channel       string    `json:"channel"`
	CreatedAt     time.Time `json:"created_at"`
}

// ProductEvent records a durable product change or health event.
type ProductEvent struct {
	ID          string         `json:"id"`
	ProductID   string         `json:"product_id"`
	EventType   string         `json:"event_type"`
	Title       string         `json:"title"`
	Description string         `json:"description"`
	Metadata    map[string]any `json:"metadata"`
	CreatedAt   time.Time      `json:"created_at"`
}

// ProductScorecard summarizes governance completeness for a product.
type ProductScorecard struct {
	ProductID           string `json:"product_id"`
	ProductSlug         string `json:"product_slug"`
	ProductName         string `json:"product_name"`
	DomainName          string `json:"domain_name"`
	TeamName            string `json:"team_name"`
	PublicationState    string `json:"publication_state"`
	CertificationState  string `json:"certification_state"`
	HasOwner            bool   `json:"has_owner"`
	HasContract         bool   `json:"has_contract"`
	HasSLO              bool   `json:"has_slo"`
	HasDocsOrAccessPath bool   `json:"has_docs_or_access_path"`
	HasPrimaryOutput    bool   `json:"has_primary_output"`
	HasWarnings         bool   `json:"has_warnings"`
	CompletenessPercent int    `json:"completeness_percent"`
}

// ProductAdoptionSummary captures ranked usage and blast-radius signals for a product.
type ProductAdoptionSummary struct {
	ProductID               string `json:"product_id"`
	ProductSlug             string `json:"product_slug"`
	ProductName             string `json:"product_name"`
	DomainName              string `json:"domain_name"`
	TeamName                string `json:"team_name"`
	SubscriberCount         int64  `json:"subscriber_count"`
	DownstreamProductCount  int64  `json:"downstream_product_count"`
	OutputCount             int64  `json:"output_count"`
	SemanticEntrypointCount int64  `json:"semantic_entrypoint_count"`
	AdoptionScore           int64  `json:"adoption_score"`
}

// ProductPortfolioGroup summarizes completeness and lifecycle by team or domain.
type ProductPortfolioGroup struct {
	Name                   string `json:"name"`
	ProductCount           int64  `json:"product_count"`
	PublishedCount         int64  `json:"published_count"`
	CertifiedCount         int64  `json:"certified_count"`
	AverageCompletenessPct int    `json:"average_completeness_pct"`
}

// OrphanResource identifies a runtime or semantic resource not attached to a product.
type OrphanResource struct {
	ResourceType string `json:"resource_type"`
	ResourceID   string `json:"resource_id"`
	ResourceName string `json:"resource_name"`
}

// ProductPortfolioReport is the aggregated reporting view for the product control plane.
type ProductPortfolioReport struct {
	TopUsed              []ProductAdoptionSummary `json:"top_used"`
	LeastAdopted         []ProductAdoptionSummary `json:"least_adopted"`
	HighBlastRadius      []ProductAdoptionSummary `json:"high_blast_radius"`
	DomainScorecards     []ProductPortfolioGroup  `json:"domain_scorecards"`
	TeamScorecards       []ProductPortfolioGroup  `json:"team_scorecards"`
	OrphanAssets         []OrphanResource         `json:"orphan_assets"`
	OrphanSemanticModels []OrphanResource         `json:"orphan_semantic_models"`
}

// DataProductListItem is a ranked discovery result for a product.
type DataProductListItem struct {
	Product       DataProduct         `json:"product"`
	Domain        Domain              `json:"domain"`
	OwnerTeam     Team                `json:"owner_team"`
	LatestVersion *DataProductVersion `json:"latest_version,omitempty"`
	Status        *DataProductStatus  `json:"status,omitempty"`
	PrimaryOutput *ProductOutput      `json:"primary_output,omitempty"`
}

// DataProductDetail is the expanded product detail view.
type DataProductDetail struct {
	Product             DataProduct                 `json:"product"`
	Domain              Domain                      `json:"domain"`
	OwnerTeam           Team                        `json:"owner_team"`
	Versions            []DataProductVersion        `json:"versions"`
	Status              *DataProductStatus          `json:"status,omitempty"`
	Outputs             []ProductOutput             `json:"outputs"`
	SemanticEntrypoints []ProductSemanticEntrypoint `json:"semantic_entrypoints"`
	Dependencies        []DataProductListItem       `json:"dependencies"`
	Subscriptions       []ProductSubscription       `json:"subscriptions"`
	Events              []ProductEvent              `json:"events"`
}

// DataProductVersionDetail is the expanded view for one immutable product version.
type DataProductVersionDetail struct {
	Product             DataProduct                 `json:"product"`
	Domain              Domain                      `json:"domain"`
	OwnerTeam           Team                        `json:"owner_team"`
	Version             DataProductVersion          `json:"version"`
	Status              *DataProductStatus          `json:"status,omitempty"`
	Outputs             []ProductOutput             `json:"outputs"`
	SemanticEntrypoints []ProductSemanticEntrypoint `json:"semantic_entrypoints"`
	Dependencies        []DataProductListItem       `json:"dependencies"`
	Events              []ProductEvent              `json:"events"`
}
