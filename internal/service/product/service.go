// Package product manages product-first control-plane operations.
package product

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"duck-demo/internal/domain"
)

// Managed runtime product kinds group system-synced runtime assets under draft products.
const (
	ManagedRuntimeProductPipelines       = "pipelines"
	ManagedRuntimeProductModels          = "models"
	ManagedRuntimeProductNotebooks       = "notebooks"
	ManagedRuntimeProductNotebookOutputs = "notebook_outputs"
	ManagedRuntimeProductSemantic        = "semantic"
	ManagedRuntimeProductDashboards      = "dashboards"
)

// Service orchestrates the product control plane.
type Service struct {
	domains  domain.DomainRepository
	teams    domain.TeamRepository
	assets   domain.DataAssetRepository
	runs     domain.AssetRunRepository
	checks   domain.AssetCheckRepository
	semantic domain.SemanticModelRepository
	projects domain.ProjectRepository
	builds   domain.BuildRepository
	repo     domain.DataProductRepository
	audit    domain.AuditRepository
}

// NewService constructs a product control-plane service.
func NewService(
	domains domain.DomainRepository,
	teams domain.TeamRepository,
	assets domain.DataAssetRepository,
	runs domain.AssetRunRepository,
	checks domain.AssetCheckRepository,
	repo domain.DataProductRepository,
	audit ...domain.AuditRepository,
) *Service {
	var auditRepo domain.AuditRepository
	if len(audit) > 0 {
		auditRepo = audit[0]
	}
	return &Service{
		domains: domains,
		teams:   teams,
		assets:  assets,
		runs:    runs,
		checks:  checks,
		repo:    repo,
		audit:   auditRepo,
	}
}

// SetSemanticModelRepository configures semantic model lookups for product entrypoints.
func (s *Service) SetSemanticModelRepository(repo domain.SemanticModelRepository) {
	if s == nil {
		return
	}
	s.semantic = repo
}

// SetBuildRepository configures internal build provenance lookups for product versions.
func (s *Service) SetBuildRepository(repo domain.BuildRepository) {
	if s == nil {
		return
	}
	s.builds = repo
}

// SetProjectRepository configures internal project lookups for build publication policy.
func (s *Service) SetProjectRepository(repo domain.ProjectRepository) {
	if s == nil {
		return
	}
	s.projects = repo
}

// ListDomains lists normalized product domains.
func (s *Service) ListDomains(ctx context.Context, page domain.PageRequest) ([]domain.Domain, int64, error) {
	if s == nil || s.domains == nil {
		return []domain.Domain{}, 0, nil
	}
	return s.domains.List(ctx, page)
}

// GetDomain returns a normalized product domain by name.
func (s *Service) GetDomain(ctx context.Context, name string) (*domain.Domain, error) {
	if strings.TrimSpace(name) == "" {
		return nil, domain.ErrValidation("domain name is required")
	}
	if s == nil || s.domains == nil {
		return nil, domain.ErrNotImplemented("domain lookup is not configured")
	}
	return s.domains.GetByName(ctx, strings.TrimSpace(name))
}

// CreateDomain creates a normalized product domain.
func (s *Service) CreateDomain(ctx context.Context, req domain.CreateDomainRequest) (*domain.Domain, error) {
	if err := req.Validate(); err != nil {
		return nil, err
	}
	if s == nil || s.domains == nil {
		return nil, domain.ErrNotImplemented("domain creation is not configured")
	}
	item, err := s.domains.Create(ctx, &domain.Domain{
		Name:        strings.TrimSpace(req.Name),
		Description: strings.TrimSpace(req.Description),
	})
	if err != nil {
		return nil, err
	}
	s.logAudit(ctx, "system", "CREATE_DOMAIN")
	return item, nil
}

// UpdateDomain updates normalized product-domain metadata.
func (s *Service) UpdateDomain(ctx context.Context, name string, req domain.UpdateDomainRequest) (*domain.Domain, error) {
	if strings.TrimSpace(name) == "" {
		return nil, domain.ErrValidation("domain name is required")
	}
	if s == nil || s.domains == nil {
		return nil, domain.ErrNotImplemented("domain update is not configured")
	}
	item, err := s.domains.Update(ctx, name, &domain.Domain{Description: strings.TrimSpace(req.Description)})
	if err != nil {
		return nil, err
	}
	s.logAudit(ctx, "system", "UPDATE_DOMAIN")
	return item, nil
}

// DeleteDomain removes a product domain.
func (s *Service) DeleteDomain(ctx context.Context, name string) error {
	if strings.TrimSpace(name) == "" {
		return domain.ErrValidation("domain name is required")
	}
	if s == nil || s.domains == nil {
		return domain.ErrNotImplemented("domain delete is not configured")
	}
	if err := s.domains.Delete(ctx, name); err != nil {
		return err
	}
	s.logAudit(ctx, "system", "DELETE_DOMAIN")
	return nil
}

// ListTeams lists normalized owner teams.
func (s *Service) ListTeams(ctx context.Context, page domain.PageRequest) ([]domain.Team, int64, error) {
	if s == nil || s.teams == nil {
		return []domain.Team{}, 0, nil
	}
	return s.teams.List(ctx, page)
}

// CreateTeam creates a normalized owner team.
func (s *Service) CreateTeam(ctx context.Context, req domain.CreateTeamRequest) (*domain.Team, error) {
	if err := req.Validate(); err != nil {
		return nil, err
	}
	if s == nil || s.teams == nil {
		return nil, domain.ErrNotImplemented("team creation is not configured")
	}
	item, err := s.teams.Create(ctx, &domain.Team{
		DomainID:       strings.TrimSpace(req.DomainID),
		Name:           strings.TrimSpace(req.Name),
		ContactChannel: strings.TrimSpace(req.ContactChannel),
	})
	if err != nil {
		return nil, err
	}
	s.logAudit(ctx, "system", "CREATE_TEAM")
	return item, nil
}

// UpdateTeam updates an owner team identified by domain and name.
func (s *Service) UpdateTeam(ctx context.Context, domainName, teamName string, req domain.UpdateTeamRequest) (*domain.Team, error) {
	if strings.TrimSpace(domainName) == "" {
		return nil, domain.ErrValidation("domain_name is required")
	}
	if strings.TrimSpace(teamName) == "" {
		return nil, domain.ErrValidation("team name is required")
	}
	if s == nil || s.teams == nil || s.domains == nil {
		return nil, domain.ErrNotImplemented("team update is not configured")
	}
	domainItem, err := s.domains.GetByName(ctx, strings.TrimSpace(domainName))
	if err != nil {
		return nil, err
	}
	item, err := s.teams.Update(ctx, domainItem.ID, teamName, &domain.Team{ContactChannel: strings.TrimSpace(req.ContactChannel)})
	if err != nil {
		return nil, err
	}
	s.logAudit(ctx, "system", "UPDATE_TEAM")
	return item, nil
}

// DeleteTeam removes an owner team identified by domain and name.
func (s *Service) DeleteTeam(ctx context.Context, domainName, teamName string) error {
	if strings.TrimSpace(domainName) == "" {
		return domain.ErrValidation("domain_name is required")
	}
	if strings.TrimSpace(teamName) == "" {
		return domain.ErrValidation("team name is required")
	}
	if s == nil || s.teams == nil || s.domains == nil {
		return domain.ErrNotImplemented("team delete is not configured")
	}
	domainItem, err := s.domains.GetByName(ctx, strings.TrimSpace(domainName))
	if err != nil {
		return err
	}
	if err := s.teams.Delete(ctx, domainItem.ID, teamName); err != nil {
		return err
	}
	s.logAudit(ctx, "system", "DELETE_TEAM")
	return nil
}

// ListProducts returns ranked product discovery results.
func (s *Service) ListProducts(ctx context.Context, filter domain.DataProductFilter) ([]domain.DataProductListItem, int64, error) {
	if s == nil || s.repo == nil {
		return []domain.DataProductListItem{}, 0, nil
	}
	items, total, err := s.repo.List(ctx, filter)
	if err != nil {
		return nil, 0, err
	}
	for i := range items {
		status, statusErr := s.computeStatus(ctx, items[i].Product.ID, items[i].Product.PublicationIntent, outputSlice(items[i].PrimaryOutput))
		if statusErr == nil && status != nil {
			items[i].Status = status
		}
	}
	slices.SortStableFunc(items, compareProductListItems)
	return items, total, nil
}

// GetProduct returns a detailed product view by slug.
func (s *Service) GetProduct(ctx context.Context, slug string) (*domain.DataProductDetail, error) {
	if s == nil || s.repo == nil {
		return nil, domain.ErrNotFound("product %q not found", slug)
	}
	item, err := s.repo.GetBySlug(ctx, slug)
	if err != nil {
		return nil, err
	}
	status, statusErr := s.computeStatus(ctx, item.Product.ID, item.Product.PublicationIntent, item.Outputs)
	if statusErr == nil && status != nil {
		if status.AdoptionMetrics == nil {
			status.AdoptionMetrics = map[string]any{}
		}
		status.AdoptionMetrics["semantic_entrypoint_count"] = len(item.SemanticEntrypoints)
		item.Status = status
	}
	return item, nil
}

// GetVersion returns the expanded view for a single product version.
func (s *Service) GetVersion(ctx context.Context, slug string, version int) (*domain.DataProductVersionDetail, error) {
	product, selectedVersion, err := s.loadVersion(ctx, slug, version)
	if err != nil {
		return nil, err
	}
	outputs, err := s.repo.ListOutputs(ctx, selectedVersion.ID)
	if err != nil {
		return nil, err
	}
	entrypoints, err := s.repo.ListSemanticEntrypoints(ctx, selectedVersion.ID)
	if err != nil {
		return nil, err
	}
	return &domain.DataProductVersionDetail{
		Product:             product.Product,
		Domain:              product.Domain,
		OwnerTeam:           product.OwnerTeam,
		Version:             *selectedVersion,
		Status:              product.Status,
		Outputs:             outputs,
		SemanticEntrypoints: entrypoints,
		Dependencies:        product.Dependencies,
		Events:              product.Events,
	}, nil
}

// GetProductForAsset resolves the owning product for a runtime asset.
func (s *Service) GetProductForAsset(ctx context.Context, assetID string) (*domain.DataProductListItem, error) {
	if s == nil || s.repo == nil {
		return nil, domain.ErrNotFound("product for asset %q not found", assetID)
	}
	return s.repo.GetByAssetID(ctx, assetID)
}

// CreateProduct creates a draft product and its initial draft version.
func (s *Service) CreateProduct(ctx context.Context, req domain.CreateDataProductRequest) (*domain.DataProductDetail, error) {
	if err := req.Validate(); err != nil {
		return nil, err
	}
	if s.repo == nil || s.domains == nil || s.teams == nil {
		return nil, domain.ErrNotImplemented("product creation is not configured")
	}

	d, err := s.ensureDomain(ctx, req.DomainName)
	if err != nil {
		return nil, err
	}
	t, err := s.ensureTeam(ctx, d.ID, req.TeamName, req.ContactChannel)
	if err != nil {
		return nil, err
	}

	product, err := s.repo.Create(ctx, &domain.DataProduct{
		Slug:                strings.TrimSpace(req.Slug),
		Name:                strings.TrimSpace(req.Name),
		Description:         strings.TrimSpace(req.Description),
		DomainID:            d.ID,
		OwnerTeamID:         t.ID,
		StewardPrincipal:    strings.TrimSpace(req.StewardPrincipal),
		ContactChannel:      strings.TrimSpace(req.ContactChannel),
		Visibility:          strings.TrimSpace(req.Visibility),
		ConsumerAudience:    strings.TrimSpace(req.ConsumerAudience),
		DocsURL:             strings.TrimSpace(req.DocsURL),
		AccessRequestPath:   strings.TrimSpace(req.AccessRequestPath),
		BusinessDefinitions: cloneStringMap(req.BusinessDefinitions),
		Contract:            req.Contract,
		SLO:                 req.SLO,
		PublicationIntent:   domain.ProductPublicationIntentDraft,
		CreatedBy:           strings.TrimSpace(req.CreatedBy),
	})
	if err != nil {
		return nil, err
	}

	versionReq := domain.CreateDataProductVersionRequest{
		CompatibilityLevel: domain.ProductCompatibilityBackwardCompatible,
		Contract:           req.Contract,
		SLO:                req.SLO,
		DocsURL:            product.DocsURL,
		AccessRequestPath:  product.AccessRequestPath,
		ProducingBuildID:   req.ProducingBuildID,
		CreatedBy:          product.CreatedBy,
	}
	if req.PrimaryAssetKey != nil && strings.TrimSpace(*req.PrimaryAssetKey) != "" {
		versionReq.OutputAssetKeys = []string{strings.TrimSpace(*req.PrimaryAssetKey)}
	}
	versionReq.SemanticModelRefs = slices.Clone(req.SemanticModelRefs)
	if _, err := s.createVersionForProduct(ctx, product, versionReq); err != nil {
		return nil, err
	}

	if err := s.repo.UpsertStatus(ctx, &domain.DataProductStatus{
		ProductID:          product.ID,
		PublicationState:   domain.ProductReleaseStateDraft,
		CertificationState: domain.CertificationDraft,
		FreshnessStatus:    "UNKNOWN",
		QualityStatus:      "UNKNOWN",
		AdoptionMetrics:    map[string]any{},
		OpenWarnings:       []string{"Draft product has not been published"},
	}); err != nil {
		return nil, err
	}

	detail, err := s.repo.GetBySlug(ctx, product.Slug)
	if err != nil {
		return nil, err
	}
	s.logAudit(ctx, product.CreatedBy, "CREATE_DATA_PRODUCT")
	s.logEvent(ctx, product.ID, "ownership_change", "Draft product created", fmt.Sprintf("Product %s was created in draft state.", product.Name), map[string]any{
		"slug": product.Slug,
	})
	return detail, nil
}

// UpdateProduct updates the authored top-level product spec.
func (s *Service) UpdateProduct(ctx context.Context, slug string, req domain.UpdateDataProductRequest) (*domain.DataProductDetail, error) {
	if strings.TrimSpace(slug) == "" {
		return nil, domain.ErrValidation("slug is required")
	}
	if err := req.Validate(); err != nil {
		return nil, err
	}
	if s.repo == nil || s.domains == nil || s.teams == nil {
		return nil, domain.ErrNotImplemented("product update is not configured")
	}
	current, err := s.GetProduct(ctx, slug)
	if err != nil {
		return nil, err
	}
	d, err := s.ensureDomain(ctx, req.DomainName)
	if err != nil {
		return nil, err
	}
	t, err := s.ensureTeam(ctx, d.ID, req.TeamName, req.ContactChannel)
	if err != nil {
		return nil, err
	}
	_, err = s.repo.Update(ctx, &domain.DataProduct{
		ID:                  current.Product.ID,
		Slug:                current.Product.Slug,
		Name:                strings.TrimSpace(req.Name),
		Description:         strings.TrimSpace(req.Description),
		DomainID:            d.ID,
		OwnerTeamID:         t.ID,
		StewardPrincipal:    strings.TrimSpace(req.StewardPrincipal),
		ContactChannel:      strings.TrimSpace(req.ContactChannel),
		Visibility:          strings.TrimSpace(req.Visibility),
		ConsumerAudience:    strings.TrimSpace(req.ConsumerAudience),
		DocsURL:             strings.TrimSpace(req.DocsURL),
		AccessRequestPath:   strings.TrimSpace(req.AccessRequestPath),
		BusinessDefinitions: cloneStringMap(req.BusinessDefinitions),
		Contract:            req.Contract,
		SLO:                 req.SLO,
		PublicationIntent:   defaultString(req.PublicationIntent, current.Product.PublicationIntent),
		CreatedBy:           current.Product.CreatedBy,
	})
	if err != nil {
		return nil, err
	}
	s.logAudit(ctx, "system", "UPDATE_DATA_PRODUCT")
	return s.repo.GetBySlug(ctx, current.Product.Slug)
}

// DeleteProduct removes a product and its control-plane state.
func (s *Service) DeleteProduct(ctx context.Context, slug string) error {
	if strings.TrimSpace(slug) == "" {
		return domain.ErrValidation("slug is required")
	}
	if s.repo == nil {
		return domain.ErrNotImplemented("product delete is not configured")
	}
	current, err := s.GetProduct(ctx, slug)
	if err != nil {
		return err
	}
	if err := s.repo.Delete(ctx, current.Product.ID); err != nil {
		return err
	}
	s.logAudit(ctx, "system", "DELETE_DATA_PRODUCT")
	return nil
}

// CreateVersion creates a new draft version for an existing product.
func (s *Service) CreateVersion(ctx context.Context, slug string, req domain.CreateDataProductVersionRequest) (*domain.DataProductDetail, error) {
	if err := req.Validate(); err != nil {
		return nil, err
	}
	product, err := s.GetProduct(ctx, slug)
	if err != nil {
		return nil, err
	}
	if _, err := s.createVersionForProduct(ctx, &product.Product, req); err != nil {
		return nil, err
	}
	detail, err := s.repo.GetBySlug(ctx, product.Product.Slug)
	if err != nil {
		return nil, err
	}
	s.logAudit(ctx, defaultString(req.CreatedBy, product.Product.CreatedBy), "CREATE_DATA_PRODUCT_VERSION")
	return detail, nil
}

// PublishVersion publishes a validated product version.
func (s *Service) PublishVersion(ctx context.Context, slug string, version int) (*domain.DataProductDetail, error) {
	product, targetVersion, err := s.loadVersion(ctx, slug, version)
	if err != nil {
		return nil, err
	}
	outputs, err := s.repo.ListOutputs(ctx, targetVersion.ID)
	if err != nil {
		return nil, err
	}
	entrypoints, err := s.repo.ListSemanticEntrypoints(ctx, targetVersion.ID)
	if err != nil {
		return nil, err
	}
	if publishErrs := s.validatePublish(ctx, product, targetVersion, outputs, entrypoints); len(publishErrs) > 0 {
		return nil, domain.ErrValidation("publish validation failed: %s", strings.Join(publishErrs, "; "))
	}

	for i := range product.Versions {
		if product.Versions[i].ReleaseState == domain.ProductReleaseStatePublished && product.Versions[i].ID != targetVersion.ID {
			if err := s.repo.UpdateVersionReleaseState(ctx, product.Versions[i].ID, domain.ProductReleaseStateDeprecated); err != nil {
				return nil, err
			}
			if err := s.updateBuildStateForVersion(ctx, &product.Versions[i], domain.BuildStateSuperseded); err != nil {
				return nil, err
			}
		}
	}
	if err := s.repo.UpdateVersionReleaseState(ctx, targetVersion.ID, domain.ProductReleaseStatePublished); err != nil {
		return nil, err
	}
	if err := s.updateBuildStateForVersion(ctx, targetVersion, domain.BuildStateReleased); err != nil {
		return nil, err
	}
	if err := s.repo.UpdatePublicationIntent(ctx, product.Product.ID, domain.ProductPublicationIntentPublished); err != nil {
		return nil, err
	}

	status, err := s.computeStatus(ctx, product.Product.ID, domain.ProductReleaseStatePublished, outputs)
	if err != nil {
		return nil, err
	}
	status.PublicationState = domain.ProductReleaseStatePublished
	status.OpenWarnings = removeWarning(status.OpenWarnings, "Draft product has not been published")
	if err := s.repo.UpsertStatus(ctx, status); err != nil {
		return nil, err
	}
	s.logEvent(ctx, product.Product.ID, "publication", fmt.Sprintf("Version v%d published", targetVersion.Version), "The product version is now the default published release.", map[string]any{
		"version": targetVersion.Version,
	})
	return s.repo.GetBySlug(ctx, product.Product.Slug)
}

// DeprecateVersion marks a product version as deprecated.
func (s *Service) DeprecateVersion(ctx context.Context, slug string, version int, replacementSlug *string) (*domain.DataProductDetail, error) {
	product, targetVersion, err := s.loadVersion(ctx, slug, version)
	if err != nil {
		return nil, err
	}
	if err := s.repo.UpdateVersionReleaseState(ctx, targetVersion.ID, domain.ProductReleaseStateDeprecated); err != nil {
		return nil, err
	}
	if err := s.updateBuildStateForVersion(ctx, targetVersion, domain.BuildStateSuperseded); err != nil {
		return nil, err
	}
	status, err := s.computeStatus(ctx, product.Product.ID, domain.ProductReleaseStateDeprecated, product.Outputs)
	if err != nil {
		return nil, err
	}
	status.PublicationState = domain.ProductReleaseStateDeprecated
	status.OpenWarnings = appendUniqueWarning(status.OpenWarnings, fmt.Sprintf("Version v%d is deprecated", targetVersion.Version))
	if replacementSlug != nil && strings.TrimSpace(*replacementSlug) != "" {
		replacement, replacementErr := s.repo.GetBySlug(ctx, strings.TrimSpace(*replacementSlug))
		if replacementErr != nil {
			return nil, replacementErr
		}
		status.ReplacementProductID = &replacement.Product.ID
	}
	if err := s.repo.UpsertStatus(ctx, status); err != nil {
		return nil, err
	}
	s.logEvent(ctx, product.Product.ID, "deprecation", fmt.Sprintf("Version v%d deprecated", targetVersion.Version), "The product version was deprecated.", map[string]any{
		"version": targetVersion.Version,
	})
	return s.repo.GetBySlug(ctx, product.Product.Slug)
}

// RetireVersion marks a product version as retired.
func (s *Service) RetireVersion(ctx context.Context, slug string, version int) (*domain.DataProductDetail, error) {
	product, targetVersion, err := s.loadVersion(ctx, slug, version)
	if err != nil {
		return nil, err
	}
	if err := s.repo.UpdateVersionReleaseState(ctx, targetVersion.ID, domain.ProductReleaseStateRetired); err != nil {
		return nil, err
	}
	if err := s.updateBuildStateForVersion(ctx, targetVersion, domain.BuildStateSuperseded); err != nil {
		return nil, err
	}
	status, err := s.computeStatus(ctx, product.Product.ID, domain.ProductReleaseStateRetired, product.Outputs)
	if err != nil {
		return nil, err
	}
	status.PublicationState = domain.ProductReleaseStateRetired
	status.OpenWarnings = appendUniqueWarning(status.OpenWarnings, fmt.Sprintf("Version v%d is retired", targetVersion.Version))
	if err := s.repo.UpsertStatus(ctx, status); err != nil {
		return nil, err
	}
	s.logEvent(ctx, product.Product.ID, "deprecation", fmt.Sprintf("Version v%d retired", targetVersion.Version), "The product version was retired from default discovery.", map[string]any{
		"version": targetVersion.Version,
	})
	return s.repo.GetBySlug(ctx, product.Product.Slug)
}

// AddDependency records a product-to-product dependency.
func (s *Service) AddDependency(ctx context.Context, slug, dependsOnSlug string) (*domain.DataProductDetail, error) {
	product, err := s.GetProduct(ctx, slug)
	if err != nil {
		return nil, err
	}
	dependency, err := s.repo.GetBySlug(ctx, dependsOnSlug)
	if err != nil {
		return nil, err
	}
	if product.Product.ID == dependency.Product.ID {
		return nil, domain.ErrValidation("product cannot depend on itself")
	}
	if err := s.repo.AddDependency(ctx, &domain.ProductDependency{
		ProductID:          product.Product.ID,
		DependsOnProductID: dependency.Product.ID,
	}); err != nil {
		return nil, err
	}
	s.logEvent(ctx, product.Product.ID, "ownership_change", "Dependency added", fmt.Sprintf("Product now depends on %s.", dependency.Product.Name), map[string]any{
		"depends_on_slug": dependency.Product.Slug,
	})
	return s.repo.GetBySlug(ctx, product.Product.Slug)
}

// Subscribe creates a product subscription for change events.
func (s *Service) Subscribe(ctx context.Context, slug, principalName, eventType, channel string) (*domain.ProductSubscription, error) {
	product, err := s.GetProduct(ctx, slug)
	if err != nil {
		return nil, err
	}
	eventType = strings.ToLower(strings.TrimSpace(eventType))
	switch eventType {
	case "schema_change", "freshness_breach", "quality_failure", "publication", "deprecation", "ownership_change":
	default:
		return nil, domain.ErrValidation("unsupported event_type %q", eventType)
	}
	if strings.TrimSpace(principalName) == "" {
		return nil, domain.ErrValidation("principal_name is required")
	}
	item, err := s.repo.AddSubscription(ctx, &domain.ProductSubscription{
		ProductID:     product.Product.ID,
		PrincipalName: strings.TrimSpace(principalName),
		EventType:     eventType,
		Channel:       defaultString(strings.TrimSpace(channel), "inbox"),
	})
	if err != nil {
		return nil, err
	}
	s.logEvent(ctx, product.Product.ID, "subscription", "Consumer subscribed", fmt.Sprintf("%s subscribed to %s via %s", item.PrincipalName, item.EventType, item.Channel), map[string]any{
		"principal_name": item.PrincipalName,
		"event_type":     item.EventType,
		"channel":        item.Channel,
	})
	return item, nil
}

// ListScorecards lists product governance scorecards.
func (s *Service) ListScorecards(ctx context.Context, page domain.PageRequest) ([]domain.ProductScorecard, int64, error) {
	items, total, err := s.ListProducts(ctx, domain.DataProductFilter{Page: page})
	if err != nil {
		return nil, 0, err
	}
	scorecards := make([]domain.ProductScorecard, 0, len(items))
	for i := range items {
		scorecards = append(scorecards, scorecardForItem(items[i]))
	}
	return scorecards, total, nil
}

// ListEvents returns recent durable product events.
func (s *Service) ListEvents(ctx context.Context, slug string, page domain.PageRequest) ([]domain.ProductEvent, int64, error) {
	product, err := s.GetProduct(ctx, slug)
	if err != nil {
		return nil, 0, err
	}
	return s.repo.ListEvents(ctx, product.Product.ID, page)
}

// GetPortfolioReport returns aggregated adoption, completeness, and orphan-resource reporting.
func (s *Service) GetPortfolioReport(ctx context.Context) (*domain.ProductPortfolioReport, error) {
	items, _, err := s.ListProducts(ctx, domain.DataProductFilter{Page: domain.PageRequest{MaxResults: domain.MaxMaxResults}})
	if err != nil {
		return nil, err
	}

	adoption := make([]domain.ProductAdoptionSummary, 0, len(items))
	domainGroups := map[string]*domain.ProductPortfolioGroup{}
	teamGroups := map[string]*domain.ProductPortfolioGroup{}
	for i := range items {
		summary := adoptionSummaryForItem(items[i])
		detail, detailErr := s.GetProduct(ctx, items[i].Product.Slug)
		if detailErr == nil {
			summary.OutputCount = int64(len(detail.Outputs))
			summary.SemanticEntrypointCount = int64(len(detail.SemanticEntrypoints))
		}
		adoption = append(adoption, summary)

		scorecard := scorecardForItem(items[i])
		domainGroup := ensurePortfolioGroup(domainGroups, items[i].Domain.Name)
		applyScorecardToGroup(domainGroup, scorecard)

		teamGroup := ensurePortfolioGroup(teamGroups, items[i].OwnerTeam.Name)
		applyScorecardToGroup(teamGroup, scorecard)
	}

	topUsed := slices.Clone(adoption)
	slices.SortStableFunc(topUsed, func(a, b domain.ProductAdoptionSummary) int {
		if diff := cmpInt64Desc(a.AdoptionScore, b.AdoptionScore); diff != 0 {
			return diff
		}
		return strings.Compare(strings.ToLower(a.ProductName), strings.ToLower(b.ProductName))
	})

	leastAdopted := slices.Clone(adoption)
	slices.SortStableFunc(leastAdopted, func(a, b domain.ProductAdoptionSummary) int {
		if diff := cmpInt64Asc(a.AdoptionScore, b.AdoptionScore); diff != 0 {
			return diff
		}
		return strings.Compare(strings.ToLower(a.ProductName), strings.ToLower(b.ProductName))
	})

	highBlastRadius := slices.Clone(adoption)
	slices.SortStableFunc(highBlastRadius, func(a, b domain.ProductAdoptionSummary) int {
		if diff := cmpInt64Desc(a.DownstreamProductCount, b.DownstreamProductCount); diff != 0 {
			return diff
		}
		return strings.Compare(strings.ToLower(a.ProductName), strings.ToLower(b.ProductName))
	})

	orphanAssets, err := s.repo.ListOrphanAssets(ctx)
	if err != nil {
		return nil, err
	}
	orphanSemanticModels, err := s.repo.ListOrphanSemanticModels(ctx)
	if err != nil {
		return nil, err
	}

	return &domain.ProductPortfolioReport{
		TopUsed:              limitAdoptionSummaries(topUsed, 10),
		LeastAdopted:         limitAdoptionSummaries(leastAdopted, 10),
		HighBlastRadius:      limitAdoptionSummaries(highBlastRadius, 10),
		DomainScorecards:     sortedPortfolioGroups(domainGroups),
		TeamScorecards:       sortedPortfolioGroups(teamGroups),
		OrphanAssets:         orphanAssets,
		OrphanSemanticModels: orphanSemanticModels,
	}, nil
}

// EnsureManagedRuntimeProduct provisions a system-owned draft product for synced runtime assets.
func (s *Service) EnsureManagedRuntimeProduct(ctx context.Context, kind string) (*domain.DataProductDetail, error) {
	if s == nil || s.repo == nil || s.domains == nil || s.teams == nil {
		return nil, domain.ErrNotImplemented("managed runtime products are not configured")
	}
	spec, err := managedRuntimeProductSpec(kind)
	if err != nil {
		return nil, err
	}
	existing, getErr := s.repo.GetBySlug(ctx, spec.slug)
	if getErr == nil {
		return existing, nil
	}
	var notFound *domain.NotFoundError
	if !errors.As(getErr, &notFound) {
		return nil, getErr
	}

	domainItem, err := s.ensureDomain(ctx, spec.domainName)
	if err != nil {
		return nil, err
	}
	teamItem, err := s.ensureTeam(ctx, domainItem.ID, spec.teamName, spec.contactChannel)
	if err != nil {
		return nil, err
	}
	product, err := s.repo.Create(ctx, &domain.DataProduct{
		Slug:              spec.slug,
		Name:              spec.name,
		Description:       spec.description,
		DomainID:          domainItem.ID,
		OwnerTeamID:       teamItem.ID,
		StewardPrincipal:  spec.steward,
		ContactChannel:    spec.contactChannel,
		Visibility:        "internal",
		ConsumerAudience:  "platform-operators",
		PublicationIntent: domain.ProductPublicationIntentDraft,
		CreatedBy:         "system",
	})
	if err != nil {
		return nil, err
	}
	if _, err := s.createVersionForProduct(ctx, product, domain.CreateDataProductVersionRequest{
		CompatibilityLevel: domain.ProductCompatibilityBackwardCompatible,
		CreatedBy:          "system",
	}); err != nil {
		return nil, err
	}
	if err := s.repo.UpsertStatus(ctx, &domain.DataProductStatus{
		ProductID:          product.ID,
		PublicationState:   domain.ProductReleaseStateDraft,
		CertificationState: domain.CertificationDraft,
		FreshnessStatus:    "UNKNOWN",
		QualityStatus:      "UNKNOWN",
		AdoptionMetrics:    map[string]any{},
		OpenWarnings: []string{
			"System-managed runtime asset product",
			"Draft product has not been published",
		},
	}); err != nil {
		return nil, err
	}
	s.logEvent(ctx, product.ID, "ownership_change", "Managed runtime product provisioned", spec.description, map[string]any{
		"kind": spec.kind,
		"slug": spec.slug,
	})
	return s.repo.GetBySlug(ctx, product.Slug)
}

func (s *Service) createVersionForProduct(ctx context.Context, product *domain.DataProduct, req domain.CreateDataProductVersionRequest) (*domain.DataProductVersion, error) {
	versions, err := s.repo.ListVersions(ctx, product.ID)
	if err != nil {
		return nil, err
	}
	nextVersion := 1
	if len(versions) > 0 {
		nextVersion = versions[0].Version + 1
	}
	if err := s.validateDraftProducingBuild(ctx, product.ID, req.ProducingBuildID); err != nil {
		return nil, err
	}

	version := &domain.DataProductVersion{
		ProductID:          product.ID,
		ProducingBuildID:   normalizedStringPtr(req.ProducingBuildID),
		Version:            nextVersion,
		ReleaseState:       domain.ProductReleaseStateDraft,
		CompatibilityLevel: defaultString(req.CompatibilityLevel, domain.ProductCompatibilityBackwardCompatible),
		Contract:           mergeContract(product.Contract, req.Contract),
		SLO:                mergeSLO(product.SLO, req.SLO),
		DocsURL:            defaultString(req.DocsURL, product.DocsURL),
		AccessRequestPath:  defaultString(req.AccessRequestPath, product.AccessRequestPath),
		CreatedBy:          strings.TrimSpace(req.CreatedBy),
	}
	created, err := s.repo.CreateVersion(ctx, version)
	if err != nil {
		return nil, err
	}
	outputs, err := s.resolveAssetOutputs(ctx, product.ID, created.ID, req.OutputAssetKeys)
	if err != nil {
		return nil, err
	}
	if len(outputs) > 0 {
		if err := s.repo.ReplaceOutputs(ctx, created.ID, outputs); err != nil {
			return nil, err
		}
	}
	entrypoints, err := s.resolveSemanticEntrypoints(ctx, created.ID, req.SemanticModelRefs)
	if err != nil {
		return nil, err
	}
	if len(entrypoints) > 0 {
		if err := s.repo.ReplaceSemanticEntrypoints(ctx, created.ID, entrypoints); err != nil {
			return nil, err
		}
	}
	s.logEvent(ctx, product.ID, "schema_change", fmt.Sprintf("Draft version v%d created", created.Version), "A new draft product version was created.", map[string]any{
		"version":             created.Version,
		"compatibility_level": created.CompatibilityLevel,
	})
	return created, nil
}

func (s *Service) validateDraftProducingBuild(ctx context.Context, productID string, buildID *string) error {
	if strings.TrimSpace(stringValue(buildID)) == "" {
		return nil
	}
	if s.builds == nil {
		return domain.ErrValidation("build provenance is not configured")
	}
	build, err := s.builds.GetByID(ctx, *buildID)
	if err != nil {
		return fmt.Errorf("resolve producing_build_id %q: %w", *buildID, err)
	}
	if build.ProductID == nil || *build.ProductID != productID {
		return domain.ErrValidation("producing_build_id must belong to the same product")
	}
	return nil
}

func (s *Service) resolveAssetOutputs(ctx context.Context, productID, productVersionID string, assetKeys []string) ([]domain.ProductOutput, error) {
	if len(assetKeys) == 0 {
		return nil, nil
	}
	if s.assets == nil {
		return nil, domain.ErrValidation("asset linkage is unavailable")
	}
	outputs := make([]domain.ProductOutput, 0, len(assetKeys))
	seen := make(map[string]struct{}, len(assetKeys))
	for i := range assetKeys {
		key := strings.TrimSpace(assetKeys[i])
		if key == "" {
			continue
		}
		if _, ok := seen[key]; ok {
			return nil, domain.ErrValidation("duplicate output asset key %q", key)
		}
		seen[key] = struct{}{}
		asset, err := s.assets.GetByKey(ctx, key)
		if err != nil {
			return nil, fmt.Errorf("resolve output asset %q: %w", key, err)
		}
		if strings.TrimSpace(asset.ProductID) != "" && asset.ProductID != productID {
			return nil, domain.ErrValidation("output asset %q already belongs to another product", key)
		}
		if strings.TrimSpace(asset.ProductID) == "" {
			asset.ProductID = productID
			if _, err := s.assets.Update(ctx, asset.ID, asset); err != nil {
				return nil, fmt.Errorf("attach output asset %q to product: %w", key, err)
			}
		}
		outputs = append(outputs, domain.ProductOutput{
			ProductVersionID: productVersionID,
			AssetID:          asset.ID,
			AssetKey:         asset.AssetKey,
			AssetType:        asset.AssetType,
			IsPrimary:        len(outputs) == 0,
		})
	}
	return outputs, nil
}

func (s *Service) loadVersion(ctx context.Context, slug string, version int) (*domain.DataProductDetail, *domain.DataProductVersion, error) {
	product, err := s.GetProduct(ctx, slug)
	if err != nil {
		return nil, nil, err
	}
	targetVersion, err := s.repo.GetVersionByNumber(ctx, product.Product.ID, version)
	if err != nil {
		return nil, nil, err
	}
	return product, targetVersion, nil
}

func (s *Service) resolveSemanticEntrypoints(ctx context.Context, productVersionID string, refs []string) ([]domain.ProductSemanticEntrypoint, error) {
	if len(refs) == 0 {
		return nil, nil
	}
	if s.semantic == nil {
		return nil, domain.ErrValidation("semantic model linkage is unavailable")
	}
	entrypoints := make([]domain.ProductSemanticEntrypoint, 0, len(refs))
	seen := make(map[string]struct{}, len(refs))
	for i := range refs {
		ref := strings.TrimSpace(refs[i])
		if ref == "" {
			continue
		}
		if _, ok := seen[ref]; ok {
			return nil, domain.ErrValidation("duplicate semantic model ref %q", ref)
		}
		seen[ref] = struct{}{}
		projectName, modelName, err := parseSemanticModelRef(ref)
		if err != nil {
			return nil, err
		}
		model, err := s.semantic.GetByName(ctx, projectName, modelName)
		if err != nil {
			return nil, fmt.Errorf("resolve semantic model %q: %w", ref, err)
		}
		entrypoints = append(entrypoints, domain.ProductSemanticEntrypoint{
			ProductVersionID: productVersionID,
			SemanticModelID:  model.ID,
			ProjectName:      model.ProjectName,
			ModelName:        model.Name,
		})
	}
	return entrypoints, nil
}

func (s *Service) ensureDomain(ctx context.Context, name string) (*domain.Domain, error) {
	name = strings.TrimSpace(name)
	existing, err := s.domains.GetByName(ctx, name)
	if err == nil {
		return existing, nil
	}
	var notFound *domain.NotFoundError
	if !errors.As(err, &notFound) {
		return nil, err
	}
	return s.domains.Create(ctx, &domain.Domain{Name: name})
}

func (s *Service) ensureTeam(ctx context.Context, domainID, name, contact string) (*domain.Team, error) {
	name = strings.TrimSpace(name)
	existing, err := s.teams.GetByDomainAndName(ctx, domainID, name)
	if err == nil {
		return existing, nil
	}
	var notFound *domain.NotFoundError
	if !errors.As(err, &notFound) {
		return nil, err
	}
	return s.teams.Create(ctx, &domain.Team{
		DomainID:       domainID,
		Name:           name,
		ContactChannel: strings.TrimSpace(contact),
	})
}

type managedRuntimeSpec struct {
	kind           string
	slug           string
	name           string
	description    string
	domainName     string
	teamName       string
	contactChannel string
	steward        string
}

func managedRuntimeProductSpec(kind string) (managedRuntimeSpec, error) {
	base := managedRuntimeSpec{
		kind:           strings.TrimSpace(kind),
		domainName:     "Platform Runtime",
		teamName:       "Control Plane",
		contactChannel: "#platform-runtime",
		steward:        "system",
	}
	switch strings.TrimSpace(kind) {
	case ManagedRuntimeProductPipelines:
		base.slug = "runtime-pipelines"
		base.name = "Runtime Pipelines"
		base.description = "System-managed product grouping pipeline-backed runtime assets."
	case ManagedRuntimeProductModels:
		base.slug = "runtime-models"
		base.name = "Runtime Models"
		base.description = "System-managed product grouping synced model runtime assets."
	case ManagedRuntimeProductNotebooks:
		base.slug = "runtime-notebooks"
		base.name = "Runtime Notebooks"
		base.description = "System-managed product grouping notebook runtime assets."
	case ManagedRuntimeProductNotebookOutputs:
		base.slug = "runtime-notebook-outputs"
		base.name = "Runtime Notebook Outputs"
		base.description = "System-managed product grouping published notebook output assets."
	case ManagedRuntimeProductSemantic:
		base.slug = "runtime-semantic"
		base.name = "Runtime Semantic Assets"
		base.description = "System-managed product grouping synced semantic runtime assets."
	case ManagedRuntimeProductDashboards:
		base.slug = "runtime-dashboards"
		base.name = "Runtime Dashboards"
		base.description = "System-managed product grouping dashboard runtime assets."
	default:
		return managedRuntimeSpec{}, domain.ErrValidation("unsupported managed runtime product kind %q", kind)
	}
	return base, nil
}

func cloneStringMap(in map[string]string) map[string]string {
	if len(in) == 0 {
		return map[string]string{}
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func outputSlice(output *domain.ProductOutput) []domain.ProductOutput {
	if output == nil {
		return nil
	}
	return []domain.ProductOutput{*output}
}

func (s *Service) computeStatus(ctx context.Context, productID, publicationIntent string, outputs []domain.ProductOutput) (*domain.DataProductStatus, error) {
	current, err := s.repo.GetStatus(ctx, productID)
	if err != nil {
		var notFound *domain.NotFoundError
		if !errors.As(err, &notFound) {
			return nil, err
		}
		current = &domain.DataProductStatus{
			ProductID:          productID,
			PublicationState:   publicationIntent,
			CertificationState: domain.CertificationDraft,
			AdoptionMetrics:    map[string]any{},
			OpenWarnings:       []string{},
		}
	}
	if len(outputs) == 0 || s.assets == nil || s.runs == nil || s.checks == nil {
		if strings.TrimSpace(current.FreshnessStatus) == "" {
			current.FreshnessStatus = "UNKNOWN"
		}
		if strings.TrimSpace(current.QualityStatus) == "" {
			current.QualityStatus = "UNKNOWN"
		}
		if len(outputs) == 0 {
			current.OpenWarnings = []string{"No product outputs linked"}
		}
		current.PublicationState = defaultPublicationState(current.PublicationState, publicationIntent)
		return current, s.repo.UpsertStatus(ctx, current)
	}

	now := time.Now().UTC()
	warnings := make([]string, 0)
	freshnessStatus := "UNKNOWN"
	qualityStatus := "UNKNOWN"
	failingChecks := int64(0)
	var lastSuccess *time.Time
	hasFreshnessPolicy := false
	hasChecks := false

	for i := range outputs {
		asset, assetErr := s.assets.GetByID(ctx, outputs[i].AssetID)
		if assetErr != nil {
			warnings = append(warnings, fmt.Sprintf("Linked asset %q is unavailable", outputs[i].AssetKey))
			continue
		}

		mats, _, matsErr := s.runs.ListMaterializationsByAsset(ctx, asset.ID, domain.PageRequest{MaxResults: 1})
		if matsErr != nil {
			return nil, matsErr
		}
		if len(mats) > 0 {
			materializedAt := mats[0].MaterializedAt
			if lastSuccess == nil || materializedAt.After(*lastSuccess) {
				lastSuccess = &materializedAt
			}
		}

		if asset.FreshnessPolicy != nil && asset.FreshnessPolicy.MaxLagSeconds > 0 {
			hasFreshnessPolicy = true
			if len(mats) == 0 || now.Sub(mats[0].MaterializedAt) > time.Duration(asset.FreshnessPolicy.MaxLagSeconds)*time.Second {
				freshnessStatus = "STALE"
			} else if freshnessStatus != "STALE" {
				freshnessStatus = "HEALTHY"
			}
		}

		checks, checksErr := s.checks.ListChecksByAsset(ctx, asset.ID)
		if checksErr != nil {
			return nil, checksErr
		}
		if len(checks) > 0 {
			hasChecks = true
			if qualityStatus == "UNKNOWN" {
				qualityStatus = "GOOD"
			}
		}
		for j := range checks {
			results, _, resultsErr := s.checks.ListCheckResults(ctx, checks[j].ID, domain.PageRequest{MaxResults: 1})
			if resultsErr != nil {
				return nil, resultsErr
			}
			if len(results) == 0 {
				continue
			}
			if strings.EqualFold(results[0].Status, "FAIL") || strings.EqualFold(results[0].Status, "ERROR") {
				failingChecks++
			}
		}
	}

	dependencies, depErr := s.repo.ListDependencies(ctx, productID)
	if depErr == nil {
		for i := range dependencies {
			if dependencies[i].Status == nil {
				continue
			}
			switch strings.ToUpper(strings.TrimSpace(dependencies[i].Status.QualityStatus)) {
			case "FAILED", "ERROR":
				warnings = append(warnings, fmt.Sprintf("Dependency %q is failing", dependencies[i].Product.Name))
				qualityStatus = "FAILED"
			}
			if strings.ToUpper(strings.TrimSpace(dependencies[i].Status.FreshnessStatus)) == "STALE" {
				warnings = append(warnings, fmt.Sprintf("Dependency %q is stale", dependencies[i].Product.Name))
				if freshnessStatus != "STALE" {
					freshnessStatus = "STALE"
				}
			}
		}
	}

	if !hasFreshnessPolicy {
		warnings = append(warnings, "No freshness policy configured on linked outputs")
		freshnessStatus = "UNKNOWN"
	}
	if !hasChecks {
		warnings = append(warnings, "No data quality checks configured on linked outputs")
		qualityStatus = "UNKNOWN"
	}
	if failingChecks > 0 {
		qualityStatus = "FAILED"
		warnings = append(warnings, fmt.Sprintf("%d linked checks are failing", failingChecks))
	}

	current.PublicationState = defaultPublicationState(current.PublicationState, publicationIntent)
	previousFreshness := current.FreshnessStatus
	previousQuality := current.QualityStatus
	current.LastSuccessfulUpdateAt = lastSuccess
	current.FreshnessStatus = freshnessStatus
	current.QualityStatus = qualityStatus
	current.FailingChecksCount = failingChecks
	current.OpenWarnings = warnings
	if current.AdoptionMetrics == nil {
		current.AdoptionMetrics = map[string]any{}
	}
	subscriptions, err := s.repo.ListSubscriptions(ctx, productID)
	if err != nil {
		return nil, err
	}
	downstreamCount, err := s.repo.CountDependents(ctx, productID)
	if err != nil {
		return nil, err
	}
	adoptionScore := int64(len(outputs)) + int64(len(subscriptions))*2 + downstreamCount*3
	current.AdoptionMetrics["subscription_count"] = len(subscriptions)
	current.AdoptionMetrics["downstream_product_count"] = downstreamCount
	current.AdoptionMetrics["output_count"] = len(outputs)
	current.AdoptionMetrics["adoption_score"] = adoptionScore
	current.AdoptionMetrics["dependency_count"] = len(dependencies)
	if err := s.repo.UpsertStatus(ctx, current); err != nil {
		return nil, err
	}
	if !strings.EqualFold(previousFreshness, current.FreshnessStatus) && strings.EqualFold(current.FreshnessStatus, "STALE") {
		s.logEvent(ctx, productID, "freshness_breach", "Freshness breach detected", "Linked outputs are stale relative to their freshness policies.", map[string]any{
			"freshness_status": current.FreshnessStatus,
		})
	}
	if !strings.EqualFold(previousQuality, current.QualityStatus) && strings.EqualFold(current.QualityStatus, "FAILED") {
		s.logEvent(ctx, productID, "quality_failure", "Quality failure detected", "Linked checks or dependencies are failing.", map[string]any{
			"quality_status":       current.QualityStatus,
			"failing_checks_count": current.FailingChecksCount,
		})
	}
	return current, nil
}

func (s *Service) validatePublish(ctx context.Context, product *domain.DataProductDetail, version *domain.DataProductVersion, outputs []domain.ProductOutput, entrypoints []domain.ProductSemanticEntrypoint) []string {
	errs := make([]string, 0)
	if strings.TrimSpace(product.Product.StewardPrincipal) == "" {
		errs = append(errs, "steward_principal is required")
	}
	if strings.TrimSpace(product.Product.ContactChannel) == "" {
		errs = append(errs, "contact_channel is required")
	}
	if !hasContract(version.Contract) {
		errs = append(errs, "contract is required")
	}
	if strings.TrimSpace(version.SLO.FreshnessSLO) == "" {
		errs = append(errs, "freshness_slo is required")
	}
	if strings.TrimSpace(version.DocsURL) == "" && strings.TrimSpace(version.AccessRequestPath) == "" {
		errs = append(errs, "docs_url or access_request_path is required")
	}
	if len(outputs) == 0 && len(entrypoints) == 0 {
		errs = append(errs, "at least one primary output or semantic entrypoint is required")
	}
	hasPrimary := false
	for i := range outputs {
		if outputs[i].IsPrimary {
			hasPrimary = true
			break
		}
	}
	if len(outputs) > 0 && !hasPrimary {
		errs = append(errs, "a primary output is required")
	}
	switch {
	case strings.TrimSpace(stringValue(version.ProducingBuildID)) == "":
		errs = append(errs, "producing_build_id is required")
	case s.builds == nil:
		errs = append(errs, "build provenance is not configured")
	default:
		build, err := s.builds.GetByID(ctx, *version.ProducingBuildID)
		if err != nil {
			errs = append(errs, fmt.Sprintf("producing_build_id %q is invalid", *version.ProducingBuildID))
		} else if build.ProductID == nil || *build.ProductID != product.Product.ID {
			errs = append(errs, "producing_build_id must belong to the same product")
		} else if projectErr := s.validateBuildPublicationPolicy(ctx, build, product.Product.ID); projectErr != nil {
			errs = append(errs, projectErr.Error())
		}
	}
	return errs
}

func (s *Service) validateBuildPublicationPolicy(ctx context.Context, build *domain.Build, productID string) error {
	if s.projects == nil {
		return domain.ErrValidation("project policy is not configured")
	}
	project, err := s.projects.GetByID(ctx, build.ProjectID)
	if err != nil {
		return fmt.Errorf("producing_build project is invalid")
	}
	if project.Kind != domain.ProjectKindShared {
		return domain.ErrValidation("producing_build_id must come from a shared project")
	}
	if project.ProductID == nil || *project.ProductID != productID {
		return domain.ErrValidation("producing_build_id project must be attached to the same product")
	}
	defaultBranchRef := "refs/heads/" + project.DefaultBranch
	if strings.TrimSpace(build.GitRef) != defaultBranchRef && !strings.HasPrefix(strings.TrimSpace(build.GitRef), "refs/tags/") {
		return domain.ErrValidation("producing_build_id must come from the project default branch or a tag")
	}
	return nil
}

func (s *Service) updateBuildStateForVersion(ctx context.Context, version *domain.DataProductVersion, state string) error {
	if version == nil || s.builds == nil || version.ProducingBuildID == nil || strings.TrimSpace(*version.ProducingBuildID) == "" {
		return nil
	}
	if err := s.builds.UpdateState(ctx, *version.ProducingBuildID, state); err != nil {
		return fmt.Errorf("update producing build %q state: %w", *version.ProducingBuildID, err)
	}
	return nil
}

func hasContract(contract domain.ProductContract) bool {
	return strings.TrimSpace(contract.DataGrain) != "" ||
		len(contract.PrimaryKeys) > 0 ||
		len(contract.Dimensions) > 0 ||
		len(contract.Measures) > 0 ||
		strings.TrimSpace(contract.UpdateCadence) != "" ||
		strings.TrimSpace(contract.BreakingChangePolicy) != ""
}

func normalizedStringPtr(value *string) *string {
	if value == nil {
		return nil
	}
	trimmed := strings.TrimSpace(*value)
	if trimmed == "" {
		return nil
	}
	return &trimmed
}

func stringValue(value *string) string {
	if value == nil {
		return ""
	}
	return strings.TrimSpace(*value)
}

func compareProductListItems(a, b domain.DataProductListItem) int {
	if diff := rankPublication(productListItemPublicationState(a)) - rankPublication(productListItemPublicationState(b)); diff != 0 {
		return diff
	}
	if diff := rankCertification(productListItemCertificationState(a)) - rankCertification(productListItemCertificationState(b)); diff != 0 {
		return diff
	}
	if diff := rankHealth(listItemHealth(a)) - rankHealth(listItemHealth(b)); diff != 0 {
		return diff
	}
	return strings.Compare(strings.ToLower(a.Product.Name), strings.ToLower(b.Product.Name))
}

func rankPublication(state string) int {
	switch strings.ToUpper(strings.TrimSpace(state)) {
	case domain.ProductReleaseStatePublished:
		return 0
	case domain.ProductReleaseStateDeprecated:
		return 1
	case domain.ProductReleaseStateDraft:
		return 2
	default:
		return 3
	}
}

func rankCertification(state string) int {
	switch strings.ToUpper(strings.TrimSpace(state)) {
	case domain.CertificationCertified:
		return 0
	case domain.CertificationDraft:
		return 1
	default:
		return 2
	}
}

func rankHealth(state string) int {
	switch strings.ToUpper(strings.TrimSpace(state)) {
	case "GOOD", "HEALTHY":
		return 0
	case "UNKNOWN":
		return 1
	case "STALE":
		return 2
	default:
		return 3
	}
}

func listItemHealth(item domain.DataProductListItem) string {
	if item.Status == nil {
		return "UNKNOWN"
	}
	if strings.TrimSpace(item.Status.QualityStatus) == "FAILED" {
		return item.Status.QualityStatus
	}
	if strings.TrimSpace(item.Status.FreshnessStatus) != "" && strings.TrimSpace(item.Status.FreshnessStatus) != "UNKNOWN" {
		return item.Status.FreshnessStatus
	}
	return item.Status.QualityStatus
}

func scorecardForItem(item domain.DataProductListItem) domain.ProductScorecard {
	hasOwner := strings.TrimSpace(item.Domain.Name) != "" && strings.TrimSpace(item.OwnerTeam.Name) != ""
	hasContract := hasContract(item.Product.Contract)
	hasSLO := strings.TrimSpace(item.Product.SLO.FreshnessSLO) != ""
	hasDocs := strings.TrimSpace(item.Product.DocsURL) != "" || strings.TrimSpace(item.Product.AccessRequestPath) != ""
	hasWarnings := item.Status != nil && len(item.Status.OpenWarnings) > 0
	completenessChecks := []bool{
		hasOwner,
		hasContract,
		hasSLO,
		hasDocs,
		item.PrimaryOutput != nil,
	}
	completed := 0
	for i := range completenessChecks {
		if completenessChecks[i] {
			completed++
		}
	}
	return domain.ProductScorecard{
		ProductID:           item.Product.ID,
		ProductSlug:         item.Product.Slug,
		ProductName:         item.Product.Name,
		DomainName:          item.Domain.Name,
		TeamName:            item.OwnerTeam.Name,
		PublicationState:    productListItemPublicationState(item),
		CertificationState:  productListItemCertificationState(item),
		HasOwner:            hasOwner,
		HasContract:         hasContract,
		HasSLO:              hasSLO,
		HasDocsOrAccessPath: hasDocs,
		HasPrimaryOutput:    item.PrimaryOutput != nil,
		HasWarnings:         hasWarnings,
		CompletenessPercent: (completed * 100) / len(completenessChecks),
	}
}

func adoptionSummaryForItem(item domain.DataProductListItem) domain.ProductAdoptionSummary {
	summary := domain.ProductAdoptionSummary{
		ProductID:   item.Product.ID,
		ProductSlug: item.Product.Slug,
		ProductName: item.Product.Name,
		DomainName:  item.Domain.Name,
		TeamName:    item.OwnerTeam.Name,
	}
	if item.Status == nil || item.Status.AdoptionMetrics == nil {
		return summary
	}
	summary.SubscriberCount = metricAsInt64(item.Status.AdoptionMetrics["subscription_count"])
	summary.DownstreamProductCount = metricAsInt64(item.Status.AdoptionMetrics["downstream_product_count"])
	summary.OutputCount = metricAsInt64(item.Status.AdoptionMetrics["output_count"])
	summary.SemanticEntrypointCount = metricAsInt64(item.Status.AdoptionMetrics["semantic_entrypoint_count"])
	summary.AdoptionScore = metricAsInt64(item.Status.AdoptionMetrics["adoption_score"])
	return summary
}

func ensurePortfolioGroup(groups map[string]*domain.ProductPortfolioGroup, name string) *domain.ProductPortfolioGroup {
	name = defaultString(strings.TrimSpace(name), "Unassigned")
	if existing, ok := groups[name]; ok {
		return existing
	}
	item := &domain.ProductPortfolioGroup{Name: name}
	groups[name] = item
	return item
}

func applyScorecardToGroup(group *domain.ProductPortfolioGroup, scorecard domain.ProductScorecard) {
	if group == nil {
		return
	}
	group.ProductCount++
	if strings.EqualFold(scorecard.PublicationState, domain.ProductReleaseStatePublished) {
		group.PublishedCount++
	}
	if strings.EqualFold(scorecard.CertificationState, domain.CertificationCertified) {
		group.CertifiedCount++
	}
	total := group.AverageCompletenessPct*int(group.ProductCount-1) + scorecard.CompletenessPercent
	group.AverageCompletenessPct = total / int(group.ProductCount)
}

func sortedPortfolioGroups(groups map[string]*domain.ProductPortfolioGroup) []domain.ProductPortfolioGroup {
	out := make([]domain.ProductPortfolioGroup, 0, len(groups))
	for _, item := range groups {
		out = append(out, *item)
	}
	slices.SortStableFunc(out, func(a, b domain.ProductPortfolioGroup) int {
		return strings.Compare(strings.ToLower(a.Name), strings.ToLower(b.Name))
	})
	return out
}

func limitAdoptionSummaries(items []domain.ProductAdoptionSummary, limit int) []domain.ProductAdoptionSummary {
	if len(items) <= limit {
		return items
	}
	return items[:limit]
}

func metricAsInt64(value any) int64 {
	switch v := value.(type) {
	case int:
		return int64(v)
	case int32:
		return int64(v)
	case int64:
		return v
	case float64:
		return int64(v)
	default:
		return 0
	}
}

func cmpInt64Asc(a, b int64) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	default:
		return 0
	}
}

func cmpInt64Desc(a, b int64) int {
	return cmpInt64Asc(b, a)
}

func mergeContract(base, override domain.ProductContract) domain.ProductContract {
	out := base
	if strings.TrimSpace(override.DataGrain) != "" {
		out.DataGrain = override.DataGrain
	}
	if len(override.PrimaryKeys) > 0 {
		out.PrimaryKeys = slices.Clone(override.PrimaryKeys)
	}
	if len(override.JoinKeys) > 0 {
		out.JoinKeys = slices.Clone(override.JoinKeys)
	}
	if len(override.Dimensions) > 0 {
		out.Dimensions = slices.Clone(override.Dimensions)
	}
	if len(override.Measures) > 0 {
		out.Measures = slices.Clone(override.Measures)
	}
	if strings.TrimSpace(override.RetentionWindow) != "" {
		out.RetentionWindow = override.RetentionWindow
	}
	if strings.TrimSpace(override.UpdateCadence) != "" {
		out.UpdateCadence = override.UpdateCadence
	}
	if len(override.QualityExpectations) > 0 {
		out.QualityExpectations = slices.Clone(override.QualityExpectations)
	}
	if strings.TrimSpace(override.BreakingChangePolicy) != "" {
		out.BreakingChangePolicy = override.BreakingChangePolicy
	}
	if len(override.SampleQueries) > 0 {
		out.SampleQueries = slices.Clone(override.SampleQueries)
	}
	return out
}

func mergeSLO(base, override domain.ProductSLO) domain.ProductSLO {
	out := base
	if strings.TrimSpace(override.FreshnessSLO) != "" {
		out.FreshnessSLO = override.FreshnessSLO
	}
	if strings.TrimSpace(override.LatencySLO) != "" {
		out.LatencySLO = override.LatencySLO
	}
	return out
}

func appendUniqueWarning(warnings []string, warning string) []string {
	for i := range warnings {
		if warnings[i] == warning {
			return warnings
		}
	}
	return append(warnings, warning)
}

func removeWarning(warnings []string, warning string) []string {
	filtered := make([]string, 0, len(warnings))
	for i := range warnings {
		if warnings[i] == warning {
			continue
		}
		filtered = append(filtered, warnings[i])
	}
	return filtered
}

func defaultPublicationState(current, fallback string) string {
	current = strings.TrimSpace(current)
	if current != "" {
		return current
	}
	fallback = strings.TrimSpace(fallback)
	if fallback != "" {
		return fallback
	}
	return domain.ProductReleaseStateDraft
}

func productListItemPublicationState(item domain.DataProductListItem) string {
	if item.Status == nil {
		return item.Product.PublicationIntent
	}
	return item.Status.PublicationState
}

func productListItemCertificationState(item domain.DataProductListItem) string {
	if item.Status == nil {
		return domain.CertificationDraft
	}
	return item.Status.CertificationState
}

func defaultString(value, fallback string) string {
	if strings.TrimSpace(value) == "" {
		return fallback
	}
	return value
}

func parseSemanticModelRef(ref string) (string, string, error) {
	parts := strings.Split(strings.TrimSpace(ref), ".")
	if len(parts) != 2 || strings.TrimSpace(parts[0]) == "" || strings.TrimSpace(parts[1]) == "" {
		return "", "", domain.ErrValidation("semantic model ref %q must be project.model", ref)
	}
	return strings.TrimSpace(parts[0]), strings.TrimSpace(parts[1]), nil
}

func (s *Service) logAudit(ctx context.Context, principal, action string) {
	if s == nil || s.audit == nil {
		return
	}
	_ = s.audit.Insert(ctx, &domain.AuditEntry{
		ID:            domain.NewID(),
		PrincipalName: defaultString(strings.TrimSpace(principal), "system"),
		Action:        action,
		Status:        "ALLOWED",
		CreatedAt:     time.Now().UTC(),
	})
}

func (s *Service) logEvent(ctx context.Context, productID, eventType, title, description string, metadata map[string]any) {
	if s == nil || s.repo == nil || strings.TrimSpace(productID) == "" {
		return
	}
	_, _ = s.repo.AddEvent(ctx, &domain.ProductEvent{
		ProductID:   productID,
		EventType:   strings.TrimSpace(eventType),
		Title:       strings.TrimSpace(title),
		Description: strings.TrimSpace(description),
		Metadata:    metadata,
	})
}
