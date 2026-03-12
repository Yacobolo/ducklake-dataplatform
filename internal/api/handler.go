// Package api provides HTTP handlers for the data platform REST API.
package api

// APIHandler implements the GenStrictServerInterface.
type APIHandler struct {
	query               queryService
	principals          principalService
	groups              groupService
	grants              grantService
	rowFilters          rowFilterService
	columnMasks         columnMaskService
	audit               auditService
	manifest            manifestService
	catalog             catalogService
	catalogRegistration catalogRegistrationService
	queryHistory        queryHistoryService
	lineage             lineageService
	search              searchService
	tags                tagService
	views               viewService
	ingestion           ingestionService
	storageCreds        storageCredentialService
	externalLocations   externalLocationService
	volumes             volumeService
	computeEndpoints    computeEndpointService
	apiKeys             apiKeyService
	pipelines           pipelineService
	notebooks           notebookService
	sessions            sessionService
	gitRepos            gitRepoService
	assets              assetService
	backfills           assetBackfillService
	products            productService
	models              modelService
	macros              macroService
	semantics           semanticService
	dashboards          dashboardService
}

// NewHandler creates a new APIHandler with all required service dependencies.
func NewHandler(
	query queryService,
	principals principalService,
	groups groupService,
	grants grantService,
	rowFilters rowFilterService,
	columnMasks columnMaskService,
	audit auditService,
	manifest manifestService,
	catalog catalogService,
	catalogRegistration catalogRegistrationService,
	queryHistory queryHistoryService,
	lineage lineageService,
	search searchService,
	tags tagService,
	views viewService,
	ingestion ingestionService,
	storageCreds storageCredentialService,
	externalLocations externalLocationService,
	volumes volumeService,
	computeEndpoints computeEndpointService,
	apiKeys apiKeyService,
	pipelines pipelineService,
	notebooks notebookService,
	sessions sessionService,
	gitRepos gitRepoService,
	assets assetService,
	backfills assetBackfillService,
	models modelService,
	macros macroService,
	semantics semanticService,
	dashboards ...dashboardService,
) *APIHandler {
	var dashboardSvc dashboardService
	if len(dashboards) > 0 {
		dashboardSvc = dashboards[0]
	}
	return &APIHandler{
		query:               query,
		principals:          principals,
		groups:              groups,
		grants:              grants,
		rowFilters:          rowFilters,
		columnMasks:         columnMasks,
		audit:               audit,
		manifest:            manifest,
		catalog:             catalog,
		catalogRegistration: catalogRegistration,
		queryHistory:        queryHistory,
		lineage:             lineage,
		search:              search,
		tags:                tags,
		views:               views,
		ingestion:           ingestion,
		storageCreds:        storageCreds,
		externalLocations:   externalLocations,
		volumes:             volumes,
		computeEndpoints:    computeEndpoints,
		apiKeys:             apiKeys,
		pipelines:           pipelines,
		notebooks:           notebooks,
		sessions:            sessions,
		gitRepos:            gitRepos,
		assets:              assets,
		backfills:           backfills,
		models:              models,
		macros:              macros,
		semantics:           semantics,
		dashboards:          dashboardSvc,
	}
}

// Ensure Handler implements the interface.
var _ GenStrictServerInterface = (*APIHandler)(nil)
