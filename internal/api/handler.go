// Package api provides HTTP handlers for the data platform REST API.
package api

// APIHandler implements the StrictServerInterface.
type APIHandler struct {
	query             queryService
	principals        principalService
	groups            groupService
	grants            grantService
	rowFilters        rowFilterService
	columnMasks       columnMaskService
	audit             auditService
	manifest          manifestService
	catalog           catalogService
	queryHistory      queryHistoryService
	lineage           lineageService
	search            searchService
	tags              tagService
	views             viewService
	ingestion         ingestionService
	storageCreds      storageCredentialService
	externalLocations externalLocationService
	volumes           volumeService
	computeEndpoints  computeEndpointService
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
) *APIHandler {
	return &APIHandler{
		query:             query,
		principals:        principals,
		groups:            groups,
		grants:            grants,
		rowFilters:        rowFilters,
		columnMasks:       columnMasks,
		audit:             audit,
		manifest:          manifest,
		catalog:           catalog,
		queryHistory:      queryHistory,
		lineage:           lineage,
		search:            search,
		tags:              tags,
		views:             views,
		ingestion:         ingestion,
		storageCreds:      storageCreds,
		externalLocations: externalLocations,
		volumes:           volumes,
		computeEndpoints:  computeEndpoints,
	}
}

// Ensure Handler implements the interface.
var _ StrictServerInterface = (*APIHandler)(nil)
