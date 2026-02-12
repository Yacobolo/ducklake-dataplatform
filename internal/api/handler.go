// Package api provides HTTP handlers for the data platform REST API.
package api

import (
	"duck-demo/internal/service/catalog"
	svccompute "duck-demo/internal/service/compute"
	"duck-demo/internal/service/governance"
	"duck-demo/internal/service/ingestion"
	"duck-demo/internal/service/query"
	"duck-demo/internal/service/security"
	"duck-demo/internal/service/storage"
)

// APIHandler implements the StrictServerInterface.
type APIHandler struct {
	query             *query.QueryService
	principals        *security.PrincipalService
	groups            *security.GroupService
	grants            *security.GrantService
	rowFilters        *security.RowFilterService
	columnMasks       *security.ColumnMaskService
	audit             *governance.AuditService
	manifest          *query.ManifestService
	catalog           *catalog.CatalogService
	queryHistory      *governance.QueryHistoryService
	lineage           *governance.LineageService
	search            *catalog.SearchService
	tags              *governance.TagService
	views             *catalog.ViewService
	ingestion         *ingestion.IngestionService
	storageCreds      *storage.StorageCredentialService
	externalLocations *storage.ExternalLocationService
	volumes           *storage.VolumeService
	computeEndpoints  *svccompute.ComputeEndpointService
}

// NewHandler creates a new APIHandler with all required service dependencies.
func NewHandler(
	query *query.QueryService,
	principals *security.PrincipalService,
	groups *security.GroupService,
	grants *security.GrantService,
	rowFilters *security.RowFilterService,
	columnMasks *security.ColumnMaskService,
	audit *governance.AuditService,
	manifest *query.ManifestService,
	catalog *catalog.CatalogService,
	queryHistory *governance.QueryHistoryService,
	lineage *governance.LineageService,
	search *catalog.SearchService,
	tags *governance.TagService,
	views *catalog.ViewService,
	ingestion *ingestion.IngestionService,
	storageCreds *storage.StorageCredentialService,
	externalLocations *storage.ExternalLocationService,
	volumes *storage.VolumeService,
	computeEndpoints *svccompute.ComputeEndpointService,
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
