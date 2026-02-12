// Package api provides HTTP handlers for the data platform REST API.
package api

import (
	"duck-demo/internal/service"
)

// APIHandler implements the StrictServerInterface.
type APIHandler struct {
	query             *service.QueryService
	principals        *service.PrincipalService
	groups            *service.GroupService
	grants            *service.GrantService
	rowFilters        *service.RowFilterService
	columnMasks       *service.ColumnMaskService
	audit             *service.AuditService
	manifest          *service.ManifestService
	catalog           *service.CatalogService
	queryHistory      *service.QueryHistoryService
	lineage           *service.LineageService
	search            *service.SearchService
	tags              *service.TagService
	views             *service.ViewService
	ingestion         *service.IngestionService
	storageCreds      *service.StorageCredentialService
	externalLocations *service.ExternalLocationService
	volumes           *service.VolumeService
	computeEndpoints  *service.ComputeEndpointService
}

// NewHandler creates a new APIHandler with all required service dependencies.
func NewHandler(
	query *service.QueryService,
	principals *service.PrincipalService,
	groups *service.GroupService,
	grants *service.GrantService,
	rowFilters *service.RowFilterService,
	columnMasks *service.ColumnMaskService,
	audit *service.AuditService,
	manifest *service.ManifestService,
	catalog *service.CatalogService,
	queryHistory *service.QueryHistoryService,
	lineage *service.LineageService,
	search *service.SearchService,
	tags *service.TagService,
	views *service.ViewService,
	ingestion *service.IngestionService,
	storageCreds *service.StorageCredentialService,
	externalLocations *service.ExternalLocationService,
	volumes *service.VolumeService,
	computeEndpoints *service.ComputeEndpointService,
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
