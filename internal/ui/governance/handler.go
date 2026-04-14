package governance

import (
	"net/http"
	"strconv"

	"github.com/go-chi/chi/v5"

	"github.com/Yacobolo/quackstack/internal/domain"
	"github.com/Yacobolo/quackstack/internal/ui/core"
)

type Handler struct{ deps *core.Dependencies }

func New(deps *core.Dependencies) *Handler { return &Handler{deps: deps} }

func (h *Handler) GovernanceHome(w http.ResponseWriter, r *http.Request) {
	http.Redirect(w, r, "/ui/governance/search", http.StatusSeeOther)
}

func (h *Handler) GovernanceSearch(w http.ResponseWriter, r *http.Request) {
	pageReq := pageFromRequest(r, 30)
	queryText := formString(r.URL.Query(), "q")
	objectType := formString(r.URL.Query(), "object_type")
	catalogName := formString(r.URL.Query(), "catalog")
	var objectTypePtr, catalogPtr *string
	var results []domain.SearchResult
	var err error
	if objectType != "" {
		objectTypePtr = &objectType
	}
	if catalogName != "" {
		catalogPtr = &catalogName
	}
	if queryText != "" {
		results, _, err = h.deps.Search.Search(r.Context(), queryText, objectTypePtr, catalogPtr, pageReq)
		if err != nil {
			renderServiceError(w, err)
			return
		}
	}
	_ = core.TrackResourceVisit(r, h.deps, domain.ResourceRef{
		ResourceType: "workspace",
		ResourceKey:  "governance/search",
		DisplayName:  "Governance Search",
		Section:      "Operate",
	})
	core.RenderHTML(w, http.StatusOK, governanceSearchPage(core.PrincipalFromContext(r.Context()), queryText, objectType, catalogName, results))
}

func (h *Handler) GovernanceTagsList(w http.ResponseWriter, r *http.Request) {
	pageReq := pageFromRequest(r, 30)
	items, total, err := h.deps.Tag.ListTags(r.Context(), pageReq)
	if err != nil {
		renderServiceError(w, err)
		return
	}
	rows := make([]governanceTagRowData, 0, len(items))
	for i := range items {
		item := items[i]
		assignments, err := h.deps.Tag.ListAssignmentsForTag(r.Context(), item.ID)
		if err != nil {
			renderServiceError(w, err)
			return
		}
		rows = append(rows, governanceTagRowData{
			ID:          item.ID,
			Key:         item.Key,
			Value:       strOrDash(item.Value),
			CreatedBy:   item.CreatedBy,
			Assignments: len(assignments),
		})
	}
	core.RenderHTML(w, http.StatusOK, governanceTagsPage(core.PrincipalFromContext(r.Context()), rows, pageReq, total, h.deps.CSRFFieldProvider(r)))
}

func (h *Handler) GovernanceTagsCreate(w http.ResponseWriter, r *http.Request) {
	if !parseFormOrRenderBadRequest(w, r) {
		return
	}
	req := domain.CreateTagRequest{Key: formString(r.Form, "key"), Value: formOptionalString(r.Form, "value")}
	if _, err := h.deps.Tag.CreateTag(r.Context(), principalName(r), req); err != nil {
		renderServiceError(w, err)
		return
	}
	http.Redirect(w, r, "/ui/governance/tags", http.StatusSeeOther)
}

func (h *Handler) GovernanceTagsDelete(w http.ResponseWriter, r *http.Request) {
	tagID := chi.URLParam(r, "tagID")
	if err := h.deps.Tag.DeleteTag(r.Context(), principalName(r), tagID); err != nil {
		renderServiceError(w, err)
		return
	}
	http.Redirect(w, r, "/ui/governance/tags", http.StatusSeeOther)
}

func (h *Handler) GovernanceTagAssignmentsCreate(w http.ResponseWriter, r *http.Request) {
	if !parseFormOrRenderBadRequest(w, r) {
		return
	}
	req := domain.AssignTagRequest{
		TagID:         formString(r.Form, "tag_id"),
		SecurableType: formString(r.Form, "securable_type"),
		SecurableID:   formString(r.Form, "securable_id"),
		ColumnName:    formOptionalString(r.Form, "column_name"),
	}
	if _, err := h.deps.Tag.AssignTag(r.Context(), principalName(r), req); err != nil {
		renderServiceError(w, err)
		return
	}
	http.Redirect(w, r, "/ui/governance/tags", http.StatusSeeOther)
}

func (h *Handler) GovernanceTagAssignmentsDelete(w http.ResponseWriter, r *http.Request) {
	if !parseFormOrRenderBadRequest(w, r) {
		return
	}
	if err := h.deps.Tag.UnassignTag(r.Context(), principalName(r), formString(r.Form, "assignment_id")); err != nil {
		renderServiceError(w, err)
		return
	}
	http.Redirect(w, r, "/ui/governance/tags", http.StatusSeeOther)
}

func (h *Handler) GovernanceAuditLogs(w http.ResponseWriter, r *http.Request) {
	pageReq := pageFromRequest(r, 30)
	filter := domain.AuditFilter{
		PrincipalName: optionalQueryValue(r, "principal"),
		Action:        optionalQueryValue(r, "action"),
		Status:        optionalQueryValue(r, "status"),
		Page:          pageReq,
	}
	items, total, err := h.deps.Audit.List(r.Context(), filter)
	if err != nil {
		renderServiceError(w, err)
		return
	}
	rows := make([]auditRowData, 0, len(items))
	for i := range items {
		item := items[i]
		rows = append(rows, auditRowData{
			Principal: item.PrincipalName,
			Action:    item.Action,
			Status:    item.Status,
			CreatedAt: formatTime(item.CreatedAt),
		})
	}
	core.RenderHTML(w, http.StatusOK, governanceAuditLogsPage(core.PrincipalFromContext(r.Context()), rows, pageReq, total))
}

func (h *Handler) GovernanceQueryHistory(w http.ResponseWriter, r *http.Request) {
	pageReq := pageFromRequest(r, 30)
	filter := domain.QueryHistoryFilter{
		PrincipalName: optionalQueryValue(r, "principal"),
		Status:        optionalQueryValue(r, "status"),
		Page:          pageReq,
	}
	items, total, err := h.deps.QueryHistory.List(r.Context(), filter)
	if err != nil {
		renderServiceError(w, err)
		return
	}
	rows := make([]queryHistoryRowData, 0, len(items))
	for i := range items {
		item := items[i]
		rows = append(rows, queryHistoryRowData{
			Principal: item.PrincipalName,
			Statement: strOrDash(item.StatementType),
			Status:    item.Status,
			CreatedAt: formatTime(item.CreatedAt),
		})
	}
	core.RenderHTML(w, http.StatusOK, governanceQueryHistoryPage(core.PrincipalFromContext(r.Context()), rows, pageReq, total))
}

func (h *Handler) GovernanceManifestPage(w http.ResponseWriter, r *http.Request) {
	if h.deps.Manifest == nil {
		core.RenderHTML(w, http.StatusInternalServerError, core.ErrorPage("Manifest Unavailable", "Manifest service is not configured."))
		return
	}
	core.RenderHTML(w, http.StatusOK, governanceManifestPage(governanceManifestPageData{
		Principal:         core.PrincipalFromContext(r.Context()),
		CSRFFieldProvider: h.deps.CSRFFieldProvider(r),
	}))
}

func (h *Handler) GovernanceManifestCreate(w http.ResponseWriter, r *http.Request) {
	if h.deps.Manifest == nil {
		core.RenderHTML(w, http.StatusInternalServerError, core.ErrorPage("Manifest Unavailable", "Manifest service is not configured."))
		return
	}
	if !parseFormOrRenderBadRequest(w, r) {
		return
	}
	result, err := h.deps.Manifest.GetManifest(
		r.Context(),
		principalName(r),
		formString(r.Form, "catalog_name"),
		formString(r.Form, "schema_name"),
		formString(r.Form, "table_name"),
	)
	if err != nil {
		renderServiceError(w, err)
		return
	}
	core.RenderHTML(w, http.StatusOK, governanceManifestPage(governanceManifestPageData{
		Principal:         core.PrincipalFromContext(r.Context()),
		CatalogName:       formString(r.Form, "catalog_name"),
		SchemaName:        formString(r.Form, "schema_name"),
		TableName:         formString(r.Form, "table_name"),
		Result:            result,
		CSRFFieldProvider: h.deps.CSRFFieldProvider(r),
	}))
}
func (h *Handler) GovernanceLineage(w http.ResponseWriter, r *http.Request) {
	schema := formString(r.URL.Query(), "schema")
	table := formString(r.URL.Query(), "table")
	column := formString(r.URL.Query(), "column")
	data := governanceLineagePageData{
		Principal:         core.PrincipalFromContext(r.Context()),
		Schema:            schema,
		Table:             table,
		Column:            column,
		CSRFFieldProvider: h.deps.CSRFFieldProvider(r),
	}
	if schema != "" && table != "" {
		qualified := schema + "." + table
		lineage, err := h.deps.Lineage.GetFullLineage(r.Context(), qualified, domain.PageRequest{MaxResults: 100})
		if err != nil {
			renderServiceError(w, err)
			return
		}
		columns, err := h.deps.Lineage.GetColumnLineageForTable(r.Context(), schema, table)
		if err != nil {
			renderServiceError(w, err)
			return
		}
		for i := range lineage.Upstream {
			edge := lineage.Upstream[i]
			data.UpstreamRows = append(data.UpstreamRows, governanceLineageEdgeRow{
				ID:     edge.ID,
				Source: edge.SourceSchema + "." + edge.SourceTable,
				Target: strOrDash(edge.TargetTable),
				Type:   edge.EdgeType,
			})
		}
		for i := range lineage.Downstream {
			edge := lineage.Downstream[i]
			data.DownstreamRows = append(data.DownstreamRows, governanceLineageEdgeRow{
				ID:     edge.ID,
				Source: edge.SourceSchema + "." + edge.SourceTable,
				Target: strOrDash(edge.TargetTable),
				Type:   edge.EdgeType,
			})
		}
		for i := range columns {
			edge := columns[i]
			data.ColumnRows = append(data.ColumnRows, governanceColumnLineageRow{
				TargetColumn: edge.TargetColumn,
				SourceColumn: edge.SourceSchema + "." + edge.SourceTable + "." + edge.SourceColumn,
				Transform:    string(edge.TransformType),
				Function:     valueOrDash(edge.Function),
			})
		}
		if column != "" {
			impact, err := h.deps.Lineage.GetColumnLineageForSourceColumn(r.Context(), schema, table, column)
			if err != nil {
				renderServiceError(w, err)
				return
			}
			for i := range impact {
				edge := impact[i]
				data.ImpactRows = append(data.ImpactRows, governanceColumnLineageRow{
					TargetColumn: edge.TargetColumn,
					SourceColumn: edge.SourceSchema + "." + edge.SourceTable + "." + edge.SourceColumn,
					Transform:    string(edge.TransformType),
					Function:     valueOrDash(edge.Function),
				})
			}
		}
	}
	core.RenderHTML(w, http.StatusOK, governanceLineagePage(data))
}
func (h *Handler) GovernanceLineageDeleteEdge(w http.ResponseWriter, r *http.Request) {
	edgeID := chi.URLParam(r, "edgeID")
	if err := h.deps.Lineage.DeleteEdge(r.Context(), edgeID); err != nil {
		renderServiceError(w, err)
		return
	}
	redirect := r.Referer()
	if redirect == "" {
		redirect = "/ui/governance/lineage"
	}
	http.Redirect(w, r, redirect, http.StatusSeeOther)
}
func (h *Handler) GovernanceLineagePurge(w http.ResponseWriter, r *http.Request) {
	if !parseFormOrRenderBadRequest(w, r) {
		return
	}
	olderThanDays, err := strconv.Atoi(formString(r.Form, "older_than_days"))
	if err != nil {
		core.RenderHTML(w, http.StatusBadRequest, core.ErrorPage("Invalid Request", "older_than_days must be numeric."))
		return
	}
	if _, err := h.deps.Lineage.PurgeOlderThan(r.Context(), olderThanDays); err != nil {
		renderServiceError(w, err)
		return
	}
	http.Redirect(w, r, "/ui/governance/lineage", http.StatusSeeOther)
}
