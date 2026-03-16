package legacy

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"

	"github.com/go-chi/chi/v5"

	"duck-demo/internal/domain"
)

func (h *Handler) CatalogsList(w http.ResponseWriter, r *http.Request) {
	items, _, err := h.CatalogRegistration.List(r.Context(), domain.PageRequest{MaxResults: 200})
	if err != nil {
		h.renderServiceError(w, r, err)
		return
	}
	if len(items) == 0 {
		renderHTML(w, http.StatusOK, catalogsListPage(principalFromContext(r.Context()), nil, domain.PageRequest{MaxResults: 30}, 0))
		return
	}

	selectedCatalog := strings.TrimSpace(r.URL.Query().Get("catalog"))
	if selectedCatalog == "" {
		for i := range items {
			if items[i].IsDefault {
				selectedCatalog = items[i].Name
				break
			}
		}
	}
	if selectedCatalog == "" {
		selectedCatalog = items[0].Name
	}

	h.renderCatalogWorkspace(w, r, items, selectedCatalog)
}

func (h *Handler) CatalogsDetail(w http.ResponseWriter, r *http.Request) {
	catalogName := chi.URLParam(r, "catalogName")
	q := r.URL.Query()
	q.Set("catalog", catalogName)
	http.Redirect(w, r, "/ui/catalogs?"+q.Encode(), http.StatusSeeOther)
}

func (h *Handler) renderCatalogWorkspace(w http.ResponseWriter, r *http.Request, catalogs []domain.CatalogRegistration, catalogName string) {
	selectedSchema := strings.TrimSpace(r.URL.Query().Get("schema"))
	selectedType := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("type")))
	selectedName := strings.TrimSpace(r.URL.Query().Get("name"))
	activeTab := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("tab")))
	historyEntity := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("history_entity")))
	if selectedType == "" {
		selectedType = "catalog"
	}
	switch selectedType {
	case "catalog", "schema", "table", "view":
	default:
		selectedType = "catalog"
	}
	if activeTab == "" {
		activeTab = "overview"
	}

	var registration *domain.CatalogRegistration
	for i := range catalogs {
		if catalogs[i].Name == catalogName {
			item := catalogs[i]
			registration = &item
			break
		}
	}
	if registration == nil {
		c, err := h.CatalogRegistration.Get(r.Context(), catalogName)
		if err != nil {
			h.renderServiceError(w, r, err)
			return
		}
		registration = c
	}

	summary, summaryErr := h.Catalog.GetMetastoreSummary(r.Context(), catalogName)
	versionSummary, versionSummaryErr := h.Catalog.GetCatalogVersionSummary(r.Context(), catalogName)
	schemas, _, schemasErr := h.Catalog.ListSchemas(r.Context(), catalogName, domain.PageRequest{MaxResults: 200})
	if selectedSchema == "" && len(schemas) > 0 {
		selectedSchema = schemas[0].Name
	}

	sidebarCatalogs := make([]catalogWorkspaceCatalogLinkData, 0, len(catalogs))
	for i := range catalogs {
		item := catalogs[i]
		sidebarCatalogs = append(sidebarCatalogs, catalogWorkspaceCatalogLinkData{
			Name:      item.Name,
			Status:    string(item.Status),
			IsDefault: item.IsDefault,
			URL:       catalogExplorerURL(item.Name, "", "catalog", ""),
			Active:    item.Name == catalogName,
		})
	}

	assetLinks, err := h.linkedAssetResolver(r.Context())
	if err != nil {
		h.renderServiceError(w, r, err)
		return
	}

	explorerSchemas := make([]catalogWorkspaceSchemaNodeData, 0, len(schemas))
	for i := range schemas {
		s := schemas[i]
		schemaNode := catalogWorkspaceSchemaNodeData{
			Name:      s.Name,
			Owner:     s.Owner,
			Created:   formatTime(s.CreatedAt),
			Updated:   formatTime(s.UpdatedAt),
			URL:       catalogExplorerURL(catalogName, s.Name, "schema", ""),
			Active:    selectedType == "schema" && selectedSchema == s.Name,
			Open:      selectedSchema == s.Name,
			EditURL:   "/ui/catalogs/" + url.PathEscape(catalogName) + "/schemas/" + url.PathEscape(s.Name) + "/edit",
			DeleteURL: "/ui/catalogs/" + url.PathEscape(catalogName) + "/schemas/" + url.PathEscape(s.Name) + "/delete",
		}

		tables, _, tableErr := h.Catalog.ListTables(r.Context(), catalogName, s.Name, domain.PageRequest{MaxResults: 200})
		if tableErr == nil {
			tableNodes := make([]catalogWorkspaceObjectNodeData, 0, len(tables))
			for j := range tables {
				t := tables[j]
				assetRef := assetLinks.resolve(catalogName, s.Name, t.Name)
				tableNodes = append(tableNodes, catalogWorkspaceObjectNodeData{
					Name:     t.Name,
					URL:      catalogExplorerURL(catalogName, s.Name, "table", t.Name),
					AssetURL: assetRef.URL,
					AssetKey: assetRef.Key,
					Active:   selectedType == "table" && selectedSchema == s.Name && selectedName == t.Name,
					Owner:    t.Owner,
					Created:  formatTime(t.CreatedAt),
					Kind:     "table",
				})
			}
			schemaNode.Tables = tableNodes
		}

		views, _, viewsErr := h.View.ListViews(r.Context(), catalogName, s.Name, domain.PageRequest{MaxResults: 200})
		if viewsErr == nil {
			viewNodes := make([]catalogWorkspaceObjectNodeData, 0, len(views))
			for j := range views {
				v := views[j]
				assetRef := assetLinks.resolve(catalogName, s.Name, v.Name)
				viewNodes = append(viewNodes, catalogWorkspaceObjectNodeData{
					Name:     v.Name,
					URL:      catalogExplorerURL(catalogName, s.Name, "view", v.Name),
					AssetURL: assetRef.URL,
					AssetKey: assetRef.Key,
					Active:   selectedType == "view" && selectedSchema == s.Name && selectedName == v.Name,
					Owner:    v.Owner,
					Created:  formatTime(v.CreatedAt),
					Kind:     "view",
				})
			}
			schemaNode.Views = viewNodes
		}

		explorerSchemas = append(explorerSchemas, schemaNode)
	}

	panel := catalogWorkspacePanelData{Mode: "catalog", Title: registration.Name, Subtitle: "Catalog", Description: dashIfEmpty(registration.Comment)}
	metastoreItems := make([]catalogWorkspaceMetaItemData, 0, 8)
	metastoreItems = append(metastoreItems,
		catalogWorkspaceMetaItemData{Label: "Status", Value: string(registration.Status)},
		catalogWorkspaceMetaItemData{Label: "Metastore", Value: string(registration.MetastoreType)},
		catalogWorkspaceMetaItemData{Label: "Data path", Value: registration.DataPath},
		catalogWorkspaceMetaItemData{Label: "Default", Value: fmt.Sprintf("%t", registration.IsDefault)},
	)
	if summary != nil {
		metastoreItems = append(metastoreItems,
			catalogWorkspaceMetaItemData{Label: "Metastore type", Value: summary.MetastoreType},
			catalogWorkspaceMetaItemData{Label: "Storage backend", Value: summary.StorageBackend},
			catalogWorkspaceMetaItemData{Label: "Schema count", Value: strconv.FormatInt(summary.SchemaCount, 10)},
			catalogWorkspaceMetaItemData{Label: "Table count", Value: strconv.FormatInt(summary.TableCount, 10)},
		)
	}
	if summaryErr != nil {
		metastoreItems = append(metastoreItems, catalogWorkspaceMetaItemData{Label: "Metastore", Value: "Unavailable"})
	}
	if schemasErr != nil {
		metastoreItems = append(metastoreItems, catalogWorkspaceMetaItemData{Label: "Schemas", Value: "Unavailable"})
	}

	panel.MetaItems = metastoreItems
	panel.VersionSummary = versionSummary
	panel.HistoryEntity = historyEntity
	if versionSummaryErr != nil {
		panel.VersionError = "Version metadata is unavailable right now."
	}
	panel.EditURL = "/ui/catalogs/" + url.PathEscape(registration.Name) + "/edit"
	panel.SetDefaultURL = "/ui/catalogs/" + url.PathEscape(registration.Name) + "/set-default"
	panel.DeleteURL = "/ui/catalogs/" + url.PathEscape(registration.Name) + "/delete"
	panel.NewSchemaURL = "/ui/catalogs/" + url.PathEscape(registration.Name) + "/schemas/new"

	if selectedType == "schema" && selectedSchema != "" {
		schema, schemaErr := h.Catalog.GetSchema(r.Context(), catalogName, selectedSchema)
		if schemaErr == nil {
			panel = catalogWorkspacePanelData{
				Mode:        "schema",
				Title:       selectedSchema,
				Subtitle:    "Schema",
				Description: dashIfEmpty(schema.Comment),
				EditURL:     "/ui/catalogs/" + url.PathEscape(catalogName) + "/schemas/" + url.PathEscape(selectedSchema) + "/edit",
				DeleteURL:   "/ui/catalogs/" + url.PathEscape(catalogName) + "/schemas/" + url.PathEscape(selectedSchema) + "/delete",
				MetaItems: []catalogWorkspaceMetaItemData{
					{Label: "Owner", Value: schema.Owner},
					{Label: "Created at", Value: formatTime(schema.CreatedAt)},
					{Label: "Comment", Value: dashIfEmpty(schema.Comment)},
					{Label: "Properties", Value: mapJSON(schema.Properties)},
					{Label: "Tags", Value: tagsLabel(schema.Tags)},
				},
			}
			panel.HistoryEntity = historyEntity
		}
	}

	if selectedType == "table" && selectedSchema != "" && selectedName != "" {
		table, tableErr := h.Catalog.GetTable(r.Context(), catalogName, selectedSchema, selectedName)
		if tableErr == nil {
			assetRef := assetLinks.resolve(catalogName, selectedSchema, selectedName)
			columnRows := make([]tableColumnRowData, 0, len(table.Columns))
			for i := range table.Columns {
				c := table.Columns[i]
				columnRows = append(columnRows, tableColumnRowData{Name: c.Name, Type: c.Type, Nullable: fmt.Sprintf("%t", c.Nullable), Comment: dashIfEmpty(c.Comment), Properties: mapJSON(c.Properties)})
			}
			panel = catalogWorkspacePanelData{
				Mode:        "table",
				Title:       selectedName,
				Subtitle:    "Table",
				Description: dashIfEmpty(table.Comment),
				MetaItems: []catalogWorkspaceMetaItemData{
					{Label: "Type", Value: table.TableType},
					{Label: "Owner", Value: table.Owner},
					{Label: "Created at", Value: formatTime(table.CreatedAt)},
					{Label: "Comment", Value: dashIfEmpty(table.Comment)},
					{Label: "Properties", Value: mapJSON(table.Properties)},
					{Label: "Tags", Value: tagsLabel(table.Tags)},
					{Label: "Updated", Value: formatTime(table.UpdatedAt)},
				},
				Columns:       columnRows,
				AssetURL:      assetRef.URL,
				AssetKey:      assetRef.Key,
				HistoryEntity: historyEntity,
			}
		}
	}

	if selectedType == "view" && selectedSchema != "" && selectedName != "" {
		v, viewErr := h.View.GetView(r.Context(), catalogName, selectedSchema, selectedName)
		if viewErr == nil {
			assetRef := assetLinks.resolve(catalogName, selectedSchema, selectedName)
			columns, _, columnsErr := h.Catalog.ListColumns(r.Context(), catalogName, selectedSchema, selectedName, domain.PageRequest{MaxResults: 200})
			columnRows := make([]tableColumnRowData, 0, len(columns))
			for i := range columns {
				c := columns[i]
				columnRows = append(columnRows, tableColumnRowData{Name: c.Name, Type: c.Type, Nullable: fmt.Sprintf("%t", c.Nullable), Comment: dashIfEmpty(c.Comment), Properties: mapJSON(c.Properties)})
			}
			panel = catalogWorkspacePanelData{
				Mode:        "view",
				Title:       selectedName,
				Subtitle:    "View",
				Description: stringPtr(v.Comment),
				MetaItems: []catalogWorkspaceMetaItemData{
					{Label: "Owner", Value: v.Owner},
					{Label: "Created at", Value: formatTime(v.CreatedAt)},
					{Label: "Comment", Value: stringPtr(v.Comment)},
					{Label: "Properties", Value: mapJSON(v.Properties)},
					{Label: "Source tables", Value: stringsJoin(v.SourceTables)},
					{Label: "Updated", Value: formatTime(v.UpdatedAt)},
				},
				Definition:       v.ViewDefinition,
				Columns:          columnRows,
				ColumnsAvailable: columnsErr == nil,
				AssetURL:         assetRef.URL,
				AssetKey:         assetRef.Key,
				HistoryEntity:    historyEntity,
			}
		}
	}

	if activeTab == "history" && selectedType != "view" {
		historyFilter := domain.CatalogHistoryFilter{EntityType: historyEntity, Limit: 50}
		if selectedType == "schema" || selectedType == "table" {
			historyFilter.SchemaName = selectedSchema
		}
		if selectedType == "table" {
			historyFilter.TableName = selectedName
		}
		historyEntries, historyErr := h.Catalog.ListCatalogHistory(r.Context(), catalogName, historyFilter)
		if historyErr == nil {
			panel.HistoryEntries = historyEntries
		} else {
			panel.VersionError = "History is unavailable right now."
		}
	}

	if !isCatalogTabAllowed(panel.Mode, activeTab) {
		activeTab = "overview"
	}

	renderHTML(w, http.StatusOK, catalogWorkspacePage(catalogWorkspacePageData{
		Principal:          principalFromContext(r.Context()),
		Catalogs:           sidebarCatalogs,
		ActiveCatalogName:  catalogName,
		SelectedSchemaName: selectedSchema,
		SelectedType:       selectedType,
		SelectedName:       selectedName,
		ActiveTab:          activeTab,
		Schemas:            explorerSchemas,
		Panel:              panel,
		QuickFilterMessage: "Filter catalogs, schemas, tables, and views",
		CSRFField:          csrfFieldProvider(r),
	}))
}

type linkedAssetRef struct {
	Key string
	URL string
}

type linkedAssetResolver struct {
	byKey map[string]linkedAssetRef
}

func (r linkedAssetResolver) resolve(catalogName, schemaName, objectName string) linkedAssetRef {
	for _, candidate := range assetLookupCandidates(catalogName, schemaName, objectName) {
		if ref, ok := r.byKey[candidate]; ok {
			return ref
		}
	}
	return linkedAssetRef{}
}

func (h *Handler) linkedAssetResolver(ctx context.Context) (linkedAssetResolver, error) {
	if h == nil || h.Asset == nil {
		return linkedAssetResolver{byKey: map[string]linkedAssetRef{}}, nil
	}

	resolver := linkedAssetResolver{byKey: map[string]linkedAssetRef{}}
	page := domain.PageRequest{MaxResults: domain.MaxMaxResults}
	offset := 0

	for {
		page.PageToken = domain.EncodePageToken(offset)
		assets, total, err := h.Asset.ListAssets(ctx, domain.AssetFilter{Page: page})
		if err != nil {
			return linkedAssetResolver{}, err
		}
		for i := range assets {
			asset := assets[i]
			resolver.byKey[asset.AssetKey] = linkedAssetRef{Key: asset.AssetKey, URL: "/ui/assets/" + asset.AssetKey}
		}
		offset += len(assets)
		if len(assets) == 0 || int64(offset) >= total {
			break
		}
	}
	return resolver, nil
}

func assetLookupCandidates(catalogName, schemaName, objectName string) []string {
	parts := []string{strings.TrimSpace(objectName), strings.TrimSpace(schemaName) + "." + strings.TrimSpace(objectName), strings.TrimSpace(catalogName) + "." + strings.TrimSpace(schemaName) + "." + strings.TrimSpace(objectName)}
	out := make([]string, 0, len(parts))
	seen := map[string]struct{}{}
	for i := range parts {
		candidate := strings.Trim(parts[i], ".")
		candidate = strings.TrimSpace(candidate)
		if candidate == "" {
			continue
		}
		if _, ok := seen[candidate]; ok {
			continue
		}
		seen[candidate] = struct{}{}
		out = append(out, candidate)
	}
	return out
}

func (h *Handler) CatalogsNew(w http.ResponseWriter, r *http.Request) {
	renderHTML(w, http.StatusOK, catalogsNewPage(principalFromContext(r.Context()), csrfFieldProvider(r)))
}

func (h *Handler) CatalogsCreate(w http.ResponseWriter, r *http.Request) {
	if !parseFormOrRenderBadRequest(w, r) {
		return
	}
	_, err := h.CatalogRegistration.Register(r.Context(), domain.CreateCatalogRequest{
		Name:          formString(r.Form, "name"),
		MetastoreType: formString(r.Form, "metastore_type"),
		DSN:           formString(r.Form, "dsn"),
		DataPath:      formString(r.Form, "data_path"),
		Comment:       formString(r.Form, "comment"),
	})
	if err != nil {
		h.renderServiceError(w, r, err)
		return
	}
	http.Redirect(w, r, "/ui/catalogs", http.StatusSeeOther)
}

func (h *Handler) CatalogsEdit(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "catalogName")
	c, err := h.CatalogRegistration.Get(r.Context(), name)
	if err != nil {
		h.renderServiceError(w, r, err)
		return
	}
	renderHTML(w, http.StatusOK, catalogsEditPage(principalFromContext(r.Context()), name, c, csrfFieldProvider(r)))
}

func (h *Handler) CatalogsUpdate(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "catalogName")
	if !parseFormOrRenderBadRequest(w, r) {
		return
	}
	_, err := h.CatalogRegistration.Update(r.Context(), name, domain.UpdateCatalogRegistrationRequest{
		Comment:  formOptionalString(r.Form, "comment"),
		DataPath: formOptionalString(r.Form, "data_path"),
		DSN:      formOptionalString(r.Form, "dsn"),
	})
	if err != nil {
		h.renderServiceError(w, r, err)
		return
	}
	http.Redirect(w, r, catalogExplorerURL(name, "", "catalog", ""), http.StatusSeeOther)
}

func (h *Handler) CatalogsDelete(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "catalogName")
	if err := h.CatalogRegistration.Delete(r.Context(), name); err != nil {
		h.renderServiceError(w, r, err)
		return
	}
	http.Redirect(w, r, "/ui/catalogs", http.StatusSeeOther)
}

func (h *Handler) CatalogsSetDefault(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "catalogName")
	if _, err := h.CatalogRegistration.SetDefault(r.Context(), name); err != nil {
		h.renderServiceError(w, r, err)
		return
	}
	http.Redirect(w, r, catalogExplorerURL(name, "", "catalog", ""), http.StatusSeeOther)
}

func (h *Handler) CatalogSchemasNew(w http.ResponseWriter, r *http.Request) {
	catalogName := chi.URLParam(r, "catalogName")
	renderHTML(w, http.StatusOK, catalogSchemasNewPage(principalFromContext(r.Context()), catalogName, csrfFieldProvider(r)))
}

func (h *Handler) CatalogSchemasCreate(w http.ResponseWriter, r *http.Request) {
	catalogName := chi.URLParam(r, "catalogName")
	principal, _ := principalLabel(r.Context())
	if !parseFormOrRenderBadRequest(w, r) {
		return
	}
	_, err := h.Catalog.CreateSchema(r.Context(), catalogName, principal, domain.CreateSchemaRequest{
		Name:         formString(r.Form, "name"),
		Comment:      formString(r.Form, "comment"),
		LocationName: formString(r.Form, "location_name"),
	})
	if err != nil {
		h.renderServiceError(w, r, err)
		return
	}
	http.Redirect(w, r, catalogExplorerURL(catalogName, "", "catalog", ""), http.StatusSeeOther)
}

func (h *Handler) CatalogSchemasDetail(w http.ResponseWriter, r *http.Request) {
	catalogName := chi.URLParam(r, "catalogName")
	schemaName := chi.URLParam(r, "schemaName")
	http.Redirect(w, r, catalogExplorerURL(catalogName, schemaName, "schema", ""), http.StatusSeeOther)
}

func (h *Handler) CatalogSchemasEdit(w http.ResponseWriter, r *http.Request) {
	catalogName := chi.URLParam(r, "catalogName")
	schemaName := chi.URLParam(r, "schemaName")
	s, err := h.Catalog.GetSchema(r.Context(), catalogName, schemaName)
	if err != nil {
		h.renderServiceError(w, r, err)
		return
	}
	renderHTML(w, http.StatusOK, catalogSchemasEditPage(principalFromContext(r.Context()), catalogName, schemaName, s, csrfFieldProvider(r)))
}

func (h *Handler) CatalogSchemasUpdate(w http.ResponseWriter, r *http.Request) {
	catalogName := chi.URLParam(r, "catalogName")
	schemaName := chi.URLParam(r, "schemaName")
	principal, _ := principalLabel(r.Context())
	if !parseFormOrRenderBadRequest(w, r) {
		return
	}
	_, err := h.Catalog.UpdateSchema(r.Context(), catalogName, principal, schemaName, domain.UpdateSchemaRequest{
		Comment: formOptionalString(r.Form, "comment"),
	})
	if err != nil {
		h.renderServiceError(w, r, err)
		return
	}
	http.Redirect(w, r, catalogExplorerURL(catalogName, schemaName, "schema", ""), http.StatusSeeOther)
}

func (h *Handler) CatalogSchemasDelete(w http.ResponseWriter, r *http.Request) {
	catalogName := chi.URLParam(r, "catalogName")
	schemaName := chi.URLParam(r, "schemaName")
	principal, _ := principalLabel(r.Context())
	if err := h.Catalog.DeleteSchema(r.Context(), catalogName, principal, schemaName, true); err != nil {
		h.renderServiceError(w, r, err)
		return
	}
	http.Redirect(w, r, catalogExplorerURL(catalogName, "", "catalog", ""), http.StatusSeeOther)
}

func (h *Handler) CatalogTablesDetail(w http.ResponseWriter, r *http.Request) {
	catalogName := chi.URLParam(r, "catalogName")
	schemaName := chi.URLParam(r, "schemaName")
	tableName := chi.URLParam(r, "tableName")
	http.Redirect(w, r, catalogExplorerURL(catalogName, schemaName, "table", tableName), http.StatusSeeOther)
}

func (h *Handler) CatalogViewsDetail(w http.ResponseWriter, r *http.Request) {
	catalogName := chi.URLParam(r, "catalogName")
	schemaName := chi.URLParam(r, "schemaName")
	viewName := chi.URLParam(r, "viewName")
	http.Redirect(w, r, catalogExplorerURL(catalogName, schemaName, "view", viewName), http.StatusSeeOther)
}

func catalogExplorerURL(catalogName, schemaName, objectType, objectName string) string {
	q := url.Values{}
	if schemaName != "" {
		q.Set("schema", schemaName)
	}
	if objectType != "" {
		q.Set("type", objectType)
	}
	if objectName != "" {
		q.Set("name", objectName)
	}
	if catalogName != "" {
		q.Set("catalog", catalogName)
	}
	query := q.Encode()
	base := "/ui/catalogs"
	if query == "" {
		return base
	}
	return base + "?" + query
}

func dashIfEmpty(v string) string {
	if v == "" {
		return "-"
	}
	return v
}

func tagsLabel(tags []domain.Tag) string {
	if len(tags) == 0 {
		return "-"
	}
	values := make([]string, 0, len(tags))
	for i := range tags {
		value := tags[i].Key
		if tags[i].Value != nil && *tags[i].Value != "" {
			value += "=" + *tags[i].Value
		}
		values = append(values, value)
	}
	sort.Strings(values)
	return stringsJoin(values)
}
