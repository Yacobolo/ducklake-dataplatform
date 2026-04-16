package catalogs

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/Yacobolo/quackstack/internal/domain"
	"github.com/Yacobolo/quackstack/internal/ui/core"
)

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
		c, err := h.deps.CatalogRegistration.Get(r.Context(), catalogName)
		if err != nil {
			renderServiceError(w, err)
			return
		}
		registration = c
	}

	summary, summaryErr := h.deps.Catalog.GetMetastoreSummary(r.Context(), catalogName)
	versionSummary, versionSummaryErr := h.deps.Catalog.GetCatalogVersionSummary(r.Context(), catalogName)
	schemas, _, schemasErr := h.deps.Catalog.ListSchemas(r.Context(), catalogName, domain.PageRequest{MaxResults: 200})
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
		renderServiceError(w, err)
		return
	}

	explorerSchemas := make([]catalogWorkspaceSchemaNodeData, 0, len(schemas))
	for i := range schemas {
		s := schemas[i]
		schemaNode := catalogWorkspaceSchemaNodeData{
			Name:      s.Name,
			Owner:     s.Owner,
			Created:   core.FormatTimeUTC(s.CreatedAt),
			Updated:   core.FormatTimeUTC(s.UpdatedAt),
			URL:       catalogExplorerURL(catalogName, s.Name, "schema", ""),
			Active:    selectedType == "schema" && selectedSchema == s.Name,
			Open:      selectedSchema == s.Name,
			EditURL:   "/ui/catalogs/" + url.PathEscape(catalogName) + "/schemas/" + url.PathEscape(s.Name) + "/edit",
			DeleteURL: "/ui/catalogs/" + url.PathEscape(catalogName) + "/schemas/" + url.PathEscape(s.Name) + "/delete",
		}

		tables, _, tableErr := h.deps.Catalog.ListTables(r.Context(), catalogName, s.Name, domain.PageRequest{MaxResults: 200})
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
					Created:  core.FormatTimeUTC(t.CreatedAt),
					Kind:     "table",
				})
			}
			schemaNode.Tables = tableNodes
		}

		views, _, viewsErr := h.deps.View.ListViews(r.Context(), catalogName, s.Name, domain.PageRequest{MaxResults: 200})
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
					Created:  core.FormatTimeUTC(v.CreatedAt),
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
		schema, schemaErr := h.deps.Catalog.GetSchema(r.Context(), catalogName, selectedSchema)
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
					{Label: "Created at", Value: core.FormatTimeUTC(schema.CreatedAt)},
					{Label: "Comment", Value: dashIfEmpty(schema.Comment)},
					{Label: "Properties", Value: mapJSON(schema.Properties)},
					{Label: "Tags", Value: tagsLabel(schema.Tags)},
				},
			}
			panel.HistoryEntity = historyEntity
		}
	}

	if selectedType == "table" && selectedSchema != "" && selectedName != "" {
		table, tableErr := h.deps.Catalog.GetTable(r.Context(), catalogName, selectedSchema, selectedName)
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
					{Label: "Created at", Value: core.FormatTimeUTC(table.CreatedAt)},
					{Label: "Comment", Value: dashIfEmpty(table.Comment)},
					{Label: "Properties", Value: mapJSON(table.Properties)},
					{Label: "Tags", Value: tagsLabel(table.Tags)},
					{Label: "Updated", Value: core.FormatTimeUTC(table.UpdatedAt)},
				},
				Columns:       columnRows,
				AssetURL:      assetRef.URL,
				AssetKey:      assetRef.Key,
				HistoryEntity: historyEntity,
			}
		}
	}

	if selectedType == "view" && selectedSchema != "" && selectedName != "" {
		v, viewErr := h.deps.View.GetView(r.Context(), catalogName, selectedSchema, selectedName)
		if viewErr == nil {
			assetRef := assetLinks.resolve(catalogName, selectedSchema, selectedName)
			columnRows := make([]tableColumnRowData, 0, len(v.Columns))
			for i := range v.Columns {
				c := v.Columns[i]
				columnRows = append(columnRows, tableColumnRowData{Name: c.Name, Type: c.Type, Nullable: fmt.Sprintf("%t", c.Nullable), Comment: dashIfEmpty(c.Comment), Properties: mapJSON(c.Properties)})
			}
			panel = catalogWorkspacePanelData{
				Mode:        "view",
				Title:       selectedName,
				Subtitle:    "View",
				Description: core.StringPtr(v.Comment),
				MetaItems: []catalogWorkspaceMetaItemData{
					{Label: "Owner", Value: v.Owner},
					{Label: "Created at", Value: core.FormatTimeUTC(v.CreatedAt)},
					{Label: "Comment", Value: core.StringPtr(v.Comment)},
					{Label: "Properties", Value: mapJSON(v.Properties)},
					{Label: "Source tables", Value: stringsJoin(v.SourceTables)},
					{Label: "Updated", Value: core.FormatTimeUTC(v.UpdatedAt)},
				},
				Definition:       v.ViewDefinition,
				Columns:          columnRows,
				ColumnsAvailable: len(v.Columns) > 0,
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
		historyEntries, historyErr := h.deps.Catalog.ListCatalogHistory(r.Context(), catalogName, historyFilter)
		if historyErr == nil {
			panel.HistoryEntries = historyEntries
		} else {
			panel.VersionError = "History is unavailable right now."
		}
	}

	if !isCatalogTabAllowed(panel.Mode, activeTab) {
		activeTab = "overview"
	}

	core.RenderHTML(w, http.StatusOK, catalogWorkspacePage(catalogWorkspacePageData{
		Principal:          core.PrincipalFromContext(r.Context()),
		Catalogs:           sidebarCatalogs,
		ActiveCatalogName:  catalogName,
		SelectedSchemaName: selectedSchema,
		SelectedType:       selectedType,
		SelectedName:       selectedName,
		ActiveTab:          activeTab,
		Schemas:            explorerSchemas,
		Panel:              panel,
		QuickFilterMessage: "Filter catalogs, schemas, tables, and views",
		CSRFField:          h.deps.CSRFFieldProvider(r),
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
	if h == nil || h.deps.Asset == nil {
		return linkedAssetResolver{byKey: map[string]linkedAssetRef{}}, nil
	}

	resolver := linkedAssetResolver{byKey: map[string]linkedAssetRef{}}
	page := domain.PageRequest{MaxResults: domain.MaxMaxResults}
	offset := 0

	for {
		page.PageToken = domain.EncodePageToken(offset)
		assets, total, err := h.deps.Asset.ListAssets(ctx, domain.AssetFilter{Page: page})
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
