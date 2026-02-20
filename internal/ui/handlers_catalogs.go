package ui

import (
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
	pageReq := pageFromRequest(r, 30)
	items, total, err := h.CatalogRegistration.List(r.Context(), pageReq)
	if err != nil {
		h.renderServiceError(w, r, err)
		return
	}

	rows := make([]catalogsListRowData, 0, len(items))
	for i := range items {
		item := items[i]
		rows = append(rows, catalogsListRowData{Filter: item.Name + " " + string(item.Status), Name: item.Name, URL: "/ui/catalogs/" + item.Name, Status: string(item.Status), Metastore: string(item.MetastoreType), Updated: formatTime(item.UpdatedAt)})
	}
	renderHTML(w, http.StatusOK, catalogsListPage(principalFromContext(r.Context()), rows, pageReq, total))
}

func (h *Handler) CatalogsDetail(w http.ResponseWriter, r *http.Request) {
	catalogName := chi.URLParam(r, "catalogName")
	registration, err := h.CatalogRegistration.Get(r.Context(), catalogName)
	if err != nil {
		h.renderServiceError(w, r, err)
		return
	}

	catalogs, _, err := h.CatalogRegistration.List(r.Context(), domain.PageRequest{MaxResults: 200})
	if err != nil {
		h.renderServiceError(w, r, err)
		return
	}

	selectedSchema := strings.TrimSpace(r.URL.Query().Get("schema"))
	selectedType := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("type")))
	selectedName := strings.TrimSpace(r.URL.Query().Get("name"))
	if selectedType == "" {
		selectedType = "catalog"
	}
	switch selectedType {
	case "catalog", "schema", "table", "view":
	default:
		selectedType = "catalog"
	}

	summary, summaryErr := h.Catalog.GetMetastoreSummary(r.Context(), catalogName)
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
			URL:       "/ui/catalogs/" + url.PathEscape(item.Name),
			Active:    item.Name == catalogName,
		})
	}

	explorerSchemas := make([]catalogWorkspaceSchemaNodeData, 0, len(schemas))
	for i := range schemas {
		s := schemas[i]
		schemaNode := catalogWorkspaceSchemaNodeData{
			Name:      s.Name,
			Owner:     s.Owner,
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
				tableNodes = append(tableNodes, catalogWorkspaceObjectNodeData{
					Name:   t.Name,
					URL:    catalogExplorerURL(catalogName, s.Name, "table", t.Name),
					Active: selectedType == "table" && selectedSchema == s.Name && selectedName == t.Name,
				})
			}
			schemaNode.Tables = tableNodes
		}

		views, _, viewsErr := h.View.ListViews(r.Context(), catalogName, s.Name, domain.PageRequest{MaxResults: 200})
		if viewsErr == nil {
			viewNodes := make([]catalogWorkspaceObjectNodeData, 0, len(views))
			for j := range views {
				v := views[j]
				viewNodes = append(viewNodes, catalogWorkspaceObjectNodeData{
					Name:   v.Name,
					URL:    catalogExplorerURL(catalogName, s.Name, "view", v.Name),
					Active: selectedType == "view" && selectedSchema == s.Name && selectedName == v.Name,
				})
			}
			schemaNode.Views = viewNodes
		}

		explorerSchemas = append(explorerSchemas, schemaNode)
	}

	panel := catalogWorkspacePanelData{Mode: "catalog", Title: registration.Name}
	metastoreItems := make([]catalogWorkspaceMetaItemData, 0, 8)
	metastoreItems = append(metastoreItems,
		catalogWorkspaceMetaItemData{Label: "Status", Value: string(registration.Status)},
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
	panel.EditURL = "/ui/catalogs/" + url.PathEscape(registration.Name) + "/edit"
	panel.SetDefaultURL = "/ui/catalogs/" + url.PathEscape(registration.Name) + "/set-default"
	panel.DeleteURL = "/ui/catalogs/" + url.PathEscape(registration.Name) + "/delete"
	panel.NewSchemaURL = "/ui/catalogs/" + url.PathEscape(registration.Name) + "/schemas/new"

	if selectedType == "schema" && selectedSchema != "" {
		schema, schemaErr := h.Catalog.GetSchema(r.Context(), catalogName, selectedSchema)
		if schemaErr == nil {
			panel = catalogWorkspacePanelData{
				Mode:      "schema",
				Title:     selectedSchema,
				Subtitle:  "Schema",
				EditURL:   "/ui/catalogs/" + url.PathEscape(catalogName) + "/schemas/" + url.PathEscape(selectedSchema) + "/edit",
				DeleteURL: "/ui/catalogs/" + url.PathEscape(catalogName) + "/schemas/" + url.PathEscape(selectedSchema) + "/delete",
				MetaItems: []catalogWorkspaceMetaItemData{
					{Label: "Owner", Value: schema.Owner},
					{Label: "Comment", Value: dashIfEmpty(schema.Comment)},
					{Label: "Properties", Value: mapJSON(schema.Properties)},
					{Label: "Tags", Value: tagsLabel(schema.Tags)},
				},
			}
		}
	}

	if selectedType == "table" && selectedSchema != "" && selectedName != "" {
		table, tableErr := h.Catalog.GetTable(r.Context(), catalogName, selectedSchema, selectedName)
		if tableErr == nil {
			columnRows := make([]tableColumnRowData, 0, len(table.Columns))
			for i := range table.Columns {
				c := table.Columns[i]
				columnRows = append(columnRows, tableColumnRowData{Name: c.Name, Type: c.Type, Nullable: fmt.Sprintf("%t", c.Nullable), Comment: dashIfEmpty(c.Comment), Properties: mapJSON(c.Properties)})
			}
			panel = catalogWorkspacePanelData{
				Mode:     "table",
				Title:    selectedName,
				Subtitle: "Table",
				MetaItems: []catalogWorkspaceMetaItemData{
					{Label: "Type", Value: table.TableType},
					{Label: "Owner", Value: table.Owner},
					{Label: "Comment", Value: dashIfEmpty(table.Comment)},
					{Label: "Properties", Value: mapJSON(table.Properties)},
					{Label: "Tags", Value: tagsLabel(table.Tags)},
					{Label: "Updated", Value: formatTime(table.UpdatedAt)},
				},
				Columns: columnRows,
			}
		}
	}

	if selectedType == "view" && selectedSchema != "" && selectedName != "" {
		v, viewErr := h.View.GetView(r.Context(), catalogName, selectedSchema, selectedName)
		if viewErr == nil {
			columns, _, columnsErr := h.Catalog.ListColumns(r.Context(), catalogName, selectedSchema, selectedName, domain.PageRequest{MaxResults: 200})
			columnRows := make([]tableColumnRowData, 0, len(columns))
			for i := range columns {
				c := columns[i]
				columnRows = append(columnRows, tableColumnRowData{Name: c.Name, Type: c.Type, Nullable: fmt.Sprintf("%t", c.Nullable), Comment: dashIfEmpty(c.Comment), Properties: mapJSON(c.Properties)})
			}
			panel = catalogWorkspacePanelData{
				Mode:     "view",
				Title:    selectedName,
				Subtitle: "View",
				MetaItems: []catalogWorkspaceMetaItemData{
					{Label: "Owner", Value: v.Owner},
					{Label: "Comment", Value: stringPtr(v.Comment)},
					{Label: "Properties", Value: mapJSON(v.Properties)},
					{Label: "Source tables", Value: stringsJoin(v.SourceTables)},
					{Label: "Updated", Value: formatTime(v.UpdatedAt)},
				},
				Definition:       v.ViewDefinition,
				Columns:          columnRows,
				ColumnsAvailable: columnsErr == nil,
			}
		}
	}

	renderHTML(w, http.StatusOK, catalogWorkspacePage(catalogWorkspacePageData{
		Principal:          principalFromContext(r.Context()),
		Catalogs:           sidebarCatalogs,
		ActiveCatalogName:  catalogName,
		SelectedSchemaName: selectedSchema,
		SelectedType:       selectedType,
		SelectedName:       selectedName,
		Schemas:            explorerSchemas,
		Panel:              panel,
		QuickFilterMessage: "Filter catalogs, schemas, tables, and views",
		CSRFField:          csrfFieldProvider(r),
	}))
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
	http.Redirect(w, r, "/ui/catalogs/"+name, http.StatusSeeOther)
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
	http.Redirect(w, r, "/ui/catalogs/"+name, http.StatusSeeOther)
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
	http.Redirect(w, r, "/ui/catalogs/"+catalogName, http.StatusSeeOther)
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
	http.Redirect(w, r, "/ui/catalogs/"+catalogName, http.StatusSeeOther)
}

func (h *Handler) CatalogSchemasDelete(w http.ResponseWriter, r *http.Request) {
	catalogName := chi.URLParam(r, "catalogName")
	schemaName := chi.URLParam(r, "schemaName")
	principal, _ := principalLabel(r.Context())
	if err := h.Catalog.DeleteSchema(r.Context(), catalogName, principal, schemaName, true); err != nil {
		h.renderServiceError(w, r, err)
		return
	}
	http.Redirect(w, r, "/ui/catalogs/"+catalogName, http.StatusSeeOther)
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
	query := q.Encode()
	base := "/ui/catalogs/" + url.PathEscape(catalogName)
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
