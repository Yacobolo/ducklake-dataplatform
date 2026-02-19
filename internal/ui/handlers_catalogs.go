package ui

import (
	"fmt"
	"net/http"
	"sort"
	"strconv"

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
	summary, _ := h.Catalog.GetMetastoreSummary(r.Context(), catalogName)
	schemas, _, _ := h.Catalog.ListSchemas(r.Context(), catalogName, domain.PageRequest{MaxResults: 20})

	schemaRows := make([]schemaRowData, 0, len(schemas))
	for i := range schemas {
		s := schemas[i]
		schemaPath := "/ui/catalogs/" + catalogName + "/schemas/" + s.Name
		schemaRows = append(schemaRows, schemaRowData{Name: s.Name, URL: schemaPath, Owner: s.Owner, Updated: formatTime(s.UpdatedAt), EditURL: "/ui/catalogs/" + catalogName + "/schemas/" + s.Name + "/edit", DeleteURL: "/ui/catalogs/" + catalogName + "/schemas/" + s.Name + "/delete"})
	}
	metastoreItems := []string{}
	if summary != nil {
		metastoreItems = append(metastoreItems,
			"Type: "+summary.MetastoreType,
			"Storage: "+summary.StorageBackend,
			"Schemas: "+strconv.FormatInt(summary.SchemaCount, 10),
			"Tables: "+strconv.FormatInt(summary.TableCount, 10),
		)
	}
	renderHTML(w, http.StatusOK, catalogDetailPage(catalogDetailPageData{
		Principal:      principalFromContext(r.Context()),
		CatalogName:    registration.Name,
		Status:         string(registration.Status),
		DataPath:       registration.DataPath,
		IsDefault:      fmt.Sprintf("%t", registration.IsDefault),
		EditURL:        "/ui/catalogs/" + registration.Name + "/edit",
		SetDefaultURL:  "/ui/catalogs/" + registration.Name + "/set-default",
		DeleteURL:      "/ui/catalogs/" + registration.Name + "/delete",
		NewSchemaURL:   "/ui/catalogs/" + registration.Name + "/schemas/new",
		MetastoreItems: metastoreItems,
		Schemas:        schemaRows,
		CSRFField:      csrfFieldProvider(r),
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

	schema, err := h.Catalog.GetSchema(r.Context(), catalogName, schemaName)
	if err != nil {
		h.renderServiceError(w, r, err)
		return
	}

	tables, _, err := h.Catalog.ListTables(r.Context(), catalogName, schemaName, domain.PageRequest{MaxResults: 200})
	if err != nil {
		h.renderServiceError(w, r, err)
		return
	}

	views, _, err := h.View.ListViews(r.Context(), catalogName, schemaName, domain.PageRequest{MaxResults: 200})
	if err != nil {
		h.renderServiceError(w, r, err)
		return
	}

	tableRows := make([]schemaTableRowData, 0, len(tables))
	for i := range tables {
		t := tables[i]
		tableRows = append(tableRows, schemaTableRowData{Name: t.Name, URL: "/ui/catalogs/" + catalogName + "/schemas/" + schemaName + "/tables/" + t.Name, Type: t.TableType, Owner: t.Owner, Updated: formatTime(t.UpdatedAt)})
	}

	viewRows := make([]schemaViewRowData, 0, len(views))
	for i := range views {
		v := views[i]
		viewRows = append(viewRows, schemaViewRowData{Name: v.Name, URL: "/ui/catalogs/" + catalogName + "/schemas/" + schemaName + "/views/" + v.Name, Owner: v.Owner, Updated: formatTime(v.UpdatedAt)})
	}
	renderHTML(w, http.StatusOK, schemaDetailPage(schemaDetailPageData{
		Principal:   principalFromContext(r.Context()),
		CatalogName: catalogName,
		SchemaName:  schemaName,
		Owner:       schema.Owner,
		Comment:     dashIfEmpty(schema.Comment),
		Properties:  mapJSON(schema.Properties),
		Tags:        tagsLabel(schema.Tags),
		BackURL:     "/ui/catalogs/" + catalogName,
		EditURL:     "/ui/catalogs/" + catalogName + "/schemas/" + schemaName + "/edit",
		DeleteURL:   "/ui/catalogs/" + catalogName + "/schemas/" + schemaName + "/delete",
		Tables:      tableRows,
		Views:       viewRows,
		CSRFField:   csrfFieldProvider(r),
	}))
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

	table, err := h.Catalog.GetTable(r.Context(), catalogName, schemaName, tableName)
	if err != nil {
		h.renderServiceError(w, r, err)
		return
	}

	columnRows := make([]tableColumnRowData, 0, len(table.Columns))
	for i := range table.Columns {
		c := table.Columns[i]
		columnRows = append(columnRows, tableColumnRowData{Name: c.Name, Type: c.Type, Nullable: fmt.Sprintf("%t", c.Nullable), Comment: dashIfEmpty(c.Comment), Properties: mapJSON(c.Properties)})
	}
	renderHTML(w, http.StatusOK, tableDetailPage(tableDetailPageData{
		Principal:  principalFromContext(r.Context()),
		Title:      "Table: " + catalogName + "." + schemaName + "." + tableName,
		Type:       table.TableType,
		Owner:      table.Owner,
		Comment:    dashIfEmpty(table.Comment),
		Properties: mapJSON(table.Properties),
		Tags:       tagsLabel(table.Tags),
		Updated:    formatTime(table.UpdatedAt),
		BackURL:    "/ui/catalogs/" + catalogName + "/schemas/" + schemaName,
		ColumnRows: columnRows,
	}))
}

func (h *Handler) CatalogViewsDetail(w http.ResponseWriter, r *http.Request) {
	catalogName := chi.URLParam(r, "catalogName")
	schemaName := chi.URLParam(r, "schemaName")
	viewName := chi.URLParam(r, "viewName")

	v, err := h.View.GetView(r.Context(), catalogName, schemaName, viewName)
	if err != nil {
		h.renderServiceError(w, r, err)
		return
	}

	columns, _, columnsErr := h.Catalog.ListColumns(r.Context(), catalogName, schemaName, viewName, domain.PageRequest{MaxResults: 200})
	columnRows := make([]tableColumnRowData, 0, len(columns))
	for i := range columns {
		c := columns[i]
		columnRows = append(columnRows, tableColumnRowData{Name: c.Name, Type: c.Type, Nullable: fmt.Sprintf("%t", c.Nullable), Comment: dashIfEmpty(c.Comment), Properties: mapJSON(c.Properties)})
	}
	renderHTML(w, http.StatusOK, viewDetailPage(viewDetailPageData{
		Principal:        principalFromContext(r.Context()),
		Title:            "View: " + catalogName + "." + schemaName + "." + viewName,
		Owner:            v.Owner,
		Comment:          stringPtr(v.Comment),
		Properties:       mapJSON(v.Properties),
		SourceTables:     stringsJoin(v.SourceTables),
		Updated:          formatTime(v.UpdatedAt),
		BackURL:          "/ui/catalogs/" + catalogName + "/schemas/" + schemaName,
		Definition:       v.ViewDefinition,
		ColumnRows:       columnRows,
		ColumnsAvailable: columnsErr == nil,
	}))
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
