package ui

import (
	"fmt"
	"net/http"
	"sort"

	"github.com/go-chi/chi/v5"

	"duck-demo/internal/domain"

	gomponents "maragu.dev/gomponents"
	html "maragu.dev/gomponents/html"
)

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

	tableRows := make([]gomponents.Node, 0, len(tables))
	for i := range tables {
		t := tables[i]
		tableRows = append(tableRows, html.Tr(
			html.Td(html.A(html.Href("/ui/catalogs/"+catalogName+"/schemas/"+schemaName+"/tables/"+t.Name), gomponents.Text(t.Name))),
			html.Td(gomponents.Text(t.TableType)),
			html.Td(gomponents.Text(t.Owner)),
			html.Td(gomponents.Text(formatTime(t.UpdatedAt))),
		))
	}

	viewRows := make([]gomponents.Node, 0, len(views))
	for i := range views {
		v := views[i]
		viewRows = append(viewRows, html.Tr(
			html.Td(html.A(html.Href("/ui/catalogs/"+catalogName+"/schemas/"+schemaName+"/views/"+v.Name), gomponents.Text(v.Name))),
			html.Td(gomponents.Text(v.Owner)),
			html.Td(gomponents.Text(formatTime(v.UpdatedAt))),
		))
	}

	p := principalFromContext(r.Context())
	renderHTML(w, http.StatusOK, appPage(
		"Schema: "+catalogName+"."+schemaName,
		"catalogs",
		p,
		html.Div(
			html.Class(cardClass()),
			html.P(gomponents.Text("Owner: "+schema.Owner)),
			html.P(gomponents.Text("Comment: "+dashIfEmpty(schema.Comment))),
			html.P(gomponents.Text("Properties: "+mapJSON(schema.Properties))),
			html.P(gomponents.Text("Tags: "+tagsLabel(schema.Tags))),
			html.A(html.Href("/ui/catalogs/"+catalogName), gomponents.Text("<- Back to catalog")),
			html.A(html.Href("/ui/catalogs/"+catalogName+"/schemas/"+schemaName+"/edit"), gomponents.Text("Edit schema")),
			html.Form(
				html.Method("post"),
				html.Action("/ui/catalogs/"+catalogName+"/schemas/"+schemaName+"/delete"),
				csrfField(r),
				html.Button(html.Type("submit"), html.Class(secondaryButtonClass()), gomponents.Text("Delete schema")),
			),
		),
		html.Div(
			html.Class(cardClass("table-wrap")),
			html.H2(gomponents.Text("Tables")),
			html.Table(
				html.THead(html.Tr(html.Th(gomponents.Text("Name")), html.Th(gomponents.Text("Type")), html.Th(gomponents.Text("Owner")), html.Th(gomponents.Text("Updated")))),
				html.TBody(gomponents.Group(tableRows)),
			),
		),
		html.Div(
			html.Class(cardClass("table-wrap")),
			html.H2(gomponents.Text("Views")),
			html.Table(
				html.THead(html.Tr(html.Th(gomponents.Text("Name")), html.Th(gomponents.Text("Owner")), html.Th(gomponents.Text("Updated")))),
				html.TBody(gomponents.Group(viewRows)),
			),
		),
	))
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

	columnRows := make([]gomponents.Node, 0, len(table.Columns))
	for i := range table.Columns {
		c := table.Columns[i]
		columnRows = append(columnRows, html.Tr(
			html.Td(gomponents.Text(c.Name)),
			html.Td(gomponents.Text(c.Type)),
			html.Td(gomponents.Text(fmt.Sprintf("%t", c.Nullable))),
			html.Td(gomponents.Text(dashIfEmpty(c.Comment))),
			html.Td(gomponents.Text(mapJSON(c.Properties))),
		))
	}

	p := principalFromContext(r.Context())
	renderHTML(w, http.StatusOK, appPage(
		"Table: "+catalogName+"."+schemaName+"."+tableName,
		"catalogs",
		p,
		html.Div(
			html.Class(cardClass()),
			html.P(gomponents.Text("Type: "+table.TableType)),
			html.P(gomponents.Text("Owner: "+table.Owner)),
			html.P(gomponents.Text("Comment: "+dashIfEmpty(table.Comment))),
			html.P(gomponents.Text("Properties: "+mapJSON(table.Properties))),
			html.P(gomponents.Text("Tags: "+tagsLabel(table.Tags))),
			html.P(gomponents.Text("Updated: "+formatTime(table.UpdatedAt))),
			html.A(html.Href("/ui/catalogs/"+catalogName+"/schemas/"+schemaName), gomponents.Text("<- Back to schema")),
		),
		html.Div(
			html.Class(cardClass("table-wrap")),
			html.H2(gomponents.Text("Columns")),
			html.Table(
				html.THead(html.Tr(html.Th(gomponents.Text("Name")), html.Th(gomponents.Text("Type")), html.Th(gomponents.Text("Nullable")), html.Th(gomponents.Text("Comment")), html.Th(gomponents.Text("Properties")))),
				html.TBody(gomponents.Group(columnRows)),
			),
		),
	))
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
	columnRows := make([]gomponents.Node, 0, len(columns))
	for i := range columns {
		c := columns[i]
		columnRows = append(columnRows, html.Tr(
			html.Td(gomponents.Text(c.Name)),
			html.Td(gomponents.Text(c.Type)),
			html.Td(gomponents.Text(fmt.Sprintf("%t", c.Nullable))),
			html.Td(gomponents.Text(dashIfEmpty(c.Comment))),
			html.Td(gomponents.Text(mapJSON(c.Properties))),
		))
	}

	columnSection := gomponents.Node(html.P(html.Class(mutedClass()), gomponents.Text("Columns unavailable for this view.")))
	if columnsErr == nil {
		columnSection = html.Table(
			html.THead(html.Tr(html.Th(gomponents.Text("Name")), html.Th(gomponents.Text("Type")), html.Th(gomponents.Text("Nullable")), html.Th(gomponents.Text("Comment")), html.Th(gomponents.Text("Properties")))),
			html.TBody(gomponents.Group(columnRows)),
		)
	}

	p := principalFromContext(r.Context())
	renderHTML(w, http.StatusOK, appPage(
		"View: "+catalogName+"."+schemaName+"."+viewName,
		"catalogs",
		p,
		html.Div(
			html.Class(cardClass()),
			html.P(gomponents.Text("Owner: "+v.Owner)),
			html.P(gomponents.Text("Comment: "+stringPtr(v.Comment))),
			html.P(gomponents.Text("Properties: "+mapJSON(v.Properties))),
			html.P(gomponents.Text("Tags: -")),
			html.P(gomponents.Text("Source tables: "+stringsJoin(v.SourceTables))),
			html.P(gomponents.Text("Updated: "+formatTime(v.UpdatedAt))),
			html.A(html.Href("/ui/catalogs/"+catalogName+"/schemas/"+schemaName), gomponents.Text("<- Back to schema")),
		),
		html.Div(html.Class(cardClass()), html.H2(gomponents.Text("Definition")), html.Pre(gomponents.Text(v.ViewDefinition))),
		html.Div(html.Class(cardClass("table-wrap")), html.H2(gomponents.Text("Columns")), columnSection),
	))
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
