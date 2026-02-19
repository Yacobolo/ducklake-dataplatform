package ui

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"duck-demo/internal/domain"
	"duck-demo/internal/service/query"

	gomponents "maragu.dev/gomponents"
	html "maragu.dev/gomponents/html"
)

const sqlEditorMaxRows = 200
const sqlEditorCSVMaxRows = 5000

func (h *Handler) SQLEditorPage(w http.ResponseWriter, r *http.Request) {
	state := h.sqlEditorState(r, nil)
	sqlText := strings.TrimSpace(r.URL.Query().Get("sql"))
	if sqlText == "" {
		sqlText = defaultSQLSnippet(r.URL.Query().Get("snippet"), state.SelectedCatalog, state.SelectedSchema)
	}
	h.renderSQLEditor(w, r, sqlText, nil, "", state)
}

func (h *Handler) SQLEditorRun(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		renderHTML(w, http.StatusBadRequest, errorPage("Invalid Request", "Unable to parse form."))
		return
	}

	sqlText := strings.TrimSpace(r.Form.Get("sql"))
	state := h.sqlEditorState(r, r.Form)
	principal, _ := principalLabel(r.Context())
	result, err := h.Query.Execute(r.Context(), principal, sqlText)
	if err != nil {
		h.renderSQLEditor(w, r, sqlText, nil, err.Error(), state)
		return
	}

	h.renderSQLEditor(w, r, sqlText, result, "", state)
}

func (h *Handler) SQLEditorDownloadCSV(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		renderHTML(w, http.StatusBadRequest, errorPage("Invalid Request", "Unable to parse form."))
		return
	}

	sqlText := strings.TrimSpace(r.Form.Get("sql"))
	state := h.sqlEditorState(r, r.Form)
	principal, _ := principalLabel(r.Context())
	result, err := h.Query.Execute(r.Context(), principal, sqlText)
	if err != nil {
		h.renderSQLEditor(w, r, sqlText, nil, err.Error(), state)
		return
	}

	rows := result.Rows
	if len(rows) > sqlEditorCSVMaxRows {
		rows = rows[:sqlEditorCSVMaxRows]
	}

	var buf bytes.Buffer
	writer := csv.NewWriter(&buf)
	if err := writer.Write(result.Columns); err != nil {
		renderHTML(w, http.StatusInternalServerError, errorPage("Export Failed", "Failed writing CSV header."))
		return
	}
	for i := range rows {
		record := make([]string, 0, len(rows[i]))
		for j := range rows[i] {
			record = append(record, sqlCellString(rows[i][j]))
		}
		if err := writer.Write(record); err != nil {
			renderHTML(w, http.StatusInternalServerError, errorPage("Export Failed", "Failed writing CSV rows."))
			return
		}
	}
	writer.Flush()
	if err := writer.Error(); err != nil {
		renderHTML(w, http.StatusInternalServerError, errorPage("Export Failed", "Failed finalizing CSV."))
		return
	}

	filename := "query-results.csv"
	if state.SelectedCatalog != "" && state.SelectedSchema != "" {
		filename = fmt.Sprintf("%s_%s_results.csv", state.SelectedCatalog, state.SelectedSchema)
	}
	w.Header().Set("Content-Type", "text/csv; charset=utf-8")
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%q", filename))
	if len(result.Rows) > sqlEditorCSVMaxRows {
		w.Header().Set("X-Duck-Results-Truncated", "true")
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(buf.Bytes())
}

func (h *Handler) renderSQLEditor(w http.ResponseWriter, r *http.Request, sqlText string, result *query.QueryResult, runError string, state sqlEditorContext) {
	p := principalFromContext(r.Context())
	resultNode := gomponents.Node(html.P(html.Class(mutedClass()), gomponents.Text("Run a query to see results.")))

	if runError != "" {
		resultNode = html.Div(
			html.Class(cardClass()),
			html.H2(gomponents.Text("Query Error")),
			html.Pre(gomponents.Text(runError)),
		)
	} else if result != nil {
		headerCols := make([]gomponents.Node, 0, len(result.Columns))
		for i := range result.Columns {
			headerCols = append(headerCols, html.Th(gomponents.Text(result.Columns[i])))
		}

		displayRows := result.Rows
		truncated := false
		if len(displayRows) > sqlEditorMaxRows {
			displayRows = displayRows[:sqlEditorMaxRows]
			truncated = true
		}

		rows := make([]gomponents.Node, 0, len(displayRows))
		for i := range displayRows {
			cells := make([]gomponents.Node, 0, len(displayRows[i]))
			for j := range displayRows[i] {
				cells = append(cells, html.Td(gomponents.Text(sqlCellString(displayRows[i][j]))))
			}
			rows = append(rows, html.Tr(gomponents.Group(cells)))
		}

		meta := fmt.Sprintf("%d row(s)", result.RowCount)
		if truncated {
			meta = fmt.Sprintf("%d row(s), showing first %d", result.RowCount, sqlEditorMaxRows)
		}

		resultNode = html.Div(
			html.Class(cardClass("table-wrap")),
			html.H2(gomponents.Text("Results")),
			html.P(html.Class(mutedClass()), gomponents.Text(meta)),
			html.Table(
				html.THead(html.Tr(gomponents.Group(headerCols))),
				html.TBody(gomponents.Group(rows)),
			),
		)
	}

	snippetsNode := snippetLinks(state.SelectedCatalog, state.SelectedSchema)
	catalogOptions := make([]gomponents.Node, 0, len(state.Catalogs)+1)
	catalogOptions = append(catalogOptions, optionSelectedValue("", state.SelectedCatalog, "(choose catalog)"))
	for i := range state.Catalogs {
		catalogOptions = append(catalogOptions, optionSelectedValue(state.Catalogs[i].Name, state.SelectedCatalog, state.Catalogs[i].Name))
	}

	schemaOptions := make([]gomponents.Node, 0, len(state.Schemas)+1)
	schemaOptions = append(schemaOptions, optionSelectedValue("", state.SelectedSchema, "(choose schema)"))
	for i := range state.Schemas {
		schemaOptions = append(schemaOptions, optionSelectedValue(state.Schemas[i].Name, state.SelectedSchema, state.Schemas[i].Name))
	}

	renderHTML(w, http.StatusOK, appPage(
		"SQL Editor",
		"sql",
		p,
		html.Div(
			html.Class(cardClass()),
			html.Form(
				html.Method("get"),
				html.Action("/ui/sql"),
				html.Label(gomponents.Text("Catalog")),
				html.Select(html.Name("catalog"), gomponents.Group(catalogOptions)),
				html.Label(gomponents.Text("Schema")),
				html.Select(html.Name("schema"), gomponents.Group(schemaOptions)),
				html.Div(html.Class("button-row"), html.Button(html.Type("submit"), html.Class(secondaryButtonClass()), gomponents.Text("Set context"))),
			),
			html.P(html.Class(mutedClass()), gomponents.Text("Context does not rewrite SQL automatically. It powers snippets and export naming.")),
			html.H2(gomponents.Text("Default snippets")),
			snippetsNode,
		),
		html.Div(
			html.Class(cardClass()),
			html.Form(
				html.Method("post"),
				html.Action("/ui/sql/run"),
				csrfField(r),
				html.Input(html.Type("hidden"), html.Name("catalog"), html.Value(state.SelectedCatalog)),
				html.Input(html.Type("hidden"), html.Name("schema"), html.Value(state.SelectedSchema)),
				html.Label(gomponents.Text("SQL")),
				html.Textarea(html.Name("sql"), html.Required(), gomponents.Text(sqlText)),
				html.Div(
					html.Class("button-row"),
					html.Button(html.Type("submit"), html.Class(primaryButtonClass()), gomponents.Text("Run query")),
					html.Button(html.Type("submit"), html.Class(secondaryButtonClass()), html.FormAction("/ui/sql/download.csv"), gomponents.Text("Download CSV")),
				),
			),
		),
		resultNode,
	))
}

type sqlEditorContext struct {
	SelectedCatalog string
	SelectedSchema  string
	Catalogs        []domain.CatalogRegistration
	Schemas         []domain.SchemaDetail
}

func (h *Handler) sqlEditorState(r *http.Request, form url.Values) sqlEditorContext {
	selectedCatalog := strings.TrimSpace(r.URL.Query().Get("catalog"))
	selectedSchema := strings.TrimSpace(r.URL.Query().Get("schema"))
	if form != nil {
		if c := strings.TrimSpace(form.Get("catalog")); c != "" {
			selectedCatalog = c
		}
		if s := strings.TrimSpace(form.Get("schema")); s != "" {
			selectedSchema = s
		}
	}

	catalogs, _, err := h.CatalogRegistration.List(r.Context(), domain.PageRequest{MaxResults: 100})
	if err != nil {
		catalogs = nil
	}
	if selectedCatalog == "" && len(catalogs) > 0 {
		selectedCatalog = catalogs[0].Name
	}

	var schemas []domain.SchemaDetail
	if selectedCatalog != "" {
		s, _, err := h.Catalog.ListSchemas(r.Context(), selectedCatalog, domain.PageRequest{MaxResults: 200})
		if err == nil {
			schemas = s
		}
	}
	if selectedSchema == "" && len(schemas) > 0 {
		selectedSchema = schemas[0].Name
	}

	return sqlEditorContext{
		SelectedCatalog: selectedCatalog,
		SelectedSchema:  selectedSchema,
		Catalogs:        catalogs,
		Schemas:         schemas,
	}
}

func optionSelectedValue(value, selected, label string) gomponents.Node {
	if value == selected {
		return html.Option(html.Value(value), html.Selected(), gomponents.Text(label))
	}
	return html.Option(html.Value(value), gomponents.Text(label))
}

func snippetLinks(catalogName, schemaName string) gomponents.Node {
	snippets := []struct {
		ID    string
		Label string
	}{
		{ID: "show_tables", Label: "Show tables"},
		{ID: "show_views", Label: "Show views"},
		{ID: "describe_table", Label: "Describe table"},
		{ID: "sample_rows", Label: "Sample rows"},
	}

	links := make([]gomponents.Node, 0, len(snippets))
	for i := range snippets {
		q := url.Values{}
		q.Set("snippet", snippets[i].ID)
		if catalogName != "" {
			q.Set("catalog", catalogName)
		}
		if schemaName != "" {
			q.Set("schema", schemaName)
		}
		links = append(links, html.A(html.Href("/ui/sql?"+q.Encode()), gomponents.Text(snippets[i].Label)))
	}
	return html.Div(html.Class("snippet-list"), gomponents.Group(links))
}

func defaultSQLSnippet(snippetID, catalogName, schemaName string) string {
	qualifiedSchema := schemaName
	if catalogName != "" && schemaName != "" {
		qualifiedSchema = catalogName + "." + schemaName
	}
	schemaFilter := schemaName
	if schemaFilter == "" {
		schemaFilter = "main"
	}

	switch snippetID {
	case "show_tables":
		return fmt.Sprintf("SELECT table_name\nFROM information_schema.tables\nWHERE table_schema = '%s'\nORDER BY table_name;", schemaFilter)
	case "show_views":
		return fmt.Sprintf("SELECT table_name\nFROM information_schema.views\nWHERE table_schema = '%s'\nORDER BY table_name;", schemaFilter)
	case "describe_table":
		if qualifiedSchema != "" {
			return fmt.Sprintf("DESCRIBE SELECT * FROM %s.<table_name>;", qualifiedSchema)
		}
		return "DESCRIBE SELECT * FROM <schema_name>.<table_name>;"
	case "sample_rows":
		if qualifiedSchema != "" {
			return fmt.Sprintf("SELECT *\nFROM %s.<table_name>\nLIMIT 50;", qualifiedSchema)
		}
		return "SELECT *\nFROM <schema_name>.<table_name>\nLIMIT 50;"
	default:
		return ""
	}
}

func sqlCellString(value interface{}) string {
	if value == nil {
		return "NULL"
	}
	return fmt.Sprintf("%v", value)
}
