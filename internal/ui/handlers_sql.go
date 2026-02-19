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
	if !parseFormOrRenderBadRequest(w, r) {
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
	if !parseFormOrRenderBadRequest(w, r) {
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
	renderHTML(w, http.StatusOK, sqlEditorPage(principalFromContext(r.Context()), sqlText, result, runError, state, func() gomponents.Node { return csrfField(r) }))
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
