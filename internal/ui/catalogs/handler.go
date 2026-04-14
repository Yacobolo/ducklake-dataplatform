package catalogs

import (
	"net/http"
	"net/url"
	"sort"
	"strings"

	"github.com/Yacobolo/quackstack/internal/domain"
	"github.com/Yacobolo/quackstack/internal/ui/core"

	"github.com/go-chi/chi/v5"

	. "maragu.dev/gomponents"
	. "maragu.dev/gomponents/html"
)

type Handler struct {
	deps *core.Dependencies
}

func New(deps *core.Dependencies) *Handler {
	return &Handler{deps: deps}
}

func (h *Handler) CatalogsList(w http.ResponseWriter, r *http.Request) {
	items, _, err := h.deps.CatalogRegistration.List(r.Context(), domain.PageRequest{MaxResults: 200})
	if err != nil {
		renderServiceError(w, err)
		return
	}
	if len(items) == 0 {
		core.RenderHTML(w, http.StatusOK, core.AppPage("Catalogs", "catalogs", core.PrincipalFromContext(r.Context()),
			Div(Class("rounded-xl border border-[var(--borderColor-default)] bg-[var(--bgColor-default)] p-4 shadow-xs"),
				H2(Class("m-0 text-xl font-semibold"), Text("Catalogs")),
				P(Class("m-0 text-sm text-[var(--fgColor-muted)]"), Text("No catalogs found yet.")),
				core.PrimaryLink("/ui/catalogs/new", "", Text("New catalog")),
			),
		))
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

	_ = core.TrackResourceVisit(r, h.deps, domain.ResourceRef{
		ResourceType: "workspace",
		ResourceKey:  "catalogs",
		DisplayName:  "Catalogs",
		Section:      "Discover",
	})
	h.renderCatalogWorkspace(w, r, items, selectedCatalog)
}

func (h *Handler) CatalogsDetail(w http.ResponseWriter, r *http.Request) {
	catalogName := chi.URLParam(r, "catalogName")
	q := r.URL.Query()
	q.Set("catalog", catalogName)
	http.Redirect(w, r, "/ui/catalogs?"+q.Encode(), http.StatusSeeOther)
}

func (h *Handler) CatalogsNew(w http.ResponseWriter, r *http.Request) {
	core.RenderHTML(w, http.StatusOK, catalogsNewPage(core.PrincipalFromContext(r.Context()), h.deps.CSRFFieldProvider(r)))
}

func (h *Handler) CatalogsCreate(w http.ResponseWriter, r *http.Request) {
	if !parseFormOrRenderBadRequest(w, r) {
		return
	}
	_, err := h.deps.CatalogRegistration.Register(r.Context(), domain.CreateCatalogRequest{
		Name:          formString(r.Form, "name"),
		MetastoreType: formString(r.Form, "metastore_type"),
		DSN:           formString(r.Form, "dsn"),
		DataPath:      formString(r.Form, "data_path"),
		Comment:       formString(r.Form, "comment"),
	})
	if err != nil {
		renderServiceError(w, err)
		return
	}
	http.Redirect(w, r, "/ui/catalogs", http.StatusSeeOther)
}

func (h *Handler) CatalogsEdit(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "catalogName")
	c, err := h.deps.CatalogRegistration.Get(r.Context(), name)
	if err != nil {
		renderServiceError(w, err)
		return
	}
	core.RenderHTML(w, http.StatusOK, catalogsEditPage(core.PrincipalFromContext(r.Context()), name, c, h.deps.CSRFFieldProvider(r)))
}

func (h *Handler) CatalogsUpdate(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "catalogName")
	if !parseFormOrRenderBadRequest(w, r) {
		return
	}
	_, err := h.deps.CatalogRegistration.Update(r.Context(), name, domain.UpdateCatalogRegistrationRequest{
		Comment:  formOptionalString(r.Form, "comment"),
		DataPath: formOptionalString(r.Form, "data_path"),
		DSN:      formOptionalString(r.Form, "dsn"),
	})
	if err != nil {
		renderServiceError(w, err)
		return
	}
	http.Redirect(w, r, catalogExplorerURL(name, "", "catalog", ""), http.StatusSeeOther)
}

func (h *Handler) CatalogsDelete(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "catalogName")
	if err := h.deps.CatalogRegistration.Delete(r.Context(), name); err != nil {
		renderServiceError(w, err)
		return
	}
	http.Redirect(w, r, "/ui/catalogs", http.StatusSeeOther)
}

func (h *Handler) CatalogsSetDefault(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "catalogName")
	if _, err := h.deps.CatalogRegistration.SetDefault(r.Context(), name); err != nil {
		renderServiceError(w, err)
		return
	}
	http.Redirect(w, r, catalogExplorerURL(name, "", "catalog", ""), http.StatusSeeOther)
}

func (h *Handler) CatalogSchemasNew(w http.ResponseWriter, r *http.Request) {
	catalogName := chi.URLParam(r, "catalogName")
	core.RenderHTML(w, http.StatusOK, catalogSchemasNewPage(core.PrincipalFromContext(r.Context()), catalogName, h.deps.CSRFFieldProvider(r)))
}

func (h *Handler) CatalogSchemasCreate(w http.ResponseWriter, r *http.Request) {
	catalogName := chi.URLParam(r, "catalogName")
	principal, _ := principalLabel(r)
	if !parseFormOrRenderBadRequest(w, r) {
		return
	}
	_, err := h.deps.Catalog.CreateSchema(r.Context(), catalogName, principal, domain.CreateSchemaRequest{
		Name:         formString(r.Form, "name"),
		Comment:      formString(r.Form, "comment"),
		LocationName: formString(r.Form, "location_name"),
	})
	if err != nil {
		renderServiceError(w, err)
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
	s, err := h.deps.Catalog.GetSchema(r.Context(), catalogName, schemaName)
	if err != nil {
		renderServiceError(w, err)
		return
	}
	core.RenderHTML(w, http.StatusOK, catalogSchemasEditPage(core.PrincipalFromContext(r.Context()), catalogName, schemaName, s, h.deps.CSRFFieldProvider(r)))
}

func (h *Handler) CatalogSchemasUpdate(w http.ResponseWriter, r *http.Request) {
	catalogName := chi.URLParam(r, "catalogName")
	schemaName := chi.URLParam(r, "schemaName")
	principal, _ := principalLabel(r)
	if !parseFormOrRenderBadRequest(w, r) {
		return
	}
	_, err := h.deps.Catalog.UpdateSchema(r.Context(), catalogName, principal, schemaName, domain.UpdateSchemaRequest{
		Comment: formOptionalString(r.Form, "comment"),
	})
	if err != nil {
		renderServiceError(w, err)
		return
	}
	http.Redirect(w, r, catalogExplorerURL(catalogName, schemaName, "schema", ""), http.StatusSeeOther)
}

func (h *Handler) CatalogSchemasDelete(w http.ResponseWriter, r *http.Request) {
	catalogName := chi.URLParam(r, "catalogName")
	schemaName := chi.URLParam(r, "schemaName")
	principal, _ := principalLabel(r)
	if err := h.deps.Catalog.DeleteSchema(r.Context(), catalogName, principal, schemaName, true); err != nil {
		renderServiceError(w, err)
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

func parseFormOrRenderBadRequest(w http.ResponseWriter, r *http.Request) bool {
	if err := r.ParseForm(); err != nil {
		core.RenderHTML(w, http.StatusBadRequest, core.ErrorPage("Invalid Request", "Unable to parse form."))
		return false
	}
	return true
}

func formString(values map[string][]string, key string) string {
	if values == nil {
		return ""
	}
	return strings.TrimSpace(first(values[key]))
}

func formOptionalString(values map[string][]string, key string) *string {
	v := formString(values, key)
	if v == "" {
		return nil
	}
	return &v
}

func first(values []string) string {
	if len(values) == 0 {
		return ""
	}
	return values[0]
}

func principalLabel(r *http.Request) (string, bool) {
	p := core.PrincipalFromContext(r.Context())
	if p.Name == "" {
		return "unknown", p.IsAdmin
	}
	return p.Name, p.IsAdmin
}

func renderServiceError(w http.ResponseWriter, err error) {
	status, message := core.ServiceErrorStatus(err)
	title := "Unexpected Error"
	switch status {
	case http.StatusNotFound:
		title = "Not Found"
	case http.StatusForbidden:
		title = "Access Denied"
	case http.StatusBadRequest:
		title = "Invalid Request"
	case http.StatusConflict:
		title = "Conflict"
	}
	core.RenderHTML(w, status, core.ErrorPage(title, message))
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
	return strings.Join(values, ", ")
}
