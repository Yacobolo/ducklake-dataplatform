package legacy

import (
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/go-chi/chi/v5"

	"duck-demo/internal/domain"
)

func (h *Handler) ProductsList(w http.ResponseWriter, r *http.Request) {
	if h.Product == nil {
		renderHTML(w, http.StatusNotFound, errorPage("Not Found", "Products UI is not configured."))
		return
	}

	pageReq := pageFromRequest(r, 24)
	query := strings.TrimSpace(r.URL.Query().Get("q"))
	var filterQuery *string
	if query != "" {
		filterQuery = &query
	}
	items, total, err := h.Product.ListProducts(r.Context(), domain.DataProductFilter{
		Query: filterQuery,
		Page:  pageReq,
	})
	if err != nil {
		h.renderServiceError(w, r, err)
		return
	}

	renderHTML(w, http.StatusOK, productsListPage(principalFromContext(r.Context()), items, pageReq, total, query))
}

func (h *Handler) ProductsDetail(w http.ResponseWriter, r *http.Request) {
	if h.Product == nil {
		renderHTML(w, http.StatusNotFound, errorPage("Not Found", "Products UI is not configured."))
		return
	}

	product, err := h.Product.GetProduct(r.Context(), chi.URLParam(r, "productSlug"))
	if err != nil {
		h.renderServiceError(w, r, err)
		return
	}

	renderHTML(w, http.StatusOK, productDetailPage(principalFromContext(r.Context()), product))
}

func (h *Handler) ProductsVersionDetail(w http.ResponseWriter, r *http.Request) {
	if h.Product == nil {
		renderHTML(w, http.StatusNotFound, errorPage("Not Found", "Products UI is not configured."))
		return
	}

	product, err := h.Product.GetProduct(r.Context(), chi.URLParam(r, "productSlug"))
	if err != nil {
		h.renderServiceError(w, r, err)
		return
	}
	version := parseIntOrDefault(chi.URLParam(r, "version"), 1)
	versionDetail, err := h.Product.GetVersion(r.Context(), chi.URLParam(r, "productSlug"), version)
	if err != nil {
		h.renderServiceError(w, r, err)
		return
	}
	renderHTML(w, http.StatusOK, productVersionPage(principalFromContext(r.Context()), product, versionDetail))
}

func (h *Handler) ProductsNew(w http.ResponseWriter, r *http.Request) {
	if h.Product == nil {
		renderHTML(w, http.StatusNotFound, errorPage("Not Found", "Products UI is not configured."))
		return
	}

	renderHTML(w, http.StatusOK, productNewPage(principalFromContext(r.Context()), csrfFieldProvider(r)))
}

func (h *Handler) ProductsCreate(w http.ResponseWriter, r *http.Request) {
	if h.Product == nil {
		renderHTML(w, http.StatusNotFound, errorPage("Not Found", "Products UI is not configured."))
		return
	}
	if err := r.ParseForm(); err != nil {
		renderHTML(w, http.StatusBadRequest, errorPage("Invalid Request", "Unable to parse product form."))
		return
	}

	var primaryAssetKey *string
	if value := strings.TrimSpace(r.FormValue("primary_asset_key")); value != "" {
		primaryAssetKey = &value
	}

	product, err := h.Product.CreateProduct(r.Context(), domain.CreateDataProductRequest{
		Slug:              strings.TrimSpace(r.FormValue("slug")),
		Name:              strings.TrimSpace(r.FormValue("name")),
		Description:       strings.TrimSpace(r.FormValue("description")),
		DomainName:        strings.TrimSpace(r.FormValue("domain_name")),
		TeamName:          strings.TrimSpace(r.FormValue("team_name")),
		StewardPrincipal:  strings.TrimSpace(r.FormValue("steward_principal")),
		ContactChannel:    strings.TrimSpace(r.FormValue("contact_channel")),
		Visibility:        strings.TrimSpace(r.FormValue("visibility")),
		ConsumerAudience:  strings.TrimSpace(r.FormValue("consumer_audience")),
		DocsURL:           strings.TrimSpace(r.FormValue("docs_url")),
		AccessRequestPath: strings.TrimSpace(r.FormValue("access_request_path")),
		Contract: domain.ProductContract{
			DataGrain:            strings.TrimSpace(r.FormValue("data_grain")),
			UpdateCadence:        strings.TrimSpace(r.FormValue("update_cadence")),
			RetentionWindow:      strings.TrimSpace(r.FormValue("retention_window")),
			BreakingChangePolicy: strings.TrimSpace(r.FormValue("breaking_change_policy")),
		},
		SLO: domain.ProductSLO{
			FreshnessSLO: strings.TrimSpace(r.FormValue("freshness_slo")),
			LatencySLO:   strings.TrimSpace(r.FormValue("latency_slo")),
		},
		PrimaryAssetKey:   primaryAssetKey,
		SemanticModelRefs: splitCSV(r.FormValue("semantic_model_refs")),
		CreatedBy:         principalFromContext(r.Context()).Name,
	})
	if err != nil {
		h.renderServiceError(w, r, err)
		return
	}

	http.Redirect(w, r, "/ui/products/"+url.PathEscape(product.Product.Slug), http.StatusSeeOther)
}

func (h *Handler) ProductsCreateVersion(w http.ResponseWriter, r *http.Request) {
	if h.Product == nil {
		renderHTML(w, http.StatusNotFound, errorPage("Not Found", "Products UI is not configured."))
		return
	}
	if err := r.ParseForm(); err != nil {
		renderHTML(w, http.StatusBadRequest, errorPage("Invalid Request", "Unable to parse version form."))
		return
	}
	outputAssetKeys := splitCSV(r.FormValue("output_asset_keys"))
	product, err := h.Product.CreateVersion(r.Context(), chi.URLParam(r, "productSlug"), domain.CreateDataProductVersionRequest{
		CompatibilityLevel: strings.TrimSpace(r.FormValue("compatibility_level")),
		Contract: domain.ProductContract{
			DataGrain:            strings.TrimSpace(r.FormValue("data_grain")),
			UpdateCadence:        strings.TrimSpace(r.FormValue("update_cadence")),
			RetentionWindow:      strings.TrimSpace(r.FormValue("retention_window")),
			BreakingChangePolicy: strings.TrimSpace(r.FormValue("breaking_change_policy")),
		},
		SLO: domain.ProductSLO{
			FreshnessSLO: strings.TrimSpace(r.FormValue("freshness_slo")),
			LatencySLO:   strings.TrimSpace(r.FormValue("latency_slo")),
		},
		DocsURL:           strings.TrimSpace(r.FormValue("docs_url")),
		AccessRequestPath: strings.TrimSpace(r.FormValue("access_request_path")),
		OutputAssetKeys:   outputAssetKeys,
		SemanticModelRefs: splitCSV(r.FormValue("semantic_model_refs")),
		CreatedBy:         principalFromContext(r.Context()).Name,
	})
	if err != nil {
		h.renderServiceError(w, r, err)
		return
	}
	http.Redirect(w, r, "/ui/products/"+url.PathEscape(product.Product.Slug), http.StatusSeeOther)
}

func (h *Handler) ProductsPublish(w http.ResponseWriter, r *http.Request) {
	h.productsMutateVersionState(w, r, "publish")
}

func (h *Handler) ProductsDeprecate(w http.ResponseWriter, r *http.Request) {
	h.productsMutateVersionState(w, r, "deprecate")
}

func (h *Handler) ProductsRetire(w http.ResponseWriter, r *http.Request) {
	h.productsMutateVersionState(w, r, "retire")
}

func (h *Handler) ProductsAddDependency(w http.ResponseWriter, r *http.Request) {
	if h.Product == nil {
		renderHTML(w, http.StatusNotFound, errorPage("Not Found", "Products UI is not configured."))
		return
	}
	if err := r.ParseForm(); err != nil {
		renderHTML(w, http.StatusBadRequest, errorPage("Invalid Request", "Unable to parse dependency form."))
		return
	}
	product, err := h.Product.AddDependency(r.Context(), chi.URLParam(r, "productSlug"), strings.TrimSpace(r.FormValue("depends_on_slug")))
	if err != nil {
		h.renderServiceError(w, r, err)
		return
	}
	http.Redirect(w, r, "/ui/products/"+url.PathEscape(product.Product.Slug), http.StatusSeeOther)
}

func (h *Handler) ProductsSubscribe(w http.ResponseWriter, r *http.Request) {
	if h.Product == nil {
		renderHTML(w, http.StatusNotFound, errorPage("Not Found", "Products UI is not configured."))
		return
	}
	if err := r.ParseForm(); err != nil {
		renderHTML(w, http.StatusBadRequest, errorPage("Invalid Request", "Unable to parse subscription form."))
		return
	}
	_, err := h.Product.Subscribe(
		r.Context(),
		chi.URLParam(r, "productSlug"),
		principalFromContext(r.Context()).Name,
		strings.TrimSpace(r.FormValue("event_type")),
		strings.TrimSpace(r.FormValue("channel")),
	)
	if err != nil {
		h.renderServiceError(w, r, err)
		return
	}
	http.Redirect(w, r, "/ui/products/"+url.PathEscape(chi.URLParam(r, "productSlug")), http.StatusSeeOther)
}

func (h *Handler) productsMutateVersionState(w http.ResponseWriter, r *http.Request, action string) {
	if h.Product == nil {
		renderHTML(w, http.StatusNotFound, errorPage("Not Found", "Products UI is not configured."))
		return
	}
	if err := r.ParseForm(); err != nil {
		renderHTML(w, http.StatusBadRequest, errorPage("Invalid Request", "Unable to parse product action form."))
		return
	}
	version := parseIntOrDefault(r.FormValue("version"), 1)
	var (
		product *domain.DataProductDetail
		err     error
	)
	switch action {
	case "publish":
		product, err = h.Product.PublishVersion(r.Context(), chi.URLParam(r, "productSlug"), version)
	case "deprecate":
		var replacementSlug *string
		if value := strings.TrimSpace(r.FormValue("replacement_slug")); value != "" {
			replacementSlug = &value
		}
		product, err = h.Product.DeprecateVersion(r.Context(), chi.URLParam(r, "productSlug"), version, replacementSlug)
	case "retire":
		product, err = h.Product.RetireVersion(r.Context(), chi.URLParam(r, "productSlug"), version)
	default:
		err = domain.ErrValidation("unsupported action %q", action)
	}
	if err != nil {
		h.renderServiceError(w, r, err)
		return
	}
	http.Redirect(w, r, "/ui/products/"+url.PathEscape(product.Product.Slug), http.StatusSeeOther)
}

func splitCSV(value string) []string {
	parts := strings.Split(value, ",")
	out := make([]string, 0, len(parts))
	for i := range parts {
		part := strings.TrimSpace(parts[i])
		if part == "" {
			continue
		}
		out = append(out, part)
	}
	return out
}

func parseIntOrDefault(value string, fallback int) int {
	parsed, err := strconv.Atoi(strings.TrimSpace(value))
	if err != nil || parsed <= 0 {
		return fallback
	}
	return parsed
}
