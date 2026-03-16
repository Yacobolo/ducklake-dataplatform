package storage

import (
	"net/http"
	"net/url"

	"github.com/go-chi/chi/v5"

	"duck-demo/internal/domain"
	"duck-demo/internal/ui/core"
)

type Handler struct{ deps *core.Dependencies }

func New(deps *core.Dependencies) *Handler { return &Handler{deps: deps} }

func (h *Handler) StorageHome(w http.ResponseWriter, r *http.Request) {
	core.RenderHTML(w, http.StatusOK, storageHomePage(core.PrincipalFromContext(r.Context())))
}

func (h *Handler) StorageCredentialsList(w http.ResponseWriter, r *http.Request) {
	pageReq := pageFromRequest(r, 30)
	items, total, err := h.deps.StorageCredential.List(r.Context(), principalName(r), pageReq)
	if err != nil {
		renderServiceError(w, err)
		return
	}
	rows := make([]storageCredentialRowData, 0, len(items))
	for i := range items {
		item := items[i]
		rows = append(rows, storageCredentialRowData{
			Name:    item.Name,
			URL:     "/ui/storage/credentials/" + url.PathEscape(item.Name),
			Type:    string(item.CredentialType),
			Owner:   item.Owner,
			Updated: formatTime(item.UpdatedAt),
		})
	}
	core.RenderHTML(w, http.StatusOK, storageCredentialsListPage(core.PrincipalFromContext(r.Context()), rows, pageReq, total))
}

func (h *Handler) StorageCredentialsNew(w http.ResponseWriter, r *http.Request) {
	core.RenderHTML(w, http.StatusOK, storageCredentialFormPage(core.PrincipalFromContext(r.Context()), "New Storage Credential", "/ui/storage/credentials", nil, h.deps.CSRFFieldProvider(r)))
}

func (h *Handler) StorageCredentialsCreate(w http.ResponseWriter, r *http.Request) {
	if !parseFormOrRenderBadRequest(w, r) {
		return
	}
	req := domain.CreateStorageCredentialRequest{
		Name:              formString(r.Form, "name"),
		CredentialType:    domain.CredentialType(formString(r.Form, "credential_type")),
		Comment:           formString(r.Form, "comment"),
		KeyID:             formString(r.Form, "key_id"),
		Secret:            formString(r.Form, "secret"),
		Endpoint:          formString(r.Form, "endpoint"),
		Region:            formString(r.Form, "region"),
		URLStyle:          formString(r.Form, "url_style"),
		AzureAccountName:  formString(r.Form, "azure_account_name"),
		AzureAccountKey:   formString(r.Form, "azure_account_key"),
		AzureClientID:     formString(r.Form, "azure_client_id"),
		AzureTenantID:     formString(r.Form, "azure_tenant_id"),
		AzureClientSecret: formString(r.Form, "azure_client_secret"),
		GCSKeyFilePath:    formString(r.Form, "gcs_key_file_path"),
	}
	item, err := h.deps.StorageCredential.Create(r.Context(), principalName(r), req)
	if err != nil {
		renderServiceError(w, err)
		return
	}
	http.Redirect(w, r, "/ui/storage/credentials/"+url.PathEscape(item.Name), http.StatusSeeOther)
}

func (h *Handler) StorageCredentialsDetail(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "credentialName")
	item, err := h.deps.StorageCredential.GetByName(r.Context(), principalName(r), name)
	if err != nil {
		renderServiceError(w, err)
		return
	}
	core.RenderHTML(w, http.StatusOK, storageCredentialDetailPage(core.PrincipalFromContext(r.Context()), item, h.deps.CSRFFieldProvider(r)))
}

func (h *Handler) StorageCredentialsEdit(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "credentialName")
	item, err := h.deps.StorageCredential.GetByName(r.Context(), principalName(r), name)
	if err != nil {
		renderServiceError(w, err)
		return
	}
	core.RenderHTML(w, http.StatusOK, storageCredentialFormPage(core.PrincipalFromContext(r.Context()), "Edit Storage Credential", "/ui/storage/credentials/"+url.PathEscape(name)+"/update", item, h.deps.CSRFFieldProvider(r)))
}

func (h *Handler) StorageCredentialsUpdate(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "credentialName")
	if !parseFormOrRenderBadRequest(w, r) {
		return
	}
	req := domain.UpdateStorageCredentialRequest{
		KeyID:             formOptionalString(r.Form, "key_id"),
		Secret:            formOptionalString(r.Form, "secret"),
		Endpoint:          formOptionalString(r.Form, "endpoint"),
		Region:            formOptionalString(r.Form, "region"),
		URLStyle:          formOptionalString(r.Form, "url_style"),
		AzureAccountName:  formOptionalString(r.Form, "azure_account_name"),
		AzureAccountKey:   formOptionalString(r.Form, "azure_account_key"),
		AzureClientID:     formOptionalString(r.Form, "azure_client_id"),
		AzureTenantID:     formOptionalString(r.Form, "azure_tenant_id"),
		AzureClientSecret: formOptionalString(r.Form, "azure_client_secret"),
		GCSKeyFilePath:    formOptionalString(r.Form, "gcs_key_file_path"),
		Comment:           formOptionalString(r.Form, "comment"),
	}
	if _, err := h.deps.StorageCredential.Update(r.Context(), principalName(r), name, req); err != nil {
		renderServiceError(w, err)
		return
	}
	http.Redirect(w, r, "/ui/storage/credentials/"+url.PathEscape(name), http.StatusSeeOther)
}

func (h *Handler) StorageCredentialsDelete(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "credentialName")
	if err := h.deps.StorageCredential.Delete(r.Context(), principalName(r), name); err != nil {
		renderServiceError(w, err)
		return
	}
	http.Redirect(w, r, "/ui/storage/credentials", http.StatusSeeOther)
}

func (h *Handler) StorageLocationsList(w http.ResponseWriter, r *http.Request) {
	pageReq := pageFromRequest(r, 30)
	items, total, err := h.deps.ExternalLocation.List(r.Context(), principalName(r), pageReq)
	if err != nil {
		renderServiceError(w, err)
		return
	}
	rows := make([]storageLocationRowData, 0, len(items))
	for i := range items {
		item := items[i]
		rows = append(rows, storageLocationRowData{
			Name:           item.Name,
			URL:            "/ui/storage/locations/" + url.PathEscape(item.Name),
			StorageURL:     item.URL,
			CredentialName: item.CredentialName,
			ReadOnly:       item.ReadOnly,
		})
	}
	core.RenderHTML(w, http.StatusOK, storageLocationsListPage(core.PrincipalFromContext(r.Context()), rows, pageReq, total))
}

func (h *Handler) StorageLocationsNew(w http.ResponseWriter, r *http.Request) {
	core.RenderHTML(w, http.StatusOK, storageLocationFormPage(core.PrincipalFromContext(r.Context()), "New External Location", "/ui/storage/locations", nil, h.deps.CSRFFieldProvider(r)))
}

func (h *Handler) StorageLocationsCreate(w http.ResponseWriter, r *http.Request) {
	if !parseFormOrRenderBadRequest(w, r) {
		return
	}
	req := domain.CreateExternalLocationRequest{
		Name:           formString(r.Form, "name"),
		URL:            formString(r.Form, "url"),
		CredentialName: formString(r.Form, "credential_name"),
		StorageType:    domain.StorageType(formString(r.Form, "storage_type")),
		Comment:        formString(r.Form, "comment"),
		ReadOnly:       formBool(r.Form, "read_only"),
	}
	item, err := h.deps.ExternalLocation.Create(r.Context(), principalName(r), req)
	if err != nil {
		renderServiceError(w, err)
		return
	}
	http.Redirect(w, r, "/ui/storage/locations/"+url.PathEscape(item.Name), http.StatusSeeOther)
}

func (h *Handler) StorageLocationsDetail(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "locationName")
	item, err := h.deps.ExternalLocation.GetByName(r.Context(), principalName(r), name)
	if err != nil {
		renderServiceError(w, err)
		return
	}
	core.RenderHTML(w, http.StatusOK, storageLocationDetailPage(core.PrincipalFromContext(r.Context()), item, h.deps.CSRFFieldProvider(r)))
}

func (h *Handler) StorageLocationsEdit(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "locationName")
	item, err := h.deps.ExternalLocation.GetByName(r.Context(), principalName(r), name)
	if err != nil {
		renderServiceError(w, err)
		return
	}
	core.RenderHTML(w, http.StatusOK, storageLocationFormPage(core.PrincipalFromContext(r.Context()), "Edit External Location", "/ui/storage/locations/"+url.PathEscape(name)+"/update", item, h.deps.CSRFFieldProvider(r)))
}

func (h *Handler) StorageLocationsUpdate(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "locationName")
	if !parseFormOrRenderBadRequest(w, r) {
		return
	}
	req := domain.UpdateExternalLocationRequest{
		URL:            formOptionalString(r.Form, "url"),
		CredentialName: formOptionalString(r.Form, "credential_name"),
		Comment:        formOptionalString(r.Form, "comment"),
		ReadOnly:       formOptionalBool(r.Form, "read_only"),
	}
	if _, err := h.deps.ExternalLocation.Update(r.Context(), principalName(r), name, req); err != nil {
		renderServiceError(w, err)
		return
	}
	http.Redirect(w, r, "/ui/storage/locations/"+url.PathEscape(name), http.StatusSeeOther)
}

func (h *Handler) StorageLocationsDelete(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "locationName")
	if err := h.deps.ExternalLocation.Delete(r.Context(), principalName(r), name); err != nil {
		renderServiceError(w, err)
		return
	}
	http.Redirect(w, r, "/ui/storage/locations", http.StatusSeeOther)
}

func (h *Handler) StorageVolumesList(w http.ResponseWriter, r *http.Request) {
	pageReq := pageFromRequest(r, 30)
	catalogName := formString(r.URL.Query(), "catalog")
	schemaName := formString(r.URL.Query(), "schema")
	rows := make([]storageVolumeRowData, 0)
	total := int64(0)
	if catalogName != "" && schemaName != "" {
		items, count, err := h.deps.Volume.List(r.Context(), principalName(r), catalogName, schemaName, pageReq)
		if err != nil {
			renderServiceError(w, err)
			return
		}
		total = count
		rows = make([]storageVolumeRowData, 0, len(items))
		for i := range items {
			item := items[i]
			rows = append(rows, storageVolumeRowData{
				Name:            item.Name,
				URL:             "/ui/storage/volumes/" + url.PathEscape(item.CatalogName) + "/" + url.PathEscape(item.SchemaName) + "/" + url.PathEscape(item.Name),
				VolumeType:      item.VolumeType,
				StorageLocation: item.StorageLocation,
				Owner:           item.Owner,
			})
		}
	}
	core.RenderHTML(w, http.StatusOK, storageVolumesListPage(core.PrincipalFromContext(r.Context()), catalogName, schemaName, rows, pageReq, total))
}
func (h *Handler) StorageVolumesNew(w http.ResponseWriter, r *http.Request) {
	core.RenderHTML(w, http.StatusOK, storageVolumeFormPage(core.PrincipalFromContext(r.Context()), "New Volume", "/ui/storage/volumes", nil, h.deps.CSRFFieldProvider(r)))
}
func (h *Handler) StorageVolumesCreate(w http.ResponseWriter, r *http.Request) {
	if !parseFormOrRenderBadRequest(w, r) {
		return
	}
	catalogName := formString(r.Form, "catalog_name")
	schemaName := formString(r.Form, "schema_name")
	req := domain.CreateVolumeRequest{
		Name:            formString(r.Form, "name"),
		VolumeType:      formString(r.Form, "volume_type"),
		StorageLocation: formString(r.Form, "storage_location"),
		Comment:         formString(r.Form, "comment"),
	}
	item, err := h.deps.Volume.Create(r.Context(), principalName(r), catalogName, schemaName, req)
	if err != nil {
		renderServiceError(w, err)
		return
	}
	http.Redirect(w, r, "/ui/storage/volumes/"+url.PathEscape(item.CatalogName)+"/"+url.PathEscape(item.SchemaName)+"/"+url.PathEscape(item.Name), http.StatusSeeOther)
}
func (h *Handler) StorageVolumesDetail(w http.ResponseWriter, r *http.Request) {
	catalogName := chi.URLParam(r, "catalogName")
	schemaName := chi.URLParam(r, "schemaName")
	volumeName := chi.URLParam(r, "volumeName")
	item, err := h.deps.Volume.GetByName(r.Context(), principalName(r), catalogName, schemaName, volumeName)
	if err != nil {
		renderServiceError(w, err)
		return
	}
	core.RenderHTML(w, http.StatusOK, storageVolumeDetailPage(core.PrincipalFromContext(r.Context()), item, h.deps.CSRFFieldProvider(r)))
}
func (h *Handler) StorageVolumesEdit(w http.ResponseWriter, r *http.Request) {
	catalogName := chi.URLParam(r, "catalogName")
	schemaName := chi.URLParam(r, "schemaName")
	volumeName := chi.URLParam(r, "volumeName")
	item, err := h.deps.Volume.GetByName(r.Context(), principalName(r), catalogName, schemaName, volumeName)
	if err != nil {
		renderServiceError(w, err)
		return
	}
	core.RenderHTML(w, http.StatusOK, storageVolumeFormPage(core.PrincipalFromContext(r.Context()), "Edit Volume", "/ui/storage/volumes/"+url.PathEscape(catalogName)+"/"+url.PathEscape(schemaName)+"/"+url.PathEscape(volumeName)+"/update", item, h.deps.CSRFFieldProvider(r)))
}
func (h *Handler) StorageVolumesUpdate(w http.ResponseWriter, r *http.Request) {
	catalogName := chi.URLParam(r, "catalogName")
	schemaName := chi.URLParam(r, "schemaName")
	volumeName := chi.URLParam(r, "volumeName")
	if !parseFormOrRenderBadRequest(w, r) {
		return
	}
	req := domain.UpdateVolumeRequest{
		NewName: formOptionalString(r.Form, "name"),
		Comment: formOptionalString(r.Form, "comment"),
	}
	if _, err := h.deps.Volume.Update(r.Context(), principalName(r), catalogName, schemaName, volumeName, req); err != nil {
		renderServiceError(w, err)
		return
	}
	http.Redirect(w, r, "/ui/storage/volumes/"+url.PathEscape(catalogName)+"/"+url.PathEscape(schemaName)+"/"+url.PathEscape(volumeName), http.StatusSeeOther)
}
func (h *Handler) StorageVolumesDelete(w http.ResponseWriter, r *http.Request) {
	catalogName := chi.URLParam(r, "catalogName")
	schemaName := chi.URLParam(r, "schemaName")
	volumeName := chi.URLParam(r, "volumeName")
	if err := h.deps.Volume.Delete(r.Context(), principalName(r), catalogName, schemaName, volumeName); err != nil {
		renderServiceError(w, err)
		return
	}
	http.Redirect(w, r, "/ui/storage/volumes?catalog="+url.QueryEscape(catalogName)+"&schema="+url.QueryEscape(schemaName), http.StatusSeeOther)
}
