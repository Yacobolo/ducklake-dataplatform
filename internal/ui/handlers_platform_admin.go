package ui

import (
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/go-chi/chi/v5"

	"duck-demo/internal/domain"

	. "maragu.dev/gomponents"
	. "maragu.dev/gomponents/html"
)

func (h *Handler) StorageHome(w http.ResponseWriter, r *http.Request) {
	renderHTML(w, http.StatusOK, sectionHomePage("Storage", "storage", principalFromContext(r.Context()), []securityCardData{
		{Title: "Credentials", Description: "Create and manage governed cloud storage credentials.", Href: "/ui/storage/credentials", LinkLabel: "Open credentials ->"},
		{Title: "Locations", Description: "Manage external locations backed by storage credentials.", Href: "/ui/storage/locations", LinkLabel: "Open locations ->"},
		{Title: "Volumes", Description: "Create and manage governed volumes in catalog schemas.", Href: "/ui/storage/volumes", LinkLabel: "Open volumes ->"},
	}))
}

func (h *Handler) StorageCredentialsList(w http.ResponseWriter, r *http.Request) {
	pageReq := pageFromRequest(r, 30)
	principal, _ := principalLabel(r.Context())
	items, total, err := h.StorageCredential.List(r.Context(), principal, pageReq)
	if err != nil {
		h.renderServiceError(w, r, err)
		return
	}
	rows := make([]adminTableRow, 0, len(items))
	for i := range items {
		item := items[i]
		rows = append(rows, adminTableRow{
			Filter: item.Name + " " + string(item.CredentialType) + " " + item.Owner,
			Cells: []Node{
				Td(A(Href("/ui/storage/credentials/"+url.PathEscape(item.Name)), Text(item.Name))),
				Td(Text(string(item.CredentialType))),
				Td(Text(item.Owner)),
				Td(Text(formatTime(item.UpdatedAt))),
			},
		})
	}
	renderHTML(w, http.StatusOK, adminTablePage("Storage: Credentials", "storage", principalFromContext(r.Context()), storageSectionNav("credentials"), "/ui/storage/credentials/new", "New credential", "Filter by name, type, or owner", "/ui/storage/credentials", pageReq, total, []string{"Name", "Type", "Owner", "Updated"}, rows))
}

func (h *Handler) StorageCredentialsNew(w http.ResponseWriter, r *http.Request) {
	renderHTML(w, http.StatusOK, storageCredentialFormPage(principalFromContext(r.Context()), "New Storage Credential", "/ui/storage/credentials", nil, csrfFieldProvider(r)))
}

func (h *Handler) StorageCredentialsCreate(w http.ResponseWriter, r *http.Request) {
	principal, _ := principalLabel(r.Context())
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
	item, err := h.StorageCredential.Create(r.Context(), principal, req)
	if err != nil {
		h.renderServiceError(w, r, err)
		return
	}
	http.Redirect(w, r, "/ui/storage/credentials/"+url.PathEscape(item.Name), http.StatusSeeOther)
}

func (h *Handler) StorageCredentialsDetail(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "credentialName")
	principal, _ := principalLabel(r.Context())
	item, err := h.StorageCredential.GetByName(r.Context(), principal, name)
	if err != nil {
		h.renderServiceError(w, r, err)
		return
	}
	renderHTML(w, http.StatusOK, appPage("Storage Credential: "+item.Name, "storage", principalFromContext(r.Context()),
		storageSectionNav("credentials"),
		Div(Class(cardClass()),
			P(Text("Type: "+string(item.CredentialType))),
			P(Text("Owner: "+item.Owner)),
			P(Text("Comment: "+dashIfEmpty(item.Comment))),
			P(Text("Updated: "+formatTime(item.UpdatedAt))),
			Div(Class(buttonRowClass()),
				A(Href("/ui/storage/credentials/"+url.PathEscape(item.Name)+"/edit"), Class(secondaryButtonClass()), Text("Edit")),
				Form(Method("post"), Action("/ui/storage/credentials/"+url.PathEscape(item.Name)+"/delete"), csrfFieldProvider(r)(), Button(Type("submit"), Class(dangerButtonClass()), Text("Delete"))),
			),
		),
	))
}

func (h *Handler) StorageCredentialsEdit(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "credentialName")
	principal, _ := principalLabel(r.Context())
	item, err := h.StorageCredential.GetByName(r.Context(), principal, name)
	if err != nil {
		h.renderServiceError(w, r, err)
		return
	}
	renderHTML(w, http.StatusOK, storageCredentialFormPage(principalFromContext(r.Context()), "Edit Storage Credential", "/ui/storage/credentials/"+url.PathEscape(name)+"/update", item, csrfFieldProvider(r)))
}

func (h *Handler) StorageCredentialsUpdate(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "credentialName")
	principal, _ := principalLabel(r.Context())
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
	if _, err := h.StorageCredential.Update(r.Context(), principal, name, req); err != nil {
		h.renderServiceError(w, r, err)
		return
	}
	http.Redirect(w, r, "/ui/storage/credentials/"+url.PathEscape(name), http.StatusSeeOther)
}

func (h *Handler) StorageCredentialsDelete(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "credentialName")
	principal, _ := principalLabel(r.Context())
	if err := h.StorageCredential.Delete(r.Context(), principal, name); err != nil {
		h.renderServiceError(w, r, err)
		return
	}
	http.Redirect(w, r, "/ui/storage/credentials", http.StatusSeeOther)
}

func (h *Handler) StorageLocationsList(w http.ResponseWriter, r *http.Request) {
	pageReq := pageFromRequest(r, 30)
	principal, _ := principalLabel(r.Context())
	items, total, err := h.ExternalLocation.List(r.Context(), principal, pageReq)
	if err != nil {
		h.renderServiceError(w, r, err)
		return
	}
	rows := make([]adminTableRow, 0, len(items))
	for i := range items {
		item := items[i]
		readOnly := "false"
		if item.ReadOnly {
			readOnly = "true"
		}
		rows = append(rows, adminTableRow{
			Filter: item.Name + " " + item.URL + " " + item.CredentialName,
			Cells: []Node{
				Td(A(Href("/ui/storage/locations/"+url.PathEscape(item.Name)), Text(item.Name))),
				Td(Text(item.URL)),
				Td(Text(item.CredentialName)),
				Td(Text(readOnly)),
			},
		})
	}
	renderHTML(w, http.StatusOK, adminTablePage("Storage: Locations", "storage", principalFromContext(r.Context()), storageSectionNav("locations"), "/ui/storage/locations/new", "New location", "Filter by name, url, or credential", "/ui/storage/locations", pageReq, total, []string{"Name", "URL", "Credential", "Read Only"}, rows))
}

func (h *Handler) StorageLocationsNew(w http.ResponseWriter, r *http.Request) {
	renderHTML(w, http.StatusOK, storageLocationFormPage(principalFromContext(r.Context()), "New External Location", "/ui/storage/locations", nil, csrfFieldProvider(r)))
}

func (h *Handler) StorageLocationsCreate(w http.ResponseWriter, r *http.Request) {
	principal, _ := principalLabel(r.Context())
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
	item, err := h.ExternalLocation.Create(r.Context(), principal, req)
	if err != nil {
		h.renderServiceError(w, r, err)
		return
	}
	http.Redirect(w, r, "/ui/storage/locations/"+url.PathEscape(item.Name), http.StatusSeeOther)
}

func (h *Handler) StorageLocationsDetail(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "locationName")
	principal, _ := principalLabel(r.Context())
	item, err := h.ExternalLocation.GetByName(r.Context(), principal, name)
	if err != nil {
		h.renderServiceError(w, r, err)
		return
	}
	renderHTML(w, http.StatusOK, appPage("External Location: "+item.Name, "storage", principalFromContext(r.Context()),
		storageSectionNav("locations"),
		Div(Class(cardClass()),
			P(Text("URL: "+item.URL)),
			P(Text("Credential: "+item.CredentialName)),
			P(Text("Type: "+string(item.StorageType))),
			P(Text("Read only: "+strconv.FormatBool(item.ReadOnly))),
			P(Text("Owner: "+item.Owner)),
			P(Text("Comment: "+dashIfEmpty(item.Comment))),
			Div(Class(buttonRowClass()),
				A(Href("/ui/storage/locations/"+url.PathEscape(item.Name)+"/edit"), Class(secondaryButtonClass()), Text("Edit")),
				Form(Method("post"), Action("/ui/storage/locations/"+url.PathEscape(item.Name)+"/delete"), csrfFieldProvider(r)(), Button(Type("submit"), Class(dangerButtonClass()), Text("Delete"))),
			),
		),
	))
}

func (h *Handler) StorageLocationsEdit(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "locationName")
	principal, _ := principalLabel(r.Context())
	item, err := h.ExternalLocation.GetByName(r.Context(), principal, name)
	if err != nil {
		h.renderServiceError(w, r, err)
		return
	}
	renderHTML(w, http.StatusOK, storageLocationFormPage(principalFromContext(r.Context()), "Edit External Location", "/ui/storage/locations/"+url.PathEscape(name)+"/update", item, csrfFieldProvider(r)))
}

func (h *Handler) StorageLocationsUpdate(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "locationName")
	principal, _ := principalLabel(r.Context())
	if !parseFormOrRenderBadRequest(w, r) {
		return
	}
	req := domain.UpdateExternalLocationRequest{
		URL:            formOptionalString(r.Form, "url"),
		CredentialName: formOptionalString(r.Form, "credential_name"),
		Comment:        formOptionalString(r.Form, "comment"),
		ReadOnly:       formOptionalBool(r.Form, "read_only"),
	}
	if _, err := h.ExternalLocation.Update(r.Context(), principal, name, req); err != nil {
		h.renderServiceError(w, r, err)
		return
	}
	http.Redirect(w, r, "/ui/storage/locations/"+url.PathEscape(name), http.StatusSeeOther)
}

func (h *Handler) StorageLocationsDelete(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "locationName")
	principal, _ := principalLabel(r.Context())
	if err := h.ExternalLocation.Delete(r.Context(), principal, name); err != nil {
		h.renderServiceError(w, r, err)
		return
	}
	http.Redirect(w, r, "/ui/storage/locations", http.StatusSeeOther)
}

func (h *Handler) StorageVolumesList(w http.ResponseWriter, r *http.Request) {
	pageReq := pageFromRequest(r, 30)
	principal, _ := principalLabel(r.Context())
	catalogName := r.URL.Query().Get("catalog")
	schemaName := r.URL.Query().Get("schema")
	rows := []adminTableRow{}
	total := int64(0)
	if catalogName != "" && schemaName != "" {
		items, count, err := h.Volume.List(r.Context(), principal, catalogName, schemaName, pageReq)
		if err != nil {
			h.renderServiceError(w, r, err)
			return
		}
		total = count
		for i := range items {
			item := items[i]
			rows = append(rows, adminTableRow{
				Filter: item.Name + " " + item.VolumeType + " " + item.StorageLocation,
				Cells: []Node{
					Td(A(Href("/ui/storage/volumes/"+url.PathEscape(item.CatalogName)+"/"+url.PathEscape(item.SchemaName)+"/"+url.PathEscape(item.Name)), Text(item.Name))),
					Td(Text(item.VolumeType)),
					Td(Text(item.StorageLocation)),
					Td(Text(item.Owner)),
				},
			})
		}
	}
	filterCard := Div(Class(cardClass()),
		Form(Class(stackFormClass()), Method("get"), Action("/ui/storage/volumes"),
			Label(Text("Catalog")),
			Input(Name("catalog"), Value(catalogName)),
			Label(Text("Schema")),
			Input(Name("schema"), Value(schemaName)),
			Div(Class(formActionsClass()), Button(Type("submit"), Class(secondaryButtonClass()), Text("Load volumes"))),
		),
	)
	renderHTML(w, http.StatusOK, adminTablePage("Storage: Volumes", "storage", principalFromContext(r.Context()), storageSectionNav("volumes"), "/ui/storage/volumes/new", "New volume", "Filter by volume name or type", "/ui/storage/volumes?catalog="+url.QueryEscape(catalogName)+"&schema="+url.QueryEscape(schemaName), pageReq, total, []string{"Name", "Type", "Location", "Owner"}, rows, filterCard))
}

func (h *Handler) StorageVolumesNew(w http.ResponseWriter, r *http.Request) {
	renderHTML(w, http.StatusOK, storageVolumeFormPage(principalFromContext(r.Context()), "New Volume", "/ui/storage/volumes", nil, csrfFieldProvider(r)))
}

func (h *Handler) StorageVolumesCreate(w http.ResponseWriter, r *http.Request) {
	principal, _ := principalLabel(r.Context())
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
	item, err := h.Volume.Create(r.Context(), principal, catalogName, schemaName, req)
	if err != nil {
		h.renderServiceError(w, r, err)
		return
	}
	http.Redirect(w, r, "/ui/storage/volumes/"+url.PathEscape(item.CatalogName)+"/"+url.PathEscape(item.SchemaName)+"/"+url.PathEscape(item.Name), http.StatusSeeOther)
}

func (h *Handler) StorageVolumesDetail(w http.ResponseWriter, r *http.Request) {
	catalogName := chi.URLParam(r, "catalogName")
	schemaName := chi.URLParam(r, "schemaName")
	volumeName := chi.URLParam(r, "volumeName")
	principal, _ := principalLabel(r.Context())
	item, err := h.Volume.GetByName(r.Context(), principal, catalogName, schemaName, volumeName)
	if err != nil {
		h.renderServiceError(w, r, err)
		return
	}
	renderHTML(w, http.StatusOK, appPage("Volume: "+item.Name, "storage", principalFromContext(r.Context()),
		storageSectionNav("volumes"),
		Div(Class(cardClass()),
			P(Text("Catalog: "+item.CatalogName)),
			P(Text("Schema: "+item.SchemaName)),
			P(Text("Type: "+item.VolumeType)),
			P(Text("Location: "+item.StorageLocation)),
			P(Text("Owner: "+item.Owner)),
			P(Text("Comment: "+dashIfEmpty(item.Comment))),
			Div(Class(buttonRowClass()),
				A(Href("/ui/storage/volumes/"+url.PathEscape(catalogName)+"/"+url.PathEscape(schemaName)+"/"+url.PathEscape(volumeName)+"/edit"), Class(secondaryButtonClass()), Text("Edit")),
				Form(Method("post"), Action("/ui/storage/volumes/"+url.PathEscape(catalogName)+"/"+url.PathEscape(schemaName)+"/"+url.PathEscape(volumeName)+"/delete"), csrfFieldProvider(r)(), Button(Type("submit"), Class(dangerButtonClass()), Text("Delete"))),
			),
		),
	))
}

func (h *Handler) StorageVolumesEdit(w http.ResponseWriter, r *http.Request) {
	catalogName := chi.URLParam(r, "catalogName")
	schemaName := chi.URLParam(r, "schemaName")
	volumeName := chi.URLParam(r, "volumeName")
	principal, _ := principalLabel(r.Context())
	item, err := h.Volume.GetByName(r.Context(), principal, catalogName, schemaName, volumeName)
	if err != nil {
		h.renderServiceError(w, r, err)
		return
	}
	renderHTML(w, http.StatusOK, storageVolumeFormPage(principalFromContext(r.Context()), "Edit Volume", "/ui/storage/volumes/"+url.PathEscape(catalogName)+"/"+url.PathEscape(schemaName)+"/"+url.PathEscape(volumeName)+"/update", item, csrfFieldProvider(r)))
}

func (h *Handler) StorageVolumesUpdate(w http.ResponseWriter, r *http.Request) {
	catalogName := chi.URLParam(r, "catalogName")
	schemaName := chi.URLParam(r, "schemaName")
	volumeName := chi.URLParam(r, "volumeName")
	principal, _ := principalLabel(r.Context())
	if !parseFormOrRenderBadRequest(w, r) {
		return
	}
	req := domain.UpdateVolumeRequest{
		NewName: formOptionalString(r.Form, "name"),
		Comment: formOptionalString(r.Form, "comment"),
	}
	if _, err := h.Volume.Update(r.Context(), principal, catalogName, schemaName, volumeName, req); err != nil {
		h.renderServiceError(w, r, err)
		return
	}
	http.Redirect(w, r, "/ui/storage/volumes/"+url.PathEscape(catalogName)+"/"+url.PathEscape(schemaName)+"/"+url.PathEscape(volumeName), http.StatusSeeOther)
}

func (h *Handler) StorageVolumesDelete(w http.ResponseWriter, r *http.Request) {
	catalogName := chi.URLParam(r, "catalogName")
	schemaName := chi.URLParam(r, "schemaName")
	volumeName := chi.URLParam(r, "volumeName")
	principal, _ := principalLabel(r.Context())
	if err := h.Volume.Delete(r.Context(), principal, catalogName, schemaName, volumeName); err != nil {
		h.renderServiceError(w, r, err)
		return
	}
	http.Redirect(w, r, "/ui/storage/volumes?catalog="+url.QueryEscape(catalogName)+"&schema="+url.QueryEscape(schemaName), http.StatusSeeOther)
}

func (h *Handler) ComputeHome(w http.ResponseWriter, r *http.Request) {
	renderHTML(w, http.StatusOK, sectionHomePage("Compute", "compute", principalFromContext(r.Context()), []securityCardData{
		{Title: "Endpoints", Description: "Create compute endpoints, manage assignments, and inspect remote health.", Href: "/ui/compute/endpoints", LinkLabel: "Open endpoints ->"},
	}))
}

func (h *Handler) ComputeEndpointsList(w http.ResponseWriter, r *http.Request) {
	pageReq := pageFromRequest(r, 30)
	principal, _ := principalLabel(r.Context())
	items, total, err := h.ComputeEndpoint.List(r.Context(), principal, pageReq)
	if err != nil {
		h.renderServiceError(w, r, err)
		return
	}
	rows := make([]adminTableRow, 0, len(items))
	for i := range items {
		item := items[i]
		rows = append(rows, adminTableRow{
			Filter: item.Name + " " + item.Type + " " + item.Status,
			Cells: []Node{
				Td(A(Href("/ui/compute/endpoints/"+url.PathEscape(item.Name)), Text(item.Name))),
				Td(Text(item.Type)),
				Td(Text(item.Status)),
				Td(Text(item.URL)),
			},
		})
	}
	renderHTML(w, http.StatusOK, adminTablePage("Compute: Endpoints", "compute", principalFromContext(r.Context()), computeSectionNav("endpoints"), "/ui/compute/endpoints/new", "New endpoint", "Filter by name, type, or status", "/ui/compute/endpoints", pageReq, total, []string{"Name", "Type", "Status", "URL"}, rows))
}

func (h *Handler) ComputeEndpointsNew(w http.ResponseWriter, r *http.Request) {
	renderHTML(w, http.StatusOK, computeEndpointFormPage(principalFromContext(r.Context()), "New Compute Endpoint", "/ui/compute/endpoints", nil, csrfFieldProvider(r)))
}

func (h *Handler) ComputeEndpointsCreate(w http.ResponseWriter, r *http.Request) {
	principal, _ := principalLabel(r.Context())
	if !parseFormOrRenderBadRequest(w, r) {
		return
	}
	maxMemoryGB, err := formOptionalInt64(r.Form, "max_memory_gb")
	if err != nil {
		renderHTML(w, http.StatusBadRequest, errorPage("Invalid Request", "max_memory_gb must be numeric."))
		return
	}
	item, err := h.ComputeEndpoint.Create(r.Context(), principal, domain.CreateComputeEndpointRequest{
		Name:        formString(r.Form, "name"),
		URL:         formString(r.Form, "url"),
		Type:        formString(r.Form, "type"),
		Size:        formString(r.Form, "size"),
		MaxMemoryGB: maxMemoryGB,
		AuthToken:   formString(r.Form, "auth_token"),
	})
	if err != nil {
		h.renderServiceError(w, r, err)
		return
	}
	http.Redirect(w, r, "/ui/compute/endpoints/"+url.PathEscape(item.Name), http.StatusSeeOther)
}

func (h *Handler) ComputeEndpointsDetail(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "endpointName")
	principal, _ := principalLabel(r.Context())
	item, err := h.ComputeEndpoint.GetByName(r.Context(), principal, name)
	if err != nil {
		h.renderServiceError(w, r, err)
		return
	}
	assignments, _, err := h.ComputeEndpoint.ListAssignments(r.Context(), principal, name, domain.PageRequest{MaxResults: 100})
	if err != nil {
		h.renderServiceError(w, r, err)
		return
	}
	healthText := "Unavailable"
	if health, healthErr := h.ComputeEndpoint.HealthCheck(r.Context(), principal, name); healthErr == nil && health != nil {
		parts := []string{}
		if health.Status != nil {
			parts = append(parts, "status="+*health.Status)
		}
		if health.DuckdbVersion != nil {
			parts = append(parts, "duckdb="+*health.DuckdbVersion)
		}
		healthText = strings.Join(parts, ", ")
	}
	assignmentRows := make([]Node, 0, len(assignments))
	for i := range assignments {
		itemAssignment := assignments[i]
		assignmentRows = append(assignmentRows, Tr(
			Td(Text(itemAssignment.PrincipalID)),
			Td(Text(itemAssignment.PrincipalType)),
			Td(Text(strconv.FormatBool(itemAssignment.IsDefault))),
			Td(Text(strconv.FormatBool(itemAssignment.FallbackLocal))),
			Td(Class("text-right"), Form(Method("post"), Action("/ui/compute/endpoints/"+url.PathEscape(name)+"/assignments/"+url.PathEscape(itemAssignment.ID)+"/delete"), csrfFieldProvider(r)(), Button(Type("submit"), Class(dangerButtonClass()), Text("Remove")))),
		))
	}
	renderHTML(w, http.StatusOK, appPage("Compute Endpoint: "+item.Name, "compute", principalFromContext(r.Context()),
		computeSectionNav("endpoints"),
		Div(Class(cardClass()),
			P(Text("Type: "+item.Type)),
			P(Text("Status: "+item.Status)),
			P(Text("URL: "+item.URL)),
			P(Text("Health: "+healthText)),
			P(Text("Owner: "+item.Owner)),
			Div(Class(buttonRowClass()),
				A(Href("/ui/compute/endpoints/"+url.PathEscape(name)+"/edit"), Class(secondaryButtonClass()), Text("Edit")),
				Form(Method("post"), Action("/ui/compute/endpoints/"+url.PathEscape(name)+"/delete"), csrfFieldProvider(r)(), Button(Type("submit"), Class(dangerButtonClass()), Text("Delete"))),
			),
		),
		Div(Class(cardClass()),
			H2(Text("Create assignment")),
			Form(Class(stackFormClass()), Method("post"), Action("/ui/compute/endpoints/"+url.PathEscape(name)+"/assignments"),
				csrfFieldProvider(r)(),
				Label(Text("Principal ID")),
				Input(Name("principal_id"), Required()),
				Label(Text("Principal type")),
				Select(Name("principal_type"), Option(Value("user"), Text("user")), Option(Value("group"), Text("group"))),
				Label(Input(Type("checkbox"), Name("is_default")), Text(" Default endpoint")),
				Label(Input(Type("checkbox"), Name("fallback_local")), Text(" Fallback to local compute")),
				Div(Class(formActionsClass()), Button(Type("submit"), Class(primaryButtonClass()), Text("Create assignment"))),
			),
		),
		Div(Class(cardClass(tableWrapClass())),
			H2(Text("Assignments")),
			Table(Class(dataTableClass()), THead(Tr(Th(Text("Principal ID")), Th(Text("Type")), Th(Text("Default")), Th(Text("Fallback Local")), Th(Class("text-right"), Text("Actions")))), TBody(Group(assignmentRows))),
		),
	))
}

func (h *Handler) ComputeEndpointsEdit(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "endpointName")
	principal, _ := principalLabel(r.Context())
	item, err := h.ComputeEndpoint.GetByName(r.Context(), principal, name)
	if err != nil {
		h.renderServiceError(w, r, err)
		return
	}
	renderHTML(w, http.StatusOK, computeEndpointFormPage(principalFromContext(r.Context()), "Edit Compute Endpoint", "/ui/compute/endpoints/"+url.PathEscape(name)+"/update", item, csrfFieldProvider(r)))
}

func (h *Handler) ComputeEndpointsUpdate(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "endpointName")
	principal, _ := principalLabel(r.Context())
	if !parseFormOrRenderBadRequest(w, r) {
		return
	}
	maxMemoryGB, err := formOptionalInt64(r.Form, "max_memory_gb")
	if err != nil {
		renderHTML(w, http.StatusBadRequest, errorPage("Invalid Request", "max_memory_gb must be numeric."))
		return
	}
	req := domain.UpdateComputeEndpointRequest{
		URL:         formOptionalString(r.Form, "url"),
		Size:        formOptionalString(r.Form, "size"),
		MaxMemoryGB: maxMemoryGB,
		AuthToken:   formOptionalString(r.Form, "auth_token"),
		Status:      formOptionalString(r.Form, "status"),
	}
	if _, err := h.ComputeEndpoint.Update(r.Context(), principal, name, req); err != nil {
		h.renderServiceError(w, r, err)
		return
	}
	http.Redirect(w, r, "/ui/compute/endpoints/"+url.PathEscape(name), http.StatusSeeOther)
}

func (h *Handler) ComputeEndpointsDelete(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "endpointName")
	principal, _ := principalLabel(r.Context())
	if err := h.ComputeEndpoint.Delete(r.Context(), principal, name); err != nil {
		h.renderServiceError(w, r, err)
		return
	}
	http.Redirect(w, r, "/ui/compute/endpoints", http.StatusSeeOther)
}

func (h *Handler) ComputeAssignmentsCreate(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "endpointName")
	principal, _ := principalLabel(r.Context())
	if !parseFormOrRenderBadRequest(w, r) {
		return
	}
	req := domain.CreateComputeAssignmentRequest{
		PrincipalID:   formString(r.Form, "principal_id"),
		PrincipalType: formString(r.Form, "principal_type"),
		IsDefault:     formBool(r.Form, "is_default"),
		FallbackLocal: formBool(r.Form, "fallback_local"),
	}
	if _, err := h.ComputeEndpoint.Assign(r.Context(), principal, name, req); err != nil {
		h.renderServiceError(w, r, err)
		return
	}
	http.Redirect(w, r, "/ui/compute/endpoints/"+url.PathEscape(name), http.StatusSeeOther)
}

func (h *Handler) ComputeAssignmentsDelete(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "endpointName")
	assignmentID := chi.URLParam(r, "assignmentID")
	principal, _ := principalLabel(r.Context())
	if err := h.ComputeEndpoint.Unassign(r.Context(), principal, assignmentID); err != nil {
		h.renderServiceError(w, r, err)
		return
	}
	http.Redirect(w, r, "/ui/compute/endpoints/"+url.PathEscape(name), http.StatusSeeOther)
}

func (h *Handler) GovernanceHome(w http.ResponseWriter, r *http.Request) {
	renderHTML(w, http.StatusOK, sectionHomePage("Governance", "governance", principalFromContext(r.Context()), []securityCardData{
		{Title: "Search", Description: "Search schemas, tables, and columns across catalogs.", Href: "/ui/governance/search", LinkLabel: "Open search ->"},
		{Title: "Tags", Description: "Manage tag definitions and assignments.", Href: "/ui/governance/tags", LinkLabel: "Open tags ->"},
		{Title: "Lineage", Description: "Inspect upstream, downstream, and column-level lineage.", Href: "/ui/governance/lineage", LinkLabel: "Open lineage ->"},
		{Title: "Audit Logs", Description: "Inspect platform audit activity.", Href: "/ui/governance/audit-logs", LinkLabel: "Open audit logs ->"},
		{Title: "Query History", Description: "Review query execution history.", Href: "/ui/governance/query-history", LinkLabel: "Open query history ->"},
		{Title: "Manifest", Description: "Generate secure table manifests with files, filters, and masks.", Href: "/ui/governance/manifest", LinkLabel: "Open manifest ->"},
	}))
}

func (h *Handler) GovernanceSearch(w http.ResponseWriter, r *http.Request) {
	pageReq := pageFromRequest(r, 30)
	queryText := formString(r.URL.Query(), "q")
	objectType := formString(r.URL.Query(), "object_type")
	catalogName := formString(r.URL.Query(), "catalog")
	var (
		objectTypePtr *string
		catalogPtr    *string
		results       []domain.SearchResult
		err           error
	)
	if objectType != "" {
		objectTypePtr = &objectType
	}
	if catalogName != "" {
		catalogPtr = &catalogName
	}
	if queryText != "" {
		results, _, err = h.Search.Search(r.Context(), queryText, objectTypePtr, catalogPtr, pageReq)
		if err != nil {
			h.renderServiceError(w, r, err)
			return
		}
	}
	renderHTML(w, http.StatusOK, governanceSearchPage(governanceSearchPageData{
		Principal:   principalFromContext(r.Context()),
		Query:       queryText,
		ObjectType:  objectType,
		CatalogName: catalogName,
		Rows:        results,
	}))
}

func (h *Handler) GovernanceTagsList(w http.ResponseWriter, r *http.Request) {
	pageReq := pageFromRequest(r, 30)
	items, total, err := h.Tag.ListTags(r.Context(), pageReq)
	if err != nil {
		h.renderServiceError(w, r, err)
		return
	}
	rows := make([]Node, 0, len(items))
	for i := range items {
		item := items[i]
		assignments, err := h.Tag.ListAssignmentsForTag(r.Context(), item.ID)
		if err != nil {
			h.renderServiceError(w, r, err)
			return
		}
		rows = append(rows, Tr(
			Td(Text(item.Key)),
			Td(Text(strOrDash(item.Value))),
			Td(Text(item.CreatedBy)),
			Td(Text(strconv.Itoa(len(assignments)))),
			Td(Class("text-right"), actionMenu("Actions",
				actionMenuPost("/ui/governance/tags/"+item.ID+"/delete", "Delete tag", csrfFieldProvider(r), true),
			)),
		))
	}
	renderHTML(w, http.StatusOK, appPage("Governance: Tags", "governance", principalFromContext(r.Context()),
		governanceSectionNav("tags"),
		Div(Class(cardClass()),
			H2(Text("Create tag")),
			Form(Class(stackFormClass()), Method("post"), Action("/ui/governance/tags"),
				csrfFieldProvider(r)(),
				Label(Text("Key")),
				Input(Name("key"), Required()),
				Label(Text("Value")),
				Input(Name("value")),
				Div(Class(formActionsClass()), Button(Type("submit"), Class(primaryButtonClass()), Text("Create tag"))),
			),
		),
		Div(Class(cardClass()),
			H2(Text("Assign tag")),
			Form(Class(stackFormClass()), Method("post"), Action("/ui/governance/tag-assignments"),
				csrfFieldProvider(r)(),
				Label(Text("Tag ID")),
				Input(Name("tag_id"), Required()),
				Label(Text("Securable type")),
				Input(Name("securable_type"), Required()),
				Label(Text("Securable ID")),
				Input(Name("securable_id"), Required()),
				Label(Text("Column name")),
				Input(Name("column_name")),
				Div(Class(formActionsClass()), Button(Type("submit"), Class(secondaryButtonClass()), Text("Assign tag"))),
			),
		),
		Div(Class(cardClass()),
			H2(Text("Remove tag assignment")),
			Form(Class(stackFormClass()), Method("post"), Action("/ui/governance/tag-assignments/delete"),
				csrfFieldProvider(r)(),
				Label(Text("Assignment ID")),
				Input(Name("assignment_id"), Required()),
				Div(Class(formActionsClass()), Button(Type("submit"), Class(dangerButtonClass()), Text("Remove assignment"))),
			),
		),
		Div(Class(cardClass(tableWrapClass())),
			Table(Class(dataTableClass()), THead(Tr(Th(Text("Key")), Th(Text("Value")), Th(Text("Created By")), Th(Text("Assignments")), Th(Class("text-right"), Text("Actions")))), TBody(Group(rows))),
		),
		paginationCard("/ui/governance/tags", pageReq, total),
	))
}

func (h *Handler) GovernanceTagsCreate(w http.ResponseWriter, r *http.Request) {
	principal, _ := principalLabel(r.Context())
	if !parseFormOrRenderBadRequest(w, r) {
		return
	}
	req := domain.CreateTagRequest{
		Key:   formString(r.Form, "key"),
		Value: formOptionalString(r.Form, "value"),
	}
	if _, err := h.Tag.CreateTag(r.Context(), principal, req); err != nil {
		h.renderServiceError(w, r, err)
		return
	}
	http.Redirect(w, r, "/ui/governance/tags", http.StatusSeeOther)
}

func (h *Handler) GovernanceTagsDelete(w http.ResponseWriter, r *http.Request) {
	tagID := chi.URLParam(r, "tagID")
	principal, _ := principalLabel(r.Context())
	if err := h.Tag.DeleteTag(r.Context(), principal, tagID); err != nil {
		h.renderServiceError(w, r, err)
		return
	}
	http.Redirect(w, r, "/ui/governance/tags", http.StatusSeeOther)
}

func (h *Handler) GovernanceTagAssignmentsCreate(w http.ResponseWriter, r *http.Request) {
	principal, _ := principalLabel(r.Context())
	if !parseFormOrRenderBadRequest(w, r) {
		return
	}
	req := domain.AssignTagRequest{
		TagID:         formString(r.Form, "tag_id"),
		SecurableType: formString(r.Form, "securable_type"),
		SecurableID:   formString(r.Form, "securable_id"),
		ColumnName:    formOptionalString(r.Form, "column_name"),
	}
	if _, err := h.Tag.AssignTag(r.Context(), principal, req); err != nil {
		h.renderServiceError(w, r, err)
		return
	}
	http.Redirect(w, r, "/ui/governance/tags", http.StatusSeeOther)
}

func (h *Handler) GovernanceTagAssignmentsDelete(w http.ResponseWriter, r *http.Request) {
	if !parseFormOrRenderBadRequest(w, r) {
		return
	}
	assignmentID := formString(r.Form, "assignment_id")
	principal, _ := principalLabel(r.Context())
	if err := h.Tag.UnassignTag(r.Context(), principal, assignmentID); err != nil {
		h.renderServiceError(w, r, err)
		return
	}
	http.Redirect(w, r, "/ui/governance/tags", http.StatusSeeOther)
}

func (h *Handler) GovernanceAuditLogs(w http.ResponseWriter, r *http.Request) {
	pageReq := pageFromRequest(r, 30)
	filter := domain.AuditFilter{
		PrincipalName: optionalQueryValue(r, "principal"),
		Action:        optionalQueryValue(r, "action"),
		Status:        optionalQueryValue(r, "status"),
		Page:          pageReq,
	}
	items, total, err := h.Audit.List(r.Context(), filter)
	if err != nil {
		h.renderServiceError(w, r, err)
		return
	}
	rows := make([]adminTableRow, 0, len(items))
	for i := range items {
		item := items[i]
		rows = append(rows, adminTableRow{
			Filter: item.PrincipalName + " " + item.Action + " " + item.Status,
			Cells: []Node{
				Td(Text(item.PrincipalName)),
				Td(Text(item.Action)),
				Td(Text(item.Status)),
				Td(Text(formatTime(item.CreatedAt))),
			},
		})
	}
	renderHTML(w, http.StatusOK, adminTablePage("Governance: Audit Logs", "governance", principalFromContext(r.Context()), governanceSectionNav("audit"), "", "", "Filter by principal, action, or status", "/ui/governance/audit-logs", pageReq, total, []string{"Principal", "Action", "Status", "Created"}, rows))
}

func (h *Handler) GovernanceQueryHistory(w http.ResponseWriter, r *http.Request) {
	pageReq := pageFromRequest(r, 30)
	filter := domain.QueryHistoryFilter{
		PrincipalName: optionalQueryValue(r, "principal"),
		Status:        optionalQueryValue(r, "status"),
		Page:          pageReq,
	}
	items, total, err := h.QueryHistory.List(r.Context(), filter)
	if err != nil {
		h.renderServiceError(w, r, err)
		return
	}
	rows := make([]adminTableRow, 0, len(items))
	for i := range items {
		item := items[i]
		rows = append(rows, adminTableRow{
			Filter: item.PrincipalName + " " + item.Status + " " + strOrDash(item.StatementType),
			Cells: []Node{
				Td(Text(item.PrincipalName)),
				Td(Text(strOrDash(item.StatementType))),
				Td(Text(item.Status)),
				Td(Text(formatTime(item.CreatedAt))),
			},
		})
	}
	renderHTML(w, http.StatusOK, adminTablePage("Governance: Query History", "governance", principalFromContext(r.Context()), governanceSectionNav("history"), "", "", "Filter by principal, status, or statement type", "/ui/governance/query-history", pageReq, total, []string{"Principal", "Statement", "Status", "Created"}, rows))
}

func optionalQueryValue(r *http.Request, key string) *string {
	value := strings.TrimSpace(r.URL.Query().Get(key))
	if value == "" {
		return nil
	}
	return &value
}

func (h *Handler) GovernanceLineage(w http.ResponseWriter, r *http.Request) {
	schema := formString(r.URL.Query(), "schema")
	table := formString(r.URL.Query(), "table")
	column := formString(r.URL.Query(), "column")
	var (
		lineage *domain.LineageNode
		columns []domain.ColumnLineageEdge
		impact  []domain.ColumnLineageEdge
		err     error
	)
	if schema != "" && table != "" {
		qualified := schema + "." + table
		lineage, err = h.Lineage.GetFullLineage(r.Context(), qualified, domain.PageRequest{MaxResults: 100})
		if err != nil {
			h.renderServiceError(w, r, err)
			return
		}
		columns, err = h.Lineage.GetColumnLineageForTable(r.Context(), schema, table)
		if err != nil {
			h.renderServiceError(w, r, err)
			return
		}
		if column != "" {
			impact, err = h.Lineage.GetColumnLineageForSourceColumn(r.Context(), schema, table, column)
			if err != nil {
				h.renderServiceError(w, r, err)
				return
			}
		}
	}
	upstreamRows := []Node{}
	downstreamRows := []Node{}
	columnRows := []Node{}
	impactRows := []Node{}
	if lineage != nil {
		for i := range lineage.Upstream {
			edge := lineage.Upstream[i]
			upstreamRows = append(upstreamRows, Tr(Td(Text(edge.SourceSchema+"."+edge.SourceTable)), Td(Text(strOrDash(edge.TargetTable))), Td(Text(edge.EdgeType)), Td(Class("text-right"), actionMenu("Actions", actionMenuPost("/ui/governance/lineage/edges/"+edge.ID+"/delete", "Delete edge", csrfFieldProvider(r), true)))))
		}
		for i := range lineage.Downstream {
			edge := lineage.Downstream[i]
			downstreamRows = append(downstreamRows, Tr(Td(Text(edge.SourceSchema+"."+edge.SourceTable)), Td(Text(strOrDash(edge.TargetTable))), Td(Text(edge.EdgeType)), Td(Class("text-right"), actionMenu("Actions", actionMenuPost("/ui/governance/lineage/edges/"+edge.ID+"/delete", "Delete edge", csrfFieldProvider(r), true)))))
		}
	}
	for i := range columns {
		edge := columns[i]
		columnRows = append(columnRows, Tr(Td(Text(edge.TargetColumn)), Td(Text(edge.SourceSchema+"."+edge.SourceTable+"."+edge.SourceColumn)), Td(Text(string(edge.TransformType))), Td(Text(edge.Function))))
	}
	for i := range impact {
		edge := impact[i]
		impactRows = append(impactRows, Tr(Td(Text(edge.SourceSchema+"."+edge.SourceTable+"."+edge.SourceColumn)), Td(Text(edge.TargetColumn)), Td(Text(string(edge.TransformType))), Td(Text(edge.Function))))
	}
	renderHTML(w, http.StatusOK, appPage("Governance: Lineage", "governance", principalFromContext(r.Context()),
		governanceSectionNav("lineage"),
		Div(Class(cardClass()),
			Form(Class(stackFormClass()), Method("get"), Action("/ui/governance/lineage"),
				Label(Text("Schema")),
				Input(Name("schema"), Value(schema)),
				Label(Text("Table")),
				Input(Name("table"), Value(table)),
				Label(Text("Source column impact")),
				Input(Name("column"), Value(column)),
				Div(Class(formActionsClass()), Button(Type("submit"), Class(primaryButtonClass()), Text("Load lineage"))),
			),
		),
		Div(Class(cardClass()),
			H2(Text("Purge lineage")),
			Form(Class(stackFormClass()), Method("post"), Action("/ui/governance/lineage/purge"),
				csrfFieldProvider(r)(),
				Label(Text("Delete edges older than days")),
				Input(Name("older_than_days"), Value("30")),
				Div(Class(formActionsClass()), Button(Type("submit"), Class(dangerButtonClass()), Text("Purge lineage"))),
			),
		),
		Div(Class(cardClass(tableWrapClass())), H2(Text("Upstream")), Table(Class(dataTableClass()), THead(Tr(Th(Text("Source")), Th(Text("Target")), Th(Text("Type")), Th(Class("text-right"), Text("Actions")))), TBody(Group(upstreamRows)))),
		Div(Class(cardClass(tableWrapClass())), H2(Text("Downstream")), Table(Class(dataTableClass()), THead(Tr(Th(Text("Source")), Th(Text("Target")), Th(Text("Type")), Th(Class("text-right"), Text("Actions")))), TBody(Group(downstreamRows)))),
		Div(Class(cardClass(tableWrapClass())), H2(Text("Column lineage")), Table(Class(dataTableClass()), THead(Tr(Th(Text("Target Column")), Th(Text("Source Column")), Th(Text("Transform")), Th(Text("Function")))), TBody(Group(columnRows)))),
		Div(Class(cardClass(tableWrapClass())), H2(Text("Column impact")), Table(Class(dataTableClass()), THead(Tr(Th(Text("Source Column")), Th(Text("Target Column")), Th(Text("Transform")), Th(Text("Function")))), TBody(Group(impactRows)))),
	))
}

func (h *Handler) GovernanceLineageDeleteEdge(w http.ResponseWriter, r *http.Request) {
	edgeID := chi.URLParam(r, "edgeID")
	if err := h.Lineage.DeleteEdge(r.Context(), edgeID); err != nil {
		h.renderServiceError(w, r, err)
		return
	}
	redirect := r.Referer()
	if redirect == "" {
		redirect = "/ui/governance/lineage"
	}
	http.Redirect(w, r, redirect, http.StatusSeeOther)
}

func (h *Handler) GovernanceLineagePurge(w http.ResponseWriter, r *http.Request) {
	if !parseFormOrRenderBadRequest(w, r) {
		return
	}
	olderThanDays, err := strconv.Atoi(formString(r.Form, "older_than_days"))
	if err != nil {
		renderHTML(w, http.StatusBadRequest, errorPage("Invalid Request", "older_than_days must be numeric."))
		return
	}
	if _, err := h.Lineage.PurgeOlderThan(r.Context(), olderThanDays); err != nil {
		h.renderServiceError(w, r, err)
		return
	}
	http.Redirect(w, r, "/ui/governance/lineage", http.StatusSeeOther)
}
