package api

import (
	"context"
	"errors"

	"duck-demo/internal/domain"
)

// storageCredentialService defines the storage credential operations used by the API handler.
type storageCredentialService interface {
	List(ctx context.Context, page domain.PageRequest) ([]domain.StorageCredential, int64, error)
	Create(ctx context.Context, principal string, req domain.CreateStorageCredentialRequest) (*domain.StorageCredential, error)
	GetByName(ctx context.Context, name string) (*domain.StorageCredential, error)
	Update(ctx context.Context, principal string, name string, req domain.UpdateStorageCredentialRequest) (*domain.StorageCredential, error)
	Delete(ctx context.Context, principal string, name string) error
}

// externalLocationService defines the external location operations used by the API handler.
type externalLocationService interface {
	List(ctx context.Context, page domain.PageRequest) ([]domain.ExternalLocation, int64, error)
	Create(ctx context.Context, principal string, req domain.CreateExternalLocationRequest) (*domain.ExternalLocation, error)
	GetByName(ctx context.Context, name string) (*domain.ExternalLocation, error)
	Update(ctx context.Context, principal string, name string, req domain.UpdateExternalLocationRequest) (*domain.ExternalLocation, error)
	Delete(ctx context.Context, principal string, name string) error
}

// volumeService defines the volume operations used by the API handler.
type volumeService interface {
	List(ctx context.Context, catalogName string, schemaName string, page domain.PageRequest) ([]domain.Volume, int64, error)
	Create(ctx context.Context, catalogName string, principal, schemaName string, req domain.CreateVolumeRequest) (*domain.Volume, error)
	GetByName(ctx context.Context, catalogName string, schemaName, name string) (*domain.Volume, error)
	Update(ctx context.Context, catalogName string, principal, schemaName, name string, req domain.UpdateVolumeRequest) (*domain.Volume, error)
	Delete(ctx context.Context, catalogName string, principal, schemaName, name string) error
}

// === Storage Credentials ===

// ListStorageCredentials implements the endpoint for listing all storage credentials.
func (h *APIHandler) ListStorageCredentials(ctx context.Context, req ListStorageCredentialsRequestObject) (ListStorageCredentialsResponseObject, error) {
	page := pageFromParams(req.Params.MaxResults, req.Params.PageToken)
	creds, total, err := h.storageCreds.List(ctx, page)
	if err != nil {
		return nil, err
	}

	data := make([]StorageCredential, len(creds))
	for i, c := range creds {
		data[i] = storageCredentialToAPI(c)
	}
	nextToken := domain.NextPageToken(page.Offset(), page.Limit(), total)
	return ListStorageCredentials200JSONResponse{
		Body:    PaginatedStorageCredentials{Data: &data, NextPageToken: optStr(nextToken)},
		Headers: ListStorageCredentials200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// CreateStorageCredential implements the endpoint for creating a new storage credential.
func (h *APIHandler) CreateStorageCredential(ctx context.Context, req CreateStorageCredentialRequestObject) (CreateStorageCredentialResponseObject, error) {
	domReq := domain.CreateStorageCredentialRequest{
		Name:           req.Body.Name,
		CredentialType: domain.CredentialType(req.Body.CredentialType),
	}
	// S3 fields
	if req.Body.KeyId != nil {
		domReq.KeyID = *req.Body.KeyId
	}
	if req.Body.Secret != nil {
		domReq.Secret = *req.Body.Secret
	}
	if req.Body.Endpoint != nil {
		domReq.Endpoint = *req.Body.Endpoint
	}
	if req.Body.Region != nil {
		domReq.Region = *req.Body.Region
	}
	if req.Body.UrlStyle != nil {
		domReq.URLStyle = *req.Body.UrlStyle
	} else {
		domReq.URLStyle = "path"
	}
	// Azure fields
	if req.Body.AzureAccountName != nil {
		domReq.AzureAccountName = *req.Body.AzureAccountName
	}
	if req.Body.AzureAccountKey != nil {
		domReq.AzureAccountKey = *req.Body.AzureAccountKey
	}
	if req.Body.AzureClientId != nil {
		domReq.AzureClientID = *req.Body.AzureClientId
	}
	if req.Body.AzureTenantId != nil {
		domReq.AzureTenantID = *req.Body.AzureTenantId
	}
	if req.Body.AzureClientSecret != nil {
		domReq.AzureClientSecret = *req.Body.AzureClientSecret
	}
	// GCS fields
	if req.Body.GcsKeyFilePath != nil {
		domReq.GCSKeyFilePath = *req.Body.GcsKeyFilePath
	}
	if req.Body.Comment != nil {
		domReq.Comment = *req.Body.Comment
	}

	cp, _ := domain.PrincipalFromContext(ctx)
	principal := cp.Name
	result, err := h.storageCreds.Create(ctx, principal, domReq)
	if err != nil {
		var accessErr *domain.AccessDeniedError
		var validErr *domain.ValidationError
		var conflictErr *domain.ConflictError
		switch {
		case errors.As(err, &accessErr):
			return CreateStorageCredential403JSONResponse{ForbiddenJSONResponse{Body: Error{Code: 403, Message: err.Error()}, Headers: ForbiddenResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		case errors.As(err, &validErr):
			return CreateStorageCredential400JSONResponse{BadRequestJSONResponse{Body: Error{Code: 400, Message: err.Error()}, Headers: BadRequestResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		case errors.As(err, &conflictErr):
			return CreateStorageCredential409JSONResponse{ConflictJSONResponse{Body: Error{Code: 409, Message: err.Error()}, Headers: ConflictResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return CreateStorageCredential400JSONResponse{BadRequestJSONResponse{Body: Error{Code: 400, Message: err.Error()}, Headers: BadRequestResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		}
	}
	return CreateStorageCredential201JSONResponse{
		Body:    storageCredentialToAPI(*result),
		Headers: CreateStorageCredential201ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// GetStorageCredential implements the endpoint for retrieving a storage credential by name.
func (h *APIHandler) GetStorageCredential(ctx context.Context, req GetStorageCredentialRequestObject) (GetStorageCredentialResponseObject, error) {
	result, err := h.storageCreds.GetByName(ctx, req.CredentialName)
	if err != nil {
		switch {
		case errors.As(err, new(*domain.NotFoundError)):
			return GetStorageCredential404JSONResponse{NotFoundJSONResponse{Body: Error{Code: 404, Message: err.Error()}, Headers: NotFoundResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return nil, err
		}
	}
	return GetStorageCredential200JSONResponse{
		Body:    storageCredentialToAPI(*result),
		Headers: GetStorageCredential200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// UpdateStorageCredential implements the endpoint for updating a storage credential by name.
func (h *APIHandler) UpdateStorageCredential(ctx context.Context, req UpdateStorageCredentialRequestObject) (UpdateStorageCredentialResponseObject, error) {
	domReq := domain.UpdateStorageCredentialRequest{
		// S3 fields
		KeyID:    req.Body.KeyId,
		Secret:   req.Body.Secret,
		Endpoint: req.Body.Endpoint,
		Region:   req.Body.Region,
		URLStyle: req.Body.UrlStyle,
		// Azure fields
		AzureAccountName:  req.Body.AzureAccountName,
		AzureAccountKey:   req.Body.AzureAccountKey,
		AzureClientID:     req.Body.AzureClientId,
		AzureTenantID:     req.Body.AzureTenantId,
		AzureClientSecret: req.Body.AzureClientSecret,
		// GCS fields
		GCSKeyFilePath: req.Body.GcsKeyFilePath,
		Comment:        req.Body.Comment,
	}

	cp, _ := domain.PrincipalFromContext(ctx)
	principal := cp.Name
	result, err := h.storageCreds.Update(ctx, principal, req.CredentialName, domReq)
	if err != nil {
		switch {
		case errors.As(err, new(*domain.AccessDeniedError)):
			return UpdateStorageCredential403JSONResponse{ForbiddenJSONResponse{Body: Error{Code: 403, Message: err.Error()}, Headers: ForbiddenResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		case errors.As(err, new(*domain.NotFoundError)):
			return UpdateStorageCredential404JSONResponse{NotFoundJSONResponse{Body: Error{Code: 404, Message: err.Error()}, Headers: NotFoundResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return nil, err
		}
	}
	return UpdateStorageCredential200JSONResponse{
		Body:    storageCredentialToAPI(*result),
		Headers: UpdateStorageCredential200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// DeleteStorageCredential implements the endpoint for deleting a storage credential by name.
func (h *APIHandler) DeleteStorageCredential(ctx context.Context, req DeleteStorageCredentialRequestObject) (DeleteStorageCredentialResponseObject, error) {
	cp, _ := domain.PrincipalFromContext(ctx)
	principal := cp.Name
	if err := h.storageCreds.Delete(ctx, principal, req.CredentialName); err != nil {
		switch {
		case errors.As(err, new(*domain.AccessDeniedError)):
			return DeleteStorageCredential403JSONResponse{ForbiddenJSONResponse{Body: Error{Code: 403, Message: err.Error()}, Headers: ForbiddenResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		case errors.As(err, new(*domain.NotFoundError)):
			return DeleteStorageCredential404JSONResponse{NotFoundJSONResponse{Body: Error{Code: 404, Message: err.Error()}, Headers: NotFoundResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return nil, err
		}
	}
	return DeleteStorageCredential204Response{}, nil
}

// === External Locations ===

// ListExternalLocations implements the endpoint for listing all external locations.
func (h *APIHandler) ListExternalLocations(ctx context.Context, req ListExternalLocationsRequestObject) (ListExternalLocationsResponseObject, error) {
	page := pageFromParams(req.Params.MaxResults, req.Params.PageToken)
	locs, total, err := h.externalLocations.List(ctx, page)
	if err != nil {
		return nil, err
	}

	data := make([]ExternalLocation, len(locs))
	for i, l := range locs {
		data[i] = externalLocationToAPI(l)
	}
	nextToken := domain.NextPageToken(page.Offset(), page.Limit(), total)
	return ListExternalLocations200JSONResponse{
		Body:    PaginatedExternalLocations{Data: &data, NextPageToken: optStr(nextToken)},
		Headers: ListExternalLocations200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// CreateExternalLocation implements the endpoint for creating a new external location.
func (h *APIHandler) CreateExternalLocation(ctx context.Context, req CreateExternalLocationRequestObject) (CreateExternalLocationResponseObject, error) {
	domReq := domain.CreateExternalLocationRequest{
		Name:           req.Body.Name,
		URL:            req.Body.Url,
		CredentialName: req.Body.CredentialName,
	}
	if req.Body.StorageType != nil {
		domReq.StorageType = domain.StorageType(*req.Body.StorageType)
	}
	if req.Body.Comment != nil {
		domReq.Comment = *req.Body.Comment
	}
	if req.Body.ReadOnly != nil {
		domReq.ReadOnly = *req.Body.ReadOnly
	}

	cp, _ := domain.PrincipalFromContext(ctx)
	principal := cp.Name
	result, err := h.externalLocations.Create(ctx, principal, domReq)
	if err != nil {
		var accessErr *domain.AccessDeniedError
		var validErr *domain.ValidationError
		var conflictErr *domain.ConflictError
		var notFoundErr *domain.NotFoundError
		switch {
		case errors.As(err, &accessErr):
			return CreateExternalLocation403JSONResponse{ForbiddenJSONResponse{Body: Error{Code: 403, Message: err.Error()}, Headers: ForbiddenResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		case errors.As(err, &validErr):
			return CreateExternalLocation400JSONResponse{BadRequestJSONResponse{Body: Error{Code: 400, Message: err.Error()}, Headers: BadRequestResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		case errors.As(err, &conflictErr):
			return CreateExternalLocation409JSONResponse{ConflictJSONResponse{Body: Error{Code: 409, Message: err.Error()}, Headers: ConflictResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		case errors.As(err, &notFoundErr):
			// Referenced credential not found — report as 400 (bad request)
			return CreateExternalLocation400JSONResponse{BadRequestJSONResponse{Body: Error{Code: 400, Message: err.Error()}, Headers: BadRequestResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return CreateExternalLocation400JSONResponse{BadRequestJSONResponse{Body: Error{Code: 400, Message: err.Error()}, Headers: BadRequestResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		}
	}
	return CreateExternalLocation201JSONResponse{
		Body:    externalLocationToAPI(*result),
		Headers: CreateExternalLocation201ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// GetExternalLocation implements the endpoint for retrieving an external location by name.
func (h *APIHandler) GetExternalLocation(ctx context.Context, req GetExternalLocationRequestObject) (GetExternalLocationResponseObject, error) {
	result, err := h.externalLocations.GetByName(ctx, req.LocationName)
	if err != nil {
		switch {
		case errors.As(err, new(*domain.NotFoundError)):
			return GetExternalLocation404JSONResponse{NotFoundJSONResponse{Body: Error{Code: 404, Message: err.Error()}, Headers: NotFoundResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return nil, err
		}
	}
	return GetExternalLocation200JSONResponse{
		Body:    externalLocationToAPI(*result),
		Headers: GetExternalLocation200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// UpdateExternalLocation implements the endpoint for updating an external location by name.
func (h *APIHandler) UpdateExternalLocation(ctx context.Context, req UpdateExternalLocationRequestObject) (UpdateExternalLocationResponseObject, error) {
	domReq := domain.UpdateExternalLocationRequest{
		URL:     req.Body.Url,
		Comment: req.Body.Comment,
		Owner:   req.Body.Owner,
	}
	if req.Body.CredentialName != nil {
		domReq.CredentialName = req.Body.CredentialName
	}
	if req.Body.ReadOnly != nil {
		domReq.ReadOnly = req.Body.ReadOnly
	}

	cp, _ := domain.PrincipalFromContext(ctx)
	principal := cp.Name
	result, err := h.externalLocations.Update(ctx, principal, req.LocationName, domReq)
	if err != nil {
		switch {
		case errors.As(err, new(*domain.AccessDeniedError)):
			return UpdateExternalLocation403JSONResponse{ForbiddenJSONResponse{Body: Error{Code: 403, Message: err.Error()}, Headers: ForbiddenResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		case errors.As(err, new(*domain.NotFoundError)):
			return UpdateExternalLocation404JSONResponse{NotFoundJSONResponse{Body: Error{Code: 404, Message: err.Error()}, Headers: NotFoundResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return nil, err
		}
	}
	return UpdateExternalLocation200JSONResponse{
		Body:    externalLocationToAPI(*result),
		Headers: UpdateExternalLocation200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// DeleteExternalLocation implements the endpoint for deleting an external location by name.
func (h *APIHandler) DeleteExternalLocation(ctx context.Context, req DeleteExternalLocationRequestObject) (DeleteExternalLocationResponseObject, error) {
	cp, _ := domain.PrincipalFromContext(ctx)
	principal := cp.Name
	if err := h.externalLocations.Delete(ctx, principal, req.LocationName); err != nil {
		switch {
		case errors.As(err, new(*domain.AccessDeniedError)):
			return DeleteExternalLocation403JSONResponse{ForbiddenJSONResponse{Body: Error{Code: 403, Message: err.Error()}, Headers: ForbiddenResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		case errors.As(err, new(*domain.NotFoundError)):
			return DeleteExternalLocation404JSONResponse{NotFoundJSONResponse{Body: Error{Code: 404, Message: err.Error()}, Headers: NotFoundResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return nil, err
		}
	}
	return DeleteExternalLocation204Response{}, nil
}

// === API Mappers for Storage Credentials / External Locations ===

// storageCredentialToAPI converts a domain StorageCredential to the API type.
// IMPORTANT: Never expose key_id, secret, azure_account_key, or azure_client_secret in API responses.
func storageCredentialToAPI(c domain.StorageCredential) StorageCredential {
	ct := StorageCredentialCredentialType(c.CredentialType)
	resp := StorageCredential{
		Id:             &c.ID,
		Name:           &c.Name,
		CredentialType: &ct,
		// S3 fields (non-sensitive)
		Endpoint: &c.Endpoint,
		Region:   &c.Region,
		UrlStyle: &c.URLStyle,
		// Azure fields (non-sensitive only)
		AzureAccountName: optStr(c.AzureAccountName),
		AzureClientId:    optStr(c.AzureClientID),
		AzureTenantId:    optStr(c.AzureTenantID),
		// GCS fields
		GcsKeyFilePath: optStr(c.GCSKeyFilePath),
		Comment:        optStr(c.Comment),
		Owner:          &c.Owner,
		CreatedAt:      &c.CreatedAt,
		UpdatedAt:      &c.UpdatedAt,
	}
	return resp
}

func externalLocationToAPI(l domain.ExternalLocation) ExternalLocation {
	st := string(l.StorageType)
	return ExternalLocation{
		Id:             &l.ID,
		Name:           &l.Name,
		Url:            &l.URL,
		CredentialName: &l.CredentialName,
		StorageType:    &st,
		Comment:        optStr(l.Comment),
		Owner:          &l.Owner,
		ReadOnly:       &l.ReadOnly,
		CreatedAt:      &l.CreatedAt,
		UpdatedAt:      &l.UpdatedAt,
	}
}

// === Volumes ===

// ListVolumes implements the endpoint for listing volumes in a schema.
func (h *APIHandler) ListVolumes(ctx context.Context, request ListVolumesRequestObject) (ListVolumesResponseObject, error) {
	page := pageFromParams(request.Params.MaxResults, request.Params.PageToken)
	vols, total, err := h.volumes.List(ctx, string(request.CatalogName), request.SchemaName, page)
	if err != nil {
		return nil, err
	}

	data := make([]VolumeDetail, len(vols))
	for i, v := range vols {
		data[i] = volumeToAPI(v)
	}
	nextToken := domain.NextPageToken(page.Offset(), page.Limit(), total)
	return ListVolumes200JSONResponse{
		Body:    PaginatedVolumes{Data: &data, NextPageToken: optStr(nextToken)},
		Headers: ListVolumes200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// CreateVolume implements the endpoint for creating a new volume in a schema.
func (h *APIHandler) CreateVolume(ctx context.Context, request CreateVolumeRequestObject) (CreateVolumeResponseObject, error) {
	domReq := domain.CreateVolumeRequest{
		Name:       request.Body.Name,
		VolumeType: string(request.Body.VolumeType),
	}
	if request.Body.StorageLocation != nil {
		domReq.StorageLocation = *request.Body.StorageLocation
	}
	if request.Body.Comment != nil {
		domReq.Comment = *request.Body.Comment
	}

	principal := principalFromCtx(ctx)
	result, err := h.volumes.Create(ctx, string(request.CatalogName), principal, request.SchemaName, domReq)
	if err != nil {
		var accessErr *domain.AccessDeniedError
		var validErr *domain.ValidationError
		var conflictErr *domain.ConflictError
		switch {
		case errors.As(err, &accessErr):
			return CreateVolume403JSONResponse{ForbiddenJSONResponse{Body: Error{Code: 403, Message: err.Error()}, Headers: ForbiddenResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		case errors.As(err, &validErr):
			return CreateVolume400JSONResponse{BadRequestJSONResponse{Body: Error{Code: 400, Message: err.Error()}, Headers: BadRequestResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		case errors.As(err, &conflictErr):
			return CreateVolume409JSONResponse{ConflictJSONResponse{Body: Error{Code: 409, Message: err.Error()}, Headers: ConflictResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return CreateVolume400JSONResponse{BadRequestJSONResponse{Body: Error{Code: 400, Message: err.Error()}, Headers: BadRequestResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		}
	}
	return CreateVolume201JSONResponse{
		Body:    volumeToAPI(*result),
		Headers: CreateVolume201ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// GetVolume implements the endpoint for retrieving a volume by name.
func (h *APIHandler) GetVolume(ctx context.Context, request GetVolumeRequestObject) (GetVolumeResponseObject, error) {
	result, err := h.volumes.GetByName(ctx, string(request.CatalogName), request.SchemaName, request.VolumeName)
	if err != nil {
		switch {
		case errors.As(err, new(*domain.NotFoundError)):
			return GetVolume404JSONResponse{NotFoundJSONResponse{Body: Error{Code: 404, Message: err.Error()}, Headers: NotFoundResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return nil, err
		}
	}
	return GetVolume200JSONResponse{
		Body:    volumeToAPI(*result),
		Headers: GetVolume200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// UpdateVolume implements the endpoint for updating a volume by name.
func (h *APIHandler) UpdateVolume(ctx context.Context, request UpdateVolumeRequestObject) (UpdateVolumeResponseObject, error) {
	domReq := domain.UpdateVolumeRequest{
		NewName: request.Body.NewName,
		Comment: request.Body.Comment,
		Owner:   request.Body.Owner,
	}

	principal := principalFromCtx(ctx)
	result, err := h.volumes.Update(ctx, string(request.CatalogName), principal, request.SchemaName, request.VolumeName, domReq)
	if err != nil {
		switch {
		case errors.As(err, new(*domain.AccessDeniedError)):
			return UpdateVolume403JSONResponse{ForbiddenJSONResponse{Body: Error{Code: 403, Message: err.Error()}, Headers: ForbiddenResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		case errors.As(err, new(*domain.NotFoundError)):
			return UpdateVolume404JSONResponse{NotFoundJSONResponse{Body: Error{Code: 404, Message: err.Error()}, Headers: NotFoundResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return nil, err
		}
	}
	return UpdateVolume200JSONResponse{
		Body:    volumeToAPI(*result),
		Headers: UpdateVolume200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// DeleteVolume implements the endpoint for deleting a volume by name.
func (h *APIHandler) DeleteVolume(ctx context.Context, request DeleteVolumeRequestObject) (DeleteVolumeResponseObject, error) {
	principal := principalFromCtx(ctx)
	if err := h.volumes.Delete(ctx, string(request.CatalogName), principal, request.SchemaName, request.VolumeName); err != nil {
		switch {
		case errors.As(err, new(*domain.AccessDeniedError)):
			return DeleteVolume403JSONResponse{ForbiddenJSONResponse{Body: Error{Code: 403, Message: err.Error()}, Headers: ForbiddenResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		case errors.As(err, new(*domain.NotFoundError)):
			return DeleteVolume404JSONResponse{NotFoundJSONResponse{Body: Error{Code: 404, Message: err.Error()}, Headers: NotFoundResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return nil, err
		}
	}
	return DeleteVolume204Response{}, nil
}

// volumeToAPI converts a domain Volume to the API VolumeDetail type.
func volumeToAPI(v domain.Volume) VolumeDetail {
	vt := VolumeDetailVolumeType(v.VolumeType)
	return VolumeDetail{
		Id:              &v.ID,
		Name:            &v.Name,
		SchemaName:      &v.SchemaName,
		CatalogName:     &v.CatalogName,
		VolumeType:      &vt,
		StorageLocation: optStr(v.StorageLocation),
		Comment:         optStr(v.Comment),
		Owner:           &v.Owner,
		CreatedAt:       &v.CreatedAt,
		UpdatedAt:       &v.UpdatedAt,
	}
}
