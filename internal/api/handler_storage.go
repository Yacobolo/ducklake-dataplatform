package api

import (
	"context"

	"duck-demo/internal/domain"
)

// storageCredentialService defines the storage credential operations used by the API handler.
type storageCredentialService interface {
	List(ctx context.Context, principal string, page domain.PageRequest) ([]domain.StorageCredential, int64, error)
	Create(ctx context.Context, principal string, req domain.CreateStorageCredentialRequest) (*domain.StorageCredential, error)
	GetByName(ctx context.Context, principal, name string) (*domain.StorageCredential, error)
	Update(ctx context.Context, principal string, name string, req domain.UpdateStorageCredentialRequest) (*domain.StorageCredential, error)
	Delete(ctx context.Context, principal string, name string) error
}

// externalLocationService defines the external location operations used by the API handler.
type externalLocationService interface {
	List(ctx context.Context, principal string, page domain.PageRequest) ([]domain.ExternalLocation, int64, error)
	Create(ctx context.Context, principal string, req domain.CreateExternalLocationRequest) (*domain.ExternalLocation, error)
	GetByName(ctx context.Context, principal, name string) (*domain.ExternalLocation, error)
	Update(ctx context.Context, principal string, name string, req domain.UpdateExternalLocationRequest) (*domain.ExternalLocation, error)
	Delete(ctx context.Context, principal string, name string) error
}

// volumeService defines the volume operations used by the API handler.
type volumeService interface {
	List(ctx context.Context, principal, catalogName string, schemaName string, page domain.PageRequest) ([]domain.Volume, int64, error)
	Create(ctx context.Context, principal, catalogName, schemaName string, req domain.CreateVolumeRequest) (*domain.Volume, error)
	GetByName(ctx context.Context, principal, catalogName string, schemaName, name string) (*domain.Volume, error)
	Update(ctx context.Context, principal, catalogName, schemaName, name string, req domain.UpdateVolumeRequest) (*domain.Volume, error)
	Delete(ctx context.Context, principal, catalogName, schemaName, name string) error
}

// === Storage Credentials ===

// ListStorageCredentials implements the endpoint for listing all storage credentials.
func (h *APIHandler) ListStorageCredentials(ctx context.Context, req GenListStorageCredentialsRequest) (GenListStorageCredentialsResponse, error) {
	page := pageFromParams(req.Params.MaxResults, req.Params.PageToken)
	principal := principalFromCtx(ctx)
	creds, total, err := h.storageCreds.List(ctx, principal, page)
	if err != nil {
		if resp, ok := respondDomainError[GenListStorageCredentialsResponse](err, domainErrorResponder[GenListStorageCredentialsResponse]{
			Forbidden: func(resp ForbiddenJSONResponse) GenListStorageCredentialsResponse {
				return GenListStorageCredentials403JSONResponse{resp}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
	}

	data := make([]StorageCredential, len(creds))
	for i, c := range creds {
		data[i] = storageCredentialToAPI(c)
	}
	nextToken := domain.NextPageToken(page.Offset(), page.Limit(), total)
	return GenListStorageCredentials200JSONResponse{
		Body:    PaginatedStorageCredentials{Data: data, NextPageToken: optStr(nextToken)},
		Headers: GenListStorageCredentials200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// CreateStorageCredential implements the endpoint for creating a new storage credential.
func (h *APIHandler) CreateStorageCredential(ctx context.Context, req GenCreateStorageCredentialRequest) (GenCreateStorageCredentialResponse, error) {
	domReq := domain.CreateStorageCredentialRequest{
		Name: req.Body.Name,
	}
	if req.Body.CredentialType != nil {
		domReq.CredentialType = domain.CredentialType(*req.Body.CredentialType)
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
		domReq.URLStyle = string(*req.Body.UrlStyle)
	}
	if req.Body.Comment != nil {
		domReq.Comment = *req.Body.Comment
	}

	cp, _ := domain.PrincipalFromContext(ctx)
	principal := cp.Name
	result, err := h.storageCreds.Create(ctx, principal, domReq)
	if err != nil {
		if resp, ok := respondDomainError[GenCreateStorageCredentialResponse](err, domainErrorResponder[GenCreateStorageCredentialResponse]{
			BadRequest: func(resp BadRequestJSONResponse) GenCreateStorageCredentialResponse {
				return CreateStorageCredential400JSONResponse{resp}
			},
			Forbidden: func(resp ForbiddenJSONResponse) GenCreateStorageCredentialResponse {
				return CreateStorageCredential403JSONResponse{resp}
			},
			Conflict: func(resp ConflictJSONResponse) GenCreateStorageCredentialResponse {
				return CreateStorageCredential409JSONResponse{resp}
			},
		}); ok {
			return resp, nil
		}
		return CreateStorageCredential400JSONResponse{badRequestErrorResponse(err)}, nil
	}
	return GenCreateStorageCredential201JSONResponse{
		Body:    storageCredentialToAPI(*result),
		Headers: GenCreateStorageCredential201ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// GetStorageCredential implements the endpoint for retrieving a storage credential by name.
func (h *APIHandler) GetStorageCredential(ctx context.Context, req GenGetStorageCredentialRequest) (GenGetStorageCredentialResponse, error) {
	principal := principalFromCtx(ctx)
	result, err := h.storageCreds.GetByName(ctx, principal, req.CredentialName)
	if err != nil {
		if resp, ok := respondDomainError[GenGetStorageCredentialResponse](err, domainErrorResponder[GenGetStorageCredentialResponse]{
			Forbidden: func(resp ForbiddenJSONResponse) GenGetStorageCredentialResponse {
				return GenGetStorageCredential403JSONResponse{resp}
			},
			NotFound: func(resp NotFoundJSONResponse) GenGetStorageCredentialResponse {
				return GenGetStorageCredential404JSONResponse{resp}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
	}
	return GenGetStorageCredential200JSONResponse{
		Body:    storageCredentialToAPI(*result),
		Headers: GenGetStorageCredential200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// UpdateStorageCredential implements the endpoint for updating a storage credential by name.
func (h *APIHandler) UpdateStorageCredential(ctx context.Context, req GenUpdateStorageCredentialRequest) (GenUpdateStorageCredentialResponse, error) {
	domReq := domain.UpdateStorageCredentialRequest{
		// S3 fields
		KeyID:    req.Body.KeyId,
		Secret:   req.Body.Secret,
		Endpoint: req.Body.Endpoint,
		Region:   req.Body.Region,
		Comment:  req.Body.Comment,
	}
	if req.Body.UrlStyle != nil {
		urlStyle := string(*req.Body.UrlStyle)
		domReq.URLStyle = &urlStyle
	}

	cp, _ := domain.PrincipalFromContext(ctx)
	principal := cp.Name
	result, err := h.storageCreds.Update(ctx, principal, req.CredentialName, domReq)
	if err != nil {
		if resp, ok := respondDomainError[GenUpdateStorageCredentialResponse](err, domainErrorResponder[GenUpdateStorageCredentialResponse]{
			Forbidden: func(resp ForbiddenJSONResponse) GenUpdateStorageCredentialResponse {
				return UpdateStorageCredential403JSONResponse{resp}
			},
			NotFound: func(resp NotFoundJSONResponse) GenUpdateStorageCredentialResponse {
				return UpdateStorageCredential404JSONResponse{resp}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
	}
	return GenUpdateStorageCredential200JSONResponse{
		Body:    storageCredentialToAPI(*result),
		Headers: GenUpdateStorageCredential200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// DeleteStorageCredential implements the endpoint for deleting a storage credential by name.
func (h *APIHandler) DeleteStorageCredential(ctx context.Context, req GenDeleteStorageCredentialRequest) (GenDeleteStorageCredentialResponse, error) {
	cp, _ := domain.PrincipalFromContext(ctx)
	principal := cp.Name
	if err := h.storageCreds.Delete(ctx, principal, req.CredentialName); err != nil {
		if resp, ok := respondDomainError[GenDeleteStorageCredentialResponse](err, domainErrorResponder[GenDeleteStorageCredentialResponse]{
			Forbidden: func(resp ForbiddenJSONResponse) GenDeleteStorageCredentialResponse {
				return DeleteStorageCredential403JSONResponse{resp}
			},
			NotFound: func(resp NotFoundJSONResponse) GenDeleteStorageCredentialResponse {
				return DeleteStorageCredential404JSONResponse{resp}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
	}
	return GenDeleteStorageCredential204Response{}, nil
}

// === External Locations ===

// ListExternalLocations implements the endpoint for listing all external locations.
func (h *APIHandler) ListExternalLocations(ctx context.Context, req GenListExternalLocationsRequest) (GenListExternalLocationsResponse, error) {
	page := pageFromParams(req.Params.MaxResults, req.Params.PageToken)
	principal := principalFromCtx(ctx)
	locs, total, err := h.externalLocations.List(ctx, principal, page)
	if err != nil {
		if resp, ok := respondDomainError[GenListExternalLocationsResponse](err, domainErrorResponder[GenListExternalLocationsResponse]{
			Forbidden: func(resp ForbiddenJSONResponse) GenListExternalLocationsResponse {
				return GenListExternalLocations403JSONResponse{resp}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
	}

	data := make([]ExternalLocation, len(locs))
	for i, l := range locs {
		data[i] = externalLocationToAPI(l)
	}
	nextToken := domain.NextPageToken(page.Offset(), page.Limit(), total)
	return GenListExternalLocations200JSONResponse{
		Body:    PaginatedExternalLocations{Data: data, NextPageToken: optStr(nextToken)},
		Headers: GenListExternalLocations200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// CreateExternalLocation implements the endpoint for creating a new external location.
func (h *APIHandler) CreateExternalLocation(ctx context.Context, req GenCreateExternalLocationRequest) (GenCreateExternalLocationResponse, error) {
	domReq := domain.CreateExternalLocationRequest{
		Name: req.Body.Name,
		URL:  req.Body.Url,
	}
	if req.Body.CredentialName != nil {
		domReq.CredentialName = *req.Body.CredentialName
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
		if resp, ok := respondDomainError[GenCreateExternalLocationResponse](err, domainErrorResponder[GenCreateExternalLocationResponse]{
			BadRequest: func(resp BadRequestJSONResponse) GenCreateExternalLocationResponse {
				return CreateExternalLocation400JSONResponse{resp}
			},
			Forbidden: func(resp ForbiddenJSONResponse) GenCreateExternalLocationResponse {
				return CreateExternalLocation403JSONResponse{resp}
			},
			NotFound: func(_ NotFoundJSONResponse) GenCreateExternalLocationResponse {
				return CreateExternalLocation400JSONResponse{badRequestErrorResponse(err)}
			},
			Conflict: func(resp ConflictJSONResponse) GenCreateExternalLocationResponse {
				return CreateExternalLocation409JSONResponse{resp}
			},
		}); ok {
			return resp, nil
		}
		return CreateExternalLocation400JSONResponse{badRequestErrorResponse(err)}, nil
	}
	return GenCreateExternalLocation201JSONResponse{
		Body:    externalLocationToAPI(*result),
		Headers: GenCreateExternalLocation201ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// GetExternalLocation implements the endpoint for retrieving an external location by name.
func (h *APIHandler) GetExternalLocation(ctx context.Context, req GenGetExternalLocationRequest) (GenGetExternalLocationResponse, error) {
	principal := principalFromCtx(ctx)
	result, err := h.externalLocations.GetByName(ctx, principal, req.LocationName)
	if err != nil {
		if resp, ok := respondDomainError[GenGetExternalLocationResponse](err, domainErrorResponder[GenGetExternalLocationResponse]{
			Forbidden: func(resp ForbiddenJSONResponse) GenGetExternalLocationResponse {
				return GenGetExternalLocation403JSONResponse{resp}
			},
			NotFound: func(resp NotFoundJSONResponse) GenGetExternalLocationResponse {
				return GenGetExternalLocation404JSONResponse{resp}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
	}
	return GenGetExternalLocation200JSONResponse{
		Body:    externalLocationToAPI(*result),
		Headers: GenGetExternalLocation200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// UpdateExternalLocation implements the endpoint for updating an external location by name.
func (h *APIHandler) UpdateExternalLocation(ctx context.Context, req GenUpdateExternalLocationRequest) (GenUpdateExternalLocationResponse, error) {
	domReq := domain.UpdateExternalLocationRequest{
		URL:     req.Body.Url,
		Comment: req.Body.Comment,
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
		if resp, ok := respondDomainError[GenUpdateExternalLocationResponse](err, domainErrorResponder[GenUpdateExternalLocationResponse]{
			Forbidden: func(resp ForbiddenJSONResponse) GenUpdateExternalLocationResponse {
				return UpdateExternalLocation403JSONResponse{resp}
			},
			NotFound: func(resp NotFoundJSONResponse) GenUpdateExternalLocationResponse {
				return UpdateExternalLocation404JSONResponse{resp}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
	}
	return GenUpdateExternalLocation200JSONResponse{
		Body:    externalLocationToAPI(*result),
		Headers: GenUpdateExternalLocation200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// DeleteExternalLocation implements the endpoint for deleting an external location by name.
func (h *APIHandler) DeleteExternalLocation(ctx context.Context, req GenDeleteExternalLocationRequest) (GenDeleteExternalLocationResponse, error) {
	cp, _ := domain.PrincipalFromContext(ctx)
	principal := cp.Name
	if err := h.externalLocations.Delete(ctx, principal, req.LocationName); err != nil {
		if resp, ok := respondDomainError[GenDeleteExternalLocationResponse](err, domainErrorResponder[GenDeleteExternalLocationResponse]{
			Forbidden: func(resp ForbiddenJSONResponse) GenDeleteExternalLocationResponse {
				return DeleteExternalLocation403JSONResponse{resp}
			},
			NotFound: func(resp NotFoundJSONResponse) GenDeleteExternalLocationResponse {
				return DeleteExternalLocation404JSONResponse{resp}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
	}
	return GenDeleteExternalLocation204Response{}, nil
}

// === API Mappers for Storage Credentials / External Locations ===

// storageCredentialToAPI converts a domain StorageCredential to the API type.
// IMPORTANT: Never expose key_id, secret, azure_account_key, or azure_client_secret in API responses.
func storageCredentialToAPI(c domain.StorageCredential) StorageCredential {
	resp := StorageCredential{
		Id:             c.ID,
		Name:           c.Name,
		CredentialType: ptrStorageCredentialType(string(c.CredentialType)),
		// S3 fields (non-sensitive)
		Endpoint:  &c.Endpoint,
		Region:    &c.Region,
		UrlStyle:  ptrURLStyle(c.URLStyle),
		Comment:   optStr(c.Comment),
		Owner:     &c.Owner,
		CreatedAt: formatTimePtr(&c.CreatedAt),
		UpdatedAt: formatTimePtr(&c.UpdatedAt),
	}
	return resp
}

func externalLocationToAPI(l domain.ExternalLocation) ExternalLocation {
	return ExternalLocation{
		Id:             l.ID,
		Name:           l.Name,
		Url:            l.URL,
		CredentialName: &l.CredentialName,
		StorageType:    ptrStorageType(string(l.StorageType)),
		Comment:        optStr(l.Comment),
		Owner:          &l.Owner,
		ReadOnly:       &l.ReadOnly,
		CreatedAt:      formatTimePtr(&l.CreatedAt),
		UpdatedAt:      formatTimePtr(&l.UpdatedAt),
	}
}

// === Volumes ===

// ListVolumes implements the endpoint for listing volumes in a schema.
func (h *APIHandler) ListVolumes(ctx context.Context, request GenListVolumesRequest) (GenListVolumesResponse, error) {
	page := pageFromParams(request.Params.MaxResults, request.Params.PageToken)
	principal := principalFromCtx(ctx)
	vols, total, err := h.volumes.List(ctx, principal, string(request.CatalogName), request.SchemaName, page)
	if err != nil {
		return nil, err
	}

	data := make([]VolumeDetail, len(vols))
	for i, v := range vols {
		data[i] = volumeToAPI(v)
	}
	nextToken := domain.NextPageToken(page.Offset(), page.Limit(), total)
	return GenListVolumes200JSONResponse{
		Body:    PaginatedVolumes{Data: data, NextPageToken: optStr(nextToken)},
		Headers: GenListVolumes200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// CreateVolume implements the endpoint for creating a new volume in a schema.
func (h *APIHandler) CreateVolume(ctx context.Context, request GenCreateVolumeRequest) (GenCreateVolumeResponse, error) {
	domReq := domain.CreateVolumeRequest{
		Name: request.Body.Name,
	}
	if request.Body.VolumeType != nil {
		domReq.VolumeType = *request.Body.VolumeType
	}
	if request.Body.StorageLocation != nil {
		domReq.StorageLocation = *request.Body.StorageLocation
	}
	if request.Body.Comment != nil {
		domReq.Comment = *request.Body.Comment
	}

	principal := principalFromCtx(ctx)
	result, err := h.volumes.Create(ctx, principal, string(request.CatalogName), request.SchemaName, domReq)
	if err != nil {
		if resp, ok := respondDomainError[GenCreateVolumeResponse](err, domainErrorResponder[GenCreateVolumeResponse]{
			BadRequest: func(resp BadRequestJSONResponse) GenCreateVolumeResponse {
				return CreateVolume400JSONResponse{resp}
			},
			Forbidden: func(resp ForbiddenJSONResponse) GenCreateVolumeResponse {
				return CreateVolume403JSONResponse{resp}
			},
			Conflict: func(resp ConflictJSONResponse) GenCreateVolumeResponse {
				return CreateVolume409JSONResponse{resp}
			},
		}); ok {
			return resp, nil
		}
		return CreateVolume400JSONResponse{badRequestErrorResponse(err)}, nil
	}
	return GenCreateVolume201JSONResponse{
		Body:    volumeToAPI(*result),
		Headers: GenCreateVolume201ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// GetVolume implements the endpoint for retrieving a volume by name.
func (h *APIHandler) GetVolume(ctx context.Context, request GenGetVolumeRequest) (GenGetVolumeResponse, error) {
	principal := principalFromCtx(ctx)
	result, err := h.volumes.GetByName(ctx, principal, string(request.CatalogName), request.SchemaName, request.VolumeName)
	if err != nil {
		if resp, ok := respondDomainError[GenGetVolumeResponse](err, domainErrorResponder[GenGetVolumeResponse]{
			NotFound: func(resp NotFoundJSONResponse) GenGetVolumeResponse {
				return GenGetVolume404JSONResponse{resp}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
	}
	return GenGetVolume200JSONResponse{
		Body:    volumeToAPI(*result),
		Headers: GenGetVolume200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// UpdateVolume implements the endpoint for updating a volume by name.
func (h *APIHandler) UpdateVolume(ctx context.Context, request GenUpdateVolumeRequest) (GenUpdateVolumeResponse, error) {
	domReq := domain.UpdateVolumeRequest{
		NewName: request.Body.NewName,
		Comment: request.Body.Comment,
	}

	principal := principalFromCtx(ctx)
	result, err := h.volumes.Update(ctx, principal, string(request.CatalogName), request.SchemaName, request.VolumeName, domReq)
	if err != nil {
		if resp, ok := respondDomainError[GenUpdateVolumeResponse](err, domainErrorResponder[GenUpdateVolumeResponse]{
			Forbidden: func(resp ForbiddenJSONResponse) GenUpdateVolumeResponse {
				return UpdateVolume403JSONResponse{resp}
			},
			NotFound: func(resp NotFoundJSONResponse) GenUpdateVolumeResponse {
				return UpdateVolume404JSONResponse{resp}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
	}
	return GenUpdateVolume200JSONResponse{
		Body:    volumeToAPI(*result),
		Headers: GenUpdateVolume200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// DeleteVolume implements the endpoint for deleting a volume by name.
func (h *APIHandler) DeleteVolume(ctx context.Context, request GenDeleteVolumeRequest) (GenDeleteVolumeResponse, error) {
	principal := principalFromCtx(ctx)
	if err := h.volumes.Delete(ctx, principal, string(request.CatalogName), request.SchemaName, request.VolumeName); err != nil {
		if resp, ok := respondDomainError[GenDeleteVolumeResponse](err, domainErrorResponder[GenDeleteVolumeResponse]{
			Forbidden: func(resp ForbiddenJSONResponse) GenDeleteVolumeResponse {
				return DeleteVolume403JSONResponse{resp}
			},
			NotFound: func(resp NotFoundJSONResponse) GenDeleteVolumeResponse {
				return DeleteVolume404JSONResponse{resp}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
	}
	return GenDeleteVolume204Response{}, nil
}

// volumeToAPI converts a domain Volume to the API VolumeDetail type.
func volumeToAPI(v domain.Volume) VolumeDetail {
	return VolumeDetail{
		Id:              v.ID,
		Name:            v.Name,
		SchemaName:      v.SchemaName,
		CatalogName:     v.CatalogName,
		VolumeType:      optStr(v.VolumeType),
		StorageLocation: optStr(v.StorageLocation),
		Comment:         optStr(v.Comment),
		Owner:           &v.Owner,
		CreatedAt:       formatTimePtr(&v.CreatedAt),
		UpdatedAt:       formatTimePtr(&v.UpdatedAt),
	}
}
