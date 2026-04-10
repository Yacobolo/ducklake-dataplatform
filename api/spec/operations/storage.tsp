using TypeSpec.Http;

namespace Duck;

@route("/storage-credentials")
@get
@tag("Storage")
@summary("List storage credentials")
op listStorageCredentials(@query max_results?: int32, @query page_token?: string): Ok<PaginatedStorageCredentials> | StandardErrorResponses;

@route("/storage-credentials")
@post
@tag("Storage")
@summary("Create storage credential")
@doc("Creates a reusable storage credential that can be referenced by external locations and managed catalogs.")
op createStorageCredential(@body body: CreateStorageCredentialRequest): Created<StorageCredential> | MutatingErrorResponses;

@route("/storage-credentials/{credential_name}")
@get
@tag("Storage")
@summary("Get storage credential")
op getStorageCredential(credential_name: string): Ok<StorageCredential> | ResourceErrorResponses;

@route("/storage-credentials/{credential_name}")
@patch(#{ implicitOptionality: true })
@tag("Storage")
@summary("Update storage credential")
op updateStorageCredential(credential_name: string, @body body: UpdateStorageCredentialRequest): Ok<StorageCredential> | ResourceErrorResponses;

@route("/storage-credentials/{credential_name}")
@delete
@tag("Storage")
@summary("Delete storage credential")
op deleteStorageCredential(credential_name: string): NoContent | ResourceErrorResponses;

@route("/external-locations")
@get
@tag("Storage")
@summary("List external locations")
op listExternalLocations(@query max_results?: int32, @query page_token?: string): Ok<PaginatedExternalLocations> | StandardErrorResponses;

@route("/external-locations")
@post
@doc("Creates a new external location and configures DuckDB with the associated credential. Catalog registrations are managed separately; creating a location does not attach or create a DuckLake catalog.")
@tag("Storage")
@summary("Create external location")
op createExternalLocation(@body body: CreateExternalLocationRequest): Created<ExternalLocation> | MutatingErrorResponses;

@route("/external-locations/{location_name}")
@get
@tag("Storage")
@summary("Get external location")
op getExternalLocation(location_name: string): Ok<ExternalLocation> | ResourceErrorResponses;

@route("/external-locations/{location_name}")
@patch(#{ implicitOptionality: true })
@tag("Storage")
@summary("Update external location")
op updateExternalLocation(location_name: string, @body body: UpdateExternalLocationRequest): Ok<ExternalLocation> | ResourceErrorResponses;

@route("/external-locations/{location_name}")
@delete
@tag("Storage")
@summary("Delete external location")
op deleteExternalLocation(location_name: string): NoContent | ResourceErrorResponses;
