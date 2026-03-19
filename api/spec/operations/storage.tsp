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

@route("/storage-credentials/{credentialName}")
@get
@tag("Storage")
@summary("Get storage credential")
op getStorageCredential(credentialName: string): Ok<StorageCredential> | ResourceErrorResponses;

@route("/storage-credentials/{credentialName}")
@patch(#{ implicitOptionality: true })
@tag("Storage")
@summary("Update storage credential")
op updateStorageCredential(credentialName: string, @body body: UpdateStorageCredentialRequest): Ok<StorageCredential> | ResourceErrorResponses;

@route("/storage-credentials/{credentialName}")
@delete
@tag("Storage")
@summary("Delete storage credential")
op deleteStorageCredential(credentialName: string): NoContent | ResourceErrorResponses;

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

@route("/external-locations/{locationName}")
@get
@tag("Storage")
@summary("Get external location")
op getExternalLocation(locationName: string): Ok<ExternalLocation> | ResourceErrorResponses;

@route("/external-locations/{locationName}")
@patch(#{ implicitOptionality: true })
@tag("Storage")
@summary("Update external location")
op updateExternalLocation(locationName: string, @body body: UpdateExternalLocationRequest): Ok<ExternalLocation> | ResourceErrorResponses;

@route("/external-locations/{locationName}")
@delete
@tag("Storage")
@summary("Delete external location")
op deleteExternalLocation(locationName: string): NoContent | ResourceErrorResponses;
