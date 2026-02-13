package domain

import "time"

// StorageType identifies the type of cloud storage.
type StorageType string

// Supported storage types for external locations.
const (
	StorageTypeS3    StorageType = "S3"
	StorageTypeAzure StorageType = "AZURE"
	StorageTypeGCS   StorageType = "GCS"
)

// CredentialType identifies the type of credential.
type CredentialType string

// Supported credential types for storage access.
const (
	CredentialTypeS3    CredentialType = "S3"
	CredentialTypeAzure CredentialType = "AZURE"
	CredentialTypeGCS   CredentialType = "GCS"
)

// StorageCredential holds cloud storage credentials.
// Sensitive fields are stored encrypted at rest and decrypted in memory.
type StorageCredential struct {
	ID             string
	Name           string
	CredentialType CredentialType

	// S3 fields
	KeyID    string // plaintext after decryption
	Secret   string // plaintext after decryption
	Endpoint string
	Region   string
	URLStyle string // "path" or "vhost"

	// Azure fields
	AzureAccountName  string
	AzureAccountKey   string // plaintext after decryption
	AzureClientID     string
	AzureTenantID     string
	AzureClientSecret string // plaintext after decryption

	// GCS fields
	GCSKeyFilePath string

	Comment   string
	Owner     string
	CreatedAt time.Time
	UpdatedAt time.Time
}

// ExternalLocation represents a named storage location that can be referenced
// by schemas. Modeled after Unity Catalog's external locations.
type ExternalLocation struct {
	ID             string
	Name           string
	URL            string // e.g. "s3://bucket/prefix/"
	CredentialName string // references StorageCredential.Name
	StorageType    StorageType
	Comment        string
	Owner          string
	ReadOnly       bool
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

// CreateStorageCredentialRequest holds parameters for creating a credential.
type CreateStorageCredentialRequest struct {
	Name           string
	CredentialType CredentialType

	// S3 fields
	KeyID    string
	Secret   string
	Endpoint string
	Region   string
	URLStyle string

	// Azure fields
	AzureAccountName  string
	AzureAccountKey   string
	AzureClientID     string
	AzureTenantID     string
	AzureClientSecret string

	// GCS fields
	GCSKeyFilePath string

	Comment string
}

// UpdateStorageCredentialRequest holds parameters for updating a credential.
type UpdateStorageCredentialRequest struct {
	// S3 fields
	KeyID    *string
	Secret   *string
	Endpoint *string
	Region   *string
	URLStyle *string

	// Azure fields
	AzureAccountName  *string
	AzureAccountKey   *string
	AzureClientID     *string
	AzureTenantID     *string
	AzureClientSecret *string

	// GCS fields
	GCSKeyFilePath *string

	Comment *string
}

// CreateExternalLocationRequest holds parameters for creating a location.
type CreateExternalLocationRequest struct {
	Name           string
	URL            string
	CredentialName string
	StorageType    StorageType
	Comment        string
	ReadOnly       bool
}

// UpdateExternalLocationRequest holds parameters for updating a location.
type UpdateExternalLocationRequest struct {
	URL            *string
	CredentialName *string
	Comment        *string
	ReadOnly       *bool
	Owner          *string
}

// Validate checks that the request is well-formed.
func (r *CreateStorageCredentialRequest) Validate() error {
	return ValidateStorageCredentialRequest(*r)
}

// ValidateStorageCredentialRequest validates a create-credential request.
func ValidateStorageCredentialRequest(req CreateStorageCredentialRequest) error {
	if req.Name == "" {
		return ErrValidation("credential name is required")
	}
	if len(req.Name) > 128 {
		return ErrValidation("credential name must be at most 128 characters")
	}

	switch req.CredentialType {
	case CredentialTypeS3:
		if req.KeyID == "" {
			return ErrValidation("key_id is required for S3 credentials")
		}
		if req.Secret == "" {
			return ErrValidation("secret is required for S3 credentials")
		}
		if req.Endpoint == "" {
			return ErrValidation("endpoint is required for S3 credentials")
		}
		if req.Region == "" {
			return ErrValidation("region is required for S3 credentials")
		}
	case CredentialTypeAzure:
		if req.AzureAccountName == "" {
			return ErrValidation("azure_account_name is required for Azure credentials")
		}
		// Either account key or service principal (client_id + tenant_id + client_secret) must be provided
		hasAccountKey := req.AzureAccountKey != ""
		hasServicePrincipal := req.AzureClientID != "" && req.AzureTenantID != "" && req.AzureClientSecret != ""
		if !hasAccountKey && !hasServicePrincipal {
			return ErrValidation("azure_account_key or (azure_client_id + azure_tenant_id + azure_client_secret) is required for Azure credentials")
		}
	case CredentialTypeGCS:
		if req.GCSKeyFilePath == "" {
			return ErrValidation("gcs_key_file_path is required for GCS credentials")
		}
	default:
		return ErrValidation("unsupported credential type %q; supported: S3, AZURE, GCS", string(req.CredentialType))
	}
	return nil
}

// Validate checks that the request is well-formed.
func (r *CreateExternalLocationRequest) Validate() error {
	return ValidateExternalLocationRequest(*r)
}

// ValidateExternalLocationRequest validates a create-location request.
func ValidateExternalLocationRequest(req CreateExternalLocationRequest) error {
	if req.Name == "" {
		return ErrValidation("location name is required")
	}
	if len(req.Name) > 128 {
		return ErrValidation("location name must be at most 128 characters")
	}
	if req.URL == "" {
		return ErrValidation("url is required")
	}
	if req.CredentialName == "" {
		return ErrValidation("credential_name is required")
	}
	if req.StorageType != "" && req.StorageType != StorageTypeS3 && req.StorageType != StorageTypeAzure && req.StorageType != StorageTypeGCS {
		return ErrValidation("unsupported storage type %q; supported: S3, AZURE, GCS", string(req.StorageType))
	}
	return nil
}
