package domain

import "time"

// StorageType identifies the type of cloud storage.
type StorageType string

const (
	StorageTypeS3 StorageType = "S3"
)

// CredentialType identifies the type of credential.
type CredentialType string

const (
	CredentialTypeS3 CredentialType = "S3"
)

// StorageCredential holds S3-compatible credentials.
// KeyID and Secret are stored encrypted at rest and decrypted in memory.
type StorageCredential struct {
	ID             int64
	Name           string
	CredentialType CredentialType
	KeyID          string // plaintext after decryption
	Secret         string // plaintext after decryption
	Endpoint       string
	Region         string
	URLStyle       string // "path" or "vhost"
	Comment        string
	Owner          string
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

// ExternalLocation represents a named storage location that can be referenced
// by schemas. Modeled after Unity Catalog's external locations.
type ExternalLocation struct {
	ID             int64
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
	KeyID          string
	Secret         string
	Endpoint       string
	Region         string
	URLStyle       string
	Comment        string
}

// UpdateStorageCredentialRequest holds parameters for updating a credential.
type UpdateStorageCredentialRequest struct {
	KeyID    *string
	Secret   *string
	Endpoint *string
	Region   *string
	URLStyle *string
	Comment  *string
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

// ValidateStorageCredentialRequest validates a create-credential request.
func ValidateStorageCredentialRequest(req CreateStorageCredentialRequest) error {
	if req.Name == "" {
		return ErrValidation("credential name is required")
	}
	if len(req.Name) > 128 {
		return ErrValidation("credential name must be at most 128 characters")
	}
	if req.CredentialType != CredentialTypeS3 {
		return ErrValidation("unsupported credential type %q; supported: S3", string(req.CredentialType))
	}
	if req.KeyID == "" {
		return ErrValidation("key_id is required")
	}
	if req.Secret == "" {
		return ErrValidation("secret is required")
	}
	if req.Endpoint == "" {
		return ErrValidation("endpoint is required")
	}
	if req.Region == "" {
		return ErrValidation("region is required")
	}
	return nil
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
	if req.StorageType != "" && req.StorageType != StorageTypeS3 {
		return ErrValidation("unsupported storage type %q; supported: S3", string(req.StorageType))
	}
	return nil
}
