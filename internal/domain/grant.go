package domain

import "time"

// Privilege constants matching the Databricks/Hive model.
const (
	PrivSelect        = "SELECT"
	PrivInsert        = "INSERT"
	PrivUpdate        = "UPDATE"
	PrivDelete        = "DELETE"
	PrivUsage         = "USAGE"
	PrivCreateTable   = "CREATE_TABLE"
	PrivCreateSchema  = "CREATE_SCHEMA"
	PrivAllPrivileges = "ALL_PRIVILEGES"

	// Storage & governance privileges.
	PrivCreateExternalLocation  = "CREATE_EXTERNAL_LOCATION"
	PrivCreateStorageCredential = "CREATE_STORAGE_CREDENTIAL"
	PrivCreateVolume            = "CREATE_VOLUME"
	PrivReadVolume              = "READ_VOLUME"
	PrivWriteVolume             = "WRITE_VOLUME"
	PrivReadFiles               = "READ_FILES"
	PrivWriteFiles              = "WRITE_FILES"
	PrivManageCompute           = "MANAGE_COMPUTE"
	PrivManagePipelines         = "MANAGE_PIPELINES"
)

// Securable type constants.
const (
	SecurableCatalog           = "catalog"
	SecurableSchema            = "schema"
	SecurableTable             = "table"
	SecurableExternalLocation  = "external_location"
	SecurableStorageCredential = "storage_credential"
	SecurableVolume            = "volume"
)

// CatalogID is the sentinel securable_id for catalog-level grants.
const CatalogID = "00000000-0000-0000-0000-000000000000"

// PrivilegeGrant represents a privilege granted to a principal on a securable.
type PrivilegeGrant struct {
	ID            string
	PrincipalID   string
	PrincipalType string // "user" or "group"
	SecurableType string // "catalog", "schema", "table"
	SecurableID   string
	Privilege     string
	GrantedBy     *string
	GrantedAt     time.Time
}

// CreateGrantRequest holds parameters for granting a privilege.
type CreateGrantRequest struct {
	PrincipalID   string
	PrincipalType string // "user" or "group"
	SecurableType string // "catalog", "schema", "table"
	SecurableID   string
	Privilege     string
}

// Validate checks that the request is well-formed.
func (r *CreateGrantRequest) Validate() error {
	if r.PrincipalID == "" {
		return ErrValidation("principal_id is required")
	}
	if r.PrincipalType != "user" && r.PrincipalType != "group" {
		return ErrValidation("principal_type must be 'user' or 'group'")
	}
	if r.SecurableType == "" {
		return ErrValidation("securable_type is required")
	}
	if r.SecurableID == "" {
		return ErrValidation("securable_id is required")
	}
	if r.Privilege == "" {
		return ErrValidation("privilege is required")
	}
	return nil
}
