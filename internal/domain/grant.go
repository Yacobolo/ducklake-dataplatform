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
const CatalogID int64 = 0

// PrivilegeGrant represents a privilege granted to a principal on a securable.
type PrivilegeGrant struct {
	ID            int64
	PrincipalID   int64
	PrincipalType string // "user" or "group"
	SecurableType string // "catalog", "schema", "table"
	SecurableID   int64
	Privilege     string
	GrantedBy     *int64
	GrantedAt     time.Time
}
