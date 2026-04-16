package domain

import "strings"

const (
	// AppSystemSchemaName is the public read-only schema that exposes the
	// control-plane SQLite database for administrative inspection.
	AppSystemSchemaName = "system"

	// TableTypeSystem marks read-only system tables surfaced from DuckDB rather
	// than managed DuckLake objects stored in the metastore.
	TableTypeSystem = "SYSTEM"

	systemObjectIDPrefix = "__system__:"
)

// DuckLakeMetadataSchemaName returns the reserved per-catalog schema that
// exposes a DuckLake catalog's internal metadata tables.
func DuckLakeMetadataSchemaName(catalogName string) string {
	return "_ducklake"
}

// IsDuckLakeSystemSchema reports whether a schema name refers to the attached
// DuckLake metadata schema for the given catalog.
func IsDuckLakeSystemSchema(catalogName, schemaName string) bool {
	return strings.EqualFold(strings.TrimSpace(schemaName), DuckLakeMetadataSchemaName(catalogName))
}

// IsAppSystemSchema reports whether a schema name refers to the public
// read-only application database schema.
func IsAppSystemSchema(schemaName string) bool {
	return strings.EqualFold(strings.TrimSpace(schemaName), AppSystemSchemaName)
}

// IsSystemSchema reports whether a schema name refers to any reserved
// read-only system schema surfaced by the platform.
func IsSystemSchema(catalogName, schemaName string) bool {
	return IsAppSystemSchema(schemaName) || IsDuckLakeSystemSchema(catalogName, schemaName)
}

// SystemSchemaObjectID returns a synthetic raw schema ID for a read-only system schema.
func SystemSchemaObjectID(schemaName string) string {
	return systemObjectIDPrefix + strings.ToLower(strings.TrimSpace(schemaName))
}

// IsSystemSchemaObjectID reports whether an ID represents a synthetic system schema.
func IsSystemSchemaObjectID(id string) bool {
	return strings.HasPrefix(strings.ToLower(strings.TrimSpace(id)), systemObjectIDPrefix)
}

// SystemTableObjectID returns a synthetic raw table ID for a read-only system table.
func SystemTableObjectID(schemaName, tableName string) string {
	return systemObjectIDPrefix + strings.ToLower(strings.TrimSpace(schemaName)) + ":" + strings.ToLower(strings.TrimSpace(tableName))
}

// IsSystemTableObjectID reports whether an ID represents a synthetic system table.
func IsSystemTableObjectID(id string) bool {
	return strings.HasPrefix(strings.ToLower(strings.TrimSpace(id)), systemObjectIDPrefix)
}
