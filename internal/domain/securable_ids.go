package domain

import "strings"

const (
	syntheticCatalogSchemaIDPrefix = "__catalog_schema__:"
	syntheticCatalogTableIDPrefix  = "__catalog_table__:"
)

// SyntheticCatalogSchemaID namespaces an attached-catalog schema ID into the
// shared control plane so grants and governance bindings do not collide with
// local metastore IDs from other catalogs.
func SyntheticCatalogSchemaID(catalogName, schemaID string) string {
	catalogName = strings.TrimSpace(catalogName)
	schemaID = strings.TrimSpace(schemaID)
	if catalogName == "" || schemaID == "" || IsPrimaryCatalog(catalogName) {
		return schemaID
	}
	return syntheticCatalogSchemaIDPrefix + catalogName + ":" + schemaID
}

// ParseSyntheticCatalogSchemaID extracts the attached catalog name and raw
// schema ID from a namespaced schema identifier.
func ParseSyntheticCatalogSchemaID(id string) (catalogName, schemaID string, ok bool) {
	if !strings.HasPrefix(id, syntheticCatalogSchemaIDPrefix) {
		return "", "", false
	}
	rest := strings.TrimPrefix(id, syntheticCatalogSchemaIDPrefix)
	parts := strings.SplitN(rest, ":", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return "", "", false
	}
	return parts[0], parts[1], true
}

// SyntheticCatalogTableID namespaces an attached-catalog table ID into the
// shared control plane. The raw schema ID is embedded so callers can recover
// parent-schema lineage when evaluating inherited privileges.
func SyntheticCatalogTableID(catalogName, schemaID, tableID string) string {
	catalogName = strings.TrimSpace(catalogName)
	schemaID = strings.TrimSpace(schemaID)
	tableID = strings.TrimSpace(tableID)
	if catalogName == "" || schemaID == "" || tableID == "" || IsPrimaryCatalog(catalogName) {
		return tableID
	}
	return syntheticCatalogTableIDPrefix + catalogName + ":" + schemaID + ":" + tableID
}

// ParseSyntheticCatalogTableID extracts the attached catalog name, raw schema
// ID, and raw table ID from a namespaced table identifier.
func ParseSyntheticCatalogTableID(id string) (catalogName, schemaID, tableID string, ok bool) {
	if !strings.HasPrefix(id, syntheticCatalogTableIDPrefix) {
		return "", "", "", false
	}
	rest := strings.TrimPrefix(id, syntheticCatalogTableIDPrefix)
	parts := strings.SplitN(rest, ":", 3)
	if len(parts) != 3 || parts[0] == "" || parts[1] == "" || parts[2] == "" {
		return "", "", "", false
	}
	return parts[0], parts[1], parts[2], true
}
