package domain

// SearchResult represents a single catalog search result.
type SearchResult struct {
	Type       string // "schema", "table", "column"
	Name       string
	SchemaName *string
	TableName  *string
	Comment    *string
	MatchField string // "name", "comment", "property"
}
