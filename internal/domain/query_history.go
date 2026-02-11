package domain

import "time"

// QueryHistoryEntry represents a single query execution record.
type QueryHistoryEntry struct {
	ID             int64
	PrincipalName  string
	OriginalSQL    *string
	RewrittenSQL   *string
	StatementType  *string
	TablesAccessed []string
	Status         string
	ErrorMessage   *string
	DurationMs     *int64
	RowsReturned   *int64
	CreatedAt      time.Time
}

// QueryHistoryFilter holds filter parameters for querying query history.
type QueryHistoryFilter struct {
	PrincipalName *string
	Status        *string
	From          *time.Time
	To            *time.Time
	Page          PageRequest
}
