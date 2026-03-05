//nolint:revive // asset-centric domain model uses self-describing exported symbols.
package domain

import "time"

const (
	AssetRunStatusQueued    = "QUEUED"
	AssetRunStatusPlanning  = "PLANNING"
	AssetRunStatusRunning   = "RUNNING"
	AssetRunStatusRetrying  = "RETRYING"
	AssetRunStatusSuccess   = "SUCCESS"
	AssetRunStatusFailed    = "FAILED"
	AssetRunStatusCancelled = "CANCELLED"
	AssetRunStatusSkipped   = "SKIPPED"
	AssetRunStatusStale     = "STALE"
)

const (
	AssetTriggerTypeManual          = "MANUAL"
	AssetTriggerTypeScheduled       = "SCHEDULED"
	AssetTriggerTypeUpstreamUpdate  = "UPSTREAM_UPDATE"
	AssetTriggerTypeFreshnessBreach = "FRESHNESS_BREACH"
	AssetTriggerTypeAPIEvent        = "API_EVENT"
	AssetTriggerTypeBackfill        = "BACKFILL"
	AssetTriggerTypeReconciler      = "RECONCILER"
	AssetTriggerTypePipeline        = "PIPELINE"
)

type AssetPartitionRunTarget struct {
	PartitionKey  *string
	PartitionFrom *string
	PartitionTo   *string
}

type AssetRun struct {
	ID            string
	AssetID       string
	RunGroupID    *string
	PartitionKey  *string
	PartitionFrom *string
	PartitionTo   *string
	Status        string
	TriggerType   string
	TriggeredBy   string
	AttemptCount  int
	MaxAttempts   int
	StartedAt     *time.Time
	FinishedAt    *time.Time
	ErrorMessage  *string
	CreatedAt     time.Time
	UpdatedAt     time.Time
}

type AssetRunEvent struct {
	ID               int64
	RunID            string
	EventType        string
	EventAt          time.Time
	Message          *string
	MetadataJSON     map[string]any
	CheckResultsJSON map[string]any
	StatsJSON        map[string]any
	CreatedAt        time.Time
}

type AssetMaterialization struct {
	ID             string
	AssetID        string
	RunID          *string
	PartitionKey   *string
	MetadataJSON   map[string]any
	RowCount       *int64
	SchemaHash     *string
	MaterializedAt time.Time
	CreatedAt      time.Time
}

type AssetCheck struct {
	ID         string
	AssetID    string
	Name       string
	CheckType  string
	Severity   string
	ConfigJSON map[string]any
	Enabled    bool
	CreatedAt  time.Time
	UpdatedAt  time.Time
}

type AssetCheckResult struct {
	ID           string
	CheckID      string
	RunID        *string
	PartitionKey *string
	Status       string
	Message      *string
	MetricsJSON  map[string]any
	CreatedAt    time.Time
}

type AssetRunFilter struct {
	AssetID *string
	Status  *string
	Page    PageRequest
}
