package ui

import (
	"duck-demo/internal/domain"

	. "maragu.dev/gomponents"
)

type assetsListRowData struct {
	Filter              string
	AssetKey            string
	URL                 string
	Type                string
	Owner               string
	Description         string
	Tags                []string
	Active              bool
	Updated             string
	FreshnessTracked    bool
	PartitionType       string
	AutoMaterialized    bool
	MaterializationMode string
}

type assetsListSummary struct {
	Total            int
	Active           int
	Partitioned      int
	FreshnessTracked int
	AutoMaterialized int
	ManualOnly       int
	TypeCounts       []assetTypeCount
	OwnerCounts      []assetOwnerCount
}

type assetTypeCount struct {
	Label string
	Count int
}

type assetOwnerCount struct {
	Label string
	Count int
}

type assetDetailPageData struct {
	Principal           domain.ContextPrincipal
	ProductSlug         string
	ProductName         string
	AssetKey            string
	AssetType           string
	Owner               string
	Description         string
	IOProfile           string
	IsActive            bool
	FreshnessLabel      string
	FreshnessTone       string
	UpdatedAt           string
	UpstreamAssetKeys   []string
	DownstreamAssetKeys []string
	DependencyEdges     []assetDependencyEdgeData
	Runs                []domain.AssetRun
	Materializations    []domain.AssetMaterialization
	Checks              []domain.AssetCheck
	Partitions          []domain.AssetPartition
	Backfills           []domain.BackfillRequest
	RetryTimeline       []assetRetryTimelineEntry
	FailureRootCauses   []assetFailureRootCauseGroup
	PartitionCalendar   []assetPartitionCalendarMonth
	PartitionStatus     map[string]int
	CanMaterialize      bool
	CanBackfill         bool
	BackfillConfigured  bool
	CSRFFieldFunc       func() Node
}

type assetRetryTimelineEntry struct {
	RunID          string
	Status         string
	TriggerType    string
	AttemptSummary string
	WindowLabel    string
	RetryHint      string
	IsRetry        bool
}

type assetFailureRootCauseGroup struct {
	Signature string
	Message   string
	Count     int
	LastSeen  string
	Statuses  []string
	RunIDs    []string
}

type assetPartitionCalendarMonth struct {
	Label string
	Cells []assetPartitionCalendarCell
}

type assetPartitionCalendarCell struct {
	DayLabel     string
	PartitionKey string
	Status       string
	Tone         string
	IsPadding    bool
	HasPartition bool
}

type assetDependencyEdgeData struct {
	FromKey string
	ToKey   string
}

type assetDetailSummary struct {
	MaterializationMode  string
	PartitionLabel       string
	LatestRunStatus      string
	LatestMaterializedAt string
	PartitionHint        string
}

type assetGraphNodeData struct {
	ID       string         `json:"id"`
	Label    string         `json:"label"`
	Role     string         `json:"role"`
	Position map[string]int `json:"position"`
}

type assetGraphEdgeData struct {
	ID     string `json:"id"`
	Source string `json:"source"`
	Target string `json:"target"`
}

type assetGraphPayload struct {
	Nodes []assetGraphNodeData `json:"nodes"`
	Edges []assetGraphEdgeData `json:"edges"`
}
