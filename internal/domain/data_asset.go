//nolint:revive // asset-centric domain model uses self-describing exported symbols.
package domain

import "time"

const (
	AssetTypeTable                  = "TABLE"
	AssetTypeView                   = "VIEW"
	AssetTypeModel                  = "MODEL"
	AssetTypeNotebook               = "NOTEBOOK"
	AssetTypeOutput                 = "OUTPUT"
	AssetTypeDashboard              = "DASHBOARD"
	AssetTypeSemanticModel          = "SEMANTIC_MODEL"
	AssetTypeMetric                 = "METRIC"
	AssetTypeSemanticPreAggregation = "SEMANTIC_PRE_AGGREGATION"
	AssetTypeNotebookOutput         = "NOTEBOOK_OUTPUT"
)

const (
	PartitionTypeUnpartitioned = "UNPARTITIONED"
	PartitionTypeDaily         = "DAILY"
	PartitionTypeHourly        = "HOURLY"
	PartitionTypeStatic        = "STATIC"
	PartitionTypeDynamic       = "DYNAMIC"
)

const (
	DependencyTypeHard = "HARD"
	DependencyTypeSoft = "SOFT"
)

type PartitionDefinition struct {
	Type         string
	Timezone     string
	StaticKeys   []string
	DynamicGroup *string
}

type AssetFreshnessPolicy struct {
	MaxLagSeconds int64
	CronSchedule  string
}

type AssetMaterializationPolicy struct {
	Mode            string
	AllowConcurrent bool
}

type AssetAutoMaterializePolicy struct {
	Mode                    string
	MinIntervalSeconds      int64
	RequireAllUpstreams     bool
	OnFreshnessBreach       bool
	OnUpstreamMaterialized  bool
	RespectDowntimeWindows  bool
	DowntimeWindowsCronExpr []string
}

type DataAsset struct {
	ID                    string
	AssetKey              string
	AssetType             string
	Owner                 string
	Description           string
	Tags                  []string
	SchemaJSON            map[string]any
	PartitionDefinition   *PartitionDefinition
	FreshnessPolicy       *AssetFreshnessPolicy
	MaterializationPolicy *AssetMaterializationPolicy
	AutoMaterializePolicy *AssetAutoMaterializePolicy
	IOProfile             string
	IsActive              bool
	CreatedBy             string
	CreatedAt             time.Time
	UpdatedAt             time.Time
}

type AssetDependency struct {
	ID                   string
	AssetID              string
	UpstreamAssetID      string
	DependencyType       string
	PartitionMappingJSON map[string]any
	CreatedAt            time.Time
}

type AssetPartition struct {
	ID                 string
	AssetID            string
	PartitionKey       string
	PartitionTime      *time.Time
	Status             string
	LastMaterializedAt *time.Time
	MetadataJSON       map[string]any
	CreatedAt          time.Time
	UpdatedAt          time.Time
}

type AssetFilter struct {
	Owner     *string
	AssetType *string
	IsActive  *bool
	Page      PageRequest
}
