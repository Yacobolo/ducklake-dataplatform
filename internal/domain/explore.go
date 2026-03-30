package domain

import "time"

const (
	ExploreKindAll           = "all"
	ExploreKindNotebook      = "notebook"
	ExploreKindModel         = "model"
	ExploreKindMacro         = "macro"
	ExploreKindDashboard     = "dashboard"
	ExploreKindPipeline      = "pipeline"
	ExploreKindSemanticModel = "semantic_model"
)

const (
	ExploreScopeFolder  = "folder"
	ExploreScopeProject = "project"
)

// ExploreFilter defines the current folder and kind selection.
type ExploreFilter struct {
	FolderID string
	Kind     string
	Page     PageRequest
}

// ExploreItem is the normalized resource row rendered in Explore.
type ExploreItem struct {
	Kind         string
	Scope        string
	ID           string
	Name         string
	Owner        string
	FolderID     *string
	ProjectName  *string
	UpdatedAt    time.Time
	GitRepoID    *string
	Shared       bool
	ProjectBound bool
}
