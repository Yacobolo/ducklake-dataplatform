package domain

import "time"

const (
	// ExploreKindFolder identifies folder rows in Explore.
	ExploreKindFolder = "folder"
	// ExploreKindAll selects all Explore kinds.
	ExploreKindAll = "all"
	// ExploreKindNotebook identifies notebook rows in Explore.
	ExploreKindNotebook = "notebook"
	// ExploreKindModel identifies model rows in Explore.
	ExploreKindModel = "model"
	// ExploreKindMacro identifies macro rows in Explore.
	ExploreKindMacro = "macro"
	// ExploreKindDashboard identifies dashboard rows in Explore.
	ExploreKindDashboard = "dashboard"
	// ExploreKindPipeline identifies pipeline rows in Explore.
	ExploreKindPipeline = "pipeline"
	// ExploreKindSemanticModel identifies semantic model rows in Explore.
	ExploreKindSemanticModel = "semantic_model"
)

const (
	// ExploreScopeFolder marks items surfaced from folder placement.
	ExploreScopeFolder = "folder"
	// ExploreScopeProject marks project-scoped items surfaced through folder context.
	ExploreScopeProject = "project"
)

// ExploreFilter defines the current folder and kind selection.
type ExploreFilter struct {
	FolderID string
	Kinds    []string
	Owners   []string
	Query    string
	Page     PageRequest
}

// ExploreItem is the normalized authored-asset row used by the Explore service.
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
