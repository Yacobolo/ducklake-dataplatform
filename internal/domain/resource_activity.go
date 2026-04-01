package domain

import "time"

// ResourceRef identifies a navigable product resource and its display metadata.
type ResourceRef struct {
	ResourceType string
	ResourceKey  string
	DisplayName  string
	ResourcePath string
	Href         string
	Section      string
}

// ResourceAccessEvent is the user-facing projection of a recent resource visit.
type ResourceAccessEvent struct {
	ResourceRef
	AccessedAt time.Time
}

// SavedResource is the user-facing projection of an explicitly saved resource.
type SavedResource struct {
	ResourceRef
	SavedAt        time.Time
	LastAccessedAt *time.Time
}
