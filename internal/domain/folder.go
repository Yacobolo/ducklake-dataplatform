package domain

import (
	"strings"
	"time"
)

const (
	// FolderSystemRoleWorkspaceRoot marks the hidden root folder of a workspace.
	FolderSystemRoleWorkspaceRoot = "WORKSPACE_ROOT"
	// FolderSystemRolePersonalRoot is retained for older tests and callers.
	FolderSystemRolePersonalRoot = FolderSystemRoleWorkspaceRoot
)

const (
	// FolderShareRoleViewer grants read-only access to a folder.
	FolderShareRoleViewer = "viewer"
	// FolderShareRoleEditor grants edit access to a folder.
	FolderShareRoleEditor = "editor"
	// FolderShareRoleManager grants administrative access to a folder.
	FolderShareRoleManager = "manager"
)

// NormalizeShareRole returns a supported share role, defaulting to viewer.
func NormalizeShareRole(role string) string {
	switch strings.TrimSpace(role) {
	case FolderShareRoleViewer:
		return FolderShareRoleViewer
	case FolderShareRoleEditor:
		return FolderShareRoleEditor
	case FolderShareRoleManager:
		return FolderShareRoleManager
	default:
		return ""
	}
}

// Folder defines the primary authoring container for namespace-organized work.
type Folder struct {
	ID                   string
	WorkspaceID          string
	Name                 string
	Owner                string
	ParentFolderID       *string
	Path                 string
	Depth                int
	SystemRole           *string
	GitRepoID            *string
	GitRootPath          *string
	DefaultProjectID     *string
	DefaultEnvironmentID *string
	CreatedAt            time.Time
	UpdatedAt            time.Time
}

// FolderShare grants access to a folder.
type FolderShare struct {
	ID            string
	FolderID      string
	PrincipalName string
	Role          string
	CreatedAt     time.Time
	UpdatedAt     time.Time
}

// NotebookShare grants access directly to a notebook.
type NotebookShare struct {
	ID            string
	NotebookID    string
	PrincipalName string
	Role          string
	CreatedAt     time.Time
	UpdatedAt     time.Time
}

// NotebookContext captures the effective inherited notebook context.
type NotebookContext struct {
	NotebookID             string
	WorkspaceID            string
	FolderID               string
	EffectiveProjectID     *string
	EffectiveEnvironmentID *string
	EffectiveGitRepoID     *string
	EffectiveGitRootPath   *string
	ProjectSourceFolderID  *string
	EnvironmentSourceID    *string
	GitSourceFolderID      *string
}

// CreateFolderRequest defines inputs for creating a folder.
type CreateFolderRequest struct {
	WorkspaceID          string
	Name                 string
	ParentFolderID       *string
	GitRepoID            *string
	GitRootPath          *string
	DefaultProjectID     *string
	DefaultEnvironmentID *string
}

// Validate validates the create folder request.
func (r *CreateFolderRequest) Validate() error {
	if strings.TrimSpace(r.WorkspaceID) == "" {
		return ErrValidation("workspace_id is required")
	}
	if strings.TrimSpace(r.Name) == "" {
		return ErrValidation("folder name is required")
	}
	return nil
}

// UpdateFolderRequest defines supported folder metadata changes.
type UpdateFolderRequest struct {
	Name                 *string
	GitRepoID            *string
	GitRootPath          *string
	DefaultProjectID     *string
	DefaultEnvironmentID *string
}

// MoveFolderRequest re-parents a folder subtree.
type MoveFolderRequest struct {
	ParentFolderID       *string
	ConfirmLeaveGit      bool
	ConfirmContextChange bool
}
