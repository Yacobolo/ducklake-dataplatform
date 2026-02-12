package security

import (
	"context"

	"duck-demo/internal/domain"
)

// GroupService provides group management operations.
type GroupService struct {
	repo  domain.GroupRepository
	audit domain.AuditRepository
}

// NewGroupService creates a new GroupService.
func NewGroupService(repo domain.GroupRepository, audit domain.AuditRepository) *GroupService {
	return &GroupService{repo: repo, audit: audit}
}

// Create validates and persists a new group.
func (s *GroupService) Create(ctx context.Context, g *domain.Group) (*domain.Group, error) {
	if g.Name == "" {
		return nil, domain.ErrValidation("group name is required")
	}
	return s.repo.Create(ctx, g)
}

// GetByID returns a group by ID.
func (s *GroupService) GetByID(ctx context.Context, id int64) (*domain.Group, error) {
	return s.repo.GetByID(ctx, id)
}

// List returns a paginated list of groups.
func (s *GroupService) List(ctx context.Context, page domain.PageRequest) ([]domain.Group, int64, error) {
	return s.repo.List(ctx, page)
}

// Delete removes a group by ID.
func (s *GroupService) Delete(ctx context.Context, id int64) error {
	return s.repo.Delete(ctx, id)
}

// AddMember adds a principal to a group.
func (s *GroupService) AddMember(ctx context.Context, m *domain.GroupMember) error {
	return s.repo.AddMember(ctx, m)
}

// RemoveMember removes a principal from a group.
func (s *GroupService) RemoveMember(ctx context.Context, m *domain.GroupMember) error {
	return s.repo.RemoveMember(ctx, m)
}

// ListMembers returns a paginated list of members in a group.
func (s *GroupService) ListMembers(ctx context.Context, groupID int64, page domain.PageRequest) ([]domain.GroupMember, int64, error) {
	return s.repo.ListMembers(ctx, groupID, page)
}
