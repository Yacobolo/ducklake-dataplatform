package security

import (
	"context"
	"fmt"

	"github.com/Yacobolo/quackstack/internal/domain"
)

// GroupService provides group management operations.
type GroupService struct {
	repo       domain.GroupRepository
	principals domain.PrincipalRepository
	audit      domain.AuditRepository
}

// NewGroupService creates a new GroupService.
func NewGroupService(repo domain.GroupRepository, principals domain.PrincipalRepository, audit domain.AuditRepository) *GroupService {
	return &GroupService{repo: repo, principals: principals, audit: audit}
}

// Create validates and persists a new group. Requires admin privileges.
func (s *GroupService) Create(ctx context.Context, req domain.CreateGroupRequest) (*domain.Group, error) {
	if err := requireAdmin(ctx); err != nil {
		return nil, err
	}
	if err := req.Validate(); err != nil {
		return nil, err
	}
	g := &domain.Group{
		Name:        req.Name,
		Description: req.Description,
	}
	result, err := s.repo.Create(ctx, g)
	if err != nil {
		return nil, err
	}
	s.logAudit(ctx, fmt.Sprintf("CREATE_GROUP(%s)", req.Name))
	return result, nil
}

// GetByID returns a group by ID.
func (s *GroupService) GetByID(ctx context.Context, id string) (*domain.Group, error) {
	return s.repo.GetByID(ctx, id)
}

// Update mutates a group's editable fields. Requires admin privileges.
func (s *GroupService) Update(ctx context.Context, id string, req domain.UpdateGroupRequest) (*domain.Group, error) {
	if err := requireAdmin(ctx); err != nil {
		return nil, err
	}
	if err := req.Validate(); err != nil {
		return nil, err
	}
	current, err := s.repo.GetByID(ctx, id)
	if err != nil {
		return nil, err
	}
	if req.Description != nil {
		current.Description = *req.Description
	}
	result, err := s.repo.Update(ctx, id, current)
	if err != nil {
		return nil, err
	}
	s.logAudit(ctx, fmt.Sprintf("UPDATE_GROUP(id=%s)", id))
	return result, nil
}

// List returns a paginated list of groups. Requires admin privileges.
func (s *GroupService) List(ctx context.Context, page domain.PageRequest) ([]domain.Group, int64, error) {
	if err := requireAdmin(ctx); err != nil {
		return nil, 0, err
	}
	return s.repo.List(ctx, page)
}

// Delete removes a group by ID. Requires admin privileges.
func (s *GroupService) Delete(ctx context.Context, id string) error {
	if err := requireAdmin(ctx); err != nil {
		return err
	}
	if err := s.repo.Delete(ctx, id); err != nil {
		return err
	}
	s.logAudit(ctx, fmt.Sprintf("DELETE_GROUP(id=%s)", id))
	return nil
}

// AddMember adds a principal to a group. Requires admin privileges.
func (s *GroupService) AddMember(ctx context.Context, req domain.AddGroupMemberRequest) error {
	if err := requireAdmin(ctx); err != nil {
		return err
	}
	if err := req.Validate(); err != nil {
		return err
	}
	if _, err := s.repo.GetByID(ctx, req.GroupID); err != nil {
		return err
	}
	if req.MemberType == "group" {
		if req.MemberID == req.GroupID {
			return domain.ErrValidation("group cannot be a member of itself")
		}
		if _, err := s.repo.GetByID(ctx, req.MemberID); err != nil {
			return err
		}
	} else {
		if _, err := s.principals.GetByID(ctx, req.MemberID); err != nil {
			return err
		}
	}
	m := &domain.GroupMember{
		GroupID:    req.GroupID,
		MemberType: req.MemberType,
		MemberID:   req.MemberID,
	}
	if err := s.repo.AddMember(ctx, m); err != nil {
		return err
	}
	s.logAudit(ctx, fmt.Sprintf("ADD_GROUP_MEMBER(group=%s, member=%s)", req.GroupID, req.MemberID))
	return nil
}

// RemoveMember removes a principal from a group. Requires admin privileges.
func (s *GroupService) RemoveMember(ctx context.Context, req domain.RemoveGroupMemberRequest) error {
	if err := requireAdmin(ctx); err != nil {
		return err
	}
	if err := req.Validate(); err != nil {
		return err
	}
	if _, err := s.repo.GetByID(ctx, req.GroupID); err != nil {
		return err
	}
	m := &domain.GroupMember{
		GroupID:    req.GroupID,
		MemberType: req.MemberType,
		MemberID:   req.MemberID,
	}
	if err := s.repo.RemoveMember(ctx, m); err != nil {
		return err
	}
	s.logAudit(ctx, fmt.Sprintf("REMOVE_GROUP_MEMBER(group=%s, member=%s)", req.GroupID, req.MemberID))
	return nil
}

// ListMembers returns a paginated list of members in a group. Requires admin privileges.
func (s *GroupService) ListMembers(ctx context.Context, groupID string, page domain.PageRequest) ([]domain.GroupMember, int64, error) {
	if err := requireAdmin(ctx); err != nil {
		return nil, 0, err
	}
	if _, err := s.repo.GetByID(ctx, groupID); err != nil {
		return nil, 0, err
	}
	return s.repo.ListMembers(ctx, groupID, page)
}

func (s *GroupService) logAudit(ctx context.Context, action string) {
	_ = s.audit.Insert(ctx, &domain.AuditEntry{
		PrincipalName: callerName(ctx),
		Action:        action,
		Status:        "ALLOWED",
	})
}
