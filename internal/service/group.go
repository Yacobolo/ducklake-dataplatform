package service

import (
	"context"

	"duck-demo/domain"
)

type GroupService struct {
	repo  domain.GroupRepository
	audit domain.AuditRepository
}

func NewGroupService(repo domain.GroupRepository, audit domain.AuditRepository) *GroupService {
	return &GroupService{repo: repo, audit: audit}
}

func (s *GroupService) Create(ctx context.Context, g *domain.Group) (*domain.Group, error) {
	if g.Name == "" {
		return nil, domain.ErrValidation("group name is required")
	}
	return s.repo.Create(ctx, g)
}

func (s *GroupService) GetByID(ctx context.Context, id int64) (*domain.Group, error) {
	return s.repo.GetByID(ctx, id)
}

func (s *GroupService) List(ctx context.Context) ([]domain.Group, error) {
	return s.repo.List(ctx)
}

func (s *GroupService) Delete(ctx context.Context, id int64) error {
	return s.repo.Delete(ctx, id)
}

func (s *GroupService) AddMember(ctx context.Context, m *domain.GroupMember) error {
	return s.repo.AddMember(ctx, m)
}

func (s *GroupService) RemoveMember(ctx context.Context, m *domain.GroupMember) error {
	return s.repo.RemoveMember(ctx, m)
}

func (s *GroupService) ListMembers(ctx context.Context, groupID int64) ([]domain.GroupMember, error) {
	return s.repo.ListMembers(ctx, groupID)
}
