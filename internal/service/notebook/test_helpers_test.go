package notebook

import (
	"context"

	"duck-demo/internal/domain"
)

const testWorkspaceID = "workspace-1"

type stubWorkspaceRepo struct {
	roleByWorkspace map[string]string
}

func newStubWorkspaceRepo(role string) *stubWorkspaceRepo {
	return &stubWorkspaceRepo{
		roleByWorkspace: map[string]string{testWorkspaceID: role},
	}
}

func (s *stubWorkspaceRepo) Create(context.Context, *domain.Workspace) (*domain.Workspace, error) {
	panic("Create not implemented")
}

func (s *stubWorkspaceRepo) GetByID(context.Context, string) (*domain.Workspace, error) {
	return &domain.Workspace{ID: testWorkspaceID}, nil
}

func (s *stubWorkspaceRepo) GetPersonalByPrincipal(context.Context, string) (*domain.Workspace, error) {
	panic("GetPersonalByPrincipal not implemented")
}

func (s *stubWorkspaceRepo) List(context.Context, domain.PageRequest) ([]domain.Workspace, int64, error) {
	panic("List not implemented")
}

func (s *stubWorkspaceRepo) ListForPrincipal(context.Context, string, domain.PageRequest) ([]domain.Workspace, int64, error) {
	panic("ListForPrincipal not implemented")
}

func (s *stubWorkspaceRepo) Update(context.Context, string, domain.UpdateWorkspaceRequest) (*domain.Workspace, error) {
	panic("Update not implemented")
}

func (s *stubWorkspaceRepo) Delete(context.Context, string) error {
	panic("Delete not implemented")
}

func (s *stubWorkspaceRepo) UpsertMember(context.Context, *domain.WorkspaceMember) (*domain.WorkspaceMember, error) {
	panic("UpsertMember not implemented")
}

func (s *stubWorkspaceRepo) DeleteMember(context.Context, string, string) error {
	panic("DeleteMember not implemented")
}

func (s *stubWorkspaceRepo) ListMembers(context.Context, string) ([]domain.WorkspaceMember, error) {
	panic("ListMembers not implemented")
}

func (s *stubWorkspaceRepo) GetMemberRole(_ context.Context, workspaceID string, _ string) (string, error) {
	return s.roleByWorkspace[workspaceID], nil
}
