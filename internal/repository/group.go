package repository

import (
	"context"
	"database/sql"

	dbstore "duck-demo/db/catalog"
	"duck-demo/domain"
	"duck-demo/internal/mapper"
)

type GroupRepo struct {
	q  *dbstore.Queries
	db *sql.DB
}

func NewGroupRepo(db *sql.DB) *GroupRepo {
	return &GroupRepo{q: dbstore.New(db), db: db}
}

func (r *GroupRepo) Create(ctx context.Context, g *domain.Group) (*domain.Group, error) {
	row, err := r.q.CreateGroup(ctx, dbstore.CreateGroupParams{
		Name:        g.Name,
		Description: sql.NullString{String: g.Description, Valid: g.Description != ""},
	})
	if err != nil {
		return nil, mapDBError(err)
	}
	return mapper.GroupFromDB(row), nil
}

func (r *GroupRepo) GetByID(ctx context.Context, id int64) (*domain.Group, error) {
	row, err := r.q.GetGroup(ctx, id)
	if err != nil {
		return nil, mapDBError(err)
	}
	return mapper.GroupFromDB(row), nil
}

func (r *GroupRepo) GetByName(ctx context.Context, name string) (*domain.Group, error) {
	row, err := r.q.GetGroupByName(ctx, name)
	if err != nil {
		return nil, mapDBError(err)
	}
	return mapper.GroupFromDB(row), nil
}

func (r *GroupRepo) List(ctx context.Context) ([]domain.Group, error) {
	rows, err := r.q.ListGroups(ctx)
	if err != nil {
		return nil, err
	}
	return mapper.GroupsFromDB(rows), nil
}

func (r *GroupRepo) Delete(ctx context.Context, id int64) error {
	return r.q.DeleteGroup(ctx, id)
}

func (r *GroupRepo) AddMember(ctx context.Context, m *domain.GroupMember) error {
	return r.q.AddGroupMember(ctx, dbstore.AddGroupMemberParams{
		GroupID:    m.GroupID,
		MemberType: m.MemberType,
		MemberID:   m.MemberID,
	})
}

func (r *GroupRepo) RemoveMember(ctx context.Context, m *domain.GroupMember) error {
	return r.q.RemoveGroupMember(ctx, dbstore.RemoveGroupMemberParams{
		GroupID:    m.GroupID,
		MemberType: m.MemberType,
		MemberID:   m.MemberID,
	})
}

func (r *GroupRepo) ListMembers(ctx context.Context, groupID int64) ([]domain.GroupMember, error) {
	rows, err := r.q.ListGroupMembers(ctx, groupID)
	if err != nil {
		return nil, err
	}
	return mapper.GroupMembersFromDB(rows), nil
}

func (r *GroupRepo) GetGroupsForMember(ctx context.Context, memberType string, memberID int64) ([]domain.Group, error) {
	rows, err := r.q.GetGroupsForMember(ctx, dbstore.GetGroupsForMemberParams{
		MemberType: memberType,
		MemberID:   memberID,
	})
	if err != nil {
		return nil, err
	}
	return mapper.GroupsFromDB(rows), nil
}
