package repository

import (
	"context"
	"database/sql"

	dbstore "duck-demo/internal/db/dbstore"
	"duck-demo/internal/db/mapper"
	"duck-demo/internal/domain"
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

func (r *GroupRepo) List(ctx context.Context, page domain.PageRequest) ([]domain.Group, int64, error) {
	var total int64
	if err := r.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM groups_table`).Scan(&total); err != nil {
		return nil, 0, err
	}

	rows, err := r.db.QueryContext(ctx,
		`SELECT id, name, description, created_at FROM groups_table ORDER BY id LIMIT ? OFFSET ?`,
		page.Limit(), page.Offset())
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	var groups []domain.Group
	for rows.Next() {
		var g dbstore.Group
		if err := rows.Scan(&g.ID, &g.Name, &g.Description, &g.CreatedAt); err != nil {
			return nil, 0, err
		}
		groups = append(groups, *mapper.GroupFromDB(g))
	}
	return groups, total, rows.Err()
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

func (r *GroupRepo) ListMembers(ctx context.Context, groupID int64, page domain.PageRequest) ([]domain.GroupMember, int64, error) {
	var total int64
	if err := r.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM group_members WHERE group_id = ?`, groupID).Scan(&total); err != nil {
		return nil, 0, err
	}

	rows, err := r.db.QueryContext(ctx,
		`SELECT group_id, member_type, member_id FROM group_members WHERE group_id = ? ORDER BY member_id LIMIT ? OFFSET ?`,
		groupID, page.Limit(), page.Offset())
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	var members []domain.GroupMember
	for rows.Next() {
		var m domain.GroupMember
		if err := rows.Scan(&m.GroupID, &m.MemberType, &m.MemberID); err != nil {
			return nil, 0, err
		}
		members = append(members, m)
	}
	return members, total, rows.Err()
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
