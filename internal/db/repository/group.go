package repository

import (
	"context"
	"database/sql"

	dbstore "duck-demo/internal/db/dbstore"
	"duck-demo/internal/db/mapper"
	"duck-demo/internal/domain"
)

// GroupRepo implements domain.GroupRepository using SQLite.
type GroupRepo struct {
	q  *dbstore.Queries
	db *sql.DB
}

// NewGroupRepo creates a new GroupRepo.
func NewGroupRepo(db *sql.DB) *GroupRepo {
	return &GroupRepo{q: dbstore.New(db), db: db}
}

// Create inserts a new group into the database.
func (r *GroupRepo) Create(ctx context.Context, g *domain.Group) (*domain.Group, error) {
	row, err := r.q.CreateGroup(ctx, dbstore.CreateGroupParams{
		ID:          newID(),
		Name:        g.Name,
		Description: sql.NullString{String: g.Description, Valid: g.Description != ""},
	})
	if err != nil {
		return nil, mapDBError(err)
	}
	return mapper.GroupFromDB(row), nil
}

// GetByID returns a group by its ID.
func (r *GroupRepo) GetByID(ctx context.Context, id string) (*domain.Group, error) {
	row, err := r.q.GetGroup(ctx, id)
	if err != nil {
		return nil, mapDBError(err)
	}
	return mapper.GroupFromDB(row), nil
}

// GetByName returns a group by its name.
func (r *GroupRepo) GetByName(ctx context.Context, name string) (*domain.Group, error) {
	row, err := r.q.GetGroupByName(ctx, name)
	if err != nil {
		return nil, mapDBError(err)
	}
	return mapper.GroupFromDB(row), nil
}

// List returns a paginated list of groups.
func (r *GroupRepo) List(ctx context.Context, page domain.PageRequest) ([]domain.Group, int64, error) {
	total, err := r.q.CountGroups(ctx)
	if err != nil {
		return nil, 0, err
	}

	rows, err := r.q.ListGroupsPaginated(ctx, dbstore.ListGroupsPaginatedParams{
		Limit:  int64(page.Limit()),
		Offset: int64(page.Offset()),
	})
	if err != nil {
		return nil, 0, err
	}

	return mapper.GroupsFromDB(rows), total, nil
}

// Delete removes a group by ID.
func (r *GroupRepo) Delete(ctx context.Context, id string) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()

	if _, err := tx.ExecContext(ctx, "DELETE FROM privilege_grants WHERE principal_type = 'group' AND principal_id = ?", id); err != nil {
		return mapDBError(err)
	}
	if _, err := tx.ExecContext(ctx, "DELETE FROM group_members WHERE member_type = 'group' AND member_id = ?", id); err != nil {
		return mapDBError(err)
	}
	result, err := tx.ExecContext(ctx, "DELETE FROM groups WHERE id = ?", id)
	if err != nil {
		return mapDBError(err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		return domain.ErrNotFound("group %s not found", id)
	}
	return tx.Commit()
}

// AddMember adds a principal to a group.
func (r *GroupRepo) AddMember(ctx context.Context, m *domain.GroupMember) error {
	return r.q.AddGroupMember(ctx, dbstore.AddGroupMemberParams{
		GroupID:    m.GroupID,
		MemberType: m.MemberType,
		MemberID:   m.MemberID,
	})
}

// RemoveMember removes a principal from a group.
func (r *GroupRepo) RemoveMember(ctx context.Context, m *domain.GroupMember) error {
	result, err := r.db.ExecContext(ctx, `
DELETE FROM group_members
WHERE group_id = ? AND member_type = ? AND member_id = ?`,
		m.GroupID,
		m.MemberType,
		m.MemberID,
	)
	if err != nil {
		return mapDBError(err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		return domain.ErrNotFound("group member %s/%s not found in group %s", m.MemberType, m.MemberID, m.GroupID)
	}
	return nil
}

// RemoveMemberLegacy preserves the sqlc-generated query shape for future regen compatibility.
func (r *GroupRepo) RemoveMemberLegacy(ctx context.Context, m *domain.GroupMember) error {
	return r.q.RemoveGroupMember(ctx, dbstore.RemoveGroupMemberParams{
		GroupID:    m.GroupID,
		MemberType: m.MemberType,
		MemberID:   m.MemberID,
	})
}

// ListMembers returns a paginated list of members in a group.
func (r *GroupRepo) ListMembers(ctx context.Context, groupID string, page domain.PageRequest) ([]domain.GroupMember, int64, error) {
	total, err := r.q.CountGroupMembers(ctx, groupID)
	if err != nil {
		return nil, 0, err
	}

	rows, err := r.q.ListGroupMembersPaginated(ctx, dbstore.ListGroupMembersPaginatedParams{
		GroupID: groupID,
		Limit:   int64(page.Limit()),
		Offset:  int64(page.Offset()),
	})
	if err != nil {
		return nil, 0, err
	}

	return mapper.GroupMembersFromDB(rows), total, nil
}

// GetGroupsForMember returns all groups that the given member belongs to.
func (r *GroupRepo) GetGroupsForMember(ctx context.Context, memberType string, memberID string) ([]domain.Group, error) {
	rows, err := r.q.GetGroupsForMember(ctx, dbstore.GetGroupsForMemberParams{
		MemberType: memberType,
		MemberID:   memberID,
	})
	if err != nil {
		return nil, err
	}
	return mapper.GroupsFromDB(rows), nil
}
