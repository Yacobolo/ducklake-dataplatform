package mapper

import (
	"database/sql"
	"encoding/json"
	"strings"
	"time"

	dbstore "duck-demo/db/catalog"
	"duck-demo/domain"
)

const timeLayout = "2006-01-02 15:04:05"

func parseTime(s string) time.Time {
	t, _ := time.Parse(timeLayout, s)
	return t
}

func formatTime(t time.Time) string {
	return t.Format(timeLayout)
}

func boolToInt(b bool) int64 {
	if b {
		return 1
	}
	return 0
}

func nullStr(s *string) sql.NullString {
	if s == nil {
		return sql.NullString{}
	}
	return sql.NullString{String: *s, Valid: true}
}

func nullStrVal(s string) sql.NullString {
	if s == "" {
		return sql.NullString{}
	}
	return sql.NullString{String: s, Valid: true}
}

func nullInt(i *int64) sql.NullInt64 {
	if i == nil {
		return sql.NullInt64{}
	}
	return sql.NullInt64{Int64: *i, Valid: true}
}

func ptrStr(ns sql.NullString) *string {
	if !ns.Valid {
		return nil
	}
	return &ns.String
}

func ptrInt(ni sql.NullInt64) *int64 {
	if !ni.Valid {
		return nil
	}
	return &ni.Int64
}

// --- Principal ---

func PrincipalFromDB(p dbstore.Principal) *domain.Principal {
	return &domain.Principal{
		ID:        p.ID,
		Name:      p.Name,
		Type:      p.Type,
		IsAdmin:   p.IsAdmin != 0,
		CreatedAt: parseTime(p.CreatedAt),
	}
}

func PrincipalsFromDB(ps []dbstore.Principal) []domain.Principal {
	out := make([]domain.Principal, len(ps))
	for i, p := range ps {
		out[i] = *PrincipalFromDB(p)
	}
	return out
}

// --- Group ---

func GroupFromDB(g dbstore.Group) *domain.Group {
	return &domain.Group{
		ID:          g.ID,
		Name:        g.Name,
		Description: g.Description.String,
		CreatedAt:   parseTime(g.CreatedAt),
	}
}

func GroupsFromDB(gs []dbstore.Group) []domain.Group {
	out := make([]domain.Group, len(gs))
	for i, g := range gs {
		out[i] = *GroupFromDB(g)
	}
	return out
}

func GroupMemberFromDB(m dbstore.GroupMember) domain.GroupMember {
	return domain.GroupMember{
		GroupID:    m.GroupID,
		MemberType: m.MemberType,
		MemberID:   m.MemberID,
	}
}

func GroupMembersFromDB(ms []dbstore.GroupMember) []domain.GroupMember {
	out := make([]domain.GroupMember, len(ms))
	for i, m := range ms {
		out[i] = GroupMemberFromDB(m)
	}
	return out
}

// --- PrivilegeGrant ---

func GrantFromDB(g dbstore.PrivilegeGrant) *domain.PrivilegeGrant {
	return &domain.PrivilegeGrant{
		ID:            g.ID,
		PrincipalID:   g.PrincipalID,
		PrincipalType: g.PrincipalType,
		SecurableType: g.SecurableType,
		SecurableID:   g.SecurableID,
		Privilege:     g.Privilege,
		GrantedBy:     ptrInt(g.GrantedBy),
		GrantedAt:     parseTime(g.GrantedAt),
	}
}

func GrantsFromDB(gs []dbstore.PrivilegeGrant) []domain.PrivilegeGrant {
	out := make([]domain.PrivilegeGrant, len(gs))
	for i, g := range gs {
		out[i] = *GrantFromDB(g)
	}
	return out
}

// --- RowFilter ---

func RowFilterFromDB(f dbstore.RowFilter) *domain.RowFilter {
	return &domain.RowFilter{
		ID:          f.ID,
		TableID:     f.TableID,
		FilterSQL:   f.FilterSql,
		Description: f.Description.String,
		CreatedAt:   parseTime(f.CreatedAt),
	}
}

func RowFiltersFromDB(fs []dbstore.RowFilter) []domain.RowFilter {
	out := make([]domain.RowFilter, len(fs))
	for i, f := range fs {
		out[i] = *RowFilterFromDB(f)
	}
	return out
}

func RowFilterBindingFromDB(b dbstore.RowFilterBinding) domain.RowFilterBinding {
	return domain.RowFilterBinding{
		ID:            b.ID,
		RowFilterID:   b.RowFilterID,
		PrincipalID:   b.PrincipalID,
		PrincipalType: b.PrincipalType,
	}
}

func RowFilterBindingsFromDB(bs []dbstore.RowFilterBinding) []domain.RowFilterBinding {
	out := make([]domain.RowFilterBinding, len(bs))
	for i, b := range bs {
		out[i] = RowFilterBindingFromDB(b)
	}
	return out
}

// --- ColumnMask ---

func ColumnMaskFromDB(m dbstore.ColumnMask) *domain.ColumnMask {
	return &domain.ColumnMask{
		ID:             m.ID,
		TableID:        m.TableID,
		ColumnName:     m.ColumnName,
		MaskExpression: m.MaskExpression,
		Description:    m.Description.String,
		CreatedAt:      parseTime(m.CreatedAt),
	}
}

func ColumnMasksFromDB(ms []dbstore.ColumnMask) []domain.ColumnMask {
	out := make([]domain.ColumnMask, len(ms))
	for i, m := range ms {
		out[i] = *ColumnMaskFromDB(m)
	}
	return out
}

func ColumnMaskBindingFromDB(b dbstore.ColumnMaskBinding) domain.ColumnMaskBinding {
	return domain.ColumnMaskBinding{
		ID:            b.ID,
		ColumnMaskID:  b.ColumnMaskID,
		PrincipalID:   b.PrincipalID,
		PrincipalType: b.PrincipalType,
		SeeOriginal:   b.SeeOriginal != 0,
	}
}

func ColumnMaskBindingsFromDB(bs []dbstore.ColumnMaskBinding) []domain.ColumnMaskBinding {
	out := make([]domain.ColumnMaskBinding, len(bs))
	for i, b := range bs {
		out[i] = ColumnMaskBindingFromDB(b)
	}
	return out
}

// --- AuditEntry ---

func AuditEntryFromDB(a dbstore.AuditLog) *domain.AuditEntry {
	var tables []string
	if a.TablesAccessed.Valid && a.TablesAccessed.String != "" {
		json.Unmarshal([]byte(a.TablesAccessed.String), &tables)
	}
	return &domain.AuditEntry{
		ID:             a.ID,
		PrincipalName:  a.PrincipalName,
		Action:         a.Action,
		StatementType:  ptrStr(a.StatementType),
		OriginalSQL:    ptrStr(a.OriginalSql),
		RewrittenSQL:   ptrStr(a.RewrittenSql),
		TablesAccessed: tables,
		Status:         a.Status,
		ErrorMessage:   ptrStr(a.ErrorMessage),
		DurationMs:     ptrInt(a.DurationMs),
		CreatedAt:      parseTime(a.CreatedAt),
	}
}

func AuditEntriesToDBParams(e *domain.AuditEntry) dbstore.InsertAuditLogParams {
	var tablesJSON string
	if len(e.TablesAccessed) > 0 {
		tablesJSON = `["` + strings.Join(e.TablesAccessed, `","`) + `"]`
	}
	return dbstore.InsertAuditLogParams{
		PrincipalName:  e.PrincipalName,
		Action:         e.Action,
		StatementType:  nullStr(e.StatementType),
		OriginalSql:    nullStr(e.OriginalSQL),
		RewrittenSql:   nullStr(e.RewrittenSQL),
		TablesAccessed: nullStrVal(tablesJSON),
		Status:         e.Status,
		ErrorMessage:   nullStr(e.ErrorMessage),
		DurationMs:     nullInt(e.DurationMs),
	}
}
