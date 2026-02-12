// Package mapper provides conversion functions between domain and database types.
package mapper

import (
	"database/sql"
	"encoding/json"
	"time"

	dbstore "duck-demo/internal/db/dbstore"
	"duck-demo/internal/domain"
)

const timeLayout = "2006-01-02 15:04:05"

func parseTime(s string) time.Time {
	t, _ := time.Parse(timeLayout, s)
	return t
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

// NullStrFromPtr converts a *string to sql.NullString.
func NullStrFromPtr(s *string) sql.NullString {
	return nullStr(s)
}

// NullStrFromStr converts a string to sql.NullString (empty string → NULL).
func NullStrFromStr(s string) sql.NullString {
	return nullStrVal(s)
}

// InterfaceFromPtr converts a *string to interface{} (nil or the string value).
// Useful for sqlc-generated nullable check parameters.
func InterfaceFromPtr(s *string) interface{} {
	if s == nil {
		return nil
	}
	return *s
}

// StringFromPtr returns the dereferenced string or empty string if nil.
func StringFromPtr(s *string) sql.NullString {
	return nullStr(s)
}

// --- Principal ---

// PrincipalFromDB converts a dbstore.Principal to a domain.Principal.
func PrincipalFromDB(p dbstore.Principal) *domain.Principal {
	return &domain.Principal{
		ID:             p.ID,
		Name:           p.Name,
		Type:           p.Type,
		IsAdmin:        p.IsAdmin != 0,
		ExternalID:     ptrStr(p.ExternalID),
		ExternalIssuer: ptrStr(p.ExternalIssuer),
		CreatedAt:      parseTime(p.CreatedAt),
	}
}

// PrincipalsFromDB converts a slice of dbstore.Principal to a slice of domain.Principal.
func PrincipalsFromDB(ps []dbstore.Principal) []domain.Principal {
	out := make([]domain.Principal, len(ps))
	for i, p := range ps {
		out[i] = *PrincipalFromDB(p)
	}
	return out
}

// --- Group ---

// GroupFromDB converts a dbstore.Group to a domain.Group.
func GroupFromDB(g dbstore.Group) *domain.Group {
	return &domain.Group{
		ID:          g.ID,
		Name:        g.Name,
		Description: g.Description.String,
		CreatedAt:   parseTime(g.CreatedAt),
	}
}

// GroupsFromDB converts a slice of dbstore.Group to a slice of domain.Group.
func GroupsFromDB(gs []dbstore.Group) []domain.Group {
	out := make([]domain.Group, len(gs))
	for i, g := range gs {
		out[i] = *GroupFromDB(g)
	}
	return out
}

// GroupMemberFromDB converts a dbstore.GroupMember to a domain.GroupMember.
func GroupMemberFromDB(m dbstore.GroupMember) domain.GroupMember {
	return domain.GroupMember{
		GroupID:    m.GroupID,
		MemberType: m.MemberType,
		MemberID:   m.MemberID,
	}
}

// GroupMembersFromDB converts a slice of dbstore.GroupMember to a slice of domain.GroupMember.
func GroupMembersFromDB(ms []dbstore.GroupMember) []domain.GroupMember {
	out := make([]domain.GroupMember, len(ms))
	for i, m := range ms {
		out[i] = GroupMemberFromDB(m)
	}
	return out
}

// --- PrivilegeGrant ---

// GrantFromDB converts a dbstore.PrivilegeGrant to a domain.PrivilegeGrant.
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

// GrantsFromDB converts a slice of dbstore.PrivilegeGrant to a slice of domain.PrivilegeGrant.
func GrantsFromDB(gs []dbstore.PrivilegeGrant) []domain.PrivilegeGrant {
	out := make([]domain.PrivilegeGrant, len(gs))
	for i, g := range gs {
		out[i] = *GrantFromDB(g)
	}
	return out
}

// --- RowFilter ---

// RowFilterFromDB converts a dbstore.RowFilter to a domain.RowFilter.
func RowFilterFromDB(f dbstore.RowFilter) *domain.RowFilter {
	return &domain.RowFilter{
		ID:          f.ID,
		TableID:     f.TableID,
		FilterSQL:   f.FilterSql,
		Description: f.Description.String,
		CreatedAt:   parseTime(f.CreatedAt),
	}
}

// RowFiltersFromDB converts a slice of dbstore.RowFilter to a slice of domain.RowFilter.
func RowFiltersFromDB(fs []dbstore.RowFilter) []domain.RowFilter {
	out := make([]domain.RowFilter, len(fs))
	for i, f := range fs {
		out[i] = *RowFilterFromDB(f)
	}
	return out
}

// RowFilterBindingFromDB converts a dbstore.RowFilterBinding to a domain.RowFilterBinding.
func RowFilterBindingFromDB(b dbstore.RowFilterBinding) domain.RowFilterBinding {
	return domain.RowFilterBinding{
		ID:            b.ID,
		RowFilterID:   b.RowFilterID,
		PrincipalID:   b.PrincipalID,
		PrincipalType: b.PrincipalType,
	}
}

// RowFilterBindingsFromDB converts a slice of dbstore.RowFilterBinding to a slice of domain.RowFilterBinding.
func RowFilterBindingsFromDB(bs []dbstore.RowFilterBinding) []domain.RowFilterBinding {
	out := make([]domain.RowFilterBinding, len(bs))
	for i, b := range bs {
		out[i] = RowFilterBindingFromDB(b)
	}
	return out
}

// --- ColumnMask ---

// ColumnMaskFromDB converts a dbstore.ColumnMask to a domain.ColumnMask.
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

// ColumnMasksFromDB converts a slice of dbstore.ColumnMask to a slice of domain.ColumnMask.
func ColumnMasksFromDB(ms []dbstore.ColumnMask) []domain.ColumnMask {
	out := make([]domain.ColumnMask, len(ms))
	for i, m := range ms {
		out[i] = *ColumnMaskFromDB(m)
	}
	return out
}

// ColumnMaskBindingFromDB converts a dbstore.ColumnMaskBinding to a domain.ColumnMaskBinding.
func ColumnMaskBindingFromDB(b dbstore.ColumnMaskBinding) domain.ColumnMaskBinding {
	return domain.ColumnMaskBinding{
		ID:            b.ID,
		ColumnMaskID:  b.ColumnMaskID,
		PrincipalID:   b.PrincipalID,
		PrincipalType: b.PrincipalType,
		SeeOriginal:   b.SeeOriginal != 0,
	}
}

// ColumnMaskBindingsFromDB converts a slice of dbstore.ColumnMaskBinding to a slice of domain.ColumnMaskBinding.
func ColumnMaskBindingsFromDB(bs []dbstore.ColumnMaskBinding) []domain.ColumnMaskBinding {
	out := make([]domain.ColumnMaskBinding, len(bs))
	for i, b := range bs {
		out[i] = ColumnMaskBindingFromDB(b)
	}
	return out
}

// --- AuditEntry ---

// AuditEntryFromDB converts a dbstore.AuditLog to a domain.AuditEntry.
func AuditEntryFromDB(a dbstore.AuditLog) *domain.AuditEntry {
	var tables []string
	if a.TablesAccessed.Valid && a.TablesAccessed.String != "" {
		_ = json.Unmarshal([]byte(a.TablesAccessed.String), &tables)
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
		RowsReturned:   ptrInt(a.RowsReturned),
		CreatedAt:      parseTime(a.CreatedAt),
	}
}

// AuditEntriesToDBParams converts a domain.AuditEntry to dbstore.InsertAuditLogParams for insertion.
func AuditEntriesToDBParams(e *domain.AuditEntry) dbstore.InsertAuditLogParams {
	var tablesJSON string
	if len(e.TablesAccessed) > 0 {
		b, err := json.Marshal(e.TablesAccessed)
		if err == nil {
			tablesJSON = string(b)
		}
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
		RowsReturned:   nullInt(e.RowsReturned),
	}
}

// --- QueryHistory ---

// QueryHistoryEntryFromDB converts a dbstore.AuditLog to a domain.QueryHistoryEntry.
func QueryHistoryEntryFromDB(a dbstore.AuditLog) *domain.QueryHistoryEntry {
	var tables []string
	if a.TablesAccessed.Valid && a.TablesAccessed.String != "" {
		_ = json.Unmarshal([]byte(a.TablesAccessed.String), &tables)
	}
	return &domain.QueryHistoryEntry{
		ID:             a.ID,
		PrincipalName:  a.PrincipalName,
		OriginalSQL:    ptrStr(a.OriginalSql),
		RewrittenSQL:   ptrStr(a.RewrittenSql),
		StatementType:  ptrStr(a.StatementType),
		TablesAccessed: tables,
		Status:         a.Status,
		ErrorMessage:   ptrStr(a.ErrorMessage),
		DurationMs:     ptrInt(a.DurationMs),
		RowsReturned:   ptrInt(a.RowsReturned),
		CreatedAt:      parseTime(a.CreatedAt),
	}
}

// --- Lineage ---

// LineageEdgeFromDB converts a dbstore.GetUpstreamLineageRow to a domain.LineageEdge.
func LineageEdgeFromDB(e dbstore.GetUpstreamLineageRow) *domain.LineageEdge {
	return &domain.LineageEdge{
		SourceTable:   e.SourceTable,
		TargetTable:   ptrStr(e.TargetTable),
		SourceSchema:  e.SourceSchema.String,
		TargetSchema:  e.TargetSchema.String,
		EdgeType:      e.EdgeType,
		PrincipalName: e.PrincipalName,
		CreatedAt:     parseTime(e.CreatedAt),
	}
}

// LineageEdgeFromDownstreamDB converts a dbstore.GetDownstreamLineageRow to a domain.LineageEdge.
func LineageEdgeFromDownstreamDB(e dbstore.GetDownstreamLineageRow) *domain.LineageEdge {
	return &domain.LineageEdge{
		SourceTable:   e.SourceTable,
		TargetTable:   ptrStr(e.TargetTable),
		SourceSchema:  e.SourceSchema.String,
		TargetSchema:  e.TargetSchema.String,
		EdgeType:      e.EdgeType,
		PrincipalName: e.PrincipalName,
		CreatedAt:     parseTime(e.CreatedAt),
	}
}

// --- Tag ---

// TagFromDB converts a dbstore.Tag to a domain.Tag.
func TagFromDB(t dbstore.Tag) *domain.Tag {
	return &domain.Tag{
		ID:        t.ID,
		Key:       t.Key,
		Value:     ptrStr(t.Value),
		CreatedBy: t.CreatedBy,
		CreatedAt: parseTime(t.CreatedAt),
	}
}

// TagAssignmentFromDB converts a dbstore.TagAssignment to a domain.TagAssignment.
func TagAssignmentFromDB(a dbstore.TagAssignment) *domain.TagAssignment {
	return &domain.TagAssignment{
		ID:            a.ID,
		TagID:         a.TagID,
		SecurableType: a.SecurableType,
		SecurableID:   a.SecurableID,
		ColumnName:    ptrStr(a.ColumnName),
		AssignedBy:    a.AssignedBy,
		AssignedAt:    parseTime(a.AssignedAt),
	}
}

// --- View ---

// ViewFromDB converts a dbstore.View to a domain.ViewDetail.
func ViewFromDB(v dbstore.View) *domain.ViewDetail {
	var props map[string]string
	if v.Properties.Valid && v.Properties.String != "" {
		_ = json.Unmarshal([]byte(v.Properties.String), &props)
	}
	if props == nil {
		props = make(map[string]string)
	}
	var sources []string
	if v.SourceTables.Valid && v.SourceTables.String != "" {
		_ = json.Unmarshal([]byte(v.SourceTables.String), &sources)
	}
	vd := &domain.ViewDetail{
		ID:             v.ID,
		SchemaID:       v.SchemaID,
		Name:           v.Name,
		ViewDefinition: v.ViewDefinition,
		Comment:        ptrStr(v.Comment),
		Properties:     props,
		Owner:          v.Owner,
		SourceTables:   sources,
		CreatedAt:      parseTime(v.CreatedAt),
		UpdatedAt:      parseTime(v.UpdatedAt),
	}
	if v.DeletedAt.Valid {
		t := parseTime(v.DeletedAt.String)
		vd.DeletedAt = &t
	}
	return vd
}
