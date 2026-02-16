package api

import (
	"math"

	"duck-demo/internal/domain"
)

// --- helpers ---

// safeIntToInt32 converts an int to int32 clamping to [math.MinInt32, math.MaxInt32].
func safeIntToInt32(v int) int32 {
	if v > math.MaxInt32 {
		return math.MaxInt32
	}
	if v < math.MinInt32 {
		return math.MinInt32
	}
	return int32(v)
}

// pageFromParams extracts a PageRequest from optional max_results/page_token params.
func pageFromParams(maxResults *MaxResults, pageToken *PageToken) domain.PageRequest {
	p := domain.PageRequest{}
	if maxResults != nil {
		p.MaxResults = int(*maxResults)
	}
	if pageToken != nil {
		p.PageToken = *pageToken
	}
	return p
}

// httpStatusFromError returns the HTTP status code for a domain error using
// the centralized mapper. Unknown errors return 500 Internal Server Error.
func httpStatusFromError(err error) int {
	return httpStatusFromDomainError(err)
}

// errorCodeFromError returns the HTTP status code for building error JSON responses.
// This is a convenience alias for use in handler methods.
func errorCodeFromError(err error) int32 {
	return int32(httpStatusFromError(err)) //nolint:gosec // HTTP status codes are always in [100,599]
}

// === Mapping helpers ===

func principalToAPI(p domain.Principal) Principal {
	t := p.CreatedAt
	pt := PrincipalType(p.Type)
	return Principal{
		Id:        &p.ID,
		Name:      &p.Name,
		Type:      &pt,
		IsAdmin:   &p.IsAdmin,
		CreatedAt: &t,
	}
}

func groupToAPI(g domain.Group) Group {
	t := g.CreatedAt
	return Group{
		Id:          &g.ID,
		Name:        &g.Name,
		Description: &g.Description,
		CreatedAt:   &t,
	}
}

func groupMemberToAPI(m domain.GroupMember, groupID string) GroupMember {
	mt := GroupMemberMemberType(m.MemberType)
	return GroupMember{
		GroupId:    &groupID,
		MemberType: &mt,
		MemberId:   &m.MemberID,
	}
}

func grantToAPI(g domain.PrivilegeGrant) PrivilegeGrant {
	t := g.GrantedAt
	pt := PrivilegeGrantPrincipalType(g.PrincipalType)
	return PrivilegeGrant{
		Id:            &g.ID,
		PrincipalId:   &g.PrincipalID,
		PrincipalType: &pt,
		SecurableType: &g.SecurableType,
		SecurableId:   &g.SecurableID,
		Privilege:     &g.Privilege,
		GrantedBy:     g.GrantedBy,
		GrantedAt:     &t,
	}
}

func rowFilterToAPI(f domain.RowFilter) RowFilter {
	t := f.CreatedAt
	return RowFilter{
		Id:          &f.ID,
		TableId:     &f.TableID,
		FilterSql:   &f.FilterSQL,
		Description: &f.Description,
		CreatedAt:   &t,
	}
}

func columnMaskToAPI(m domain.ColumnMask) ColumnMask {
	t := m.CreatedAt
	return ColumnMask{
		Id:             &m.ID,
		TableId:        &m.TableID,
		ColumnName:     &m.ColumnName,
		MaskExpression: &m.MaskExpression,
		Description:    &m.Description,
		CreatedAt:      &t,
	}
}

func auditEntryToAPI(e domain.AuditEntry) AuditEntry {
	t := e.CreatedAt
	return AuditEntry{
		Id:             &e.ID,
		PrincipalName:  &e.PrincipalName,
		Action:         &e.Action,
		StatementType:  e.StatementType,
		OriginalSql:    e.OriginalSQL,
		RewrittenSql:   e.RewrittenSQL,
		TablesAccessed: &e.TablesAccessed,
		Status:         &e.Status,
		ErrorMessage:   e.ErrorMessage,
		DurationMs:     e.DurationMs,
		CreatedAt:      &t,
	}
}

func catalogInfoToAPI(c domain.CatalogInfo) CatalogInfo {
	return CatalogInfo{
		Name:      &c.Name,
		Comment:   &c.Comment,
		CreatedAt: &c.CreatedAt,
		UpdatedAt: &c.UpdatedAt,
	}
}

func schemaDetailToAPI(s domain.SchemaDetail) SchemaDetail {
	tags := make([]Tag, len(s.Tags))
	for i, t := range s.Tags {
		tags[i] = tagToAPI(t)
	}
	return SchemaDetail{
		SchemaId:    &s.SchemaID,
		Name:        &s.Name,
		CatalogName: &s.CatalogName,
		Comment:     &s.Comment,
		Owner:       &s.Owner,
		Properties:  &s.Properties,
		CreatedAt:   &s.CreatedAt,
		UpdatedAt:   &s.UpdatedAt,
		Tags:        &tags,
		DeletedAt:   s.DeletedAt,
	}
}

func tableDetailToAPI(t domain.TableDetail) TableDetail {
	cols := make([]ColumnDetail, len(t.Columns))
	for i, c := range t.Columns {
		cols[i] = columnDetailToAPI(c)
	}
	tags := make([]Tag, len(t.Tags))
	for i, tg := range t.Tags {
		tags[i] = tagToAPI(tg)
	}
	td := TableDetail{
		TableId:     &t.TableID,
		Name:        &t.Name,
		SchemaName:  &t.SchemaName,
		CatalogName: &t.CatalogName,
		TableType:   &t.TableType,
		Columns:     &cols,
		Comment:     &t.Comment,
		Owner:       &t.Owner,
		Properties:  &t.Properties,
		CreatedAt:   &t.CreatedAt,
		UpdatedAt:   &t.UpdatedAt,
		Tags:        &tags,
		DeletedAt:   t.DeletedAt,
	}
	if t.Statistics != nil {
		td.Statistics = tableStatisticsPtr(t.Statistics)
	}
	if t.StoragePath != "" {
		td.StoragePath = &t.StoragePath
	}
	if t.SourcePath != "" {
		td.SourcePath = &t.SourcePath
	}
	if t.FileFormat != "" {
		td.FileFormat = &t.FileFormat
	}
	if t.LocationName != "" {
		td.LocationName = &t.LocationName
	}
	return td
}

func columnDetailToAPI(c domain.ColumnDetail) ColumnDetail {
	pos := safeIntToInt32(c.Position)
	return ColumnDetail{
		Name:       &c.Name,
		Type:       &c.Type,
		Position:   &pos,
		Nullable:   &c.Nullable,
		Comment:    &c.Comment,
		Properties: &c.Properties,
	}
}

func queryHistoryEntryToAPI(e domain.QueryHistoryEntry) QueryHistoryEntry {
	t := e.CreatedAt
	return QueryHistoryEntry{
		Id:             &e.ID,
		PrincipalName:  &e.PrincipalName,
		OriginalSql:    e.OriginalSQL,
		RewrittenSql:   e.RewrittenSQL,
		StatementType:  e.StatementType,
		TablesAccessed: &e.TablesAccessed,
		Status:         &e.Status,
		ErrorMessage:   e.ErrorMessage,
		DurationMs:     e.DurationMs,
		RowsReturned:   e.RowsReturned,
		CreatedAt:      &t,
	}
}

func searchResultToAPI(r domain.SearchResult) SearchResult {
	return SearchResult{
		Type:       &r.Type,
		Name:       &r.Name,
		SchemaName: r.SchemaName,
		TableName:  r.TableName,
		Comment:    r.Comment,
		MatchField: &r.MatchField,
	}
}

func lineageEdgeToAPI(e domain.LineageEdge) LineageEdge {
	t := e.CreatedAt
	et := LineageEdgeEdgeType(e.EdgeType)
	return LineageEdge{
		Id:            &e.ID,
		SourceTable:   &e.SourceTable,
		TargetTable:   e.TargetTable,
		SourceSchema:  strPtrIfNonEmpty(e.SourceSchema),
		TargetSchema:  strPtrIfNonEmpty(e.TargetSchema),
		EdgeType:      &et,
		PrincipalName: &e.PrincipalName,
		CreatedAt:     &t,
	}
}

func columnLineageEdgeToAPI(e domain.ColumnLineageEdge) ColumnLineageEdge {
	tt := ColumnLineageEdgeTransformType(e.TransformType)
	return ColumnLineageEdge{
		Id:            &e.ID,
		LineageEdgeId: &e.LineageEdgeID,
		TargetColumn:  &e.TargetColumn,
		SourceSchema:  strPtrIfNonEmpty(e.SourceSchema),
		SourceTable:   &e.SourceTable,
		SourceColumn:  &e.SourceColumn,
		TransformType: &tt,
		Function:      strPtrIfNonEmpty(e.Function),
	}
}

func strPtrIfNonEmpty(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

func tagToAPI(t domain.Tag) Tag {
	ct := t.CreatedAt
	return Tag{
		Id:        &t.ID,
		Key:       &t.Key,
		Value:     t.Value,
		CreatedBy: &t.CreatedBy,
		CreatedAt: &ct,
	}
}

func tagAssignmentToAPI(a domain.TagAssignment) TagAssignment {
	t := a.AssignedAt
	return TagAssignment{
		Id:            &a.ID,
		TagId:         &a.TagID,
		SecurableType: &a.SecurableType,
		SecurableId:   &a.SecurableID,
		ColumnName:    a.ColumnName,
		AssignedBy:    &a.AssignedBy,
		AssignedAt:    &t,
	}
}

func viewDetailToAPI(v domain.ViewDetail) ViewDetail {
	ct := v.CreatedAt
	ut := v.UpdatedAt
	return ViewDetail{
		Id:             &v.ID,
		SchemaId:       &v.SchemaID,
		SchemaName:     &v.SchemaName,
		CatalogName:    &v.CatalogName,
		Name:           &v.Name,
		ViewDefinition: &v.ViewDefinition,
		Comment:        v.Comment,
		Properties:     &v.Properties,
		Owner:          &v.Owner,
		SourceTables:   &v.SourceTables,
		CreatedAt:      &ct,
		UpdatedAt:      &ut,
	}
}

func tableStatisticsToAPI(s *domain.TableStatistics) TableStatistics {
	if s == nil {
		return TableStatistics{}
	}
	return TableStatistics{
		RowCount:       s.RowCount,
		SizeBytes:      s.SizeBytes,
		ColumnCount:    s.ColumnCount,
		LastProfiledAt: s.LastProfiledAt,
		ProfiledBy:     &s.ProfiledBy,
	}
}

func tableStatisticsPtr(s *domain.TableStatistics) *TableStatistics {
	if s == nil {
		return nil
	}
	ts := tableStatisticsToAPI(s)
	return &ts
}

// optStr returns a pointer to the string if non-empty, otherwise nil.
func optStr(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

// Default rate-limit header values used by all success responses.
const (
	defaultRateLimitLimit     = 1000
	defaultRateLimitRemaining = 999
	defaultRateLimitReset     = 0
)
