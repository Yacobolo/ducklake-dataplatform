package api

import (
	"fmt"
	"math"
	"reflect"
	"time"

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

func isNilService(v interface{}) bool {
	if v == nil {
		return true
	}
	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Ptr, reflect.Map, reflect.Slice, reflect.Interface, reflect.Func:
		return rv.IsNil()
	default:
		return false
	}
}

// pageFromParams extracts a PageRequest from optional max_results/page_token params.
func pageFromParams[MaxResultsType ~int32, PageTokenType ~string](maxResults *MaxResultsType, pageToken *PageTokenType) domain.PageRequest {
	p := domain.PageRequest{}
	if maxResults != nil {
		p.MaxResults = int(*maxResults)
	}
	if pageToken != nil {
		p.PageToken = string(*pageToken)
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
	return Principal{
		Id:        p.ID,
		Name:      p.Name,
		Type:      p.Type,
		IsAdmin:   p.IsAdmin,
		CreatedAt: formatTimePtr(&p.CreatedAt),
	}
}

func groupToAPI(g domain.Group) Group {
	return Group{
		Id:          g.ID,
		Name:        g.Name,
		Description: &g.Description,
		CreatedAt:   formatTimePtr(&g.CreatedAt),
	}
}

func groupMemberToAPI(m domain.GroupMember, groupID string) GroupMember {
	return GroupMember{
		GroupId:    groupID,
		MemberType: m.MemberType,
		MemberId:   m.MemberID,
	}
}

func grantToAPI(g domain.PrivilegeGrant) PrivilegeGrant {
	return PrivilegeGrant{
		Id:            g.ID,
		PrincipalId:   g.PrincipalID,
		PrincipalType: g.PrincipalType,
		SecurableType: g.SecurableType,
		SecurableId:   g.SecurableID,
		Privilege:     g.Privilege,
		GrantedBy:     g.GrantedBy,
		GrantedAt:     formatTimePtr(&g.GrantedAt),
	}
}

func rowFilterToAPI(f domain.RowFilter) RowFilter {
	return RowFilter{
		Id:          f.ID,
		TableId:     f.TableID,
		FilterSql:   f.FilterSQL,
		Description: &f.Description,
		CreatedAt:   formatTimePtr(&f.CreatedAt),
	}
}

func columnMaskToAPI(m domain.ColumnMask) ColumnMask {
	return ColumnMask{
		Id:             m.ID,
		TableId:        m.TableID,
		ColumnName:     m.ColumnName,
		MaskExpression: m.MaskExpression,
		Description:    &m.Description,
		CreatedAt:      formatTimePtr(&m.CreatedAt),
	}
}

func auditEntryToAPI(e domain.AuditEntry) AuditEntry {
	return AuditEntry{
		Id:             e.ID,
		PrincipalName:  &e.PrincipalName,
		Action:         &e.Action,
		StatementType:  e.StatementType,
		OriginalSql:    e.OriginalSQL,
		RewrittenSql:   e.RewrittenSQL,
		TablesAccessed: &e.TablesAccessed,
		Status:         &e.Status,
		ErrorMessage:   e.ErrorMessage,
		DurationMs:     safeInt64ToInt32Ptr(e.DurationMs),
		CreatedAt:      formatTimePtr(&e.CreatedAt),
	}
}

func catalogInfoToAPI(c domain.CatalogInfo) CatalogInfo {
	return CatalogInfo{
		Name:      c.Name,
		Comment:   &c.Comment,
		CreatedAt: formatTimePtr(&c.CreatedAt),
		UpdatedAt: formatTimePtr(&c.UpdatedAt),
	}
}

func schemaDetailToAPI(s domain.SchemaDetail) SchemaDetail {
	tags := make([]Tag, len(s.Tags))
	for i, t := range s.Tags {
		tags[i] = tagToAPI(t)
	}
	return SchemaDetail{
		SchemaId:    s.SchemaID,
		Name:        s.Name,
		CatalogName: s.CatalogName,
		Comment:     &s.Comment,
		Tags:        &tags,
		Owner:       &s.Owner,
		Properties:  stringMapToRecord(s.Properties),
		CreatedAt:   formatTimePtr(&s.CreatedAt),
		UpdatedAt:   formatTimePtr(&s.UpdatedAt),
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
		TableId:     t.TableID,
		Name:        t.Name,
		SchemaName:  t.SchemaName,
		CatalogName: t.CatalogName,
		TableType:   &t.TableType,
		Columns:     &cols,
		Comment:     &t.Comment,
		Properties:  stringMapToRecord(t.Properties),
		Owner:       &t.Owner,
		Tags:        &tags,
		Statistics:  tableStatisticsPtr(t.Statistics),
		CreatedAt:   formatTimePtr(&t.CreatedAt),
		UpdatedAt:   formatTimePtr(&t.UpdatedAt),
	}
	return td
}

func columnDetailToAPI(c domain.ColumnDetail) ColumnDetail {
	pos := safeIntToInt32(c.Position)
	return ColumnDetail{
		Name:     c.Name,
		Type:     c.Type,
		Position: &pos,
		Nullable: &c.Nullable,
		Comment:  &c.Comment,
	}
}

func queryHistoryEntryToAPI(e domain.QueryHistoryEntry) QueryHistoryEntry {
	return QueryHistoryEntry{
		Id:             e.ID,
		PrincipalName:  &e.PrincipalName,
		OriginalSql:    e.OriginalSQL,
		RewrittenSql:   e.RewrittenSQL,
		StatementType:  e.StatementType,
		TablesAccessed: &e.TablesAccessed,
		Status:         &e.Status,
		ErrorMessage:   e.ErrorMessage,
		DurationMs:     safeInt64ToInt32Ptr(e.DurationMs),
		RowsReturned:   safeInt64ToInt32Ptr(e.RowsReturned),
		CreatedAt:      formatTimePtr(&e.CreatedAt),
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
	return LineageEdge{
		Id:            &e.ID,
		SourceTable:   &e.SourceTable,
		TargetTable:   e.TargetTable,
		SourceSchema:  strPtrIfNonEmpty(e.SourceSchema),
		TargetSchema:  strPtrIfNonEmpty(e.TargetSchema),
		EdgeType:      strPtrIfNonEmpty(e.EdgeType),
		PrincipalName: &e.PrincipalName,
		CreatedAt:     formatTimePtr(&e.CreatedAt),
	}
}

func columnLineageEdgeToAPI(e domain.ColumnLineageEdge) ColumnLineageEdge {
	tt := ColumnLineageEdgeTransformType(e.TransformType)
	id := safeInt64ToInt32(e.ID)
	return ColumnLineageEdge{
		Id:            &id,
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

func formatTimePtr(t *time.Time) *string {
	if t == nil {
		return nil
	}
	s := t.UTC().Format(time.RFC3339)
	return &s
}

func safeInt64ToInt32(v int64) int32 {
	if v > math.MaxInt32 {
		return math.MaxInt32
	}
	if v < math.MinInt32 {
		return math.MinInt32
	}
	return int32(v)
}

func safeInt64ToInt32Ptr(v *int64) *int32 {
	if v == nil {
		return nil
	}
	out := safeInt64ToInt32(*v)
	return &out
}

func int32PtrToInt64Ptr(v *int32) *int64 {
	if v == nil {
		return nil
	}
	out := int64(*v)
	return &out
}

func stringMapToRecord(m map[string]string) *Record {
	if len(m) == 0 {
		return nil
	}
	record := make(Record, len(m))
	for k, v := range m {
		record[k] = v
	}
	return &record
}

func recordToStringMap(record *Record) map[string]string {
	if record == nil {
		return nil
	}
	out := make(map[string]string, len(*record))
	for k, v := range *record {
		switch typed := v.(type) {
		case string:
			out[k] = typed
		default:
			out[k] = fmt.Sprint(typed)
		}
	}
	return out
}

func rowsToStringMatrix(rows [][]interface{}) *[][]string {
	if len(rows) == 0 {
		return nil
	}
	out := make([][]string, len(rows))
	for i, row := range rows {
		converted := make([]string, len(row))
		for j, value := range row {
			if value == nil {
				converted[j] = ""
				continue
			}
			converted[j] = fmt.Sprint(value)
		}
		out[i] = converted
	}
	return &out
}

func tagToAPI(t domain.Tag) Tag {
	return Tag{
		Id:        &t.ID,
		Key:       &t.Key,
		Value:     t.Value,
		CreatedBy: &t.CreatedBy,
		CreatedAt: formatTimePtr(&t.CreatedAt),
	}
}

func tagAssignmentToAPI(a domain.TagAssignment) TagAssignment {
	st := TagAssignmentSecurableType(a.SecurableType)
	return TagAssignment{
		Id:            &a.ID,
		TagId:         &a.TagID,
		SecurableType: &st,
		SecurableId:   &a.SecurableID,
		ColumnName:    a.ColumnName,
		AssignedBy:    &a.AssignedBy,
		AssignedAt:    formatTimePtr(&a.AssignedAt),
	}
}

func catalogVersionSummaryToAPI(summary domain.CatalogVersionSummary) CatalogVersionSummary {
	return CatalogVersionSummary{
		CatalogName:      &summary.CatalogName,
		Version:          &summary.Version,
		CreatedBy:        &summary.CreatedBy,
		Encrypted:        summary.Encrypted,
		DataPath:         &summary.DataPath,
		LatestSnapshotId: safeInt64ToInt32Ptr(summary.LatestSnapshotID),
		Schemas:          versionedObjectSummaryPtr(summary.Schemas),
		Tables:           versionedObjectSummaryPtr(summary.Tables),
		Columns:          versionedObjectSummaryPtr(summary.Columns),
	}
}

func versionedObjectSummaryPtr(summary domain.VersionedObjectSummary) *VersionedObjectSummary {
	return &VersionedObjectSummary{
		TotalCount:       safeInt64ToInt32Ptr(&summary.TotalCount),
		ActiveCount:      safeInt64ToInt32Ptr(&summary.ActiveCount),
		HistoricalCount:  safeInt64ToInt32Ptr(&summary.HistoricalCount),
		HasHistory:       &summary.HasHistory,
		LatestSnapshotId: safeInt64ToInt32Ptr(summary.LatestSnapshotID),
	}
}

func catalogHistoryEntryToAPI(entry domain.CatalogHistoryEntry) CatalogHistoryEntry {
	return CatalogHistoryEntry{
		EntityType:       &entry.EntityType,
		SchemaName:       optStr(entry.SchemaName),
		TableName:        optStr(entry.TableName),
		ColumnName:       optStr(entry.ColumnName),
		ObjectName:       &entry.ObjectName,
		ObjectId:         &entry.ObjectID,
		BeginSnapshotId:  safeInt64ToInt32Ptr(entry.BeginSnapshotID),
		EndSnapshotId:    safeInt64ToInt32Ptr(entry.EndSnapshotID),
		LatestSnapshotId: safeInt64ToInt32Ptr(entry.LatestSnapshotID),
		IsActive:         &entry.IsActive,
		HasHistory:       &entry.HasHistory,
	}
}

func viewDetailToAPI(v domain.ViewDetail) ViewDetail {
	return ViewDetail{
		Id:             v.ID,
		SchemaId:       &v.SchemaID,
		SchemaName:     v.SchemaName,
		CatalogName:    v.CatalogName,
		Name:           v.Name,
		ViewDefinition: &v.ViewDefinition,
		Comment:        v.Comment,
		Owner:          &v.Owner,
		SourceTables:   &v.SourceTables,
		CreatedAt:      formatTimePtr(&v.CreatedAt),
		UpdatedAt:      formatTimePtr(&v.UpdatedAt),
	}
}

func tableStatisticsToAPI(s *domain.TableStatistics) TableStatistics {
	if s == nil {
		return TableStatistics{}
	}
	return TableStatistics{
		RowCount:       safeInt64ToInt32Ptr(s.RowCount),
		SizeBytes:      safeInt64ToInt32Ptr(s.SizeBytes),
		ColumnCount:    safeInt64ToInt32Ptr(s.ColumnCount),
		LastProfiledAt: formatTimePtr(s.LastProfiledAt),
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
