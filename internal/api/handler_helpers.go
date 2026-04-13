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
	principalType := PrincipalType(p.Type)
	return Principal{
		Id:        p.ID,
		Name:      p.Name,
		Type:      principalType,
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
	memberType := PrincipalType(m.MemberType)
	return GroupMember{
		GroupId:    groupID,
		MemberType: memberType,
		MemberId:   m.MemberID,
	}
}

func grantToAPI(g domain.PrivilegeGrant) PrivilegeGrant {
	principalType := PrincipalType(g.PrincipalType)
	return PrivilegeGrant{
		Id:            g.ID,
		PrincipalId:   g.PrincipalID,
		PrincipalType: principalType,
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
		Name:        f.Name,
		FilterSql:   f.FilterSQL,
		Description: &f.Description,
		CreatedAt:   formatTimePtr(&f.CreatedAt),
	}
}

func columnMaskToAPI(m domain.ColumnMask) ColumnMask {
	return ColumnMask{
		Id:             m.ID,
		TableId:        m.TableID,
		Name:           m.Name,
		ColumnName:     m.ColumnName,
		MaskExpression: m.MaskExpression,
		Description:    &m.Description,
		CreatedAt:      formatTimePtr(&m.CreatedAt),
	}
}

func auditEntryToAPI(e domain.AuditEntry) AuditEntry {
	status := AuditDecisionStatus(e.Status)
	return AuditEntry{
		Id:             e.ID,
		PrincipalName:  &e.PrincipalName,
		Action:         &e.Action,
		StatementType:  e.StatementType,
		OriginalSql:    e.OriginalSQL,
		RewrittenSql:   e.RewrittenSQL,
		TablesAccessed: &e.TablesAccessed,
		Status:         &status,
		ErrorMessage:   e.ErrorMessage,
		DurationMs:     safeInt64ToInt32Ptr(e.DurationMs),
		CreatedAt:      formatTimePtr(&e.CreatedAt),
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
		Properties:  stringMapToAnyMap(s.Properties),
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
		Properties:  stringMapToAnyMap(t.Properties),
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
	status := AuditDecisionStatus(e.Status)
	return QueryHistoryEntry{
		Id:             e.ID,
		PrincipalName:  &e.PrincipalName,
		OriginalSql:    e.OriginalSQL,
		RewrittenSql:   e.RewrittenSQL,
		StatementType:  e.StatementType,
		TablesAccessed: &e.TablesAccessed,
		Status:         &status,
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

func stringMapToAnyMap(m map[string]string) *map[string]any {
	if len(m) == 0 {
		return nil
	}
	out := make(map[string]any, len(m))
	for k, v := range m {
		out[k] = v
	}
	return &out
}

func anyMapToStringMap(values *map[string]any) map[string]string {
	if values == nil {
		return nil
	}
	out := make(map[string]string, len(*values))
	for k, v := range *values {
		switch typed := v.(type) {
		case string:
			out[k] = typed
		default:
			out[k] = fmt.Sprint(typed)
		}
	}
	return out
}

func anyMapPtrToStringMap(values *map[string]any) *map[string]string {
	if values == nil {
		return nil
	}
	out := anyMapToStringMap(values)
	return &out
}

func tabularColumns(columns []string, rows [][]interface{}) []TabularColumn {
	names := normalizedColumnNames(columns, rows)
	out := make([]TabularColumn, len(names))
	for i, name := range names {
		out[i] = TabularColumn{Name: name}
	}
	return out
}

func ptrPrincipalType(value string) *PrincipalType {
	if value == "" {
		return nil
	}
	typed := PrincipalType(value)
	return &typed
}

func ptrMetastoreType(value string) *MetastoreType {
	if value == "" {
		return nil
	}
	typed := MetastoreType(value)
	return &typed
}

func ptrCatalogStatus(value string) *CatalogStatus {
	if value == "" {
		return nil
	}
	typed := CatalogStatus(value)
	return &typed
}

func ptrAssetRunStatus(value string) *AssetRunStatus {
	if value == "" {
		return nil
	}
	typed := AssetRunStatus(value)
	return &typed
}

func ptrAssetTriggerType(value string) *AssetTriggerType {
	if value == "" {
		return nil
	}
	typed := AssetTriggerType(value)
	return &typed
}

func ptrAssetCheckSeverity(value string) *AssetCheckSeverity {
	if value == "" {
		return nil
	}
	typed := AssetCheckSeverity(value)
	return &typed
}

func ptrMacroType(value string) *MacroType {
	if value == "" {
		return nil
	}
	typed := MacroType(value)
	return &typed
}

func ptrMacroVisibility(value string) *MacroVisibility {
	if value == "" {
		return nil
	}
	typed := MacroVisibility(value)
	return &typed
}

func ptrMacroStatus(value string) *MacroStatus {
	if value == "" {
		return nil
	}
	typed := MacroStatus(value)
	return &typed
}

func ptrStorageCredentialType(value string) *StorageCredentialType {
	if value == "" {
		return nil
	}
	typed := StorageCredentialType(value)
	return &typed
}

func ptrStorageType(value string) *StorageType {
	if value == "" {
		return nil
	}
	typed := StorageType(value)
	return &typed
}

func ptrURLStyle(value string) *URLStyle {
	if value == "" {
		return nil
	}
	typed := URLStyle(value)
	return &typed
}

func rowsToAnyMaps(columns []string, rows [][]interface{}) []map[string]any {
	if len(rows) == 0 {
		return []map[string]any{}
	}
	names := normalizedColumnNames(columns, rows)
	out := make([]map[string]any, len(rows))
	for i, row := range rows {
		record := make(map[string]any, len(names))
		for j, name := range names {
			if j < len(row) {
				record[name] = row[j]
				continue
			}
			record[name] = nil
		}
		out[i] = record
	}
	return out
}

func ptrTabularColumns(columns []TabularColumn) *[]TabularColumn {
	if len(columns) == 0 {
		return nil
	}
	return &columns
}

func ptrAnyMaps(rows []map[string]any) *[]map[string]any {
	if len(rows) == 0 {
		return nil
	}
	return &rows
}

func normalizedColumnNames(columns []string, rows [][]interface{}) []string {
	width := len(columns)
	for _, row := range rows {
		if len(row) > width {
			width = len(row)
		}
	}
	if width == 0 {
		return []string{}
	}

	names := make([]string, width)
	used := make(map[string]int, width)
	for i := 0; i < width; i++ {
		name := ""
		if i < len(columns) {
			name = columns[i]
		}
		if name == "" {
			name = fmt.Sprintf("column_%d", i+1)
		}
		if count, exists := used[name]; exists {
			count++
			used[name] = count
			name = fmt.Sprintf("%s_%d", name, count)
		} else {
			used[name] = 1
		}
		names[i] = name
	}
	return names
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
