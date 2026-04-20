package model

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"

	"github.com/Yacobolo/quackstack/internal/domain"
	"github.com/Yacobolo/quackstack/internal/sqlrewrite"
)

type compileAnalysisResult struct {
	lineage         []domain.CompiledColumnLineage
	diagnostics     []domain.CompileDiagnostic
	stateSnapshot   *domain.BuildStateSnapshot
	coverageByModel map[string]string
}

type compileLineageResolver struct {
	svc         *Service
	columnCache map[string][]string
}

func (r *compileLineageResolver) ResolveColumns(ctx context.Context, schema, table string) ([]string, error) {
	key := lineageColumnKey("", schema, table)
	if cols, ok := r.columnCache[key]; ok {
		return append([]string(nil), cols...), nil
	}
	cols, err := r.svc.loadRelationColumns(ctx, schema, table)
	if err != nil {
		return nil, err
	}
	if len(cols) > 0 {
		r.columnCache[key] = append([]string(nil), cols...)
	}
	return cols, nil
}

func (s *Service) analyzeCompiledModels(
	ctx context.Context,
	principal string,
	selected []domain.Model,
	artifacts map[string]compileResult,
	runCtx *resolvedRunContext,
	req domain.TriggerModelRunRequest,
) (*compileAnalysisResult, error) {
	result := &compileAnalysisResult{
		lineage:         make([]domain.CompiledColumnLineage, 0),
		diagnostics:     make([]domain.CompileDiagnostic, 0),
		coverageByModel: make(map[string]string),
	}
	sourceRegistry, _, err := s.loadSourceRegistry(ctx, runCtx)
	if err != nil {
		return nil, fmt.Errorf("load source registry for analysis: %w", err)
	}
	snapshot, snapshotDiagnostics := s.buildSourceStateSnapshot(ctx, principal, sourceRegistry, artifacts)
	result.stateSnapshot = snapshot
	result.diagnostics = append(result.diagnostics, snapshotDiagnostics...)

	tiers, err := ResolveDAG(selected)
	if err != nil {
		return nil, fmt.Errorf("resolve dag for analysis: %w", err)
	}
	resolver := &compileLineageResolver{
		svc:         s,
		columnCache: make(map[string][]string),
	}
	expandedByTarget := make(map[string][]domain.ColumnLineageSourceRef)

	for _, tier := range tiers {
		for _, node := range tier {
			model := *node.Model
			artifact, ok := artifacts[model.ID]
			if !ok {
				continue
			}

			var (
				items []domain.CompiledColumnLineage
				diags []domain.CompileDiagnostic
			)
			switch model.Materialization {
			case domain.MaterializationSeed:
				items, diags = s.analyzeSeedModel(ctx, model, req)
			default:
				items, diags = s.analyzeSQLModel(ctx, model, artifact, resolver, req, expandedByTarget)
			}
			result.diagnostics = append(result.diagnostics, diags...)
			for i := range items {
				items[i].BuildID = ""
				items[i].ProjectName = model.ProjectName
				items[i].ModelName = model.QualifiedName()
				items[i].TargetCatalog = req.TargetCatalog
				items[i].TargetSchema = effectiveSchema(req.TargetSchema, model.Config.Schema)
				items[i].TargetTable = model.Name
				items[i].Sensitivity = s.inferSensitivity(ctx, items[i])
				targetKey := lineageColumnKey(items[i].TargetCatalog, items[i].TargetSchema, items[i].TargetTable) + "|" + strings.ToLower(items[i].TargetColumn)
				expandedByTarget[targetKey] = append([]domain.ColumnLineageSourceRef(nil), items[i].Sources...)
			}
			if model.Materialization != domain.MaterializationEphemeral {
				result.lineage = append(result.lineage, items...)
				result.coverageByModel[model.QualifiedName()] = lineageCoverage(items)
			}
			cols := targetColumns(items)
			resolver.columnCache[lineageColumnKey(req.TargetCatalog, effectiveSchema(req.TargetSchema, model.Config.Schema), model.Name)] = cols
			resolver.columnCache[lineageColumnKey("", effectiveSchema(req.TargetSchema, model.Config.Schema), model.Name)] = cols
		}
	}

	return result, nil
}

func (s *Service) analyzeSQLModel(
	ctx context.Context,
	model domain.Model,
	artifact compileResult,
	resolver sqlrewrite.CatalogResolver,
	req domain.TriggerModelRunRequest,
	expandedByTarget map[string][]domain.ColumnLineageSourceRef,
) ([]domain.CompiledColumnLineage, []domain.CompileDiagnostic) {
	diagnostics := make([]domain.CompileDiagnostic, 0)
	if containsSelectStar(artifact.sql) {
		diagnostics = append(diagnostics, domain.CompileDiagnostic{
			Severity:  domain.DiagnosticSeverityWarning,
			Code:      "lineage.select_star",
			Message:   "select * can cause drift and partial lineage coverage",
			ModelName: model.QualifiedName(),
		})
	}

	entries, err := sqlrewrite.ExtractColumnLineage(ctx, artifact.sql, effectiveSchema(req.TargetSchema, model.Config.Schema), resolver)
	if err != nil {
		diagnostics = append(diagnostics, domain.CompileDiagnostic{
			Severity:  domain.DiagnosticSeverityWarning,
			Code:      "lineage.analysis_failed",
			Message:   fmt.Sprintf("column lineage analysis could not fully resolve this model: %v", err),
			ModelName: model.QualifiedName(),
		})
	}
	if len(entries) == 0 {
		return nil, append(diagnostics, domain.CompileDiagnostic{
			Severity:  domain.DiagnosticSeverityWarning,
			Code:      "lineage.no_results",
			Message:   "column lineage could not determine output columns for this model",
			ModelName: model.QualifiedName(),
		})
	}

	items := make([]domain.CompiledColumnLineage, 0, len(entries))
	for _, entry := range entries {
		item := domain.CompiledColumnLineage{
			TargetColumn:  entry.TargetColumn,
			TransformType: classifyTransform(entry),
			Function:      entry.Function,
			Partial:       len(entry.Sources) == 0,
			TargetSchema:  effectiveSchema(req.TargetSchema, model.Config.Schema),
			TargetTable:   model.Name,
			TargetCatalog: req.TargetCatalog,
			ProjectName:   model.ProjectName,
			ModelName:     model.QualifiedName(),
		}
		sources := make([]domain.ColumnLineageSourceRef, 0, len(entry.Sources))
		for _, src := range entry.Sources {
			expanded := expandTransitiveSource(src, req.TargetCatalog, expandedByTarget)
			if len(expanded) == 0 {
				sources = append(sources, domain.ColumnLineageSourceRef{
					Catalog: req.TargetCatalog,
					Schema:  src.Schema,
					Table:   src.Table,
					Column:  src.Column,
					Kind:    "SOURCE",
				})
				continue
			}
			sources = append(sources, expanded...)
		}
		item.Sources = dedupeSourceRefs(sources)
		if len(item.Sources) == 0 {
			item.Partial = true
			diagnostics = append(diagnostics, domain.CompileDiagnostic{
				Severity:   domain.DiagnosticSeverityError,
				Code:       "lineage.unresolved_column",
				Message:    fmt.Sprintf("column %q could not be traced to upstream source columns", entry.TargetColumn),
				ModelName:  model.QualifiedName(),
				ColumnName: entry.TargetColumn,
			})
		} else if item.Partial {
			diagnostics = append(diagnostics, domain.CompileDiagnostic{
				Severity:   domain.DiagnosticSeverityWarning,
				Code:       "lineage.partial_coverage",
				Message:    fmt.Sprintf("column %q has partial lineage coverage", entry.TargetColumn),
				ModelName:  model.QualifiedName(),
				ColumnName: entry.TargetColumn,
			})
		}
		items = append(items, item)
	}
	return items, diagnostics
}

func (s *Service) analyzeSeedModel(ctx context.Context, model domain.Model, req domain.TriggerModelRunRequest) ([]domain.CompiledColumnLineage, []domain.CompileDiagnostic) {
	seed, err := s.seeds.GetByName(ctx, model.ProjectName, model.Name)
	if err != nil {
		return nil, []domain.CompileDiagnostic{{
			Severity:  domain.DiagnosticSeverityWarning,
			Code:      "lineage.seed_lookup_failed",
			Message:   fmt.Sprintf("seed metadata could not be loaded: %v", err),
			ModelName: model.QualifiedName(),
		}}
	}
	columns := seedColumns(*seed)
	if len(columns) == 0 {
		return nil, []domain.CompileDiagnostic{{
			Severity:  domain.DiagnosticSeverityWarning,
			Code:      "lineage.seed_columns_unknown",
			Message:   "seed columns could not be inferred; add column_types or a header row for full lineage",
			ModelName: model.QualifiedName(),
		}}
	}
	items := make([]domain.CompiledColumnLineage, 0, len(columns))
	for _, col := range columns {
		items = append(items, domain.CompiledColumnLineage{
			ProjectName:   model.ProjectName,
			ModelName:     model.QualifiedName(),
			TargetCatalog: req.TargetCatalog,
			TargetSchema:  effectiveSchema(req.TargetSchema, model.Config.Schema),
			TargetTable:   model.Name,
			TargetColumn:  col,
			TransformType: domain.TransformDirect,
			Sources: []domain.ColumnLineageSourceRef{{
				Schema: model.ProjectName,
				Table:  seed.Name,
				Column: col,
				Kind:   "SEED",
			}},
		})
	}
	return items, nil
}

func (s *Service) buildSourceStateSnapshot(
	ctx context.Context,
	principal string,
	sources map[string]compileSourceDefinition,
	artifacts map[string]compileResult,
) (*domain.BuildStateSnapshot, []domain.CompileDiagnostic) {
	used := make(map[string]struct{})
	for _, artifact := range artifacts {
		for key := range artifact.sourcesUsed {
			used[key] = struct{}{}
		}
	}
	snapshot := &domain.BuildStateSnapshot{Version: 1, Sources: make([]domain.BuildSourceStateSnapshot, 0, len(used))}
	diagnostics := make([]domain.CompileDiagnostic, 0)
	keys := mapKeys(used)
	sort.Strings(keys)
	for _, key := range keys {
		sourceDef, ok := sources[key]
		if !ok {
			continue
		}
		item := domain.BuildSourceStateSnapshot{
			SourceKey:   key,
			RelationRef: sourceDef.relationRef,
		}
		if sourceDef.freshness != nil {
			item.TimestampColumn = sourceDef.freshness.TimestampColumn
			item.MaxLagSeconds = sourceDef.freshness.MaxLagSeconds
			_, schema, table := parseRelationRef(sourceDef.relationRef, "", "")
			status, err := s.CheckSourceFreshness(ctx, principal, schema, table, sourceDef.freshness.TimestampColumn, sourceDef.freshness.MaxLagSeconds)
			if err != nil {
				diagnostics = append(diagnostics, domain.CompileDiagnostic{
					Severity:  domain.DiagnosticSeverityWarning,
					Code:      "planning.source_state_unavailable",
					Message:   fmt.Sprintf("source %s state could not be captured: %v", key, err),
					ModelName: sourceDef.projectName,
				})
			} else {
				item.LastLoadedAt = status.LastLoadedAt
				item.FreshnessBreached = !status.IsFresh
				item.StaleSince = status.StaleSince
			}
		}
		snapshot.Sources = append(snapshot.Sources, item)
	}
	return snapshot, diagnostics
}

func (s *Service) inferSensitivity(ctx context.Context, item domain.CompiledColumnLineage) *domain.ColumnSensitivityInfo {
	info := &domain.ColumnSensitivityInfo{
		Status:       "",
		Partial:      item.Partial || s.tags == nil || s.introspection == nil,
		Reasons:      make([]string, 0),
		SourceFields: make([]domain.ColumnLineageSourceRef, 0),
	}
	if s.tags == nil || s.introspection == nil {
		if info.Partial {
			info.Status = "PARTIAL"
			return info
		}
		return nil
	}
	for _, src := range item.Sources {
		if src.Schema == "" || src.Table == "" || src.Column == "" || src.Kind == "SEED" {
			continue
		}
		table, err := s.introspection.GetTableBySchemaAndName(ctx, src.Schema, src.Table)
		if err != nil {
			info.Partial = true
			continue
		}
		columns, _, err := s.introspection.ListColumns(ctx, table.ID, domain.PageRequest{MaxResults: domain.MaxMaxResults})
		if err != nil {
			info.Partial = true
			continue
		}
		var columnID string
		for _, col := range columns {
			if strings.EqualFold(col.Name, src.Column) {
				columnID = col.ID
				break
			}
		}
		if columnID == "" {
			info.Partial = true
			continue
		}
		tags, err := s.tags.ListTagsForSecurable(ctx, domain.TagSecurableTypeColumn, columnID, nil)
		if err != nil {
			info.Partial = true
			continue
		}
		if hasSensitiveTag(tags) {
			info.Status = "INFERRED_SENSITIVE"
			info.Reasons = append(info.Reasons, fmt.Sprintf("%s.%s.%s is tagged sensitive", src.Schema, src.Table, src.Column))
			info.SourceFields = append(info.SourceFields, src)
		}
	}
	info.Reasons = dedupeSorted(info.Reasons)
	info.SourceFields = dedupeSourceRefs(info.SourceFields)
	if info.Status == "" && info.Partial {
		info.Status = "PARTIAL"
	}
	if info.Status == "" && !info.Partial {
		return nil
	}
	return info
}

func (s *Service) loadRelationColumns(ctx context.Context, schema, table string) ([]string, error) {
	if s.duckDB == nil {
		return nil, nil
	}
	rows, err := s.duckDB.QueryContext(ctx,
		`SELECT column_name
		 FROM information_schema.columns
		 WHERE table_schema = ? AND table_name = ?
		 ORDER BY ordinal_position`, schema, table)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	cols := make([]string, 0)
	for rows.Next() {
		var col string
		if err := rows.Scan(&col); err != nil {
			return nil, err
		}
		cols = append(cols, col)
	}
	return cols, rows.Err()
}

func expandTransitiveSource(
	src domain.ColumnSource,
	defaultCatalog string,
	expandedByTarget map[string][]domain.ColumnLineageSourceRef,
) []domain.ColumnLineageSourceRef {
	key := lineageColumnKey(defaultCatalog, src.Schema, src.Table) + "|" + strings.ToLower(src.Column)
	if expanded, ok := expandedByTarget[key]; ok && len(expanded) > 0 {
		return append([]domain.ColumnLineageSourceRef(nil), expanded...)
	}
	return nil
}

func classifyTransform(entry domain.ColumnLineageEntry) domain.TransformType {
	if len(entry.Sources) == 0 {
		return domain.TransformUnknown
	}
	if strings.EqualFold(string(entry.TransformType), string(domain.TransformDirect)) {
		if len(entry.Sources) == 1 && !strings.EqualFold(entry.TargetColumn, entry.Sources[0].Column) {
			return domain.TransformRename
		}
		return domain.TransformDirect
	}
	if isAggregateFunction(entry.Function) {
		return domain.TransformAggregate
	}
	return domain.TransformExpression
}

func isAggregateFunction(function string) bool {
	switch strings.ToUpper(strings.TrimSpace(function)) {
	case "SUM", "COUNT", "AVG", "MIN", "MAX", "MEDIAN", "LIST", "STRING_AGG":
		return true
	default:
		return false
	}
}

func containsSelectStar(sqlText string) bool {
	upper := strings.ToUpper(sqlText)
	return strings.Contains(upper, "SELECT *") || strings.Contains(upper, ".*")
}

func lineageCoverage(items []domain.CompiledColumnLineage) string {
	if len(items) == 0 {
		return "none"
	}
	for _, item := range items {
		if item.Partial || len(item.Sources) == 0 {
			return "partial"
		}
	}
	return "full"
}

func targetColumns(items []domain.CompiledColumnLineage) []string {
	cols := make([]string, 0, len(items))
	for _, item := range items {
		cols = append(cols, item.TargetColumn)
	}
	return dedupeSorted(cols)
}

func lineageColumnKey(catalog, schema, table string) string {
	return strings.ToLower(strings.TrimSpace(catalog) + "|" + strings.TrimSpace(schema) + "|" + strings.TrimSpace(table))
}

func dedupeSourceRefs(items []domain.ColumnLineageSourceRef) []domain.ColumnLineageSourceRef {
	if len(items) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(items))
	out := make([]domain.ColumnLineageSourceRef, 0, len(items))
	for _, item := range items {
		key := strings.ToLower(item.Catalog + "|" + item.Schema + "|" + item.Table + "|" + item.Column + "|" + item.Kind + "|" + item.ModelName)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, item)
	}
	return out
}

func hasSensitiveTag(tags []domain.Tag) bool {
	for _, tag := range tags {
		key := strings.ToLower(strings.TrimSpace(tag.Key))
		value := ""
		if tag.Value != nil {
			value = strings.ToLower(strings.TrimSpace(*tag.Value))
		}
		if strings.HasPrefix(key, domain.ClassificationPrefix) {
			switch value {
			case "pii", "sensitive", "confidential", "personal_data":
				return true
			}
		}
		if strings.HasPrefix(key, domain.SensitivityPrefix) {
			return true
		}
	}
	return false
}

func seedColumns(seed domain.Seed) []string {
	if len(seed.ColumnTypes) > 0 {
		cols := make([]string, 0, len(seed.ColumnTypes))
		for name := range seed.ColumnTypes {
			cols = append(cols, name)
		}
		sort.Strings(cols)
		return cols
	}
	if !strings.EqualFold(seed.Format, "csv") || !seed.HasHeader {
		return nil
	}
	f, err := os.Open(seed.InputRef) // #nosec G304 -- seed input refs are project-owned local files in dev/test flows
	if err != nil {
		return nil
	}
	defer func() { _ = f.Close() }()
	reader := csv.NewReader(f)
	header, err := reader.Read()
	if err != nil {
		return nil
	}
	for i := range header {
		header[i] = strings.TrimSpace(header[i])
	}
	return dedupeSorted(header)
}

// GetBuildLineage returns compile-time column lineage for a build, optionally scoped to one model.
func (s *Service) GetBuildLineage(ctx context.Context, buildID string, modelName *string) ([]domain.CompiledColumnLineage, error) {
	if s.colLineage == nil {
		return nil, nil
	}
	if modelName != nil && strings.TrimSpace(*modelName) != "" {
		return s.colLineage.ListBuildLineageByModel(ctx, buildID, strings.TrimSpace(*modelName))
	}
	return s.colLineage.ListBuildLineage(ctx, buildID)
}

// GetCompilationLineage returns compile-time column lineage for a compilation, optionally scoped to one model.
func (s *Service) GetCompilationLineage(ctx context.Context, compilationID string, modelName *string) ([]domain.CompiledColumnLineage, error) {
	if s.colLineage == nil {
		return nil, nil
	}
	if modelName != nil && strings.TrimSpace(*modelName) != "" {
		return s.colLineage.ListCompilationLineageByModel(ctx, compilationID, strings.TrimSpace(*modelName))
	}
	return s.colLineage.ListCompilationLineage(ctx, compilationID)
}

// GetBuildDiagnostics returns structured diagnostics for a build with optional filters.
func (s *Service) GetBuildDiagnostics(ctx context.Context, buildID string, filter domain.BuildDiagnosticsFilter) ([]domain.CompileDiagnostic, error) {
	if s.builds == nil {
		return nil, nil
	}
	build, err := s.builds.GetByID(ctx, buildID)
	if err != nil {
		return nil, err
	}
	diagnostics := diagnosticsFromJSONOrNil(ptrValue(build.CompileDiagnostics))
	if diagnostics == nil {
		return nil, nil
	}
	items := diagnostics.Items
	if len(items) == 0 {
		items = diagnosticsToItems(*diagnostics)
	}
	out := make([]domain.CompileDiagnostic, 0, len(items))
	for _, item := range items {
		if filter.ModelName != nil && strings.TrimSpace(*filter.ModelName) != "" && item.ModelName != strings.TrimSpace(*filter.ModelName) {
			continue
		}
		if filter.Severity != nil && item.Severity != *filter.Severity {
			continue
		}
		if filter.Code != nil && item.Code != strings.TrimSpace(*filter.Code) {
			continue
		}
		out = append(out, item)
	}
	return out, nil
}

// GetCompilationDiagnostics returns structured diagnostics for a compilation with optional filters.
func (s *Service) GetCompilationDiagnostics(ctx context.Context, compilationID string, filter domain.BuildDiagnosticsFilter) ([]domain.CompileDiagnostic, error) {
	if s.compilations == nil {
		return nil, nil
	}
	compilation, err := s.compilations.GetByID(ctx, compilationID)
	if err != nil {
		return nil, err
	}
	diagnostics := diagnosticsFromJSONOrNil(ptrValue(compilation.CompileDiagnostics))
	if diagnostics == nil {
		return nil, nil
	}
	items := diagnostics.Items
	if len(items) == 0 {
		items = diagnosticsToItems(*diagnostics)
	}
	out := make([]domain.CompileDiagnostic, 0, len(items))
	for _, item := range items {
		if filter.ModelName != nil && strings.TrimSpace(*filter.ModelName) != "" && item.ModelName != strings.TrimSpace(*filter.ModelName) {
			continue
		}
		if filter.Severity != nil && item.Severity != *filter.Severity {
			continue
		}
		if filter.Code != nil && item.Code != strings.TrimSpace(*filter.Code) {
			continue
		}
		out = append(out, item)
	}
	return out, nil
}

// GetBuildSourceColumnImpact returns impacted compiled columns for a source column within a build.
func (s *Service) GetBuildSourceColumnImpact(ctx context.Context, buildID, schema, table, column string) ([]domain.CompiledColumnLineage, error) {
	if s.colLineage == nil {
		return nil, nil
	}
	return s.colLineage.ListBuildImpactsForSourceColumn(ctx, buildID, schema, table, column)
}

// GetCompilationSourceColumnImpact returns impacted compiled columns for a source column within a compilation.
func (s *Service) GetCompilationSourceColumnImpact(ctx context.Context, compilationID, schema, table, column string) ([]domain.CompiledColumnLineage, error) {
	if s.colLineage == nil {
		return nil, nil
	}
	return s.colLineage.ListCompilationImpactsForSourceColumn(ctx, compilationID, schema, table, column)
}

// PlanRebuild returns a code+data-aware rebuild plan for a project/environment.
func (s *Service) PlanRebuild(ctx context.Context, principal string, req domain.PlanRebuildRequest) (*domain.RebuildPlan, error) {
	selected, artifacts, analysisResult, runCtx, err := s.prepareBuildAnalysis(ctx, principal, req.ProjectName, req.EnvironmentName, req.Selector)
	if err != nil {
		return nil, err
	}

	plan := &domain.RebuildPlan{
		ProjectName:     runCtx.project.Name,
		EnvironmentName: runCtx.environment.Name,
		SelectedModels:  make([]domain.RebuildPlanItem, 0),
		UnchangedModels: make([]string, 0),
	}
	reasonsByModel := make(map[string]map[domain.RebuildReason]struct{})

	baselineRun, baselineBuild, err := s.latestSuccessfulRunAndBuild(ctx, runCtx.project.Name, runCtx.environment.Name)
	if err != nil {
		return nil, err
	}
	var baselineHashes map[string]string
	if baselineRun != nil && baselineRun.CompileManifest != nil {
		baselineHashes, err = modelHashByNameFromManifest(*baselineRun.CompileManifest)
		if err != nil {
			return nil, err
		}
	} else {
		baselineHashes = map[string]string{}
	}
	if baselineBuild != nil {
		plan.BaselineBuildID = &baselineBuild.ID
	}

	for _, m := range selected {
		artifact := artifacts[m.ID]
		if baselineHashes[m.QualifiedName()] != artifact.compiledHash {
			addRebuildReason(reasonsByModel, m.QualifiedName(), domain.RebuildReasonCodeModified)
		}
	}

	currentSources := snapshotBySourceKey(analysisResult.stateSnapshot)
	baselineSources := snapshotBySourceKey(buildStateSnapshotFromBuild(baselineBuild))
	for _, m := range selected {
		artifact := artifacts[m.ID]
		for sourceKey := range artifact.sourcesUsed {
			current := currentSources[sourceKey]
			baseline := baselineSources[sourceKey]
			switch {
			case current.FreshnessBreached:
				addRebuildReason(reasonsByModel, m.QualifiedName(), domain.RebuildReasonFreshnessBreached)
			case sourceStateAdvanced(current, baseline):
				addRebuildReason(reasonsByModel, m.QualifiedName(), domain.RebuildReasonUpstreamDataChanged)
			}
		}
	}

	reverseDeps := reverseModelDependencies(selected)
	queue := make([]string, 0)
	for modelName := range reasonsByModel {
		queue = append(queue, modelName)
	}
	sort.Strings(queue)
	seen := make(map[string]struct{}, len(queue))
	for len(queue) > 0 {
		modelName := queue[0]
		queue = queue[1:]
		if _, ok := seen[modelName]; ok {
			continue
		}
		seen[modelName] = struct{}{}
		reasons := reasonsByModel[modelName]
		for _, downstream := range reverseDeps[modelName] {
			if _, exists := reasonsByModel[downstream]; !exists {
				reasonsByModel[downstream] = make(map[domain.RebuildReason]struct{})
			}
			if hasRebuildReason(reasons, domain.RebuildReasonCodeModified) || hasRebuildReason(reasons, domain.RebuildReasonUpstreamCodeChanged) {
				reasonsByModel[downstream][domain.RebuildReasonUpstreamCodeChanged] = struct{}{}
			}
			if hasRebuildReason(reasons, domain.RebuildReasonUpstreamDataChanged) || hasRebuildReason(reasons, domain.RebuildReasonFreshnessBreached) {
				reasonsByModel[downstream][domain.RebuildReasonUpstreamDataChanged] = struct{}{}
			}
			queue = append(queue, downstream)
		}
	}

	for _, m := range selected {
		reasons := sortedRebuildReasons(reasonsByModel[m.QualifiedName()])
		if len(reasons) == 0 {
			plan.UnchangedModels = append(plan.UnchangedModels, m.QualifiedName())
			continue
		}
		plan.SelectedModels = append(plan.SelectedModels, domain.RebuildPlanItem{ModelName: m.QualifiedName(), Reasons: reasons})
	}
	return plan, nil
}

// CompareBuilds returns a machine-readable build diff.
func (s *Service) CompareBuilds(ctx context.Context, principal string, req domain.CompareBuildsRequest) (*domain.BuildCompareResult, error) {
	if s.builds == nil {
		return nil, domain.ErrNotImplemented("builds are not configured")
	}
	fromBuild, err := s.builds.GetByID(ctx, req.FromBuildID)
	if err != nil {
		return nil, err
	}
	fromManifest, err := parseCompileManifest(fromBuild.CompileManifest)
	if err != nil {
		return nil, err
	}
	fromLineage, err := s.GetBuildLineage(ctx, fromBuild.ID, nil)
	if err != nil {
		return nil, err
	}
	fromDiagnostics, err := s.GetBuildDiagnostics(ctx, fromBuild.ID, domain.BuildDiagnosticsFilter{})
	if err != nil {
		return nil, err
	}

	result := &domain.BuildCompareResult{
		ProjectName: fromBuild.ProjectName,
		FromBuildID: fromBuild.ID,
		ModelDiffs:  make([]domain.BuildCompareModelDiff, 0),
	}

	var (
		toManifest     compileManifest
		toLineage      []domain.CompiledColumnLineage
		toDiagnostics  []domain.CompileDiagnostic
		impactedModels map[string][]string
	)

	if req.CompareToHead {
		selected, artifacts, analysisResult, runCtx, err := s.prepareBuildAnalysis(ctx, principal, fromBuild.ProjectName, fromBuild.EnvironmentName, fromBuild.Selector)
		if err != nil {
			return nil, err
		}
		manifestJSON, err := buildCompileManifest(selected, artifacts, runCtx, analysisResult.coverageByModel)
		if err != nil {
			return nil, err
		}
		toManifest, err = parseCompileManifest(manifestJSON)
		if err != nil {
			return nil, err
		}
		toLineage = analysisResult.lineage
		toDiagnostics = dedupeDiagnostics(analysisResult.diagnostics)
		result.ComparedToHead = true
	} else {
		if req.ToBuildID == nil || strings.TrimSpace(*req.ToBuildID) == "" {
			return nil, domain.ErrValidation("to_build_id is required unless compare_to_head is true")
		}
		toBuild, err := s.builds.GetByID(ctx, strings.TrimSpace(*req.ToBuildID))
		if err != nil {
			return nil, err
		}
		result.ToBuildID = &toBuild.ID
		toManifest, err = parseCompileManifest(toBuild.CompileManifest)
		if err != nil {
			return nil, err
		}
		toLineage, err = s.GetBuildLineage(ctx, toBuild.ID, nil)
		if err != nil {
			return nil, err
		}
		toDiagnostics, err = s.GetBuildDiagnostics(ctx, toBuild.ID, domain.BuildDiagnosticsFilter{})
		if err != nil {
			return nil, err
		}
	}
	impactedModels = downstreamImpactsFromManifest(toManifest)

	allModelNames := mergeStringSets(mapKeys(fromManifest.Models), mapKeys(toManifest.Models))
	for _, modelName := range allModelNames {
		fromModel, hasFrom := fromManifest.Models[modelName]
		toModel, hasTo := toManifest.Models[modelName]
		diff := domain.BuildCompareModelDiff{ModelName: modelName}
		switch {
		case !hasFrom:
			diff.ChangeType = "ADDED"
		case !hasTo:
			diff.ChangeType = "REMOVED"
		case fromModel.CompiledHash != toModel.CompiledHash:
			diff.ChangeType = "MODIFIED"
		default:
			continue
		}
		diff.FromCompiledHash = fromModel.CompiledHash
		diff.ToCompiledHash = toModel.CompiledHash
		diff.AddedColumns, diff.RemovedColumns, diff.ChangedColumns = compareModelLineage(lineageByModel(fromLineage, modelName), lineageByModel(toLineage, modelName))
		diff.ImpactedModels = impactedModels[modelName]
		diff.ImpactedTests = s.impactedTests(ctx, diff.ImpactedModels)
		diff.ImpactedProducts = s.impactedProducts(ctx, fromBuild.ProjectName)
		result.ModelDiffs = append(result.ModelDiffs, diff)
	}
	result.DiagnosticsAdded, result.DiagnosticsRemoved = diffDiagnostics(fromDiagnostics, toDiagnostics)
	return result, nil
}

// GetModelImpact returns downstream impact for a model within a build.
func (s *Service) GetModelImpact(ctx context.Context, projectName string, buildID *string, modelName string) (*domain.BuildImpactResult, error) {
	manifest, effectiveBuildID, err := s.loadImpactManifest(ctx, projectName, buildID)
	if err != nil {
		return nil, err
	}
	impacted := downstreamImpactsFromManifest(manifest)[modelName]
	lineageItems := make([]domain.CompiledColumnLineage, 0)
	if effectiveBuildID != nil {
		lineageItems, _ = s.GetBuildLineage(ctx, *effectiveBuildID, &modelName)
	}
	return &domain.BuildImpactResult{
		ProjectName:      projectName,
		Kind:             "MODEL",
		Key:              modelName,
		BuildID:          effectiveBuildID,
		ImpactedModels:   impacted,
		ImpactedColumns:  lineageItems,
		ImpactedTests:    s.impactedTests(ctx, impacted),
		ImpactedProducts: s.impactedProducts(ctx, projectName),
	}, nil
}

// GetMacroImpact returns models/tests/products impacted by a macro.
func (s *Service) GetMacroImpact(ctx context.Context, projectName string, buildID *string, macroName string) (*domain.BuildImpactResult, error) {
	manifest, effectiveBuildID, err := s.loadImpactManifest(ctx, projectName, buildID)
	if err != nil {
		return nil, err
	}
	impacted := make([]string, 0)
	for modelName, item := range manifest.Models {
		if containsString(item.MacrosUsed, macroName) {
			impacted = append(impacted, modelName)
		}
	}
	sort.Strings(impacted)
	return &domain.BuildImpactResult{
		ProjectName:      projectName,
		Kind:             "MACRO",
		Key:              macroName,
		BuildID:          effectiveBuildID,
		ImpactedModels:   impacted,
		ImpactedTests:    s.impactedTests(ctx, impacted),
		ImpactedProducts: s.impactedProducts(ctx, projectName),
	}, nil
}

// GetCompilationModelImpact returns downstream impact for a model within a compilation artifact.
func (s *Service) GetCompilationModelImpact(ctx context.Context, compilationID string, modelName string) (*domain.BuildImpactResult, error) {
	if s.compilations == nil {
		return nil, domain.ErrNotImplemented("compilations are not configured")
	}
	compilation, err := s.compilations.GetByID(ctx, compilationID)
	if err != nil {
		return nil, err
	}
	manifest, err := parseCompileManifest(compilation.CompileManifest)
	if err != nil {
		return nil, fmt.Errorf("parse compilation manifest: %w", err)
	}
	impacted := downstreamImpactsFromManifest(manifest)[modelName]
	lineageItems, _ := s.GetCompilationLineage(ctx, compilation.ID, &modelName)
	return &domain.BuildImpactResult{
		ProjectName:      compilation.ProjectName,
		Kind:             "MODEL",
		Key:              modelName,
		ImpactedModels:   impacted,
		ImpactedColumns:  lineageItems,
		ImpactedTests:    s.impactedTests(ctx, impacted),
		ImpactedProducts: s.impactedProducts(ctx, compilation.ProjectName),
	}, nil
}

// GetCompilationMacroImpact returns model/test/product impact for a macro within a compilation artifact.
func (s *Service) GetCompilationMacroImpact(ctx context.Context, compilationID string, macroName string) (*domain.BuildImpactResult, error) {
	if s.compilations == nil {
		return nil, domain.ErrNotImplemented("compilations are not configured")
	}
	compilation, err := s.compilations.GetByID(ctx, compilationID)
	if err != nil {
		return nil, err
	}
	manifest, err := parseCompileManifest(compilation.CompileManifest)
	if err != nil {
		return nil, fmt.Errorf("parse compilation manifest: %w", err)
	}
	impacted := make([]string, 0)
	for modelName, item := range manifest.Models {
		if containsString(item.MacrosUsed, macroName) {
			impacted = append(impacted, modelName)
		}
	}
	sort.Strings(impacted)
	return &domain.BuildImpactResult{
		ProjectName:      compilation.ProjectName,
		Kind:             "MACRO",
		Key:              macroName,
		ImpactedModels:   impacted,
		ImpactedTests:    s.impactedTests(ctx, impacted),
		ImpactedProducts: s.impactedProducts(ctx, compilation.ProjectName),
	}, nil
}

func (s *Service) prepareBuildAnalysis(
	ctx context.Context,
	principal string,
	projectName string,
	environmentName string,
	selector string,
) ([]domain.Model, map[string]compileResult, *compileAnalysisResult, *resolvedRunContext, error) {
	req := domain.TriggerModelRunRequest{
		ProjectName:     projectName,
		EnvironmentName: environmentName,
		Selector:        selector,
		TriggerType:     domain.ModelTriggerTypeManual,
		Variables:       map[string]string{},
	}
	runCtx, err := s.resolveExecutionContext(ctx, projectName, environmentName, req)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	req.TargetCatalog = runCtx.targetCatalog
	req.TargetSchema = runCtx.targetSchema
	req.Variables = cloneStringMap(runCtx.variables)
	allModels, _, err := s.loadCompilationModelScope(ctx, runCtx)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	selected := filterByProject(allModels, runCtx.project.Name)
	selected, _, err = s.selectModelsForRun(ctx, principal, req, selected, allModels, runCtx)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	artifacts, _, err := s.compileSelectedModels(ctx, principal, selected, allModels, runCtx, req)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	rollupEphemeralArtifacts(selected, artifacts)
	analysisResult, err := s.analyzeCompiledModels(ctx, principal, selected, artifacts, runCtx, req)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	selected = resolveEphemeralModels(selected, req.TargetCatalog, req.TargetSchema)
	if err := s.syncCompiledArtifacts(selected, artifacts, req); err != nil {
		return nil, nil, nil, nil, err
	}
	return selected, artifacts, analysisResult, runCtx, nil
}

func (s *Service) latestSuccessfulRunAndBuild(ctx context.Context, projectName, environmentName string) (*domain.ModelRun, *domain.Build, error) {
	status := domain.ModelRunStatusSuccess
	runs, _, err := s.runs.ListRuns(ctx, domain.ModelRunFilter{
		Status: &status,
		Page:   domain.PageRequest{MaxResults: domain.MaxMaxResults},
	})
	if err != nil {
		return nil, nil, err
	}
	for i := range runs {
		run := runs[i]
		if run.ProjectName != projectName || run.EnvironmentName != environmentName {
			continue
		}
		if run.BuildID == nil || s.builds == nil {
			return &run, nil, nil
		}
		build, err := s.builds.GetByID(ctx, *run.BuildID)
		if err != nil {
			return &run, nil, err
		}
		return &run, build, nil
	}
	return nil, nil, nil
}

type compileManifestModel struct {
	ModelName       string             `json:"model_name"`
	CompiledHash    string             `json:"compiled_hash"`
	DependsOn       []string           `json:"depends_on,omitempty"`
	MacrosUsed      []string           `json:"macros_used,omitempty"`
	LineageCoverage string             `json:"lineage_coverage,omitempty"`
	EffectiveConfig domain.ModelConfig `json:"effective_config,omitempty"`
}

type compileManifest struct {
	ProjectName     string                          `json:"project_name,omitempty"`
	EnvironmentName string                          `json:"environment_name,omitempty"`
	Models          map[string]compileManifestModel `json:"-"`
	orderedModels   []compileManifestModel
}

func parseCompileManifest(raw string) (compileManifest, error) {
	type payload struct {
		ProjectName     string                 `json:"project_name,omitempty"`
		EnvironmentName string                 `json:"environment_name,omitempty"`
		Models          []compileManifestModel `json:"models"`
	}
	var decoded payload
	if err := json.Unmarshal([]byte(raw), &decoded); err != nil {
		return compileManifest{}, err
	}
	out := compileManifest{
		ProjectName:     decoded.ProjectName,
		EnvironmentName: decoded.EnvironmentName,
		Models:          make(map[string]compileManifestModel, len(decoded.Models)),
		orderedModels:   decoded.Models,
	}
	for _, item := range decoded.Models {
		out.Models[item.ModelName] = item
	}
	return out, nil
}

func snapshotBySourceKey(snapshot *domain.BuildStateSnapshot) map[string]domain.BuildSourceStateSnapshot {
	out := make(map[string]domain.BuildSourceStateSnapshot)
	if snapshot == nil {
		return out
	}
	for _, item := range snapshot.Sources {
		out[item.SourceKey] = item
	}
	return out
}

func buildStateSnapshotFromBuild(build *domain.Build) *domain.BuildStateSnapshot {
	if build == nil || build.StateSnapshot == nil || strings.TrimSpace(*build.StateSnapshot) == "" {
		return nil
	}
	var snapshot domain.BuildStateSnapshot
	if err := json.Unmarshal([]byte(*build.StateSnapshot), &snapshot); err != nil {
		return nil
	}
	return &snapshot
}

func sourceStateAdvanced(current, baseline domain.BuildSourceStateSnapshot) bool {
	if current.LastLoadedAt == nil {
		return false
	}
	if baseline.LastLoadedAt == nil {
		return true
	}
	return current.LastLoadedAt.After(*baseline.LastLoadedAt)
}

func reverseModelDependencies(models []domain.Model) map[string][]string {
	out := make(map[string][]string)
	for _, model := range models {
		for _, dep := range model.DependsOn {
			if strings.HasPrefix(dep, "source:") {
				continue
			}
			out[dep] = append(out[dep], model.QualifiedName())
		}
	}
	for key := range out {
		sort.Strings(out[key])
	}
	return out
}

func addRebuildReason(target map[string]map[domain.RebuildReason]struct{}, modelName string, reason domain.RebuildReason) {
	if _, ok := target[modelName]; !ok {
		target[modelName] = make(map[domain.RebuildReason]struct{})
	}
	target[modelName][reason] = struct{}{}
}

func hasRebuildReason(items map[domain.RebuildReason]struct{}, reason domain.RebuildReason) bool {
	if items == nil {
		return false
	}
	_, ok := items[reason]
	return ok
}

func sortedRebuildReasons(items map[domain.RebuildReason]struct{}) []domain.RebuildReason {
	if len(items) == 0 {
		return nil
	}
	out := make([]domain.RebuildReason, 0, len(items))
	for reason := range items {
		out = append(out, reason)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

func diagnosticsToItems(d domain.ModelCompileDiagnostics) []domain.CompileDiagnostic {
	out := make([]domain.CompileDiagnostic, 0, len(d.Warnings)+len(d.Errors))
	for _, warning := range d.Warnings {
		out = append(out, domain.CompileDiagnostic{Severity: domain.DiagnosticSeverityWarning, Code: "compile.warning", Message: warning})
	}
	for _, item := range d.Errors {
		out = append(out, domain.CompileDiagnostic{Severity: domain.DiagnosticSeverityError, Code: "compile.error", Message: item})
	}
	return out
}

func ptrValue(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

func downstreamImpactsFromManifest(manifest compileManifest) map[string][]string {
	reverse := make(map[string][]string)
	for _, model := range manifest.orderedModels {
		for _, dep := range model.DependsOn {
			if strings.HasPrefix(dep, "source:") {
				continue
			}
			reverse[dep] = append(reverse[dep], model.ModelName)
		}
	}
	for key := range reverse {
		reverse[key] = dedupeSorted(reverse[key])
	}
	result := make(map[string][]string)
	for modelName := range manifest.Models {
		queue := append([]string(nil), reverse[modelName]...)
		seen := make(map[string]struct{})
		for len(queue) > 0 {
			name := queue[0]
			queue = queue[1:]
			if _, ok := seen[name]; ok {
				continue
			}
			seen[name] = struct{}{}
			result[modelName] = append(result[modelName], name)
			queue = append(queue, reverse[name]...)
		}
		sort.Strings(result[modelName])
	}
	return result
}

func lineageByModel(items []domain.CompiledColumnLineage, modelName string) []domain.CompiledColumnLineage {
	out := make([]domain.CompiledColumnLineage, 0)
	for _, item := range items {
		if item.ModelName == modelName {
			out = append(out, item)
		}
	}
	return out
}

func compareModelLineage(from, to []domain.CompiledColumnLineage) ([]string, []string, []string) {
	fromMap := make(map[string]string)
	toMap := make(map[string]string)
	for _, item := range from {
		fromMap[item.TargetColumn] = lineageSignature(item)
	}
	for _, item := range to {
		toMap[item.TargetColumn] = lineageSignature(item)
	}
	added := make([]string, 0)
	removed := make([]string, 0)
	changed := make([]string, 0)
	for col, sig := range toMap {
		if fromSig, ok := fromMap[col]; !ok {
			added = append(added, col)
		} else if fromSig != sig {
			changed = append(changed, col)
		}
	}
	for col := range fromMap {
		if _, ok := toMap[col]; !ok {
			removed = append(removed, col)
		}
	}
	sort.Strings(added)
	sort.Strings(removed)
	sort.Strings(changed)
	return added, removed, changed
}

func lineageSignature(item domain.CompiledColumnLineage) string {
	parts := []string{string(item.TransformType), item.Function}
	for _, src := range item.Sources {
		parts = append(parts, src.Catalog, src.Schema, src.Table, src.Column, src.Kind, src.ModelName)
	}
	return strings.Join(parts, "|")
}

func diffDiagnostics(from, to []domain.CompileDiagnostic) ([]domain.CompileDiagnostic, []domain.CompileDiagnostic) {
	fromMap := make(map[string]domain.CompileDiagnostic)
	toMap := make(map[string]domain.CompileDiagnostic)
	for _, item := range from {
		fromMap[diagnosticKey(item)] = item
	}
	for _, item := range to {
		toMap[diagnosticKey(item)] = item
	}
	added := make([]domain.CompileDiagnostic, 0)
	removed := make([]domain.CompileDiagnostic, 0)
	for key, item := range toMap {
		if _, ok := fromMap[key]; !ok {
			added = append(added, item)
		}
	}
	for key, item := range fromMap {
		if _, ok := toMap[key]; !ok {
			removed = append(removed, item)
		}
	}
	return dedupeDiagnostics(added), dedupeDiagnostics(removed)
}

func diagnosticKey(item domain.CompileDiagnostic) string {
	return strings.Join([]string{string(item.Severity), item.Code, item.Message, item.ModelName, item.ColumnName}, "|")
}

func (s *Service) impactedTests(ctx context.Context, modelNames []string) []string {
	out := make([]string, 0)
	if s.tests == nil {
		return out
	}
	for _, qualified := range dedupeSorted(modelNames) {
		projectName, modelName := splitQualifiedName(qualified)
		modelItem, err := s.models.GetByName(ctx, projectName, modelName)
		if err != nil {
			continue
		}
		tests, err := s.tests.ListByModel(ctx, modelItem.ID)
		if err != nil {
			continue
		}
		for _, test := range tests {
			out = append(out, test.Name)
		}
	}
	return dedupeSorted(out)
}

func (s *Service) impactedProducts(ctx context.Context, projectName string) []string {
	project, err := s.projects.GetByName(ctx, projectName)
	if err != nil || project.ProductID == nil {
		return nil
	}
	return []string{*project.ProductID}
}

func splitQualifiedName(name string) (string, string) {
	parts := strings.SplitN(name, ".", 2)
	if len(parts) != 2 {
		return "", name
	}
	return parts[0], parts[1]
}

func (s *Service) loadImpactManifest(ctx context.Context, projectName string, buildID *string) (compileManifest, *string, error) {
	if buildID != nil && strings.TrimSpace(*buildID) != "" {
		build, err := s.builds.GetByID(ctx, strings.TrimSpace(*buildID))
		if err != nil {
			return compileManifest{}, nil, err
		}
		manifest, err := parseCompileManifest(build.CompileManifest)
		if err != nil {
			return compileManifest{}, nil, err
		}
		return manifest, &build.ID, nil
	}
	builds, _, err := s.builds.ListByProject(ctx, s.mustProjectID(ctx, projectName), domain.PageRequest{MaxResults: 1})
	if err != nil || len(builds) == 0 {
		return compileManifest{}, nil, err
	}
	manifest, err := parseCompileManifest(builds[0].CompileManifest)
	if err != nil {
		return compileManifest{}, nil, err
	}
	return manifest, &builds[0].ID, nil
}

func (s *Service) mustProjectID(ctx context.Context, projectName string) string {
	project, err := s.projects.GetByName(ctx, projectName)
	if err != nil {
		return ""
	}
	return project.ID
}

func mergeStringSets(a, b []string) []string {
	seen := make(map[string]struct{}, len(a)+len(b))
	out := make([]string, 0, len(a)+len(b))
	for _, item := range append(append([]string(nil), a...), b...) {
		if _, ok := seen[item]; ok {
			continue
		}
		seen[item] = struct{}{}
		out = append(out, item)
	}
	sort.Strings(out)
	return out
}

func containsString(items []string, value string) bool {
	for _, item := range items {
		if item == value {
			return true
		}
	}
	return false
}
