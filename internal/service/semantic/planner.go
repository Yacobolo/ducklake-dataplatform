package semantic

import (
	"context"
	"fmt"
	"math"
	"regexp"
	"sort"
	"strings"

	"duck-demo/internal/domain"
	"duck-demo/internal/sqlrewrite"
)

type edge struct {
	from string
	to   string
	rel  domain.SemanticRelationship
}

var forbiddenSQLPatterns = []*regexp.Regexp{
	regexp.MustCompile(`(?i)--`),
	regexp.MustCompile(`(?i)/\*`),
	regexp.MustCompile(`(?i)\*/`),
	regexp.MustCompile(`(?i)\b(attach|detach|install|load|pragma|copy|import|export|call)\b`),
	regexp.MustCompile(`(?i)\b(read_csv|read_json|read_parquet|parquet_scan|httpfs)\b`),
}

// ExplainMetricQuery compiles a semantic metric request into executable SQL and join metadata.
func (s *Service) ExplainMetricQuery(ctx context.Context, req MetricQueryRequest) (*MetricQueryPlan, error) {
	if strings.TrimSpace(req.ProjectName) == "" {
		return nil, domain.ErrValidation("project_name is required")
	}
	if strings.TrimSpace(req.SemanticModelName) == "" {
		return nil, domain.ErrValidation("semantic_model_name is required")
	}
	if len(req.Metrics) == 0 {
		return nil, domain.ErrValidation("at least one metric is required")
	}
	if req.Limit != nil && *req.Limit <= 0 {
		return nil, domain.ErrValidation("limit must be > 0")
	}
	if err := validateRequestSQLFragments(req); err != nil {
		return nil, err
	}

	baseModel, err := s.models.GetByName(ctx, req.ProjectName, req.SemanticModelName)
	if err != nil {
		return nil, err
	}

	metricList, err := s.metrics.ListByModel(ctx, baseModel.ID)
	if err != nil {
		return nil, err
	}
	metricByName := make(map[string]domain.SemanticMetric, len(metricList))
	for _, m := range metricList {
		metricByName[m.Name] = m
	}

	selectedMetrics := make([]domain.SemanticMetric, 0, len(req.Metrics))
	for _, name := range req.Metrics {
		m, ok := metricByName[name]
		if !ok {
			return nil, domain.ErrValidation("metric %q not found in semantic model %q", name, req.SemanticModelName)
		}
		if err := validateMetricExpression(m); err != nil {
			return nil, err
		}
		selectedMetrics = append(selectedMetrics, m)
	}
	resolvedMetricSQL := make(map[string]string, len(selectedMetrics))
	for _, metric := range selectedMetrics {
		sqlExpr, err := resolveMetricExpression(metric.Name, metricByName, map[string]bool{})
		if err != nil {
			return nil, err
		}
		resolvedMetricSQL[metric.Name] = applyMetricFilterSQL(metric, sqlExpr)
	}

	models, _, err := s.models.List(ctx, &req.ProjectName, domain.PageRequest{MaxResults: 10000})
	if err != nil {
		return nil, fmt.Errorf("list semantic models: %w", err)
	}
	modelByName := make(map[string]domain.SemanticModel, len(models))
	modelByID := make(map[string]domain.SemanticModel, len(models))
	for _, m := range models {
		modelByName[m.Name] = m
		modelByID[m.ID] = m
	}

	relationships, _, err := s.relationships.List(ctx, domain.PageRequest{MaxResults: 10000})
	if err != nil {
		return nil, fmt.Errorf("list semantic relationships: %w", err)
	}

	needModels := map[string]bool{baseModel.Name: true}
	for _, dim := range req.Dimensions {
		prefix := modelPrefix(dim)
		if prefix != "" {
			if _, ok := modelByName[prefix]; !ok {
				return nil, domain.ErrValidation("dimension %q references unknown semantic model %q", dim, prefix)
			}
			needModels[prefix] = true
		}
	}

	joinSteps := []JoinStep{}
	joins := []string{}
	joinedNames := map[string]bool{baseModel.Name: true}
	for needed := range needModels {
		if needed == baseModel.Name {
			continue
		}
		path, err := shortestPath(baseModel.Name, needed, relationships, modelByID)
		if err != nil {
			return nil, err
		}
		for _, step := range path {
			if joinedNames[step.ToModel] {
				continue
			}
			switch step.RelationshipType {
			case domain.RelationshipTypeManyToOne, domain.RelationshipTypeOneToOne:
			default:
				return nil, domain.ErrValidation("relationship %q is not join-safe for metric queries in dashboards", step.RelationshipName)
			}
			joins = append(joins, fmt.Sprintf("LEFT JOIN %s AS %s ON %s", modelByName[step.ToModel].BaseModelRef, step.ToModel, step.JoinSQL))
			joinedNames[step.ToModel] = true
			joinSteps = append(joinSteps, step)
		}
	}

	selectedPreAgg, preAggRelation := s.matchPreAggregation(ctx, baseModel.ID, req)
	fromRelation := baseModel.BaseModelRef
	if preAggRelation != "" {
		fromRelation = preAggRelation
		joins = nil
		joinSteps = nil
	}

	selectParts := make([]string, 0, len(req.Dimensions)+len(selectedMetrics))
	groupByParts := make([]string, 0, len(req.Dimensions))
	for _, dim := range req.Dimensions {
		selectParts = append(selectParts, dim)
		groupByParts = append(groupByParts, dim)
	}
	if req.TimeGrain != nil && strings.TrimSpace(*req.TimeGrain) != "" {
		timeDim := strings.TrimSpace(baseModel.DefaultTimeDimension)
		if timeDim == "" {
			return nil, domain.ErrValidation("semantic model %q does not define a default_time_dimension", baseModel.Name)
		}
		timeExpr := fmt.Sprintf("date_trunc('%s', %s) AS %s", strings.TrimSpace(*req.TimeGrain), timeDim, timeGrainAlias)
		selectParts = append(selectParts, timeExpr)
		groupByParts = append(groupByParts, timeGrainAlias)
	}
	for _, m := range selectedMetrics {
		selectParts = append(selectParts, fmt.Sprintf("%s AS %s", resolvedMetricSQL[m.Name], m.Name))
	}

	query := fmt.Sprintf("SELECT %s FROM %s AS %s", strings.Join(selectParts, ", "), fromRelation, baseModel.Name)
	if len(joins) > 0 {
		query += " " + strings.Join(joins, " ")
	}
	if len(req.Filters) > 0 {
		query += " WHERE " + strings.Join(req.Filters, " AND ")
	}
	if len(groupByParts) > 0 {
		query += " GROUP BY " + strings.Join(groupByParts, ", ")
	}
	if len(req.OrderBy) > 0 {
		query += " ORDER BY " + strings.Join(req.OrderBy, ", ")
	}
	if req.Limit != nil && *req.Limit > 0 {
		query += fmt.Sprintf(" LIMIT %d", *req.Limit)
	}

	freshnessBasis := []string{baseModel.BaseModelRef}
	for _, step := range joinSteps {
		if model, ok := modelByName[step.ToModel]; ok {
			freshnessBasis = append(freshnessBasis, model.BaseModelRef)
		}
	}
	sort.Strings(freshnessBasis)

	return &MetricQueryPlan{
		BaseModelName:          baseModel.Name,
		BaseRelation:           baseModel.BaseModelRef,
		Metrics:                req.Metrics,
		Dimensions:             req.Dimensions,
		TimeGrain:              req.TimeGrain,
		JoinPath:               joinSteps,
		SelectedPreAggregation: selectedPreAgg,
		GeneratedSQL:           query,
		FreshnessStatus:        "UNKNOWN",
		FreshnessBasis:         freshnessBasis,
	}, nil
}

func validateMetricExpression(metric domain.SemanticMetric) error {
	expr := strings.TrimSpace(metric.Expression)
	if expr == "" {
		return domain.ErrValidation("metric %q has empty expression", metric.Name)
	}
	if metric.ExpressionMode == domain.MetricExpressionModeSQL {
		if strings.Contains(expr, ";") {
			return domain.ErrValidation("metric %q SQL expression must not contain semicolons", metric.Name)
		}
		if forbidden := matchForbiddenSQLPattern(expr); forbidden != "" {
			return domain.ErrValidation("metric %q SQL expression contains forbidden token: %s", metric.Name, forbidden)
		}
		stmt := fmt.Sprintf("SELECT %s FROM semantic_expr_guard", expr)
		stmtType, err := sqlrewrite.ClassifyStatement(stmt)
		if err != nil {
			return domain.ErrValidation("metric %q SQL expression is invalid: %v", metric.Name, err)
		}
		if stmtType != sqlrewrite.StmtSelect {
			return domain.ErrValidation("metric %q SQL expression must compile to SELECT", metric.Name)
		}
	}
	return nil
}

func validateRequestSQLFragments(req MetricQueryRequest) error {
	for _, dim := range req.Dimensions {
		expr := strings.TrimSpace(dim)
		if expr == "" {
			return domain.ErrValidation("dimensions must not include empty values")
		}
		if strings.Contains(expr, ";") {
			return domain.ErrValidation("dimension %q must not contain semicolons", dim)
		}
		if forbidden := matchForbiddenSQLPattern(expr); forbidden != "" {
			return domain.ErrValidation("dimension %q contains forbidden token: %s", dim, forbidden)
		}
		if _, err := sqlrewrite.ClassifyStatement(fmt.Sprintf("SELECT %s FROM semantic_expr_guard", expr)); err != nil {
			return domain.ErrValidation("dimension %q is invalid SQL: %v", dim, err)
		}
	}

	for _, filter := range req.Filters {
		expr := strings.TrimSpace(filter)
		if expr == "" {
			return domain.ErrValidation("filters must not include empty values")
		}
		if strings.Contains(expr, ";") {
			return domain.ErrValidation("filter %q must not contain semicolons", filter)
		}
		if forbidden := matchForbiddenSQLPattern(expr); forbidden != "" {
			return domain.ErrValidation("filter %q contains forbidden token: %s", filter, forbidden)
		}
		if _, err := sqlrewrite.ClassifyStatement(fmt.Sprintf("SELECT 1 FROM semantic_expr_guard WHERE (%s)", expr)); err != nil {
			return domain.ErrValidation("filter %q is invalid SQL: %v", filter, err)
		}
	}

	for _, orderBy := range req.OrderBy {
		expr := strings.TrimSpace(orderBy)
		if expr == "" {
			return domain.ErrValidation("order_by must not include empty values")
		}
		if strings.Contains(expr, ";") {
			return domain.ErrValidation("order_by %q must not contain semicolons", orderBy)
		}
		if forbidden := matchForbiddenSQLPattern(expr); forbidden != "" {
			return domain.ErrValidation("order_by %q contains forbidden token: %s", orderBy, forbidden)
		}
		if _, err := sqlrewrite.ClassifyStatement(fmt.Sprintf("SELECT 1 FROM semantic_expr_guard ORDER BY %s", expr)); err != nil {
			return domain.ErrValidation("order_by %q is invalid SQL: %v", orderBy, err)
		}
	}

	return nil
}

func matchForbiddenSQLPattern(expr string) string {
	for _, pattern := range forbiddenSQLPatterns {
		if loc := pattern.FindStringIndex(expr); loc != nil {
			return expr[loc[0]:loc[1]]
		}
	}
	return ""
}

func metricSQLExpression(metric domain.SemanticMetric) string {
	if metric.ExpressionMode == domain.MetricExpressionModeSQL {
		return metric.Expression
	}
	return metric.Expression
}

const timeGrainAlias = "__time_grain"

func resolveMetricExpression(name string, metricByName map[string]domain.SemanticMetric, visiting map[string]bool) (string, error) {
	metric, ok := metricByName[name]
	if !ok {
		return "", domain.ErrValidation("metric %q not found", name)
	}
	if visiting[name] {
		return "", domain.ErrValidation("metric %q contains a derived-metric cycle", name)
	}

	visiting[name] = true
	defer delete(visiting, name)

	expr := metricSQLExpression(metric)
	refs := metricReferencePattern.FindAllStringSubmatch(expr, -1)
	for _, ref := range refs {
		refName := ref[1]
		resolved, err := resolveMetricExpression(refName, metricByName, visiting)
		if err != nil {
			return "", err
		}
		expr = strings.ReplaceAll(expr, ref[0], fmt.Sprintf("(%s)", resolved))
	}
	return expr, nil
}

var metricReferencePattern = regexp.MustCompile(`\$\{([a-zA-Z0-9_]+)\}`)

func applyMetricFilterSQL(metric domain.SemanticMetric, expr string) string {
	_ = metric
	return expr
}

func modelPrefix(identifier string) string {
	parts := strings.SplitN(strings.TrimSpace(identifier), ".", 2)
	if len(parts) == 2 {
		return parts[0]
	}
	return ""
}

func shortestPath(baseName, targetName string, relationships []domain.SemanticRelationship, modelByID map[string]domain.SemanticModel) ([]JoinStep, error) {
	if baseName == targetName {
		return nil, nil
	}

	adj := map[string][]edge{}
	for _, rel := range relationships {
		fromModel, okFrom := modelByID[rel.FromSemanticID]
		toModel, okTo := modelByID[rel.ToSemanticID]
		if !okFrom || !okTo {
			continue
		}
		adj[fromModel.Name] = append(adj[fromModel.Name], edge{from: fromModel.Name, to: toModel.Name, rel: rel})
		adj[toModel.Name] = append(adj[toModel.Name], edge{from: toModel.Name, to: fromModel.Name, rel: rel})
	}

	dist := map[string]int{baseName: 0}
	pathCount := map[string]int{baseName: 1}
	parent := map[string]edge{}
	queue := []string{baseName}

	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		for _, next := range adj[cur] {
			nd := dist[cur] + 1
			cd, ok := dist[next.to]
			if !ok {
				cd = math.MaxInt
			}

			if nd < cd {
				dist[next.to] = nd
				pathCount[next.to] = pathCount[cur]
				parent[next.to] = edge{from: cur, to: next.to, rel: next.rel}
				queue = append(queue, next.to)
			} else if nd == cd {
				pathCount[next.to] += pathCount[cur]
			}
		}
	}

	if _, ok := dist[targetName]; !ok {
		return nil, domain.ErrValidation("no join path from %q to %q", baseName, targetName)
	}

	if pathCount[targetName] > 1 {
		return nil, domain.ErrValidation("ambiguous join path from %q to %q (%d shortest paths)", baseName, targetName, pathCount[targetName])
	}

	steps := []JoinStep{}
	cur := targetName
	for cur != baseName {
		e := parent[cur]
		steps = append(steps, JoinStep{
			RelationshipName: e.rel.Name,
			FromModel:        e.from,
			ToModel:          e.to,
			RelationshipType: e.rel.RelationshipType,
			JoinSQL:          e.rel.JoinSQL,
		})
		cur = e.from
	}

	for i, j := 0, len(steps)-1; i < j; i, j = i+1, j-1 {
		steps[i], steps[j] = steps[j], steps[i]
	}
	return steps, nil
}

func (s *Service) matchPreAggregation(ctx context.Context, semanticModelID string, req MetricQueryRequest) (*string, string) {
	preAggs, err := s.preAggs.ListByModel(ctx, semanticModelID)
	if err != nil {
		return nil, ""
	}

	wantMetrics := append([]string(nil), req.Metrics...)
	wantDims := append([]string(nil), req.Dimensions...)
	sort.Strings(wantMetrics)
	sort.Strings(wantDims)

	for _, p := range preAggs {
		mset := append([]string(nil), p.MetricSet...)
		dset := append([]string(nil), p.DimensionSet...)
		sort.Strings(mset)
		sort.Strings(dset)
		if strings.Join(mset, "|") == strings.Join(wantMetrics, "|") && strings.Join(dset, "|") == strings.Join(wantDims, "|") {
			name := p.Name
			return &name, p.TargetRelation
		}
	}

	return nil, ""
}
