package semantic

import (
	"context"
	"errors"
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

type dimensionBinding struct {
	Requested   string
	ResultAlias string
	IsBase      bool
	InnerAlias  string
}

type explainMetricQueryOptions struct {
	DisablePreAggregationID string
}

// ExplainMetricQuery compiles a semantic metric request into executable SQL and join metadata.
func (s *Service) ExplainMetricQuery(ctx context.Context, req MetricQueryRequest) (*MetricQueryPlan, error) {
	return s.explainMetricQuery(ctx, req, explainMetricQueryOptions{})
}

func (s *Service) explainMetricQuery(ctx context.Context, req MetricQueryRequest, opts explainMetricQueryOptions) (*MetricQueryPlan, error) {
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
	if req.Offset != nil && *req.Offset < 0 {
		return nil, domain.ErrValidation("offset must be >= 0")
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
		filteredExpr, err := applyMetricFilterSQL(metric, sqlExpr)
		if err != nil {
			return nil, err
		}
		resolvedMetricSQL[metric.Name] = filteredExpr
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
			joins = append(joins, fmt.Sprintf("LEFT JOIN %s AS %s ON %s", modelByName[step.ToModel].BaseModelRef, step.ToModel, step.JoinSQL))
			joinedNames[step.ToModel] = true
			joinSteps = append(joinSteps, step)
		}
	}

	selectedPreAgg, preAggRelation := s.matchPreAggregation(ctx, baseModel.ID, req, opts)
	fromRelation := baseModel.BaseModelRef
	if preAggRelation != "" {
		fromRelation = preAggRelation
		joins = nil
		joinSteps = nil
	}
	dimensions := bindDimensions(req.Dimensions, baseModel.Name)
	query, err := s.buildMetricQuery(ctx, buildMetricQueryArgs{
		Request:             req,
		BaseModel:           baseModel,
		SelectedMetrics:     selectedMetrics,
		ResolvedMetricSQL:   resolvedMetricSQL,
		ModelByName:         modelByName,
		JoinSteps:           joinSteps,
		Joins:               joins,
		FromRelation:        fromRelation,
		Dimensions:          dimensions,
		UsingPreAggregation: preAggRelation != "",
	})
	if err != nil {
		return nil, err
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
		BaseRelation:           fromRelation,
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
	if err := validateMetricFilterSQL(metric.FilterSQL); err != nil {
		return err
	}
	return nil
}

func validateMetricFilterSQL(filterSQL string) error {
	filter := strings.TrimSpace(filterSQL)
	if filter == "" {
		return nil
	}
	if strings.Contains(filter, ";") {
		return domain.ErrValidation("metric filter_sql must not contain semicolons")
	}
	if forbidden := matchForbiddenSQLPattern(filter); forbidden != "" {
		return domain.ErrValidation("metric filter_sql contains forbidden token: %s", forbidden)
	}
	if _, err := sqlrewrite.ClassifyStatement(fmt.Sprintf("SELECT 1 FROM semantic_expr_guard WHERE (%s)", filter)); err != nil {
		return domain.ErrValidation("metric filter_sql is invalid SQL: %v", err)
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

func applyMetricFilterSQL(metric domain.SemanticMetric, expr string) (string, error) {
	if strings.TrimSpace(metric.FilterSQL) == "" {
		return expr, nil
	}
	fn, arg, distinct, err := extractAggregateCall(expr)
	if err != nil {
		return "", domain.ErrValidation("metric %q filter_sql requires a simple aggregate expression: %v", metric.Name, err)
	}
	filter := strings.TrimSpace(metric.FilterSQL)
	switch metric.MetricType {
	case domain.MetricTypeSum:
		return fmt.Sprintf("SUM(CASE WHEN %s THEN %s END)", filter, arg), nil
	case domain.MetricTypeCount:
		countArg := arg
		if strings.TrimSpace(arg) == "*" {
			countArg = "1"
		}
		return fmt.Sprintf("COUNT(CASE WHEN %s THEN %s END)", filter, countArg), nil
	case domain.MetricTypeCountDistinct:
		if !distinct && !strings.EqualFold(fn, "COUNT") {
			return "", domain.ErrValidation("metric %q count_distinct filter_sql requires COUNT(DISTINCT ...)", metric.Name)
		}
		return fmt.Sprintf("COUNT(DISTINCT CASE WHEN %s THEN %s END)", filter, arg), nil
	case domain.MetricTypeAverage:
		return fmt.Sprintf("AVG(CASE WHEN %s THEN %s END)", filter, arg), nil
	case domain.MetricTypeMin:
		return fmt.Sprintf("MIN(CASE WHEN %s THEN %s END)", filter, arg), nil
	case domain.MetricTypeMax:
		return fmt.Sprintf("MAX(CASE WHEN %s THEN %s END)", filter, arg), nil
	default:
		return "", domain.ErrValidation("metric %q filter_sql is not supported for metric_type %q", metric.Name, metric.MetricType)
	}
}

func extractAggregateCall(expr string) (string, string, bool, error) {
	match := aggregateCallPattern.FindStringSubmatch(strings.TrimSpace(expr))
	if match == nil {
		return "", "", false, fmt.Errorf("expression %q is not a simple aggregate call", expr)
	}
	arg := strings.TrimSpace(match[2])
	distinct := false
	if strings.HasPrefix(strings.ToUpper(arg), "DISTINCT ") {
		distinct = true
		arg = strings.TrimSpace(arg[len("DISTINCT "):])
	}
	return strings.ToUpper(strings.TrimSpace(match[1])), arg, distinct, nil
}

var aggregateCallPattern = regexp.MustCompile(`(?is)^([a-z_][a-z0-9_]*)\s*\((.*)\)$`)

func modelPrefix(identifier string) string {
	parts := strings.SplitN(strings.TrimSpace(identifier), ".", 2)
	if len(parts) == 2 {
		return parts[0]
	}
	return ""
}

type buildMetricQueryArgs struct {
	Request             MetricQueryRequest
	BaseModel           *domain.SemanticModel
	SelectedMetrics     []domain.SemanticMetric
	ResolvedMetricSQL   map[string]string
	ModelByName         map[string]domain.SemanticModel
	JoinSteps           []JoinStep
	Joins               []string
	FromRelation        string
	Dimensions          []dimensionBinding
	UsingPreAggregation bool
}

func (s *Service) buildMetricQuery(ctx context.Context, args buildMetricQueryArgs) (string, error) {
	if args.UsingPreAggregation || !hasUnsafeJoin(args.JoinSteps) {
		return buildSafeMetricQuery(args)
	}
	uniqueKeys, err := s.baseModelUniqueKeys(ctx, args.BaseModel)
	if err != nil {
		return "", err
	}
	if len(uniqueKeys) == 0 {
		return "", domain.ErrValidation("semantic model %q requires a base model unique_key for unsafe dashboard joins", args.BaseModel.Name)
	}
	return buildUnsafeMetricQuery(args, uniqueKeys)
}

func hasUnsafeJoin(steps []JoinStep) bool {
	for _, step := range steps {
		switch step.RelationshipType {
		case domain.RelationshipTypeOneToMany, domain.RelationshipTypeManyMany:
			return true
		}
	}
	return false
}

func bindDimensions(dimensions []string, baseModelName string) []dimensionBinding {
	out := make([]dimensionBinding, 0, len(dimensions))
	for i, dim := range dimensions {
		prefix := modelPrefix(dim)
		isBase := prefix == "" || prefix == baseModelName
		out = append(out, dimensionBinding{
			Requested:   dim,
			ResultAlias: dim,
			IsBase:      isBase,
			InnerAlias:  fmt.Sprintf("__dim_%d", i),
		})
	}
	return out
}

func buildSafeMetricQuery(args buildMetricQueryArgs) (string, error) {
	selectParts := make([]string, 0, len(args.Dimensions)+len(args.SelectedMetrics)+1)
	groupByParts := make([]string, 0, len(args.Dimensions)+1)
	for _, dim := range args.Dimensions {
		selectParts = append(selectParts, fmt.Sprintf("%s AS %s", dim.Requested, quoteIdent(dim.ResultAlias)))
		groupByParts = append(groupByParts, dim.Requested)
	}
	if args.Request.TimeGrain != nil && strings.TrimSpace(*args.Request.TimeGrain) != "" {
		timeDim := strings.TrimSpace(args.BaseModel.DefaultTimeDimension)
		if timeDim == "" {
			return "", domain.ErrValidation("semantic model %q does not define a default_time_dimension", args.BaseModel.Name)
		}
		timeExpr := fmt.Sprintf("date_trunc('%s', %s) AS %s", strings.TrimSpace(*args.Request.TimeGrain), timeDim, quoteIdent(timeGrainAlias))
		selectParts = append(selectParts, timeExpr)
		groupByParts = append(groupByParts, fmt.Sprintf("date_trunc('%s', %s)", strings.TrimSpace(*args.Request.TimeGrain), timeDim))
	}
	for _, m := range args.SelectedMetrics {
		selectParts = append(selectParts, fmt.Sprintf("%s AS %s", args.ResolvedMetricSQL[m.Name], quoteIdent(m.Name)))
	}

	query := fmt.Sprintf("SELECT %s FROM %s AS %s", strings.Join(selectParts, ", "), args.FromRelation, args.BaseModel.Name)
	if len(args.Joins) > 0 {
		query += " " + strings.Join(args.Joins, " ")
	}
	if len(args.Request.Filters) > 0 {
		query += " WHERE " + strings.Join(args.Request.Filters, " AND ")
	}
	if len(groupByParts) > 0 {
		query += " GROUP BY " + strings.Join(groupByParts, ", ")
	}
	if len(args.Request.OrderBy) > 0 {
		query += " ORDER BY " + strings.Join(args.Request.OrderBy, ", ")
	}
	if args.Request.Limit != nil && *args.Request.Limit > 0 {
		query += fmt.Sprintf(" LIMIT %d", *args.Request.Limit)
	}
	if args.Request.Offset != nil && *args.Request.Offset > 0 {
		if args.Request.Limit == nil || *args.Request.Limit <= 0 {
			query += " LIMIT ALL"
		}
		query += fmt.Sprintf(" OFFSET %d", *args.Request.Offset)
	}
	return query, nil
}

func buildUnsafeMetricQuery(args buildMetricQueryArgs, uniqueKeys []string) (string, error) {
	baseAlias := args.BaseModel.Name
	outerAlias := "__metric_base"
	joinedModelNames := joinedModelNames(args.JoinSteps)
	innerFilters, outerFilters := splitExpressionsByJoinedModel(args.Request.Filters, joinedModelNames)

	neededBaseColumns := collectBaseColumns(baseAlias, joinSQLs(args.JoinSteps))
	neededBaseColumns = append(neededBaseColumns, collectBaseColumns(baseAlias, outerFilters)...)
	neededBaseColumns = append(neededBaseColumns, collectBaseColumns(baseAlias, args.Request.OrderBy)...)

	groupByParts := make([]string, 0, len(uniqueKeys))
	innerSelectParts := make([]string, 0, len(uniqueKeys)+len(neededBaseColumns)+len(args.Dimensions)+len(args.SelectedMetrics)*2+1)
	selectedBaseAliases := map[string]bool{}
	for _, key := range uniqueKeys {
		qualified := qualifyBaseKey(baseAlias, key)
		alias := keyAlias(key)
		groupByParts = append(groupByParts, qualified)
		innerSelectParts = append(innerSelectParts, fmt.Sprintf("%s AS %s", qualified, quoteIdent(alias)))
		selectedBaseAliases[alias] = true
	}
	for _, col := range dedupeStrings(neededBaseColumns) {
		alias := keyAlias(col)
		if selectedBaseAliases[alias] {
			continue
		}
		innerSelectParts = append(innerSelectParts, fmt.Sprintf("ANY_VALUE(%s.%s) AS %s", baseAlias, col, quoteIdent(alias)))
		selectedBaseAliases[alias] = true
	}
	for _, dim := range args.Dimensions {
		if !dim.IsBase {
			continue
		}
		innerSelectParts = append(innerSelectParts, fmt.Sprintf("ANY_VALUE(%s) AS %s", dim.Requested, quoteIdent(dim.InnerAlias)))
	}
	if args.Request.TimeGrain != nil && strings.TrimSpace(*args.Request.TimeGrain) != "" {
		timeDim := strings.TrimSpace(args.BaseModel.DefaultTimeDimension)
		if timeDim == "" {
			return "", domain.ErrValidation("semantic model %q does not define a default_time_dimension", args.BaseModel.Name)
		}
		innerSelectParts = append(innerSelectParts, fmt.Sprintf("ANY_VALUE(date_trunc('%s', %s)) AS %s", strings.TrimSpace(*args.Request.TimeGrain), timeDim, quoteIdent(timeGrainAlias)))
	}

	outerMetricSelects := make([]string, 0, len(args.SelectedMetrics))
	for _, metric := range args.SelectedMetrics {
		plan, err := buildUnsafeMetricPlan(metric, args.ResolvedMetricSQL[metric.Name])
		if err != nil {
			return "", err
		}
		innerSelectParts = append(innerSelectParts, plan.InnerSelects...)
		outerMetricSelects = append(outerMetricSelects, plan.OuterSelect)
	}

	innerQuery := fmt.Sprintf("SELECT %s FROM %s AS %s", strings.Join(innerSelectParts, ", "), args.FromRelation, baseAlias)
	if len(innerFilters) > 0 {
		innerQuery += " WHERE " + strings.Join(innerFilters, " AND ")
	}
	innerQuery += " GROUP BY " + strings.Join(groupByParts, ", ")

	outerSelectParts := make([]string, 0, len(args.Dimensions)+len(outerMetricSelects)+1)
	outerGroupParts := make([]string, 0, len(args.Dimensions)+1)
	for _, dim := range args.Dimensions {
		if dim.IsBase {
			outerExpr := fmt.Sprintf("%s.%s", outerAlias, quoteIdent(dim.InnerAlias))
			outerSelectParts = append(outerSelectParts, fmt.Sprintf("%s AS %s", outerExpr, quoteIdent(dim.ResultAlias)))
			outerGroupParts = append(outerGroupParts, outerExpr)
			continue
		}
		outerSelectParts = append(outerSelectParts, fmt.Sprintf("%s AS %s", rewriteBaseAlias(dim.Requested, baseAlias, outerAlias), quoteIdent(dim.ResultAlias)))
		outerGroupParts = append(outerGroupParts, rewriteBaseAlias(dim.Requested, baseAlias, outerAlias))
	}
	if args.Request.TimeGrain != nil && strings.TrimSpace(*args.Request.TimeGrain) != "" {
		timeExpr := fmt.Sprintf("%s.%s", outerAlias, quoteIdent(timeGrainAlias))
		outerSelectParts = append(outerSelectParts, fmt.Sprintf("%s AS %s", timeExpr, quoteIdent(timeGrainAlias)))
		outerGroupParts = append(outerGroupParts, timeExpr)
	}
	outerSelectParts = append(outerSelectParts, outerMetricSelects...)

	query := fmt.Sprintf("WITH %s AS (%s) SELECT %s FROM %s", outerAlias, innerQuery, strings.Join(outerSelectParts, ", "), outerAlias)
	if len(args.Joins) > 0 {
		rewrittenJoins := make([]string, 0, len(args.Joins))
		for _, join := range args.Joins {
			rewrittenJoins = append(rewrittenJoins, rewriteBaseAlias(join, baseAlias, outerAlias))
		}
		query += " " + strings.Join(rewrittenJoins, " ")
	}
	if len(outerFilters) > 0 {
		rewrittenFilters := make([]string, 0, len(outerFilters))
		for _, filter := range outerFilters {
			rewrittenFilters = append(rewrittenFilters, rewriteBaseAlias(filter, baseAlias, outerAlias))
		}
		query += " WHERE " + strings.Join(rewrittenFilters, " AND ")
	}
	if len(outerGroupParts) > 0 {
		query += " GROUP BY " + strings.Join(outerGroupParts, ", ")
	}
	if len(args.Request.OrderBy) > 0 {
		query += " ORDER BY " + strings.Join(rewriteOrderBy(args.Request.OrderBy, args.Dimensions, baseAlias, outerAlias), ", ")
	}
	if args.Request.Limit != nil && *args.Request.Limit > 0 {
		query += fmt.Sprintf(" LIMIT %d", *args.Request.Limit)
	}
	if args.Request.Offset != nil && *args.Request.Offset > 0 {
		if args.Request.Limit == nil || *args.Request.Limit <= 0 {
			query += " LIMIT ALL"
		}
		query += fmt.Sprintf(" OFFSET %d", *args.Request.Offset)
	}
	return query, nil
}

type unsafeMetricPlan struct {
	InnerSelects []string
	OuterSelect  string
}

func buildUnsafeMetricPlan(metric domain.SemanticMetric, expr string) (*unsafeMetricPlan, error) {
	plan, err := buildUnsafeScalarExprPlan(metric.Name, expr)
	if err != nil {
		return nil, domain.ErrValidation("metric %q cannot be planned across unsafe joins: %v", metric.Name, err)
	}
	return &unsafeMetricPlan{
		InnerSelects: plan.InnerSelects,
		OuterSelect:  fmt.Sprintf("%s AS %s", plan.OuterExpr, quoteIdent(metric.Name)),
	}, nil
}

type unsafeScalarExprPlan struct {
	InnerSelects []string
	OuterExpr    string
}

func buildUnsafeScalarExprPlan(aliasBase, expr string) (*unsafeScalarExprPlan, error) {
	trimmed := trimOuterParens(expr)
	if idx, op, ok := findTopLevelBinaryOp(trimmed, "+-"); ok {
		return combineUnsafeScalarPlans(aliasBase, trimmed, idx, op)
	}
	if idx, op, ok := findTopLevelBinaryOp(trimmed, "*/"); ok {
		return combineUnsafeScalarPlans(aliasBase, trimmed, idx, op)
	}
	if fnName, args, ok := parseTopLevelFunctionCall(trimmed); ok {
		if isAggregateFunctionName(fnName) {
			goto aggregate
		}
		plannedArgs := make([]string, 0, len(args))
		innerSelects := make([]string, 0, len(args))
		for i, arg := range args {
			if isLiteralScalarArg(arg) {
				plannedArgs = append(plannedArgs, strings.TrimSpace(arg))
				continue
			}
			argPlan, err := buildUnsafeScalarExprPlan(fmt.Sprintf("%s_arg_%d", aliasBase, i), arg)
			if err != nil {
				return nil, err
			}
			innerSelects = append(innerSelects, argPlan.InnerSelects...)
			plannedArgs = append(plannedArgs, argPlan.OuterExpr)
		}
		return &unsafeScalarExprPlan{
			InnerSelects: innerSelects,
			OuterExpr:    fmt.Sprintf("%s(%s)", fnName, strings.Join(plannedArgs, ", ")),
		}, nil
	}
aggregate:
	plan, err := buildUnsafeAggregateExprPlan(aliasBase, trimmed)
	if err != nil {
		return nil, err
	}
	return &unsafeScalarExprPlan{
		InnerSelects: plan.InnerSelects,
		OuterExpr:    plan.OuterExpr,
	}, nil
}

func combineUnsafeScalarPlans(aliasBase, expr string, idx int, op rune) (*unsafeScalarExprPlan, error) {
	leftExpr := strings.TrimSpace(expr[:idx])
	rightExpr := strings.TrimSpace(expr[idx+1:])
	if leftExpr == "" || rightExpr == "" {
		return nil, fmt.Errorf("invalid arithmetic expression %q", expr)
	}
	leftPlan, err := buildUnsafeScalarExprPlan(aliasBase+"_lhs", leftExpr)
	if err != nil {
		return nil, err
	}
	rightPlan, err := buildUnsafeScalarExprPlan(aliasBase+"_rhs", rightExpr)
	if err != nil {
		return nil, err
	}
	outerExpr := fmt.Sprintf("(%s) %c (%s)", leftPlan.OuterExpr, op, rightPlan.OuterExpr)
	if op == '/' {
		outerExpr = fmt.Sprintf("(%s) / NULLIF((%s), 0)", leftPlan.OuterExpr, rightPlan.OuterExpr)
	}
	return &unsafeScalarExprPlan{
		InnerSelects: append(leftPlan.InnerSelects, rightPlan.InnerSelects...),
		OuterExpr:    outerExpr,
	}, nil
}

type unsafeAggregateExprPlan struct {
	InnerSelects []string
	OuterExpr    string
}

func buildUnsafeAggregateExprPlan(aliasBase, expr string) (*unsafeAggregateExprPlan, error) {
	fn, arg, distinct, err := extractAggregateCall(trimOuterParens(expr))
	if err != nil {
		return nil, err
	}
	alias := "__metric_" + aliasBase
	switch {
	case strings.EqualFold(fn, "SUM"):
		return &unsafeAggregateExprPlan{
			InnerSelects: []string{fmt.Sprintf("%s AS %s", trimOuterParens(expr), quoteIdent(alias))},
			OuterExpr:    fmt.Sprintf("SUM(%s)", quoteIdent(alias)),
		}, nil
	case strings.EqualFold(fn, "COUNT") && distinct:
		distinctAlias := alias + "_distinct"
		return &unsafeAggregateExprPlan{
			InnerSelects: []string{fmt.Sprintf("ANY_VALUE(%s) AS %s", arg, quoteIdent(distinctAlias))},
			OuterExpr:    fmt.Sprintf("COUNT(DISTINCT %s)", quoteIdent(distinctAlias)),
		}, nil
	case strings.EqualFold(fn, "COUNT"):
		return &unsafeAggregateExprPlan{
			InnerSelects: []string{fmt.Sprintf("%s AS %s", trimOuterParens(expr), quoteIdent(alias))},
			OuterExpr:    fmt.Sprintf("SUM(%s)", quoteIdent(alias)),
		}, nil
	case strings.EqualFold(fn, "MIN"):
		return &unsafeAggregateExprPlan{
			InnerSelects: []string{fmt.Sprintf("%s AS %s", trimOuterParens(expr), quoteIdent(alias))},
			OuterExpr:    fmt.Sprintf("MIN(%s)", quoteIdent(alias)),
		}, nil
	case strings.EqualFold(fn, "MAX"):
		return &unsafeAggregateExprPlan{
			InnerSelects: []string{fmt.Sprintf("%s AS %s", trimOuterParens(expr), quoteIdent(alias))},
			OuterExpr:    fmt.Sprintf("MAX(%s)", quoteIdent(alias)),
		}, nil
	case strings.EqualFold(fn, "AVG"):
		sumAlias := alias + "_sum"
		countAlias := alias + "_count"
		return &unsafeAggregateExprPlan{
			InnerSelects: []string{
				fmt.Sprintf("SUM(%s) AS %s", arg, quoteIdent(sumAlias)),
				fmt.Sprintf("COUNT(%s) AS %s", arg, quoteIdent(countAlias)),
			},
			OuterExpr: fmt.Sprintf("SUM(%s) / NULLIF(SUM(%s), 0)", quoteIdent(sumAlias), quoteIdent(countAlias)),
		}, nil
	default:
		return nil, fmt.Errorf("unsupported aggregate expression %q", expr)
	}
}

func trimOuterParens(expr string) string {
	trimmed := strings.TrimSpace(expr)
	for len(trimmed) >= 2 && trimmed[0] == '(' && trimmed[len(trimmed)-1] == ')' {
		depth := 0
		wrapped := true
		for i, r := range trimmed {
			switch r {
			case '(':
				depth++
			case ')':
				depth--
				if depth == 0 && i < len(trimmed)-1 {
					wrapped = false
				}
			}
			if depth < 0 {
				wrapped = false
				break
			}
		}
		if !wrapped || depth != 0 {
			break
		}
		trimmed = strings.TrimSpace(trimmed[1 : len(trimmed)-1])
	}
	return trimmed
}

func findTopLevelBinaryOp(expr string, operators string) (int, rune, bool) {
	depth := 0
	for i := len(expr) - 1; i >= 0; i-- {
		switch expr[i] {
		case ')':
			depth++
		case '(':
			if depth > 0 {
				depth--
			}
		default:
			if depth != 0 || !strings.ContainsRune(operators, rune(expr[i])) {
				continue
			}
			prev := previousNonSpace(expr, i)
			if prev < 0 || strings.ContainsRune("+-*/(", rune(expr[prev])) {
				continue
			}
			return i, rune(expr[i]), true
		}
	}
	return 0, 0, false
}

func previousNonSpace(expr string, idx int) int {
	for i := idx - 1; i >= 0; i-- {
		if expr[i] != ' ' && expr[i] != '\t' && expr[i] != '\n' && expr[i] != '\r' {
			return i
		}
	}
	return -1
}

func parseTopLevelFunctionCall(expr string) (string, []string, bool) {
	trimmed := strings.TrimSpace(expr)
	openIdx := strings.Index(trimmed, "(")
	if openIdx <= 0 || trimmed[len(trimmed)-1] != ')' {
		return "", nil, false
	}
	name := strings.TrimSpace(trimmed[:openIdx])
	if name == "" || strings.ContainsAny(name, " \t\n\r") {
		return "", nil, false
	}
	argsBody := trimmed[openIdx+1 : len(trimmed)-1]
	args, ok := splitTopLevelArgs(argsBody)
	if !ok {
		return "", nil, false
	}
	return name, args, true
}

func isAggregateFunctionName(name string) bool {
	switch strings.ToUpper(strings.TrimSpace(name)) {
	case "SUM", "COUNT", "AVG", "MIN", "MAX":
		return true
	default:
		return false
	}
}

func splitTopLevelArgs(expr string) ([]string, bool) {
	depth := 0
	start := 0
	args := []string{}
	for i, r := range expr {
		switch r {
		case '(':
			depth++
		case ')':
			depth--
			if depth < 0 {
				return nil, false
			}
		case ',':
			if depth == 0 {
				args = append(args, strings.TrimSpace(expr[start:i]))
				start = i + 1
			}
		}
	}
	if depth != 0 {
		return nil, false
	}
	last := strings.TrimSpace(expr[start:])
	if last == "" && len(args) == 0 {
		return []string{}, true
	}
	if last == "" {
		return nil, false
	}
	args = append(args, last)
	return args, true
}

func isLiteralScalarArg(expr string) bool {
	trimmed := strings.TrimSpace(expr)
	if trimmed == "" {
		return false
	}
	if regexp.MustCompile(`(?is)^null$`).MatchString(trimmed) {
		return true
	}
	if regexp.MustCompile(`(?is)^-?\d+(\.\d+)?$`).MatchString(trimmed) {
		return true
	}
	if len(trimmed) >= 2 && trimmed[0] == '\'' && trimmed[len(trimmed)-1] == '\'' {
		return true
	}
	return false
}

func (s *Service) baseModelUniqueKeys(ctx context.Context, baseModel *domain.SemanticModel) ([]string, error) {
	if s.modelRepo == nil {
		return nil, nil
	}
	projectName, modelName, ok := parseQualifiedModelRef(baseModel.BaseModelRef)
	if !ok {
		return nil, nil
	}
	model, err := s.modelRepo.GetByName(ctx, projectName, modelName)
	if err != nil {
		var notFound *domain.NotFoundError
		if errors.As(err, &notFound) {
			return nil, nil
		}
		return nil, fmt.Errorf("lookup base model %q: %w", baseModel.BaseModelRef, err)
	}
	return model.Config.UniqueKey, nil
}

func parseQualifiedModelRef(ref string) (string, string, bool) {
	parts := strings.SplitN(strings.TrimSpace(ref), ".", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return "", "", false
	}
	return parts[0], parts[1], true
}

func joinedModelNames(steps []JoinStep) []string {
	out := make([]string, 0, len(steps))
	for _, step := range steps {
		out = append(out, step.ToModel)
	}
	return dedupeStrings(out)
}

func splitExpressionsByJoinedModel(items []string, joinedModels []string) ([]string, []string) {
	inner := make([]string, 0, len(items))
	outer := make([]string, 0, len(items))
	for _, item := range items {
		if referencesAnyModel(item, joinedModels) {
			outer = append(outer, item)
			continue
		}
		inner = append(inner, item)
	}
	return inner, outer
}

func referencesAnyModel(expr string, modelNames []string) bool {
	for _, modelName := range modelNames {
		if strings.Contains(expr, modelName+".") {
			return true
		}
	}
	return false
}

func collectBaseColumns(baseAlias string, expressions []string) []string {
	colSet := make(map[string]struct{})
	pattern := regexp.MustCompile(`\b` + regexp.QuoteMeta(baseAlias) + `\.([a-zA-Z_][a-zA-Z0-9_]*)\b`)
	for _, expr := range expressions {
		for _, match := range pattern.FindAllStringSubmatch(expr, -1) {
			colSet[match[1]] = struct{}{}
		}
	}
	out := make([]string, 0, len(colSet))
	for col := range colSet {
		out = append(out, col)
	}
	sort.Strings(out)
	return out
}

func joinSQLs(steps []JoinStep) []string {
	out := make([]string, 0, len(steps))
	for _, step := range steps {
		out = append(out, step.JoinSQL)
	}
	return out
}

func qualifyBaseKey(baseAlias, key string) string {
	trimmed := strings.TrimSpace(key)
	if strings.Contains(trimmed, ".") {
		return trimmed
	}
	return baseAlias + "." + trimmed
}

func keyAlias(key string) string {
	trimmed := strings.TrimSpace(key)
	if idx := strings.LastIndex(trimmed, "."); idx >= 0 {
		trimmed = trimmed[idx+1:]
	}
	return strings.ReplaceAll(trimmed, `"`, "")
}

func rewriteBaseAlias(expr, oldAlias, newAlias string) string {
	return strings.ReplaceAll(expr, oldAlias+".", newAlias+".")
}

func rewriteOrderBy(orderBy []string, dimensions []dimensionBinding, oldAlias, newAlias string) []string {
	out := make([]string, 0, len(orderBy))
	for _, item := range orderBy {
		rewritten := item
		for _, dim := range dimensions {
			trimmed := strings.TrimSpace(item)
			switch {
			case trimmed == dim.Requested:
				rewritten = quoteIdent(dim.ResultAlias)
			case strings.HasPrefix(trimmed, dim.Requested+" "):
				rewritten = quoteIdent(dim.ResultAlias) + trimmed[len(dim.Requested):]
			}
		}
		out = append(out, rewriteBaseAlias(rewritten, oldAlias, newAlias))
	}
	return out
}

func quoteIdent(value string) string {
	return `"` + strings.ReplaceAll(value, `"`, `""`) + `"`
}

func dedupeStrings(items []string) []string {
	seen := make(map[string]struct{}, len(items))
	out := make([]string, 0, len(items))
	for _, item := range items {
		if _, ok := seen[item]; ok {
			continue
		}
		seen[item] = struct{}{}
		out = append(out, item)
	}
	return out
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

func (s *Service) matchPreAggregation(ctx context.Context, semanticModelID string, req MetricQueryRequest, opts explainMetricQueryOptions) (*string, string) {
	preAggs, err := s.preAggs.ListByModel(ctx, semanticModelID)
	if err != nil {
		return nil, ""
	}

	wantMetrics := append([]string(nil), req.Metrics...)
	wantDims := append([]string(nil), req.Dimensions...)
	sort.Strings(wantMetrics)
	sort.Strings(wantDims)

	for _, p := range preAggs {
		if opts.DisablePreAggregationID != "" && p.ID == opts.DisablePreAggregationID {
			continue
		}
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

var safeTargetRelationPattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*){0,2}$`)

func normalizeTargetRelation(targetRelation string) (string, error) {
	targetRelation = strings.TrimSpace(targetRelation)
	if targetRelation == "" {
		return "", domain.ErrValidation("target_relation is required")
	}
	if !safeTargetRelationPattern.MatchString(targetRelation) {
		return "", domain.ErrValidation("target_relation must be a simple dotted identifier")
	}

	parts := strings.Split(targetRelation, ".")
	quoted := make([]string, 0, len(parts))
	for _, part := range parts {
		quoted = append(quoted, quoteIdent(part))
	}
	return strings.Join(quoted, "."), nil
}
