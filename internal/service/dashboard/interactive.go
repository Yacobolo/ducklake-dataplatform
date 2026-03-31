package dashboard

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"duck-demo/internal/domain"
)

const (
	dashboardTimeGrainFilterSeparator = "@"
	dashboardTimeGrainResultAlias     = "__time_grain"
	dashboardInteractionDisabledCopy  = "Not interactive in this dashboard."
)

// InteractiveFilter captures one active dashboard filter dimension with one or more selected values.
type InteractiveFilter struct {
	WidgetID  string   `json:"widget_id,omitempty"`
	Dimension string   `json:"dimension"`
	Values    []string `json:"values"`
}

// ResolvedWidgetInteraction describes whether a widget participates in dashboard cross-filtering.
type ResolvedWidgetInteraction struct {
	Participates   bool                             `json:"participates"`
	CanInitiate    bool                             `json:"can_initiate"`
	DisabledReason string                           `json:"disabled_reason,omitempty"`
	Bindings       []ResolvedWidgetInteractionField `json:"bindings,omitempty"`
	ActiveFilters  map[string][]string              `json:"active_filters,omitempty"`
}

// ResolvedWidgetInteractionField maps a clickable chart encoding to a dashboard filter dimension key.
type ResolvedWidgetInteractionField struct {
	Encoding  string `json:"encoding"`
	Dimension string `json:"dimension"`
}

type interactiveFilterSpec struct {
	Key       string
	Dimension string
	TimeGrain string
}

type dashboardInteractionContext struct {
	Interactive     bool
	BoundModel      *domain.SemanticModel
	FilterSpecs     map[string]interactiveFilterSpec
	ActiveFilters   []InteractiveFilter
	ActiveFilterMap map[string][]string
}

// ResolveWidgetsForDashboard resolves widgets in the context of a dashboard-level semantic binding and active filters.
func (s *Service) ResolveWidgetsForDashboard(ctx context.Context, principal string, dashboard *domain.Dashboard, widgets []domain.DashboardWidget, filters []InteractiveFilter) ([]ResolvedWidget, error) {
	interactionCtx, err := s.buildDashboardInteractionContext(ctx, dashboard, widgets, filters)
	if err != nil {
		return nil, err
	}

	out := make([]ResolvedWidget, 0, len(widgets))
	for _, widget := range widgets {
		resolved, err := s.resolveWidgetWithContext(ctx, principal, widget, interactionCtx)
		if err != nil {
			return nil, fmt.Errorf("resolve widget %q: %w", widget.Name, err)
		}
		out = append(out, *resolved)
	}
	return out, nil
}

func (s *Service) buildDashboardInteractionContext(ctx context.Context, dashboard *domain.Dashboard, widgets []domain.DashboardWidget, filters []InteractiveFilter) (*dashboardInteractionContext, error) {
	if dashboard == nil || strings.TrimSpace(dashboard.SemanticProjectName) == "" || strings.TrimSpace(dashboard.SemanticModelName) == "" || s.semantic == nil {
		return nil, nil
	}

	boundModel, err := s.semantic.GetSemanticModel(ctx, dashboard.SemanticProjectName, dashboard.SemanticModelName)
	if err != nil {
		return nil, fmt.Errorf("get dashboard semantic model: %w", err)
	}

	filterSpecs := make(map[string]interactiveFilterSpec)
	for _, widget := range widgets {
		if !widgetMatchesDashboardBinding(widget, dashboard) {
			continue
		}
		for _, dim := range widget.Source.SemanticQuery.Dimensions {
			dim = strings.TrimSpace(dim)
			if dim == "" {
				continue
			}
			filterSpecs[dim] = interactiveFilterSpec{
				Key:       dim,
				Dimension: dim,
			}
		}
		if widget.Source.SemanticQuery.TimeGrain != nil && strings.TrimSpace(boundModel.DefaultTimeDimension) != "" {
			key := dashboardTimeFilterKey(boundModel.DefaultTimeDimension, *widget.Source.SemanticQuery.TimeGrain)
			filterSpecs[key] = interactiveFilterSpec{
				Key:       key,
				Dimension: strings.TrimSpace(boundModel.DefaultTimeDimension),
				TimeGrain: strings.TrimSpace(*widget.Source.SemanticQuery.TimeGrain),
			}
		}
	}

	activeFilters := sanitizeDashboardInteractiveFilters(filters, filterSpecs)
	return &dashboardInteractionContext{
		Interactive:     true,
		BoundModel:      boundModel,
		FilterSpecs:     filterSpecs,
		ActiveFilters:   activeFilters,
		ActiveFilterMap: interactiveFilterMap(activeFilters),
	}, nil
}

func buildResolvedWidgetInteraction(widget domain.DashboardWidget, interactionCtx *dashboardInteractionContext) *ResolvedWidgetInteraction {
	if interactionCtx == nil || !interactionCtx.Interactive {
		return nil
	}

	if !widgetParticipatesInDashboardInteraction(widget, interactionCtx) {
		return &ResolvedWidgetInteraction{
			Participates:   false,
			CanInitiate:    false,
			DisabledReason: dashboardInteractionDisabledCopy,
		}
	}

	bindings := widgetInteractionBindings(widget, interactionCtx.BoundModel)
	canInitiate := widgetCanInitiateDashboardFilters(widget, bindings)

	return &ResolvedWidgetInteraction{
		Participates:  true,
		CanInitiate:   canInitiate,
		Bindings:      bindings,
		ActiveFilters: interactionCtx.ActiveFilterMap,
	}
}

func widgetParticipatesInDashboardInteraction(widget domain.DashboardWidget, interactionCtx *dashboardInteractionContext) bool {
	if interactionCtx == nil || !interactionCtx.Interactive {
		return false
	}
	if widget.Source.Kind != domain.DashboardWidgetSourceSemanticQuery || widget.Source.SemanticQuery == nil {
		return false
	}
	return widget.Source.SemanticQuery.ProjectName == interactionCtx.BoundModel.ProjectName &&
		widget.Source.SemanticQuery.SemanticModelName == interactionCtx.BoundModel.Name
}

func widgetMatchesDashboardBinding(widget domain.DashboardWidget, dashboard *domain.Dashboard) bool {
	if dashboard == nil || widget.Source.Kind != domain.DashboardWidgetSourceSemanticQuery || widget.Source.SemanticQuery == nil {
		return false
	}
	return strings.TrimSpace(widget.Source.SemanticQuery.ProjectName) == strings.TrimSpace(dashboard.SemanticProjectName) &&
		strings.TrimSpace(widget.Source.SemanticQuery.SemanticModelName) == strings.TrimSpace(dashboard.SemanticModelName)
}

func widgetInteractionBindings(widget domain.DashboardWidget, boundModel *domain.SemanticModel) []ResolvedWidgetInteractionField {
	if widget.Source.Kind != domain.DashboardWidgetSourceSemanticQuery || widget.Source.SemanticQuery == nil || widget.VisualSpec == nil {
		return nil
	}
	if widget.VisualSpec.Kind != domain.VisualOutputChart || widget.VisualSpec.ChartType == nil {
		return nil
	}

	addBinding := func(bindings []ResolvedWidgetInteractionField, encoding, field string) []ResolvedWidgetInteractionField {
		dimension := semanticFieldToDashboardFilterDimension(widget.Source.SemanticQuery, boundModel, field)
		if dimension == "" {
			return bindings
		}
		return append(bindings, ResolvedWidgetInteractionField{
			Encoding:  encoding,
			Dimension: dimension,
		})
	}

	bindings := make([]ResolvedWidgetInteractionField, 0, 2)
	switch *widget.VisualSpec.ChartType {
	case domain.VisualChartPie, domain.VisualChartDoughnut:
		if widget.VisualSpec.Encodings.Label != nil {
			bindings = addBinding(bindings, "label", widget.VisualSpec.Encodings.Label.Field)
		}
	case domain.VisualChartScatter:
		return nil
	default:
		if widget.VisualSpec.Encodings.X != nil {
			bindings = addBinding(bindings, "x", widget.VisualSpec.Encodings.X.Field)
		}
		if widget.VisualSpec.Encodings.Series != nil {
			bindings = addBinding(bindings, "series", widget.VisualSpec.Encodings.Series.Field)
		}
	}
	return bindings
}

func widgetCanInitiateDashboardFilters(widget domain.DashboardWidget, bindings []ResolvedWidgetInteractionField) bool {
	if widget.VisualSpec == nil || widget.VisualSpec.Kind != domain.VisualOutputChart || widget.VisualSpec.ChartType == nil {
		return false
	}
	if *widget.VisualSpec.ChartType == domain.VisualChartScatter {
		return false
	}
	return len(bindings) > 0
}

func widgetQueryFilters(widget domain.DashboardWidget, interactionCtx *dashboardInteractionContext) []InteractiveFilter {
	if interactionCtx == nil || !interactionCtx.Interactive {
		return nil
	}
	if len(interactionCtx.ActiveFilters) == 0 {
		return nil
	}
	if !widgetParticipatesInDashboardInteraction(widget, interactionCtx) {
		return interactionCtx.ActiveFilters
	}

	filtered := make([]InteractiveFilter, 0, len(interactionCtx.ActiveFilters))
	for _, filter := range interactionCtx.ActiveFilters {
		if strings.TrimSpace(filter.WidgetID) != "" && strings.TrimSpace(filter.WidgetID) == strings.TrimSpace(widget.ID) {
			continue
		}
		filtered = append(filtered, filter)
	}
	return filtered
}

func semanticFieldToDashboardFilterDimension(source *domain.DashboardSemanticQuerySource, boundModel *domain.SemanticModel, field string) string {
	field = strings.TrimSpace(field)
	if source == nil || field == "" {
		return ""
	}
	for _, dimension := range source.Dimensions {
		if strings.TrimSpace(dimension) == field {
			return field
		}
	}
	if field == dashboardTimeGrainResultAlias && source.TimeGrain != nil && boundModel != nil && strings.TrimSpace(boundModel.DefaultTimeDimension) != "" {
		return dashboardTimeFilterKey(boundModel.DefaultTimeDimension, *source.TimeGrain)
	}
	return ""
}

func sanitizeDashboardInteractiveFilters(filters []InteractiveFilter, specs map[string]interactiveFilterSpec) []InteractiveFilter {
	if len(filters) == 0 || len(specs) == 0 {
		return nil
	}

	out := make([]InteractiveFilter, 0, len(filters))
	order := make([]string, 0, len(filters))
	grouped := make(map[string]InteractiveFilter, len(filters))
	seen := make(map[string]map[string]struct{}, len(filters))
	for _, filter := range filters {
		widgetID := strings.TrimSpace(filter.WidgetID)
		dimension := strings.TrimSpace(filter.Dimension)
		if dimension == "" {
			continue
		}
		if _, ok := specs[dimension]; !ok {
			continue
		}
		groupKey := widgetID + "\x00" + dimension
		if _, ok := grouped[groupKey]; !ok {
			grouped[groupKey] = InteractiveFilter{
				WidgetID:  widgetID,
				Dimension: dimension,
			}
			order = append(order, groupKey)
		}
		if _, ok := seen[groupKey]; !ok {
			seen[groupKey] = make(map[string]struct{})
		}
		for _, value := range filter.Values {
			value = strings.TrimSpace(value)
			if value == "" {
				continue
			}
			if _, ok := seen[groupKey][value]; ok {
				continue
			}
			seen[groupKey][value] = struct{}{}
			groupedFilter := grouped[groupKey]
			groupedFilter.Values = append(groupedFilter.Values, value)
			grouped[groupKey] = groupedFilter
		}
	}

	for _, groupKey := range order {
		groupedFilter := grouped[groupKey]
		if len(groupedFilter.Values) == 0 {
			continue
		}
		out = append(out, groupedFilter)
	}
	return out
}

func interactiveFilterMap(filters []InteractiveFilter) map[string][]string {
	if len(filters) == 0 {
		return nil
	}
	out := make(map[string][]string, len(filters))
	seen := make(map[string]map[string]struct{}, len(filters))
	for _, filter := range filters {
		dimension := strings.TrimSpace(filter.Dimension)
		if dimension == "" {
			continue
		}
		if _, ok := seen[dimension]; !ok {
			seen[dimension] = make(map[string]struct{})
		}
		for _, value := range filter.Values {
			value = strings.TrimSpace(value)
			if value == "" {
				continue
			}
			if _, ok := seen[dimension][value]; ok {
				continue
			}
			seen[dimension][value] = struct{}{}
			out[dimension] = append(out[dimension], value)
		}
	}
	return out
}

func buildDashboardFilterClauses(filters []InteractiveFilter, specs map[string]interactiveFilterSpec) []string {
	if len(filters) == 0 {
		return nil
	}

	grouped := make(map[string][]string, len(filters))
	seen := make(map[string]map[string]struct{}, len(filters))
	for _, filter := range filters {
		dimension := strings.TrimSpace(filter.Dimension)
		_, ok := specs[dimension]
		if !ok {
			continue
		}
		if _, ok := seen[dimension]; !ok {
			seen[dimension] = make(map[string]struct{})
		}
		for _, value := range filter.Values {
			value = strings.TrimSpace(value)
			if value == "" {
				continue
			}
			if _, ok := seen[dimension][value]; ok {
				continue
			}
			seen[dimension][value] = struct{}{}
			grouped[dimension] = append(grouped[dimension], value)
		}
	}

	clauses := make([]string, 0, len(grouped))
	dimensions := make([]string, 0, len(grouped))
	for dimension := range grouped {
		dimensions = append(dimensions, dimension)
	}
	sort.Strings(dimensions)
	for _, dimension := range dimensions {
		spec := specs[dimension]
		expr := spec.Dimension
		if spec.TimeGrain != "" {
			expr = fmt.Sprintf("date_trunc('%s', %s)", escapeSQLString(spec.TimeGrain), spec.Dimension)
		}
		valueClauses := make([]string, 0, len(grouped[dimension]))
		for _, value := range grouped[dimension] {
			valueClauses = append(valueClauses, fmt.Sprintf("%s = %s", expr, dashboardFilterSQLLiteral(value, spec.TimeGrain != "")))
		}
		if len(valueClauses) == 1 {
			clauses = append(clauses, valueClauses[0])
		} else if len(valueClauses) > 1 {
			clauses = append(clauses, "("+strings.Join(valueClauses, " OR ")+")")
		}
	}
	return clauses
}

func dashboardFilterSQLLiteral(value string, isTimeBucket bool) string {
	value = strings.TrimSpace(value)
	if isTimeBucket {
		return fmt.Sprintf("CAST('%s' AS TIMESTAMP)", escapeSQLString(value))
	}
	if parsed, err := strconv.ParseBool(value); err == nil {
		if parsed {
			return "TRUE"
		}
		return "FALSE"
	}
	if _, err := strconv.ParseInt(value, 10, 64); err == nil {
		return value
	}
	if _, err := strconv.ParseFloat(value, 64); err == nil {
		return value
	}
	return fmt.Sprintf("'%s'", escapeSQLString(value))
}

func escapeSQLString(value string) string {
	return strings.ReplaceAll(value, "'", "''")
}

func dashboardTimeFilterKey(dimension, grain string) string {
	dimension = strings.TrimSpace(dimension)
	grain = strings.TrimSpace(grain)
	if dimension == "" || grain == "" {
		return ""
	}
	return dimension + dashboardTimeGrainFilterSeparator + grain
}
