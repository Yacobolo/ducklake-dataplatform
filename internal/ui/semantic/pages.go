package semantic

import (
	"strconv"

	"duck-demo/internal/domain"
	semsvc "duck-demo/internal/service/semantic"
	"duck-demo/internal/ui/core"

	. "maragu.dev/gomponents"
	. "maragu.dev/gomponents/html"
)

type semanticModelRowData struct {
	Name      string
	URL       string
	BaseModel string
	Owner     string
	UpdatedAt string
}

type semanticMetricRowData struct {
	Name       string
	Type       string
	Expression string
	Status     string
	EditURL    string
	DeleteURL  string
}

type semanticPreAggRowData struct {
	Name      string
	Grain     string
	Target    string
	EditURL   string
	DeleteURL string
}

type semanticModelDetailPageData struct {
	Principal         domain.ContextPrincipal
	ProjectName       string
	ModelName         string
	BaseModelRef      string
	DefaultTimeDim    string
	Description       string
	EditURL           string
	DeleteURL         string
	MetricsCreateURL  string
	PreAggCreateURL   string
	QueryExplainURL   string
	QueryRunURL       string
	Metrics           []semanticMetricRowData
	PreAggregations   []semanticPreAggRowData
	CSRFFieldProvider func() Node
}

type semanticOptionData struct {
	Value string
	Label string
}

type semanticRelationshipRowData struct {
	Name      string
	FromModel string
	ToModel   string
	Type      string
	JoinSQL   string
	EditURL   string
	DeleteURL string
}

type semanticRelationshipsPageData struct {
	Principal         domain.ContextPrincipal
	Rows              []semanticRelationshipRowData
	ModelOptions      []semanticOptionData
	Page              domain.PageRequest
	Total             int64
	CSRFFieldProvider func() Node
}

type semanticQueryResultPageData struct {
	Principal         domain.ContextPrincipal
	Request           semsvc.MetricQueryRequest
	Plan              *semsvc.MetricQueryPlan
	Result            *semsvc.MetricQueryResult
	CSRFFieldProvider func() Node
}

func semanticModelsListPage(principal domain.ContextPrincipal, rows []semanticModelRowData, page domain.PageRequest, total int64) Node {
	table := Node(P(Class("text-xs text-[var(--fgColor-muted)]"), Text("No semantic models defined.")))
	if len(rows) > 0 {
		tableRows := make([]Node, 0, len(rows))
		for i := range rows {
			row := rows[i]
			tableRows = append(tableRows, Tr(
				Td(A(Href(row.URL), Class("font-medium text-[var(--fgColor-accent)]"), Text(row.Name))),
				Td(Text(row.BaseModel)),
				Td(Text(row.Owner)),
				Td(Text(row.UpdatedAt)),
			))
		}
		table = Div(Class("overflow-x-auto"), Table(Class("min-w-full text-left text-sm"),
			THead(Tr(Th(Text("Model")), Th(Text("Base model")), Th(Text("Owner")), Th(Text("Updated")))),
			TBody(Group(tableRows)),
		))
	}
	return core.AppPage("Semantic Models", "semantic", principal,
		semanticSectionNav("models"),
		core.PageHeader("Build", "Semantic models", "Use the semantic workspace for the consumer-facing model layer. Relationship paths stay nearby, but model management remains the default landing surface.", core.PrimaryLink("/ui/semantic/models/new", "", Text("New semantic model"))),
		core.SectionSurface(
			core.SectionHeader("Model inventory", "Review the semantic layer before drilling into metrics or pre-aggregations."),
			table,
			P(Class("mt-4 text-sm text-[var(--fgColor-muted)]"), Text("Showing up to "+strconv.Itoa(page.MaxResults)+" semantic models. Total: "+strconv.FormatInt(total, 10))),
		),
	)
}

func semanticModelsNewPage(principal domain.ContextPrincipal, csrfFieldProvider func() Node) Node {
	return semanticFormPage(principal, "New Semantic Model", "/ui/semantic/models", csrfFieldProvider,
		Label(Text("Project")),
		core.InputControl("", Name("project_name"), Required()),
		Label(Text("Name")),
		core.InputControl("", Name("name"), Required()),
		Label(Text("Description")),
		core.TextareaControl("min-h-24", Name("description")),
		Label(Text("Base model reference")),
		core.InputControl("", Name("base_model_ref"), Required()),
		Label(Text("Default time dimension")),
		core.InputControl("", Name("default_time_dimension")),
		Label(Text("Tags (comma separated)")),
		core.InputControl("", Name("tags")),
	)
}

func semanticModelDetailPage(d semanticModelDetailPageData) Node {
	metricRows := Node(P(Class("text-xs text-[var(--fgColor-muted)]"), Text("No metrics created yet.")))
	if len(d.Metrics) > 0 {
		rows := make([]Node, 0, len(d.Metrics))
		for i := range d.Metrics {
			metric := d.Metrics[i]
			rows = append(rows, Tr(
				Td(Text(metric.Name)),
				Td(Text(metric.Type)),
				Td(Text(metric.Expression)),
				Td(Text(metric.Status)),
				Td(Class("text-right"), Div(Class("mt-0 flex flex-wrap items-center justify-end gap-2 [&_form]:m-0 [&_form]:inline-flex"),
					core.SecondaryLink(metric.EditURL, "small", Text("Edit")),
					Form(Method("post"), Action(metric.DeleteURL), d.CSRFFieldProvider(), core.DangerButton("small", Type("submit"), Text("Delete"))),
				)),
			))
		}
		metricRows = Div(Class("overflow-x-auto"), Table(Class("min-w-full text-left text-sm"),
			THead(Tr(Th(Text("Name")), Th(Text("Type")), Th(Text("Expression")), Th(Text("Status")), Th(Class("text-right"), Text("Actions")))),
			TBody(Group(rows)),
		))
	}

	preAggRows := Node(P(Class("text-xs text-[var(--fgColor-muted)]"), Text("No pre-aggregations created yet.")))
	if len(d.PreAggregations) > 0 {
		rows := make([]Node, 0, len(d.PreAggregations))
		for i := range d.PreAggregations {
			item := d.PreAggregations[i]
			rows = append(rows, Tr(
				Td(Text(item.Name)),
				Td(Text(item.Grain)),
				Td(Text(item.Target)),
				Td(Class("text-right"), Div(Class("mt-0 flex flex-wrap items-center justify-end gap-2 [&_form]:m-0 [&_form]:inline-flex"),
					core.SecondaryLink(item.EditURL, "small", Text("Edit")),
					Form(Method("post"), Action(item.DeleteURL), d.CSRFFieldProvider(), core.DangerButton("small", Type("submit"), Text("Delete"))),
				)),
			))
		}
		preAggRows = Div(Class("overflow-x-auto"), Table(Class("min-w-full text-left text-sm"),
			THead(Tr(Th(Text("Name")), Th(Text("Grain")), Th(Text("Target")), Th(Class("text-right"), Text("Actions")))),
			TBody(Group(rows)),
		))
	}

	return core.AppPage("Semantic Model: "+d.ProjectName+"."+d.ModelName, "semantic", d.Principal,
		semanticSectionNav("models"),
		core.DetailShell(
			core.DetailHero(
				core.DetailHeroCopy(
					core.Kicker("Semantic model"),
					core.DetailTitle(d.ProjectName+"."+d.ModelName),
					core.DetailDescription(valueOrDash(d.Description)),
					core.BadgeRow(core.Badge("Base "+d.BaseModelRef, "accent"), core.Badge("Time "+d.DefaultTimeDim, "")),
				),
				core.DetailHeroMeta(
					core.MetaItem("Base model", d.BaseModelRef),
					core.MetaItem("Default time dimension", d.DefaultTimeDim),
				),
			),
			core.DetailLayout(
				core.DetailMain(
					core.SectionSurface(
						core.SectionHeader("Metrics", "Manage the current metric definitions before adding more."),
						metricRows,
					),
					core.SectionSurface(
						core.SectionHeader("Pre-aggregations", "Inspect materialized semantic accelerators tied to this model."),
						preAggRows,
					),
					semanticQueryCard(d.ProjectName, d.ModelName, d.QueryExplainURL, d.QueryRunURL, d.CSRFFieldProvider),
				),
				core.DetailRail(
					core.SectionSurface(
						core.SectionHeader("Actions", "Core model-level actions and nearby semantic navigation."),
						Div(Class("flex flex-wrap items-center gap-3 [&_form]:m-0 [&_form]:inline-flex"),
							core.SecondaryLink(d.EditURL, "", Text("Edit model")),
							core.SecondaryLink("/ui/semantic/relationships", "", Text("Relationships")),
							Form(Method("post"), Action(d.DeleteURL), d.CSRFFieldProvider(), core.DangerButton("", Type("submit"), Text("Delete"))),
						),
					),
					core.SectionSurface(
						core.SectionHeader("Add metric", "Create metric definitions from a compact side workflow."),
						Form(Class("grid gap-3"), Method("post"), Action(d.MetricsCreateURL),
							d.CSRFFieldProvider(),
							Label(Text("Name")),
							core.InputControl("", Name("name"), Required()),
							Label(Text("Label")),
							core.InputControl("", Name("label")),
							Label(Text("Description")),
							core.InputControl("", Name("description")),
							Label(Text("Metric type")),
							core.SelectControl("", Name("metric_type"), Option(Value("SUM"), Text("SUM")), Option(Value("COUNT"), Text("COUNT")), Option(Value("COUNT_DISTINCT"), Text("COUNT_DISTINCT")), Option(Value("AVG"), Text("AVG")), Option(Value("MIN"), Text("MIN")), Option(Value("MAX"), Text("MAX")), Option(Value("RATIO"), Text("RATIO"))),
							Label(Text("Expression mode")),
							core.SelectControl("", Name("expression_mode"), Option(Value("DSL"), Text("DSL")), Option(Value("SQL"), Text("SQL"))),
							Label(Text("Expression")),
							core.TextareaControl("min-h-24", Name("expression"), Required()),
							Label(Text("Metric filter SQL")),
							core.InputControl("", Name("filter_sql")),
							Label(Text("Default time grain")),
							core.InputControl("", Name("default_time_grain")),
							Label(Text("Format")),
							core.InputControl("", Name("format")),
							Label(Text("Certification state")),
							core.SelectControl("", Name("certification_state"), Option(Value("DRAFT"), Text("DRAFT")), Option(Value("CERTIFIED"), Text("CERTIFIED")), Option(Value("DEPRECATED"), Text("DEPRECATED"))),
							Div(Class("mt-2"), core.PrimaryButton("", Type("submit"), Text("Create metric"))),
						),
					),
					core.SectionSurface(
						core.SectionHeader("Add pre-aggregation", "Keep acceleration authoring close, but secondary to the existing model inventory."),
						Form(Class("grid gap-3"), Method("post"), Action(d.PreAggCreateURL),
							d.CSRFFieldProvider(),
							Label(Text("Name")),
							core.InputControl("", Name("name"), Required()),
							Label(Text("Metric set (comma separated)")),
							core.InputControl("", Name("metric_set")),
							Label(Text("Dimension set (comma separated)")),
							core.InputControl("", Name("dimension_set")),
							Label(Text("Grain")),
							core.InputControl("", Name("grain")),
							Label(Text("Target relation")),
							core.InputControl("", Name("target_relation"), Required()),
							Label(Text("Refresh policy")),
							core.InputControl("", Name("refresh_policy")),
							Div(Class("mt-2"), core.PrimaryButton("", Type("submit"), Text("Create pre-aggregation"))),
						),
					),
				),
			),
		),
	)
}

func semanticRelationshipsPage(d semanticRelationshipsPageData) Node {
	modelOptions := make([]Node, 0, len(d.ModelOptions))
	for i := range d.ModelOptions {
		modelOptions = append(modelOptions, Option(Value(d.ModelOptions[i].Value), Text(d.ModelOptions[i].Label)))
	}

	table := Node(P(Class("text-xs text-[var(--fgColor-muted)]"), Text("No relationships defined.")))
	if len(d.Rows) > 0 {
		rows := make([]Node, 0, len(d.Rows))
		for i := range d.Rows {
			row := d.Rows[i]
			rows = append(rows, Tr(
				Td(Text(row.Name)),
				Td(Text(row.FromModel)),
				Td(Text(row.ToModel)),
				Td(Text(row.Type)),
				Td(Text(row.JoinSQL)),
				Td(Class("text-right"), Div(Class("mt-0 flex flex-wrap items-center justify-end gap-2 [&_form]:m-0 [&_form]:inline-flex"),
					core.SecondaryLink(row.EditURL, "small", Text("Edit")),
					Form(Method("post"), Action(row.DeleteURL), d.CSRFFieldProvider(), core.DangerButton("small", Type("submit"), Text("Delete"))),
				)),
			))
		}
		table = Div(Class("overflow-x-auto"), Table(Class("min-w-full text-left text-sm"),
			THead(Tr(Th(Text("Name")), Th(Text("From")), Th(Text("To")), Th(Text("Type")), Th(Text("Join SQL")), Th(Class("text-right"), Text("Actions")))),
			TBody(Group(rows)),
		))
	}

	return core.AppPage("Semantic Relationships", "semantic", d.Principal,
		semanticSectionNav("relationships"),
		core.PageHeader("Build", "Semantic relationships", "Model relationships stay adjacent to the semantic model inventory, but they are a secondary workflow to the core model workspace."),
		core.SectionSurface(
			core.SectionHeader("Create relationship", "Define reusable join paths between semantic models."),
			Form(Class("grid gap-3"), Method("post"), Action("/ui/semantic/relationships"),
				d.CSRFFieldProvider(),
				Label(Text("Name")),
				core.InputControl("", Name("name"), Required()),
				Label(Text("From model")),
				core.SelectControl("", Name("from_semantic_id"), Group(modelOptions)),
				Label(Text("To model")),
				core.SelectControl("", Name("to_semantic_id"), Group(modelOptions)),
				Label(Text("Relationship type")),
				core.SelectControl("", Name("relationship_type"), Option(Value("ONE_TO_ONE"), Text("ONE_TO_ONE")), Option(Value("ONE_TO_MANY"), Text("ONE_TO_MANY")), Option(Value("MANY_TO_ONE"), Text("MANY_TO_ONE")), Option(Value("MANY_TO_MANY"), Text("MANY_TO_MANY"))),
				Label(Text("Join SQL")),
				core.TextareaControl("min-h-24 font-mono text-xs", Name("join_sql"), Required()),
				Label(Text("Cost")),
				core.InputControl("", Name("cost"), Value("0")),
				Label(Text("Max hops")),
				core.InputControl("", Name("max_hops"), Value("0")),
				Label(Class("inline-flex items-center gap-2"), Input(Type("checkbox"), Name("is_default"), Value("true"), Class("h-4 w-4")), Span(Text("Default relationship"))),
				Div(Class("mt-2"), core.PrimaryButton("", Type("submit"), Text("Create relationship"))),
			),
		),
		core.SectionSurface(
			core.SectionHeader("Existing relationships", "Review and maintain relationship paths independently from model authoring."),
			table,
			P(Class("mt-4 text-sm text-[var(--fgColor-muted)]"), Text("Showing up to "+strconv.Itoa(d.Page.MaxResults)+" relationships. Total: "+strconv.FormatInt(d.Total, 10))),
		),
	)
}

func semanticQueryResultPage(d semanticQueryResultPageData) Node {
	resultNode := Node(nil)
	if d.Plan != nil {
		joinRows := make([]Node, 0, len(d.Plan.JoinPath))
		for i := range d.Plan.JoinPath {
			join := d.Plan.JoinPath[i]
			joinRows = append(joinRows, Tr(Td(Text(join.RelationshipName)), Td(Text(join.FromModel)), Td(Text(join.ToModel)), Td(Text(join.JoinSQL))))
		}
		resultNode = Group([]Node{
			Div(Class("rounded-xl border border-[var(--borderColor-default)] bg-[var(--bgColor-default)] p-4 shadow-xs"),
				H2(Class("mt-0 text-lg font-semibold"), Text("Query plan")),
				P(Text("Base model: "+d.Plan.BaseModelName)),
				P(Text("Base relation: "+d.Plan.BaseRelation)),
				P(Text("Metrics: "+stringsJoin(d.Plan.Metrics))),
				P(Text("Dimensions: "+stringsJoin(d.Plan.Dimensions))),
				P(Text("Freshness status: "+d.Plan.FreshnessStatus)),
				P(Text("Freshness basis: "+stringsJoin(d.Plan.FreshnessBasis))),
				H3(Class("mt-4 text-lg font-semibold"), Text("Generated SQL")),
				Pre(Class("overflow-x-auto rounded-lg border border-[var(--borderColor-muted)] bg-[var(--bgColor-muted)] p-3 text-sm"), Text(d.Plan.GeneratedSQL)),
			),
			Div(Class("rounded-xl border border-[var(--borderColor-default)] bg-[var(--bgColor-default)] p-4 shadow-xs"), H2(Class("mt-0 text-lg font-semibold"), Text("Join path")),
				Div(Class("overflow-x-auto"), Table(Class("min-w-full text-left text-sm"),
					THead(Tr(Th(Text("Relationship")), Th(Text("From")), Th(Text("To")), Th(Text("Join SQL")))),
					TBody(Group(joinRows)),
				)),
			),
		})
	}
	if d.Result != nil && d.Result.Result != nil {
		headers := make([]Node, 0, len(d.Result.Result.Columns))
		for i := range d.Result.Result.Columns {
			headers = append(headers, Th(Text(d.Result.Result.Columns[i])))
		}
		rows := make([]Node, 0, len(d.Result.Result.Rows))
		for i := range d.Result.Result.Rows {
			cells := make([]Node, 0, len(d.Result.Result.Rows[i]))
			for j := range d.Result.Result.Rows[i] {
				cells = append(cells, Td(Text(sqlCellString(d.Result.Result.Rows[i][j]))))
			}
			rows = append(rows, Tr(Group(cells)))
		}
		resultNode = Group([]Node{
			resultNode,
			Div(Class("rounded-xl border border-[var(--borderColor-default)] bg-[var(--bgColor-default)] p-4 shadow-xs"), H2(Class("mt-0 text-lg font-semibold"), Text("Execution result")),
				Div(Class("overflow-x-auto"), Table(Class("min-w-full text-left text-sm"),
					THead(Tr(Group(headers))),
					TBody(Group(rows)),
				)),
			),
		})
	}
	return core.AppPage("Semantic Query", "semantic", d.Principal,
		semanticQueryCard(d.Request.ProjectName, d.Request.SemanticModelName, "/ui/semantic/query/explain", "/ui/semantic/query/run", d.CSRFFieldProvider, &d.Request),
		resultNode,
	)
}

func semanticQueryCard(projectName, semanticModelName, explainURL, runURL string, csrfFieldProvider func() Node, req ...*semsvc.MetricQueryRequest) Node {
	var request *semsvc.MetricQueryRequest
	if len(req) > 0 {
		request = req[0]
	}
	metrics := ""
	dimensions := ""
	filters := ""
	orderBy := ""
	limit := ""
	timeGrain := ""
	if request != nil {
		metrics = csvValues(request.Metrics)
		dimensions = csvValues(request.Dimensions)
		filters = csvValues(request.Filters)
		orderBy = csvValues(request.OrderBy)
		if request.Limit != nil {
			limit = strconv.Itoa(*request.Limit)
		}
		if request.TimeGrain != nil {
			timeGrain = *request.TimeGrain
		}
	}
	return Div(Class("rounded-xl border border-[var(--borderColor-default)] bg-[var(--bgColor-default)] p-4 shadow-xs"),
		H3(Class("mt-0 text-lg font-semibold"), Text("Metric query")),
		Form(Class("grid gap-3"), Method("post"), Action(explainURL),
			csrfFieldProvider(),
			Label(Text("Project")),
			core.InputControl("", Name("project_name"), Value(projectName), Required()),
			Label(Text("Semantic model")),
			core.InputControl("", Name("semantic_model_name"), Value(semanticModelName), Required()),
			Label(Text("Metrics (comma separated)")),
			core.InputControl("", Name("metrics"), Value(metrics), Required()),
			Label(Text("Dimensions (comma separated)")),
			core.InputControl("", Name("dimensions"), Value(dimensions)),
			Label(Text("Filters (comma separated)")),
			core.InputControl("", Name("filters"), Value(filters)),
			Label(Text("Order by (comma separated)")),
			core.InputControl("", Name("order_by"), Value(orderBy)),
			Label(Text("Limit")),
			core.InputControl("", Name("limit"), Value(limit)),
			Label(Text("Time grain")),
			core.InputControl("", Name("time_grain"), Value(timeGrain)),
			Div(Class("mt-1 flex flex-wrap items-center gap-2 [&_form]:m-0 [&_form]:inline-flex"),
				core.PrimaryButton("", Type("submit"), Text("Explain query")),
				core.SecondaryButton("", Type("submit"), FormAction(runURL), Text("Run query")),
			),
		),
	)
}

func semanticModelsEditPage(principal domain.ContextPrincipal, projectName, semanticModelName string, item *domain.SemanticModel, csrfFieldProvider func() Node) Node {
	return semanticFormPage(principal, "Edit Semantic Model", "/ui/semantic/models/"+projectName+"/"+semanticModelName+"/update", csrfFieldProvider,
		Label(Text("Description")),
		core.TextareaControl("min-h-24", Name("description"), Text(item.Description)),
		Label(Text("Base model reference")),
		core.InputControl("", Name("base_model_ref"), Value(item.BaseModelRef), Required()),
		Label(Text("Default time dimension")),
		core.InputControl("", Name("default_time_dimension"), Value(item.DefaultTimeDimension)),
		Label(Text("Tags (comma separated)")),
		core.InputControl("", Name("tags"), Value(csvValues(item.Tags))),
	)
}

func semanticMetricEditPage(principal domain.ContextPrincipal, projectName, semanticModelName string, metric *domain.SemanticMetric, csrfFieldProvider func() Node) Node {
	return semanticFormPage(principal, "Edit Semantic Metric", "/ui/semantic/models/"+projectName+"/"+semanticModelName+"/metrics/"+metric.Name+"/update", csrfFieldProvider,
		Label(Text("Label")),
		core.InputControl("", Name("label"), Value(metric.Label)),
		Label(Text("Description")),
		core.TextareaControl("min-h-24", Name("description"), Text(metric.Description)),
		Label(Text("Metric type")),
		core.SelectControl("", Name("metric_type"), optionSelected("SUM", metric.MetricType), optionSelected("COUNT", metric.MetricType), optionSelected("COUNT_DISTINCT", metric.MetricType), optionSelected("AVG", metric.MetricType), optionSelected("MIN", metric.MetricType), optionSelected("MAX", metric.MetricType), optionSelected("RATIO", metric.MetricType)),
		Label(Text("Expression mode")),
		core.SelectControl("", Name("expression_mode"), optionSelected("DSL", metric.ExpressionMode), optionSelected("SQL", metric.ExpressionMode)),
		Label(Text("Expression")),
		core.TextareaControl("min-h-24", Name("expression"), Required(), Text(metric.Expression)),
		Label(Text("Metric filter SQL")),
		core.InputControl("", Name("filter_sql"), Value(metric.FilterSQL)),
		Label(Text("Default time grain")),
		core.InputControl("", Name("default_time_grain"), Value(metric.DefaultTimeGrain)),
		Label(Text("Format")),
		core.InputControl("", Name("format"), Value(metric.Format)),
		Label(Text("Certification state")),
		core.SelectControl("", Name("certification_state"), optionSelected("DRAFT", metric.CertificationState), optionSelected("CERTIFIED", metric.CertificationState), optionSelected("DEPRECATED", metric.CertificationState)),
	)
}

func semanticPreAggregationEditPage(principal domain.ContextPrincipal, projectName, semanticModelName string, item *domain.SemanticPreAggregation, csrfFieldProvider func() Node) Node {
	return semanticFormPage(principal, "Edit Pre-Aggregation", "/ui/semantic/models/"+projectName+"/"+semanticModelName+"/pre-aggregations/"+item.Name+"/update", csrfFieldProvider,
		Label(Text("Metric set (comma separated)")),
		core.InputControl("", Name("metric_set"), Value(csvValues(item.MetricSet))),
		Label(Text("Dimension set (comma separated)")),
		core.InputControl("", Name("dimension_set"), Value(csvValues(item.DimensionSet))),
		Label(Text("Grain")),
		core.InputControl("", Name("grain"), Value(item.Grain)),
		Label(Text("Target relation")),
		core.InputControl("", Name("target_relation"), Value(item.TargetRelation), Required()),
		Label(Text("Refresh policy")),
		core.InputControl("", Name("refresh_policy"), Value(item.RefreshPolicy)),
	)
}

func semanticRelationshipEditPage(principal domain.ContextPrincipal, item *domain.SemanticRelationship, modelOptions []semanticOptionData, csrfFieldProvider func() Node) Node {
	fromOptions := make([]Node, 0, len(modelOptions))
	toOptions := make([]Node, 0, len(modelOptions))
	for i := range modelOptions {
		opt := modelOptions[i]
		fromOptions = append(fromOptions, Option(Value(opt.Value), selectedIf(opt.Value == item.FromSemanticID), Text(opt.Label)))
		toOptions = append(toOptions, Option(Value(opt.Value), selectedIf(opt.Value == item.ToSemanticID), Text(opt.Label)))
	}
	return semanticFormPage(principal, "Edit Relationship", "/ui/semantic/relationships/"+item.Name+"/update", csrfFieldProvider,
		Label(Text("From model")),
		core.SelectControl("", Name("from_semantic_id"), Disabled(), Group(fromOptions)),
		Label(Text("To model")),
		core.SelectControl("", Name("to_semantic_id"), Disabled(), Group(toOptions)),
		Label(Text("Relationship type")),
		core.SelectControl("", Name("relationship_type"), optionSelected("ONE_TO_ONE", item.RelationshipType), optionSelected("ONE_TO_MANY", item.RelationshipType), optionSelected("MANY_TO_ONE", item.RelationshipType), optionSelected("MANY_TO_MANY", item.RelationshipType)),
		Label(Text("Join SQL")),
		core.TextareaControl("min-h-24 font-mono text-xs", Name("join_sql"), Required(), Text(item.JoinSQL)),
		Label(Text("Cost")),
		core.InputControl("", Name("cost"), Value(strconv.Itoa(item.Cost))),
		Label(Text("Max hops")),
		core.InputControl("", Name("max_hops"), Value(strconv.Itoa(item.MaxHops))),
		Label(Class("inline-flex items-center gap-2"), Input(Type("checkbox"), Name("is_default"), Value("true"), checkedIf(item.IsDefault), Class("h-4 w-4")), Span(Text("Default relationship"))),
	)
}

func semanticFormPage(principal domain.ContextPrincipal, title, action string, csrfFieldProvider func() Node, fields ...Node) Node {
	nodes := []Node{csrfFieldProvider()}
	nodes = append(nodes, fields...)
	nodes = append(nodes, Div(Class("mt-4"), core.PrimaryButton("", Type("submit"), Text("Save"))))
	return core.AppPage(title, "semantic", principal,
		semanticSectionNav("models"),
		core.SectionSurface(
			core.SectionHeader(title, "Create or update semantic workspace records."),
			Form(Class("grid gap-3"), Method("post"), Action(action), Group(nodes)),
		),
	)
}

func sectionHeader(title, copy, href, action string) Node {
	return core.PageHeader("Build", title, copy, core.PrimaryLink(href, "", Text(action)))
}

func semanticSectionNav(active string) Node {
	return core.SectionTabs([]core.SectionTab{
		{Label: "Models", Href: "/ui/semantic/models", Active: active == "models"},
		{Label: "Relationships", Href: "/ui/semantic/relationships", Active: active == "relationships"},
	})
}

func optionSelected(value, selected string) Node {
	if value == selected {
		return Option(Value(value), Selected(), Text(value))
	}
	return Option(Value(value), Text(value))
}

func selectedIf(v bool) Node {
	if v {
		return Selected()
	}
	return nil
}

func checkedIf(v bool) Node {
	if v {
		return Checked()
	}
	return nil
}
