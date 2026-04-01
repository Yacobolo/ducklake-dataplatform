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
	Name              string
	Type              string
	Expression        string
	RelationshipNames []string
	Status            string
	EditURL           string
	DeleteURL         string
}

type semanticPreAggRowData struct {
	Name      string
	Grain     string
	Target    string
	EditURL   string
	DeleteURL string
}

type semanticModelDetailPageData struct {
	Principal            domain.ContextPrincipal
	ProjectName          string
	ModelName            string
	BaseModelRef         string
	DefaultTimeDim       string
	Description          string
	EditURL              string
	DeleteURL            string
	MetricsCreateURL     string
	PreAggCreateURL      string
	QueryExplainURL      string
	QueryRunURL          string
	GraphNodesJSON       string
	GraphEdgesJSON       string
	RelationshipCount    int
	ConnectedModelCount  int
	RelatedRelationships []semanticRelatedRelationshipRowData
	Metrics              []semanticMetricRowData
	PreAggregations      []semanticPreAggRowData
	CSRFFieldProvider    func() Node
}

type semanticEditableRelationshipRowData struct {
	Name            string
	RelatedRelation string
	Type            string
	Cardinality     string
	JoinSQL         string
	Cost            int
	MaxHops         int
	UpdateURL       string
	DeleteURL       string
}

type semanticModelEditPageData struct {
	Principal             domain.ContextPrincipal
	ProjectName           string
	ModelName             string
	Description           string
	BaseModelRef          string
	DefaultTimeDim        string
	TagsCSV               string
	UpdateURL             string
	DeleteURL             string
	RelationshipCreateURL string
	MetricsCreateURL      string
	PreAggCreateURL       string
	QueryExplainURL       string
	QueryRunURL           string
	RelatedModelOptions   []semanticOptionData
	Relationships         []semanticEditableRelationshipRowData
	Metrics               []semanticMetricRowData
	PreAggregations       []semanticPreAggRowData
	CSRFFieldProvider     func() Node
}

type semanticOptionData struct {
	Value string
	Label string
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
				core.TablePrimaryCell(
					core.ResourceIcon("semantic-model"),
					A(Href(row.URL), Class("font-mono text-[13px] font-semibold text-[var(--fgColor-accent)] no-underline visited:text-[var(--fgColor-accent)] hover:text-[var(--fgColor-accent)] hover:underline active:text-[var(--fgColor-accent)]"), Text(row.Name)),
				),
				Td(core.TableMetaText(row.BaseModel)),
				Td(core.TableMetaText(row.Owner)),
				Td(core.TableMetaText(row.UpdatedAt)),
			))
		}
		table = core.TableContainer("",
			core.DataTable("",
				THead(Tr(Th(Scope("col"), Text("Semantic model")), Th(Scope("col"), Text("Base relation")), Th(Scope("col"), Text("Owner")), Th(Scope("col"), Text("Updated")))),
				TBody(Group(tableRows)),
			),
		)
	}
	return core.AppPage("Semantic Models", "semantic", principal,
		core.ListPageLayout(
			core.ListPageHeader("Semantic models", "Use the semantic workspace for the consumer-facing semantic layer. Relationship paths stay nearby, but semantic model management remains the default landing surface.", core.PrimaryLink("/ui/semantic/models/new", "", Text("New semantic model"))),
			core.ListPageBody(
				table,
				core.ListPagination("/ui/semantic/models", page, total),
			),
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
		Label(Text("Base relation reference")),
		core.InputControl("", Name("base_model_ref"), Required()),
		Label(Text("Default time dimension")),
		core.InputControl("", Name("default_time_dimension")),
		Label(Text("Tags (comma separated)")),
		core.InputControl("", Name("tags")),
	)
}

func semanticModelDetailPage(d semanticModelDetailPageData) Node {
	metricRows := semanticMetricsTable(d.Metrics, d.CSRFFieldProvider, false)
	relationshipRows := semanticRelatedRelationshipsTable(d.RelatedRelationships, false)
	descriptionNode := Node(nil)
	if d.Description != "" {
		descriptionNode = P(Class("m-0 max-w-3xl text-sm leading-6 text-[var(--fgColor-muted)]"), Text(d.Description))
	}

	return core.AppPage("Semantic Model: "+d.ProjectName+"."+d.ModelName, "semantic", d.Principal,
		core.DetailShell(
			core.SectionSurface(
				Div(Class("flex flex-col gap-4 lg:flex-row lg:items-start lg:justify-between"),
					Div(Class("grid gap-3"),
						core.Kicker("Semantic model"),
						H1(Class("m-0 text-3xl font-semibold tracking-tight"), Text(d.ProjectName+"."+d.ModelName)),
						descriptionNode,
						core.BadgeRow(
				core.Badge("Base relation "+d.BaseModelRef, "accent"),
				core.Badge("Time "+valueOrDash(d.DefaultTimeDim), ""),
				core.Badge(strconv.Itoa(d.RelationshipCount)+" join paths", ""),
				core.Badge(strconv.Itoa(len(d.Metrics))+" metrics", ""),
				core.Badge(strconv.Itoa(d.ConnectedModelCount)+" connected relations", ""),
						),
					),
					Div(Class("flex flex-wrap items-center gap-3 [&_form]:m-0 [&_form]:inline-flex"),
						core.SecondaryLink(d.EditURL, "", Text("Edit semantic model")),
						core.ActionMenu("More",
							core.ActionMenuPost(d.DeleteURL, "Delete semantic model", d.CSRFFieldProvider, true),
						),
					),
				),
			),
			core.SectionSurface(
				core.SectionHeader("Semantic model map", "Power BI-style overview of the semantic model and its direct joins before you edit anything."),
				El("semantic-model-flow",
					Class("block"),
					Attr("nodes", d.GraphNodesJSON),
					Attr("edges", d.GraphEdgesJSON),
				),
			),
			core.SectionSurface(
				core.SectionHeader("Join paths", "Directed join paths owned by this semantic model, shown the same way they are authored."),
				relationshipRows,
			),
			core.SectionSurface(
				core.SectionHeader("Metrics", "The primary semantic outputs exposed by this model."),
				metricRows,
			),
			Script(Type("module"), Src(core.UIScriptHref("semantic-model-flow.js"))),
		),
	)
}

func semanticRelatedRelationshipsTable(rows []semanticRelatedRelationshipRowData, showActions bool) Node {
	if len(rows) == 0 {
		return core.EmptyState("waypoints", "No direct relationships yet", "Add a relationship from the semantic model edit page to see join paths appear here and in the semantic model map.", nil)
	}

	tableRows := make([]Node, 0, len(rows))
	for i := range rows {
		row := rows[i]
		cells := []Node{
			Td(Text(row.Name)),
			Td(
				Div(Class("grid gap-1"),
					Span(Class("font-medium text-[var(--fgColor-default)]"), Text(row.JoinLabel)),
					Span(Class("text-xs text-[var(--fgColor-muted)]"), Text(row.JoinSQL)),
				),
			),
			Td(Text(row.RelatedRelation)),
			Td(Text(row.Cardinality)),
		}
		if showActions {
			cells = append(cells, core.TableActionCell(core.SecondaryLink(row.EditURL, "small", Text("Edit"))))
		}
		tableRows = append(tableRows, Tr(Group(cells)))
	}

	headers := []Node{
		Th(Text("Join path")),
		Th(Text("Join")),
		Th(Text("Related relation")),
		Th(Text("Cardinality")),
	}
	if showActions {
		headers = append(headers, core.TableActionHeader())
	}

	return core.TableContainer("",
		core.DataTable("",
			THead(Tr(Group(headers))),
			TBody(Group(tableRows)),
		),
	)
}

func semanticMetricsTable(rows []semanticMetricRowData, csrfFieldProvider func() Node, showActions bool) Node {
	if len(rows) == 0 {
		return P(Class("text-xs text-[var(--fgColor-muted)]"), Text("No metrics created yet."))
	}

	tableRows := make([]Node, 0, len(rows))
	for i := range rows {
		metric := rows[i]
		cells := []Node{
			Td(Text(metric.Name)),
			Td(Text(metric.Type)),
			Td(Text(metric.Expression)),
			Td(Text(valueOrDash(csvValues(metric.RelationshipNames)))),
			Td(Text(metric.Status)),
		}
		if showActions {
			cells = append(cells, core.TableActionCell(
				core.SecondaryLink(metric.EditURL, "small", Text("Edit")),
				Form(Method("post"), Action(metric.DeleteURL), csrfFieldProvider(), core.DangerButton("small", Type("submit"), Text("Delete"))),
			))
		}
		tableRows = append(tableRows, Tr(Group(cells)))
	}

	headers := []Node{Th(Text("Name")), Th(Text("Type")), Th(Text("Expression")), Th(Text("Join paths")), Th(Text("Status"))}
	if showActions {
		headers = append(headers, core.TableActionHeader())
	}

	return core.TableContainer("",
		core.DataTable("",
			THead(Tr(Group(headers))),
			TBody(Group(tableRows)),
		),
	)
}

func semanticPreAggregationsTable(rows []semanticPreAggRowData, csrfFieldProvider func() Node, showActions bool) Node {
	if len(rows) == 0 {
		return P(Class("text-xs text-[var(--fgColor-muted)]"), Text("No pre-aggregations created yet."))
	}

	tableRows := make([]Node, 0, len(rows))
	for i := range rows {
		item := rows[i]
		cells := []Node{
			Td(Text(item.Name)),
			Td(Text(item.Grain)),
			Td(Text(item.Target)),
		}
		if showActions {
			cells = append(cells, core.TableActionCell(
				core.SecondaryLink(item.EditURL, "small", Text("Edit")),
				Form(Method("post"), Action(item.DeleteURL), csrfFieldProvider(), core.DangerButton("small", Type("submit"), Text("Delete"))),
			))
		}
		tableRows = append(tableRows, Tr(Group(cells)))
	}

	headers := []Node{Th(Text("Name")), Th(Text("Grain")), Th(Text("Target"))}
	if showActions {
		headers = append(headers, core.TableActionHeader())
	}

	return core.TableContainer("",
		core.DataTable("",
			THead(Tr(Group(headers))),
			TBody(Group(tableRows)),
		),
	)
}

func semanticMetricCreateForm(d semanticModelDetailPageData) Node {
	return Div(Class("grid gap-3"),
		P(Class("m-0 text-sm leading-6 text-[var(--fgColor-muted)]"), Text("Define a new metric after reviewing the current inventory.")),
		Form(Class("grid gap-3 sm:grid-cols-2"), Method("post"), Action(d.MetricsCreateURL),
			d.CSRFFieldProvider(),
			Div(Class("grid gap-2"),
				Label(Text("Name")),
				core.InputControl("", Name("name"), Required()),
			),
			Div(Class("grid gap-2"),
				Label(Text("Label")),
				core.InputControl("", Name("label")),
			),
			Div(Class("grid gap-2 sm:col-span-2"),
				Label(Text("Description")),
				core.InputControl("", Name("description")),
			),
			Div(Class("grid gap-2"),
				Label(Text("Metric type")),
				core.SelectControl("", Name("metric_type"), Option(Value("SUM"), Text("SUM")), Option(Value("COUNT"), Text("COUNT")), Option(Value("COUNT_DISTINCT"), Text("COUNT_DISTINCT")), Option(Value("AVG"), Text("AVG")), Option(Value("MIN"), Text("MIN")), Option(Value("MAX"), Text("MAX")), Option(Value("RATIO"), Text("RATIO"))),
			),
			Div(Class("grid gap-2"),
				Label(Text("Expression mode")),
				core.SelectControl("", Name("expression_mode"), Option(Value("DSL"), Text("DSL")), Option(Value("SQL"), Text("SQL"))),
			),
			Div(Class("grid gap-2 sm:col-span-2"),
				Label(Text("Expression")),
				core.TextareaControl("min-h-24", Name("expression"), Required()),
			),
			Div(Class("grid gap-2 sm:col-span-2"),
				Label(Text("Join paths (comma separated)")),
				core.InputControl("", Name("relationship_names")),
			),
			Div(Class("grid gap-2"),
				Label(Text("Metric filter SQL")),
				core.InputControl("", Name("filter_sql")),
			),
			Div(Class("grid gap-2"),
				Label(Text("Default time grain")),
				core.InputControl("", Name("default_time_grain")),
			),
			Div(Class("grid gap-2"),
				Label(Text("Format")),
				core.InputControl("", Name("format")),
			),
			Div(Class("grid gap-2"),
				Label(Text("Certification state")),
				core.SelectControl("", Name("certification_state"), Option(Value("DRAFT"), Text("DRAFT")), Option(Value("CERTIFIED"), Text("CERTIFIED")), Option(Value("DEPRECATED"), Text("DEPRECATED"))),
			),
			Div(Class("sm:col-span-2"), core.PrimaryButton("", Type("submit"), Text("Create metric"))),
		),
	)
}

func semanticPreAggregationCreateForm(d semanticModelDetailPageData) Node {
	return Div(Class("grid gap-3"),
		P(Class("m-0 text-sm leading-6 text-[var(--fgColor-muted)]"), Text("Keep acceleration authoring close at hand, but separate from the core model overview.")),
		Form(Class("grid gap-3 sm:grid-cols-2"), Method("post"), Action(d.PreAggCreateURL),
			d.CSRFFieldProvider(),
			Div(Class("grid gap-2"),
				Label(Text("Name")),
				core.InputControl("", Name("name"), Required()),
			),
			Div(Class("grid gap-2"),
				Label(Text("Grain")),
				core.InputControl("", Name("grain")),
			),
			Div(Class("grid gap-2 sm:col-span-2"),
				Label(Text("Metric set (comma separated)")),
				core.InputControl("", Name("metric_set")),
			),
			Div(Class("grid gap-2 sm:col-span-2"),
				Label(Text("Dimension set (comma separated)")),
				core.InputControl("", Name("dimension_set")),
			),
			Div(Class("grid gap-2 sm:col-span-2"),
				Label(Text("Target relation")),
				core.InputControl("", Name("target_relation"), Required()),
			),
			Div(Class("grid gap-2 sm:col-span-2"),
				Label(Text("Refresh policy")),
				core.InputControl("", Name("refresh_policy")),
			),
			Div(Class("sm:col-span-2"), core.PrimaryButton("", Type("submit"), Text("Create pre-aggregation"))),
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
				P(Text("Semantic model: "+d.Plan.BaseModelName)),
				P(Text("Base relation: "+d.Plan.BaseRelation)),
				P(Text("Metrics: "+stringsJoin(d.Plan.Metrics))),
				P(Text("Dimensions: "+stringsJoin(d.Plan.Dimensions))),
				P(Text("Freshness status: "+d.Plan.FreshnessStatus)),
				P(Text("Freshness basis: "+stringsJoin(d.Plan.FreshnessBasis))),
				H3(Class("mt-4 text-lg font-semibold"), Text("Generated SQL")),
				Pre(Class("overflow-x-auto rounded-lg border border-[var(--borderColor-muted)] bg-[var(--bgColor-muted)] p-3 text-sm"), Text(d.Plan.GeneratedSQL)),
			),
			Div(Class("rounded-xl border border-[var(--borderColor-default)] bg-[var(--bgColor-default)] p-4 shadow-xs"), H2(Class("mt-0 text-lg font-semibold"), Text("Join path")),
				core.TableContainer("",
					core.DataTable("",
						THead(Tr(Th(Text("Relationship")), Th(Text("From")), Th(Text("To")), Th(Text("Join SQL")))),
						TBody(Group(joinRows)),
					),
				),
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
				core.TableContainer("",
					core.DataTable("",
						THead(Tr(Group(headers))),
						TBody(Group(rows)),
					),
				),
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
	relationshipNames := ""
	timeGrain := ""
	if request != nil {
		metrics = csvValues(request.Metrics)
		relationshipNames = csvValues(request.RelationshipNames)
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
			Label(Text("Join paths (comma separated)")),
			core.InputControl("", Name("relationship_names"), Value(relationshipNames)),
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

func semanticModelEditPage(d semanticModelEditPageData) Node {
	metricRows := semanticMetricsTable(d.Metrics, d.CSRFFieldProvider, true)
	preAggRows := semanticPreAggregationsTable(d.PreAggregations, d.CSRFFieldProvider, true)

	createModelOptions := make([]Node, 0, len(d.RelatedModelOptions))
	for i := range d.RelatedModelOptions {
		createModelOptions = append(createModelOptions, Option(Value(d.RelatedModelOptions[i].Value), Text(d.RelatedModelOptions[i].Label)))
	}

	relationshipCards := make([]Node, 0, len(d.Relationships))
	for i := range d.Relationships {
		row := d.Relationships[i]
		relationshipCards = append(relationshipCards,
			Details(Class("rounded-xl border border-[var(--borderColor-default)] bg-[var(--bgColor-default)] p-4 shadow-xs"),
				Summary(Class("cursor-pointer list-none text-sm font-medium text-[var(--fgColor-default)] [&::-webkit-details-marker]:hidden"),
					Div(Class("flex flex-wrap items-center justify-between gap-3"),
						Span(Text(row.Name+" -> "+row.RelatedRelation)),
						Span(Class("text-xs text-[var(--fgColor-muted)]"), Text(row.Type+" ("+row.Cardinality+")")),
					),
				),
				Div(Class("grid gap-4 pt-4"),
					Form(Class("grid gap-3 sm:grid-cols-2"), Method("post"), Action(row.UpdateURL),
						d.CSRFFieldProvider(),
						Div(Class("grid gap-2"),
							Label(Text("Relationship type")),
							core.SelectControl("", Name("relationship_type"), optionSelected("ONE_TO_ONE", row.Type), optionSelected("ONE_TO_MANY", row.Type), optionSelected("MANY_TO_ONE", row.Type), optionSelected("MANY_TO_MANY", row.Type)),
						),
						Div(Class("grid gap-2 sm:col-span-2"),
							Label(Text("Join SQL")),
							core.TextareaControl("min-h-24 font-mono text-xs", Name("join_sql"), Required(), Text(row.JoinSQL)),
						),
						Div(Class("grid gap-2"),
							Label(Text("Cost")),
							core.InputControl("", Name("cost"), Value(strconv.Itoa(row.Cost))),
						),
						Div(Class("grid gap-2"),
							Label(Text("Max hops")),
							core.InputControl("", Name("max_hops"), Value(strconv.Itoa(row.MaxHops))),
						),
						Div(Class("sm:col-span-2"),
							core.PrimaryButton("", Type("submit"), Text("Save join path")),
						),
					),
					Form(Method("post"), Action(row.DeleteURL), d.CSRFFieldProvider(), core.DangerButton("", Type("submit"), Text("Delete"))),
				),
			),
		)
	}
	if len(relationshipCards) == 0 {
		relationshipCards = append(relationshipCards, core.EmptyState("waypoints", "No join paths yet", "Create the first model-local join path below.", nil))
	}

	return core.AppPage("Edit Semantic Model: "+d.ProjectName+"."+d.ModelName, "semantic", d.Principal,
		core.DetailShell(
			core.SectionSurface(
				Div(Class("flex flex-wrap items-center justify-between gap-3"),
					Div(Class("grid gap-2"),
						core.Kicker("Edit semantic model"),
						H1(Class("m-0 text-3xl font-semibold tracking-tight"), Text(d.ProjectName+"."+d.ModelName)),
					),
					core.SecondaryLink("/ui/semantic/models/"+d.ProjectName+"/"+d.ModelName, "", Text("Back to overview")),
				),
			),
			core.SectionSurface(
				core.SectionHeader("Semantic model metadata", "Update the core definition for this semantic model."),
				Div(Class("grid gap-3"),
					Form(Class("grid gap-3"), Method("post"), Action(d.UpdateURL),
						d.CSRFFieldProvider(),
						Label(Text("Description")),
						core.TextareaControl("min-h-24", Name("description"), Text(d.Description)),
						Label(Text("Base relation reference")),
						core.InputControl("", Name("base_model_ref"), Value(d.BaseModelRef), Required()),
						Label(Text("Default time dimension")),
						core.InputControl("", Name("default_time_dimension"), Value(d.DefaultTimeDim)),
						Label(Text("Tags (comma separated)")),
						core.InputControl("", Name("tags"), Value(d.TagsCSV)),
						Div(Class("mt-4"), core.PrimaryButton("", Type("submit"), Text("Save semantic model"))),
					),
					Form(Method("post"), Action(d.DeleteURL), d.CSRFFieldProvider(), core.DangerButton("", Type("submit"), Text("Delete semantic model"))),
				),
			),
			core.SectionSurface(
				core.SectionHeader("Join paths", "Manage directed join paths owned by this semantic model."),
				Div(Class("grid gap-4"), Group(relationshipCards)),
				Details(
					Class("rounded-xl border border-[var(--borderColor-default)] bg-[var(--bgColor-default)] p-4 shadow-xs"),
					Summary(Class("cursor-pointer list-none text-sm font-medium text-[var(--fgColor-default)] [&::-webkit-details-marker]:hidden"), Text("New join path")),
					Div(Class("grid gap-3 pt-4"),
						P(Class("m-0 text-sm leading-6 text-[var(--fgColor-muted)]"), Text("The current semantic model is the fixed source for every join path. Choose the related relation and define the join SQL.")),
						Form(Class("grid gap-3 sm:grid-cols-2"), Method("post"), Action(d.RelationshipCreateURL),
							d.CSRFFieldProvider(),
							Div(Class("grid gap-2"),
								Label(Text("Join path name")),
								core.InputControl("", Name("name"), Required()),
							),
							Div(Class("grid gap-2"),
								Label(Text("Related relation")),
								core.SelectControl("", Name("related_semantic_id"), Group(createModelOptions)),
							),
							Div(Class("grid gap-2"),
								Label(Text("Relationship type")),
								core.SelectControl("", Name("relationship_type"), Option(Value("ONE_TO_ONE"), Text("ONE_TO_ONE")), Option(Value("ONE_TO_MANY"), Text("ONE_TO_MANY")), Option(Value("MANY_TO_ONE"), Text("MANY_TO_ONE")), Option(Value("MANY_TO_MANY"), Text("MANY_TO_MANY"))),
							),
							Div(Class("grid gap-2 sm:col-span-2"),
								Label(Text("Join SQL")),
								core.TextareaControl("min-h-24 font-mono text-xs", Name("join_sql"), Required()),
							),
							Div(Class("grid gap-2"),
								Label(Text("Cost")),
								core.InputControl("", Name("cost"), Value("0")),
							),
							Div(Class("grid gap-2"),
								Label(Text("Max hops")),
								core.InputControl("", Name("max_hops"), Value("0")),
							),
							Div(Class("sm:col-span-2"), core.PrimaryButton("", Type("submit"), Text("Create join path"))),
						),
					),
				),
			),
			core.SectionSurface(
				core.SectionHeader("Metrics", "Model-scoped metric authoring stays with the model edit surface."),
				metricRows,
				Details(
					Class("rounded-xl border border-[var(--borderColor-default)] bg-[var(--bgColor-default)] p-4 shadow-xs"),
					Summary(Class("cursor-pointer list-none text-sm font-medium text-[var(--fgColor-default)] [&::-webkit-details-marker]:hidden"), Text("New metric")),
					Div(Class("pt-4"), semanticMetricCreateForm(semanticModelDetailPageData{
						MetricsCreateURL:  d.MetricsCreateURL,
						CSRFFieldProvider: d.CSRFFieldProvider,
					})),
				),
			),
			core.SectionSurface(
				core.SectionHeader("Advanced tools", "Secondary authoring tools stay available here, not on the read-oriented detail page."),
				Details(
					Class("rounded-xl border border-[var(--borderColor-default)] bg-[var(--bgColor-default)] p-4 shadow-xs"),
					Summary(Class("cursor-pointer list-none text-sm font-medium text-[var(--fgColor-default)] [&::-webkit-details-marker]:hidden"), Text("Pre-aggregations and query tools")),
					Div(Class("grid gap-6 pt-4"),
						Div(Class("grid gap-4"),
							H3(Class("m-0 text-lg font-semibold"), Text("Pre-aggregations")),
							preAggRows,
							semanticPreAggregationCreateForm(semanticModelDetailPageData{
								PreAggCreateURL:   d.PreAggCreateURL,
								CSRFFieldProvider: d.CSRFFieldProvider,
							}),
						),
						Div(Class("grid gap-4 border-t border-[var(--borderColor-default)] pt-6"),
							H3(Class("m-0 text-lg font-semibold"), Text("Metric query")),
							semanticQueryCard(d.ProjectName, d.ModelName, d.QueryExplainURL, d.QueryRunURL, d.CSRFFieldProvider),
						),
					),
				),
			),
		),
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
	Label(Text("Join paths (comma separated)")),
	core.InputControl("", Name("relationship_names"), Value(csvValues(metric.RelationshipNames))),
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

func semanticFormPage(principal domain.ContextPrincipal, title, action string, csrfFieldProvider func() Node, fields ...Node) Node {
	nodes := []Node{csrfFieldProvider()}
	nodes = append(nodes, fields...)
	nodes = append(nodes, Div(Class("mt-4"), core.PrimaryButton("", Type("submit"), Text("Save"))))
	return core.AppPage(title, "semantic", principal,
		core.SectionSurface(
			core.SectionHeader(title, "Create or update semantic workspace records."),
			Form(Class("grid gap-3"), Method("post"), Action(action), Group(nodes)),
		),
	)
}

func sectionHeader(title, copy, href, action string) Node {
	return core.PageHeader("Build", title, copy, core.PrimaryLink(href, "", Text(action)))
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
