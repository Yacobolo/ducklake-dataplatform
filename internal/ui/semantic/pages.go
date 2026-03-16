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

func semanticHomePage(principal domain.ContextPrincipal) Node {
	return core.AppPage("Semantic", "semantic", principal,
		Div(Class("grid gap-3 md:grid-cols-2 xl:grid-cols-3"),
			semanticCard("Semantic Models", "Manage semantic models, metrics, and pre-aggregations.", "/ui/semantic/models"),
			semanticCard("Relationships", "Define join paths between semantic models.", "/ui/semantic/relationships"),
		),
	)
}

func semanticModelsListPage(principal domain.ContextPrincipal, rows []semanticModelRowData, page domain.PageRequest, total int64) Node {
	table := Node(P(Class(core.MutedClass()), Text("No semantic models defined.")))
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
		table = Div(Class(core.TableWrapClass()), Table(Class("min-w-full text-left text-sm"),
			THead(Tr(Th(Text("Model")), Th(Text("Base model")), Th(Text("Owner")), Th(Text("Updated")))),
			TBody(Group(tableRows)),
		))
	}
	return core.AppPage("Semantic Models", "semantic", principal,
		sectionHeader("Semantic models", "Manage semantic models and their runtime semantics.", "/ui/semantic/models/new", "New semantic model"),
		Div(Class(core.CardClass()),
			table,
			P(Class("mt-4 text-sm text-[var(--fgColor-muted)]"), Text("Showing up to "+strconv.Itoa(page.MaxResults)+" semantic models. Total: "+strconv.FormatInt(total, 10))),
		),
	)
}

func semanticModelsNewPage(principal domain.ContextPrincipal, csrfFieldProvider func() Node) Node {
	return semanticFormPage(principal, "New Semantic Model", "/ui/semantic/models", csrfFieldProvider,
		Label(Text("Project")),
		Input(Name("project_name"), Required(), Class(core.FormControlClass())),
		Label(Text("Name")),
		Input(Name("name"), Required(), Class(core.FormControlClass())),
		Label(Text("Description")),
		Textarea(Name("description"), Class(core.FormControlClass("min-h-24"))),
		Label(Text("Base model reference")),
		Input(Name("base_model_ref"), Required(), Class(core.FormControlClass())),
		Label(Text("Default time dimension")),
		Input(Name("default_time_dimension"), Class(core.FormControlClass())),
		Label(Text("Tags (comma separated)")),
		Input(Name("tags"), Class(core.FormControlClass())),
	)
}

func semanticModelDetailPage(d semanticModelDetailPageData) Node {
	metricRows := Node(P(Class(core.MutedClass()), Text("No metrics created yet.")))
	if len(d.Metrics) > 0 {
		rows := make([]Node, 0, len(d.Metrics))
		for i := range d.Metrics {
			metric := d.Metrics[i]
			rows = append(rows, Tr(
				Td(Text(metric.Name)),
				Td(Text(metric.Type)),
				Td(Text(metric.Expression)),
				Td(Text(metric.Status)),
				Td(Class("text-right"), Div(Class(core.ButtonRowClass("mt-0")),
					A(Href(metric.EditURL), Class(core.SecondaryButtonClass("small")), Text("Edit")),
					Form(Method("post"), Action(metric.DeleteURL), d.CSRFFieldProvider(), Button(Type("submit"), Class(core.DangerButtonClass("small")), Text("Delete"))),
				)),
			))
		}
		metricRows = Div(Class(core.TableWrapClass()), Table(Class("min-w-full text-left text-sm"),
			THead(Tr(Th(Text("Name")), Th(Text("Type")), Th(Text("Expression")), Th(Text("Status")), Th(Class("text-right"), Text("Actions")))),
			TBody(Group(rows)),
		))
	}

	preAggRows := Node(P(Class(core.MutedClass()), Text("No pre-aggregations created yet.")))
	if len(d.PreAggregations) > 0 {
		rows := make([]Node, 0, len(d.PreAggregations))
		for i := range d.PreAggregations {
			item := d.PreAggregations[i]
			rows = append(rows, Tr(
				Td(Text(item.Name)),
				Td(Text(item.Grain)),
				Td(Text(item.Target)),
				Td(Class("text-right"), Div(Class(core.ButtonRowClass("mt-0")),
					A(Href(item.EditURL), Class(core.SecondaryButtonClass("small")), Text("Edit")),
					Form(Method("post"), Action(item.DeleteURL), d.CSRFFieldProvider(), Button(Type("submit"), Class(core.DangerButtonClass("small")), Text("Delete"))),
				)),
			))
		}
		preAggRows = Div(Class(core.TableWrapClass()), Table(Class("min-w-full text-left text-sm"),
			THead(Tr(Th(Text("Name")), Th(Text("Grain")), Th(Text("Target")), Th(Class("text-right"), Text("Actions")))),
			TBody(Group(rows)),
		))
	}

	return core.AppPage("Semantic Model: "+d.ProjectName+"."+d.ModelName, "semantic", d.Principal,
		Div(Class(core.CardClass()),
			H2(Class("mt-0 text-lg font-semibold"), Text(d.ProjectName+"."+d.ModelName)),
			P(Class("m-0 text-sm"), Strong(Text("Base model: ")), Text(d.BaseModelRef)),
			P(Class("m-0 text-sm"), Strong(Text("Default time dimension: ")), Text(d.DefaultTimeDim)),
			P(Class("m-0 text-sm"), Strong(Text("Description: ")), Text(valueOrDash(d.Description))),
			Div(Class(core.ButtonRowClass()),
				A(Href(d.EditURL), Class(core.SecondaryButtonClass()), Text("Edit")),
				A(Href("/ui/semantic/relationships"), Class(core.SecondaryButtonClass()), Text("Relationships")),
				Form(Method("post"), Action(d.DeleteURL), d.CSRFFieldProvider(), Button(Type("submit"), Class(core.DangerButtonClass()), Text("Delete"))),
			),
		),
		Div(Class(core.CardClass()),
			H3(Class("mt-0 text-lg font-semibold"), Text("Create metric")),
			Form(Class("grid gap-3"), Method("post"), Action(d.MetricsCreateURL),
				d.CSRFFieldProvider(),
				Label(Text("Name")),
				Input(Name("name"), Required(), Class(core.FormControlClass())),
				Label(Text("Label")),
				Input(Name("label"), Class(core.FormControlClass())),
				Label(Text("Description")),
				Input(Name("description"), Class(core.FormControlClass())),
				Label(Text("Metric type")),
				Select(Name("metric_type"), Class(core.FormControlClass()), Option(Value("SUM"), Text("SUM")), Option(Value("COUNT"), Text("COUNT")), Option(Value("COUNT_DISTINCT"), Text("COUNT_DISTINCT")), Option(Value("AVG"), Text("AVG")), Option(Value("MIN"), Text("MIN")), Option(Value("MAX"), Text("MAX")), Option(Value("RATIO"), Text("RATIO"))),
				Label(Text("Expression mode")),
				Select(Name("expression_mode"), Class(core.FormControlClass()), Option(Value("DSL"), Text("DSL")), Option(Value("SQL"), Text("SQL"))),
				Label(Text("Expression")),
				Textarea(Name("expression"), Required(), Class(core.FormControlClass("min-h-24"))),
				Label(Text("Metric filter SQL")),
				Input(Name("filter_sql"), Class(core.FormControlClass())),
				Label(Text("Default time grain")),
				Input(Name("default_time_grain"), Class(core.FormControlClass())),
				Label(Text("Format")),
				Input(Name("format"), Class(core.FormControlClass())),
				Label(Text("Certification state")),
				Select(Name("certification_state"), Class(core.FormControlClass()), Option(Value("DRAFT"), Text("DRAFT")), Option(Value("CERTIFIED"), Text("CERTIFIED")), Option(Value("DEPRECATED"), Text("DEPRECATED"))),
				Div(Class("mt-2"), Button(Type("submit"), Class(core.PrimaryButtonClass()), Text("Create metric"))),
			),
		),
		Div(Class(core.CardClass()), H3(Class("mt-0 text-lg font-semibold"), Text("Metrics")), metricRows),
		Div(Class(core.CardClass()),
			H3(Class("mt-0 text-lg font-semibold"), Text("Create pre-aggregation")),
			Form(Class("grid gap-3"), Method("post"), Action(d.PreAggCreateURL),
				d.CSRFFieldProvider(),
				Label(Text("Name")),
				Input(Name("name"), Required(), Class(core.FormControlClass())),
				Label(Text("Metric set (comma separated)")),
				Input(Name("metric_set"), Class(core.FormControlClass())),
				Label(Text("Dimension set (comma separated)")),
				Input(Name("dimension_set"), Class(core.FormControlClass())),
				Label(Text("Grain")),
				Input(Name("grain"), Class(core.FormControlClass())),
				Label(Text("Target relation")),
				Input(Name("target_relation"), Required(), Class(core.FormControlClass())),
				Label(Text("Refresh policy")),
				Input(Name("refresh_policy"), Class(core.FormControlClass())),
				Div(Class("mt-2"), Button(Type("submit"), Class(core.PrimaryButtonClass()), Text("Create pre-aggregation"))),
			),
		),
		Div(Class(core.CardClass()), H3(Class("mt-0 text-lg font-semibold"), Text("Pre-aggregations")), preAggRows),
		semanticQueryCard(d.ProjectName, d.ModelName, d.QueryExplainURL, d.QueryRunURL, d.CSRFFieldProvider),
	)
}

func semanticRelationshipsPage(d semanticRelationshipsPageData) Node {
	modelOptions := make([]Node, 0, len(d.ModelOptions))
	for i := range d.ModelOptions {
		modelOptions = append(modelOptions, Option(Value(d.ModelOptions[i].Value), Text(d.ModelOptions[i].Label)))
	}

	table := Node(P(Class(core.MutedClass()), Text("No relationships defined.")))
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
				Td(Class("text-right"), Div(Class(core.ButtonRowClass("mt-0")),
					A(Href(row.EditURL), Class(core.SecondaryButtonClass("small")), Text("Edit")),
					Form(Method("post"), Action(row.DeleteURL), d.CSRFFieldProvider(), Button(Type("submit"), Class(core.DangerButtonClass("small")), Text("Delete"))),
				)),
			))
		}
		table = Div(Class(core.TableWrapClass()), Table(Class("min-w-full text-left text-sm"),
			THead(Tr(Th(Text("Name")), Th(Text("From")), Th(Text("To")), Th(Text("Type")), Th(Text("Join SQL")), Th(Class("text-right"), Text("Actions")))),
			TBody(Group(rows)),
		))
	}

	return core.AppPage("Semantic Relationships", "semantic", d.Principal,
		Div(Class(core.CardClass()),
			H2(Class("mt-0 text-lg font-semibold"), Text("Create relationship")),
			Form(Class("grid gap-3"), Method("post"), Action("/ui/semantic/relationships"),
				d.CSRFFieldProvider(),
				Label(Text("Name")),
				Input(Name("name"), Required(), Class(core.FormControlClass())),
				Label(Text("From model")),
				Select(Name("from_semantic_id"), Class(core.FormControlClass()), Group(modelOptions)),
				Label(Text("To model")),
				Select(Name("to_semantic_id"), Class(core.FormControlClass()), Group(modelOptions)),
				Label(Text("Relationship type")),
				Select(Name("relationship_type"), Class(core.FormControlClass()), Option(Value("ONE_TO_ONE"), Text("ONE_TO_ONE")), Option(Value("ONE_TO_MANY"), Text("ONE_TO_MANY")), Option(Value("MANY_TO_ONE"), Text("MANY_TO_ONE")), Option(Value("MANY_TO_MANY"), Text("MANY_TO_MANY"))),
				Label(Text("Join SQL")),
				Textarea(Name("join_sql"), Required(), Class(core.FormControlClass("min-h-24 font-mono text-xs"))),
				Label(Text("Cost")),
				Input(Name("cost"), Value("0"), Class(core.FormControlClass())),
				Label(Text("Max hops")),
				Input(Name("max_hops"), Value("0"), Class(core.FormControlClass())),
				Label(Class("inline-flex items-center gap-2"), Input(Type("checkbox"), Name("is_default"), Value("true"), Class("h-4 w-4")), Span(Text("Default relationship"))),
				Div(Class("mt-2"), Button(Type("submit"), Class(core.PrimaryButtonClass()), Text("Create relationship"))),
			),
		),
		Div(Class(core.CardClass()),
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
			Div(Class(core.CardClass()),
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
			Div(Class(core.CardClass()), H2(Class("mt-0 text-lg font-semibold"), Text("Join path")),
				Div(Class(core.TableWrapClass()), Table(Class("min-w-full text-left text-sm"),
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
			Div(Class(core.CardClass()), H2(Class("mt-0 text-lg font-semibold"), Text("Execution result")),
				Div(Class(core.TableWrapClass()), Table(Class("min-w-full text-left text-sm"),
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
	return Div(Class(core.CardClass()),
		H3(Class("mt-0 text-lg font-semibold"), Text("Metric query")),
		Form(Class("grid gap-3"), Method("post"), Action(explainURL),
			csrfFieldProvider(),
			Label(Text("Project")),
			Input(Name("project_name"), Value(projectName), Required(), Class(core.FormControlClass())),
			Label(Text("Semantic model")),
			Input(Name("semantic_model_name"), Value(semanticModelName), Required(), Class(core.FormControlClass())),
			Label(Text("Metrics (comma separated)")),
			Input(Name("metrics"), Value(metrics), Required(), Class(core.FormControlClass())),
			Label(Text("Dimensions (comma separated)")),
			Input(Name("dimensions"), Value(dimensions), Class(core.FormControlClass())),
			Label(Text("Filters (comma separated)")),
			Input(Name("filters"), Value(filters), Class(core.FormControlClass())),
			Label(Text("Order by (comma separated)")),
			Input(Name("order_by"), Value(orderBy), Class(core.FormControlClass())),
			Label(Text("Limit")),
			Input(Name("limit"), Value(limit), Class(core.FormControlClass())),
			Label(Text("Time grain")),
			Input(Name("time_grain"), Value(timeGrain), Class(core.FormControlClass())),
			Div(Class(core.ButtonRowClass()),
				Button(Type("submit"), Class(core.PrimaryButtonClass()), Text("Explain query")),
				Button(Type("submit"), FormAction(runURL), Class(core.SecondaryButtonClass()), Text("Run query")),
			),
		),
	)
}

func semanticModelsEditPage(principal domain.ContextPrincipal, projectName, semanticModelName string, item *domain.SemanticModel, csrfFieldProvider func() Node) Node {
	return semanticFormPage(principal, "Edit Semantic Model", "/ui/semantic/models/"+projectName+"/"+semanticModelName+"/update", csrfFieldProvider,
		Label(Text("Description")),
		Textarea(Name("description"), Class(core.FormControlClass("min-h-24")), Text(item.Description)),
		Label(Text("Base model reference")),
		Input(Name("base_model_ref"), Value(item.BaseModelRef), Required(), Class(core.FormControlClass())),
		Label(Text("Default time dimension")),
		Input(Name("default_time_dimension"), Value(item.DefaultTimeDimension), Class(core.FormControlClass())),
		Label(Text("Tags (comma separated)")),
		Input(Name("tags"), Value(csvValues(item.Tags)), Class(core.FormControlClass())),
	)
}

func semanticMetricEditPage(principal domain.ContextPrincipal, projectName, semanticModelName string, metric *domain.SemanticMetric, csrfFieldProvider func() Node) Node {
	return semanticFormPage(principal, "Edit Semantic Metric", "/ui/semantic/models/"+projectName+"/"+semanticModelName+"/metrics/"+metric.Name+"/update", csrfFieldProvider,
		Label(Text("Label")),
		Input(Name("label"), Value(metric.Label), Class(core.FormControlClass())),
		Label(Text("Description")),
		Textarea(Name("description"), Class(core.FormControlClass("min-h-24")), Text(metric.Description)),
		Label(Text("Metric type")),
		Select(Name("metric_type"), Class(core.FormControlClass()), optionSelected("SUM", metric.MetricType), optionSelected("COUNT", metric.MetricType), optionSelected("COUNT_DISTINCT", metric.MetricType), optionSelected("AVG", metric.MetricType), optionSelected("MIN", metric.MetricType), optionSelected("MAX", metric.MetricType), optionSelected("RATIO", metric.MetricType)),
		Label(Text("Expression mode")),
		Select(Name("expression_mode"), Class(core.FormControlClass()), optionSelected("DSL", metric.ExpressionMode), optionSelected("SQL", metric.ExpressionMode)),
		Label(Text("Expression")),
		Textarea(Name("expression"), Required(), Class(core.FormControlClass("min-h-24")), Text(metric.Expression)),
		Label(Text("Metric filter SQL")),
		Input(Name("filter_sql"), Value(metric.FilterSQL), Class(core.FormControlClass())),
		Label(Text("Default time grain")),
		Input(Name("default_time_grain"), Value(metric.DefaultTimeGrain), Class(core.FormControlClass())),
		Label(Text("Format")),
		Input(Name("format"), Value(metric.Format), Class(core.FormControlClass())),
		Label(Text("Certification state")),
		Select(Name("certification_state"), Class(core.FormControlClass()), optionSelected("DRAFT", metric.CertificationState), optionSelected("CERTIFIED", metric.CertificationState), optionSelected("DEPRECATED", metric.CertificationState)),
	)
}

func semanticPreAggregationEditPage(principal domain.ContextPrincipal, projectName, semanticModelName string, item *domain.SemanticPreAggregation, csrfFieldProvider func() Node) Node {
	return semanticFormPage(principal, "Edit Pre-Aggregation", "/ui/semantic/models/"+projectName+"/"+semanticModelName+"/pre-aggregations/"+item.Name+"/update", csrfFieldProvider,
		Label(Text("Metric set (comma separated)")),
		Input(Name("metric_set"), Value(csvValues(item.MetricSet)), Class(core.FormControlClass())),
		Label(Text("Dimension set (comma separated)")),
		Input(Name("dimension_set"), Value(csvValues(item.DimensionSet)), Class(core.FormControlClass())),
		Label(Text("Grain")),
		Input(Name("grain"), Value(item.Grain), Class(core.FormControlClass())),
		Label(Text("Target relation")),
		Input(Name("target_relation"), Value(item.TargetRelation), Required(), Class(core.FormControlClass())),
		Label(Text("Refresh policy")),
		Input(Name("refresh_policy"), Value(item.RefreshPolicy), Class(core.FormControlClass())),
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
		Select(Name("from_semantic_id"), Disabled(), Class(core.FormControlClass()), Group(fromOptions)),
		Label(Text("To model")),
		Select(Name("to_semantic_id"), Disabled(), Class(core.FormControlClass()), Group(toOptions)),
		Label(Text("Relationship type")),
		Select(Name("relationship_type"), Class(core.FormControlClass()), optionSelected("ONE_TO_ONE", item.RelationshipType), optionSelected("ONE_TO_MANY", item.RelationshipType), optionSelected("MANY_TO_ONE", item.RelationshipType), optionSelected("MANY_TO_MANY", item.RelationshipType)),
		Label(Text("Join SQL")),
		Textarea(Name("join_sql"), Required(), Class(core.FormControlClass("min-h-24 font-mono text-xs")), Text(item.JoinSQL)),
		Label(Text("Cost")),
		Input(Name("cost"), Value(strconv.Itoa(item.Cost)), Class(core.FormControlClass())),
		Label(Text("Max hops")),
		Input(Name("max_hops"), Value(strconv.Itoa(item.MaxHops)), Class(core.FormControlClass())),
		Label(Class("inline-flex items-center gap-2"), Input(Type("checkbox"), Name("is_default"), Value("true"), checkedIf(item.IsDefault), Class("h-4 w-4")), Span(Text("Default relationship"))),
	)
}

func semanticFormPage(principal domain.ContextPrincipal, title, action string, csrfFieldProvider func() Node, fields ...Node) Node {
	nodes := []Node{csrfFieldProvider()}
	nodes = append(nodes, fields...)
	nodes = append(nodes, Div(Class("mt-4"), Button(Type("submit"), Class(core.PrimaryButtonClass()), Text("Save"))))
	return core.AppPage(title, "semantic", principal, Div(Class(core.CardClass()), Form(Class("grid gap-3"), Method("post"), Action(action), Group(nodes))))
}

func semanticCard(title, copy, href string) Node {
	return Div(Class(core.CardClass()), H2(Class("mt-0 text-lg font-semibold"), Text(title)), P(Class("text-sm text-[var(--fgColor-muted)]"), Text(copy)), A(Href(href), Class(core.SecondaryButtonClass()), Text("Open")))
}

func sectionHeader(title, copy, href, action string) Node {
	return Div(Class(core.CardClass()),
		Div(Class("flex flex-wrap items-start justify-between gap-3"),
			Div(H2(Class("m-0 text-xl font-semibold"), Text(title)), P(Class("m-0 text-sm text-[var(--fgColor-muted)]"), Text(copy))),
			A(Href(href), Class(core.PrimaryButtonClass()), Text(action)),
		),
	)
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
