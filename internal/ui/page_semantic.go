package ui

import (
	"strconv"

	"duck-demo/internal/domain"
	semsvc "duck-demo/internal/service/semantic"

	. "maragu.dev/gomponents"
	data "maragu.dev/gomponents-datastar"
	. "maragu.dev/gomponents/html"
)

func semanticHomePage(principal domain.ContextPrincipal) Node {
	cards := []overviewCardData{
		{Title: "Semantic Models", Description: "Manage semantic models, metrics, and pre-aggregations.", Href: "/ui/semantic/models", LinkLabel: "Open models ->"},
		{Title: "Relationships", Description: "Define join paths between semantic models.", Href: "/ui/semantic/relationships", LinkLabel: "Open relationships ->"},
	}
	nodes := make([]Node, 0, len(cards))
	for i := range cards {
		card := cards[i]
		nodes = append(nodes, Div(Class(cardClass()), H2(Text(card.Title)), P(Text(card.Description)), A(Href(card.Href), Text(card.LinkLabel))))
	}
	return appPage(
		"Semantic",
		"semantic",
		principal,
		Div(Class("grid"), Group(nodes)),
	)
}

type semanticModelRowData struct {
	Name       string
	URL        string
	BaseModel  string
	Owner      string
	UpdatedAt  string
	FilterText string
}

type semanticModelsListPageData struct {
	Principal domain.ContextPrincipal
	Rows      []semanticModelRowData
	Page      domain.PageRequest
	Total     int64
}

func semanticModelsListPage(d semanticModelsListPageData) Node {
	rows := make([]Node, 0, len(d.Rows))
	for i := range d.Rows {
		row := d.Rows[i]
		rows = append(rows, Tr(
			data.Show(containsExpr(row.FilterText)),
			Td(A(Href(row.URL), Text(row.Name))),
			Td(Text(row.BaseModel)),
			Td(Text(row.Owner)),
			Td(Text(row.UpdatedAt)),
		))
	}
	tableNode := Node(emptyStateCard("No semantic models defined.", "New semantic model", "/ui/semantic/models/new"))
	if len(rows) > 0 {
		tableNode = Div(Class(cardClass("table-wrap")), Table(Class("data-table"), THead(Tr(Th(Text("Model")), Th(Text("Base model")), Th(Text("Owner")), Th(Text("Updated")))), TBody(Group(rows))))
	}
	return appPage("Semantic Models", "semantic", d.Principal, pageToolbar("/ui/semantic/models/new", "New semantic model"), pageToolbar("/ui/semantic/relationships", "Relationships"), quickFilterCard("Filter by semantic model or base model"), tableNode, paginationCard("/ui/semantic/models", d.Page, d.Total))
}

func semanticModelsNewPage(principal domain.ContextPrincipal, csrfFieldProvider func() Node) Node {
	return formPage(principal, "New Semantic Model", "semantic", "/ui/semantic/models", csrfFieldProvider,
		Label(Text("Project")),
		Input(Name("project_name"), Required()),
		Label(Text("Name")),
		Input(Name("name"), Required()),
		Label(Text("Description")),
		Textarea(Name("description")),
		Label(Text("Base model reference")),
		Input(Name("base_model_ref"), Required()),
		Label(Text("Default time dimension")),
		Input(Name("default_time_dimension")),
		Label(Text("Tags (comma separated)")),
		Input(Name("tags")),
	)
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

func semanticModelDetailPage(d semanticModelDetailPageData) Node {
	metricRows := make([]Node, 0, len(d.Metrics))
	for i := range d.Metrics {
		metric := d.Metrics[i]
		metricRows = append(metricRows, Tr(
			Td(Text(metric.Name)),
			Td(Text(metric.Type)),
			Td(Text(metric.Expression)),
			Td(statusLabel(metric.Status, "accent")),
			Td(Class("text-right"), actionMenu("Actions", actionMenuLink(metric.EditURL, "Edit metric"), actionMenuPost(metric.DeleteURL, "Delete metric", d.CSRFFieldProvider, true))),
		))
	}
	preAggRows := make([]Node, 0, len(d.PreAggregations))
	for i := range d.PreAggregations {
		preAgg := d.PreAggregations[i]
		preAggRows = append(preAggRows, Tr(
			Td(Text(preAgg.Name)),
			Td(Text(preAgg.Grain)),
			Td(Text(preAgg.Target)),
			Td(Class("text-right"), actionMenu("Actions", actionMenuLink(preAgg.EditURL, "Edit pre-aggregation"), actionMenuPost(preAgg.DeleteURL, "Delete pre-aggregation", d.CSRFFieldProvider, true))),
		))
	}
	return appPage(
		"Semantic Model: "+d.ProjectName+"."+d.ModelName,
		"semantic",
		d.Principal,
		Div(
			Class(cardClass()),
			P(Text("Base model: "+d.BaseModelRef)),
			P(Text("Default time dimension: "+d.DefaultTimeDim)),
			P(Text("Description: "+valueOrDash(d.Description))),
			Div(Class("BtnGroup"), A(Href(d.EditURL), Class(secondaryButtonClass()), Text("Edit")), A(Href("/ui/semantic/relationships"), Class(secondaryButtonClass()), Text("Relationships")), Form(Method("post"), Action(d.DeleteURL), d.CSRFFieldProvider(), Button(Type("submit"), Class("btn btn-danger"), Text("Delete")))),
		),
		Div(
			Class(cardClass()),
			H2(Text("Create metric")),
			Form(
				Method("post"),
				Action(d.MetricsCreateURL),
				d.CSRFFieldProvider(),
				Label(Text("Name")),
				Input(Name("name"), Required()),
				Label(Text("Description")),
				Input(Name("description")),
				Label(Text("Metric type")),
				Select(Name("metric_type"), Option(Value("SUM"), Text("SUM")), Option(Value("COUNT"), Text("COUNT")), Option(Value("COUNT_DISTINCT"), Text("COUNT_DISTINCT")), Option(Value("AVG"), Text("AVG")), Option(Value("MIN"), Text("MIN")), Option(Value("MAX"), Text("MAX")), Option(Value("RATIO"), Text("RATIO"))),
				Label(Text("Expression mode")),
				Select(Name("expression_mode"), Option(Value("DSL"), Text("DSL")), Option(Value("SQL"), Text("SQL"))),
				Label(Text("Expression")),
				Textarea(Name("expression"), Required()),
				Label(Text("Default time grain")),
				Input(Name("default_time_grain")),
				Label(Text("Format")),
				Input(Name("format")),
				Label(Text("Certification state")),
				Select(Name("certification_state"), Option(Value("DRAFT"), Text("DRAFT")), Option(Value("CERTIFIED"), Text("CERTIFIED")), Option(Value("DEPRECATED"), Text("DEPRECATED"))),
				Button(Type("submit"), Class(primaryButtonClass()), Text("Create metric")),
			),
		),
		Div(Class(cardClass("table-wrap")), H2(Text("Metrics")), Table(Class("data-table"), THead(Tr(Th(Text("Name")), Th(Text("Type")), Th(Text("Expression")), Th(Text("Status")), Th(Class("text-right"), Text("Actions")))), TBody(Group(metricRows)))),
		Div(
			Class(cardClass()),
			H2(Text("Create pre-aggregation")),
			Form(
				Method("post"),
				Action(d.PreAggCreateURL),
				d.CSRFFieldProvider(),
				Label(Text("Name")),
				Input(Name("name"), Required()),
				Label(Text("Metric set (comma separated)")),
				Input(Name("metric_set")),
				Label(Text("Dimension set (comma separated)")),
				Input(Name("dimension_set")),
				Label(Text("Grain")),
				Input(Name("grain")),
				Label(Text("Target relation")),
				Input(Name("target_relation"), Required()),
				Label(Text("Refresh policy")),
				Input(Name("refresh_policy")),
				Button(Type("submit"), Class(primaryButtonClass()), Text("Create pre-aggregation")),
			),
		),
		Div(Class(cardClass("table-wrap")), H2(Text("Pre-aggregations")), Table(Class("data-table"), THead(Tr(Th(Text("Name")), Th(Text("Grain")), Th(Text("Target")), Th(Class("text-right"), Text("Actions")))), TBody(Group(preAggRows)))),
		semanticQueryCard(d.ProjectName, d.ModelName, d.QueryExplainURL, d.QueryRunURL, d.CSRFFieldProvider, nil, nil),
	)
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

func semanticRelationshipsPage(d semanticRelationshipsPageData) Node {
	modelOptions := make([]Node, 0, len(d.ModelOptions))
	for i := range d.ModelOptions {
		modelOptions = append(modelOptions, Option(Value(d.ModelOptions[i].Value), Text(d.ModelOptions[i].Label)))
	}
	rows := make([]Node, 0, len(d.Rows))
	for i := range d.Rows {
		row := d.Rows[i]
		rows = append(rows, Tr(
			Td(Text(row.Name)),
			Td(Text(row.FromModel)),
			Td(Text(row.ToModel)),
			Td(Text(row.Type)),
			Td(Text(row.JoinSQL)),
			Td(Class("text-right"), actionMenu("Actions", actionMenuLink(row.EditURL, "Edit relationship"), actionMenuPost(row.DeleteURL, "Delete relationship", d.CSRFFieldProvider, true))),
		))
	}
	tableNode := Node(emptyStateCard("No relationships defined.", "Create semantic model", "/ui/semantic/models/new"))
	if len(rows) > 0 {
		tableNode = Div(Class(cardClass("table-wrap")), Table(Class("data-table"), THead(Tr(Th(Text("Name")), Th(Text("From")), Th(Text("To")), Th(Text("Type")), Th(Text("Join SQL")), Th(Class("text-right"), Text("Actions")))), TBody(Group(rows))))
	}
	return appPage(
		"Semantic Relationships",
		"semantic",
		d.Principal,
		Div(
			Class(cardClass()),
			H2(Text("Create relationship")),
			Form(
				Method("post"),
				Action("/ui/semantic/relationships"),
				d.CSRFFieldProvider(),
				Label(Text("Name")),
				Input(Name("name"), Required()),
				Label(Text("From model")),
				Select(Name("from_semantic_id"), Group(modelOptions)),
				Label(Text("To model")),
				Select(Name("to_semantic_id"), Group(modelOptions)),
				Label(Text("Relationship type")),
				Select(Name("relationship_type"), Option(Value("ONE_TO_ONE"), Text("ONE_TO_ONE")), Option(Value("ONE_TO_MANY"), Text("ONE_TO_MANY")), Option(Value("MANY_TO_ONE"), Text("MANY_TO_ONE")), Option(Value("MANY_TO_MANY"), Text("MANY_TO_MANY"))),
				Label(Text("Join SQL")),
				Textarea(Name("join_sql"), Required()),
				Label(Text("Cost")),
				Input(Name("cost"), Value("0")),
				Label(Text("Max hops")),
				Input(Name("max_hops"), Value("0")),
				Label(Text("Default relationship")),
				Input(Type("checkbox"), Name("is_default"), Value("true")),
				Button(Type("submit"), Class(primaryButtonClass()), Text("Create relationship")),
			),
		),
		tableNode,
		paginationCard("/ui/semantic/relationships", d.Page, d.Total),
	)
}

type semanticQueryResultPageData struct {
	Principal         domain.ContextPrincipal
	Request           semsvc.MetricQueryRequest
	Plan              *semsvc.MetricQueryPlan
	Result            *semsvc.MetricQueryResult
	CSRFFieldProvider func() Node
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
			Div(Class(cardClass()), H2(Text("Query plan")), P(Text("Base model: "+d.Plan.BaseModelName)), P(Text("Base relation: "+d.Plan.BaseRelation)), P(Text("Metrics: "+stringsJoin(d.Plan.Metrics))), P(Text("Dimensions: "+stringsJoin(d.Plan.Dimensions))), P(Text("Freshness status: "+d.Plan.FreshnessStatus)), P(Text("Freshness basis: "+stringsJoin(d.Plan.FreshnessBasis))), H3(Text("Generated SQL")), Pre(Text(d.Plan.GeneratedSQL))),
			Div(Class(cardClass("table-wrap")), H2(Text("Join path")), Table(Class("data-table"), THead(Tr(Th(Text("Relationship")), Th(Text("From")), Th(Text("To")), Th(Text("Join SQL")))), TBody(Group(joinRows)))),
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
			Div(Class(cardClass("table-wrap")), H2(Text("Execution result")), Table(Class("data-table"), THead(Tr(Group(headers))), TBody(Group(rows)))),
		})
	}
	return appPage("Semantic Query", "semantic", d.Principal, semanticQueryCard(d.Request.ProjectName, d.Request.SemanticModelName, "/ui/semantic/query/explain", "/ui/semantic/query/run", d.CSRFFieldProvider, &d.Request, d.Result), resultNode)
}

func semanticQueryCard(projectName, semanticModelName, explainURL, runURL string, csrfFieldProvider func() Node, req *semsvc.MetricQueryRequest, _ *semsvc.MetricQueryResult) Node {
	metrics := ""
	dimensions := ""
	filters := ""
	orderBy := ""
	limit := ""
	if req != nil {
		metrics = csvValues(req.Metrics)
		dimensions = csvValues(req.Dimensions)
		filters = csvValues(req.Filters)
		orderBy = csvValues(req.OrderBy)
		if req.Limit != nil {
			limit = strconv.Itoa(*req.Limit)
		}
	}
	return Div(
		Class(cardClass()),
		H2(Text("Metric query")),
		Form(
			Method("post"),
			Action(explainURL),
			csrfFieldProvider(),
			Label(Text("Project")),
			Input(Name("project_name"), Value(projectName), Required()),
			Label(Text("Semantic model")),
			Input(Name("semantic_model_name"), Value(semanticModelName), Required()),
			Label(Text("Metrics (comma separated)")),
			Input(Name("metrics"), Value(metrics), Required()),
			Label(Text("Dimensions (comma separated)")),
			Input(Name("dimensions"), Value(dimensions)),
			Label(Text("Filters (comma separated)")),
			Input(Name("filters"), Value(filters)),
			Label(Text("Order by (comma separated)")),
			Input(Name("order_by"), Value(orderBy)),
			Label(Text("Limit")),
			Input(Name("limit"), Value(limit)),
			Div(Class("BtnGroup"),
				Button(Type("submit"), Class(primaryButtonClass()), Text("Explain query")),
				Button(Type("submit"), FormAction(runURL), Class(secondaryButtonClass()), Text("Run query")),
			),
		),
	)
}

func semanticModelsEditPage(principal domain.ContextPrincipal, projectName, semanticModelName string, item *domain.SemanticModel, csrfFieldProvider func() Node) Node {
	return formPage(principal, "Edit Semantic Model", "semantic", "/ui/semantic/models/"+projectName+"/"+semanticModelName+"/update", csrfFieldProvider,
		Label(Text("Description")),
		Textarea(Name("description"), Text(item.Description)),
		Label(Text("Base model reference")),
		Input(Name("base_model_ref"), Value(item.BaseModelRef), Required()),
		Label(Text("Default time dimension")),
		Input(Name("default_time_dimension"), Value(item.DefaultTimeDimension)),
		Label(Text("Tags (comma separated)")),
		Input(Name("tags"), Value(csvValues(item.Tags))),
	)
}

func semanticMetricEditPage(principal domain.ContextPrincipal, projectName, semanticModelName string, metric *domain.SemanticMetric, csrfFieldProvider func() Node) Node {
	return formPage(principal, "Edit Semantic Metric", "semantic", "/ui/semantic/models/"+projectName+"/"+semanticModelName+"/metrics/"+metric.Name+"/update", csrfFieldProvider,
		Label(Text("Description")),
		Textarea(Name("description"), Text(metric.Description)),
		Label(Text("Metric type")),
		Select(Name("metric_type"), optionSelected("SUM", metric.MetricType), optionSelected("COUNT", metric.MetricType), optionSelected("COUNT_DISTINCT", metric.MetricType), optionSelected("AVG", metric.MetricType), optionSelected("MIN", metric.MetricType), optionSelected("MAX", metric.MetricType), optionSelected("RATIO", metric.MetricType)),
		Label(Text("Expression mode")),
		Select(Name("expression_mode"), optionSelected("DSL", metric.ExpressionMode), optionSelected("SQL", metric.ExpressionMode)),
		Label(Text("Expression")),
		Textarea(Name("expression"), Text(metric.Expression), Required()),
		Label(Text("Default time grain")),
		Input(Name("default_time_grain"), Value(metric.DefaultTimeGrain)),
		Label(Text("Format")),
		Input(Name("format"), Value(metric.Format)),
		Label(Text("Certification state")),
		Select(Name("certification_state"), optionSelected("DRAFT", metric.CertificationState), optionSelected("CERTIFIED", metric.CertificationState), optionSelected("DEPRECATED", metric.CertificationState)),
	)
}

func semanticPreAggregationEditPage(principal domain.ContextPrincipal, projectName, semanticModelName string, item *domain.SemanticPreAggregation, csrfFieldProvider func() Node) Node {
	return formPage(principal, "Edit Pre-Aggregation", "semantic", "/ui/semantic/models/"+projectName+"/"+semanticModelName+"/pre-aggregations/"+item.Name+"/update", csrfFieldProvider,
		Label(Text("Metric set (comma separated)")),
		Input(Name("metric_set"), Value(csvValues(item.MetricSet))),
		Label(Text("Dimension set (comma separated)")),
		Input(Name("dimension_set"), Value(csvValues(item.DimensionSet))),
		Label(Text("Grain")),
		Input(Name("grain"), Value(item.Grain)),
		Label(Text("Target relation")),
		Input(Name("target_relation"), Value(item.TargetRelation), Required()),
		Label(Text("Refresh policy")),
		Input(Name("refresh_policy"), Value(item.RefreshPolicy)),
	)
}

func semanticRelationshipEditPage(principal domain.ContextPrincipal, item *domain.SemanticRelationship, modelOptions []semanticOptionData, csrfFieldProvider func() Node) Node {
	fromOptions := make([]Node, 0, len(modelOptions))
	toOptions := make([]Node, 0, len(modelOptions))
	for i := range modelOptions {
		opt := modelOptions[i]
		fromOptions = append(fromOptions, Option(Value(opt.Value), Text(opt.Label), selectedIf(opt.Value == item.FromSemanticID)))
		toOptions = append(toOptions, Option(Value(opt.Value), Text(opt.Label), selectedIf(opt.Value == item.ToSemanticID)))
	}
	return formPage(principal, "Edit Relationship", "semantic", "/ui/semantic/relationships/"+item.Name+"/update", csrfFieldProvider,
		Label(Text("From model")),
		Select(Name("from_semantic_id"), Disabled(), Group(fromOptions)),
		Label(Text("To model")),
		Select(Name("to_semantic_id"), Disabled(), Group(toOptions)),
		Label(Text("Relationship type")),
		Select(Name("relationship_type"), optionSelected("ONE_TO_ONE", item.RelationshipType), optionSelected("ONE_TO_MANY", item.RelationshipType), optionSelected("MANY_TO_ONE", item.RelationshipType), optionSelected("MANY_TO_MANY", item.RelationshipType)),
		Label(Text("Join SQL")),
		Textarea(Name("join_sql"), Text(item.JoinSQL), Required()),
		Label(Text("Cost")),
		Input(Name("cost"), Value(strconv.Itoa(item.Cost))),
		Label(Text("Max hops")),
		Input(Name("max_hops"), Value(strconv.Itoa(item.MaxHops))),
		Label(Text("Default relationship")),
		Input(Type("checkbox"), Name("is_default"), Value("true"), checkedIf(item.IsDefault)),
	)
}

func selectedIf(v bool) Node {
	if v {
		return Selected()
	}
	return nil
}
