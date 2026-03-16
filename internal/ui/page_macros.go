package ui

import (
	"duck-demo/internal/domain"
	"strconv"

	. "maragu.dev/gomponents"
	data "maragu.dev/gomponents-datastar"
	. "maragu.dev/gomponents/html"
)

type macrosListRowData struct {
	Filter     string
	Name       string
	URL        string
	Type       string
	Visibility string
	Status     string
}

func macrosListPage(principal domain.ContextPrincipal, rows []macrosListRowData, page domain.PageRequest, total int64) Node {
	tableRows := make([]Node, 0, len(rows))
	for i := range rows {
		r := rows[i]
		tableRows = append(tableRows, Tr(data.Show(containsExpr(r.Filter)), Td(A(Href(r.URL), Text(r.Name))), Td(statusLabel(r.Type, "accent")), Td(Text(r.Visibility)), Td(statusLabel(r.Status, "attention"))))
	}
	tableNode := Node(emptyStateCard("No macros yet.", "New macro", "/ui/macros/new"))
	if len(tableRows) > 0 {
		tableNode = Div(Class(cardClass(tableWrapClass())), Table(Class(dataTableClass()), THead(Tr(Th(Text("Name")), Th(Text("Type")), Th(Text("Visibility")), Th(Text("Status")))), TBody(Group(tableRows))))
	}
	return appPage("Macros", "macros", principal, pageToolbar("/ui/macros/new", "New macro"), quickFilterCard("Filter by macro name or visibility"), tableNode, paginationCard("/ui/macros", page, total))
}

type macroRevisionRowData struct {
	Version   string
	Status    string
	CreatedBy string
	Created   string
}

type macroDetailPageData struct {
	Principal     domain.ContextPrincipal
	Name          string
	Type          string
	Visibility    string
	Status        string
	Owner         string
	EditURL       string
	DiffURL       string
	ImpactURL     string
	DeleteURL     string
	Definition    string
	Revisions     []macroRevisionRowData
	CSRFFieldFunc func() Node
}

func macroDetailPage(d macroDetailPageData) Node {
	revRows := make([]Node, 0, len(d.Revisions))
	for i := range d.Revisions {
		r := d.Revisions[i]
		revRows = append(revRows, Tr(Td(Text(r.Version)), Td(Text(r.Status)), Td(Text(r.CreatedBy)), Td(Text(r.Created))))
	}
	return appPage("Macro: "+d.Name, "macros", d.Principal, Div(Class(cardClass()), P(Text("Type: "+d.Type)), P(Text("Visibility: "+d.Visibility)), P(Text("Status: "+d.Status)), P(Text("Owner: "+d.Owner)), Div(Class(buttonRowClass()), A(Href(d.EditURL), Class(secondaryButtonClass()), Text("Edit")), A(Href(d.DiffURL), Class(secondaryButtonClass()), Text("Diff revisions")), A(Href(d.ImpactURL), Class(secondaryButtonClass()), Text("Impact")), Form(Method("post"), Action(d.DeleteURL), d.CSRFFieldFunc(), Button(Type("submit"), Class(dangerButtonClass()), Text("Delete"))))), Div(Class(cardClass()), H2(Text("Definition")), Pre(Text(d.Definition))), Div(Class(cardClass(tableWrapClass())), H2(Text("Revisions")), Table(Class(dataTableClass()), THead(Tr(Th(Text("Version")), Th(Text("Status")), Th(Text("Created by")), Th(Text("Created")))), TBody(Group(revRows)))))
}

func macrosNewPage(principal domain.ContextPrincipal, csrfFieldProvider func() Node) Node {
	return formPage(principal, "New Macro", "macros", "/ui/macros", csrfFieldProvider,
		Label(Text("Name")),
		Input(Name("name"), Required()),
		Label(Text("Type")),
		Select(Name("macro_type"), Option(Value("SCALAR"), Text("SCALAR")), Option(Value("TABLE"), Text("TABLE"))),
		Label(Text("Visibility")),
		Select(Name("visibility"), Option(Value("project"), Text("project")), Option(Value("catalog_global"), Text("catalog_global")), Option(Value("system"), Text("system"))),
		Label(Text("Description")),
		Textarea(Name("description")),
		Label(Text("Parameters (comma separated)")),
		Input(Name("parameters")),
		Label(Text("Body")),
		Textarea(Name("body"), Required()),
	)
}

func macrosEditPage(principal domain.ContextPrincipal, macroName string, macro *domain.Macro, csrfFieldProvider func() Node) Node {
	return formPage(principal, "Edit Macro", "macros", "/ui/macros/"+macroName+"/update", csrfFieldProvider,
		Label(Text("Description")),
		Textarea(Name("description"), Text(macro.Description)),
		Label(Text("Visibility")),
		Select(Name("visibility"), optionSelected("project", macro.Visibility), optionSelected("catalog_global", macro.Visibility), optionSelected("system", macro.Visibility)),
		Label(Text("Parameters (comma separated)")),
		Input(Name("parameters"), Value(csvValues(macro.Parameters))),
		Label(Text("Body")),
		Textarea(Name("body"), Text(macro.Body), Required()),
		Label(Text("Status")),
		Select(Name("status"), optionSelected("ACTIVE", macro.Status), optionSelected("DEPRECATED", macro.Status)),
	)
}

type macroRevisionOptionData struct {
	Value string
	Label string
}

type macroDiffPageData struct {
	Principal       domain.ContextPrincipal
	Name            string
	FromVersion     int
	ToVersion       int
	RevisionOptions []macroRevisionOptionData
	Diff            *domain.MacroRevisionDiff
	ImpactAdded     []macroImpactRowData
	ImpactRemoved   []macroImpactRowData
	ImpactUnchanged []macroImpactRowData
}

func macroDiffPage(d macroDiffPageData) Node {
	if d.Diff == nil {
		return appPage("Macro Diff: "+d.Name, "macros", d.Principal, emptyStateCard("At least two revisions are required to diff a macro.", "Back to macro", "/ui/macros/"+d.Name))
	}
	fromOptions := make([]Node, 0, len(d.RevisionOptions))
	toOptions := make([]Node, 0, len(d.RevisionOptions))
	for i := range d.RevisionOptions {
		option := d.RevisionOptions[i]
		fromOptions = append(fromOptions, optionSelected(option.Value, strconv.Itoa(d.FromVersion)))
		toOptions = append(toOptions, optionSelected(option.Value, strconv.Itoa(d.ToVersion)))
	}
	return appPage(
		"Macro Diff: "+d.Name,
		"macros",
		d.Principal,
		Div(
			Class(cardClass()),
			Form(
				Method("get"),
				Action("/ui/macros/"+d.Name+"/diff"),
				Label(Text("From revision")),
				Select(Name("from"), Group(fromOptions)),
				Label(Text("To revision")),
				Select(Name("to"), Group(toOptions)),
				Button(Type("submit"), Class(primaryButtonClass()), Text("Compare revisions")),
			),
			P(Text("Changed: "), statusLabel(boolLabel(d.Diff.Changed), boolTone(d.Diff.Changed))),
			P(Text("Parameters changed: "), statusLabel(boolLabel(d.Diff.ParametersChanged), boolTone(d.Diff.ParametersChanged))),
			P(Text("Body changed: "), statusLabel(boolLabel(d.Diff.BodyChanged), boolTone(d.Diff.BodyChanged))),
			P(Text("Description changed: "), statusLabel(boolLabel(d.Diff.DescriptionChanged), boolTone(d.Diff.DescriptionChanged))),
			P(Text("Status changed: "), statusLabel(boolLabel(d.Diff.StatusChanged), boolTone(d.Diff.StatusChanged))),
		),
		Div(Class(cardClass()), H2(Text("Parameters")), P(Text("From: "+stringsJoin(d.Diff.FromParameters))), P(Text("To: "+stringsJoin(d.Diff.ToParameters)))),
		Div(Class(cardClass()), H2(Text("Description")), P(Text("From: "+d.Diff.FromDescription)), P(Text("To: "+d.Diff.ToDescription))),
		Div(Class(cardClass()), H2(Text("Body")), H3(Text("From")), Pre(Text(d.Diff.FromBody)), H3(Text("To")), Pre(Text(d.Diff.ToBody))),
		macroImpactSection("Impact added", d.ImpactAdded, "No newly impacted models."),
		macroImpactSection("Impact removed", d.ImpactRemoved, "No removed impacted models."),
		macroImpactSection("Impact unchanged", d.ImpactUnchanged, "No unchanged impacted models."),
	)
}

type macroImpactRowData struct {
	ModelName string
	LastSeen  string
	URL       string
}

type macroImpactPageData struct {
	Principal domain.ContextPrincipal
	Name      string
	Rows      []macroImpactRowData
}

func macroImpactPage(d macroImpactPageData) Node {
	rows := make([]Node, 0, len(d.Rows))
	for i := range d.Rows {
		row := d.Rows[i]
		rows = append(rows, Tr(Td(A(Href(row.URL), Text(row.ModelName))), Td(Text(row.LastSeen))))
	}
	tableNode := Node(emptyStateCard("No impacted models found for this macro.", "Back to macro", "/ui/macros/"+d.Name))
	if len(rows) > 0 {
		tableNode = Div(Class(cardClass(tableWrapClass())), Table(Class(dataTableClass()), THead(Tr(Th(Text("Model")), Th(Text("Last seen")))), TBody(Group(rows))))
	}
	return appPage("Macro Impact: "+d.Name, "macros", d.Principal, tableNode)
}

func macroImpactSection(title string, rowsData []macroImpactRowData, emptyMessage string) Node {
	if len(rowsData) == 0 {
		return Div(Class(cardClass()), H2(Text(title)), P(Class(mutedClass()), Text(emptyMessage)))
	}
	rows := make([]Node, 0, len(rowsData))
	for i := range rowsData {
		row := rowsData[i]
		rows = append(rows, Tr(Td(A(Href(row.URL), Text(row.ModelName))), Td(Text(row.LastSeen))))
	}
	return Div(Class(cardClass(tableWrapClass())), H2(Text(title)), Table(Class(dataTableClass()), THead(Tr(Th(Text("Model")), Th(Text("Last seen")))), TBody(Group(rows))))
}
