package macros

import (
	"strconv"
	"strings"

	"duck-demo/internal/domain"
	"duck-demo/internal/ui/core"

	. "maragu.dev/gomponents"
	. "maragu.dev/gomponents/html"
)

type macrosListRowData struct {
	Name       string
	URL        string
	Type       string
	Visibility string
	Status     string
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

type macroImpactPageData struct {
	Principal domain.ContextPrincipal
	Name      string
	Rows      []macroImpactRowData
}

func macrosListPage(principal domain.ContextPrincipal, rows []macrosListRowData, page domain.PageRequest, total int64) Node {
	table := Node(P(Class(core.MutedClass()), Text("No macros yet.")))
	if len(rows) > 0 {
		tableRows := make([]Node, 0, len(rows))
		for i := range rows {
			row := rows[i]
			tableRows = append(tableRows, Tr(
				Td(A(Href(row.URL), Class("font-medium text-[var(--fgColor-accent)]"), Text(row.Name))),
				Td(statusPill(row.Type, "accent")),
				Td(Text(row.Visibility)),
				Td(statusPill(row.Status, "neutral")),
			))
		}
		table = Div(Class(core.TableWrapClass()),
			Table(Class("min-w-full text-left text-sm"),
				THead(Tr(Th(Text("Name")), Th(Text("Type")), Th(Text("Visibility")), Th(Text("Status")))),
				TBody(Group(tableRows)),
			),
		)
	}

	return core.AppPage("Macros", "macros", principal,
		Div(Class(core.CardClass()),
			Div(Class("mb-4 flex flex-wrap items-center justify-between gap-3"),
				Div(H2(Class("m-0 text-xl font-semibold"), Text("Macros")), P(Class("m-0 text-sm text-[var(--fgColor-muted)]"), Text("Create and manage reusable SQL and transformation helpers."))),
				A(Href("/ui/macros/new"), Class(core.PrimaryButtonClass()), Text("New macro")),
			),
			table,
			P(Class("mt-4 text-sm text-[var(--fgColor-muted)]"), Text("Showing up to "+strconv.Itoa(page.MaxResults)+" macros. Total: "+strconv.FormatInt(total, 10))),
		),
	)
}

func macroDetailPage(d macroDetailPageData) Node {
	revisions := Node(P(Class(core.MutedClass()), Text("No revisions yet.")))
	if len(d.Revisions) > 0 {
		rows := make([]Node, 0, len(d.Revisions))
		for i := range d.Revisions {
			rev := d.Revisions[i]
			rows = append(rows, Tr(
				Td(Text(rev.Version)),
				Td(Text(rev.Status)),
				Td(Text(emptyDash(rev.CreatedBy))),
				Td(Text(rev.Created)),
			))
		}
		revisions = Div(Class(core.TableWrapClass()),
			Table(Class("min-w-full text-left text-sm"),
				THead(Tr(Th(Text("Version")), Th(Text("Status")), Th(Text("Created by")), Th(Text("Created")))),
				TBody(Group(rows)),
			),
		)
	}

	return core.AppPage("Macro: "+d.Name, "macros", d.Principal,
		Div(Class(core.CardClass()),
			Div(Class("flex flex-wrap items-start justify-between gap-3"),
				Div(
					H2(Class("m-0 text-xl font-semibold"), Text(d.Name)),
					P(Class("m-0 text-sm text-[var(--fgColor-muted)]"), Text("Owner: "+emptyDash(d.Owner))),
				),
				Div(Class(core.ButtonRowClass("mt-0")),
					A(Href(d.EditURL), Class(core.SecondaryButtonClass()), Text("Edit")),
					A(Href(d.DiffURL), Class(core.SecondaryButtonClass()), Text("Diff revisions")),
					A(Href(d.ImpactURL), Class(core.SecondaryButtonClass()), Text("Impact")),
					Form(Method("post"), Action(d.DeleteURL), d.CSRFFieldFunc(), Button(Type("submit"), Class(core.DangerButtonClass()), Text("Delete"))),
				),
			),
			Dl(Class("mt-4 grid gap-3 sm:grid-cols-3"),
				metaRow("Type", d.Type),
				metaRow("Visibility", d.Visibility),
				metaRow("Status", d.Status),
			),
		),
		Div(Class(core.CardClass()),
			H3(Class("mt-0 text-lg font-semibold"), Text("Definition")),
			Pre(Class("overflow-x-auto rounded-lg border border-[var(--borderColor-muted)] bg-[var(--bgColor-muted)] p-3 text-sm"), Text(d.Definition)),
		),
		Div(Class(core.CardClass()),
			H3(Class("mt-0 text-lg font-semibold"), Text("Revisions")),
			revisions,
		),
	)
}

func macrosNewPage(principal domain.ContextPrincipal, csrfFieldProvider func() Node) Node {
	return macroFormPage(principal, "New Macro", "/ui/macros", csrfFieldProvider,
		Label(Text("Name")),
		Input(Name("name"), Required(), Class(core.FormControlClass())),
		Label(Text("Type")),
		Select(Name("macro_type"), Class(core.FormControlClass()),
			Option(Value("SCALAR"), Text("SCALAR")),
			Option(Value("TABLE"), Text("TABLE")),
		),
		Label(Text("Visibility")),
		Select(Name("visibility"), Class(core.FormControlClass()),
			Option(Value("project"), Text("project")),
			Option(Value("catalog_global"), Text("catalog_global")),
			Option(Value("system"), Text("system")),
		),
		Label(Text("Description")),
		Textarea(Name("description"), Class(core.FormControlClass("min-h-24"))),
		Label(Text("Parameters (comma separated)")),
		Input(Name("parameters"), Class(core.FormControlClass())),
		Label(Text("Body")),
		Textarea(Name("body"), Required(), Class(core.FormControlClass("min-h-40 font-mono text-xs"))),
	)
}

func macrosEditPage(principal domain.ContextPrincipal, macroName string, macro *domain.Macro, csrfFieldProvider func() Node) Node {
	return macroFormPage(principal, "Edit Macro", "/ui/macros/"+macroName+"/update", csrfFieldProvider,
		Label(Text("Description")),
		Textarea(Name("description"), Class(core.FormControlClass("min-h-24")), Text(macro.Description)),
		Label(Text("Visibility")),
		Select(Name("visibility"), Class(core.FormControlClass()),
			optionSelected("project", macro.Visibility),
			optionSelected("catalog_global", macro.Visibility),
			optionSelected("system", macro.Visibility),
		),
		Label(Text("Parameters (comma separated)")),
		Input(Name("parameters"), Value(csvValues(macro.Parameters)), Class(core.FormControlClass())),
		Label(Text("Body")),
		Textarea(Name("body"), Required(), Class(core.FormControlClass("min-h-40 font-mono text-xs")), Text(macro.Body)),
		Label(Text("Status")),
		Select(Name("status"), Class(core.FormControlClass()),
			optionSelected("ACTIVE", macro.Status),
			optionSelected("DEPRECATED", macro.Status),
		),
	)
}

func macroFormPage(principal domain.ContextPrincipal, title, action string, csrfFieldProvider func() Node, fields ...Node) Node {
	nodes := []Node{csrfFieldProvider()}
	nodes = append(nodes, fields...)
	nodes = append(nodes, Div(Class("mt-4"), Button(Type("submit"), Class(core.PrimaryButtonClass()), Text("Save"))))

	return core.AppPage(title, "macros", principal,
		Div(Class(core.CardClass()),
			Form(Class("grid gap-3"), Method("post"), Action(action), Group(nodes)),
		),
	)
}

func macroDiffPage(d macroDiffPageData) Node {
	if d.Diff == nil {
		return core.AppPage("Macro Diff: "+d.Name, "macros", d.Principal,
			Div(Class(core.CardClass()),
				H2(Class("mt-0 text-xl font-semibold"), Text("Macro diff")),
				P(Class(core.MutedClass()), Text("At least two revisions are required to diff a macro.")),
				A(Href("/ui/macros/"+d.Name), Class(core.SecondaryButtonClass()), Text("Back to macro")),
			),
		)
	}

	fromOptions := make([]Node, 0, len(d.RevisionOptions))
	toOptions := make([]Node, 0, len(d.RevisionOptions))
	for i := range d.RevisionOptions {
		option := d.RevisionOptions[i]
		fromOptions = append(fromOptions, optionSelected(option.Value, strconv.Itoa(d.FromVersion)))
		toOptions = append(toOptions, optionSelected(option.Value, strconv.Itoa(d.ToVersion)))
	}

	return core.AppPage("Macro Diff: "+d.Name, "macros", d.Principal,
		Div(Class(core.CardClass()),
			H2(Class("mt-0 text-xl font-semibold"), Text("Compare revisions")),
			Form(Class("grid gap-3 md:grid-cols-2"), Method("get"), Action("/ui/macros/"+d.Name+"/diff"),
				Div(Label(Text("From revision")), Select(Name("from"), Class(core.FormControlClass()), Group(fromOptions))),
				Div(Label(Text("To revision")), Select(Name("to"), Class(core.FormControlClass()), Group(toOptions))),
				Div(Class("md:col-span-2"), Button(Type("submit"), Class(core.PrimaryButtonClass()), Text("Compare revisions"))),
			),
			Dl(Class("mt-4 grid gap-3 sm:grid-cols-2 xl:grid-cols-5"),
				metaRow("Changed", boolLabel(d.Diff.Changed)),
				metaRow("Parameters changed", boolLabel(d.Diff.ParametersChanged)),
				metaRow("Body changed", boolLabel(d.Diff.BodyChanged)),
				metaRow("Description changed", boolLabel(d.Diff.DescriptionChanged)),
				metaRow("Status changed", boolLabel(d.Diff.StatusChanged)),
			),
		),
		Div(Class(core.CardClass()),
			H3(Class("mt-0 text-lg font-semibold"), Text("Parameters")),
			P(Text("From: "+stringsJoin(d.Diff.FromParameters))),
			P(Text("To: "+stringsJoin(d.Diff.ToParameters))),
		),
		Div(Class(core.CardClass()),
			H3(Class("mt-0 text-lg font-semibold"), Text("Description")),
			P(Text("From: "+emptyDash(d.Diff.FromDescription))),
			P(Text("To: "+emptyDash(d.Diff.ToDescription))),
		),
		Div(Class(core.CardClass()),
			H3(Class("mt-0 text-lg font-semibold"), Text("Body")),
			H4(Class("mb-2 text-sm font-semibold uppercase tracking-wide text-[var(--fgColor-muted)]"), Text("From")),
			Pre(Class("overflow-x-auto rounded-lg border border-[var(--borderColor-muted)] bg-[var(--bgColor-muted)] p-3 text-sm"), Text(d.Diff.FromBody)),
			H4(Class("mb-2 text-sm font-semibold uppercase tracking-wide text-[var(--fgColor-muted)]"), Text("To")),
			Pre(Class("overflow-x-auto rounded-lg border border-[var(--borderColor-muted)] bg-[var(--bgColor-muted)] p-3 text-sm"), Text(d.Diff.ToBody)),
		),
		macroImpactSection("Impact added", d.ImpactAdded, "No newly impacted models."),
		macroImpactSection("Impact removed", d.ImpactRemoved, "No removed impacted models."),
		macroImpactSection("Impact unchanged", d.ImpactUnchanged, "No unchanged impacted models."),
	)
}

func macroImpactPage(d macroImpactPageData) Node {
	return core.AppPage("Macro Impact: "+d.Name, "macros", d.Principal,
		macroImpactSection("Impacted models", d.Rows, "No impacted models found for this macro."),
	)
}

func macroImpactSection(title string, rowsData []macroImpactRowData, emptyMessage string) Node {
	if len(rowsData) == 0 {
		return Div(Class(core.CardClass()),
			H2(Class("mt-0 text-lg font-semibold"), Text(title)),
			P(Class(core.MutedClass()), Text(emptyMessage)),
		)
	}
	rows := make([]Node, 0, len(rowsData))
	for i := range rowsData {
		row := rowsData[i]
		rows = append(rows, Tr(Td(A(Href(row.URL), Class("font-medium text-[var(--fgColor-accent)]"), Text(row.ModelName))), Td(Text(row.LastSeen))))
	}
	return Div(Class(core.CardClass()),
		H2(Class("mt-0 text-lg font-semibold"), Text(title)),
		Div(Class(core.TableWrapClass()),
			Table(Class("min-w-full text-left text-sm"),
				THead(Tr(Th(Text("Model")), Th(Text("Last seen")))),
				TBody(Group(rows)),
			),
		),
	)
}

func optionSelected(value, current string) Node {
	if value == current {
		return Option(Value(value), Selected(), Text(value))
	}
	return Option(Value(value), Text(value))
}

func metaRow(label, value string) Node {
	return Div(
		Class("rounded-lg border border-[var(--borderColor-muted)] bg-[var(--bgColor-muted)] p-3"),
		Dt(Class("text-xs font-medium uppercase tracking-wide text-[var(--fgColor-muted)]"), Text(label)),
		Dd(Class("mt-1 ml-0 text-sm text-[var(--fgColor-default)]"), Text(emptyDash(value))),
	)
}

func statusPill(text, tone string) Node {
	className := "inline-flex items-center rounded-full px-2.5 py-1 text-xs font-medium"
	switch tone {
	case "accent":
		className += " bg-[var(--bgColor-accent-muted)] text-[var(--fgColor-accent)]"
	default:
		className += " bg-[var(--bgColor-muted)] text-[var(--fgColor-default)]"
	}
	return Span(Class(className), Text(text))
}

func emptyDash(v string) string {
	if v == "" {
		return "-"
	}
	return v
}

func boolLabel(value bool) string {
	if value {
		return "Yes"
	}
	return "No"
}

func stringsJoin(values []string) string {
	if len(values) == 0 {
		return "-"
	}
	return strings.Join(values, ", ")
}
