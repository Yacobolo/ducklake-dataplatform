package compute

import (
	"net/url"
	"strconv"

	"duck-demo/internal/domain"
	"duck-demo/internal/ui/core"

	. "maragu.dev/gomponents"
	. "maragu.dev/gomponents/html"
)

type computeEndpointRowData struct {
	Name    string
	URL     string
	Type    string
	Status  string
	URLText string
}

type computeAssignmentRowData struct {
	ID            string
	PrincipalID   string
	PrincipalType string
	IsDefault     bool
	FallbackLocal bool
}

func computeHomePage(principal domain.ContextPrincipal) Node {
	return core.AppPage("Compute", "compute", principal,
		computeSectionNav(""),
		Div(Class("grid gap-3 md:grid-cols-2 xl:grid-cols-3"),
			computeCard("Endpoints", "Create compute endpoints, manage assignments, and inspect remote health.", "/ui/compute/endpoints"),
		),
	)
}

func computeEndpointsListPage(principal domain.ContextPrincipal, rows []computeEndpointRowData, page domain.PageRequest, total int64) Node {
	table := Node(P(Class(core.MutedClass()), Text("No compute endpoints found.")))
	if len(rows) > 0 {
		tableRows := make([]Node, 0, len(rows))
		for i := range rows {
			row := rows[i]
			tableRows = append(tableRows, Tr(
				Td(A(Href(row.URL), Class("font-medium text-[var(--fgColor-accent)]"), Text(row.Name))),
				Td(Text(row.Type)),
				Td(Text(row.Status)),
				Td(Text(row.URLText)),
			))
		}
		table = Div(Class(core.TableWrapClass()), Table(Class("min-w-full text-left text-sm"),
			THead(Tr(Th(Text("Name")), Th(Text("Type")), Th(Text("Status")), Th(Text("URL")))),
			TBody(Group(tableRows)),
		))
	}
	return core.AppPage("Compute: Endpoints", "compute", principal,
		computeSectionNav("endpoints"),
		sectionHeader("Compute endpoints", "Create compute endpoints and manage assignments.", "/ui/compute/endpoints/new", "New endpoint"),
		Div(Class(core.CardClass()),
			table,
			P(Class("mt-4 text-sm text-[var(--fgColor-muted)]"), Text("Showing up to "+strconv.Itoa(page.MaxResults)+" endpoints. Total: "+strconv.FormatInt(total, 10))),
		),
	)
}

func computeEndpointDetailPage(principal domain.ContextPrincipal, item *domain.ComputeEndpoint, healthText string, assignments []computeAssignmentRowData, csrfFieldProvider func() Node) Node {
	assignTable := Node(P(Class(core.MutedClass()), Text("No assignments yet.")))
	if len(assignments) > 0 {
		rows := make([]Node, 0, len(assignments))
		for i := range assignments {
			a := assignments[i]
			rows = append(rows, Tr(
				Td(Text(a.PrincipalID)),
				Td(Text(a.PrincipalType)),
				Td(Text(strconv.FormatBool(a.IsDefault))),
				Td(Text(strconv.FormatBool(a.FallbackLocal))),
				Td(Class("text-right"), Form(Method("post"), Action("/ui/compute/endpoints/"+url.PathEscape(item.Name)+"/assignments/"+url.PathEscape(a.ID)+"/delete"), csrfFieldProvider(), Button(Type("submit"), Class(core.DangerButtonClass("small")), Text("Remove")))),
			))
		}
		assignTable = Div(Class(core.TableWrapClass()), Table(Class("min-w-full text-left text-sm"),
			THead(Tr(Th(Text("Principal ID")), Th(Text("Type")), Th(Text("Default")), Th(Text("Fallback Local")), Th(Class("text-right"), Text("Actions")))),
			TBody(Group(rows)),
		))
	}

	return core.AppPage("Compute Endpoint: "+item.Name, "compute", principal,
		computeSectionNav("endpoints"),
		Div(Class(core.CardClass()),
			H2(Class("mt-0 text-lg font-semibold"), Text(item.Name)),
			P(Class("m-0 text-sm"), Strong(Text("Type: ")), Text(item.Type)),
			P(Class("m-0 text-sm"), Strong(Text("Status: ")), Text(item.Status)),
			P(Class("m-0 text-sm"), Strong(Text("URL: ")), Text(item.URL)),
			P(Class("m-0 text-sm"), Strong(Text("Health: ")), Text(healthText)),
			P(Class("m-0 text-sm"), Strong(Text("Owner: ")), Text(item.Owner)),
			Div(Class(core.ButtonRowClass()),
				A(Href("/ui/compute/endpoints/"+url.PathEscape(item.Name)+"/edit"), Class(core.SecondaryButtonClass()), Text("Edit")),
				Form(Method("post"), Action("/ui/compute/endpoints/"+url.PathEscape(item.Name)+"/delete"), csrfFieldProvider(), Button(Type("submit"), Class(core.DangerButtonClass()), Text("Delete"))),
			),
		),
		Div(Class(core.CardClass()),
			H3(Class("mt-0 text-lg font-semibold"), Text("Create assignment")),
			Form(Class("grid gap-3"), Method("post"), Action("/ui/compute/endpoints/"+url.PathEscape(item.Name)+"/assignments"),
				csrfFieldProvider(),
				Label(Text("Principal ID")),
				Input(Name("principal_id"), Required(), Class(core.FormControlClass())),
				Label(Text("Principal type")),
				Select(Name("principal_type"), Class(core.FormControlClass()), Option(Value("user"), Text("user")), Option(Value("group"), Text("group"))),
				Label(Class("inline-flex items-center gap-2"), Input(Type("checkbox"), Name("is_default"), Class("h-4 w-4")), Span(Text("Default endpoint"))),
				Label(Class("inline-flex items-center gap-2"), Input(Type("checkbox"), Name("fallback_local"), Class("h-4 w-4")), Span(Text("Fallback to local compute"))),
				Div(Class("mt-2"), Button(Type("submit"), Class(core.PrimaryButtonClass()), Text("Create assignment"))),
			),
		),
		Div(Class(core.CardClass()),
			H3(Class("mt-0 text-lg font-semibold"), Text("Assignments")),
			assignTable,
		),
	)
}

func computeEndpointFormPage(principal domain.ContextPrincipal, title, action string, item *domain.ComputeEndpoint, csrfFieldProvider func() Node) Node {
	endpointType := "REMOTE"
	size := "MEDIUM"
	if item != nil {
		if item.Type != "" {
			endpointType = item.Type
		}
		if item.Size != "" {
			size = item.Size
		}
	}
	return core.AppPage(title, "compute", principal,
		computeSectionNav("endpoints"),
		Div(Class(core.CardClass()),
			Form(Class("grid gap-3"), Method("post"), Action(action),
				csrfFieldProvider(),
				Label(Text("Name")),
				Input(Name("name"), Value(optionalEndpointName(item)), Required(), Class(core.FormControlClass())),
				Label(Text("URL")),
				Input(Name("url"), Value(optionalEndpointURL(item)), Required(), Class(core.FormControlClass())),
				Label(Text("Type")),
				Select(Name("type"), Class(core.FormControlClass()), optionSelected("LOCAL", endpointType), optionSelected("REMOTE", endpointType)),
				Label(Text("Size")),
				Select(Name("size"), Class(core.FormControlClass()), optionSelected("SMALL", size), optionSelected("MEDIUM", size), optionSelected("LARGE", size)),
				Label(Text("Max memory (GB)")),
				Input(Name("max_memory_gb"), Value(optionalEndpointMemory(item)), Class(core.FormControlClass())),
				Label(Text("Auth token")),
				Input(Name("auth_token"), Value(optionalEndpointToken(item)), Class(core.FormControlClass())),
				Label(Text("Status")),
				Input(Name("status"), Value(optionalEndpointStatus(item)), Class(core.FormControlClass())),
				Div(Class("mt-4"), Button(Type("submit"), Class(core.PrimaryButtonClass()), Text("Save"))),
			),
		),
	)
}

func computeSectionNav(active string) Node {
	return Div(Class(core.CardClass()), Div(Class("flex flex-wrap gap-2"), navButton("Endpoints", "/ui/compute/endpoints", active == "endpoints")))
}

func navButton(label, href string, active bool) Node {
	className := core.SecondaryButtonClass()
	if active {
		className = core.PrimaryButtonClass()
	}
	return A(Href(href), Class(className), Text(label))
}

func sectionHeader(title, copy, href, action string) Node {
	return Div(Class(core.CardClass()),
		Div(Class("flex flex-wrap items-start justify-between gap-3"),
			Div(H2(Class("m-0 text-xl font-semibold"), Text(title)), P(Class("m-0 text-sm text-[var(--fgColor-muted)]"), Text(copy))),
			A(Href(href), Class(core.PrimaryButtonClass()), Text(action)),
		),
	)
}

func computeCard(title, copy, href string) Node {
	return Div(Class(core.CardClass()), H2(Class("mt-0 text-lg font-semibold"), Text(title)), P(Class("text-sm text-[var(--fgColor-muted)]"), Text(copy)), A(Href(href), Class(core.SecondaryButtonClass()), Text("Open")))
}

func optionSelected(value, current string) Node {
	if value == current {
		return Option(Value(value), Selected(), Text(value))
	}
	return Option(Value(value), Text(value))
}

func optionalEndpointName(item *domain.ComputeEndpoint) string {
	if item == nil {
		return ""
	}
	return item.Name
}

func optionalEndpointURL(item *domain.ComputeEndpoint) string {
	if item == nil {
		return ""
	}
	return item.URL
}

func optionalEndpointMemory(item *domain.ComputeEndpoint) string {
	if item == nil || item.MaxMemoryGB == nil {
		return ""
	}
	return strconv.FormatInt(*item.MaxMemoryGB, 10)
}

func optionalEndpointToken(item *domain.ComputeEndpoint) string {
	if item == nil {
		return ""
	}
	return item.AuthToken
}

func optionalEndpointStatus(item *domain.ComputeEndpoint) string {
	if item == nil {
		return ""
	}
	return item.Status
}
