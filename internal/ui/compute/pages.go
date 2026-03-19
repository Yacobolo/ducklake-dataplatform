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

func computeEndpointsListPage(principal domain.ContextPrincipal, rows []computeEndpointRowData, page domain.PageRequest, total int64) Node {
	table := Node(P(Class("text-xs text-[var(--fgColor-muted)]"), Text("No compute endpoints found.")))
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
		table = Div(Class("overflow-x-auto"), Table(Class("min-w-full text-left text-sm"),
			THead(Tr(Th(Text("Name")), Th(Text("Type")), Th(Text("Status")), Th(Text("URL")))),
			TBody(Group(tableRows)),
		))
	}
	return core.AppPage("Compute: Endpoints", "compute", principal,
		computeSectionNav("endpoints"),
		core.PageHeader("Operate", "Compute endpoints", "Create compute endpoints, inspect remote health, and route principals onto the right execution plane.", core.PrimaryLink("/ui/compute/endpoints/new", "", Text("New endpoint"))),
		core.SectionSurface(
			core.SectionHeader("Endpoint inventory", "Use the list below as the operational starting point for health checks, assignments, and endpoint-level edits."),
			table,
			P(Class("mt-4 text-sm text-[var(--fgColor-muted)]"), Text("Showing up to "+strconv.Itoa(page.MaxResults)+" endpoints. Total: "+strconv.FormatInt(total, 10))),
		),
	)
}

func computeEndpointDetailPage(principal domain.ContextPrincipal, item *domain.ComputeEndpoint, healthText string, assignments []computeAssignmentRowData, csrfFieldProvider func() Node) Node {
	assignTable := Node(P(Class("text-xs text-[var(--fgColor-muted)]"), Text("No assignments yet.")))
	if len(assignments) > 0 {
		rows := make([]Node, 0, len(assignments))
		for i := range assignments {
			a := assignments[i]
			rows = append(rows, Tr(
				Td(Text(a.PrincipalID)),
				Td(Text(a.PrincipalType)),
				Td(Text(strconv.FormatBool(a.IsDefault))),
				Td(Text(strconv.FormatBool(a.FallbackLocal))),
				Td(Class("text-right"), Form(Method("post"), Action("/ui/compute/endpoints/"+url.PathEscape(item.Name)+"/assignments/"+url.PathEscape(a.ID)+"/delete"), csrfFieldProvider(), core.DangerButton("small", Type("submit"), Text("Remove")))),
			))
		}
		assignTable = Div(Class("overflow-x-auto"), Table(Class("min-w-full text-left text-sm"),
			THead(Tr(Th(Text("Principal ID")), Th(Text("Type")), Th(Text("Default")), Th(Text("Fallback Local")), Th(Class("text-right"), Text("Actions")))),
			TBody(Group(rows)),
		))
	}

	return core.AppPage("Compute Endpoint: "+item.Name, "compute", principal,
		computeSectionNav("endpoints"),
		core.DetailShell(
			core.DetailHero(
				core.DetailHeroCopy(
					core.Kicker("Endpoint summary"),
					core.DetailTitle(item.Name),
					core.DetailDescription("Inspect endpoint health first, then manage the principal assignment rules that control where compute runs."),
					core.BadgeRow(
						core.Badge(item.Type, "accent"),
						core.Badge(item.Status, "success"),
					),
				),
				core.DetailHeroMeta(
					core.MetaItem("Owner", item.Owner),
					core.MetaItem("URL", item.URL),
					core.MetaItem("Health", healthText),
				),
			),
			core.DetailLayout(
				core.DetailMain(
					core.SectionSurface(
						core.SectionHeader("Assignments", "The current routing table for principals using this endpoint."),
						assignTable,
					),
				),
				core.DetailRail(
					core.SectionSurface(
						core.SectionHeader("Actions", "Endpoint-level actions stay separate from assignment management."),
						Div(Class("flex flex-wrap items-center gap-3 [&_form]:m-0 [&_form]:inline-flex"),
							core.SecondaryLink("/ui/compute/endpoints/"+url.PathEscape(item.Name)+"/edit", "", Text("Edit endpoint")),
							Form(Method("post"), Action("/ui/compute/endpoints/"+url.PathEscape(item.Name)+"/delete"), csrfFieldProvider(), core.DangerButton("", Type("submit"), Text("Delete endpoint"))),
						),
					),
					core.SectionSurface(
						core.SectionHeader("Assign principal", "Add a default or fallback routing rule for a user or group."),
						Form(Class("grid gap-3"), Method("post"), Action("/ui/compute/endpoints/"+url.PathEscape(item.Name)+"/assignments"),
							csrfFieldProvider(),
							Label(Text("Principal ID")),
							core.InputControl("", Name("principal_id"), Required()),
							Label(Text("Principal type")),
							core.SelectControl("", Name("principal_type"), Option(Value("user"), Text("user")), Option(Value("group"), Text("group"))),
							Label(Class("inline-flex items-center gap-2"), Input(Type("checkbox"), Name("is_default"), Class("h-4 w-4")), Span(Text("Default endpoint"))),
							Label(Class("inline-flex items-center gap-2"), Input(Type("checkbox"), Name("fallback_local"), Class("h-4 w-4")), Span(Text("Fallback to local compute"))),
							Div(Class("mt-2"), core.PrimaryButton("", Type("submit"), Text("Create assignment"))),
						),
					),
				),
			),
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
		core.SectionSurface(
			core.SectionHeader(title, "Create or update the endpoint metadata and runtime connection details."),
			Form(Class("grid gap-3"), Method("post"), Action(action),
				csrfFieldProvider(),
				Label(Text("Name")),
				core.InputControl("", Name("name"), Value(optionalEndpointName(item)), Required()),
				Label(Text("URL")),
				core.InputControl("", Name("url"), Value(optionalEndpointURL(item)), Required()),
				Label(Text("Type")),
				core.SelectControl("", Name("type"), optionSelected("LOCAL", endpointType), optionSelected("REMOTE", endpointType)),
				Label(Text("Size")),
				core.SelectControl("", Name("size"), optionSelected("SMALL", size), optionSelected("MEDIUM", size), optionSelected("LARGE", size)),
				Label(Text("Max memory (GB)")),
				core.InputControl("", Name("max_memory_gb"), Value(optionalEndpointMemory(item))),
				Label(Text("Auth token")),
				core.InputControl("", Name("auth_token"), Value(optionalEndpointToken(item))),
				Label(Text("Status")),
				core.InputControl("", Name("status"), Value(optionalEndpointStatus(item))),
				Div(Class("mt-4"), core.PrimaryButton("", Type("submit"), Text("Save"))),
			),
		),
	)
}

func computeSectionNav(active string) Node {
	return core.SectionTabs([]core.SectionTab{
		{Label: "Endpoints", Href: "/ui/compute/endpoints", Active: active == "endpoints"},
	})
}

func sectionHeader(title, copy, href, action string) Node {
	return core.PageHeader("Operate", title, copy, core.PrimaryLink(href, "", Text(action)))
}

func computeCard(title, copy, href string) Node {
	return Div(Class("rounded-xl border border-[var(--borderColor-default)] bg-[var(--bgColor-default)] p-4 shadow-xs"), H2(Class("mt-0 text-lg font-semibold"), Text(title)), P(Class("text-sm text-[var(--fgColor-muted)]"), Text(copy)), core.SecondaryLink(href, "", Text("Open")))
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
