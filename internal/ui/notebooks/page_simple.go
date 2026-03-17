package notebooks

import (
	"strconv"

	"duck-demo/internal/domain"
	"duck-demo/internal/ui/core"

	. "maragu.dev/gomponents"
	. "maragu.dev/gomponents/html"
)

type notebookListRow struct {
	Name    string
	URL     string
	Owner   string
	Updated string
}

func notebooksListPage(principal domain.ContextPrincipal, rows []notebookListRow, page domain.PageRequest, total int64) Node {
	tableRows := make([]Node, 0, len(rows))
	for i := range rows {
		row := rows[i]
		tableRows = append(tableRows, Tr(Td(A(Href(row.URL), Text(row.Name))), Td(Text(row.Owner)), Td(Text(row.Updated))))
	}

	body := []Node{
		pageToolbar("Workspaces", "Create and manage notebooks.", "/ui/notebooks/new", "New notebook"),
		pageToolbar("Sources", "Manage notebook Git sources.", "/ui/notebooks/git-repos", "Git repos"),
	}
	if len(tableRows) == 0 {
		body = append(body, emptyStateCard("No notebooks yet.", "Create your first notebook", "/ui/notebooks/new"))
	} else {
		body = append(body, tableCard([]string{"Name", "Owner", "Updated"}, tableRows))
	}
	body = append(body, paginationCard("/ui/notebooks", page, total))
	return core.AppPage("Notebooks", "notebooks", principal, body...)
}

type notebookJobRow struct {
	ID      string
	URL     string
	State   string
	Updated string
}

type notebookJobsListPageData struct {
	Principal  domain.ContextPrincipal
	NotebookID string
	Rows       []notebookJobRow
	Page       domain.PageRequest
	Total      int64
}

func notebookJobsListPage(d notebookJobsListPageData) Node {
	rows := make([]Node, 0, len(d.Rows))
	for i := range d.Rows {
		row := d.Rows[i]
		rows = append(rows, Tr(Td(A(Href(row.URL), Text(row.ID))), Td(Text(row.State)), Td(Text(row.Updated))))
	}
	body := []Node{pageToolbar("Notebook Jobs", "Async runs for this notebook.", "/ui/notebooks/"+d.NotebookID, "Back to notebook")}
	if len(rows) == 0 {
		body = append(body, emptyStateCard("No notebook jobs found.", "Back to notebook", "/ui/notebooks/"+d.NotebookID))
	} else {
		body = append(body, tableCard([]string{"Job ID", "State", "Updated"}, rows))
	}
	body = append(body, paginationCard("/ui/notebooks/"+d.NotebookID+"/jobs", d.Page, d.Total))
	return core.AppPage("Notebook Jobs", "notebooks", d.Principal, body...)
}

type notebookJobDetailPageData struct {
	Principal  domain.ContextPrincipal
	NotebookID string
	JobID      string
	State      string
	Result     string
	ErrorText  string
	CreatedAt  string
	UpdatedAt  string
}

func notebookJobDetailPage(d notebookJobDetailPageData) Node {
	return core.AppPage(
		"Notebook Job: "+d.JobID,
		"notebooks",
		d.Principal,
		pageToolbar("Notebook Job", "Inspect a notebook run.", "/ui/notebooks/"+d.NotebookID+"/jobs", "Back to jobs"),
		Div(Class("rounded-xl border border-border bg-background p-4 shadow-xs"),
			P(Text("State: "+d.State)),
			P(Text("Created: "+d.CreatedAt)),
			P(Text("Updated: "+d.UpdatedAt)),
			P(Text("Error: "+d.ErrorText)),
		),
		Div(Class("rounded-xl border border-border bg-background p-4 shadow-xs"),
			H2(Class("m-0 text-lg font-semibold"), Text("Result payload")),
			Pre(Class("mt-3 overflow-x-auto rounded-lg bg-surface-muted p-3 text-xs"), Text(d.Result)),
		),
	)
}

type gitRepoRow struct {
	ID         string
	URL        string
	Repository string
	Branch     string
	Path       string
	Owner      string
	LastSync   string
}

type notebookGitReposListPageData struct {
	Principal domain.ContextPrincipal
	Rows      []gitRepoRow
	Page      domain.PageRequest
	Total     int64
}

func notebookGitReposListPage(d notebookGitReposListPageData) Node {
	rows := make([]Node, 0, len(d.Rows))
	for i := range d.Rows {
		row := d.Rows[i]
		rows = append(rows, Tr(
			Td(A(Href(row.URL), Text(row.Repository))),
			Td(Text(row.Branch)),
			Td(Text(row.Path)),
			Td(Text(row.Owner)),
			Td(Text(row.LastSync)),
		))
	}
	body := []Node{pageToolbar("Git Repos", "Registered sources for notebook sync.", "/ui/notebooks/git-repos/new", "Register Git repo")}
	if len(rows) == 0 {
		body = append(body, emptyStateCard("No Git repositories registered.", "Register Git repo", "/ui/notebooks/git-repos/new"))
	} else {
		body = append(body, tableCard([]string{"Repository", "Branch", "Path", "Owner", "Last sync"}, rows))
	}
	body = append(body, paginationCard("/ui/notebooks/git-repos", d.Page, d.Total))
	return core.AppPage("Notebook Git Repos", "notebooks", d.Principal, body...)
}

func notebooksNewPage(principal domain.ContextPrincipal, csrfFieldProvider func() Node) Node {
	return notebookFormPage(principal, "New Notebook", "/ui/notebooks", csrfFieldProvider,
		Label(Text("Name")),
		core.InputControl("", Name("name"), Required()),
		Label(Text("Description")),
		core.TextareaControl("min-h-28", Name("description")),
		Label(Text("Source")),
		core.InputControl("", Name("source")),
	)
}

func notebooksEditPage(principal domain.ContextPrincipal, notebookID string, notebook *domain.Notebook, csrfFieldProvider func() Node) Node {
	description := ""
	if notebook.Description != nil {
		description = *notebook.Description
	}
	return notebookFormPage(principal, "Edit Notebook", "/ui/notebooks/"+notebookID+"/update", csrfFieldProvider,
		Label(Text("Name")),
		core.InputControl("", Name("name"), Value(notebook.Name), Required()),
		Label(Text("Description")),
		core.TextareaControl("min-h-28", Name("description"), Text(description)),
	)
}

func notebookCellsNewPage(principal domain.ContextPrincipal, notebookID string, csrfFieldProvider func() Node) Node {
	return notebookFormPage(principal, "New Notebook Cell", "/ui/notebooks/"+notebookID+"/cells", csrfFieldProvider,
		Label(Text("Cell Type")),
		core.SelectControl("", Name("cell_type"),
			Option(Value("sql"), Text("sql")),
			Option(Value("markdown"), Text("markdown")),
		),
		Label(Text("Content")),
		core.TextareaControl("min-h-40 font-mono", Name("content"), Required()),
		Label(Text("Position (optional)")),
		core.InputControl("", Name("position")),
	)
}

func notebookCellsEditPage(principal domain.ContextPrincipal, notebookID, cellID string, cell *domain.Cell, csrfFieldProvider func() Node) Node {
	nodes := []Node{
		Label(Text("Content")),
		core.TextareaControl("min-h-40 font-mono", Name("content"), Required(), Text(cell.Content)),
		Label(Text("Position")),
		core.InputControl("", Name("position"), Value(strconv.Itoa(cell.Position))),
	}
	if cell.CellType == domain.CellTypeSQL {
		visualKind := "table"
		chartType := ""
		title := ""
		subtitle := ""
		xField := ""
		yField := ""
		seriesField := ""
		labelField := ""
		valueField := ""
		if cell.VisualSpec != nil {
			visualKind = string(cell.VisualSpec.Kind)
			title = cell.VisualSpec.Title
			subtitle = cell.VisualSpec.Subtitle
			if cell.VisualSpec.ChartType != nil {
				chartType = string(*cell.VisualSpec.ChartType)
			}
			if cell.VisualSpec.Encodings.X != nil {
				xField = cell.VisualSpec.Encodings.X.Field
			}
			if cell.VisualSpec.Encodings.Y != nil {
				yField = cell.VisualSpec.Encodings.Y.Field
			}
			if cell.VisualSpec.Encodings.Series != nil {
				seriesField = cell.VisualSpec.Encodings.Series.Field
			}
			if cell.VisualSpec.Encodings.Label != nil {
				labelField = cell.VisualSpec.Encodings.Label.Field
			}
			if cell.VisualSpec.Encodings.Value != nil {
				valueField = cell.VisualSpec.Encodings.Value.Field
			}
		}
		nodes = append(nodes,
			Label(Text("Visual kind")),
			core.SelectControl("", Name("visual_kind"),
				optionSelected("table", visualKind),
				optionSelected("metric", visualKind),
				optionSelected("chart", visualKind),
			),
			Label(Text("Chart type")),
			core.SelectControl("", Name("chart_type"),
				optionSelected("bar", chartType),
				optionSelected("line", chartType),
				optionSelected("area", chartType),
				optionSelected("pie", chartType),
			),
			Label(Text("Visual title")),
			core.InputControl("", Name("visual_title"), Value(title)),
			Label(Text("Visual subtitle")),
			core.InputControl("", Name("visual_subtitle"), Value(subtitle)),
			Label(Text("X field")),
			core.InputControl("", Name("visual_x"), Value(xField)),
			Label(Text("Y field")),
			core.InputControl("", Name("visual_y"), Value(yField)),
			Label(Text("Series field")),
			core.InputControl("", Name("visual_series"), Value(seriesField)),
			Label(Text("Label field")),
			core.InputControl("", Name("visual_label"), Value(labelField)),
			Label(Text("Value field")),
			core.InputControl("", Name("visual_value"), Value(valueField)),
		)
	}
	return notebookFormPage(principal, "Edit Notebook Cell", "/ui/notebooks/"+notebookID+"/cells/"+cellID+"/update", csrfFieldProvider, nodes...)
}

func notebookGitReposNewPage(principal domain.ContextPrincipal, csrfFieldProvider func() Node) Node {
	return notebookFormPage(principal, "Register Git Repo", "/ui/notebooks/git-repos", csrfFieldProvider,
		Label(Text("Repository URL")),
		core.InputControl("", Name("url"), Required()),
		Label(Text("Branch")),
		core.InputControl("", Name("branch"), Value("main"), Required()),
		Label(Text("Path")),
		core.InputControl("", Name("path")),
		Label(Text("Auth token")),
		core.InputControl("", Name("auth_token"), Type("password")),
	)
}

type notebookGitRepoDetailPageData struct {
	Principal     domain.ContextPrincipal
	ID            string
	URL           string
	Branch        string
	Path          string
	Owner         string
	LastSync      string
	LastCommit    string
	DeleteURL     string
	SyncURL       string
	CSRFFieldFunc func() Node
}

func notebookGitRepoDetailPage(d notebookGitRepoDetailPageData) Node {
	return core.AppPage(
		"Git Repo",
		"notebooks",
		d.Principal,
		pageToolbar("Git Repo", "Repository details and sync controls.", "/ui/notebooks/git-repos", "Back to repos"),
		Div(Class("rounded-xl border border-border bg-background p-4 shadow-xs"),
			P(Text("Repository: "+d.URL)),
			P(Text("Branch: "+d.Branch)),
			P(Text("Path: "+d.Path)),
			P(Text("Owner: "+d.Owner)),
			P(Text("Last sync: "+d.LastSync)),
			P(Text("Last commit: "+d.LastCommit)),
			Div(Class("mt-1 flex flex-wrap items-center gap-2 [&_form]:m-0 [&_form]:inline-flex"),
				Form(Method("post"), Action(d.SyncURL), d.CSRFFieldFunc(), core.PrimaryButton("", Type("submit"), Text("Sync repo"))),
				Form(Method("post"), Action(d.DeleteURL), d.CSRFFieldFunc(), core.DangerButton("", Type("submit"), Text("Delete repo"))),
			),
		),
	)
}

type notebookGitRepoSyncResultPageData struct {
	Principal domain.ContextPrincipal
	GitRepoID string
	Result    *domain.GitSyncResult
}

func notebookGitRepoSyncResultPage(d notebookGitRepoSyncResultPageData) Node {
	if d.Result == nil {
		return core.AppPage("Git Sync", "notebooks", d.Principal, emptyStateCard("No sync result available.", "Back to repo", "/ui/notebooks/git-repos/"+d.GitRepoID))
	}
	return core.AppPage(
		"Git Sync",
		"notebooks",
		d.Principal,
		pageToolbar("Git Sync", "Latest sync result.", "/ui/notebooks/git-repos/"+d.GitRepoID, "Back to repo"),
		Div(Class("rounded-xl border border-border bg-background p-4 shadow-xs"),
			P(Text("Created notebooks: "+strconv.Itoa(d.Result.NotebooksCreated))),
			P(Text("Updated notebooks: "+strconv.Itoa(d.Result.NotebooksUpdated))),
			P(Text("Deleted notebooks: "+strconv.Itoa(d.Result.NotebooksDeleted))),
			P(Text("Commit: "+d.Result.CommitSHA)),
		),
	)
}

type notebookGitRepoSyncUnavailablePageData struct {
	Principal domain.ContextPrincipal
	GitRepoID string
	RepoURL   string
	Branch    string
	Path      string
	Message   string
}

func notebookGitRepoSyncUnavailablePage(d notebookGitRepoSyncUnavailablePageData) Node {
	return core.AppPage(
		"Git Sync Unavailable",
		"notebooks",
		d.Principal,
		pageToolbar("Git Sync", "Sync is not available yet.", "/ui/notebooks/git-repos/"+d.GitRepoID, "Back to repo"),
		Div(Class("rounded-xl border border-border bg-background p-4 shadow-xs"),
			H2(Class("m-0 text-lg font-semibold"), Text("Sync is not available yet")),
			P(Text(d.Message)),
			P(Text("Repository: "+d.RepoURL)),
			P(Text("Branch: "+d.Branch)),
			P(Text("Path: "+d.Path)),
			P(Class("text-xs text-muted"), Text("The repo is registered correctly, but server-side sync execution has not been implemented yet.")),
		),
	)
}

func notebookFormPage(principal domain.ContextPrincipal, title, action string, csrfFieldProvider func() Node, fields ...Node) Node {
	nodes := []Node{csrfFieldProvider()}
	nodes = append(nodes, fields...)
	nodes = append(nodes, Div(Class("mt-3"), core.PrimaryButton("", Type("submit"), Text("Save"))))
	return core.AppPage(
		title,
		"notebooks",
		principal,
		Div(Class("rounded-xl border border-border bg-background p-4 shadow-xs"),
			Form(Method("post"), Action(action), Class("grid gap-3"), Group(nodes)),
		),
	)
}

func optionSelected(value, selected string) Node {
	if value == selected {
		return Option(Value(value), Selected(), Text(value))
	}
	return Option(Value(value), Text(value))
}

func pageToolbar(title, description, href, label string) Node {
	return Div(Class("rounded-xl border border-border bg-background p-4 shadow-xs"),
		Div(Class("flex flex-wrap items-center justify-between gap-3"),
			Div(Class("flex min-w-0 flex-col gap-1"),
				Span(Class("inline-flex items-center rounded-full bg-surface-muted px-2 py-0.5 text-xs font-medium text-muted"), Text(title)),
				P(Class("m-0 text-xs text-muted"), Text(description)),
			),
			core.PrimaryLink(href, "", Text(label)),
		),
	)
}

func emptyStateCard(message, ctaLabel, ctaHref string) Node {
	cta := Node(nil)
	if ctaLabel != "" && ctaHref != "" {
		cta = core.PrimaryLink(ctaHref, "", Text(ctaLabel))
	}
	return Div(Class("rounded-xl border border-border bg-background p-4 text-center shadow-xs"),
		P(Class("m-0 text-lg font-semibold"), Text("No results yet")),
		P(Class("m-0 text-sm text-muted"), Text(message)),
		cta,
	)
}

func tableCard(headers []string, rows []Node) Node {
	headerNodes := make([]Node, 0, len(headers))
	for i := range headers {
		headerNodes = append(headerNodes, Th(Class("px-4 py-3 text-left text-xs font-semibold uppercase tracking-[0.02em] text-muted"), Text(headers[i])))
	}
	return Div(Class("overflow-x-auto rounded-xl border border-border bg-background p-4 shadow-xs"),
		Table(Class("min-w-full border-collapse"),
			THead(Tr(Group(headerNodes))),
			TBody(Group(rows)),
		),
	)
}

func paginationCard(basePath string, page domain.PageRequest, total int64) Node {
	shown := page.MaxResults
	if total < int64(shown) {
		shown = int(total)
	}
	summary := "Showing " + strconv.Itoa(shown) + " of " + strconv.FormatInt(total, 10) + " entries."
	nextToken := domain.NextPageToken(page.Offset(), page.Limit(), total)
	if nextToken == "" {
		return Div(Class("rounded-xl border border-border bg-background p-4 shadow-xs"),
			Div(Class("flex items-center justify-between gap-3"),
				P(Class("m-0 text-xs text-muted"), Text(summary)),
				Span(Class("inline-flex min-h-8 items-center justify-center rounded-lg border border-border px-3 text-sm font-medium text-foreground opacity-60 pointer-events-none"), Attr("aria-disabled", "true"), Text("Next")),
			),
		)
	}
	u := basePath + "?max_results=" + strconv.Itoa(page.Limit()) + "&page_token=" + nextToken
	return Div(Class("rounded-xl border border-border bg-background p-4 shadow-xs"),
		Div(Class("flex items-center justify-between gap-3"),
			P(Class("m-0 text-xs text-muted"), Text(summary)),
			core.SecondaryLink(u, "small", Text("Next page")),
		),
	)
}
