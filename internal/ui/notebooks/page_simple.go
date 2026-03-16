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
		Div(Class(core.CardClass()),
			P(Text("State: "+d.State)),
			P(Text("Created: "+d.CreatedAt)),
			P(Text("Updated: "+d.UpdatedAt)),
			P(Text("Error: "+d.ErrorText)),
		),
		Div(Class(core.CardClass()),
			H2(Class("m-0 text-lg font-semibold"), Text("Result payload")),
			Pre(Class("mt-3 overflow-x-auto rounded-lg bg-[var(--bgColor-muted)] p-3 text-xs"), Text(d.Result)),
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
		Input(Name("name"), Required(), Class(core.FormControlClass())),
		Label(Text("Description")),
		Textarea(Name("description"), Class(core.FormControlClass("min-h-28"))),
		Label(Text("Source")),
		Input(Name("source"), Class(core.FormControlClass())),
	)
}

func notebooksEditPage(principal domain.ContextPrincipal, notebookID string, notebook *domain.Notebook, csrfFieldProvider func() Node) Node {
	description := ""
	if notebook.Description != nil {
		description = *notebook.Description
	}
	return notebookFormPage(principal, "Edit Notebook", "/ui/notebooks/"+notebookID+"/update", csrfFieldProvider,
		Label(Text("Name")),
		Input(Name("name"), Value(notebook.Name), Required(), Class(core.FormControlClass())),
		Label(Text("Description")),
		Textarea(Name("description"), Class(core.FormControlClass("min-h-28")), Text(description)),
	)
}

func notebookCellsNewPage(principal domain.ContextPrincipal, notebookID string, csrfFieldProvider func() Node) Node {
	return notebookFormPage(principal, "New Notebook Cell", "/ui/notebooks/"+notebookID+"/cells", csrfFieldProvider,
		Label(Text("Cell Type")),
		Select(Name("cell_type"), Class(core.FormControlClass()),
			Option(Value("sql"), Text("sql")),
			Option(Value("markdown"), Text("markdown")),
		),
		Label(Text("Content")),
		Textarea(Name("content"), Required(), Class(core.FormControlClass("min-h-40 font-mono"))),
		Label(Text("Position (optional)")),
		Input(Name("position"), Class(core.FormControlClass())),
	)
}

func notebookCellsEditPage(principal domain.ContextPrincipal, notebookID, cellID string, cell *domain.Cell, csrfFieldProvider func() Node) Node {
	nodes := []Node{
		Label(Text("Content")),
		Textarea(Name("content"), Text(cell.Content), Required(), Class(core.FormControlClass("min-h-40 font-mono"))),
		Label(Text("Position")),
		Input(Name("position"), Value(strconv.Itoa(cell.Position)), Class(core.FormControlClass())),
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
			Select(Name("visual_kind"), Class(core.FormControlClass()),
				optionSelected("table", visualKind),
				optionSelected("metric", visualKind),
				optionSelected("chart", visualKind),
			),
			Label(Text("Chart type")),
			Select(Name("chart_type"), Class(core.FormControlClass()),
				optionSelected("bar", chartType),
				optionSelected("line", chartType),
				optionSelected("area", chartType),
				optionSelected("pie", chartType),
			),
			Label(Text("Visual title")),
			Input(Name("visual_title"), Value(title), Class(core.FormControlClass())),
			Label(Text("Visual subtitle")),
			Input(Name("visual_subtitle"), Value(subtitle), Class(core.FormControlClass())),
			Label(Text("X field")),
			Input(Name("visual_x"), Value(xField), Class(core.FormControlClass())),
			Label(Text("Y field")),
			Input(Name("visual_y"), Value(yField), Class(core.FormControlClass())),
			Label(Text("Series field")),
			Input(Name("visual_series"), Value(seriesField), Class(core.FormControlClass())),
			Label(Text("Label field")),
			Input(Name("visual_label"), Value(labelField), Class(core.FormControlClass())),
			Label(Text("Value field")),
			Input(Name("visual_value"), Value(valueField), Class(core.FormControlClass())),
		)
	}
	return notebookFormPage(principal, "Edit Notebook Cell", "/ui/notebooks/"+notebookID+"/cells/"+cellID+"/update", csrfFieldProvider, nodes...)
}

func notebookGitReposNewPage(principal domain.ContextPrincipal, csrfFieldProvider func() Node) Node {
	return notebookFormPage(principal, "Register Git Repo", "/ui/notebooks/git-repos", csrfFieldProvider,
		Label(Text("Repository URL")),
		Input(Name("url"), Required(), Class(core.FormControlClass())),
		Label(Text("Branch")),
		Input(Name("branch"), Value("main"), Required(), Class(core.FormControlClass())),
		Label(Text("Path")),
		Input(Name("path"), Class(core.FormControlClass())),
		Label(Text("Auth token")),
		Input(Name("auth_token"), Type("password"), Class(core.FormControlClass())),
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
		Div(Class(core.CardClass()),
			P(Text("Repository: "+d.URL)),
			P(Text("Branch: "+d.Branch)),
			P(Text("Path: "+d.Path)),
			P(Text("Owner: "+d.Owner)),
			P(Text("Last sync: "+d.LastSync)),
			P(Text("Last commit: "+d.LastCommit)),
			Div(Class(core.ButtonRowClass()),
				Form(Method("post"), Action(d.SyncURL), d.CSRFFieldFunc(), Button(Type("submit"), Class(core.PrimaryButtonClass()), Text("Sync repo"))),
				Form(Method("post"), Action(d.DeleteURL), d.CSRFFieldFunc(), Button(Type("submit"), Class(core.DangerButtonClass()), Text("Delete repo"))),
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
		Div(Class(core.CardClass()),
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
		Div(Class(core.CardClass()),
			H2(Class("m-0 text-lg font-semibold"), Text("Sync is not available yet")),
			P(Text(d.Message)),
			P(Text("Repository: "+d.RepoURL)),
			P(Text("Branch: "+d.Branch)),
			P(Text("Path: "+d.Path)),
			P(Class(core.MutedClass()), Text("The repo is registered correctly, but server-side sync execution has not been implemented yet.")),
		),
	)
}

func notebookFormPage(principal domain.ContextPrincipal, title, action string, csrfFieldProvider func() Node, fields ...Node) Node {
	nodes := []Node{csrfFieldProvider()}
	nodes = append(nodes, fields...)
	nodes = append(nodes, Div(Class("mt-3"), Button(Type("submit"), Class(core.PrimaryButtonClass()), Text("Save"))))
	return core.AppPage(
		title,
		"notebooks",
		principal,
		Div(Class(core.CardClass()),
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
	return Div(Class(core.CardClass()),
		Div(Class("flex flex-wrap items-center justify-between gap-3"),
			Div(Class("flex min-w-0 flex-col gap-1"),
				Span(Class("inline-flex items-center rounded-full bg-[var(--bgColor-muted)] px-2 py-0.5 text-xs font-medium text-[var(--fgColor-muted)]"), Text(title)),
				P(Class("m-0 text-xs text-[var(--fgColor-muted)]"), Text(description)),
			),
			A(Href(href), Class(core.PrimaryButtonClass()), Text(label)),
		),
	)
}

func emptyStateCard(message, ctaLabel, ctaHref string) Node {
	cta := Node(nil)
	if ctaLabel != "" && ctaHref != "" {
		cta = A(Href(ctaHref), Class(core.PrimaryButtonClass()), Text(ctaLabel))
	}
	return Div(Class(core.CardClass("text-center")),
		P(Class("m-0 text-lg font-semibold"), Text("No results yet")),
		P(Class("m-0 text-sm text-[var(--fgColor-muted)]"), Text(message)),
		cta,
	)
}

func tableCard(headers []string, rows []Node) Node {
	headerNodes := make([]Node, 0, len(headers))
	for i := range headers {
		headerNodes = append(headerNodes, Th(Class("px-4 py-3 text-left text-xs font-semibold uppercase tracking-[0.02em] text-[var(--fgColor-muted)]"), Text(headers[i])))
	}
	return Div(Class(core.CardClass("overflow-x-auto")),
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
		return Div(Class(core.CardClass()),
			Div(Class("flex items-center justify-between gap-3"),
				P(Class("m-0 text-xs text-[var(--fgColor-muted)]"), Text(summary)),
				Span(Class(core.ClassNames(core.SecondaryButtonClass("small"), "pointer-events-none opacity-60")), Attr("aria-disabled", "true"), Text("Next")),
			),
		)
	}
	u := basePath + "?max_results=" + strconv.Itoa(page.Limit()) + "&page_token=" + nextToken
	return Div(Class(core.CardClass()),
		Div(Class("flex items-center justify-between gap-3"),
			P(Class("m-0 text-xs text-[var(--fgColor-muted)]"), Text(summary)),
			A(Href(u), Class(core.SecondaryButtonClass("small")), Text("Next page")),
		),
	)
}
