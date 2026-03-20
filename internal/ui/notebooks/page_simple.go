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
		tableRows = append(tableRows, Tr(
			Td(
				Div(Class("flex items-center gap-3"),
					Span(Class("inline-flex h-8 w-8 items-center justify-center rounded-lg bg-[var(--bgColor-accent-muted)] text-[var(--fgColor-accent)]"),
						I(Class(core.NavIconClass("h-4 w-4")), Attr("data-lucide", "file-text"), Attr("aria-hidden", "true")),
					),
					Div(Class("min-w-0"),
						core.TextLink(row.URL, Text(row.Name)),
					),
				),
			),
			Td(core.Badge(row.Owner, "")),
			Td(Span(Class("text-[var(--fgColor-muted)]"), Text(row.Updated))),
		))
	}

	body := []Node{
		core.PageHeader(
			"Build",
			"Notebooks",
			"Create and manage notebooks.",
			core.SecondaryLink("/ui/notebooks/git-repos", "",
				I(Class(core.IconGlyphClass()), Attr("data-lucide", "github"), Attr("aria-hidden", "true")),
				Span(Text("Git repos")),
			),
			core.PrimaryLink("/ui/notebooks/new", "",
				I(Class(core.IconGlyphClass()), Attr("data-lucide", "plus"), Attr("aria-hidden", "true")),
				Span(Text("New notebook")),
			),
		),
	}
	if len(tableRows) == 0 {
		body = append(body, core.ListPageBody(
			core.WorkspaceEmptyState("inbox", "No notebooks yet.", "Create your first notebook to start building notebook workflows. Manage Git repos only when you need sync-backed notebooks.", core.PrimaryLink("/ui/notebooks/new", "", Text("Create your first notebook"))),
		))
	} else {
		body = append(body, core.ListPageBody(
			notebookTable([]string{"Name", "Owner", "Last updated"}, tableRows),
			core.ListPagination("/ui/notebooks", page, total),
		))
	}
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
		rows = append(rows, Tr(Td(core.TextLink(row.URL, Text(row.ID))), Td(Text(row.State)), Td(Text(row.Updated))))
	}
	body := []Node{core.PageHeader("Build", "Notebook jobs", "Async runs for this notebook.", core.SecondaryLink("/ui/notebooks/"+d.NotebookID, "", Text("Back to notebook")))}
	if len(rows) == 0 {
		body = append(body, core.ListPageBody(
			core.WorkspaceEmptyState("history", "No notebook jobs found.", "Runs will appear here after notebook execution starts.", core.SecondaryLink("/ui/notebooks/"+d.NotebookID, "", Text("Back to notebook"))),
		))
	} else {
		body = append(body, core.ListPageBody(
			notebookTable([]string{"Job ID", "State", "Updated"}, rows),
			core.ListPagination("/ui/notebooks/"+d.NotebookID+"/jobs", d.Page, d.Total),
		))
	}
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
		core.ResultPageLayout("Build", "Notebook job: "+d.JobID, "Inspect notebook execution as a result workspace instead of a stack of generic cards.",
			core.PageHeader("", "Notebook job", "Inspect a notebook run.", core.SecondaryLink("/ui/notebooks/"+d.NotebookID+"/jobs", "", Text("Back to jobs"))),
			core.DetailLayout(
				core.DetailMain(
					core.SectionSurface(
						core.SectionHeader("Result payload", "Execution output lives in the main report column."),
						Pre(Class("mt-0 overflow-x-auto rounded-lg bg-[var(--bgColor-muted)] p-3 text-xs"), Text(d.Result)),
					),
				),
				core.DetailRail(
					core.DetailRailCard("Run summary", "Keep status and timestamps visible while reviewing the payload.",
						core.KeyValueGrid([][2]string{
							{"State", d.State},
							{"Created", d.CreatedAt},
							{"Updated", d.UpdatedAt},
							{"Error", emptyDash(d.ErrorText)},
						}),
					),
				),
			),
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
			Td(core.TextLink(row.URL, Text(row.Repository))),
			Td(Text(row.Branch)),
			Td(Text(row.Path)),
			Td(Text(row.Owner)),
			Td(Text(row.LastSync)),
		))
	}
	body := []Node{core.PageHeader("Build", "Git repos", "Registered sources for notebook sync.", core.PrimaryLink("/ui/notebooks/git-repos/new", "", Text("Register Git repo")))}
	if len(rows) == 0 {
		body = append(body, core.ListPageBody(
			core.WorkspaceEmptyState("git-branch", "No Git repositories registered.", "Register a repository to connect notebook sync.", core.PrimaryLink("/ui/notebooks/git-repos/new", "", Text("Register Git repo"))),
		))
	} else {
		body = append(body, core.ListPageBody(
			notebookTable([]string{"Repository", "Branch", "Path", "Owner", "Last sync"}, rows),
			core.ListPagination("/ui/notebooks/git-repos", d.Page, d.Total),
		))
	}
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
		core.DetailShell(
			core.PageHeader("Build", "Git repo", "Repository details and sync controls.", core.SecondaryLink("/ui/notebooks/git-repos", "", Text("Back to repos"))),
			core.DetailLayout(
				core.DetailMain(
					core.SectionSurface(
						core.SectionHeader("Repository details", "Keep current sync configuration in the main detail column."),
						core.KeyValueGrid([][2]string{
							{"Repository", d.URL},
							{"Branch", d.Branch},
							{"Path", d.Path},
							{"Owner", d.Owner},
							{"Last sync", d.LastSync},
							{"Last commit", d.LastCommit},
						}),
					),
				),
				core.DetailRail(
					core.DetailRailCard("Actions", "Sync and deletion stay together in the secondary rail.",
						core.ButtonGroup("",
							Form(Method("post"), Action(d.SyncURL), d.CSRFFieldFunc(), core.PrimaryButton("", Type("submit"), Text("Sync repo"))),
							Form(Method("post"), Action(d.DeleteURL), d.CSRFFieldFunc(), core.DangerButton("", Type("submit"), Text("Delete repo"))),
						),
					),
				),
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
		return core.AppPage("Git Sync", "notebooks", d.Principal, core.ListPageBody(core.WorkspaceEmptyState("git-branch", "No sync result available.", "Run a sync first to generate a result summary.", core.SecondaryLink("/ui/notebooks/git-repos/"+d.GitRepoID, "", Text("Back to repo")))))
	}
	return core.AppPage(
		"Git Sync",
		"notebooks",
		d.Principal,
		core.ResultPageLayout("Build", "Git sync", "Sync outcomes use the shared result layout so they read like execution reports.",
			core.PageHeader("", "Git sync", "Latest sync result.", core.SecondaryLink("/ui/notebooks/git-repos/"+d.GitRepoID, "", Text("Back to repo"))),
			core.SectionSurface(
				core.SectionHeader("Sync result", ""),
				core.MetadataSummary([][2]string{
					{"Created notebooks", strconv.Itoa(d.Result.NotebooksCreated)},
					{"Updated notebooks", strconv.Itoa(d.Result.NotebooksUpdated)},
					{"Deleted notebooks", strconv.Itoa(d.Result.NotebooksDeleted)},
					{"Commit", d.Result.CommitSHA},
				}),
			),
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
		core.ResultPageLayout("Build", "Git sync unavailable", "Unavailable states use the same result-oriented layout as other notebook outcomes.",
			core.PageHeader("", "Git sync", "Sync is not available yet.", core.SecondaryLink("/ui/notebooks/git-repos/"+d.GitRepoID, "", Text("Back to repo"))),
			core.SectionSurface(
				core.SectionHeader("Sync is not available yet", "The repo is registered correctly, but server-side sync execution has not been implemented yet."),
				P(Text(d.Message)),
				core.KeyValueGrid([][2]string{
					{"Repository", d.RepoURL},
					{"Branch", d.Branch},
					{"Path", d.Path},
				}),
			),
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
		core.FormPageLayout("Build", title, "Notebook authoring uses the shared single-surface form layout.",
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

func notebookTable(headers []string, rows []Node) Node {
	headerNodes := make([]Node, 0, len(headers))
	for i := range headers {
		headerNodes = append(headerNodes, Th(Scope("col"), Text(headers[i])))
	}
	return core.TableContainer("",
		core.DataTable("",
			THead(Tr(Group(headerNodes))),
			TBody(Group(rows)),
		),
	)
}

func emptyDash(v string) string {
	if v == "" {
		return "-"
	}
	return v
}
