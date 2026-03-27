package notebooks

import (
	"fmt"
	"net/url"
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
	Folder  string
	Updated string
}

type notebookFolderNavItem struct {
	Label  string
	URL    string
	Depth  int
	Count  string
	Active bool
}

func notebooksListPage(principal domain.ContextPrincipal, rows []notebookListRow, folders []notebookFolderNavItem, selectedFolderID string, page domain.PageRequest, total int64) Node {
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
						Div(Class("mt-1 text-xs font-mono text-[var(--fgColor-muted)]"), Text(row.Folder)),
					),
				),
			),
			Td(core.Badge(row.Owner, "")),
			Td(Span(Class("text-[var(--fgColor-muted)]"), Text(row.Updated))),
		))
	}

	mainContent := Node(core.ListPageBody(
		core.WorkspaceEmptyState("inbox", "No notebooks yet.", "Create your first notebook to start building notebook workflows. Manage Git repos only when you need sync-backed notebooks.", core.PrimaryLink(notebookNewURL(selectedFolderID), "", Text("Create your first notebook"))),
	))
	if len(tableRows) > 0 {
		mainContent = core.ListPageBody(
			notebookTable([]string{"Name", "Owner", "Last updated"}, tableRows),
			notebookListPagination(selectedFolderID, page, total),
		)
	}

	return core.AppPage(
		"Notebooks",
		"notebooks",
		principal,
		core.PageHeader(
			"Build",
			"Notebooks",
			"Create and manage notebooks.",
			core.SecondaryLink("/ui/notebooks/folders", "",
				I(Class(core.IconGlyphClass()), Attr("data-lucide", "folder-tree"), Attr("aria-hidden", "true")),
				Span(Text("Folders")),
			),
			core.SecondaryLink("/ui/notebooks/git-repos", "",
				I(Class(core.IconGlyphClass()), Attr("data-lucide", "github"), Attr("aria-hidden", "true")),
				Span(Text("Git repos")),
			),
			core.PrimaryLink(notebookNewURL(selectedFolderID), "",
				I(Class(core.IconGlyphClass()), Attr("data-lucide", "plus"), Attr("aria-hidden", "true")),
				Span(Text("New notebook")),
			),
		),
		core.WorkspaceLayout(
			"",
			core.WorkspaceAside(
				"notebook-folder-filter",
				"notebook-folder-aside",
				[]core.WorkspaceAsideTab{
					{ID: "folders", Label: "Folders", Icon: "folder-tree", Count: strconv.Itoa(len(folders)), Content: notebookFolderFilterPanel(folders)},
				},
				"folders",
			),
			mainContent,
		),
	)
}

type folderListRow struct {
	Name          string
	URL           string
	Owner         string
	Path          string
	SystemRole    string
	ProjectID     string
	EnvironmentID string
	GitRepoID     string
	CanManage     bool
	Updated       string
}

type folderSelectOption struct {
	ID          string
	Label       string
	Description string
	Selected    bool
}

type gitRepoSelectOption struct {
	ID       string
	Label    string
	Selected bool
}

func shareRoleSelectNodes(selected string) []Node {
	roles := []struct {
		Value string
		Label string
	}{
		{Value: domain.FolderShareRoleViewer, Label: "Viewer"},
		{Value: domain.FolderShareRoleEditor, Label: "Editor"},
		{Value: domain.FolderShareRoleManager, Label: "Manager"},
	}
	nodes := make([]Node, 0, len(roles)+1)
	nodes = append(nodes, Name("role"))
	for _, role := range roles {
		option := Option(Value(role.Value), Text(role.Label))
		if selected == role.Value {
			option = Option(Value(role.Value), Selected(), Text(role.Label))
		}
		nodes = append(nodes, option)
	}
	return nodes
}

func shareManagementSection(title string, shares []accessShareRow, createURL string, csrfFieldProvider func() Node) Node {
	rows := make([]Node, 0, len(shares))
	for i := range shares {
		share := shares[i]
		rows = append(rows, Tr(
			Td(Text(share.Principal)),
			Td(core.Badge(share.Role, "")),
			Td(Form(Method("post"), Action(share.DeleteURL), Class("m-0"), csrfFieldProvider(), core.DangerButton("", Type("submit"), Text("Remove")))),
		))
	}

	body := Node(P(Class("m-0 text-sm text-[var(--fgColor-muted)]"), Text("No explicit shares yet. Inherited folder access still applies where relevant.")))
	if len(rows) > 0 {
		body = notebookTable([]string{"Principal", "Role", "Action"}, rows)
	}

	return core.SectionSurface(
		core.SectionHeader(title, "Manage direct access for collaborators."),
		Form(Method("post"), Action(createURL), Class("grid gap-3 rounded-xl border border-[var(--borderColor-muted)] bg-[var(--bgColor-muted)] p-4"), csrfFieldProvider(),
			Label(Text("Principal")),
			core.InputControl("", Name("principal_name"), Placeholder("analyst@example.com"), Required()),
			Label(Text("Role")),
			core.SelectControl("", shareRoleSelectNodes(domain.FolderShareRoleViewer)...),
			Div(Class("flex justify-end"), core.PrimaryButton("", Type("submit"), Text("Add share"))),
		),
		body,
	)
}

func notebookFoldersListPage(principal domain.ContextPrincipal, rows []folderListRow) Node {
	tableRows := make([]Node, 0, len(rows))
	for i := range rows {
		row := rows[i]
		nameNode := Node(Span(Text(row.Name)))
		if row.CanManage {
			nameNode = core.TextLink(row.URL, Text(row.Name))
		}
		contextParts := []Node{}
		if row.SystemRole != "-" {
			contextParts = append(contextParts, core.Badge(row.SystemRole, "accent"))
		}
		if row.ProjectID != "-" {
			contextParts = append(contextParts, core.Badge("Project "+row.ProjectID, ""))
		}
		if row.EnvironmentID != "-" {
			contextParts = append(contextParts, core.Badge("Env "+row.EnvironmentID, ""))
		}
		if row.GitRepoID != "-" {
			contextParts = append(contextParts, core.Badge("Git "+row.GitRepoID, "success"))
		}
		contextNode := Node(Span(Class("text-[var(--fgColor-muted)]"), Text("-")))
		if len(contextParts) > 0 {
			contextNode = Div(Class("flex flex-wrap gap-2"), Group(contextParts))
		}
		tableRows = append(tableRows, Tr(
			Td(
				Div(Class("flex items-center gap-3"),
					Span(Class("inline-flex h-8 w-8 items-center justify-center rounded-lg bg-[var(--bgColor-accent-muted)] text-[var(--fgColor-accent)]"),
						I(Class(core.NavIconClass("h-4 w-4")), Attr("data-lucide", "folder"), Attr("aria-hidden", "true")),
					),
					Div(Class("min-w-0"), nameNode),
				),
			),
			Td(Span(Class("font-mono text-xs text-[var(--fgColor-muted)]"), Text(row.Path))),
			Td(contextNode),
			Td(core.Badge(row.Owner, "")),
			Td(Span(Class("text-[var(--fgColor-muted)]"), Text(row.Updated))),
		))
	}

	body := []Node{
		core.PageHeader(
			"Build",
			"Notebook folders",
			"Organize notebooks with inherited project, environment, and Git defaults.",
			core.SecondaryLink("/ui/notebooks", "", Text("Back to notebooks")),
			core.PrimaryLink("/ui/notebooks/folders/new", "",
				I(Class(core.IconGlyphClass()), Attr("data-lucide", "folder-plus"), Attr("aria-hidden", "true")),
				Span(Text("New folder")),
			),
		),
	}
	if len(tableRows) == 0 {
		body = append(body, core.ListPageBody(
			core.WorkspaceEmptyState("folder-tree", "No folders yet.", "Create a folder to define shared notebook organization and inherited execution defaults.", core.PrimaryLink("/ui/notebooks/folders/new", "", Text("Create folder"))),
		))
	} else {
		body = append(body, core.ListPageBody(notebookTable([]string{"Name", "Path", "Inherited context", "Owner", "Updated"}, tableRows)))
	}
	return core.AppPage("Notebook Folders", "notebooks", principal, body...)
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

func notebookFoldersNewPage(principal domain.ContextPrincipal, folderOptions []folderSelectOption, gitRepoOptions []gitRepoSelectOption, csrfFieldProvider func() Node) Node {
	parentFolderNodes := append([]Node{Name("parent_folder_id"), Option(Value(""), Text("Personal root"))}, folderSelectNodes(folderOptions)...)
	gitRepoNodes := append([]Node{Name("git_repo_id"), Option(Value(""), Text("No Git repo"))}, gitRepoSelectNodes(gitRepoOptions)...)
	return notebookFormPage(principal, "New Folder", "/ui/notebooks/folders", csrfFieldProvider,
		Label(Text("Name")),
		core.InputControl("", Name("name"), Required()),
		Label(Text("Parent folder")),
		core.SelectControl("", parentFolderNodes...),
		Label(Text("Default project ID")),
		core.InputControl("", Name("default_project_id")),
		Label(Text("Default environment ID")),
		core.InputControl("", Name("default_environment_id")),
		Label(Text("Git repo")),
		core.SelectControl("", gitRepoNodes...),
		Label(Text("Git root path")),
		core.InputControl("", Name("git_root_path")),
	)
}

func notebookFoldersEditPage(principal domain.ContextPrincipal, folder *domain.Folder, gitRepoOptions []gitRepoSelectOption, shares []accessShareRow, csrfFieldProvider func() Node) Node {
	gitRepoID := ""
	if folder.GitRepoID != nil {
		gitRepoID = *folder.GitRepoID
	}
	gitRootPath := ""
	if folder.GitRootPath != nil {
		gitRootPath = *folder.GitRootPath
	}
	defaultProjectID := ""
	if folder.DefaultProjectID != nil {
		defaultProjectID = *folder.DefaultProjectID
	}
	defaultEnvironmentID := ""
	if folder.DefaultEnvironmentID != nil {
		defaultEnvironmentID = *folder.DefaultEnvironmentID
	}
	gitRepoNodes := append([]Node{Name("git_repo_id"), Option(Value(""), Text("No Git repo"))}, gitRepoSelectNodes(selectGitRepo(gitRepoOptions, gitRepoID))...)

	fields := []Node{
		Label(Text("Name")),
		core.InputControl("", Name("name"), Value(folder.Name), Required()),
		Label(Text("Default project ID")),
		core.InputControl("", Name("default_project_id"), Value(defaultProjectID)),
		Label(Text("Default environment ID")),
		core.InputControl("", Name("default_environment_id"), Value(defaultEnvironmentID)),
		Label(Text("Git repo")),
		core.SelectControl("", gitRepoNodes...),
		Label(Text("Git root path")),
		core.InputControl("", Name("git_root_path"), Value(gitRootPath)),
	}

	deleteNode := Node(nil)
	if folder.SystemRole == nil || *folder.SystemRole != domain.FolderSystemRolePersonalRoot {
		deleteNode = Div(Class("mt-2"),
			Form(Method("post"), Action("/ui/notebooks/folders/"+folder.ID+"/delete"), Class("m-0"), csrfFieldProvider(), core.DangerButton("", Type("submit"), Text("Delete folder"))),
		)
	}
	formNodes := []Node{csrfFieldProvider()}
	formNodes = append(formNodes, fields...)
	formNodes = append(formNodes, Div(Class("mt-3"), core.PrimaryButton("", Type("submit"), Text("Save"))))

	return core.AppPage(
		"Edit Folder",
		"notebooks",
		principal,
		core.FormPageLayout("Build", "Edit Folder", "Folder defaults control notebook organization and inherited execution context.",
			core.SectionSurface(
				core.SectionHeader("Folder details", "Update folder-level defaults. Personal roots are protected."),
				P(Class("m-0 text-sm text-[var(--fgColor-muted)]"), Text("Path: "+folder.Path)),
				Form(Method("post"), Action("/ui/notebooks/folders/"+folder.ID+"/update"), Class("grid gap-3"), Group(formNodes)),
				deleteNode,
			),
			shareManagementSection("Folder sharing", shares, "/ui/notebooks/folders/"+folder.ID+"/share", csrfFieldProvider),
		),
	)
}

func notebooksNewPage(principal domain.ContextPrincipal, folderOptions []folderSelectOption, csrfFieldProvider func() Node) Node {
	folderNodes := append([]Node{Name("folder_id"), Option(Value(""), Text("My notebooks"))}, folderSelectNodes(folderOptions)...)
	return notebookFormPage(principal, "New Notebook", "/ui/notebooks", csrfFieldProvider,
		Label(Text("Name")),
		core.InputControl("", Name("name"), Required()),
		Label(Text("Description")),
		core.TextareaControl("min-h-28", Name("description")),
		Label(Text("Source")),
		core.InputControl("", Name("source")),
		Label(Text("Folder")),
		core.SelectControl("", folderNodes...),
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

func notebooksMovePage(principal domain.ContextPrincipal, notebook *domain.Notebook, folderOptions []folderSelectOption, csrfFieldProvider func() Node) Node {
	folderNodes := append([]Node{Name("folder_id")}, folderSelectNodes(folderOptions)...)
	return notebookFormPage(principal, "Move Notebook", "/ui/notebooks/"+notebook.ID+"/move", csrfFieldProvider,
		Label(Text("Destination folder")),
		core.SelectControl("", folderNodes...),
		Label(Text("Destination Git path (optional)")),
		core.InputControl("", Name("git_path")),
		Label(Class("flex items-center gap-2"), Input(Type("checkbox"), Name("confirm_context_change"), Value("true")), Span(Text("Confirm project/environment context change if required"))),
		Label(Class("flex items-center gap-2"), Input(Type("checkbox"), Name("confirm_leave_git"), Value("true")), Span(Text("Confirm leaving Git governance if required"))),
	)
}

func notebooksDuplicatePage(principal domain.ContextPrincipal, notebook *domain.Notebook, folderOptions []folderSelectOption, csrfFieldProvider func() Node) Node {
	folderNodes := append([]Node{Name("folder_id")}, folderSelectNodes(folderOptions)...)
	return notebookFormPage(principal, "Duplicate Notebook", "/ui/notebooks/"+notebook.ID+"/duplicate", csrfFieldProvider,
		Label(Text("Destination folder")),
		core.SelectControl("", folderNodes...),
		Label(Text("New notebook name (optional)")),
		core.InputControl("", Name("name"), Placeholder(notebook.Name+" copy")),
		Label(Text("Destination Git path (optional)")),
		core.InputControl("", Name("git_path")),
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

func notebookFolderFilterPanel(items []notebookFolderNavItem) Node {
	if len(items) == 0 {
		return P(Class("text-xs text-[var(--fgColor-muted)]"), Text("No folders available."))
	}
	nodes := make([]Node, 0, len(items))
	for i := range items {
		item := items[i]
		className := "flex items-center justify-between gap-2 rounded-lg px-2 py-2 text-sm text-[var(--fgColor-muted)] no-underline transition-colors hover:bg-[var(--bgColor-default)] hover:text-[var(--fgColor-default)]"
		if item.Active {
			className += " bg-[var(--bgColor-accent-muted)] font-medium text-[var(--fgColor-accent)]"
		}
		nodes = append(nodes, Li(
			A(
				Href(item.URL),
				Class(className),
				Span(Class(folderIndentClass(item.Depth)), Text(item.Label)),
				Span(Class("rounded-full bg-[var(--bgColor-default)] px-2 py-0.5 text-[11px] font-semibold text-[var(--fgColor-muted)]"), Text(item.Count)),
			),
		))
	}
	return Ul(Class("m-0 grid list-none gap-1 p-0"), Group(nodes))
}

func folderIndentClass(depth int) string {
	switch {
	case depth <= 0:
		return "truncate"
	case depth == 1:
		return "truncate pl-3"
	case depth == 2:
		return "truncate pl-5"
	default:
		return "truncate pl-7"
	}
}

func notebookListPagination(folderID string, page domain.PageRequest, total int64) Node {
	offset := page.Offset()
	limit := page.Limit()
	shown := limit
	if remaining := int(total) - offset; remaining < shown {
		shown = remaining
	}
	if shown < 0 {
		shown = 0
	}
	summary := Span(
		Class("text-sm text-[var(--fgColor-muted)]"),
		Text("Showing "),
		Span(Class("font-semibold text-[var(--fgColor-default)]"), Text(fmt.Sprintf("%d", shown))),
		Text(" of "),
		Span(Class("font-semibold text-[var(--fgColor-default)]"), Text(fmt.Sprintf("%d", total))),
		Text(" entries."),
	)
	prevOffset := offset - limit
	if prevOffset < 0 {
		prevOffset = 0
	}
	prevToken := domain.EncodePageToken(prevOffset)
	nextToken := domain.NextPageToken(offset, limit, total)

	prevNode := Node(Span(Class("inline-flex min-h-10 items-center justify-center rounded-l-lg border border-[var(--borderColor-default)] bg-[var(--bgColor-default)] px-3 text-sm font-medium text-[var(--fgColor-default)] opacity-60 pointer-events-none"), Attr("aria-disabled", "true"), Text("Previous")))
	if offset > 0 {
		prevNode = A(Href(notebookListPageURL(limit, prevToken, folderID)), Class("inline-flex min-h-10 items-center justify-center rounded-l-lg border border-[var(--button-default-borderColor-rest)] border-r-0 bg-[var(--button-default-bgColor-rest)] px-3 text-sm font-medium text-[var(--button-default-fgColor-rest)] no-underline transition-colors duration-100 ease-out hover:border-[var(--button-default-borderColor-hover)] hover:bg-[var(--button-default-bgColor-hover)] hover:text-[var(--button-default-fgColor-rest)]"), Text("Previous"))
	}

	nextNode := Node(Span(Class("inline-flex min-h-10 items-center justify-center rounded-r-lg border border-[var(--borderColor-default)] bg-[var(--bgColor-default)] px-3 text-sm font-medium text-[var(--fgColor-default)] opacity-60 pointer-events-none"), Attr("aria-disabled", "true"), Text("Next")))
	if nextToken != "" {
		nextNode = A(Href(notebookListPageURL(limit, nextToken, folderID)), Class("inline-flex min-h-10 items-center justify-center rounded-r-lg border border-[var(--button-default-borderColor-rest)] bg-[var(--button-default-bgColor-rest)] px-3 text-sm font-medium text-[var(--button-default-fgColor-rest)] no-underline transition-colors duration-100 ease-out hover:border-[var(--button-default-borderColor-hover)] hover:bg-[var(--button-default-bgColor-hover)] hover:text-[var(--button-default-fgColor-rest)]"), Text("Next"))
	}

	return Div(
		Class("flex items-center justify-between gap-4 border-t border-[var(--borderColor-default)] bg-[var(--bgColor-default)] px-6 py-4 max-sm:flex-col max-sm:items-start max-sm:px-4"),
		summary,
		Nav(Attr("aria-label", "Pagination"),
			Div(Class("inline-flex items-center"),
				prevNode,
				nextNode,
			),
		),
	)
}

func notebookListPageURL(limit int, token, folderID string) string {
	q := url.Values{}
	q.Set("max_results", fmt.Sprintf("%d", limit))
	if token != "" {
		q.Set("page_token", token)
	}
	if folderID != "" {
		q.Set("folder_id", folderID)
	}
	return "/ui/notebooks?" + q.Encode()
}

func notebookNewURL(folderID string) string {
	if folderID == "" {
		return "/ui/notebooks/new"
	}
	q := url.Values{}
	q.Set("folder_id", folderID)
	return "/ui/notebooks/new?" + q.Encode()
}

func folderSelectNodes(options []folderSelectOption) []Node {
	nodes := make([]Node, 0, len(options))
	for i := range options {
		option := options[i]
		label := option.Label
		if option.Description != "" {
			label += " - " + option.Description
		}
		if option.Selected {
			nodes = append(nodes, Option(Value(option.ID), Selected(), Text(label)))
			continue
		}
		nodes = append(nodes, Option(Value(option.ID), Text(label)))
	}
	return nodes
}

func gitRepoSelectNodes(options []gitRepoSelectOption) []Node {
	nodes := make([]Node, 0, len(options))
	for i := range options {
		option := options[i]
		if option.Selected {
			nodes = append(nodes, Option(Value(option.ID), Selected(), Text(option.Label)))
			continue
		}
		nodes = append(nodes, Option(Value(option.ID), Text(option.Label)))
	}
	return nodes
}

func selectGitRepo(options []gitRepoSelectOption, selectedID string) []gitRepoSelectOption {
	items := make([]gitRepoSelectOption, 0, len(options))
	for i := range options {
		option := options[i]
		option.Selected = option.ID == selectedID
		items = append(items, option)
	}
	return items
}

func emptyDash(v string) string {
	if v == "" {
		return "-"
	}
	return v
}
