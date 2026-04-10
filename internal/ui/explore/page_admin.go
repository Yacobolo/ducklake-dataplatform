package explore

import (
	"strconv"

	"duck-demo/internal/domain"
	"duck-demo/internal/ui/core"

	. "maragu.dev/gomponents"
	. "maragu.dev/gomponents/html"
)

type accessShareRow struct {
	Principal string
	Role      string
	DeleteURL string
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

type gitRepoRow struct {
	ID         string
	URL        string
	Repository string
	Branch     string
	Path       string
	Owner      string
	LastSync   string
}

type gitReposListPageData struct {
	Principal domain.ContextPrincipal
	Rows      []gitRepoRow
	Page      domain.PageRequest
	Total     int64
}

func folderNewPage(principal domain.ContextPrincipal, folderOptions []folderSelectOption, gitRepoOptions []gitRepoSelectOption, csrfFieldProvider func() Node) Node {
	parentFolderNodes := append([]Node{Name("parent_folder_id"), Option(Value(""), Text("Personal root"))}, folderSelectNodes(folderOptions)...)
	gitRepoNodes := append([]Node{Name("git_repo_id"), Option(Value(""), Text("No Git repo"))}, gitRepoSelectNodes(gitRepoOptions)...)
	return formPage(principal, "New Folder", "/ui/explore/folders", csrfFieldProvider,
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

func folderEditPage(principal domain.ContextPrincipal, folder *domain.Folder, parentFolderOptions []folderSelectOption, gitRepoOptions []gitRepoSelectOption, shares []accessShareRow, csrfFieldProvider func() Node) Node {
	gitRepoID := stringValue(folder.GitRepoID)
	gitRootPath := stringValue(folder.GitRootPath)
	defaultProjectID := stringValue(folder.DefaultProjectID)
	defaultEnvironmentID := stringValue(folder.DefaultEnvironmentID)
	gitRepoNodes := append([]Node{Name("git_repo_id"), Option(Value(""), Text("No Git repo"))}, gitRepoSelectNodes(selectGitRepo(gitRepoOptions, gitRepoID))...)
	parentFolderNodes := append([]Node{Name("parent_folder_id"), Option(Value(""), Text("Personal root"))}, folderSelectNodes(parentFolderOptions)...)

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
	if folder.SystemRole == nil || *folder.SystemRole != domain.FolderSystemRoleWorkspaceRoot {
		deleteNode = Div(Class("mt-2"),
			Form(Method("post"), Action("/ui/explore/folders/"+folder.ID+"/delete"), Class("m-0"), csrfFieldProvider(), core.DangerButton("", Type("submit"), Text("Delete folder"))),
		)
	}

	formNodes := []Node{csrfFieldProvider()}
	formNodes = append(formNodes, fields...)
	formNodes = append(formNodes, Div(Class("mt-3"), core.PrimaryButton("", Type("submit"), Text("Save"))))

	return core.AppPage(
		"Edit Folder",
		"explore",
		principal,
		core.FormPageLayout("Discover", "Edit Folder", "Folder defaults control asset organization and inherited execution context.",
			core.SectionSurface(
				core.SectionHeader("Folder details", "Update folder-level defaults. Personal roots are protected."),
				P(Class("m-0 text-sm text-[var(--fgColor-muted)]"), Text("Path: "+folder.Path)),
				Form(Method("post"), Action("/ui/explore/folders/"+folder.ID+"/update"), Class("grid gap-3"), Group(formNodes)),
				deleteNode,
			),
			core.SectionSurface(
				core.SectionHeader("Move folder", "Re-parent this folder beneath a different parent. Cross-repo moves stay blocked."),
				Form(Method("post"), Action("/ui/explore/folders/"+folder.ID+"/move"), Class("grid gap-3"), csrfFieldProvider(),
					Label(Text("New parent folder")),
					core.SelectControl("", parentFolderNodes...),
					Label(Class("inline-flex items-center gap-2"), Input(Type("checkbox"), Name("confirm_leave_git"), Value("true")), Span(Text("I understand this move may remove inherited Git governance."))),
					Label(Class("inline-flex items-center gap-2"), Input(Type("checkbox"), Name("confirm_context_change"), Value("true")), Span(Text("I understand this move may change project or environment context."))),
					Div(Class("mt-3"), core.PrimaryButton("", Type("submit"), Text("Move folder"))),
				),
			),
			shareManagementSection("Folder sharing", shares, "/ui/explore/folders/"+folder.ID+"/share", csrfFieldProvider),
		),
	)
}

func gitReposNewPage(principal domain.ContextPrincipal, csrfFieldProvider func() Node) Node {
	return formPage(principal, "Register Git Repo", "/ui/explore/git-repos", csrfFieldProvider,
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

func gitReposListPage(d gitReposListPageData) Node {
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
	body := []Node{core.PageHeader("Discover", "Git repos", "Registered sources for folder-backed authoring sync.", core.PrimaryLink("/ui/explore/git-repos/new", "", Text("Register Git repo")))}
	if len(rows) == 0 {
		body = append(body, core.ListPageBody(core.WorkspaceEmptyState("git-branch", "No Git repositories registered.", "Register a repository to connect folder-backed sync.", core.PrimaryLink("/ui/explore/git-repos/new", "", Text("Register Git repo")))))
	} else {
		body = append(body, core.ListPageBody(table([]string{"Repository", "Branch", "Path", "Owner", "Last sync"}, rows), core.ListPagination("/ui/explore/git-repos", d.Page, d.Total)))
	}
	return core.AppPage("Notebook Git Repos", "explore", d.Principal, body...)
}

type gitRepoDetailPageData struct {
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

func gitRepoDetailPage(d gitRepoDetailPageData) Node {
	return core.AppPage(
		"Git Repo",
		"explore",
		d.Principal,
		core.DetailShell(
			core.PageHeader("Discover", "Git repo", "Repository details and sync controls.", core.SecondaryLink("/ui/explore/git-repos", "", Text("Back to repos"))),
			core.DetailLayout(
				core.DetailMain(core.SectionSurface(
					core.SectionHeader("Repository details", "Keep current sync configuration in the main detail column."),
					core.KeyValueGrid([][2]string{{"Repository", d.URL}, {"Branch", d.Branch}, {"Path", d.Path}, {"Owner", d.Owner}, {"Last sync", d.LastSync}, {"Last commit", d.LastCommit}}),
				)),
				core.DetailRail(core.DetailRailCard("Actions", "Sync and deletion stay together in the secondary rail.",
					core.ButtonGroup("",
						Form(Method("post"), Action(d.SyncURL), d.CSRFFieldFunc(), core.PrimaryButton("", Type("submit"), Text("Sync repo"))),
						Form(Method("post"), Action(d.DeleteURL), d.CSRFFieldFunc(), core.DangerButton("", Type("submit"), Text("Delete repo"))),
					),
				)),
			),
		),
	)
}

type gitRepoSyncResultPageData struct {
	Principal domain.ContextPrincipal
	GitRepoID string
	Result    *domain.GitSyncResult
}

func gitRepoSyncResultPage(d gitRepoSyncResultPageData) Node {
	if d.Result == nil {
		return core.AppPage("Git Sync", "explore", d.Principal, core.ListPageBody(core.WorkspaceEmptyState("git-branch", "No sync result available.", "Run a sync first to generate a result summary.", core.SecondaryLink("/ui/explore/git-repos/"+d.GitRepoID, "", Text("Back to repo")))))
	}
	return core.AppPage(
		"Git Sync",
		"explore",
		d.Principal,
		core.ResultPageLayout("Discover", "Git sync", "Sync outcomes use the shared result layout so they read like execution reports.",
			core.PageHeader("", "Git sync", "Latest sync result.", core.SecondaryLink("/ui/explore/git-repos/"+d.GitRepoID, "", Text("Back to repo"))),
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

type gitRepoSyncUnavailablePageData struct {
	Principal domain.ContextPrincipal
	GitRepoID string
	RepoURL   string
	Branch    string
	Path      string
	Message   string
}

func gitRepoSyncUnavailablePage(d gitRepoSyncUnavailablePageData) Node {
	return core.AppPage(
		"Git Sync Unavailable",
		"explore",
		d.Principal,
		core.ResultPageLayout("Discover", "Git sync unavailable", "Unavailable states use the same result-oriented layout as other notebook outcomes.",
			core.PageHeader("", "Git sync", "Sync is not available yet.", core.SecondaryLink("/ui/explore/git-repos/"+d.GitRepoID, "", Text("Back to repo"))),
			core.SectionSurface(
				core.SectionHeader("Sync is not available yet", "The repo is registered correctly, but server-side sync execution has not been implemented yet."),
				P(Text(d.Message)),
				core.KeyValueGrid([][2]string{{"Repository", d.RepoURL}, {"Branch", d.Branch}, {"Path", d.Path}}),
			),
		),
	)
}

func formPage(principal domain.ContextPrincipal, title, action string, csrfFieldProvider func() Node, fields ...Node) Node {
	nodes := []Node{csrfFieldProvider()}
	nodes = append(nodes, fields...)
	nodes = append(nodes, Div(Class("mt-3"), core.PrimaryButton("", Type("submit"), Text("Save"))))
	return core.AppPage(
		title,
		"explore",
		principal,
		core.FormPageLayout("Build", title, "Notebook authoring uses the shared single-surface form layout.",
			Form(Method("post"), Action(action), Class("grid gap-3"), Group(nodes)),
		),
	)
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
		body = table([]string{"Principal", "Role", "Action"}, rows)
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
