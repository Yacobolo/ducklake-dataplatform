package notebooks

import (
	"strconv"

	"duck-demo/internal/domain"
	"duck-demo/internal/ui/core"

	. "maragu.dev/gomponents"
	. "maragu.dev/gomponents/html"
)

type folderSelectOption struct {
	ID          string
	Label       string
	Description string
	Selected    bool
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
	return core.AppPage("Notebook Jobs", "explore", d.Principal, body...)
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
		"explore",
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
							{"Error", valueOrDash(d.ErrorText)},
						}),
					),
				),
			),
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

func notebookFormPage(principal domain.ContextPrincipal, title, action string, csrfFieldProvider func() Node, fields ...Node) Node {
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
