package governance

import (
	"sort"
	"strconv"

	"github.com/Yacobolo/quackstack/internal/domain"
	"github.com/Yacobolo/quackstack/internal/service/query"
	"github.com/Yacobolo/quackstack/internal/ui/core"

	. "maragu.dev/gomponents"
	. "maragu.dev/gomponents/html"
)

type governanceTagRowData struct {
	ID          string
	Key         string
	Value       string
	CreatedBy   string
	Assignments int
}

type auditRowData struct {
	Principal string
	Action    string
	Status    string
	CreatedAt string
}

type queryHistoryRowData struct {
	Principal string
	Statement string
	Status    string
	CreatedAt string
}

type governanceLineagePageData struct {
	Principal         domain.ContextPrincipal
	Schema            string
	Table             string
	Column            string
	UpstreamRows      []governanceLineageEdgeRow
	DownstreamRows    []governanceLineageEdgeRow
	ColumnRows        []governanceColumnLineageRow
	ImpactRows        []governanceColumnLineageRow
	CSRFFieldProvider func() Node
}

type governanceLineageEdgeRow struct {
	ID     string
	Source string
	Target string
	Type   string
}

type governanceColumnLineageRow struct {
	TargetColumn string
	SourceColumn string
	Transform    string
	Function     string
}

type governanceManifestPageData struct {
	Principal         domain.ContextPrincipal
	CatalogName       string
	SchemaName        string
	TableName         string
	Result            *query.ManifestResult
	CSRFFieldProvider func() Node
}

func governanceHomePage(principal domain.ContextPrincipal) Node {
	return core.AppPage("Governance", "governance", principal,
		governanceSectionNav(""),
		Div(Class("grid gap-3 md:grid-cols-2 xl:grid-cols-3"),
			core.SectionSurface(core.SectionHeader("Search", "Search schemas, tables, and columns across catalogs.", core.SecondaryLink("/ui/governance/search", "", Text("Open")))),
			core.SectionSurface(core.SectionHeader("Tags", "Manage tag definitions and assignments.", core.SecondaryLink("/ui/governance/tags", "", Text("Open")))),
			core.SectionSurface(core.SectionHeader("Lineage", "Inspect upstream, downstream, and column-level lineage.", core.SecondaryLink("/ui/governance/lineage", "", Text("Open")))),
			core.SectionSurface(core.SectionHeader("Audit Logs", "Inspect platform audit activity.", core.SecondaryLink("/ui/governance/audit-logs", "", Text("Open")))),
			core.SectionSurface(core.SectionHeader("Query History", "Review query execution history.", core.SecondaryLink("/ui/governance/query-history", "", Text("Open")))),
			core.SectionSurface(core.SectionHeader("Manifest", "Generate secure table manifests with files, filters, and masks.", core.SecondaryLink("/ui/governance/manifest", "", Text("Open")))),
		),
	)
}

func governanceSearchPage(principal domain.ContextPrincipal, queryText, objectType, catalogName string, rows []domain.SearchResult) Node {
	resultsNode := Node(P(Class("text-xs text-[var(--fgColor-muted)]"), Text("Run a search to see matching objects.")))
	if len(rows) > 0 {
		tableRows := make([]Node, 0, len(rows))
		for i := range rows {
			row := rows[i]
			tableRows = append(tableRows, Tr(
				Td(Text(row.Type)),
				Td(Text(strOrDash(row.SchemaName))),
				Td(Text(strOrDash(row.TableName))),
				Td(Text(row.Name)),
				Td(Text(valueOrDash(row.MatchField))),
			))
		}
		resultsNode = core.TableContainer("",
			core.DataTable("",
				THead(Tr(Th(Text("Type")), Th(Text("Schema")), Th(Text("Table")), Th(Text("Name")), Th(Text("Match Field")))),
				TBody(Group(tableRows)),
			),
		)
	}

	return core.AppPage("Governance: Search", "governance", principal,
		governanceSectionNav("search"),
		core.PageHeader("Operate", "Governance search", "Search across catalog metadata, then pivot into the deeper governance workspaces only when you need assignment, lineage, or policy detail."),
		core.SectionSurface(
			core.SectionHeader("Search catalog metadata", "Use broad search as the default governance starting point."),
			Form(Class("grid gap-3 md:grid-cols-3"), Method("get"), Action("/ui/governance/search"),
				Div(Label(Text("Query")), core.InputControl("", Name("q"), Value(queryText))),
				Div(Label(Text("Object type")), core.InputControl("", Name("object_type"), Value(objectType))),
				Div(Label(Text("Catalog")), core.InputControl("", Name("catalog"), Value(catalogName))),
				Div(Class("md:col-span-3"), core.PrimaryButton("", Type("submit"), Text("Search"))),
			),
		),
		core.SectionSurface(
			core.SectionHeader("Results", "Search results stay separate from authoring actions so the page reads as an inspection workspace."),
			resultsNode,
		),
	)
}

func governanceTagsPage(principal domain.ContextPrincipal, rows []governanceTagRowData, page domain.PageRequest, total int64, csrfFieldProvider func() Node) Node {
	table := Node(P(Class("text-xs text-[var(--fgColor-muted)]"), Text("No tags defined yet.")))
	if len(rows) > 0 {
		tableRows := make([]Node, 0, len(rows))
		for i := range rows {
			row := rows[i]
			tableRows = append(tableRows, Tr(
				Td(Text(row.Key)),
				Td(Text(row.Value)),
				Td(Text(row.CreatedBy)),
				Td(Text(strconv.Itoa(row.Assignments))),
				core.TableActionCell(core.TableIconActionPost("/ui/governance/tags/"+row.ID+"/delete", "Delete tag", "x", "danger", csrfFieldProvider)),
			))
		}
		table = core.TableContainer("",
			core.DataTable("",
				THead(Tr(Th(Text("Key")), Th(Text("Value")), Th(Text("Created By")), Th(Text("Assignments")), core.TableActionHeader())),
				TBody(Group(tableRows)),
			),
		)
	}

	return core.AppPage("Governance: Tags", "governance", principal,
		governanceSectionNav("tags"),
		core.PageHeader("Operate", "Governance tags", "Separate tag authoring from inspection so it is obvious whether you are defining governance metadata or reviewing the current tag catalog."),
		Div(Class("grid gap-4 xl:grid-cols-[minmax(0,1fr)_minmax(20rem,0.9fr)]"),
			core.SectionSurface(
				core.SectionHeader("Tag catalog", "Review current tag definitions before creating or updating assignments."),
				table,
				P(Class("mt-4 text-sm text-[var(--fgColor-muted)]"), Text("Showing up to "+strconv.Itoa(page.MaxResults)+" tags. Total: "+strconv.FormatInt(total, 10))),
			),
			Div(Class("grid gap-4"),
				core.SectionSurface(
					core.SectionHeader("Create tag", "Define reusable governance metadata."),
					Form(Class("grid gap-3"), Method("post"), Action("/ui/governance/tags"),
						csrfFieldProvider(),
						Label(Text("Key")),
						core.InputControl("", Name("key"), Required()),
						Label(Text("Value")),
						core.InputControl("", Name("value")),
						Div(Class("mt-2"), core.PrimaryButton("", Type("submit"), Text("Create tag"))),
					),
				),
				core.SectionSurface(
					core.SectionHeader("Manage assignments", "Attach a tag to a securable or remove an existing assignment."),
					Form(Class("grid gap-3"), Method("post"), Action("/ui/governance/tag-assignments"),
						csrfFieldProvider(),
						Label(Text("Tag ID")),
						core.InputControl("", Name("tag_id"), Required()),
						Label(Text("Securable type")),
						core.InputControl("", Name("securable_type"), Required()),
						Label(Text("Securable ID")),
						core.InputControl("", Name("securable_id"), Required()),
						Label(Text("Column name")),
						core.InputControl("", Name("column_name")),
						Div(Class("mt-2"), core.SecondaryButton("", Type("submit"), Text("Assign tag"))),
					),
					Form(Class("grid gap-3 border-t border-[var(--borderColor-default)] pt-4"), Method("post"), Action("/ui/governance/tag-assignments/delete"),
						csrfFieldProvider(),
						Label(Text("Assignment ID")),
						core.InputControl("", Name("assignment_id"), Required()),
						Div(Class("mt-2"), core.DangerButton("", Type("submit"), Text("Remove assignment"))),
					),
				),
			),
		),
	)
}

func governanceAuditLogsPage(principal domain.ContextPrincipal, rows []auditRowData, page domain.PageRequest, total int64) Node {
	table := Node(P(Class("text-xs text-[var(--fgColor-muted)]"), Text("No audit logs found.")))
	if len(rows) > 0 {
		tableRows := make([]Node, 0, len(rows))
		for i := range rows {
			row := rows[i]
			tableRows = append(tableRows, Tr(Td(Text(row.Principal)), Td(Text(row.Action)), Td(Text(row.Status)), Td(Text(row.CreatedAt))))
		}
		table = core.TableContainer("",
			core.DataTable("",
				THead(Tr(Th(Scope("col"), Text("Principal")), Th(Scope("col"), Text("Action")), Th(Scope("col"), Text("Status")), Th(Scope("col"), Text("Created")))),
				TBody(Group(tableRows)),
			),
		)
	}
	return core.AppPage("Governance: Audit Logs", "governance", principal,
		governanceSectionNav("audit"),
		core.ListPageLayout(
			core.ListPageHeader("Audit logs", "Review governance activity in a dedicated inspection workspace."),
			core.ListPageBody(
				table,
				core.ListPagination("/ui/governance/audit-logs", page, total),
			),
		),
	)
}

func governanceQueryHistoryPage(principal domain.ContextPrincipal, rows []queryHistoryRowData, page domain.PageRequest, total int64) Node {
	table := Node(P(Class("text-xs text-[var(--fgColor-muted)]"), Text("No query history found.")))
	if len(rows) > 0 {
		tableRows := make([]Node, 0, len(rows))
		for i := range rows {
			row := rows[i]
			tableRows = append(tableRows, Tr(Td(Text(row.Principal)), Td(Text(row.Statement)), Td(Text(row.Status)), Td(Text(row.CreatedAt))))
		}
		table = core.TableContainer("",
			core.DataTable("",
				THead(Tr(Th(Scope("col"), Text("Principal")), Th(Scope("col"), Text("Statement")), Th(Scope("col"), Text("Status")), Th(Scope("col"), Text("Created")))),
				TBody(Group(tableRows)),
			),
		)
	}
	return core.AppPage("Governance: Query History", "governance", principal,
		governanceSectionNav("history"),
		core.ListPageLayout(
			core.ListPageHeader("Query history", "Keep query inspection separate from policy authoring workflows."),
			core.ListPageBody(
				table,
				core.ListPagination("/ui/governance/query-history", page, total),
			),
		),
	)
}

func governanceManifestPage(d governanceManifestPageData) Node {
	resultNode := Node(nil)
	if d.Result != nil {
		columnRows := make([]Node, 0, len(d.Result.Columns))
		for i := range d.Result.Columns {
			columnRows = append(columnRows, Tr(Td(Text(d.Result.Columns[i].Name)), Td(Text(d.Result.Columns[i].Type))))
		}
		fileRows := make([]Node, 0, len(d.Result.Files))
		for i := range d.Result.Files {
			fileRows = append(fileRows, Li(Code(Text(d.Result.Files[i]))))
		}
		filterRows := make([]Node, 0, len(d.Result.RowFilters))
		for i := range d.Result.RowFilters {
			filterRows = append(filterRows, Li(Code(Text(d.Result.RowFilters[i]))))
		}
		maskKeys := make([]string, 0, len(d.Result.ColumnMasks))
		for column := range d.Result.ColumnMasks {
			maskKeys = append(maskKeys, column)
		}
		sort.Strings(maskKeys)
		maskRows := make([]Node, 0, len(maskKeys))
		for _, column := range maskKeys {
			maskRows = append(maskRows, Tr(Td(Text(column)), Td(Code(Text(d.Result.ColumnMasks[column])))))
		}
		resultNode = Group([]Node{
			core.SectionSurface(
				core.SectionHeader("Manifest summary", "Manifest output reads like a report instead of a stack of generic cards."),
				core.KeyValueGrid([][2]string{
					{"Table", d.Result.Schema + "." + d.Result.Table},
					{"Expires at", formatTime(d.Result.ExpiresAt)},
				}),
			),
			core.SectionSurface(
				core.SectionHeader("Columns", ""),
				core.TableContainer("", core.DataTable("", THead(Tr(Th(Text("Name")), Th(Text("Type")))), TBody(Group(columnRows)))),
			),
			core.SectionSurface(
				core.SectionHeader("Files", ""),
				Ul(Group(fileRows)),
			),
			core.SectionSurface(
				core.SectionHeader("Row filters", ""),
				Ul(Group(filterRows)),
			),
			core.SectionSurface(
				core.SectionHeader("Column masks", ""),
				core.TableContainer("", core.DataTable("", THead(Tr(Th(Text("Column")), Th(Text("Mask")))), TBody(Group(maskRows)))),
			),
		})
	}
	return core.AppPage("Manifest", "governance", d.Principal,
		governanceSectionNav("manifest"),
		core.ResultPageLayout("Operate", "Manifest", "Manifest generation uses a shared result layout so the request form and output read as one workflow.",
			core.SectionSurface(
				core.SectionHeader("Generate manifest", "Generate secure table manifests with files, filters, and masks."),
				Form(Class("grid gap-3"), Method("post"), Action("/ui/governance/manifest"),
					d.CSRFFieldProvider(),
					Label(Text("Catalog")),
					core.InputControl("", Name("catalog_name"), Value(d.CatalogName)),
					Label(Text("Schema")),
					core.InputControl("", Name("schema_name"), Value(d.SchemaName), Required()),
					Label(Text("Table")),
					core.InputControl("", Name("table_name"), Value(d.TableName), Required()),
					core.PrimaryButton("", Type("submit"), Text("Generate manifest")),
				),
			),
			resultNode,
		),
	)
}

func governanceLineagePage(d governanceLineagePageData) Node {
	upstreamTable := Node(P(Class("text-xs text-[var(--fgColor-muted)]"), Text("No upstream lineage found.")))
	if len(d.UpstreamRows) > 0 {
		rows := make([]Node, 0, len(d.UpstreamRows))
		for i := range d.UpstreamRows {
			row := d.UpstreamRows[i]
			rows = append(rows, Tr(
				Td(Text(row.Source)),
				Td(Text(row.Target)),
				Td(Text(row.Type)),
				core.TableActionCell(core.TableIconActionPost("/ui/governance/lineage/edges/"+row.ID+"/delete", "Delete edge", "x", "danger", d.CSRFFieldProvider)),
			))
		}
		upstreamTable = core.TableContainer("",
			core.DataTable("",
				THead(Tr(Th(Text("Source")), Th(Text("Target")), Th(Text("Type")), core.TableActionHeader())),
				TBody(Group(rows)),
			),
		)
	}

	downstreamTable := Node(P(Class("text-xs text-[var(--fgColor-muted)]"), Text("No downstream lineage found.")))
	if len(d.DownstreamRows) > 0 {
		rows := make([]Node, 0, len(d.DownstreamRows))
		for i := range d.DownstreamRows {
			row := d.DownstreamRows[i]
			rows = append(rows, Tr(
				Td(Text(row.Source)),
				Td(Text(row.Target)),
				Td(Text(row.Type)),
				core.TableActionCell(core.TableIconActionPost("/ui/governance/lineage/edges/"+row.ID+"/delete", "Delete edge", "x", "danger", d.CSRFFieldProvider)),
			))
		}
		downstreamTable = core.TableContainer("",
			core.DataTable("",
				THead(Tr(Th(Text("Source")), Th(Text("Target")), Th(Text("Type")), core.TableActionHeader())),
				TBody(Group(rows)),
			),
		)
	}

	columnTable := Node(P(Class("text-xs text-[var(--fgColor-muted)]"), Text("No column lineage found.")))
	if len(d.ColumnRows) > 0 {
		rows := make([]Node, 0, len(d.ColumnRows))
		for i := range d.ColumnRows {
			row := d.ColumnRows[i]
			rows = append(rows, Tr(Td(Text(row.TargetColumn)), Td(Text(row.SourceColumn)), Td(Text(row.Transform)), Td(Text(row.Function))))
		}
		columnTable = core.TableContainer("",
			core.DataTable("",
				THead(Tr(Th(Text("Target Column")), Th(Text("Source Column")), Th(Text("Transform")), Th(Text("Function")))),
				TBody(Group(rows)),
			),
		)
	}

	impactTable := Node(P(Class("text-xs text-[var(--fgColor-muted)]"), Text("No column impact found.")))
	if len(d.ImpactRows) > 0 {
		rows := make([]Node, 0, len(d.ImpactRows))
		for i := range d.ImpactRows {
			row := d.ImpactRows[i]
			rows = append(rows, Tr(Td(Text(row.SourceColumn)), Td(Text(row.TargetColumn)), Td(Text(row.Transform)), Td(Text(row.Function))))
		}
		impactTable = core.TableContainer("",
			core.DataTable("",
				THead(Tr(Th(Text("Source Column")), Th(Text("Target Column")), Th(Text("Transform")), Th(Text("Function")))),
				TBody(Group(rows)),
			),
		)
	}

	return core.AppPage("Governance: Lineage", "governance", d.Principal,
		governanceSectionNav("lineage"),
		core.ResultPageLayout("Operate", "Governance lineage", "Lineage inspection now follows the same report-style governance layout as manifest and search results.",
			core.SectionSurface(
				core.SectionHeader("Inspect lineage", "Load lineage by schema, table, and optional source column."),
				Form(Class("grid gap-3 md:grid-cols-3"), Method("get"), Action("/ui/governance/lineage"),
					Div(Label(Text("Schema")), core.InputControl("", Name("schema"), Value(d.Schema))),
					Div(Label(Text("Table")), core.InputControl("", Name("table"), Value(d.Table))),
					Div(Label(Text("Source column impact")), core.InputControl("", Name("column"), Value(d.Column))),
					Div(Class("md:col-span-3"), core.PrimaryButton("", Type("submit"), Text("Load lineage"))),
				),
			),
			core.SectionSurface(
				core.SectionHeader("Purge lineage", "Administrative cleanup remains available, but separate from inspection results."),
				Form(Class("grid gap-3 md:grid-cols-[minmax(0,18rem)_auto] md:items-end"), Method("post"), Action("/ui/governance/lineage/purge"),
					d.CSRFFieldProvider(),
					Div(Label(Text("Delete edges older than days")), core.InputControl("", Name("older_than_days"), Value("30"))),
					Div(core.DangerButton("", Type("submit"), Text("Purge lineage"))),
				),
			),
			core.SectionSurface(core.SectionHeader("Upstream", ""), upstreamTable),
			core.SectionSurface(core.SectionHeader("Downstream", ""), downstreamTable),
			core.SectionSurface(core.SectionHeader("Column lineage", ""), columnTable),
			core.SectionSurface(core.SectionHeader("Column impact", ""), impactTable),
		),
	)
}

func governanceSectionNav(active string) Node {
	return core.SectionTabs([]core.SectionTab{
		{Label: "Search", Href: "/ui/governance/search", Active: active == "search"},
		{Label: "Tags", Href: "/ui/governance/tags", Active: active == "tags"},
		{Label: "Lineage", Href: "/ui/governance/lineage", Active: active == "lineage"},
		{Label: "Audit Logs", Href: "/ui/governance/audit-logs", Active: active == "audit"},
		{Label: "Query History", Href: "/ui/governance/query-history", Active: active == "history"},
		{Label: "Manifest", Href: "/ui/governance/manifest", Active: active == "manifest"},
	})
}
