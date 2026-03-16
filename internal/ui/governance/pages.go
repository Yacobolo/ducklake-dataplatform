package governance

import (
	"sort"
	"strconv"

	"duck-demo/internal/domain"
	"duck-demo/internal/service/query"
	"duck-demo/internal/ui/core"

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
			governanceCard("Search", "Search schemas, tables, and columns across catalogs.", "/ui/governance/search"),
			governanceCard("Tags", "Manage tag definitions and assignments.", "/ui/governance/tags"),
			governanceCard("Lineage", "Inspect upstream, downstream, and column-level lineage.", "/ui/governance/lineage"),
			governanceCard("Audit Logs", "Inspect platform audit activity.", "/ui/governance/audit-logs"),
			governanceCard("Query History", "Review query execution history.", "/ui/governance/query-history"),
			governanceCard("Manifest", "Generate secure table manifests with files, filters, and masks.", "/ui/governance/manifest"),
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
		resultsNode = Div(Class("overflow-x-auto"), Table(Class("min-w-full text-left text-sm"),
			THead(Tr(Th(Text("Type")), Th(Text("Schema")), Th(Text("Table")), Th(Text("Name")), Th(Text("Match Field")))),
			TBody(Group(tableRows)),
		))
	}

	return core.AppPage("Governance: Search", "governance", principal,
		governanceSectionNav("search"),
		Div(Class("rounded-xl border border-[var(--borderColor-default)] bg-[var(--bgColor-default)] p-4 shadow-[var(--shadow-resting-xsmall)]"),
			H2(Class("mt-0 text-lg font-semibold"), Text("Search catalog metadata")),
			Form(Class("grid gap-3 md:grid-cols-3"), Method("get"), Action("/ui/governance/search"),
				Div(Label(Text("Query")), core.InputControl("", Name("q"), Value(queryText))),
				Div(Label(Text("Object type")), core.InputControl("", Name("object_type"), Value(objectType))),
				Div(Label(Text("Catalog")), core.InputControl("", Name("catalog"), Value(catalogName))),
				Div(Class("md:col-span-3"), core.PrimaryButton("", Type("submit"), Text("Search"))),
			),
		),
		Div(Class("rounded-xl border border-[var(--borderColor-default)] bg-[var(--bgColor-default)] p-4 shadow-[var(--shadow-resting-xsmall)]"), resultsNode),
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
				Td(Class("text-right"), Form(Method("post"), Action("/ui/governance/tags/"+row.ID+"/delete"), csrfFieldProvider(), core.DangerButton("small", Type("submit"), Text("Delete")))),
			))
		}
		table = Div(Class("overflow-x-auto"), Table(Class("min-w-full text-left text-sm"),
			THead(Tr(Th(Text("Key")), Th(Text("Value")), Th(Text("Created By")), Th(Text("Assignments")), Th(Class("text-right"), Text("Actions")))),
			TBody(Group(tableRows)),
		))
	}

	return core.AppPage("Governance: Tags", "governance", principal,
		governanceSectionNav("tags"),
		Div(Class("rounded-xl border border-[var(--borderColor-default)] bg-[var(--bgColor-default)] p-4 shadow-[var(--shadow-resting-xsmall)]"),
			H2(Class("mt-0 text-lg font-semibold"), Text("Create tag")),
			Form(Class("grid gap-3"), Method("post"), Action("/ui/governance/tags"),
				csrfFieldProvider(),
				Label(Text("Key")),
				core.InputControl("", Name("key"), Required()),
				Label(Text("Value")),
				core.InputControl("", Name("value")),
				Div(Class("mt-2"), core.PrimaryButton("", Type("submit"), Text("Create tag"))),
			),
		),
		Div(Class("rounded-xl border border-[var(--borderColor-default)] bg-[var(--bgColor-default)] p-4 shadow-[var(--shadow-resting-xsmall)]"),
			H2(Class("mt-0 text-lg font-semibold"), Text("Assign tag")),
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
		),
		Div(Class("rounded-xl border border-[var(--borderColor-default)] bg-[var(--bgColor-default)] p-4 shadow-[var(--shadow-resting-xsmall)]"),
			H2(Class("mt-0 text-lg font-semibold"), Text("Remove tag assignment")),
			Form(Class("grid gap-3"), Method("post"), Action("/ui/governance/tag-assignments/delete"),
				csrfFieldProvider(),
				Label(Text("Assignment ID")),
				core.InputControl("", Name("assignment_id"), Required()),
				Div(Class("mt-2"), core.DangerButton("", Type("submit"), Text("Remove assignment"))),
			),
		),
		Div(Class("rounded-xl border border-[var(--borderColor-default)] bg-[var(--bgColor-default)] p-4 shadow-[var(--shadow-resting-xsmall)]"),
			table,
			P(Class("mt-4 text-sm text-[var(--fgColor-muted)]"), Text("Showing up to "+strconv.Itoa(page.MaxResults)+" tags. Total: "+strconv.FormatInt(total, 10))),
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
		table = Div(Class("overflow-x-auto"), Table(Class("min-w-full text-left text-sm"),
			THead(Tr(Th(Text("Principal")), Th(Text("Action")), Th(Text("Status")), Th(Text("Created")))),
			TBody(Group(tableRows)),
		))
	}
	return core.AppPage("Governance: Audit Logs", "governance", principal,
		governanceSectionNav("audit"),
		Div(Class("rounded-xl border border-[var(--borderColor-default)] bg-[var(--bgColor-default)] p-4 shadow-[var(--shadow-resting-xsmall)]"),
			table,
			P(Class("mt-4 text-sm text-[var(--fgColor-muted)]"), Text("Showing up to "+strconv.Itoa(page.MaxResults)+" audit events. Total: "+strconv.FormatInt(total, 10))),
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
		table = Div(Class("overflow-x-auto"), Table(Class("min-w-full text-left text-sm"),
			THead(Tr(Th(Text("Principal")), Th(Text("Statement")), Th(Text("Status")), Th(Text("Created")))),
			TBody(Group(tableRows)),
		))
	}
	return core.AppPage("Governance: Query History", "governance", principal,
		governanceSectionNav("history"),
		Div(Class("rounded-xl border border-[var(--borderColor-default)] bg-[var(--bgColor-default)] p-4 shadow-[var(--shadow-resting-xsmall)]"),
			table,
			P(Class("mt-4 text-sm text-[var(--fgColor-muted)]"), Text("Showing up to "+strconv.Itoa(page.MaxResults)+" query history entries. Total: "+strconv.FormatInt(total, 10))),
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
			Div(Class("rounded-xl border border-[var(--borderColor-default)] bg-[var(--bgColor-default)] p-4 shadow-[var(--shadow-resting-xsmall)]"), H2(Class("mt-0 text-lg font-semibold"), Text("Manifest summary")), P(Text("Table: "+d.Result.Schema+"."+d.Result.Table)), P(Text("Expires at: "+formatTime(d.Result.ExpiresAt)))),
			Div(Class("rounded-xl border border-[var(--borderColor-default)] bg-[var(--bgColor-default)] p-4 shadow-[var(--shadow-resting-xsmall)]"), H3(Class("mt-0 text-lg font-semibold"), Text("Columns")), Div(Class("overflow-x-auto"), Table(Class("min-w-full text-left text-sm"), THead(Tr(Th(Text("Name")), Th(Text("Type")))), TBody(Group(columnRows))))),
			Div(Class("rounded-xl border border-[var(--borderColor-default)] bg-[var(--bgColor-default)] p-4 shadow-[var(--shadow-resting-xsmall)]"), H3(Class("mt-0 text-lg font-semibold"), Text("Files")), Ul(Group(fileRows))),
			Div(Class("rounded-xl border border-[var(--borderColor-default)] bg-[var(--bgColor-default)] p-4 shadow-[var(--shadow-resting-xsmall)]"), H3(Class("mt-0 text-lg font-semibold"), Text("Row filters")), Ul(Group(filterRows))),
			Div(Class("rounded-xl border border-[var(--borderColor-default)] bg-[var(--bgColor-default)] p-4 shadow-[var(--shadow-resting-xsmall)]"), H3(Class("mt-0 text-lg font-semibold"), Text("Column masks")), Div(Class("overflow-x-auto"), Table(Class("min-w-full text-left text-sm"), THead(Tr(Th(Text("Column")), Th(Text("Mask")))), TBody(Group(maskRows))))),
		})
	}
	return core.AppPage("Manifest", "governance", d.Principal,
		governanceSectionNav("manifest"),
		Div(Class("rounded-xl border border-[var(--borderColor-default)] bg-[var(--bgColor-default)] p-4 shadow-[var(--shadow-resting-xsmall)]"),
			H2(Class("mt-0 text-lg font-semibold"), Text("Generate manifest")),
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
				Td(Class("text-right"),
					Form(Method("post"), Action("/ui/governance/lineage/edges/"+row.ID+"/delete"),
						d.CSRFFieldProvider(),
						core.DangerButton("small", Type("submit"), Text("Delete edge")),
					),
				),
			))
		}
		upstreamTable = Div(Class("overflow-x-auto"),
			Table(Class("min-w-full text-left text-sm"),
				THead(Tr(Th(Text("Source")), Th(Text("Target")), Th(Text("Type")), Th(Class("text-right"), Text("Actions")))),
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
				Td(Class("text-right"),
					Form(Method("post"), Action("/ui/governance/lineage/edges/"+row.ID+"/delete"),
						d.CSRFFieldProvider(),
						core.DangerButton("small", Type("submit"), Text("Delete edge")),
					),
				),
			))
		}
		downstreamTable = Div(Class("overflow-x-auto"),
			Table(Class("min-w-full text-left text-sm"),
				THead(Tr(Th(Text("Source")), Th(Text("Target")), Th(Text("Type")), Th(Class("text-right"), Text("Actions")))),
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
		columnTable = Div(Class("overflow-x-auto"),
			Table(Class("min-w-full text-left text-sm"),
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
		impactTable = Div(Class("overflow-x-auto"),
			Table(Class("min-w-full text-left text-sm"),
				THead(Tr(Th(Text("Source Column")), Th(Text("Target Column")), Th(Text("Transform")), Th(Text("Function")))),
				TBody(Group(rows)),
			),
		)
	}

	return core.AppPage("Governance: Lineage", "governance", d.Principal,
		governanceSectionNav("lineage"),
		Div(Class("rounded-xl border border-[var(--borderColor-default)] bg-[var(--bgColor-default)] p-4 shadow-[var(--shadow-resting-xsmall)]"),
			H2(Class("mt-0 text-lg font-semibold"), Text("Inspect lineage")),
			Form(Class("grid gap-3 md:grid-cols-3"), Method("get"), Action("/ui/governance/lineage"),
				Div(Label(Text("Schema")), core.InputControl("", Name("schema"), Value(d.Schema))),
				Div(Label(Text("Table")), core.InputControl("", Name("table"), Value(d.Table))),
				Div(Label(Text("Source column impact")), core.InputControl("", Name("column"), Value(d.Column))),
				Div(Class("md:col-span-3"), core.PrimaryButton("", Type("submit"), Text("Load lineage"))),
			),
		),
		Div(Class("rounded-xl border border-[var(--borderColor-default)] bg-[var(--bgColor-default)] p-4 shadow-[var(--shadow-resting-xsmall)]"),
			H2(Class("mt-0 text-lg font-semibold"), Text("Purge lineage")),
			Form(Class("grid gap-3 md:grid-cols-[minmax(0,18rem)_auto] md:items-end"), Method("post"), Action("/ui/governance/lineage/purge"),
				d.CSRFFieldProvider(),
				Div(Label(Text("Delete edges older than days")), core.InputControl("", Name("older_than_days"), Value("30"))),
				Div(core.DangerButton("", Type("submit"), Text("Purge lineage"))),
			),
		),
		Div(Class("rounded-xl border border-[var(--borderColor-default)] bg-[var(--bgColor-default)] p-4 shadow-[var(--shadow-resting-xsmall)]"), H2(Class("mt-0 text-lg font-semibold"), Text("Upstream")), upstreamTable),
		Div(Class("rounded-xl border border-[var(--borderColor-default)] bg-[var(--bgColor-default)] p-4 shadow-[var(--shadow-resting-xsmall)]"), H2(Class("mt-0 text-lg font-semibold"), Text("Downstream")), downstreamTable),
		Div(Class("rounded-xl border border-[var(--borderColor-default)] bg-[var(--bgColor-default)] p-4 shadow-[var(--shadow-resting-xsmall)]"), H2(Class("mt-0 text-lg font-semibold"), Text("Column lineage")), columnTable),
		Div(Class("rounded-xl border border-[var(--borderColor-default)] bg-[var(--bgColor-default)] p-4 shadow-[var(--shadow-resting-xsmall)]"), H2(Class("mt-0 text-lg font-semibold"), Text("Column impact")), impactTable),
	)
}

func governanceSectionNav(active string) Node {
	return Div(Class("rounded-xl border border-[var(--borderColor-default)] bg-[var(--bgColor-default)] p-4 shadow-[var(--shadow-resting-xsmall)]"),
		Div(Class("flex flex-wrap gap-2"),
			navButton("Search", "/ui/governance/search", active == "search"),
			navButton("Tags", "/ui/governance/tags", active == "tags"),
			navButton("Lineage", "/ui/governance/lineage", active == "lineage"),
			navButton("Audit Logs", "/ui/governance/audit-logs", active == "audit"),
			navButton("Query History", "/ui/governance/query-history", active == "history"),
			navButton("Manifest", "/ui/governance/manifest", active == "manifest"),
		),
	)
}

func navButton(label, href string, active bool) Node {
	if active {
		return core.PrimaryLink(href, "", Text(label))
	}
	return core.SecondaryLink(href, "", Text(label))
}

func governanceCard(title, copy, href string) Node {
	return Div(Class("rounded-xl border border-[var(--borderColor-default)] bg-[var(--bgColor-default)] p-4 shadow-[var(--shadow-resting-xsmall)]"), H2(Class("mt-0 text-lg font-semibold"), Text(title)), P(Class("text-sm text-[var(--fgColor-muted)]"), Text(copy)), core.SecondaryLink(href, "", Text("Open")))
}
