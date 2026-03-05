package ui

import (
	"duck-demo/internal/domain"

	. "maragu.dev/gomponents"
	data "maragu.dev/gomponents-datastar"
	. "maragu.dev/gomponents/html"
)

type assetsListRowData struct {
	Filter   string
	AssetKey string
	URL      string
	Type     string
	Owner    string
	Active   bool
	Updated  string
}

func assetsListPage(principal domain.ContextPrincipal, rows []assetsListRowData, page domain.PageRequest, total int64) Node {
	tableRows := make([]Node, 0, len(rows))
	for i := range rows {
		row := rows[i]
		tone := "severe"
		if row.Active {
			tone = "success"
		}
		tableRows = append(tableRows,
			Tr(
				data.Show(containsExpr(row.Filter)),
				Td(A(Href(row.URL), Text(row.AssetKey))),
				Td(statusLabel(row.Type, "accent")),
				Td(Text(row.Owner)),
				Td(statusLabel(boolLabel(row.Active), tone)),
				Td(Text(row.Updated)),
			),
		)
	}

	tableNode := Node(emptyStateCard("No assets found.", "", ""))
	if len(tableRows) > 0 {
		tableNode = Div(
			Class(cardClass("table-wrap")),
			Table(
				Class("data-table"),
				THead(Tr(Th(Text("Asset key")), Th(Text("Type")), Th(Text("Owner")), Th(Text("Active")), Th(Text("Updated")))),
				TBody(Group(tableRows)),
			),
		)
	}

	return appPage(
		"Assets",
		"assets",
		principal,
		quickFilterCard("Filter by asset key, type, or owner"),
		tableNode,
		paginationCard("/ui/assets", page, total),
	)
}

type assetDetailPageData struct {
	Principal           domain.ContextPrincipal
	AssetKey            string
	AssetType           string
	Owner               string
	Description         string
	IOProfile           string
	IsActive            bool
	UpdatedAt           string
	UpstreamAssetKeys   []string
	DownstreamAssetKeys []string
	Runs                []domain.AssetRun
	Materializations    []domain.AssetMaterialization
	Checks              []domain.AssetCheck
	Partitions          []domain.AssetPartition
	Backfills           []domain.BackfillRequest
	CSRFFieldFunc       func() Node
}

func assetDetailPage(d assetDetailPageData) Node {
	upstream := Node(P(Class("color-fg-muted"), Text("No upstream dependencies.")))
	if len(d.UpstreamAssetKeys) > 0 {
		items := make([]Node, 0, len(d.UpstreamAssetKeys))
		for i := range d.UpstreamAssetKeys {
			items = append(items, Li(A(Href("/ui/assets/"+d.UpstreamAssetKeys[i]), Text(d.UpstreamAssetKeys[i]))))
		}
		upstream = Ul(Group(items))
	}

	downstream := Node(P(Class("color-fg-muted"), Text("No downstream dependencies.")))
	if len(d.DownstreamAssetKeys) > 0 {
		items := make([]Node, 0, len(d.DownstreamAssetKeys))
		for i := range d.DownstreamAssetKeys {
			items = append(items, Li(A(Href("/ui/assets/"+d.DownstreamAssetKeys[i]), Text(d.DownstreamAssetKeys[i]))))
		}
		downstream = Ul(Group(items))
	}

	runRows := make([]Node, 0, len(d.Runs))
	for i := range d.Runs {
		r := d.Runs[i]
		runRows = append(runRows, Tr(Td(Text(r.ID)), Td(statusLabel(r.Status, "attention")), Td(Text(r.TriggerType)), Td(Text(formatTimePtr(r.StartedAt))), Td(Text(formatTimePtr(r.FinishedAt)))))
	}
	runsTable := Node(P(Class("color-fg-muted"), Text("No runs yet.")))
	if len(runRows) > 0 {
		runsTable = Table(Class("data-table"), THead(Tr(Th(Text("Run ID")), Th(Text("Status")), Th(Text("Trigger")), Th(Text("Started")), Th(Text("Finished")))), TBody(Group(runRows)))
	}

	matRows := make([]Node, 0, len(d.Materializations))
	for i := range d.Materializations {
		m := d.Materializations[i]
		matRows = append(matRows, Tr(Td(Text(m.ID)), Td(Text(stringPtr(m.PartitionKey))), Td(Text(formatTime(m.MaterializedAt)))))
	}
	matTable := Node(P(Class("color-fg-muted"), Text("No materializations yet.")))
	if len(matRows) > 0 {
		matTable = Table(Class("data-table"), THead(Tr(Th(Text("ID")), Th(Text("Partition")), Th(Text("Materialized At")))), TBody(Group(matRows)))
	}

	checkRows := make([]Node, 0, len(d.Checks))
	for i := range d.Checks {
		c := d.Checks[i]
		enabledTone := "severe"
		if c.Enabled {
			enabledTone = "success"
		}
		checkRows = append(checkRows, Tr(Td(Text(c.Name)), Td(Text(c.CheckType)), Td(Text(c.Severity)), Td(statusLabel(boolLabel(c.Enabled), enabledTone))))
	}
	checksTable := Node(P(Class("color-fg-muted"), Text("No checks configured.")))
	if len(checkRows) > 0 {
		checksTable = Table(Class("data-table"), THead(Tr(Th(Text("Name")), Th(Text("Type")), Th(Text("Severity")), Th(Text("Enabled")))), TBody(Group(checkRows)))
	}

	partitionRows := make([]Node, 0, len(d.Partitions))
	for i := range d.Partitions {
		p := d.Partitions[i]
		partitionRows = append(partitionRows, Tr(Td(Text(p.PartitionKey)), Td(Text(p.Status)), Td(Text(formatTimePtr(p.LastMaterializedAt)))))
	}
	partitionsTable := Node(P(Class("color-fg-muted"), Text("No partitions recorded.")))
	if len(partitionRows) > 0 {
		partitionsTable = Table(Class("data-table"), THead(Tr(Th(Text("Partition")), Th(Text("Status")), Th(Text("Last Materialized")))), TBody(Group(partitionRows)))
	}

	backfillRows := make([]Node, 0, len(d.Backfills))
	for i := range d.Backfills {
		b := d.Backfills[i]
		backfillRows = append(backfillRows, Tr(Td(Text(b.ID)), Td(Text(b.PartitionFrom+" -> "+b.PartitionTo)), Td(Text(b.Status)), Td(Text(b.RequestedBy)), Td(Text(formatTime(b.CreatedAt)))))
	}
	backfillsTable := Node(P(Class("color-fg-muted"), Text("No backfills requested.")))
	if len(backfillRows) > 0 {
		backfillsTable = Table(Class("data-table"), THead(Tr(Th(Text("ID")), Th(Text("Range")), Th(Text("Status")), Th(Text("Requested By")), Th(Text("Created")))), TBody(Group(backfillRows)))
	}

	return appPage(
		"Asset: "+d.AssetKey,
		"assets",
		d.Principal,
		Div(Class(cardClass()),
			P(Text("Type: "+d.AssetType)),
			P(Text("Owner: "+d.Owner)),
			P(Text("Description: "+fallbackString(d.Description, "-"))),
			P(Text("IO Profile: "+fallbackString(d.IOProfile, "-"))),
			P(Text("Active: "+boolLabel(d.IsActive))),
			P(Text("Updated: "+d.UpdatedAt)),
			Div(Class("d-flex flex-wrap gap-2 mt-2"),
				Form(Method("post"), Action("/ui/assets/"+d.AssetKey+"/materialize"), d.CSRFFieldFunc(), Input(Type("text"), Name("partition_key"), Placeholder("Partition key (optional)")), Button(Type("submit"), Class(primaryButtonClass()), Text("Trigger materialization"))),
				Form(Method("post"), Action("/ui/assets/"+d.AssetKey+"/backfills"), d.CSRFFieldFunc(), Input(Type("text"), Name("partition_from"), Placeholder("partition_from (YYYY-MM-DD)"), Required()), Input(Type("text"), Name("partition_to"), Placeholder("partition_to (YYYY-MM-DD)"), Required()), Input(Type("number"), Name("max_parallelism"), Placeholder("max parallelism")), Button(Type("submit"), Class(secondaryButtonClass()), Text("Create backfill"))),
			),
		),
		Div(Class(cardClass("table-wrap")), H2(Text("Dependencies")), H3(Text("Upstream")), upstream, H3(Text("Downstream")), downstream),
		Div(Class(cardClass("table-wrap")), H2(Text("Recent Runs")), runsTable),
		Div(Class(cardClass("table-wrap")), H2(Text("Materializations")), matTable),
		Div(Class(cardClass("table-wrap")), H2(Text("Checks")), checksTable),
		Div(Class(cardClass("table-wrap")), H2(Text("Partitions")), partitionsTable),
		Div(Class(cardClass("table-wrap")), H2(Text("Backfills")), backfillsTable),
	)
}

func boolLabel(v bool) string {
	if v {
		return "true"
	}
	return "false"
}
