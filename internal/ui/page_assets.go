package ui

import (
	"fmt"
	"sort"
	"strconv"
	"strings"

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

func assetsListPage(principal domain.ContextPrincipal, rows []assetsListRowData, page domain.PageRequest, total int64, filterValue string, canMaterialize bool, backfillConfigured bool) Node {
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

	emptyMessage := "No assets found yet."
	hint := "Apply declarative config or sync a catalog to populate this list."
	if !canMaterialize {
		hint = "You can browse assets once they exist. Triggering materialization requires execute asset materialization permission."
	}
	if !backfillConfigured {
		hint += " Backfill is not configured in this environment."
	}

	tableNode := Div(
		emptyStateCard(emptyMessage, "Open catalogs", "/ui/catalogs"),
		Div(Class(cardClass()), P(Class(mutedClass()), Text(hint))),
	)
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
		quickFilterCardWithValue("Filter by asset key, type, or owner", filterValue),
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
	FreshnessLabel      string
	FreshnessTone       string
	UpdatedAt           string
	UpstreamAssetKeys   []string
	DownstreamAssetKeys []string
	DependencyEdges     []assetDependencyEdgeData
	Runs                []domain.AssetRun
	Materializations    []domain.AssetMaterialization
	Checks              []domain.AssetCheck
	Partitions          []domain.AssetPartition
	Backfills           []domain.BackfillRequest
	RetryTimeline       []assetRetryTimelineEntry
	FailureRootCauses   []assetFailureRootCauseGroup
	PartitionCalendar   []assetPartitionCalendarMonth
	PartitionStatus     map[string]int
	CanMaterialize      bool
	CanBackfill         bool
	BackfillConfigured  bool
	CSRFFieldFunc       func() Node
}

type assetRetryTimelineEntry struct {
	RunID          string
	Status         string
	TriggerType    string
	AttemptSummary string
	WindowLabel    string
	RetryHint      string
	IsRetry        bool
}

type assetFailureRootCauseGroup struct {
	Signature string
	Message   string
	Count     int
	LastSeen  string
	Statuses  []string
	RunIDs    []string
}

type assetPartitionCalendarMonth struct {
	Label string
	Cells []assetPartitionCalendarCell
}

type assetPartitionCalendarCell struct {
	DayLabel     string
	PartitionKey string
	Status       string
	Tone         string
	IsPadding    bool
	HasPartition bool
}

type assetDependencyEdgeData struct {
	FromKey string
	ToKey   string
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
	hasRunErrors := false
	for i := range d.Runs {
		r := d.Runs[i]
		errorMessage := stringPtr(r.ErrorMessage)
		if errorMessage != "-" {
			hasRunErrors = true
		}
		runRows = append(runRows,
			Tr(
				Td(Text(r.ID)),
				Td(statusLabel(r.Status, runStatusTone(r.Status))),
				Td(Text(r.TriggerType)),
				Td(Text(formatAttemptSummary(r.AttemptCount, r.MaxAttempts))),
				Td(Text(formatTimePtr(r.StartedAt))),
				Td(Text(formatTimePtr(r.FinishedAt))),
				Td(Text(errorMessage)),
			),
		)
	}
	runsTable := Node(P(Class("color-fg-muted"), Text("No runs yet.")))
	if len(runRows) > 0 {
		headCells := []Node{Th(Text("Run ID")), Th(Text("Status")), Th(Text("Trigger")), Th(Text("Attempts")), Th(Text("Started")), Th(Text("Finished"))}
		if hasRunErrors {
			headCells = append(headCells, Th(Text("Error")))
		}
		rowsWithOptionalError := runRows
		if !hasRunErrors {
			rowsWithOptionalError = make([]Node, 0, len(d.Runs))
			for i := range d.Runs {
				r := d.Runs[i]
				rowsWithOptionalError = append(rowsWithOptionalError,
					Tr(
						Td(Text(r.ID)),
						Td(statusLabel(r.Status, runStatusTone(r.Status))),
						Td(Text(r.TriggerType)),
						Td(Text(formatAttemptSummary(r.AttemptCount, r.MaxAttempts))),
						Td(Text(formatTimePtr(r.StartedAt))),
						Td(Text(formatTimePtr(r.FinishedAt))),
					),
				)
			}
		}
		runsTable = Table(Class("data-table"), THead(Tr(Group(headCells))), TBody(Group(rowsWithOptionalError)))
	}

	matRows := make([]Node, 0, len(d.Materializations))
	for i := range d.Materializations {
		m := d.Materializations[i]
		matRows = append(matRows,
			Tr(
				Td(Text(m.ID)),
				Td(Text(stringPtr(m.PartitionKey))),
				Td(Text(nullableInt64(m.RowCount))),
				Td(Text(stringPtr(m.SchemaHash))),
				Td(Text(materializationMetadataSummary(m.MetadataJSON))),
				Td(Text(formatTime(m.MaterializedAt))),
			),
		)
	}
	matTable := Node(P(Class("color-fg-muted"), Text("No materializations yet.")))
	if len(matRows) > 0 {
		matTable = Table(Class("data-table"), THead(Tr(Th(Text("ID")), Th(Text("Partition")), Th(Text("Rows")), Th(Text("Schema Hash")), Th(Text("Metadata")), Th(Text("Materialized At")))), TBody(Group(matRows)))
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

	retryTimelineNode := retryTimelinePanel(d.RetryTimeline)
	failureRootCauseNode := failureRootCausePanel(d.FailureRootCauses)
	partitionCalendarNode := partitionCalendarPanel(d.PartitionCalendar)

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
			P(Text("Freshness: "), statusLabel(d.FreshnessLabel, d.FreshnessTone)),
			P(Text("Updated: "+d.UpdatedAt)),
			Div(Class("d-flex flex-wrap gap-2 mt-2"),
				materializeForm(d),
				backfillForm(d),
			),
		),
		Div(Class(cardClass("table-wrap")), H2(Text("Dependencies")), dependencyAdjacencyView(d.AssetKey, d.DependencyEdges), H3(Text("Upstream")), upstream, H3(Text("Downstream")), downstream),
		Div(Class(cardClass("table-wrap")), H2(Text("Recent Runs")), runsTable),
		Div(Class(cardClass("table-wrap")), H2(Text("Retry Timeline")), retryTimelineNode),
		Div(Class(cardClass("table-wrap")), H2(Text("Failure Root Cause")), failureRootCauseNode),
		Div(Class(cardClass("table-wrap")), H2(Text("Materializations")), matTable),
		Div(Class(cardClass("table-wrap")), H2(Text("Checks")), checksTable),
		Div(Class(cardClass("table-wrap")), H2(Text("Partitions")), partitionSummary(d.PartitionStatus), partitionCalendarNode, partitionsTable),
		Div(Class(cardClass("table-wrap")), H2(Text("Backfills")), backfillsTable),
	)
}

func materializeForm(d assetDetailPageData) Node {
	if d.CanMaterialize {
		return Form(
			Method("post"),
			Action("/ui/assets/"+d.AssetKey+"/materialize"),
			d.CSRFFieldFunc(),
			Input(Type("text"), Name("partition_key"), Placeholder("Partition key (optional)")),
			Button(Type("submit"), Class(primaryButtonClass()), Text("Trigger materialization")),
		)
	}

	return Div(
		Class(cardClass()),
		P(Class("color-fg-muted mb-2"), Text("Materialization unavailable.")),
		Form(
			Method("post"),
			Action("/ui/assets/"+d.AssetKey+"/materialize"),
			d.CSRFFieldFunc(),
			FieldSet(Disabled(),
				Input(Type("text"), Name("partition_key"), Placeholder("Partition key (optional)")),
				Button(Type("submit"), Class(primaryButtonClass()), Text("Trigger materialization")),
			),
		),
		P(Class("color-fg-muted text-small mt-2"), Text("Requires execute asset materialization on catalog.")),
	)
}

func backfillForm(d assetDetailPageData) Node {
	if !d.BackfillConfigured {
		return Div(
			Class(cardClass()),
			P(Class("color-fg-muted"), Text("Backfill service is not configured.")),
		)
	}

	if d.CanBackfill {
		return Form(
			Method("post"),
			Action("/ui/assets/"+d.AssetKey+"/backfills"),
			d.CSRFFieldFunc(),
			Input(Type("text"), Name("partition_from"), Placeholder("partition_from (YYYY-MM-DD)"), Required()),
			Input(Type("text"), Name("partition_to"), Placeholder("partition_to (YYYY-MM-DD)"), Required()),
			Input(Type("number"), Name("max_parallelism"), Placeholder("max parallelism")),
			Button(Type("submit"), Class(secondaryButtonClass()), Text("Create backfill")),
		)
	}

	return Div(
		Class(cardClass()),
		P(Class("color-fg-muted mb-2"), Text("Backfill unavailable.")),
		Form(
			Method("post"),
			Action("/ui/assets/"+d.AssetKey+"/backfills"),
			d.CSRFFieldFunc(),
			FieldSet(Disabled(),
				Input(Type("text"), Name("partition_from"), Placeholder("partition_from (YYYY-MM-DD)"), Required()),
				Input(Type("text"), Name("partition_to"), Placeholder("partition_to (YYYY-MM-DD)"), Required()),
				Input(Type("number"), Name("max_parallelism"), Placeholder("max parallelism")),
				Button(Type("submit"), Class(secondaryButtonClass()), Text("Create backfill")),
			),
		),
		P(Class("color-fg-muted text-small mt-2"), Text("Requires execute asset materialization on catalog.")),
	)
}

func retryTimelinePanel(entries []assetRetryTimelineEntry) Node {
	if len(entries) == 0 {
		return P(Class("color-fg-muted"), Text("No runs yet."))
	}

	items := make([]Node, 0, len(entries))
	for i := range entries {
		entry := entries[i]
		retryTag := Node(Text(""))
		if entry.IsRetry {
			retryTag = statusLabel("retry", "attention")
		}
		items = append(items,
			Li(Attr("style", "list-style:none; margin:0; padding:0 0 0 1rem; border-left: 2px solid var(--borderColor-muted);"),
				Div(Class("d-flex flex-wrap gap-2 flex-items-center mb-1"),
					Strong(Text(entry.RunID)),
					statusLabel(entry.Status, runStatusTone(entry.Status)),
					retryTag,
					Span(Class("color-fg-muted text-small"), Text(entry.AttemptSummary)),
				),
				Div(Class("color-fg-muted text-small"), Text(entry.WindowLabel+" | trigger: "+entry.TriggerType)),
				If(strings.TrimSpace(entry.RetryHint) != "", P(Class("mb-2"), Text(entry.RetryHint))),
			),
		)
	}

	return Ul(Attr("style", "margin:0; padding:0;"), Group(items))
}

func failureRootCausePanel(groups []assetFailureRootCauseGroup) Node {
	if len(groups) == 0 {
		return P(Class("color-fg-muted"), Text("No recent run failures."))
	}

	rows := make([]Node, 0, len(groups))
	for i := range groups {
		group := groups[i]
		statusBadges := make([]Node, 0, len(group.Statuses))
		for j := range group.Statuses {
			status := group.Statuses[j]
			statusBadges = append(statusBadges, statusLabel(status, runStatusTone(status)))
		}
		runIDSummary := "-"
		if len(group.RunIDs) > 0 {
			runIDSummary = strings.Join(group.RunIDs, ", ")
		}
		rows = append(rows,
			Tr(
				Td(Code(Text(group.Signature))),
				Td(Text(group.Message)),
				Td(Text(strconv.Itoa(group.Count))),
				Td(Text(group.LastSeen)),
				Td(Div(Class("d-flex flex-wrap gap-1"), Group(statusBadges))),
				Td(Text(runIDSummary)),
			),
		)
	}

	return Table(
		Class("data-table"),
		THead(Tr(Th(Text("Signature")), Th(Text("Message")), Th(Text("Count")), Th(Text("Last seen")), Th(Text("Statuses")), Th(Text("Runs")))),
		TBody(Group(rows)),
	)
}

func partitionCalendarPanel(months []assetPartitionCalendarMonth) Node {
	if len(months) == 0 {
		return P(Class("color-fg-muted mb-2"), Text("No partition dates available for calendar view."))
	}

	legend := Div(Class("d-flex flex-wrap gap-2 mb-2"),
		statusLabel("healthy", "success"),
		statusLabel("warning", "attention"),
		statusLabel("error", "severe"),
		statusLabel("other", "accent"),
		Span(Class("color-fg-muted text-small"), Text("blank cells have no partition record")),
	)

	monthNodes := make([]Node, 0, len(months))
	for i := range months {
		month := months[i]
		cells := make([]Node, 0, len(month.Cells))
		for j := range month.Cells {
			cell := month.Cells[j]
			if cell.IsPadding {
				cells = append(cells, Div(Attr("style", "height: 2.5rem;")))
				continue
			}
			cellStyle := "height:2.5rem; border-radius:6px; border:1px solid var(--borderColor-muted); padding:0.2rem; font-size:0.75rem;"
			if cell.HasPartition {
				cellStyle = cellStyle + partitionCalendarToneStyle(cell.Tone)
			}
			cellTitle := cell.DayLabel
			if cell.HasPartition {
				cellTitle = cell.PartitionKey + " | " + cell.Status
			}
			cellValue := cell.DayLabel
			if cell.HasPartition {
				cellValue = cell.DayLabel + " " + shortPartitionStatus(cell.Status)
			}
			cells = append(cells,
				Div(
					Attr("title", cellTitle),
					Attr("style", cellStyle),
					Text(cellValue),
				),
			)
		}

		monthNodes = append(monthNodes,
			Div(Class("mb-3"),
				H3(Text(month.Label)),
				Div(Attr("style", "display:grid; grid-template-columns: repeat(7, minmax(0, 1fr)); gap:0.35rem;"), Group(cells)),
			),
		)
	}

	return Div(Class("mb-3"), legend, Group(monthNodes))
}

func partitionCalendarToneStyle(tone string) string {
	switch tone {
	case "success":
		return "background: var(--bgColor-success-muted); color: var(--fgColor-success); border-color: var(--borderColor-success-muted);"
	case "attention":
		return "background: var(--bgColor-attention-muted); color: var(--fgColor-attention); border-color: var(--borderColor-attention-muted);"
	case "severe":
		return "background: var(--bgColor-danger-muted); color: var(--fgColor-danger); border-color: var(--borderColor-danger-muted);"
	default:
		return "background: var(--bgColor-accent-muted); color: var(--fgColor-accent); border-color: var(--borderColor-accent-muted);"
	}
}

func shortPartitionStatus(status string) string {
	trimmed := strings.TrimSpace(status)
	if len(trimmed) <= 8 {
		return trimmed
	}
	return trimmed[:8]
}

func partitionSummary(statuses map[string]int) Node {
	if len(statuses) == 0 {
		return P(Class("color-fg-muted"), Text("No partition status recorded."))
	}
	keys := sortedPartitionStatusKeys(statuses)
	total := 0
	for i := range keys {
		total += statuses[keys[i]]
	}

	chips := make([]Node, 0, len(keys))
	bars := make([]Node, 0, len(keys))
	for i := range keys {
		status := keys[i]
		count := statuses[status]
		percent := 0
		if total > 0 {
			percent = int(float64(count) / float64(total) * 100)
		}
		chips = append(chips, Span(Class("mr-2"), statusLabel(status+": "+strconv.Itoa(count), partitionStatusTone(status))))
		bars = append(bars,
			Div(Class("mb-2"),
				Div(Class("d-flex flex-items-center flex-wrap gap-2 mb-1"), statusLabel(status, partitionStatusTone(status)), Span(Class("color-fg-muted text-small"), Text(strconv.Itoa(count)+" / "+strconv.Itoa(total)))),
				Div(Class("Box"), Attr("style", "height:8px; background: var(--canvas-subtle); border-radius: 999px; overflow: hidden;"),
					Div(Attr("style", "height:100%; width:"+strconv.Itoa(percent)+"%; background: var(--bgColor-accent-emphasis);")),
				),
			),
		)
	}
	return Div(Class("mb-2"), Div(Class("mb-2"), Group(chips)), Div(Group(bars)))
}

func dependencyAdjacencyView(assetKey string, edges []assetDependencyEdgeData) Node {
	if len(edges) == 0 {
		return Div(Class("mb-2"), P(Class("color-fg-muted"), Text("No dependency edges recorded.")))
	}

	lines := make([]string, 0, len(edges)+1)
	lines = append(lines, "graph LR")
	edgeItems := make([]Node, 0, len(edges))
	for i := range edges {
		edge := edges[i]
		fromID := "n" + strconv.Itoa(i*2)
		toID := "n" + strconv.Itoa(i*2+1)
		lines = append(lines, "    "+fromID+"[\""+escapeMermaidLabel(edge.FromKey)+"\"] --> "+toID+"[\""+escapeMermaidLabel(edge.ToKey)+"\"]")
		edgeItems = append(edgeItems, Li(Text(edge.FromKey+" -> "+edge.ToKey)))
	}

	return Div(Class("mb-3"),
		P(Class("color-fg-muted"), Text("Adjacency view for "+assetKey+":")),
		Ul(Class("mb-2"), Group(edgeItems)),
		Details(
			Class("mb-2"),
			Summary(Text("Mermaid view")),
			Pre(Class("Box p-2"), Code(Text(strings.Join(lines, "\n")))),
		),
	)
}

func escapeMermaidLabel(value string) string {
	return strings.ReplaceAll(value, "\"", "\\\"")
}

func runStatusTone(status string) string {
	switch status {
	case domain.AssetRunStatusSuccess:
		return "success"
	case domain.AssetRunStatusFailed, domain.AssetRunStatusCancelled, domain.AssetRunStatusStale:
		return "severe"
	case domain.AssetRunStatusRetrying:
		return "attention"
	default:
		return "accent"
	}
}

func formatAttemptSummary(attemptCount, maxAttempts int) string {
	if maxAttempts <= 0 {
		return strconv.Itoa(attemptCount) + "/-"
	}
	return strconv.Itoa(attemptCount) + "/" + strconv.Itoa(maxAttempts)
}

func nullableInt64(value *int64) string {
	if value == nil {
		return "-"
	}
	return strconv.FormatInt(*value, 10)
}

func materializationMetadataSummary(values map[string]any) string {
	if len(values) == 0 {
		return "-"
	}
	keys := make([]string, 0, len(values))
	for k := range values {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	parts := make([]string, 0, 3)
	for i := range keys {
		if len(parts) == 3 {
			break
		}
		value := strings.TrimSpace(fmt.Sprintf("%v", values[keys[i]]))
		if value == "" {
			continue
		}
		if len(value) > 48 {
			value = value[:48] + "..."
		}
		parts = append(parts, keys[i]+"="+value)
	}
	if len(parts) == 0 {
		return "-"
	}
	if len(keys) > len(parts) {
		parts = append(parts, "+"+strconv.Itoa(len(keys)-len(parts))+" more")
	}
	return strings.Join(parts, ", ")
}

func sortedPartitionStatusKeys(statuses map[string]int) []string {
	keys := make([]string, 0, len(statuses))
	for status := range statuses {
		keys = append(keys, status)
	}
	sort.Strings(keys)
	return keys
}

func partitionStatusTone(status string) string {
	upper := strings.ToUpper(strings.TrimSpace(status))
	switch upper {
	case "READY", "MATERIALIZED", "SUCCESS", "HEALTHY":
		return "success"
	case "FAILED", "ERROR", "LATE", "MISSING":
		return "severe"
	case "RUNNING", "PENDING", "QUEUED", "STALE":
		return "attention"
	default:
		return "accent"
	}
}

func boolLabel(v bool) string {
	if v {
		return "true"
	}
	return "false"
}
