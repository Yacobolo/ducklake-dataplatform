package ui

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"duck-demo/internal/domain"

	. "maragu.dev/gomponents"
	. "maragu.dev/gomponents/html"
)

func assetDetailPage(d assetDetailPageData) Node {
	summary := buildAssetDetailSummary(d)
	upstreamCount := len(d.UpstreamAssetKeys)
	downstreamCount := len(d.DownstreamAssetKeys)
	dependencyCount := upstreamCount + downstreamCount
	partitionCoverage := assetPartitionCoverage(d.PartitionStatus)
	executionSummary := assetExecutionSummaryBadges(d)
	historySummary := assetHistorySummaryBadges(d)

	return appPage(
		"Asset: "+d.AssetKey,
		"assets",
		d.Principal,
		Div(Class("asset-detail-shell"),
			Div(Class("asset-detail-hero"),
				Div(Class("asset-detail-hero-copy"),
					P(Class("assets-kicker"), Text("Asset command center")),
					Div(Class("asset-detail-title-row"),
						H2(Class("asset-detail-title"), Text(d.AssetKey)),
						statusLabel(strings.ToUpper(d.AssetType), assetTypeTone(d.AssetType)),
					),
					P(Class("asset-detail-description"), Text(fallbackString(d.Description, "No description provided yet."))),
					Div(Class("assets-badge-row"),
						statusLabel(d.FreshnessLabel, d.FreshnessTone),
						statusLabel(assetActiveLabel(d.IsActive), assetActiveTone(d.IsActive)),
						statusLabel(summary.MaterializationMode, "accent"),
						statusLabel(summary.PartitionLabel, "accent"),
					),
				),
				Div(Class("asset-detail-hero-meta"),
					assetDetailMetaRow("Owner", fallbackString(d.Owner, "unknown")),
					assetDetailMetaRow("IO profile", fallbackString(d.IOProfile, "-")),
					assetDetailMetaRow("Updated", d.UpdatedAt),
					assetDetailMetaRow("Latest materialization", summary.LatestMaterializedAt),
				),
			),
			Div(Class("asset-detail-metrics"),
				assetDetailMetricCard("Freshness", d.FreshnessLabel, freshnessHelperText(d.FreshnessLabel)),
				assetDetailMetricCard("Dependencies", strconv.Itoa(dependencyCount), fmt.Sprintf("%d upstream, %d downstream", upstreamCount, downstreamCount)),
				assetDetailMetricCard("Recent runs", strconv.Itoa(len(d.Runs)), fmt.Sprintf("latest: %s", summary.LatestRunStatus)),
				assetDetailMetricCard("Partitions", partitionCoverage, summary.PartitionHint),
			),
			Div(Class("asset-detail-layout"),
				Div(Class("asset-detail-main"),
					Div(Class(cardClass("asset-detail-section")),
						Div(Class("assets-section-head"), H2(Text("Dependency flow")), P(Class(mutedClass()), Text("See how this asset fans in and fans out across the runtime graph."))),
						Div(Class("assets-badge-row"), Group(assetDependencySummaryBadges(d.AssetKey, d.DependencyEdges))),
						Div(Class("asset-detail-dependency-grid"),
							Div(Class("asset-detail-subpanel"), H3(Text("Upstream")), P(Class("asset-detail-subpanel-copy"), Text("Inputs that must be ready before this asset can run.")), assetLinkList(d.UpstreamAssetKeys, "No upstream dependencies.")),
							Div(Class("asset-detail-subpanel"), H3(Text("Downstream")), P(Class("asset-detail-subpanel-copy"), Text("Consumers and derivatives affected by this asset.")), assetLinkList(d.DownstreamAssetKeys, "No downstream dependencies.")),
						),
						dependencyAdjacencyView(d.AssetKey, d.DependencyEdges),
					),
					Div(Class(cardClass("asset-detail-section")),
						Div(Class("assets-section-head"), H2(Text("Execution health")), P(Class(mutedClass()), Text("The most recent runs, retries, and failure signatures in one place."))),
						Div(Class("assets-badge-row"), Group(executionSummary)),
						Div(Class("asset-detail-health-grid"),
							Div(Class("asset-detail-health-panel"), H3(Text("Recent Runs")), assetRunsTable(d.Runs)),
							Div(Class("asset-detail-health-panel"), H3(Text("Retry Timeline")), retryTimelinePanel(d.RetryTimeline)),
						),
						Div(Class("asset-detail-health-panel asset-detail-health-panel-wide"), H3(Text("Failure Root Cause")), failureRootCausePanel(d.FailureRootCauses)),
					),
					Div(Class(cardClass("asset-detail-section")),
						Div(Class("assets-section-head"), H2(Text("Materialization history")), P(Class(mutedClass()), Text("Completed outputs and configured checks for this asset."))),
						Div(Class("assets-badge-row"), Group(historySummary)),
						Div(Class("asset-detail-history-grid"),
							Div(Class("asset-detail-history-panel"), H3(Text("Materializations")), assetMaterializationsTable(d.Materializations)),
							Div(Class("asset-detail-history-panel"), H3(Text("Checks")), assetChecksTable(d.Checks)),
						),
					),
					Div(Class(cardClass("asset-detail-section")),
						Div(Class("assets-section-head"), H2(Text("Partitions and recovery")), P(Class(mutedClass()), Text("Partition coverage, freshness drift, and backfill activity."))),
						partitionSummary(d.PartitionStatus),
						partitionCalendarPanel(d.PartitionCalendar),
						Div(Class("asset-detail-history-grid"),
							Div(Class("asset-detail-history-panel"), H3(Text("Partitions")), assetPartitionsTable(d.Partitions)),
							Div(Class("asset-detail-history-panel"), H3(Text("Backfills")), assetBackfillsTable(d.Backfills)),
						),
					),
				),
				Div(Class("asset-detail-rail"),
					Div(Class(cardClass("asset-rail-card")),
						H2(Text("Operate")),
						P(Class(mutedClass()), Text("Kick the asset manually or request a targeted backfill from the same control rail.")),
						materializeForm(d),
						backfillForm(d),
					),
					Div(Class(cardClass("asset-rail-card")),
						H2(Text("At a glance")),
						assetFactList([][2]string{{"Owner", fallbackString(d.Owner, "unknown")}, {"Freshness", d.FreshnessLabel}, {"Materializations", strconv.Itoa(len(d.Materializations))}, {"Checks", strconv.Itoa(len(d.Checks))}, {"Runs", strconv.Itoa(len(d.Runs))}, {"Backfills", strconv.Itoa(len(d.Backfills))}}),
					),
				),
			),
		),
		Script(Type("module"), Src(uiScriptHref("asset-graph.js"))),
	)
}

func assetLinkList(assetKeys []string, emptyMessage string) Node {
	if len(assetKeys) == 0 {
		return P(Class("color-fg-muted"), Text(emptyMessage))
	}
	items := make([]Node, 0, len(assetKeys))
	for i := range assetKeys {
		items = append(items, Li(Class("asset-link-list-item"), A(Href("/ui/assets/"+assetKeys[i]), Text(assetKeys[i]))))
	}
	return Ul(Class("asset-link-list"), Group(items))
}

func assetDependencySummaryBadges(assetKey string, edges []assetDependencyEdgeData) []Node {
	if len(edges) == 0 {
		return []Node{statusLabel("No graph edges", "attention")}
	}
	upstream := 0
	downstream := 0
	for i := range edges {
		switch {
		case edges[i].ToKey == assetKey:
			upstream++
		case edges[i].FromKey == assetKey:
			downstream++
		}
	}
	return []Node{
		statusLabel(strconv.Itoa(len(edges))+" edges", "accent"),
		statusLabel(strconv.Itoa(upstream)+" upstream", "success"),
		statusLabel(strconv.Itoa(downstream)+" downstream", "attention"),
	}
}

func assetExecutionSummaryBadges(d assetDetailPageData) []Node {
	retryCount := 0
	for i := range d.RetryTimeline {
		if d.RetryTimeline[i].IsRetry {
			retryCount++
		}
	}
	failures := 0
	for i := range d.Runs {
		if runStatusTone(d.Runs[i].Status) == "severe" {
			failures++
		}
	}
	badges := []Node{statusLabel(strconv.Itoa(len(d.Runs))+" tracked runs", "accent")}
	if failures > 0 {
		badges = append(badges, statusLabel(strconv.Itoa(failures)+" failures", "severe"))
	} else {
		badges = append(badges, statusLabel("No recent failures", "success"))
	}
	if retryCount > 0 {
		badges = append(badges, statusLabel(strconv.Itoa(retryCount)+" retries", "attention"))
	}
	return badges
}

func assetHistorySummaryBadges(d assetDetailPageData) []Node {
	badges := []Node{statusLabel(strconv.Itoa(len(d.Materializations))+" materializations", "accent")}
	enabledChecks := 0
	for i := range d.Checks {
		if d.Checks[i].Enabled {
			enabledChecks++
		}
	}
	badges = append(badges, statusLabel(strconv.Itoa(enabledChecks)+" enabled checks", "success"))
	if len(d.Materializations) > 0 {
		latestPartition := stringPtr(d.Materializations[0].PartitionKey)
		if latestPartition != "-" {
			badges = append(badges, statusLabel("Latest "+latestPartition, "attention"))
		}
	}
	return badges
}

func assetRunsTable(runs []domain.AssetRun) Node {
	runRows := make([]Node, 0, len(runs))
	hasRunErrors := false
	for i := range runs {
		r := runs[i]
		errorMessage := stringPtr(r.ErrorMessage)
		if errorMessage != "-" {
			hasRunErrors = true
		}
		runRows = append(runRows,
			Tr(
				Td(
					Span(Class("asset-run-id"), Text(shortAssetID(r.ID))),
					P(Class("assets-table-subtitle"), Text(r.ID)),
				),
				Td(statusLabel(r.Status, runStatusTone(r.Status))),
				Td(Text(r.TriggerType)),
				Td(Text(formatRunWindow(r))),
				Td(Text(formatAttemptSummary(r.AttemptCount, r.MaxAttempts))),
				Td(Text(formatRunDuration(r.StartedAt, r.FinishedAt))),
				Td(Text(errorMessage)),
			),
		)
	}
	if len(runRows) == 0 {
		return P(Class("color-fg-muted"), Text("No runs yet."))
	}
	headCells := []Node{Th(Text("Run")), Th(Text("Status")), Th(Text("Trigger")), Th(Text("Window")), Th(Text("Attempts")), Th(Text("Duration"))}
	rowsWithOptionalError := runRows
	if hasRunErrors {
		headCells = append(headCells, Th(Text("Error")))
	} else {
		rowsWithOptionalError = make([]Node, 0, len(runs))
		for i := range runs {
			r := runs[i]
			rowsWithOptionalError = append(rowsWithOptionalError,
				Tr(
					Td(
						Span(Class("asset-run-id"), Text(shortAssetID(r.ID))),
						P(Class("assets-table-subtitle"), Text(r.ID)),
					),
					Td(statusLabel(r.Status, runStatusTone(r.Status))),
					Td(Text(r.TriggerType)),
					Td(Text(formatRunWindow(r))),
					Td(Text(formatAttemptSummary(r.AttemptCount, r.MaxAttempts))),
					Td(Text(formatRunDuration(r.StartedAt, r.FinishedAt))),
				),
			)
		}
	}
	return Table(Class("data-table"), THead(Tr(Group(headCells))), TBody(Group(rowsWithOptionalError)))
}

func assetMaterializationsTable(items []domain.AssetMaterialization) Node {
	matRows := make([]Node, 0, len(items))
	for i := range items {
		m := items[i]
		matRows = append(matRows,
			Tr(
				Td(
					Span(Class("asset-run-id"), Text(shortAssetID(m.ID))),
					P(Class("assets-table-subtitle"), Text(m.ID)),
				),
				Td(Text(stringPtr(m.PartitionKey))),
				Td(Text(nullableInt64(m.RowCount))),
				Td(Text(stringPtr(m.SchemaHash))),
				Td(Text(materializationMetadataSummary(m.MetadataJSON))),
				Td(Text(formatTime(m.MaterializedAt))),
			),
		)
	}
	if len(matRows) == 0 {
		return P(Class("color-fg-muted"), Text("No materializations yet."))
	}
	return Table(Class("data-table"), THead(Tr(Th(Text("Materialization")), Th(Text("Partition")), Th(Text("Rows")), Th(Text("Schema Hash")), Th(Text("Metadata")), Th(Text("Materialized At")))), TBody(Group(matRows)))
}

func assetChecksTable(checks []domain.AssetCheck) Node {
	checkRows := make([]Node, 0, len(checks))
	for i := range checks {
		c := checks[i]
		enabledTone := "severe"
		if c.Enabled {
			enabledTone = "success"
		}
		checkRows = append(checkRows, Tr(Td(Text(c.Name)), Td(Text(c.CheckType)), Td(Text(c.Severity)), Td(statusLabel(boolLabel(c.Enabled), enabledTone))))
	}
	if len(checkRows) == 0 {
		return P(Class("color-fg-muted"), Text("No checks configured."))
	}
	return Table(Class("data-table"), THead(Tr(Th(Text("Name")), Th(Text("Type")), Th(Text("Severity")), Th(Text("Enabled")))), TBody(Group(checkRows)))
}

func assetPartitionsTable(partitions []domain.AssetPartition) Node {
	partitionRows := make([]Node, 0, len(partitions))
	for i := range partitions {
		p := partitions[i]
		partitionRows = append(partitionRows, Tr(Td(Text(p.PartitionKey)), Td(Text(p.Status)), Td(Text(formatTimePtr(p.LastMaterializedAt)))))
	}
	if len(partitionRows) == 0 {
		return P(Class("color-fg-muted"), Text("No partitions recorded."))
	}
	return Table(Class("data-table"), THead(Tr(Th(Text("Partition")), Th(Text("Status")), Th(Text("Last Materialized")))), TBody(Group(partitionRows)))
}

func assetBackfillsTable(backfills []domain.BackfillRequest) Node {
	backfillRows := make([]Node, 0, len(backfills))
	for i := range backfills {
		b := backfills[i]
		backfillRows = append(backfillRows, Tr(Td(Text(b.ID)), Td(Text(b.PartitionFrom+" -> "+b.PartitionTo)), Td(Text(b.Status)), Td(Text(b.RequestedBy)), Td(Text(formatTime(b.CreatedAt)))))
	}
	if len(backfillRows) == 0 {
		return P(Class("color-fg-muted"), Text("No backfills requested."))
	}
	return Table(Class("data-table"), THead(Tr(Th(Text("ID")), Th(Text("Range")), Th(Text("Status")), Th(Text("Requested By")), Th(Text("Created")))), TBody(Group(backfillRows)))
}

func materializeForm(d assetDetailPageData) Node {
	if d.CanMaterialize {
		return Form(
			Class("asset-action-form"),
			Method("post"),
			Action("/ui/assets/"+d.AssetKey+"/materialize"),
			d.CSRFFieldFunc(),
			Div(Class("asset-action-head"),
				P(Class("asset-action-title"), Text("Trigger materialization")),
				P(Class("asset-action-copy"), Text("Run the asset now, optionally scoped to a single partition.")),
			),
			Div(Class("asset-action-fields"),
				Input(Type("text"), Name("partition_key"), Placeholder("Partition key (optional)")),
			),
			Button(Type("submit"), Class(primaryButtonClass()), Text("Trigger materialization")),
		)
	}

	return Form(
		Class("asset-action-form asset-action-form-disabled"),
		Method("post"),
		Action("/ui/assets/"+d.AssetKey+"/materialize"),
		d.CSRFFieldFunc(),
		Div(Class("asset-action-head"),
			P(Class("asset-action-title"), Text("Materialization unavailable")),
			P(Class("asset-action-copy"), Text("Manual runs are disabled for your current principal.")),
		),
		FieldSet(Disabled(),
			Class("asset-action-fields"),
			Input(Type("text"), Name("partition_key"), Placeholder("Partition key (optional)")),
			Button(Type("submit"), Class(primaryButtonClass()), Text("Trigger materialization")),
		),
		P(Class("asset-action-note"), Text("Requires execute asset materialization on catalog.")),
	)
}

func backfillForm(d assetDetailPageData) Node {
	if !d.BackfillConfigured {
		return Div(
			Class("asset-action-form asset-action-form-disabled"),
			Div(Class("asset-action-head"),
				P(Class("asset-action-title"), Text("Backfill unavailable")),
				P(Class("asset-action-copy"), Text("Backfill service is not configured.")),
			),
		)
	}

	if d.CanBackfill {
		return Form(
			Class("asset-action-form"),
			Method("post"),
			Action("/ui/assets/"+d.AssetKey+"/backfills"),
			d.CSRFFieldFunc(),
			Div(Class("asset-action-head"),
				P(Class("asset-action-title"), Text("Create backfill")),
				P(Class("asset-action-copy"), Text("Generate recovery slices across a partition range.")),
			),
			Div(Class("asset-action-fields asset-action-inline"),
				Input(Type("text"), Name("partition_from"), Placeholder("partition_from (YYYY-MM-DD)"), Required()),
				Input(Type("text"), Name("partition_to"), Placeholder("partition_to (YYYY-MM-DD)"), Required()),
				Input(Class("asset-action-span"), Type("number"), Name("max_parallelism"), Placeholder("max parallelism")),
			),
			Button(Type("submit"), Class(secondaryButtonClass()), Text("Create backfill")),
		)
	}

	return Form(
		Class("asset-action-form asset-action-form-disabled"),
		Method("post"),
		Action("/ui/assets/"+d.AssetKey+"/backfills"),
		d.CSRFFieldFunc(),
		Div(Class("asset-action-head"),
			P(Class("asset-action-title"), Text("Backfill unavailable")),
			P(Class("asset-action-copy"), Text("Backfill requests require materialization privileges.")),
		),
		FieldSet(Disabled(),
			Class("asset-action-fields asset-action-inline"),
			Input(Type("text"), Name("partition_from"), Placeholder("partition_from (YYYY-MM-DD)"), Required()),
			Input(Type("text"), Name("partition_to"), Placeholder("partition_to (YYYY-MM-DD)"), Required()),
			Input(Class("asset-action-span"), Type("number"), Name("max_parallelism"), Placeholder("max parallelism")),
			Button(Type("submit"), Class(secondaryButtonClass()), Text("Create backfill")),
		),
		P(Class("asset-action-note"), Text("Requires execute asset materialization on catalog.")),
	)
}

func buildAssetDetailSummary(d assetDetailPageData) assetDetailSummary {
	summary := assetDetailSummary{
		MaterializationMode:  inferAssetMaterializationMode(d),
		PartitionLabel:       inferAssetPartitionLabel(d),
		LatestRunStatus:      "No runs yet",
		LatestMaterializedAt: "Never materialized",
		PartitionHint:        "No partition inventory recorded.",
	}
	if len(d.Runs) > 0 {
		summary.LatestRunStatus = strings.Title(strings.ToLower(strings.ReplaceAll(d.Runs[0].Status, "_", " ")))
	}
	if len(d.Materializations) > 0 {
		summary.LatestMaterializedAt = formatTime(d.Materializations[0].MaterializedAt)
	}
	if total := assetPartitionTotal(d.PartitionStatus); total > 0 {
		summary.PartitionHint = fmt.Sprintf("%d tracked partitions across %d states.", total, len(d.PartitionStatus))
	}
	return summary
}

func inferAssetMaterializationMode(d assetDetailPageData) string {
	if len(d.Materializations) > 0 && len(d.Backfills) > 0 {
		return "Backfill-capable"
	}
	if len(d.Partitions) > 0 {
		return "Partition-aware"
	}
	if d.CanMaterialize {
		return "Manual trigger"
	}
	return "Read only"
}

func inferAssetPartitionLabel(d assetDetailPageData) string {
	if len(d.Partitions) == 0 {
		return "Unpartitioned"
	}
	keys := make(map[string]struct{})
	for i := range d.Partitions {
		key := strings.TrimSpace(d.Partitions[i].PartitionKey)
		if key == "" {
			continue
		}
		keys[key] = struct{}{}
	}
	if len(keys) == 1 {
		return "Single window"
	}
	if len(keys) > 1 {
		return "Multi partition"
	}
	return "Unpartitioned"
}

func assetDetailMetaRow(label string, value string) Node {
	return Div(Class("asset-detail-meta-row"),
		Span(Class("asset-detail-meta-label"), Text(label)),
		Span(Class("asset-detail-meta-value"), Text(value)),
	)
}

func assetDetailMetricCard(label string, value string, hint string) Node {
	return Div(Class("asset-detail-metric-card"),
		P(Class("asset-detail-metric-label"), Text(label)),
		P(Class("asset-detail-metric-value"), Text(value)),
		P(Class("asset-detail-metric-hint"), Text(hint)),
	)
}

func freshnessHelperText(label string) string {
	switch strings.ToUpper(strings.TrimSpace(label)) {
	case "HEALTHY":
		return "Recent materialization is within the configured freshness window."
	case "STALE":
		return "This asset has drifted beyond its freshness target and should be investigated."
	case "NEVER MATERIALIZED":
		return "The asset exists in orchestration but has not produced any output yet."
	default:
		return "Freshness depends on whether the asset has an SLA policy and recent output."
	}
}

func assetFactList(items [][2]string) Node {
	rows := make([]Node, 0, len(items))
	for i := range items {
		rows = append(rows,
			Div(Class("asset-fact-row"),
				Span(Class("asset-fact-label"), Text(items[i][0])),
				Span(Class("asset-fact-value"), Text(items[i][1])),
			),
		)
	}
	return Div(Class("asset-fact-list"), Group(rows))
}

func assetActiveLabel(v bool) string {
	if v {
		return "Active"
	}
	return "Paused"
}

func assetActiveTone(v bool) string {
	if v {
		return "success"
	}
	return "severe"
}

func assetPartitionTotal(statuses map[string]int) int {
	total := 0
	for _, count := range statuses {
		total += count
	}
	return total
}

func assetPartitionCoverage(statuses map[string]int) string {
	total := assetPartitionTotal(statuses)
	if total == 0 {
		return "0 tracked"
	}
	ready := 0
	for status, count := range statuses {
		if partitionStatusTone(status) == "success" {
			ready += count
		}
	}
	return fmt.Sprintf("%d/%d ready", ready, total)
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
			Li(Attr("style", "list-style:none; margin:0; padding:0 0 0 1rem; border-left: 2px solid var(--color-border-muted);"),
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
			cellStyle := "height:2.5rem; border-radius:6px; border:1px solid var(--color-border-muted); padding:0.2rem; font-size:0.75rem;"
			if cell.HasPartition {
				cellStyle += partitionCalendarToneStyle(cell.Tone)
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
		return "background: var(--color-success-subtle); color: var(--color-success-fg); border-color: var(--color-success-emphasis);"
	case "attention":
		return "background: var(--color-attention-subtle); color: var(--color-attention-fg); border-color: var(--color-attention-emphasis);"
	case "severe":
		return "background: var(--color-danger-subtle); color: var(--color-danger-fg); border-color: var(--color-danger-emphasis);"
	default:
		return "background: var(--color-accent-subtle); color: var(--color-accent-fg); border-color: var(--color-accent-emphasis);"
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
				Div(Class("Box"), Attr("style", "height:8px; background: var(--color-canvas-subtle); border-radius: 999px; overflow: hidden;"),
					Div(Attr("style", "height:100%; width:"+strconv.Itoa(percent)+"%; background: var(--color-accent-emphasis);")),
				),
			),
		)
	}
	return Div(Class("mb-2"), Div(Class("mb-2"), Group(chips)), Div(Group(bars)))
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

func shortAssetID(value string) string {
	trimmed := strings.TrimSpace(value)
	if len(trimmed) <= 8 {
		return trimmed
	}
	return trimmed[:8]
}

func formatRunWindow(run domain.AssetRun) string {
	started := formatTimePtr(run.StartedAt)
	finished := formatTimePtr(run.FinishedAt)
	if started == "-" && finished == "-" {
		return "queued"
	}
	if finished == "-" {
		return started + " -> running"
	}
	if started == "-" {
		return "finished " + finished
	}
	return started + " -> " + finished
}

func formatRunDuration(startedAt, finishedAt *time.Time) string {
	if startedAt == nil || finishedAt == nil || startedAt.IsZero() || finishedAt.IsZero() {
		return "-"
	}
	delta := finishedAt.Sub(*startedAt)
	if delta < 0 {
		return "-"
	}
	return delta.Round(time.Second).String()
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
