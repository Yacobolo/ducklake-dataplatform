package ui

import (
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"duck-demo/internal/domain"

	. "maragu.dev/gomponents"
	data "maragu.dev/gomponents-datastar"
	. "maragu.dev/gomponents/html"
)

type assetsListRowData struct {
	Filter              string
	AssetKey            string
	URL                 string
	Type                string
	Owner               string
	Description         string
	Tags                []string
	Active              bool
	Updated             string
	FreshnessTracked    bool
	PartitionType       string
	AutoMaterialized    bool
	MaterializationMode string
}

func assetsListPage(principal domain.ContextPrincipal, rows []assetsListRowData, page domain.PageRequest, total int64, filterValue string, canMaterialize bool, backfillConfigured bool) Node {
	summary := summarizeAssetsRows(rows)
	emptyMessage := "No assets found yet."
	hint := "Apply declarative config or sync a catalog to populate this list."
	if !canMaterialize {
		hint = "You can browse assets once they exist. Triggering materialization requires execute asset materialization permission."
	}
	if !backfillConfigured {
		hint += " Backfill is not configured in this environment."
	}

	tableNode := assetsInventorySection(rows, emptyMessage, hint)

	return appPage(
		"Assets",
		"assets",
		principal,
		Div(Class("assets-shell"),
			assetsHero(summary, canMaterialize, backfillConfigured),
			quickFilterCardWithValue("Filter by asset key, type, owner, or tag", filterValue),
			assetsMetricsGrid(summary),
			assetsTypeBand(summary.TypeCounts),
			assetsShowcaseGrid(rows),
			tableNode,
			paginationCard("/ui/assets", page, total),
		),
	)
}

type assetsListSummary struct {
	Total            int
	Active           int
	Partitioned      int
	FreshnessTracked int
	AutoMaterialized int
	ManualOnly       int
	TypeCounts       []assetTypeCount
	OwnerCounts      []assetOwnerCount
}

type assetTypeCount struct {
	Label string
	Count int
}

type assetOwnerCount struct {
	Label string
	Count int
}

func summarizeAssetsRows(rows []assetsListRowData) assetsListSummary {
	summary := assetsListSummary{Total: len(rows)}
	typeCounts := make(map[string]int)
	ownerCounts := make(map[string]int)
	for i := range rows {
		row := rows[i]
		if row.Active {
			summary.Active++
		}
		if !strings.EqualFold(strings.TrimSpace(row.PartitionType), "unpartitioned") {
			summary.Partitioned++
		}
		if row.FreshnessTracked {
			summary.FreshnessTracked++
		}
		if row.AutoMaterialized {
			summary.AutoMaterialized++
		} else {
			summary.ManualOnly++
		}
		typeLabel := strings.ToUpper(strings.TrimSpace(row.Type))
		if typeLabel == "" {
			typeLabel = "UNKNOWN"
		}
		typeCounts[typeLabel]++
		ownerLabel := strings.TrimSpace(row.Owner)
		if ownerLabel == "" {
			ownerLabel = "unassigned"
		}
		ownerCounts[ownerLabel]++
	}

	for label, count := range typeCounts {
		summary.TypeCounts = append(summary.TypeCounts, assetTypeCount{Label: label, Count: count})
	}
	for label, count := range ownerCounts {
		summary.OwnerCounts = append(summary.OwnerCounts, assetOwnerCount{Label: label, Count: count})
	}
	sort.Slice(summary.TypeCounts, func(i, j int) bool {
		if summary.TypeCounts[i].Count == summary.TypeCounts[j].Count {
			return summary.TypeCounts[i].Label < summary.TypeCounts[j].Label
		}
		return summary.TypeCounts[i].Count > summary.TypeCounts[j].Count
	})
	sort.Slice(summary.OwnerCounts, func(i, j int) bool {
		if summary.OwnerCounts[i].Count == summary.OwnerCounts[j].Count {
			return summary.OwnerCounts[i].Label < summary.OwnerCounts[j].Label
		}
		return summary.OwnerCounts[i].Count > summary.OwnerCounts[j].Count
	})
	return summary
}

func assetsHero(summary assetsListSummary, canMaterialize bool, backfillConfigured bool) Node {
	message := "Browse every orchestrated data product, from physical tables to notebooks and outputs."
	if summary.Total > 0 {
		message = fmt.Sprintf("Track %d assets across %d owners with graph-aware orchestration, checks, and backfills.", summary.Total, len(summary.OwnerCounts))
	}
	actionLabel := "Open catalogs"
	actionHref := "/ui/catalogs"
	if canMaterialize {
		actionLabel = "Open operations"
		actionHref = "/ui/assets"
	}
	backfillLabel := "Backfill unavailable"
	backfillTone := "attention"
	if backfillConfigured {
		backfillLabel = "Backfill ready"
		backfillTone = "success"
	}
	ownerText := "No owners mapped yet"
	if len(summary.OwnerCounts) > 0 {
		ownerText = "Top owner: " + summary.OwnerCounts[0].Label
	}
	return Div(Class("assets-hero"),
		Div(Class("assets-hero-copy"),
			P(Class("assets-kicker"), Text("Operations cockpit")),
			H2(Class("assets-hero-title"), Text("Assets are where metadata turns into runtime behavior.")),
			P(Class("assets-hero-text"), Text(message)),
			Div(Class("assets-hero-actions"),
				A(Href(actionHref), Class(primaryButtonClass()), Text(actionLabel)),
				A(Href("/ui/catalogs"), Class(secondaryButtonClass()), Text("Browse source catalog")),
			),
		),
		Div(Class("assets-hero-meta"),
			Div(Class("assets-hero-chip"), statusLabel(strconv.Itoa(summary.Active)+" active", "success")),
			Div(Class("assets-hero-chip"), statusLabel(strconv.Itoa(summary.Partitioned)+" partitioned", "accent")),
			Div(Class("assets-hero-chip"), statusLabel(backfillLabel, backfillTone)),
			P(Class("assets-hero-caption"), Text(ownerText)),
		),
	)
}

func assetsMetricsGrid(summary assetsListSummary) Node {
	items := []struct {
		Label string
		Value int
		Hint  string
	}{
		{Label: "Total assets", Value: summary.Total, Hint: "Everything currently registered in orchestration."},
		{Label: "Active", Value: summary.Active, Hint: "Assets ready to run and appear in dependency flows."},
		{Label: "Freshness tracked", Value: summary.FreshnessTracked, Hint: "Assets with an SLA or max lag policy attached."},
		{Label: "Auto materialized", Value: summary.AutoMaterialized, Hint: "Assets driven by automatic orchestration policies."},
	}
	nodes := make([]Node, 0, len(items))
	for i := range items {
		item := items[i]
		nodes = append(nodes,
			Div(Class("assets-metric-card"),
				P(Class("assets-metric-label"), Text(item.Label)),
				P(Class("assets-metric-value"), Text(strconv.Itoa(item.Value))),
				P(Class("assets-metric-hint"), Text(item.Hint)),
			),
		)
	}
	return Div(Class("assets-metrics-grid"), Group(nodes))
}

func assetsTypeBand(counts []assetTypeCount) Node {
	if len(counts) == 0 {
		return Div(Class(cardClass()),
			H2(Text("Asset mix")),
			P(Class(mutedClass()), Text("Seed or sync assets to see the type distribution.")),
		)
	}
	chips := make([]Node, 0, len(counts))
	for i := range counts {
		count := counts[i]
		chips = append(chips,
			Div(Class("assets-type-chip"),
				statusLabel(count.Label, assetTypeTone(count.Label)),
				Span(Class("assets-type-count"), Text(strconv.Itoa(count.Count))),
			),
		)
	}
	return Div(Class(cardClass("assets-type-band")),
		Div(Class("assets-section-head"),
			H2(Text("Asset mix")),
			P(Class(mutedClass()), Text("A quick split between physical relations and higher-level products.")),
		),
		Div(Class("assets-type-list"), Group(chips)),
	)
}

func assetsShowcaseGrid(rows []assetsListRowData) Node {
	if len(rows) == 0 {
		return Div(
			emptyStateCard("No asset showcase yet.", "Open catalogs", "/ui/catalogs"),
		)
	}
	cards := make([]Node, 0, len(rows))
	ordered := append([]assetsListRowData(nil), rows...)
	sort.SliceStable(ordered, func(i, j int) bool {
		return assetShowcaseScore(ordered[i]) > assetShowcaseScore(ordered[j])
	})
	for i := range ordered {
		row := ordered[i]
		cards = append(cards,
			A(
				Href(row.URL),
				Class("assets-showcase-card"),
				data.Show(containsExpr(row.Filter)),
				Div(Class("assets-showcase-head"),
					Div(
						P(Class("assets-showcase-key"), Text(row.AssetKey)),
						P(Class("assets-showcase-owner"), Text("Owned by "+fallbackString(row.Owner, "unknown"))),
					),
					statusLabel(strings.ToUpper(row.Type), assetTypeTone(row.Type)),
				),
				P(Class("assets-showcase-description"), Text(fallbackString(row.Description, "No description yet."))),
				Div(Class("assets-badge-row"), Group(assetOperationalBadges(row))),
				Div(Class("assets-showcase-foot"),
					Span(Class("assets-showcase-updated"), Text("Updated "+row.Updated)),
					Span(Class("assets-showcase-link"), Text("Inspect ->")),
				),
			),
		)
	}
	return Div(Class(cardClass("assets-showcase-section")),
		Div(Class("assets-section-head"),
			H2(Text("Asset showcase")),
			P(Class(mutedClass()), Text("Browse the assets carrying the most orchestration context first.")),
		),
		Div(Class("assets-showcase-grid"), Group(cards)),
	)
}

func assetsInventorySection(rows []assetsListRowData, emptyMessage string, hint string) Node {
	tableRows := make([]Node, 0, len(rows))
	for i := range rows {
		row := rows[i]
		activeTone := "severe"
		if row.Active {
			activeTone = "success"
		}
		tableRows = append(tableRows,
			Tr(
				data.Show(containsExpr(row.Filter)),
				Td(
					A(Href(row.URL), Text(row.AssetKey)),
					P(Class("assets-table-subtitle"), Text(fallbackString(row.Description, "No description yet."))),
				),
				Td(statusLabel(strings.ToUpper(row.Type), assetTypeTone(row.Type))),
				Td(Text(row.Owner)),
				Td(Div(Class("assets-badge-stack"), Group(assetOperationalBadges(row)))),
				Td(statusLabel(boolLabel(row.Active), activeTone)),
				Td(Text(row.Updated)),
			),
		)
	}
	if len(tableRows) == 0 {
		return Div(
			emptyStateCard(emptyMessage, "Open catalogs", "/ui/catalogs"),
			Div(Class(cardClass()), P(Class(mutedClass()), Text(hint))),
		)
	}
	return Div(Class(cardClass("table-wrap")),
		Div(Class("assets-section-head"),
			H2(Text("Inventory")),
			P(Class(mutedClass()), Text("The full asset register stays searchable and operationally legible.")),
		),
		Table(
			Class("data-table"),
			THead(Tr(Th(Text("Asset key")), Th(Text("Type")), Th(Text("Owner")), Th(Text("Signals")), Th(Text("Active")), Th(Text("Updated")))),
			TBody(Group(tableRows)),
		),
	)
}

func assetOperationalBadges(row assetsListRowData) []Node {
	badges := []Node{statusLabel(strings.Title(strings.ToLower(row.MaterializationMode)), "accent")}
	if row.FreshnessTracked {
		badges = append(badges, statusLabel("SLA", "success"))
	} else {
		badges = append(badges, statusLabel("No SLA", "attention"))
	}
	partitionLabel := row.PartitionType
	if strings.TrimSpace(partitionLabel) == "" {
		partitionLabel = "Unpartitioned"
	}
	badges = append(badges, statusLabel(partitionLabel, "accent"))
	if row.AutoMaterialized {
		badges = append(badges, statusLabel("Auto", "success"))
	}
	return badges
}

func assetShowcaseScore(row assetsListRowData) int {
	score := 0
	if row.Active {
		score += 4
	}
	if row.FreshnessTracked {
		score += 3
	}
	if row.AutoMaterialized {
		score += 3
	}
	if !strings.EqualFold(strings.TrimSpace(row.PartitionType), "unpartitioned") {
		score += 2
	}
	if strings.TrimSpace(row.Description) != "" {
		score++
	}
	return score
}

func assetTypeTone(value string) string {
	switch strings.ToUpper(strings.TrimSpace(value)) {
	case domain.AssetTypeTable:
		return "success"
	case domain.AssetTypeView:
		return "accent"
	case domain.AssetTypeModel:
		return "attention"
	case domain.AssetTypeNotebook:
		return "severe"
	default:
		return "accent"
	}
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

type assetDetailSummary struct {
	MaterializationMode  string
	PartitionLabel       string
	LatestRunStatus      string
	LatestMaterializedAt string
	PartitionHint        string
}

func assetDetailPage(d assetDetailPageData) Node {
	summary := buildAssetDetailSummary(d)
	upstreamCount := len(d.UpstreamAssetKeys)
	downstreamCount := len(d.DownstreamAssetKeys)
	dependencyCount := upstreamCount + downstreamCount
	partitionCoverage := assetPartitionCoverage(d.PartitionStatus)

	upstream := Node(P(Class("color-fg-muted"), Text("No upstream dependencies.")))
	if len(d.UpstreamAssetKeys) > 0 {
		items := make([]Node, 0, len(d.UpstreamAssetKeys))
		for i := range d.UpstreamAssetKeys {
			items = append(items, Li(Class("asset-link-list-item"), A(Href("/ui/assets/"+d.UpstreamAssetKeys[i]), Text(d.UpstreamAssetKeys[i]))))
		}
		upstream = Ul(Class("asset-link-list"), Group(items))
	}

	downstream := Node(P(Class("color-fg-muted"), Text("No downstream dependencies.")))
	if len(d.DownstreamAssetKeys) > 0 {
		items := make([]Node, 0, len(d.DownstreamAssetKeys))
		for i := range d.DownstreamAssetKeys {
			items = append(items, Li(Class("asset-link-list-item"), A(Href("/ui/assets/"+d.DownstreamAssetKeys[i]), Text(d.DownstreamAssetKeys[i]))))
		}
		downstream = Ul(Class("asset-link-list"), Group(items))
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
	runsTable := Node(P(Class("color-fg-muted"), Text("No runs yet.")))
	if len(runRows) > 0 {
		headCells := []Node{Th(Text("Run")), Th(Text("Status")), Th(Text("Trigger")), Th(Text("Window")), Th(Text("Attempts")), Th(Text("Duration"))}
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
		runsTable = Table(Class("data-table"), THead(Tr(Group(headCells))), TBody(Group(rowsWithOptionalError)))
	}

	matRows := make([]Node, 0, len(d.Materializations))
	for i := range d.Materializations {
		m := d.Materializations[i]
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
	matTable := Node(P(Class("color-fg-muted"), Text("No materializations yet.")))
	if len(matRows) > 0 {
		matTable = Table(Class("data-table"), THead(Tr(Th(Text("Materialization")), Th(Text("Partition")), Th(Text("Rows")), Th(Text("Schema Hash")), Th(Text("Metadata")), Th(Text("Materialized At")))), TBody(Group(matRows)))
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
						Div(Class("asset-detail-dependency-grid"),
							Div(Class("asset-detail-subpanel"), H3(Text("Upstream")), P(Class("asset-detail-subpanel-copy"), Text("Inputs that must be ready before this asset can run.")), upstream),
							Div(Class("asset-detail-subpanel"), H3(Text("Downstream")), P(Class("asset-detail-subpanel-copy"), Text("Consumers and derivatives affected by this asset.")), downstream),
						),
						dependencyAdjacencyView(d.AssetKey, d.DependencyEdges),
					),
					Div(Class(cardClass("asset-detail-section")),
						Div(Class("assets-section-head"), H2(Text("Execution health")), P(Class(mutedClass()), Text("The most recent runs, retries, and failure signatures in one place."))),
						Div(Class("asset-detail-health-grid"),
							Div(Class("asset-detail-health-panel"), H3(Text("Recent Runs")), runsTable),
							Div(Class("asset-detail-health-panel"), H3(Text("Retry Timeline")), retryTimelineNode),
						),
						Div(Class("asset-detail-health-panel asset-detail-health-panel-wide"), H3(Text("Failure Root Cause")), failureRootCauseNode),
					),
					Div(Class(cardClass("asset-detail-section")),
						Div(Class("assets-section-head"), H2(Text("Materialization history")), P(Class(mutedClass()), Text("Completed outputs and configured checks for this asset."))),
						Div(Class("asset-detail-history-grid"),
							Div(Class("asset-detail-history-panel"), H3(Text("Materializations")), matTable),
							Div(Class("asset-detail-history-panel"), H3(Text("Checks")), checksTable),
						),
					),
					Div(Class(cardClass("asset-detail-section")),
						Div(Class("assets-section-head"), H2(Text("Partitions and recovery")), P(Class(mutedClass()), Text("Partition coverage, freshness drift, and backfill activity."))),
						partitionSummary(d.PartitionStatus),
						partitionCalendarNode,
						Div(Class("asset-detail-history-grid"),
							Div(Class("asset-detail-history-panel"), H3(Text("Partitions")), partitionsTable),
							Div(Class("asset-detail-history-panel"), H3(Text("Backfills")), backfillsTable),
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
		tone := partitionStatusTone(status)
		if tone == "success" {
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

func dependencyAdjacencyView(assetKey string, edges []assetDependencyEdgeData) Node {
	if len(edges) == 0 {
		return Div(Class("mb-2"), P(Class("color-fg-muted"), Text("No dependency edges recorded.")))
	}

	graphData := buildAssetGraphData(assetKey, edges)
	nodesJSON, edgesJSON := assetGraphJSON(graphData)
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
		P(Class("color-fg-muted"), Text("Interactive dependency map for "+assetKey+":")),
		El("asset-graph-view",
			Class("asset-graph-host"),
			Attr("nodes", nodesJSON),
			Attr("edges", edgesJSON),
		),
		Details(
			Class("mb-2"),
			Summary(Text("Adjacency list")),
			Ul(Class("mb-2"), Group(edgeItems)),
		),
		Details(
			Class("mb-2"),
			Summary(Text("Mermaid view")),
			Pre(Class("Box p-2"), Code(Text(strings.Join(lines, "\n")))),
		),
	)
}

type assetGraphNodeData struct {
	ID       string         `json:"id"`
	Label    string         `json:"label"`
	Role     string         `json:"role"`
	Position map[string]int `json:"position"`
}

type assetGraphEdgeData struct {
	ID     string `json:"id"`
	Source string `json:"source"`
	Target string `json:"target"`
}

type assetGraphPayload struct {
	Nodes []assetGraphNodeData `json:"nodes"`
	Edges []assetGraphEdgeData `json:"edges"`
}

func buildAssetGraphData(assetKey string, edges []assetDependencyEdgeData) assetGraphPayload {
	graphEdges := make([]assetGraphEdgeData, 0, len(edges))
	roles := map[string]string{assetKey: "current"}
	upstream := make([]string, 0, len(edges))
	downstream := make([]string, 0, len(edges))
	for i := range edges {
		edge := edges[i]
		graphEdges = append(graphEdges, assetGraphEdgeData{ID: fmt.Sprintf("e%d", i), Source: edge.FromKey, Target: edge.ToKey})
		if edge.ToKey == assetKey {
			roles[edge.FromKey] = "upstream"
			upstream = append(upstream, edge.FromKey)
		} else if edge.FromKey == assetKey {
			roles[edge.ToKey] = "downstream"
			downstream = append(downstream, edge.ToKey)
		} else {
			if _, ok := roles[edge.FromKey]; !ok {
				roles[edge.FromKey] = "related"
			}
			if _, ok := roles[edge.ToKey]; !ok {
				roles[edge.ToKey] = "related"
			}
		}
	}

	ids := make([]string, 0, len(roles))
	for id := range roles {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	nodes := make([]assetGraphNodeData, 0, len(ids))
	for _, id := range ids {
		nodes = append(nodes, assetGraphNodeData{ID: id, Label: id, Role: roles[id]})
	}
	positionAssetGraphNodes(nodes, assetKey, dedupeStrings(upstream), dedupeStrings(downstream))
	return assetGraphPayload{Nodes: nodes, Edges: graphEdges}
}

func positionAssetGraphNodes(nodes []assetGraphNodeData, assetKey string, upstream []string, downstream []string) {
	positions := map[string]map[string]int{assetKey: {"x": 320, "y": 120}}
	for i := range upstream {
		positions[upstream[i]] = map[string]int{"x": 36, "y": 36 + i*96}
	}
	for i := range downstream {
		positions[downstream[i]] = map[string]int{"x": 604, "y": 36 + i*96}
	}
	relatedIndex := 0
	for i := range nodes {
		if pos, ok := positions[nodes[i].ID]; ok {
			nodes[i].Position = pos
			continue
		}
		nodes[i].Position = map[string]int{"x": 320, "y": 240 + relatedIndex*96}
		relatedIndex++
	}
}

func assetGraphJSON(payload assetGraphPayload) (string, string) {
	nodesJSON, err := json.Marshal(payload.Nodes)
	if err != nil {
		return "[]", "[]"
	}
	edgesJSON, err := json.Marshal(payload.Edges)
	if err != nil {
		return string(nodesJSON), "[]"
	}
	return string(nodesJSON), string(edgesJSON)
}

func dedupeStrings(values []string) []string {
	seen := map[string]struct{}{}
	out := make([]string, 0, len(values))
	for i := range values {
		if _, ok := seen[values[i]]; ok {
			continue
		}
		seen[values[i]] = struct{}{}
		out = append(out, values[i])
	}
	return out
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
	if delta < time.Minute {
		return delta.Round(time.Second).String()
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

func boolLabel(v bool) string {
	if v {
		return "true"
	}
	return "false"
}
