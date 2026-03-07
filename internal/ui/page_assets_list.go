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
		return Div(emptyStateCard("No asset showcase yet.", "Open catalogs", "/ui/catalogs"))
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

func boolLabel(v bool) string {
	if v {
		return "true"
	}
	return "false"
}
