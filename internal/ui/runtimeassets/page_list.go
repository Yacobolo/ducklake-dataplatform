package runtimeassets

import (
	"strconv"
	"strings"

	"github.com/Yacobolo/quackstack/internal/domain"
	"github.com/Yacobolo/quackstack/internal/ui/core"

	. "maragu.dev/gomponents"
	. "maragu.dev/gomponents/html"
)

type assetsListRowData struct {
	AssetKey            string
	URL                 string
	Type                string
	Owner               string
	Description         string
	Active              bool
	Updated             string
	FreshnessTracked    bool
	PartitionType       string
	AutoMaterialized    bool
	MaterializationMode string
}

type assetsListSummary struct {
	Total            int
	Active           int
	Partitioned      int
	FreshnessTracked int
	AutoMaterialized int
	ManualOnly       int
}

func assetsListPage(principal domain.ContextPrincipal, rows []assetsListRowData, page domain.PageRequest, total int64, canMaterialize bool, backfillConfigured bool) Node {
	summary := summarizeAssetsRows(rows)
	return core.AppPage(
		"Assets",
		"assets",
		principal,
		core.ListPageLayout(
			core.ListPageHeader("Runtime assets", assetsHeroText(summary, canMaterialize, backfillConfigured), core.SecondaryLink("/ui/catalogs", "", Text("Browse catalogs"))),
			core.SectionSurface(
				core.SectionHeader("Asset signals", "Keep high-level asset coverage metrics separate from the detailed registry table."),
				core.MetricsGrid(
					core.MetricCard("Total", strconv.Itoa(summary.Total), "Registered runtime objects"),
					core.MetricCard("Active", strconv.Itoa(summary.Active), "Assets ready to run"),
					core.MetricCard("Freshness", strconv.Itoa(summary.FreshnessTracked), "Assets with SLAs"),
					core.MetricCard("Auto", strconv.Itoa(summary.AutoMaterialized), "Auto materialized assets"),
				),
			),
			core.ListPageBody(
				assetsListTable(rows),
				core.ListPagination("/ui/assets", page, total),
			),
		),
	)
}

func summarizeAssetsRows(rows []assetsListRowData) assetsListSummary {
	summary := assetsListSummary{Total: len(rows)}
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
	}
	return summary
}

func assetsHeroText(summary assetsListSummary, canMaterialize bool, backfillConfigured bool) string {
	if summary.Total == 0 {
		return "Sync a catalog or declarative config to populate runtime assets."
	}
	text := "Track orchestration-ready tables, views, notebooks, and derived objects in one place."
	if !canMaterialize {
		text += " Materialization requires the appropriate runtime permission."
	}
	if !backfillConfigured {
		text += " Backfill is not configured in this environment."
	}
	return text
}

func assetsListTable(rows []assetsListRowData) Node {
	if len(rows) == 0 {
		return P(Class("text-xs text-[var(--fgColor-muted)]"), Text("No assets found yet."))
	}

	tableRows := make([]Node, 0, len(rows))
	for i := range rows {
		row := rows[i]
		tableRows = append(tableRows, Tr(
			core.TablePrimaryCell(
				core.ResourceIcon("runtime-asset"),
				core.TablePrimaryLink(row.URL, row.AssetKey),
				core.TableSubtleCopy(fallbackString(row.Description, "No description yet.")),
			),
			Td(statusPill(strings.ToUpper(row.Type), assetTypeTone(row.Type))),
			Td(core.TableMetaText(row.Owner)),
			Td(Div(Class("flex flex-wrap gap-2"),
				statusPill(core.TitleizeWords(row.MaterializationMode), "accent"),
				func() Node {
					if row.FreshnessTracked {
						return statusPill("SLA", "success")
					}
					return statusPill("No SLA", "attention")
				}(),
				statusPill(fallbackString(row.PartitionType, "Unpartitioned"), "neutral"),
			)),
			Td(func() Node {
				if row.Active {
					return statusPill("true", "success")
				}
				return statusPill("false", "severe")
			}()),
			Td(core.TableMetaText(row.Updated)),
		))
	}

	return core.TableContainer("",
		core.DataTable("",
			THead(Tr(Th(Scope("col"), Text("Asset key")), Th(Scope("col"), Text("Type")), Th(Scope("col"), Text("Owner")), Th(Scope("col"), Text("Signals")), Th(Scope("col"), Text("Active")), Th(Scope("col"), Text("Updated")))),
			TBody(Group(tableRows)),
		),
	)
}

func statusPill(text, tone string) Node {
	className := "inline-flex items-center rounded-full px-2.5 py-1 text-xs font-medium"
	switch tone {
	case "success":
		className += " bg-[var(--bgColor-success-muted)] text-[var(--fgColor-success)]"
	case "attention":
		className += " bg-[var(--bgColor-attention-muted)] text-[var(--fgColor-attention)]"
	case "severe":
		className += " bg-[var(--bgColor-danger-muted)] text-[var(--fgColor-danger)]"
	case "neutral":
		className += " bg-[var(--bgColor-muted)] text-[var(--fgColor-default)]"
	default:
		className += " bg-[var(--bgColor-accent-muted)] text-[var(--fgColor-accent)]"
	}
	return Span(Class(className), Text(text))
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
