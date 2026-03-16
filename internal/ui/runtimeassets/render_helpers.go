package runtimeassets

import (
	"duck-demo/internal/domain"
	"duck-demo/internal/ui/core"

	. "maragu.dev/gomponents"
)

func appPage(title, active string, principal domain.ContextPrincipal, body ...Node) Node {
	return core.AppPage(title, active, principal, body...)
}

func cardClass(extra ...string) string {
	return core.CardClass(extra...)
}

func mutedClass() string {
	return core.MutedClass()
}

func primaryButtonClass(size ...string) string {
	return core.PrimaryButtonClass(size...)
}

func secondaryButtonClass(size ...string) string {
	return core.SecondaryButtonClass(size...)
}

func formControlClass(extra ...string) string {
	return core.FormControlClass(extra...)
}

func tableWrapClass(extra ...string) string {
	return core.TableWrapClass(extra...)
}

func uiScriptHref(name string) string {
	return core.UIScriptHref(name)
}

func subtleLinkClass() string {
	return "text-[var(--fgColor-muted)] no-underline hover:text-[var(--fgColor-default)]"
}

func sectionTitleClass() string {
	return "m-0 text-lg font-semibold text-[var(--fgColor-default)]"
}

func sectionCopyClass() string {
	return "m-0 text-sm text-[var(--fgColor-muted)]"
}

func dataTableClass(extra ...string) string {
	return core.ClassNames("min-w-full border-collapse overflow-hidden rounded-xl border border-[var(--borderColor-default)] bg-[var(--bgColor-default)] [&_tbody_tr:hover]:bg-[var(--control-bgColor-hover)] [&_td]:border-b [&_td]:border-[var(--borderColor-default)] [&_td]:px-4 [&_td]:py-3 [&_td]:align-top [&_td]:text-[0.8125rem] [&_th]:sticky [&_th]:top-0 [&_th]:z-[1] [&_th]:border-b [&_th]:border-[var(--borderColor-default)] [&_th]:bg-[var(--bgColor-muted)] [&_th]:px-4 [&_th]:py-3 [&_th]:text-left [&_th]:text-[0.8125rem] [&_th]:font-semibold [&_th]:uppercase [&_th]:tracking-[0.02em] [&_th]:text-[var(--fgColor-muted)]", core.ClassNames(extra...))
}

func statusLabel(text, tone string) Node {
	return statusPill(text, tone)
}

func stringPtr(v *string) string {
	if v == nil || *v == "" {
		return "-"
	}
	return *v
}

func boolLabel(value bool) string {
	if value {
		return "true"
	}
	return "false"
}

func titleizeWords(value string) string {
	return core.TitleizeWords(value)
}

func assetDetailShellClass() string {
	return "flex flex-col gap-4"
}

func assetHeroClass() string {
	return "grid gap-4 rounded-2xl border border-[var(--borderColor-default)] bg-[linear-gradient(135deg,var(--bgColor-muted)_0%,var(--bgColor-default)_65%)] p-5 shadow-[var(--shadow-resting-small)] lg:grid-cols-[minmax(0,1.5fr)_minmax(16rem,0.8fr)]"
}

func assetHeroCopyClass() string {
	return "flex min-w-0 flex-col gap-3"
}

func assetHeroMetaClass() string {
	return "grid gap-2 rounded-xl border border-[var(--borderColor-default)] bg-[var(--bgColor-default)] p-4"
}

func assetKickerClass() string {
	return "m-0 text-[11px] font-semibold uppercase tracking-[0.08em] text-[var(--fgColor-muted)]"
}

func assetTitleRowClass() string {
	return "flex flex-wrap items-center gap-3"
}

func assetTitleClass() string {
	return "m-0 text-3xl font-semibold leading-tight text-[var(--fgColor-default)]"
}

func assetDescriptionClass() string {
	return "m-0 max-w-3xl text-sm leading-6 text-[var(--fgColor-muted)]"
}

func assetBadgeRowClass() string {
	return "flex flex-wrap items-center gap-2"
}

func assetMetricsGridClass() string {
	return "grid gap-3 sm:grid-cols-2 xl:grid-cols-4"
}

func assetMetricCardClass() string {
	return "flex flex-col gap-1 rounded-xl border border-[var(--borderColor-default)] bg-[var(--bgColor-muted)] p-4 shadow-[var(--shadow-resting-xsmall)]"
}

func assetMetricLabelClass() string {
	return "m-0 text-xs font-semibold uppercase tracking-[0.04em] text-[var(--fgColor-muted)]"
}

func assetMetricValueClass() string {
	return "m-0 text-2xl font-semibold text-[var(--fgColor-default)]"
}

func assetMetricHintClass() string {
	return "m-0 text-xs text-[var(--fgColor-muted)]"
}

func assetDetailLayoutClass() string {
	return "grid gap-4 xl:grid-cols-[minmax(0,1.7fr)_minmax(18rem,0.8fr)]"
}

func assetDetailMainClass() string {
	return "flex min-w-0 flex-col gap-4"
}

func assetDetailRailClass() string {
	return "flex min-w-0 flex-col gap-4"
}

func assetSectionClass() string {
	return "flex flex-col gap-4"
}

func assetSectionHeadClass() string {
	return "flex flex-wrap items-start justify-between gap-2"
}

func assetSubGridClass() string {
	return "grid gap-4 lg:grid-cols-2"
}

func assetPanelClass(extra ...string) string {
	return core.ClassNames("flex min-w-0 flex-col gap-3 rounded-xl border border-[var(--borderColor-default)] bg-[var(--bgColor-muted)] p-4", core.ClassNames(extra...))
}

func assetListClass(extra ...string) string {
	return core.ClassNames("grid gap-2", core.ClassNames(extra...))
}

func assetListItemClass() string {
	return "rounded-lg border border-[var(--borderColor-default)] bg-[var(--bgColor-default)] px-3 py-2 text-sm"
}

func assetMetaRowClass() string {
	return "grid gap-1 border-b border-[var(--borderColor-default)] pb-2 last:border-b-0 last:pb-0"
}

func assetMetaLabelClass() string {
	return "text-[11px] font-semibold uppercase tracking-[0.04em] text-[var(--fgColor-muted)]"
}

func assetMetaValueClass() string {
	return "text-sm text-[var(--fgColor-default)]"
}

func assetFactListClass() string {
	return "grid gap-2"
}

func assetFactRowClass() string {
	return "flex items-start justify-between gap-3 rounded-lg border border-[var(--borderColor-default)] bg-[var(--bgColor-default)] px-3 py-2"
}

func assetFactLabelClass() string {
	return "text-xs font-semibold uppercase tracking-[0.04em] text-[var(--fgColor-muted)]"
}

func assetFactValueClass() string {
	return "text-sm text-right text-[var(--fgColor-default)]"
}

func assetTableSubtitleClass() string {
	return "mt-1 mb-0 text-xs leading-5 text-[var(--fgColor-muted)]"
}
