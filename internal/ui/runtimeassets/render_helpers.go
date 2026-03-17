package runtimeassets

import (
	"duck-demo/internal/ui/core"

	. "maragu.dev/gomponents"
)

func statusLabel(text, tone string) Node {
	return statusPill(text, tone)
}

func assetBadgeRowClass() string {
	return "flex flex-wrap items-center gap-2"
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

func assetTableSubtitleClass() string {
	return "mt-1 mb-0 text-xs leading-5 text-[var(--fgColor-muted)]"
}
