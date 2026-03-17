package catalogs

import (
	"encoding/json"

	"duck-demo/internal/ui/core"
)

type workspaceAsideTab = core.WorkspaceAsideTab
type catalogExplorerObjectItem = core.CatalogExplorerObjectItem
type catalogExplorerSchemaItem = core.CatalogExplorerSchemaItem
type catalogExplorerCatalogItem = core.CatalogExplorerCatalogItem
type catalogExplorerPanelData = core.CatalogExplorerPanelData

func mapJSON(m map[string]string) string {
	if len(m) == 0 {
		return "-"
	}
	b, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return "-"
	}
	return string(b)
}

func stringsJoin(values []string) string {
	if len(values) == 0 {
		return "-"
	}
	out := values[0]
	for i := 1; i < len(values); i++ {
		out += ", " + values[i]
	}
	return out
}

func catalogSectionClass(extra ...string) string {
	return core.ClassNames("flex flex-col gap-3 rounded-xl border border-border bg-surface-muted p-4", core.ClassNames(extra...))
}

func catalogMetaListClass(extra ...string) string {
	return core.MetaGridClass(extra...)
}

func catalogMetaRowClass() string {
	return core.MetaRowClass()
}

func catalogMetaLabelClass() string {
	return core.MetaLabelClass()
}

func catalogMetaValueClass() string {
	return core.MetaValueClass()
}

func catalogTabsClass() string {
	return "flex flex-wrap gap-2 border-b border-border pb-3"
}

func catalogTabClass(active bool) string {
	base := "rounded-lg px-3 py-2 text-sm font-medium no-underline transition-colors"
	if active {
		return core.ClassNames(base, "bg-accent-muted text-accent")
	}
	return core.ClassNames(base, "text-muted hover:bg-background hover:text-foreground")
}

func catalogHistoryFilterClass(active bool) string {
	base := "inline-flex min-h-9 items-center rounded-full border px-3 text-sm no-underline transition-colors"
	if active {
		return core.ClassNames(base, "border-border-accent bg-accent-muted text-accent")
	}
	return core.ClassNames(base, "border-border-muted text-muted hover:text-foreground")
}

func catalogBreadcrumbClass() string {
	return "m-0"
}

func catalogBreadcrumbListClass() string {
	return "flex max-w-full min-w-0 flex-wrap items-center gap-1 text-xs text-muted"
}

func catalogBreadcrumbItemClass() string {
	return "inline-flex min-w-0 items-center gap-1"
}

func catalogBreadcrumbLabelClass(current bool) string {
	base := "inline-block max-w-[min(32ch,40vw)] overflow-hidden text-ellipsis whitespace-nowrap align-bottom"
	if current {
		return core.ClassNames(base, "font-semibold text-foreground")
	}
	return base
}

func catalogOverviewToolbarClass() string {
	return "flex flex-wrap items-center justify-between gap-3"
}

func catalogSectionTitleClass() string {
	return "m-0 text-lg font-semibold text-foreground"
}

func catalogMutedCopyClass() string {
	return "text-sm text-muted"
}

func catalogTableWrapClass(extra ...string) string {
	return core.ClassNames("overflow-x-auto", core.ClassNames(extra...))
}

func catalogButtonRowClass(extra ...string) string {
	return core.ClassNames("mt-0 flex flex-wrap items-center gap-2 [&_form]:m-0 [&_form]:inline-flex", core.ClassNames(extra...))
}
