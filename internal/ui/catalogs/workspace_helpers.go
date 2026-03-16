package catalogs

import (
	"encoding/json"

	"duck-demo/internal/ui/core"

	. "maragu.dev/gomponents"
	. "maragu.dev/gomponents/html"
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
	return core.ClassNames("flex flex-col gap-3 rounded-xl border border-[var(--borderColor-default)] bg-[var(--bgColor-muted)] p-4", core.ClassNames(extra...))
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
	return "flex flex-wrap gap-2 border-b border-[var(--borderColor-default)] pb-3"
}

func catalogTabClass(active bool) string {
	base := "rounded-lg px-3 py-2 text-sm font-medium no-underline transition-colors"
	if active {
		return core.ClassNames(base, "bg-[var(--bgColor-accent-muted)] text-[var(--fgColor-accent)]")
	}
	return core.ClassNames(base, "text-[var(--fgColor-muted)] hover:bg-[var(--bgColor-default)] hover:text-[var(--fgColor-default)]")
}

func catalogHistoryFilterClass(active bool) string {
	base := "inline-flex min-h-9 items-center rounded-full border px-3 text-sm no-underline transition-colors"
	if active {
		return core.ClassNames(base, "border-[var(--borderColor-accent-emphasis)] bg-[var(--bgColor-accent-muted)] text-[var(--fgColor-accent)]")
	}
	return core.ClassNames(base, "border-[var(--borderColor-muted)] text-[var(--fgColor-muted)] hover:text-[var(--fgColor-default)]")
}

func catalogBreadcrumbClass() string {
	return "m-0"
}

func catalogBreadcrumbListClass() string {
	return "flex max-w-full min-w-0 flex-wrap items-center gap-1 text-xs text-[var(--fgColor-muted)]"
}

func catalogBreadcrumbItemClass() string {
	return "inline-flex min-w-0 items-center gap-1"
}

func catalogBreadcrumbLabelClass(current bool) string {
	base := "inline-block max-w-[min(32ch,40vw)] overflow-hidden text-ellipsis whitespace-nowrap align-bottom"
	if current {
		return core.ClassNames(base, "font-semibold text-[var(--fgColor-default)]")
	}
	return base
}

func catalogOverviewToolbarClass() string {
	return "flex flex-wrap items-center justify-between gap-3"
}

func catalogSectionTitleClass() string {
	return "m-0 text-lg font-semibold text-[var(--fgColor-default)]"
}

func catalogMutedCopyClass() string {
	return "text-sm text-[var(--fgColor-muted)]"
}

func catalogTableWrapClass(extra ...string) string {
	return core.ClassNames("overflow-x-auto", core.ClassNames(extra...))
}

func catalogButtonRowClass(extra ...string) string {
	return core.ClassNames("mt-0 flex flex-wrap items-center gap-2 [&_form]:m-0 [&_form]:inline-flex", core.ClassNames(extra...))
}

func actionMenu(label string, items ...Node) Node {
	return Details(
		Class(core.DetailsClass()),
		Summary(Class("list-none [&::-webkit-details-marker]:hidden inline-flex min-h-[var(--control-small-size)] items-center justify-center rounded-lg border border-[var(--borderColor-default)] bg-[var(--bgColor-default)] px-3 text-sm font-medium text-[var(--fgColor-default)] shadow-[var(--shadow-resting-xsmall)] hover:bg-[var(--control-bgColor-hover)]"), Text(label)),
		Div(Class(core.DropdownMenuClass()), Group(items)),
	)
}

func actionMenuPost(action, label string, csrfField func() Node, danger bool) Node {
	btnClass := core.DropdownItemClass()
	if danger {
		btnClass += " text-[var(--fgColor-danger)] hover:bg-[var(--bgColor-danger-muted)]"
	} else {
		btnClass += " text-[var(--fgColor-default)]"
	}
	button := Form(
		Method("post"),
		Action(action),
		csrfField(),
		Button(Type("submit"), Class(btnClass), Span(Text(label))),
	)
	if danger {
		return Group([]Node{
			Div(Class("dropdown-divider my-1 border-t border-[var(--borderColor-muted)]")),
			button,
		})
	}
	return button
}
