package catalogs

import (
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"duck-demo/internal/domain"
	"duck-demo/internal/ui/core"

	. "maragu.dev/gomponents"
	. "maragu.dev/gomponents/html"
)

type workspaceAsideTab = core.WorkspaceAsideTab
type catalogExplorerObjectItem = core.CatalogExplorerObjectItem
type catalogExplorerSchemaItem = core.CatalogExplorerSchemaItem
type catalogExplorerCatalogItem = core.CatalogExplorerCatalogItem
type catalogExplorerPanelData = core.CatalogExplorerPanelData

func appPage(title, active string, principal domain.ContextPrincipal, body ...Node) Node {
	return core.AppPage(title, active, principal, body...)
}

func workspaceLayout(className string, aside Node, main ...Node) Node {
	return core.WorkspaceLayout(className, aside, main...)
}

func workspaceAside(storageKey, className string, tabs []workspaceAsideTab, defaultTab string) Node {
	return core.WorkspaceAside(storageKey, className, tabs, defaultTab)
}

func catalogExplorerPanel(d catalogExplorerPanelData) Node {
	return core.CatalogExplorerPanel(d)
}

func fallbackString(value, fallback string) string {
	if strings.TrimSpace(value) == "" {
		return fallback
	}
	return value
}

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

func stringPtr(v *string) string {
	if v == nil || *v == "" {
		return "-"
	}
	return *v
}

func buttonRowClass(extra ...string) string {
	return core.ButtonRowClass(extra...)
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

func navIconClass(extra ...string) string {
	return core.NavIconClass(extra...)
}

func formControlClass(extra ...string) string {
	return core.FormControlClass(extra...)
}

func sectionTitleClass() string {
	return "m-0 text-lg font-semibold text-[var(--fgColor-default)]"
}

func sectionCopyClass() string {
	return "m-0 text-sm text-[var(--fgColor-muted)]"
}

func subtleLinkClass() string {
	return "text-[var(--fgColor-muted)] no-underline hover:text-[var(--fgColor-default)]"
}

func dataTableClass(extra ...string) string {
	return core.ClassNames("min-w-full border-collapse overflow-hidden rounded-xl border border-[var(--borderColor-default)] bg-[var(--bgColor-default)] [&_tbody_tr:hover]:bg-[var(--control-bgColor-hover)] [&_td]:border-b [&_td]:border-[var(--borderColor-default)] [&_td]:px-4 [&_td]:py-3 [&_td]:align-top [&_td]:text-[0.8125rem] [&_th]:sticky [&_th]:top-0 [&_th]:z-[1] [&_th]:border-b [&_th]:border-[var(--borderColor-default)] [&_th]:bg-[var(--bgColor-muted)] [&_th]:px-4 [&_th]:py-3 [&_th]:text-left [&_th]:text-[0.8125rem] [&_th]:font-semibold [&_th]:uppercase [&_th]:tracking-[0.02em] [&_th]:text-[var(--fgColor-muted)]", core.ClassNames(extra...))
}

func tableWrapClass(extra ...string) string {
	return core.TableWrapClass(extra...)
}

func catalogSectionClass(extra ...string) string {
	return core.ClassNames("flex flex-col gap-3 rounded-xl border border-[var(--borderColor-default)] bg-[var(--bgColor-muted)] p-4", core.ClassNames(extra...))
}

func catalogMetaListClass(extra ...string) string {
	return core.ClassNames("grid gap-3 sm:grid-cols-2", core.ClassNames(extra...))
}

func catalogMetaRowClass() string {
	return "grid gap-1 rounded-lg border border-[var(--borderColor-default)] bg-[var(--bgColor-default)] px-3 py-3"
}

func catalogMetaLabelClass() string {
	return "text-[11px] font-semibold uppercase tracking-[0.04em] text-[var(--fgColor-muted)]"
}

func catalogMetaValueClass() string {
	return "m-0 text-sm text-[var(--fgColor-default)]"
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

func labelClass(tone string) string {
	base := "inline-flex items-center rounded-full border border-transparent px-2 py-0.5 text-xs font-medium"
	switch tone {
	case "accent":
		return core.ClassNames(base, "bg-[var(--label-blue-bgColor-rest)] text-[var(--label-blue-fgColor-rest)]")
	case "attention":
		return core.ClassNames(base, "bg-[var(--label-yellow-bgColor-rest)] text-[var(--label-yellow-fgColor-rest)]")
	case "success":
		return core.ClassNames(base, "bg-[var(--label-green-bgColor-rest)] text-[var(--label-green-fgColor-rest)]")
	case "severe":
		return core.ClassNames(base, "bg-[var(--label-orange-bgColor-rest)] text-[var(--label-orange-fgColor-rest)]")
	default:
		return core.ClassNames(base, "bg-[var(--label-gray-bgColor-rest)] text-[var(--label-gray-fgColor-rest)]")
	}
}

func statusLabel(text, tone string) Node {
	return Span(Class(labelClass(tone)), Text(text))
}

func detailsClass(extra ...string) string {
	return core.ClassNames("relative inline-block", core.ClassNames(extra...))
}

func detailsSummaryClass(extra ...string) string {
	return core.ClassNames("list-none [&::-webkit-details-marker]:hidden", core.ClassNames(extra...))
}

func dropdownMenuClass(extra ...string) string {
	return core.ClassNames("absolute right-0 top-full z-20 mt-1 min-w-[var(--overlay-width-xsmall)] rounded-xl border border-[var(--overlay-borderColor)] bg-[var(--overlay-bgColor)] p-1 shadow-[var(--shadow-floating-small)]", core.ClassNames(extra...))
}

func dropdownItemClass(extra ...string) string {
	return core.ClassNames("flex w-full items-center gap-2 rounded-lg px-3 py-2 text-left text-[0.8125rem] no-underline hover:bg-[var(--control-bgColor-hover)]", core.ClassNames(extra...))
}

func actionMenu(label string, items ...Node) Node {
	return Details(
		Class(detailsClass()),
		Summary(Class(detailsSummaryClass(secondaryButtonClass("small"))), Text(label)),
		Div(Class(dropdownMenuClass()), Group(items)),
	)
}

func actionMenuPost(action, label string, csrfField func() Node, danger bool) Node {
	btnClass := dropdownItemClass()
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

func principalFromContext(r *http.Request) domain.ContextPrincipal {
	return core.PrincipalFromContext(r.Context())
}

func formatTime(ts time.Time) string {
	if ts.IsZero() {
		return "-"
	}
	return ts.UTC().Format("2006-01-02 15:04 UTC")
}
