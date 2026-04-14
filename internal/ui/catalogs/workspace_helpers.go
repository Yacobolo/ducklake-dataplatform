package catalogs

import (
	"encoding/json"
	"strings"

	"github.com/Yacobolo/quackstack/internal/ui/core"
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
	return core.ClassNames("flex flex-col gap-3", core.ClassNames(extra...))
}

func catalogMetaListClass(extra ...string) string {
	return core.MetaGridClass(extra...)
}

func catalogMetaRowClass() string {
	return "grid gap-1 border-b border-[var(--borderColor-default)] pb-3 last:border-b-0 last:pb-0"
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

type catalogColumnTypeVisual struct {
	Group string
	Icon  string
	Tone  string
}

func catalogColumnTypeInfo(typeName string) catalogColumnTypeVisual {
	normalized := strings.ToLower(strings.TrimSpace(typeName))
	switch {
	case containsAny(normalized, "timestamp", "datetime", "date", "time", "interval"):
		return catalogColumnTypeVisual{Group: "temporal", Icon: "clock-3", Tone: "blue"}
	case containsAny(normalized, "bool"):
		return catalogColumnTypeVisual{Group: "boolean", Icon: "toggle-left", Tone: "green"}
	case containsAny(normalized, "json", "struct", "map", "list", "array", "union"):
		return catalogColumnTypeVisual{Group: "nested", Icon: "braces", Tone: "plum"}
	case containsAny(normalized, "decimal", "numeric", "number"):
		return catalogColumnTypeVisual{Group: "decimal", Icon: "circle-dollar-sign", Tone: "orange"}
	case containsAny(normalized, "double", "float", "real"):
		return catalogColumnTypeVisual{Group: "floating", Icon: "binary", Tone: "indigo"}
	case containsAny(normalized, "tinyint", "smallint", "integer", "bigint", "hugeint", "utinyint", "usmallint", "uinteger", "ubigint", "int", "serial"):
		return catalogColumnTypeVisual{Group: "integer", Icon: "hash", Tone: "gray"}
	case containsAny(normalized, "blob", "binary", "byte", "bytes", "bit", "varbinary"):
		return catalogColumnTypeVisual{Group: "binary", Icon: "binary", Tone: "red"}
	case containsAny(normalized, "varchar", "char", "text", "string", "uuid", "enum"):
		return catalogColumnTypeVisual{Group: "text", Icon: "file-text", Tone: "teal"}
	default:
		return catalogColumnTypeVisual{Group: "other", Icon: "database", Tone: "gray"}
	}
}

func catalogColumnTypeBadgeClass(tone string) string {
	base := "inline-flex items-center rounded-md px-2 py-0.5 text-xs font-semibold"
	return core.ClassNames(base, catalogColumnBadgeColorClass(tone))
}

func catalogColumnNameIconClass(tone string) string {
	base := "shrink-0"
	return core.ClassNames(base, catalogColumnIconColorClass(tone))
}

func catalogColumnNullableClass(nullable bool) string {
	base := "text-[10px] font-bold uppercase tracking-[0.08em]"
	if nullable {
		return core.ClassNames(base, "text-[var(--fgColor-muted)]")
	}
	return core.ClassNames(base, "text-[var(--fgColor-default)]")
}

func catalogColumnNullableLabel(nullable string) string {
	if strings.EqualFold(strings.TrimSpace(nullable), "true") {
		return "NULLABLE"
	}
	return "REQUIRED"
}

func catalogColumnTypeDisplayLabel(typeName string) string {
	return strings.ToLower(strings.TrimSpace(typeName))
}

func containsAny(haystack string, needles ...string) bool {
	for i := range needles {
		if strings.Contains(haystack, needles[i]) {
			return true
		}
	}
	return false
}

func catalogColumnBadgeColorClass(tone string) string {
	switch strings.TrimSpace(tone) {
	case "blue":
		return "bg-[var(--display-blue-scale-0)] text-[var(--display-blue-scale-6)]"
	case "green":
		return "bg-[var(--display-green-scale-0)] text-[var(--display-green-scale-6)]"
	case "plum":
		return "bg-[var(--display-plum-scale-0)] text-[var(--display-plum-scale-6)]"
	case "orange":
		return "bg-[var(--display-orange-scale-0)] text-[var(--display-orange-scale-6)]"
	case "indigo":
		return "bg-[var(--display-indigo-scale-0)] text-[var(--display-indigo-scale-6)]"
	case "red":
		return "bg-[var(--display-red-scale-0)] text-[var(--display-red-scale-6)]"
	case "teal":
		return "bg-[var(--display-teal-scale-0)] text-[var(--display-teal-scale-6)]"
	default:
		return "bg-[var(--display-gray-scale-0)] text-[var(--display-gray-scale-7)]"
	}
}

func catalogColumnIconColorClass(tone string) string {
	switch strings.TrimSpace(tone) {
	case "blue":
		return "text-[var(--display-blue-scale-6)]"
	case "green":
		return "text-[var(--display-green-scale-6)]"
	case "plum":
		return "text-[var(--display-plum-scale-6)]"
	case "orange":
		return "text-[var(--display-orange-scale-6)]"
	case "indigo":
		return "text-[var(--display-indigo-scale-6)]"
	case "red":
		return "text-[var(--display-red-scale-6)]"
	case "teal":
		return "text-[var(--display-teal-scale-6)]"
	default:
		return "text-[var(--display-gray-scale-7)]"
	}
}
