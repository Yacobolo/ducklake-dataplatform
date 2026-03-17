package core

import (
	"strings"

	. "maragu.dev/gomponents"
	. "maragu.dev/gomponents/html"
)

func ClassNames(parts ...string) string {
	filtered := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		filtered = append(filtered, part)
	}
	return strings.Join(filtered, " ")
}

func cardClass(extra ...string) string {
	return ClassNames(
		"rounded-xl border border-border bg-background p-4 shadow-xs",
		strings.Join(extra, " "),
	)
}

func mutedClass() string {
	return "text-xs text-muted"
}

func buttonBaseClass(size string) string {
	sizeClasses := "min-h-10 px-4 py-2 text-sm"
	switch strings.TrimSpace(size) {
	case "small":
		sizeClasses = "min-h-8 px-3 py-1.5 text-xs"
	}
	return ClassNames(
		"inline-flex items-center justify-center gap-2 rounded-lg border font-medium transition-colors duration-100 ease-out focus-visible:outline-2 focus-visible:outline-offset-0 focus-visible:outline-[var(--color-ring)] disabled:cursor-not-allowed disabled:border-border-muted disabled:bg-surface-muted disabled:text-muted",
		"border-border bg-background text-foreground hover:bg-surface-muted active:bg-surface-muted",
		sizeClasses,
	)
}

func secondaryButtonClass(size ...string) string {
	buttonSize := ""
	if len(size) > 0 {
		buttonSize = size[0]
	}
	return buttonBaseClass(buttonSize)
}

func primaryButtonClass(size ...string) string {
	buttonSize := ""
	if len(size) > 0 {
		buttonSize = size[0]
	}
	return ClassNames(
		buttonBaseClass(buttonSize),
		"border-transparent bg-primary text-primary-foreground hover:opacity-90 active:opacity-85",
	)
}

func dangerButtonClass(size ...string) string {
	buttonSize := ""
	if len(size) > 0 {
		buttonSize = size[0]
	}
	return ClassNames(
		buttonBaseClass(buttonSize),
		"border-transparent bg-danger text-danger-foreground hover:opacity-90 active:opacity-85",
	)
}

func iconButtonClass(size string) string {
	sizeClasses := "h-10 w-10"
	if strings.TrimSpace(size) == "small" {
		sizeClasses = "h-8 w-8"
	}
	return ClassNames(secondaryButtonClass(size), sizeClasses, "p-0")
}

func linkButtonClass(base string) string {
	return ClassNames(
		base,
		"no-underline visited:text-inherit hover:text-inherit active:text-inherit",
	)
}

func IconGlyphClass() string {
	return "h-4 w-4"
}

func NavIconClass(extra ...string) string {
	return ClassNames("h-4 w-4 shrink-0", strings.Join(extra, " "))
}

func ErrorTextClass(extra ...string) string {
	return ClassNames("mt-0 text-[var(--color-danger-text)]", strings.Join(extra, " "))
}

func formControlClass(extra ...string) string {
	return ClassNames(
		"w-full max-w-full rounded-lg border border-border bg-background px-3 py-2 text-sm text-foreground placeholder:text-muted focus-visible:outline-2 focus-visible:outline-offset-0 focus-visible:outline-[var(--color-ring)]",
		strings.Join(extra, " "),
	)
}

func tableWrapClass(extra ...string) string {
	return ClassNames("overflow-x-auto", strings.Join(extra, " "))
}

func buttonRowClass(extra ...string) string {
	return ClassNames("mt-1 flex flex-wrap items-center gap-2 [&_form]:m-0 [&_form]:inline-flex", strings.Join(extra, " "))
}

func FallbackString(value, fallback string) string {
	if strings.TrimSpace(value) == "" {
		return fallback
	}
	return value
}

func StringPtr(v *string) string {
	if v == nil || *v == "" {
		return "-"
	}
	return *v
}

func sectionTitleClass() string {
	return "m-0 text-lg font-semibold text-foreground"
}

func sectionCopyClass() string {
	return "m-0 text-sm text-muted"
}

func subtleLinkClass() string {
	return "text-muted no-underline hover:text-foreground"
}

func dataTableClass(extra ...string) string {
	return ClassNames("min-w-full border-collapse overflow-hidden rounded-xl border border-border bg-background [&_tbody_tr:hover]:bg-surface-muted [&_td]:border-b [&_td]:border-border [&_td]:px-4 [&_td]:py-3 [&_td]:align-top [&_td]:text-[0.8125rem] [&_th]:sticky [&_th]:top-0 [&_th]:z-[1] [&_th]:border-b [&_th]:border-border [&_th]:bg-surface-muted [&_th]:px-4 [&_th]:py-3 [&_th]:text-left [&_th]:text-[0.8125rem] [&_th]:font-semibold [&_th]:uppercase [&_th]:tracking-[0.02em] [&_th]:text-muted", strings.Join(extra, " "))
}

func labelClass(tone string) string {
	base := "inline-flex items-center rounded-full border border-transparent px-2 py-0.5 text-xs font-medium"
	switch tone {
	case "accent":
		return ClassNames(base, "bg-accent-muted text-accent")
	case "attention":
		return ClassNames(base, "bg-warning-muted text-warning-text")
	case "success":
		return ClassNames(base, "bg-success-muted text-success-text")
	case "severe":
		return ClassNames(base, "bg-danger-muted text-danger-text")
	default:
		return ClassNames(base, "bg-surface-muted text-foreground")
	}
}

func StatusLabel(text, tone string) Node {
	return Span(Class(labelClass(tone)), Text(text))
}

func DetailsClass(extra ...string) string {
	return ClassNames("relative inline-block", strings.Join(extra, " "))
}

func DetailsSummaryClass(extra ...string) string {
	return ClassNames("list-none [&::-webkit-details-marker]:hidden", strings.Join(extra, " "))
}

func DropdownMenuClass(extra ...string) string {
	return ClassNames("absolute right-0 top-full z-20 mt-1 min-w-40 rounded-xl border border-border bg-popover p-1 shadow-md", strings.Join(extra, " "))
}

func DropdownItemClass(extra ...string) string {
	return ClassNames("flex w-full items-center gap-2 rounded-lg px-3 py-2 text-left text-[0.8125rem] no-underline hover:bg-surface-muted", strings.Join(extra, " "))
}

func MetaGridClass(extra ...string) string {
	return ClassNames("grid gap-3 sm:grid-cols-2", strings.Join(extra, " "))
}

func MetaRowClass() string {
	return "grid gap-1 rounded-lg border border-border bg-background px-3 py-3"
}

func MetaLabelClass() string {
	return "text-[11px] font-semibold uppercase tracking-[0.04em] text-muted"
}

func MetaValueClass() string {
	return "m-0 text-sm text-foreground"
}
