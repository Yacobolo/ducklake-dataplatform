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
		"rounded-xl border border-[var(--borderColor-default)] bg-[var(--bgColor-default)] p-4 shadow-xs",
		strings.Join(extra, " "),
	)
}

func mutedClass() string {
	return "text-xs text-[var(--fgColor-muted)]"
}

func buttonBaseClass(size string) string {
	sizeClasses := "min-h-10 px-4 py-2 text-sm"
	switch strings.TrimSpace(size) {
	case "small":
		sizeClasses = "min-h-10 px-3 py-2 text-sm"
	}
	return ClassNames(
		"inline-flex items-center justify-center gap-2 rounded-md border font-semibold shadow-xs transition-colors duration-100 ease-out focus-visible:outline-2 focus-visible:outline-offset-0 focus-visible:outline-[var(--focus-outlineColor)] disabled:cursor-not-allowed",
		sizeClasses,
	)
}

func secondaryButtonClass(size ...string) string {
	buttonSize := ""
	if len(size) > 0 {
		buttonSize = size[0]
	}
	return ClassNames(
		buttonBaseClass(buttonSize),
		"border-[var(--button-default-borderColor-rest)] bg-[var(--button-default-bgColor-rest)] text-[var(--button-default-fgColor-rest)] hover:border-[var(--button-default-borderColor-hover)] hover:bg-[var(--button-default-bgColor-hover)] active:border-[var(--button-default-borderColor-active)] active:bg-[var(--button-default-bgColor-active)]",
		"disabled:border-[var(--button-default-borderColor-disabled)] disabled:bg-[var(--button-default-bgColor-disabled)] disabled:text-[var(--fgColor-muted)]",
	)
}

func primaryButtonClass(size ...string) string {
	buttonSize := ""
	if len(size) > 0 {
		buttonSize = size[0]
	}
	return ClassNames(
		buttonBaseClass(buttonSize),
		"border-[var(--button-primary-borderColor-rest)] bg-[var(--button-primary-bgColor-rest)] text-[var(--button-primary-fgColor-rest)] hover:border-[var(--button-primary-borderColor-hover)] hover:bg-[var(--button-primary-bgColor-hover)] active:border-[var(--button-primary-borderColor-active)] active:bg-[var(--button-primary-bgColor-active)]",
		"disabled:border-[var(--button-primary-borderColor-disabled)] disabled:bg-[var(--button-primary-bgColor-disabled)] disabled:text-[var(--button-primary-fgColor-disabled)]",
	)
}

func dangerButtonClass(size ...string) string {
	buttonSize := ""
	if len(size) > 0 {
		buttonSize = size[0]
	}
	return ClassNames(
		buttonBaseClass(buttonSize),
		"border-[var(--button-danger-borderColor-rest)] bg-[var(--button-danger-bgColor-rest)] text-[var(--button-danger-fgColor-rest)] hover:border-[var(--button-danger-borderColor-hover)] hover:bg-[var(--button-danger-bgColor-hover)] hover:text-[var(--button-danger-fgColor-hover)] active:border-[var(--button-danger-borderColor-active)] active:bg-[var(--button-danger-bgColor-active)] active:text-[var(--button-danger-fgColor-active)]",
		"disabled:border-[var(--button-danger-borderColor-rest)] disabled:bg-[var(--button-danger-bgColor-disabled)] disabled:text-[var(--button-danger-fgColor-disabled)]",
	)
}

func iconButtonClass(size string) string {
	sizeClasses := "h-10 w-10"
	if strings.TrimSpace(size) == "small" {
		sizeClasses = "h-11 w-11"
	}
	return ClassNames(secondaryButtonClass(size), sizeClasses, "p-0")
}

func linkButtonClass(base string) string {
	return ClassNames(
		base,
		"no-underline",
	)
}

func IconGlyphClass() string {
	return "h-4 w-4"
}

func NavIconClass(extra ...string) string {
	return ClassNames("h-4 w-4 shrink-0", strings.Join(extra, " "))
}

func ErrorTextClass(extra ...string) string {
	return ClassNames("mt-0 text-[var(--fgColor-danger)]", strings.Join(extra, " "))
}

func formControlClass(extra ...string) string {
	return ClassNames(
		"w-full max-w-full rounded-lg border border-[var(--borderColor-default)] bg-[var(--bgColor-default)] px-3 py-2 text-sm text-[var(--fgColor-default)] placeholder:text-[var(--fgColor-muted)] focus-visible:outline-2 focus-visible:outline-offset-0 focus-visible:outline-[var(--focus-outlineColor)]",
		strings.Join(extra, " "),
	)
}

func tableWrapClass(extra ...string) string {
	return ClassNames("overflow-hidden rounded-xl border border-[var(--borderColor-default)] bg-[var(--bgColor-default)] shadow-xs", strings.Join(extra, " "))
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
	return "m-0 text-lg font-semibold text-[var(--fgColor-default)]"
}

func sectionCopyClass() string {
	return "m-0 text-sm text-[var(--fgColor-muted)]"
}

func subtleLinkClass() string {
	return "text-[var(--fgColor-muted)] no-underline hover:text-[var(--fgColor-default)]"
}

func dataTableClass(extra ...string) string {
	return ClassNames("min-w-full border-collapse bg-[var(--bgColor-default)] [&_thead]:bg-[var(--bgColor-muted)] [&_tbody_tr]:border-b [&_tbody_tr]:border-[var(--borderColor-default)] [&_tbody_tr:hover]:bg-[color-mix(in_srgb,var(--bgColor-muted)_70%,transparent)] [&_tbody_tr:last-child]:border-b-0 [&_td]:px-6 [&_td]:py-4 [&_td]:align-middle [&_td]:text-sm [&_th]:px-6 [&_th]:py-4 [&_th]:text-left [&_th]:text-xs [&_th]:font-semibold [&_th]:uppercase [&_th]:tracking-[0.08em] [&_th]:text-[var(--fgColor-muted)]", strings.Join(extra, " "))
}

func labelClass(tone string) string {
	base := "inline-flex items-center rounded-full border border-transparent px-2 py-0.5 text-xs font-medium"
	switch tone {
	case "accent":
		return ClassNames(base, "bg-[var(--bgColor-accent-muted)] text-[var(--fgColor-accent)]")
	case "attention":
		return ClassNames(base, "bg-[var(--bgColor-attention-muted)] text-[var(--fgColor-attention)]")
	case "success":
		return ClassNames(base, "bg-[var(--bgColor-success-muted)] text-[var(--fgColor-success)]")
	case "severe":
		return ClassNames(base, "bg-[var(--bgColor-danger-muted)] text-[var(--fgColor-danger)]")
	default:
		return ClassNames(base, "bg-[var(--bgColor-muted)] text-[var(--fgColor-default)]")
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
	return ClassNames("absolute right-0 top-full z-20 mt-1 min-w-40 rounded-xl border border-[var(--borderColor-default)] bg-[var(--overlay-bgColor)] p-1 shadow-md", strings.Join(extra, " "))
}

func DropdownItemClass(extra ...string) string {
	return ClassNames("flex w-full items-center gap-2 rounded-lg px-3 py-2 text-left text-[0.8125rem] no-underline hover:bg-[var(--bgColor-muted)]", strings.Join(extra, " "))
}

func MetaGridClass(extra ...string) string {
	return ClassNames("grid gap-3 sm:grid-cols-2", strings.Join(extra, " "))
}

func MetaRowClass() string {
	return "grid gap-1 rounded-lg border border-[var(--borderColor-default)] bg-[var(--bgColor-default)] px-3 py-3"
}

func MetaLabelClass() string {
	return "text-[11px] font-semibold uppercase tracking-[0.04em] text-[var(--fgColor-muted)]"
}

func MetaValueClass() string {
	return "m-0 text-sm text-[var(--fgColor-default)]"
}
