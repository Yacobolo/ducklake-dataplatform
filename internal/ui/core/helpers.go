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
		"rounded-xl border border-[var(--borderColor-default)] bg-[var(--bgColor-default)] p-4 shadow-[var(--shadow-resting-xsmall)]",
		strings.Join(extra, " "),
	)
}

func CardClass(extra ...string) string {
	return cardClass(extra...)
}

func mutedClass() string {
	return "text-xs text-[var(--fgColor-muted)]"
}

func MutedClass() string {
	return mutedClass()
}

func buttonBaseClass(size string) string {
	sizeClasses := "min-h-10 px-4 py-2 text-sm"
	switch strings.TrimSpace(size) {
	case "small":
		sizeClasses = "min-h-8 px-3 py-1.5 text-xs"
	}
	return ClassNames(
		"inline-flex items-center justify-center gap-2 rounded-lg border font-medium transition-colors duration-100 ease-out focus-visible:outline-2 focus-visible:outline-offset-0 focus-visible:outline-[var(--focus-outlineColor)] disabled:cursor-not-allowed disabled:border-[var(--control-borderColor-disabled)] disabled:bg-[var(--button-default-bgColor-disabled)] disabled:text-[var(--fgColor-disabled)]",
		"border-[var(--button-default-borderColor-rest)] bg-[var(--button-default-bgColor-rest)] text-[var(--button-default-fgColor-rest)] hover:bg-[var(--button-default-bgColor-hover)] active:bg-[var(--button-default-bgColor-active)]",
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

func SecondaryButtonClass(size ...string) string {
	return secondaryButtonClass(size...)
}

func primaryButtonClass(size ...string) string {
	buttonSize := ""
	if len(size) > 0 {
		buttonSize = size[0]
	}
	return ClassNames(
		buttonBaseClass(buttonSize),
		"border-[var(--button-primary-borderColor-rest)] bg-[var(--button-primary-bgColor-rest)] text-[var(--button-primary-fgColor-rest)] hover:bg-[var(--button-primary-bgColor-hover)] active:bg-[var(--button-primary-bgColor-active)]",
	)
}

func PrimaryButtonClass(size ...string) string {
	return primaryButtonClass(size...)
}

func dangerButtonClass(size ...string) string {
	buttonSize := ""
	if len(size) > 0 {
		buttonSize = size[0]
	}
	return ClassNames(
		buttonBaseClass(buttonSize),
		"border-[var(--button-danger-borderColor-rest)] bg-[var(--button-danger-bgColor-rest)] text-[var(--button-danger-fgColor-rest)] hover:border-[var(--button-danger-borderColor-hover)] hover:bg-[var(--button-danger-bgColor-hover)] hover:text-[var(--button-danger-fgColor-hover)]",
	)
}

func DangerButtonClass(size ...string) string {
	return dangerButtonClass(size...)
}

func iconButtonClass(size string) string {
	sizeClasses := "h-10 w-10"
	if strings.TrimSpace(size) == "small" {
		sizeClasses = "h-8 w-8"
	}
	return ClassNames(secondaryButtonClass(size), sizeClasses, "p-0")
}

func IconButtonClass(size string) string {
	return iconButtonClass(size)
}

func IconGlyphClass() string {
	return "h-4 w-4"
}

func NavIconClass(extra ...string) string {
	return ClassNames("h-4 w-4 shrink-0", strings.Join(extra, " "))
}

func ErrorTextClass(extra ...string) string {
	return ClassNames("mt-0 text-[var(--danger)]", strings.Join(extra, " "))
}

func formControlClass(extra ...string) string {
	return ClassNames(
		"w-full max-w-full rounded-lg border border-[var(--control-borderColor-rest)] bg-[var(--bgColor-default)] px-3 py-2 text-sm text-[var(--fgColor-default)] placeholder:text-[var(--fgColor-muted)] focus-visible:outline-2 focus-visible:outline-offset-0 focus-visible:outline-[var(--focus-outlineColor)]",
		strings.Join(extra, " "),
	)
}

func FormControlClass(extra ...string) string {
	return formControlClass(extra...)
}

func FormSelectClass(extra ...string) string {
	return formControlClass(extra...)
}

func tableWrapClass(extra ...string) string {
	return ClassNames("overflow-x-auto", strings.Join(extra, " "))
}

func TableWrapClass(extra ...string) string {
	return tableWrapClass(extra...)
}

func buttonRowClass(extra ...string) string {
	return ClassNames("mt-1 flex flex-wrap items-center gap-2 [&_form]:m-0 [&_form]:inline-flex", strings.Join(extra, " "))
}

func ButtonRowClass(extra ...string) string {
	return buttonRowClass(extra...)
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

func SectionTitleClass() string {
	return sectionTitleClass()
}

func sectionCopyClass() string {
	return "m-0 text-sm text-[var(--fgColor-muted)]"
}

func SectionCopyClass() string {
	return sectionCopyClass()
}

func subtleLinkClass() string {
	return "text-[var(--fgColor-muted)] no-underline hover:text-[var(--fgColor-default)]"
}

func SubtleLinkClass() string {
	return subtleLinkClass()
}

func dataTableClass(extra ...string) string {
	return ClassNames("min-w-full border-collapse overflow-hidden rounded-xl border border-[var(--borderColor-default)] bg-[var(--bgColor-default)] [&_tbody_tr:hover]:bg-[var(--control-bgColor-hover)] [&_td]:border-b [&_td]:border-[var(--borderColor-default)] [&_td]:px-4 [&_td]:py-3 [&_td]:align-top [&_td]:text-[0.8125rem] [&_th]:sticky [&_th]:top-0 [&_th]:z-[1] [&_th]:border-b [&_th]:border-[var(--borderColor-default)] [&_th]:bg-[var(--bgColor-muted)] [&_th]:px-4 [&_th]:py-3 [&_th]:text-left [&_th]:text-[0.8125rem] [&_th]:font-semibold [&_th]:uppercase [&_th]:tracking-[0.02em] [&_th]:text-[var(--fgColor-muted)]", strings.Join(extra, " "))
}

func DataTableClass(extra ...string) string {
	return dataTableClass(extra...)
}

func labelClass(tone string) string {
	base := "inline-flex items-center rounded-full border border-transparent px-2 py-0.5 text-xs font-medium"
	switch tone {
	case "accent":
		return ClassNames(base, "bg-[var(--label-blue-bgColor-rest)] text-[var(--label-blue-fgColor-rest)]")
	case "attention":
		return ClassNames(base, "bg-[var(--label-yellow-bgColor-rest)] text-[var(--label-yellow-fgColor-rest)]")
	case "success":
		return ClassNames(base, "bg-[var(--label-green-bgColor-rest)] text-[var(--label-green-fgColor-rest)]")
	case "severe":
		return ClassNames(base, "bg-[var(--label-orange-bgColor-rest)] text-[var(--label-orange-fgColor-rest)]")
	default:
		return ClassNames(base, "bg-[var(--label-gray-bgColor-rest)] text-[var(--label-gray-fgColor-rest)]")
	}
}

func LabelClass(tone string) string {
	return labelClass(tone)
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
	return ClassNames("absolute right-0 top-full z-20 mt-1 min-w-[var(--overlay-width-xsmall)] rounded-xl border border-[var(--overlay-borderColor)] bg-[var(--overlay-bgColor)] p-1 shadow-[var(--shadow-floating-small)]", strings.Join(extra, " "))
}

func DropdownItemClass(extra ...string) string {
	return ClassNames("flex w-full items-center gap-2 rounded-lg px-3 py-2 text-left text-[0.8125rem] no-underline hover:bg-[var(--control-bgColor-hover)]", strings.Join(extra, " "))
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
