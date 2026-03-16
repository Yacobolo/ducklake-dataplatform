package core

import (
	"strings"
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

func CardClass(extra ...string) string {
	return ClassNames(
		"rounded-xl border border-[var(--borderColor-default)] bg-[var(--bgColor-default)] p-4 shadow-[var(--shadow-resting-xsmall)]",
		strings.Join(extra, " "),
	)
}

func MutedClass() string {
	return "text-xs text-[var(--fgColor-muted)]"
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

func SecondaryButtonClass(size ...string) string {
	buttonSize := ""
	if len(size) > 0 {
		buttonSize = size[0]
	}
	return buttonBaseClass(buttonSize)
}

func PrimaryButtonClass(size ...string) string {
	buttonSize := ""
	if len(size) > 0 {
		buttonSize = size[0]
	}
	return ClassNames(
		buttonBaseClass(buttonSize),
		"border-[var(--button-primary-borderColor-rest)] bg-[var(--button-primary-bgColor-rest)] text-[var(--button-primary-fgColor-rest)] hover:bg-[var(--button-primary-bgColor-hover)] active:bg-[var(--button-primary-bgColor-active)]",
	)
}

func DangerButtonClass(size ...string) string {
	buttonSize := ""
	if len(size) > 0 {
		buttonSize = size[0]
	}
	return ClassNames(
		buttonBaseClass(buttonSize),
		"border-[var(--button-danger-borderColor-rest)] bg-[var(--button-danger-bgColor-rest)] text-[var(--button-danger-fgColor-rest)] hover:border-[var(--button-danger-borderColor-hover)] hover:bg-[var(--button-danger-bgColor-hover)] hover:text-[var(--button-danger-fgColor-hover)]",
	)
}

func IconButtonClass(size string) string {
	sizeClasses := "h-10 w-10"
	if strings.TrimSpace(size) == "small" {
		sizeClasses = "h-8 w-8"
	}
	return ClassNames(SecondaryButtonClass(size), sizeClasses, "p-0")
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

func FormControlClass(extra ...string) string {
	return ClassNames(
		"w-full max-w-full rounded-lg border border-[var(--control-borderColor-rest)] bg-[var(--bgColor-default)] px-3 py-2 text-sm text-[var(--fgColor-default)] placeholder:text-[var(--fgColor-muted)] focus-visible:outline-2 focus-visible:outline-offset-0 focus-visible:outline-[var(--focus-outlineColor)]",
		strings.Join(extra, " "),
	)
}

func FormSelectClass(extra ...string) string {
	return FormControlClass(extra...)
}

func TableWrapClass(extra ...string) string {
	return ClassNames("overflow-x-auto", strings.Join(extra, " "))
}

func ButtonRowClass(extra ...string) string {
	return ClassNames("mt-1 flex flex-wrap items-center gap-2 [&_form]:m-0 [&_form]:inline-flex", strings.Join(extra, " "))
}
