package legacy

import (
	"strings"

	. "maragu.dev/gomponents"
	. "maragu.dev/gomponents/html"
)

func formField(cfg formFieldConfig) Node {
	inputType := strings.TrimSpace(cfg.Type)
	if inputType == "" {
		inputType = "text"
	}

	inputClass := formControlClass()
	if cfg.Invalid {
		inputClass += " border-[var(--borderColor-danger-emphasis)]"
	}

	labelNodes := []Node{Text(cfg.Label)}
	if cfg.Required {
		labelNodes = append(labelNodes, Span(Class("ml-1 text-[var(--fgColor-danger)]"), Text("*")))
	}

	inputNodes := []Node{
		Type(inputType),
		Class(inputClass),
		Name(cfg.Name),
		Value(cfg.Value),
	}
	if strings.TrimSpace(cfg.Placeholder) != "" {
		inputNodes = append(inputNodes, Placeholder(cfg.Placeholder))
	}
	if cfg.Required {
		inputNodes = append(inputNodes, Required())
	}
	if cfg.Invalid {
		inputNodes = append(inputNodes, Attr("aria-invalid", "true"))
	}

	helpNode := Node(nil)
	if strings.TrimSpace(cfg.HelpText) != "" {
		helpNode = P(Class("m-0 text-xs leading-[var(--text-caption-lineHeight)] text-[var(--fgColor-muted)]"), Text(cfg.HelpText))
	}

	errorNode := Node(nil)
	if strings.TrimSpace(cfg.ErrorMessage) != "" {
		errorNode = P(Class("m-0 text-xs leading-[var(--text-caption-lineHeight)] text-[var(--fgColor-danger)]"), Text(cfg.ErrorMessage))
	}

	return Div(
		Class("flex flex-col gap-1"),
		Label(Class("text-xs font-semibold text-[var(--fgColor-muted)]"), Group(labelNodes)),
		Input(inputNodes...),
		helpNode,
		errorNode,
	)
}

func checkboxOption(id, name, label string, checked bool) Node {
	checkedNode := Node(nil)
	if checked {
		checkedNode = Checked()
	}

	return Label(
		Class("inline-flex items-center gap-2 text-sm font-medium text-[var(--fgColor-default)]"),
		Input(
			Type("checkbox"),
			ID(id),
			Name(name),
			Value(label),
			checkedNode,
			Class("m-0 inline-grid h-[var(--control-minTarget-fine)] w-[var(--control-minTarget-fine)] shrink-0 appearance-none place-content-center rounded-md border border-[var(--borderColor-muted)] bg-[var(--bgColor-default)] transition-colors after:h-[calc(var(--control-minTarget-fine)*0.52)] after:w-[calc(var(--control-minTarget-fine)*0.28)] after:origin-center after:rotate-45 after:scale-0 after:border-b-[3px] after:border-r-[3px] after:border-b-[var(--fgColor-onEmphasis)] after:border-r-[var(--fgColor-onEmphasis)] after:content-[''] checked:border-[var(--control-checked-borderColor-rest)] checked:bg-[var(--control-checked-bgColor-rest)] checked:after:scale-100 hover:border-[var(--control-borderColor-emphasis)] focus-visible:outline focus-visible:outline-2 focus-visible:outline-[var(--focus-outlineColor)] focus-visible:outline-offset-0 disabled:cursor-not-allowed disabled:border-[var(--control-borderColor-disabled)] disabled:bg-[var(--control-bgColor-disabled)]"),
		),
		Span(Text(label)),
	)
}

func radioOption(id, name, label string, checked bool) Node {
	checkedNode := Node(nil)
	if checked {
		checkedNode = Checked()
	}

	return Label(
		Class("inline-flex items-center gap-2 text-sm font-medium text-[var(--fgColor-default)]"),
		Input(
			Type("radio"),
			ID(id),
			Name(name),
			Value(label),
			checkedNode,
			Class("m-0 inline-grid h-[var(--control-small-size)] w-[var(--control-small-size)] shrink-0 appearance-none place-content-center rounded-full border border-[var(--borderColor-muted)] bg-[var(--bgColor-default)] transition-colors after:h-[calc(var(--control-xsmall-size)*0.45)] after:w-[calc(var(--control-xsmall-size)*0.45)] after:scale-0 after:rounded-full after:bg-[var(--fgColor-onEmphasis)] after:transition-transform after:content-[''] checked:border-[var(--control-checked-borderColor-rest)] checked:bg-[var(--bgColor-default)] checked:after:scale-100 checked:after:bg-[var(--control-checked-bgColor-rest)] hover:border-[var(--control-borderColor-emphasis)] focus-visible:outline focus-visible:outline-2 focus-visible:outline-[var(--focus-outlineColor)] focus-visible:outline-offset-0 disabled:cursor-not-allowed disabled:border-[var(--control-borderColor-disabled)] disabled:bg-[var(--control-bgColor-disabled)]"),
		),
		Span(Text(label)),
	)
}

func toggleSwitch(id, name, label string, checked bool) Node {
	checkedNode := Node(nil)
	stateLabel := "Off"
	if checked {
		checkedNode = Checked()
		stateLabel = "On"
	}

	return Label(
		Class("inline-grid grid-cols-[1fr_auto_auto] items-center gap-3 text-sm font-medium text-[var(--fgColor-default)]"),
		Span(Class("text-[var(--fgColor-default)]"), Text(label)),
		Span(Class("min-w-[var(--control-small-size)] text-right text-xs leading-[var(--text-caption-lineHeight)] text-[var(--fgColor-muted)]"), Text(stateLabel)),
		Input(Type("checkbox"), ID(id), Name(name), checkedNode, Class("peer sr-only")),
		Span(Class("relative inline-flex h-[var(--control-small-size)] w-[calc(var(--control-medium-size)+var(--control-small-size))] items-center justify-start rounded-full border border-[var(--controlTrack-borderColor-rest)] bg-[var(--controlTrack-bgColor-rest)] p-[var(--control-xsmall-gap)] transition-colors peer-checked:border-[var(--control-checked-borderColor-rest)] peer-checked:bg-[var(--control-checked-bgColor-rest)] peer-focus-visible:outline peer-focus-visible:outline-2 peer-focus-visible:outline-[var(--focus-outlineColor)] peer-focus-visible:outline-offset-0"),
			Span(Class("h-[var(--control-xsmall-size)] w-[var(--control-xsmall-size)] rounded-full bg-[var(--controlKnob-bgColor-rest)] shadow-[var(--shadow-resting-xsmall)] transition-transform peer-checked:translate-x-[calc(var(--control-medium-size)-var(--control-xsmall-gap))] peer-checked:bg-[var(--bgColor-default)]")),
		),
	)
}
