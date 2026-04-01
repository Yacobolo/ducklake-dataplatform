package core

import (
	"fmt"
	"net/url"
	"sort"
	"strings"

	"duck-demo/internal/domain"

	. "maragu.dev/gomponents"
	data "maragu.dev/gomponents-datastar"
	. "maragu.dev/gomponents/html"
)

type SectionTab struct {
	Label  string
	Href   string
	Active bool
}

type FilterMenuOption struct {
	Label    string
	Value    string
	Icon     string
	Selected bool
}

type FilterMenuGroup struct {
	Name     string
	Label    string
	Expanded bool
	Options  []FilterMenuOption
}

type FilterMenuConfig struct {
	Label             string
	Action            string
	ClearURL          string
	SelectedCount     int
	SelectedCountExpr string
	HiddenFields      map[string]string
	RootAttrs         []Node
	FormAttrs         []Node
	ClearAttrs        []Node
	OptionInputAttrs  []Node
	HideApply         bool
	Groups            []FilterMenuGroup
}

func Card(nodes ...Node) Node {
	return Div(append([]Node{Class(cardClass())}, nodes...)...)
}

func MutedText(nodes ...Node) Node {
	return P(append([]Node{Class(mutedClass())}, nodes...)...)
}

func PrimaryButton(size string, nodes ...Node) Node {
	return Button(append([]Node{Class(primaryButtonClass(size))}, nodes...)...)
}

func SecondaryButton(size string, nodes ...Node) Node {
	return Button(append([]Node{Class(secondaryButtonClass(size))}, nodes...)...)
}

func DangerButton(size string, nodes ...Node) Node {
	return Button(append([]Node{Class(dangerButtonClass(size))}, nodes...)...)
}

func IconButton(size string, nodes ...Node) Node {
	return Button(append([]Node{Class(iconButtonClass(size))}, nodes...)...)
}

func PrimaryLink(href, size string, nodes ...Node) Node {
	base := []Node{Href(href), Class(ClassNames(linkButtonClass(primaryButtonClass(size)), "text-[var(--button-primary-fgColor-rest)] visited:text-[var(--button-primary-fgColor-rest)] hover:text-[var(--button-primary-fgColor-rest)] active:text-[var(--button-primary-fgColor-rest)]"))}
	return A(append(base, nodes...)...)
}

func SecondaryLink(href, size string, nodes ...Node) Node {
	base := []Node{Href(href), Class(ClassNames(linkButtonClass(secondaryButtonClass(size)), "text-[var(--button-default-fgColor-rest)] visited:text-[var(--button-default-fgColor-rest)] hover:text-[var(--button-default-fgColor-rest)] active:text-[var(--button-default-fgColor-rest)]"))}
	return A(append(base, nodes...)...)
}

func DangerLink(href, size string, nodes ...Node) Node {
	base := []Node{Href(href), Class(ClassNames(linkButtonClass(dangerButtonClass(size)), "text-[var(--button-danger-fgColor-rest)] visited:text-[var(--button-danger-fgColor-rest)] hover:text-[var(--button-danger-fgColor-hover)] active:text-[var(--button-danger-fgColor-active)]"))}
	return A(append(base, nodes...)...)
}

func TextLink(href string, nodes ...Node) Node {
	base := []Node{
		Href(href),
		Class("font-medium text-[var(--fgColor-accent)] no-underline visited:text-[var(--fgColor-accent)] hover:text-[var(--fgColor-accent)] hover:underline active:text-[var(--fgColor-accent)]"),
	}
	return A(append(base, nodes...)...)
}

func FieldLabel(text string) Node {
	return Label(Class("mb-1 block text-xs font-semibold text-[var(--fgColor-muted)]"), Text(text))
}

func InputControl(extraClass string, nodes ...Node) Node {
	base := []Node{Class(formControlClass(extraClass))}
	return Input(append(base, nodes...)...)
}

func InputWithIcon(iconName, extraClass string, inputNodes ...Node) Node {
	inputAttrs := append([]Node{
		Class("block min-w-0 flex-1 border-0 bg-transparent px-0 py-2 text-sm text-[var(--fgColor-default)] placeholder:text-[var(--fgColor-muted)] focus-visible:outline-none"),
	}, inputNodes...)
	return Div(
		Class(ClassNames("flex w-full items-center gap-2 rounded-lg border border-[var(--borderColor-default)] bg-[var(--bgColor-default)] px-3 shadow-xs transition-colors focus-within:border-[var(--borderColor-accent-emphasis)] focus-within:outline-none focus-within:ring-2 focus-within:ring-[var(--focus-outlineColor)]", extraClass)),
		Span(
			Class("pointer-events-none inline-flex h-4 w-4 shrink-0 items-center justify-center text-[var(--fgColor-muted)]"),
			Icon(iconName, Class("h-4 w-4")),
		),
		Input(inputAttrs...),
	)
}

func SearchInput(label, placeholder, extraClass string, inputNodes ...Node) Node {
	labelText := strings.TrimSpace(label)
	if labelText == "" {
		labelText = "Search"
	}

	placeholderText := strings.TrimSpace(placeholder)
	inputAttrs := []Node{
		Type("search"),
		Placeholder(placeholderText),
		Attr("aria-label", labelText),
	}
	inputAttrs = append(inputAttrs, inputNodes...)

	return Group([]Node{
		Label(Class("sr-only"), Text(labelText)),
		InputWithIcon("search", extraClass, inputAttrs...),
	})
}

func TextareaControl(extraClass string, nodes ...Node) Node {
	base := []Node{Class(formControlClass(extraClass))}
	return Textarea(append(base, nodes...)...)
}

func SelectControl(extraClass string, nodes ...Node) Node {
	base := []Node{Class(formControlClass(extraClass))}
	return Select(append(base, nodes...)...)
}

func ResourceKindLabel(kind string) string {
	switch kind {
	case "runtime-asset":
		return "Runtime Asset"
	case "semantic-model":
		return "Semantic Model"
	case "compute-endpoint":
		return "Compute Endpoint"
	default:
		return TitleizeWords(kind)
	}
}

func ResourceKindIcon(kind string) string {
	switch kind {
	case "notebook":
		return "notebook-text"
	case "dashboard":
		return "chart-column"
	case "product":
		return "package-open"
	case "runtime-asset":
		return "git-fork"
	case "model":
		return "boxes"
	case "semantic-model":
		return "waypoints"
	case "pipeline":
		return "workflow"
	case "compute-endpoint":
		return "server"
	default:
		return "file-stack"
	}
}

func ResourceKindIconWrapClass(kind string) string {
	base := "inline-flex h-9 w-9 shrink-0 items-center justify-center rounded-lg"
	switch kind {
	case "notebook":
		return base + " bg-[var(--display-blue-scale-0)] text-[var(--display-blue-scale-6)]"
	case "dashboard":
		return base + " bg-[var(--display-plum-scale-0)] text-[var(--display-plum-scale-6)]"
	case "product":
		return base + " bg-[var(--display-orange-scale-0)] text-[var(--display-orange-scale-6)]"
	case "runtime-asset":
		return base + " bg-[var(--display-gray-scale-0)] text-[var(--display-gray-scale-7)]"
	case "model":
		return base + " bg-[var(--display-green-scale-0)] text-[var(--display-green-scale-6)]"
	case "semantic-model":
		return base + " bg-[var(--display-indigo-scale-0)] text-[var(--display-indigo-scale-6)]"
	case "pipeline":
		return base + " bg-[var(--display-teal-scale-0)] text-[var(--display-teal-scale-6)]"
	case "compute-endpoint":
		return base + " bg-[var(--display-red-scale-0)] text-[var(--display-red-scale-6)]"
	default:
		return base + " bg-[var(--bgColor-muted)] text-[var(--fgColor-muted)]"
	}
}

func ResourceIcon(kind string) Node {
	return Span(
		Class(ResourceKindIconWrapClass(kind)),
		Icon(
			ResourceKindIcon(kind),
			Class("h-5 w-5 shrink-0"),
			Attr("style", "stroke-width:1.75"),
		),
	)
}

func Badge(text, tone string) Node {
	return Span(Class(labelClass(tone)), Text(text))
}

func ActionMenu(label string, items ...Node) Node {
	summaryClass := "list-none [&::-webkit-details-marker]:hidden inline-flex min-h-10 items-center justify-center rounded-lg border border-[var(--borderColor-default)] bg-[var(--bgColor-default)] px-3 text-sm font-medium text-[var(--fgColor-default)] shadow-xs hover:bg-[var(--bgColor-muted)]"
	summaryContent := Node(Text(label))
	if label == "More" || label == "Actions" {
		summaryClass = "ui-icon-action-trigger list-none [&::-webkit-details-marker]:hidden inline-flex h-8 w-8 items-center justify-center rounded-md p-1 transition-colors duration-150 ease-out focus-visible:outline-2 focus-visible:outline-offset-0 focus-visible:outline-[var(--borderColor-accent-emphasis)]"
		summaryContent = Group([]Node{
			Icon("ellipsis-vertical", Class("h-[18px] w-[18px]")),
			Span(Class("sr-only"), Text(label)),
		})
	}
	return Details(
		Class(ClassNames("dropdown", DetailsClass())),
		Summary(Class(summaryClass), Title(label), Attr("aria-label", label), summaryContent),
		Div(Class(DropdownMenuClass()), Group(items)),
	)
}

func ActionMenuLink(href, label string) Node {
	return A(Href(href), Class(DropdownItemClass("text-[var(--fgColor-default)]")), Span(Text(label)))
}

func ActionMenuPost(action, label string, csrfField func() Node, danger bool) Node {
	btnClass := DropdownItemClass()
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

func FilterMenu(cfg FilterMenuConfig) Node {
	label := cfg.Label
	if label == "" {
		label = "Filter"
	}

	countNode := Node(nil)
	if cfg.SelectedCountExpr != "" {
		countNode = Span(
			data.Show(cfg.SelectedCountExpr+" > 0"),
			Class("inline-flex min-w-5 items-center justify-center rounded-full bg-[var(--bgColor-accent-muted)] px-1.5 py-0.5 text-[11px] font-semibold text-[var(--fgColor-accent)]"),
			data.Text(cfg.SelectedCountExpr),
		)
	} else if cfg.SelectedCount > 0 {
		countNode = Span(
			Class("inline-flex min-w-5 items-center justify-center rounded-full bg-[var(--bgColor-accent-muted)] px-1.5 py-0.5 text-[11px] font-semibold text-[var(--fgColor-accent)]"),
			Text(fmt.Sprintf("%d", cfg.SelectedCount)),
		)
	}

	hiddenKeys := make([]string, 0, len(cfg.HiddenFields))
	for key := range cfg.HiddenFields {
		hiddenKeys = append(hiddenKeys, key)
	}
	sort.Strings(hiddenKeys)
	hiddenNodes := make([]Node, 0, len(hiddenKeys))
	for _, key := range hiddenKeys {
		value := cfg.HiddenFields[key]
		if value == "" {
			continue
		}
		hiddenNodes = append(hiddenNodes, Input(Type("hidden"), Name(key), Value(value)))
	}

	groupNodes := make([]Node, 0, len(cfg.Groups))
	for _, group := range cfg.Groups {
		options := make([]Node, 0, len(group.Options))
		for _, option := range group.Options {
			checkedNode := Node(nil)
			if option.Selected {
				checkedNode = Checked()
			}
			iconNode := Node(nil)
			if option.Icon != "" {
				iconNode = Span(
					Class("inline-flex h-5 w-5 items-center justify-center text-[var(--fgColor-muted)]"),
					Icon(option.Icon, Class("h-4 w-4")),
				)
			}
			inputAttrs := append([]Node{
				Type("checkbox"),
				Name(group.Name),
				Value(option.Value),
				checkedNode,
				Class("m-0 inline-grid h-4 w-4 shrink-0 appearance-none place-content-center rounded border border-[var(--borderColor-muted)] bg-[var(--bgColor-default)] transition-colors after:h-2 after:w-1 after:origin-center after:rotate-45 after:scale-0 after:border-b-2 after:border-r-2 after:border-b-[var(--button-primary-fgColor-rest)] after:border-r-[var(--button-primary-fgColor-rest)] after:content-[''] checked:border-[var(--control-checked-borderColor-rest)] checked:bg-[var(--control-checked-bgColor-rest)] checked:after:scale-100"),
			}, cfg.OptionInputAttrs...)
			options = append(options, Label(
				Class("flex cursor-pointer items-center gap-3 px-2 py-2 text-sm text-[var(--fgColor-default)] hover:bg-[var(--bgColor-muted)]"),
				Input(inputAttrs...),
				iconNode,
				Span(Class("truncate"), Text(option.Label)),
			))
		}
		groupNodes = append(groupNodes, Details(
			Class("border-b border-[var(--borderColor-default)] last:border-b-0"),
			OpenIf(group.Expanded),
			Summary(
				Class("list-none [&::-webkit-details-marker]:hidden flex cursor-pointer items-center justify-between px-2 py-2 text-sm font-medium text-[var(--fgColor-default)]"),
				Span(Text(group.Label)),
				Icon("chevron-down", Class("h-4 w-4 text-[var(--fgColor-muted)]")),
			),
			Div(Class("grid pb-1"), Group(options)),
		))
	}

	clearNode := Node(nil)
	if cfg.ClearURL != "" {
		clearAttrs := append([]Node{
			Href(cfg.ClearURL),
			Class("flex items-center gap-2 border-b border-[var(--borderColor-default)] px-2 py-2 text-sm font-medium text-[var(--fgColor-muted)] no-underline hover:bg-[var(--bgColor-muted)] hover:text-[var(--fgColor-default)]"),
		}, cfg.ClearAttrs...)
		clearAttrs = append(clearAttrs,
			Icon("filter-x", Class("h-4 w-4")),
			Span(Text("Clear all")),
		)
		clearNode = A(clearAttrs...)
	}

	formAttrs := append([]Node{
		Method("get"),
		Action(cfg.Action),
		Class("grid"),
	}, cfg.FormAttrs...)
	formAttrs = append(formAttrs,
		Group(hiddenNodes),
		clearNode,
		Group(groupNodes),
	)
	if !cfg.HideApply {
		formAttrs = append(formAttrs,
			Div(
				Class("flex items-center justify-end gap-2 border-t border-[var(--borderColor-default)] p-2"),
				PrimaryButton("small", Type("submit"), Text("Apply")),
			),
		)
	}

	rootAttrs := append([]Node{
		Class(DetailsClass()),
	}, cfg.RootAttrs...)
	rootAttrs = append(rootAttrs,
		Summary(
			Class(DetailsSummaryClass("inline-flex min-h-10 items-center justify-center gap-2 rounded-lg border border-[var(--button-default-borderColor-rest)] bg-[var(--button-default-bgColor-rest)] px-3 py-2 text-sm font-medium text-[var(--button-default-fgColor-rest)] shadow-xs hover:border-[var(--button-default-borderColor-hover)] hover:bg-[var(--button-default-bgColor-hover)]")),
			Title(label),
			Attr("aria-label", label),
			Icon("filter", Class("h-4 w-4")),
			Span(Text(label)),
			countNode,
			Icon("chevron-down", Class("h-4 w-4")),
		),
		Div(
			Class(DropdownMenuClass("w-[15rem] min-w-[15rem] max-w-[15rem] p-0")),
			Form(formAttrs...),
		),
	)
	return Details(rootAttrs...)
}

func OpenIf(open bool) Node {
	if !open {
		return nil
	}
	return Attr("open", "")
}

func ButtonGroup(extraClass string, nodes ...Node) Node {
	base := []Node{Class(buttonRowClass(extraClass))}
	return Div(append(base, nodes...)...)
}

func EmptyState(iconName, title, message string, action Node) Node {
	icon := Node(nil)
	if iconName != "" {
		icon = Div(
			Class("flex h-10 w-10 items-center justify-center rounded-xl border border-[var(--borderColor-default)] bg-[var(--bgColor-muted)] text-[var(--fgColor-accent)]"),
			Icon(iconName, Class(NavIconClass())),
		)
	}
	return Div(
		Class("grid gap-4 rounded-2xl border border-dashed border-[var(--borderColor-default)] bg-[var(--bgColor-default)] p-5"),
		Div(
			Class("flex items-start gap-3"),
			icon,
			Div(
				Class("flex min-w-0 flex-1 flex-col gap-2"),
				P(Class("m-0 text-lg font-semibold"), Text(title)),
				P(Class("m-0 text-sm leading-6 text-[var(--fgColor-muted)]"), Text(message)),
				action,
			),
		),
	)
}

func WorkspaceEmptyState(iconName, title, message string, action Node) Node {
	icon := Node(nil)
	if iconName != "" {
		icon = Div(
			Class("flex h-10 w-10 items-center justify-center rounded-xl bg-[var(--bgColor-muted)] text-[var(--fgColor-accent)]"),
			Icon(iconName, Class(NavIconClass())),
		)
	}
	return Div(
		Class("grid min-h-[12rem] place-items-center"),
		Div(
			Class("flex max-w-xl items-start gap-4"),
			icon,
			Div(
				Class("grid gap-2"),
				P(Class("m-0 text-lg font-semibold text-[var(--fgColor-default)]"), Text(title)),
				P(Class("m-0 text-sm leading-6 text-[var(--fgColor-muted)]"), Text(message)),
				action,
			),
		),
	)
}

func MetricCard(label, value, hint string) Node {
	hintNode := Node(nil)
	if hint != "" {
		hintNode = P(Class("m-0 text-xs leading-5 text-[var(--fgColor-muted)]"), Text(hint))
	}
	return Div(
		Class("grid gap-2 rounded-xl border border-[var(--borderColor-default)] bg-[var(--bgColor-default)] p-4"),
		P(Class("m-0 text-[11px] font-semibold uppercase tracking-[0.08em] text-[var(--fgColor-muted)]"), Text(label)),
		Div(
			Class("flex items-end justify-between gap-3"),
			P(Class("m-0 text-3xl font-semibold leading-none text-[var(--fgColor-default)]"), Text(value)),
			hintNode,
		),
	)
}

func MetaItem(label, value string) Node {
	return Div(
		Class("grid gap-1 border-b border-[var(--borderColor-default)] pb-2 last:border-b-0 last:pb-0"),
		Span(Class("text-[11px] font-semibold uppercase tracking-[0.04em] text-[var(--fgColor-muted)]"), Text(label)),
		Span(Class("text-sm text-[var(--fgColor-default)]"), Text(value)),
	)
}

func Checkbox(id, name, value, label string, checked bool) Node {
	checkedNode := Node(nil)
	if checked {
		checkedNode = Checked()
	}
	return Label(
		Class("inline-flex items-center gap-3 text-sm font-medium text-[var(--fgColor-default)]"),
		Input(
			Type("checkbox"),
			ID(id),
			Name(name),
			Value(value),
			checkedNode,
			Class("m-0 inline-grid h-5 w-5 shrink-0 appearance-none place-content-center rounded-md border border-[var(--borderColor-muted)] bg-[var(--bgColor-default)] transition-colors after:h-2.5 after:w-1.5 after:origin-center after:rotate-45 after:scale-0 after:border-b-[3px] after:border-r-[3px] after:border-b-[var(--button-primary-fgColor-rest)] after:border-r-[var(--button-primary-fgColor-rest)] after:content-[''] checked:border-[var(--control-checked-borderColor-rest)] checked:bg-[var(--control-checked-bgColor-rest)] checked:after:scale-100 hover:border-[var(--control-borderColor-emphasis)] focus-visible:outline focus-visible:outline-2 focus-visible:outline-[var(--focus-outlineColor)] focus-visible:outline-offset-0 disabled:cursor-not-allowed disabled:border-[var(--control-borderColor-disabled)] disabled:bg-[var(--control-bgColor-disabled)]"),
		),
		Span(Text(label)),
	)
}

func Radio(id, name, value, label string, checked bool) Node {
	checkedNode := Node(nil)
	if checked {
		checkedNode = Checked()
	}
	return Label(
		Class("inline-flex items-center gap-3 text-sm font-medium text-[var(--fgColor-default)]"),
		Input(
			Type("radio"),
			ID(id),
			Name(name),
			Value(value),
			checkedNode,
			Class("m-0 inline-grid h-4 w-4 shrink-0 appearance-none place-content-center rounded-full border border-[var(--borderColor-muted)] bg-[var(--bgColor-default)] transition-colors after:h-2 after:w-2 after:scale-0 after:rounded-full after:bg-[var(--button-primary-fgColor-rest)] after:transition-transform after:content-[''] checked:border-[var(--control-checked-borderColor-rest)] checked:bg-[var(--bgColor-default)] checked:after:scale-100 checked:after:bg-[var(--control-checked-bgColor-rest)] hover:border-[var(--control-borderColor-emphasis)] focus-visible:outline focus-visible:outline-2 focus-visible:outline-[var(--focus-outlineColor)] focus-visible:outline-offset-0 disabled:cursor-not-allowed disabled:border-[var(--control-borderColor-disabled)] disabled:bg-[var(--control-bgColor-disabled)]"),
		),
		Span(Text(label)),
	)
}

func Toggle(id, name, label string, checked bool) Node {
	checkedNode := Node(nil)
	stateLabel := "Off"
	if checked {
		checkedNode = Checked()
		stateLabel = "On"
	}
	return Label(
		Class("inline-grid grid-cols-[1fr_auto_auto] items-center gap-3 text-sm font-medium text-[var(--fgColor-default)]"),
		Span(Class("text-[var(--fgColor-default)]"), Text(label)),
		Span(Class("min-w-8 text-right text-xs leading-4 text-[var(--fgColor-muted)]"), Text(stateLabel)),
		Input(Type("checkbox"), ID(id), Name(name), checkedNode, Class("peer sr-only")),
		Span(
			Class("relative inline-flex h-5 w-10 items-center justify-start rounded-full border border-[var(--controlTrack-borderColor-rest)] bg-[var(--controlTrack-bgColor-rest)] p-0.5 transition-colors peer-checked:border-[var(--control-checked-borderColor-rest)] peer-checked:bg-[var(--control-checked-bgColor-rest)] peer-focus-visible:outline peer-focus-visible:outline-2 peer-focus-visible:outline-[var(--focus-outlineColor)] peer-focus-visible:outline-offset-0"),
			Span(Class("h-4 w-4 rounded-full bg-[var(--controlKnob-bgColor-rest)] shadow-xs transition-transform peer-checked:translate-x-5 peer-checked:bg-[var(--bgColor-default)]")),
		),
	)
}

func FactList(items [][2]string) Node {
	rows := make([]Node, 0, len(items))
	for i := range items {
		rows = append(rows, Div(
			Class("flex items-start justify-between gap-3 rounded-lg border border-[var(--borderColor-default)] bg-[var(--bgColor-default)] px-3 py-2"),
			Span(Class("text-xs font-semibold uppercase tracking-[0.04em] text-[var(--fgColor-muted)]"), Text(items[i][0])),
			Span(Class("text-sm text-right text-[var(--fgColor-default)]"), Text(items[i][1])),
		))
	}
	return Div(Class("grid gap-2"), Group(rows))
}

func SubtleLink(href string, nodes ...Node) Node {
	base := []Node{
		Href(href),
		Class("text-[var(--fgColor-muted)] no-underline hover:text-[var(--fgColor-default)]"),
	}
	return A(append(base, nodes...)...)
}

func DetailShell(nodes ...Node) Node {
	return Div(append([]Node{Class("flex flex-col gap-4")}, nodes...)...)
}

func DetailHero(nodes ...Node) Node {
	return Div(append([]Node{Class("grid gap-4 rounded-2xl border border-[var(--borderColor-default)] bg-[linear-gradient(135deg,var(--bgColor-muted)_0%,var(--bgColor-default)_65%)] p-5 shadow-sm lg:grid-cols-[minmax(0,1.5fr)_minmax(16rem,0.8fr)]")}, nodes...)...)
}

func DetailHeroCopy(nodes ...Node) Node {
	return Div(append([]Node{Class("flex min-w-0 flex-col gap-3")}, nodes...)...)
}

func DetailHeroMeta(nodes ...Node) Node {
	return Div(append([]Node{Class("grid gap-2 rounded-xl border border-[var(--borderColor-default)] bg-[var(--bgColor-default)] p-4")}, nodes...)...)
}

func Kicker(text string) Node {
	return P(Class("m-0 text-xs font-bold uppercase tracking-[0.14em] text-[var(--fgColor-accent)]"), Text(text))
}

func DetailTitleRow(nodes ...Node) Node {
	return Div(append([]Node{Class("flex flex-wrap items-center gap-3")}, nodes...)...)
}

func DetailTitle(text string) Node {
	return H2(Class("m-0 text-3xl font-semibold leading-tight text-[var(--fgColor-default)]"), Text(text))
}

func DetailDescription(text string) Node {
	return P(Class("m-0 max-w-3xl text-sm leading-6 text-[var(--fgColor-muted)]"), Text(text))
}

func PageHeader(kicker, title, description string, actions ...Node) Node {
	kickerNode := Node(nil)
	if kicker != "" {
		kickerNode = Kicker(kicker)
	}
	descriptionNode := Node(nil)
	if description != "" {
		descriptionNode = DetailDescription(description)
	}
	actionGroup := Node(nil)
	if len(actions) > 0 {
		actionGroup = Div(Class("flex flex-wrap items-center justify-end gap-3 [&>a]:shrink-0 [&>form]:m-0 [&>form]:inline-flex"), Group(actions))
	}
	return Div(
		Class("grid gap-4 border-b border-[var(--borderColor-default)] pb-6"),
		kickerNode,
		Div(Class("flex flex-col gap-4 md:flex-row md:items-center md:justify-between"),
			Div(Class("flex min-w-0 max-w-3xl flex-col gap-1.5"),
				H1(Class("m-0 text-3xl font-extrabold tracking-tight text-[var(--fgColor-default)]"), Text(title)),
				descriptionNode,
			),
			actionGroup,
		),
	)
}

func SectionSurface(nodes ...Node) Node {
	return Div(append([]Node{Class("grid gap-4 rounded-2xl border border-[var(--borderColor-default)] bg-[var(--bgColor-default)] p-5 max-md:p-4")}, nodes...)...)
}

func ListPageLayout(nodes ...Node) Node {
	return Div(append([]Node{Class("grid gap-6")}, nodes...)...)
}

func ListPageHeader(title, description string, actions ...Node) Node {
	return PageHeader("", title, description, actions...)
}

func ListPageBody(nodes ...Node) Node {
	return Div(append([]Node{Class("grid gap-0")}, nodes...)...)
}

func ListPageFooter(summary string) Node {
	return P(Class("m-0 text-sm text-[var(--fgColor-muted)]"), Text(summary))
}

func ListPagination(basePath string, page domain.PageRequest, total int64) Node {
	offset := page.Offset()
	limit := page.Limit()
	shown := limit
	if remaining := int(total) - offset; remaining < shown {
		shown = remaining
	}
	if shown < 0 {
		shown = 0
	}
	summary := Span(
		Class("text-sm text-[var(--fgColor-muted)]"),
		Text("Showing "),
		Span(Class("font-semibold text-[var(--fgColor-default)]"), Text(fmt.Sprintf("%d", shown))),
		Text(" of "),
		Span(Class("font-semibold text-[var(--fgColor-default)]"), Text(fmt.Sprintf("%d", total))),
		Text(" entries."),
	)
	prevOffset := offset - limit
	if prevOffset < 0 {
		prevOffset = 0
	}
	prevToken := domain.EncodePageToken(prevOffset)
	nextToken := domain.NextPageToken(offset, limit, total)

	prevNode := Node(Span(Class("inline-flex min-h-10 items-center justify-center rounded-l-lg border border-[var(--borderColor-default)] bg-[var(--bgColor-default)] px-3 text-sm font-medium text-[var(--fgColor-default)] opacity-60 pointer-events-none"), Attr("aria-disabled", "true"), Text("Previous")))
	if offset > 0 {
		prevNode = A(
			Href(listPageURL(basePath, limit, prevToken)),
			Class("inline-flex min-h-10 items-center justify-center rounded-l-lg border border-[var(--button-default-borderColor-rest)] border-r-0 bg-[var(--button-default-bgColor-rest)] px-3 text-sm font-medium text-[var(--button-default-fgColor-rest)] no-underline transition-colors duration-100 ease-out hover:border-[var(--button-default-borderColor-hover)] hover:bg-[var(--button-default-bgColor-hover)] hover:text-[var(--button-default-fgColor-rest)]"),
			Text("Previous"),
		)
	}

	nextNode := Node(Span(Class("inline-flex min-h-10 items-center justify-center rounded-r-lg border border-[var(--borderColor-default)] bg-[var(--bgColor-default)] px-3 text-sm font-medium text-[var(--fgColor-default)] opacity-60 pointer-events-none"), Attr("aria-disabled", "true"), Text("Next")))
	if nextToken != "" {
		nextNode = A(
			Href(listPageURL(basePath, limit, nextToken)),
			Class("inline-flex min-h-10 items-center justify-center rounded-r-lg border border-[var(--button-default-borderColor-rest)] bg-[var(--button-default-bgColor-rest)] px-3 text-sm font-medium text-[var(--button-default-fgColor-rest)] no-underline transition-colors duration-100 ease-out hover:border-[var(--button-default-borderColor-hover)] hover:bg-[var(--button-default-bgColor-hover)] hover:text-[var(--button-default-fgColor-rest)]"),
			Text("Next"),
		)
	}

	return Div(
		Class("flex items-center justify-between gap-4 border-t border-[var(--borderColor-default)] bg-[var(--bgColor-default)] px-6 py-4 max-sm:flex-col max-sm:items-start max-sm:px-4"),
		summary,
		Nav(Attr("aria-label", "Pagination"),
			Div(Class("inline-flex items-center"),
				prevNode,
				nextNode,
			),
		),
	)
}

func listPageURL(basePath string, limit int, token string) string {
	q := url.Values{}
	q.Set("max_results", fmt.Sprintf("%d", limit))
	if token != "" {
		q.Set("page_token", token)
	}
	return basePath + "?" + q.Encode()
}

func FormPageLayout(kicker, title, description string, nodes ...Node) Node {
	parts := []Node{PageHeader(kicker, title, description)}
	if len(nodes) > 0 {
		parts = append(parts, SectionSurface(nodes...))
	}
	return ListPageLayout(parts...)
}

func ResultPageLayout(kicker, title, description string, nodes ...Node) Node {
	parts := []Node{PageHeader(kicker, title, description)}
	parts = append(parts, nodes...)
	return ListPageLayout(parts...)
}

func SectionHeader(title, description string, actions ...Node) Node {
	descriptionNode := Node(nil)
	if description != "" {
		descriptionNode = P(Class("m-0 text-sm leading-6 text-[var(--fgColor-muted)]"), Text(description))
	}
	actionGroup := Node(nil)
	if len(actions) > 0 {
		actionGroup = Div(Class("flex flex-wrap items-center gap-3"), Group(actions))
	}
	return Div(
		Class("flex flex-wrap items-start justify-between gap-4"),
		Div(Class("flex min-w-0 max-w-3xl flex-col gap-1"),
			H2(Class("m-0 text-xl font-semibold text-[var(--fgColor-default)]"), Text(title)),
			descriptionNode,
		),
		actionGroup,
	)
}

func SectionTabs(tabs []SectionTab) Node {
	nodes := make([]Node, 0, len(tabs))
	for i := range tabs {
		tab := tabs[i]
		className := "inline-flex min-h-10 items-center border-b-2 border-transparent px-1 text-sm font-medium text-[var(--fgColor-muted)] no-underline transition-colors hover:text-[var(--fgColor-default)]"
		current := Node(nil)
		if tab.Active {
			className = "inline-flex min-h-10 items-center border-b-2 border-[var(--borderColor-accent-emphasis)] px-1 text-sm font-semibold text-[var(--fgColor-default)] no-underline"
			current = Attr("aria-current", "page")
		}
		nodes = append(nodes, A(Href(tab.Href), Class(className), current, Text(tab.Label)))
	}
	return Nav(
		Class("flex flex-wrap items-center gap-5 border-b border-[var(--borderColor-default)]"),
		Attr("aria-label", "Section navigation"),
		Group(nodes),
	)
}

func DetailSummaryList(items [][2]string) Node {
	rows := make([]Node, 0, len(items))
	for i := range items {
		rows = append(rows, Div(
			Class("grid gap-1 border-b border-[var(--borderColor-default)] pb-3 last:border-b-0 last:pb-0"),
			Span(Class("text-[11px] font-semibold uppercase tracking-[0.04em] text-[var(--fgColor-muted)]"), Text(items[i][0])),
			Span(Class("text-sm text-[var(--fgColor-default)]"), Text(items[i][1])),
		))
	}
	return Div(Class("grid gap-3"), Group(rows))
}

func BadgeRow(nodes ...Node) Node {
	return Div(append([]Node{Class("flex flex-wrap items-center gap-2")}, nodes...)...)
}

func MetricsGrid(nodes ...Node) Node {
	return Div(append([]Node{Class("grid gap-3 sm:grid-cols-2 xl:grid-cols-4")}, nodes...)...)
}

func DetailLayout(nodes ...Node) Node {
	return Div(append([]Node{Class("grid gap-4 xl:grid-cols-[minmax(0,1.7fr)_minmax(18rem,0.8fr)]")}, nodes...)...)
}

func DetailMain(nodes ...Node) Node {
	return Div(append([]Node{Class("flex min-w-0 flex-col gap-4")}, nodes...)...)
}

func DetailRail(nodes ...Node) Node {
	return Div(append([]Node{Class("flex min-w-0 flex-col gap-4")}, nodes...)...)
}

func DetailRailCard(title, description string, nodes ...Node) Node {
	parts := make([]Node, 0, len(nodes)+1)
	if title != "" || description != "" {
		parts = append(parts, SectionHeader(title, description))
	}
	parts = append(parts, nodes...)
	return SectionSurface(parts...)
}

func MetadataSummary(items [][2]string) Node {
	cards := make([]Node, 0, len(items))
	for i := range items {
		cards = append(cards, Div(
			Class("grid gap-1 rounded-xl border border-[var(--borderColor-default)] bg-[var(--bgColor-muted)] px-3 py-3"),
			Span(Class("text-[11px] font-semibold uppercase tracking-[0.04em] text-[var(--fgColor-muted)]"), Text(items[i][0])),
			Span(Class("text-sm text-[var(--fgColor-default)]"), Text(items[i][1])),
		))
	}
	return Div(Class("grid gap-3 sm:grid-cols-2 xl:grid-cols-3"), Group(cards))
}

func KeyValueGrid(items [][2]string) Node {
	rows := make([]Node, 0, len(items))
	for i := range items {
		rows = append(rows, Div(
			Class("grid gap-1 border-b border-[var(--borderColor-default)] pb-3 last:border-b-0 last:pb-0"),
			Span(Class("text-[11px] font-semibold uppercase tracking-[0.04em] text-[var(--fgColor-muted)]"), Text(items[i][0])),
			Span(Class("text-sm text-[var(--fgColor-default)]"), Text(items[i][1])),
		))
	}
	return Div(Class("grid gap-3"), Group(rows))
}

func ItemList(extraClass string, nodes ...Node) Node {
	className := "grid gap-2"
	if extraClass != "" {
		className += " " + extraClass
	}
	return Ul(append([]Node{Class(className)}, nodes...)...)
}

func ItemListEntry(nodes ...Node) Node {
	return Li(append([]Node{Class("rounded-lg border border-[var(--borderColor-default)] bg-[var(--bgColor-default)] px-3 py-2 text-sm")}, nodes...)...)
}
