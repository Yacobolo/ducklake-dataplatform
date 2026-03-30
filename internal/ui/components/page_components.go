package components

import (
	"duck-demo/internal/domain"
	"duck-demo/internal/ui/core"

	. "maragu.dev/gomponents"
	. "maragu.dev/gomponents/html"
)

func componentsPage(principal domain.ContextPrincipal) Node {
	buttonShowcase := Div(
		Class("flex flex-wrap items-center gap-3"),
		core.SecondaryButton("", Type("button"), Text("Default")),
		core.PrimaryButton("", Type("button"), Text("Primary")),
		core.DangerButton("", Type("button"), Text("Danger")),
		core.SecondaryLink("/ui/components", "", Text("Secondary link")),
		core.PrimaryLink("/ui/components", "", Text("Primary link")),
		core.DangerLink("/ui/components", "", Text("Danger link")),
		core.SecondaryButton("small", Type("button"), Text("Small")),
		core.IconButton("small", Type("button"), Attr("aria-label", "Settings"), core.Icon("settings", Class(core.IconGlyphClass()))),
	)

	buttonGroupShowcase := Div(
		Class("flex flex-wrap items-center gap-3"),
		Div(
			Class("mt-1 flex flex-wrap items-center gap-2 [&_form]:m-0 [&_form]:inline-flex"),
			core.SecondaryButton("small", Type("button"), Text("Run")),
			core.SecondaryButton("small", Type("button"), Text("Explain")),
			core.SecondaryButton("small", Type("button"), Text("History")),
		),
		Div(
			Class("mt-1 flex flex-wrap items-center gap-2 [&_form]:m-0 [&_form]:inline-flex"),
			core.IconButton("small", Type("button"), Attr("aria-label", "Refresh"), core.Icon("refresh-cw", Class(core.IconGlyphClass()))),
			core.IconButton("small", Type("button"), Attr("aria-label", "Download"), core.Icon("download", Class(core.IconGlyphClass()))),
			core.IconButton("small", Type("button"), Attr("aria-label", "Share"), core.Icon("share-2", Class(core.IconGlyphClass()))),
		),
	)

	labelShowcase := Div(
		Class("flex flex-wrap items-center gap-3"),
		core.Badge("Default", ""),
		core.Badge("Accent", "accent"),
		core.Badge("Success", "success"),
		core.Badge("Attention", "attention"),
		core.Badge("Severe", "severe"),
	)

	breadcrumbShowcase := breadcrumbs([]breadcrumbItem{
		{Label: "Catalogs", Href: "/ui/catalogs"},
		{Label: "analytics", Href: "/ui/catalogs/analytics"},
		{Label: "core", Href: "/ui/catalogs/analytics/schemas/core", Active: true},
	})

	avatarShowcase := Div(
		Class("flex flex-wrap items-center gap-3"),
		avatar(avatarConfig{Label: "Data Platform", Tone: "accent", Size: "small"}),
		avatar(avatarConfig{Label: "Analytics Team", Tone: "success", Size: "medium"}),
		avatar(avatarConfig{Label: "Operations", Tone: "attention", Size: "large"}),
	)

	formShowcase := Div(
		Class("flex flex-col gap-3"),
		Label(Text("Search assets")),
		core.InputWithIcon("search", "max-w-md", Type("search"), Placeholder("Search assets")),
		Label(Text("Notebook Git path")),
		core.InputWithIcon("git-branch", "max-w-md", Type("text"), Placeholder("analytics/notebooks/revenue_daily.ipynb")),
		Label(Text("Catalog")),
		core.SelectControl("", Option(Text("analytics")), Option(Text("warehouse"))),
		Label(Text("Description")),
		core.TextareaControl("", Placeholder("Add details for teammates")),
		Div(Class("mt-1 flex flex-wrap items-center gap-2 [&_form]:m-0 [&_form]:inline-flex"), core.PrimaryButton("", Type("button"), Text("Save")), core.SecondaryButton("", Type("button"), Text("Cancel"))),
	)

	twoColumnFormShowcase := Div(
		Class("grid gap-4 md:grid-cols-2"),
		Div(
			Class("flex flex-col gap-3"),
			Label(Text("Model name")),
			core.InputControl("", Type("text"), Value("revenue_daily")),
		),
		Div(
			Class("flex flex-col gap-3"),
			Label(Text("Owner")),
			core.InputControl("", Type("text"), Value("data-platform")),
		),
	)

	selectionControlShowcase := Div(
		Class("grid gap-4 md:grid-cols-2"),
		Div(
			Class("flex flex-col gap-3"),
			P(Class("text-xs text-[var(--fgColor-muted)]"), Text("Checkbox Group")),
			core.Checkbox("feature-audit", "feature", "Enable audit logs", "Enable audit logs", true),
			core.Checkbox("feature-masking", "feature", "Enable column masking", "Enable column masking", true),
			core.Checkbox("feature-rls", "feature", "Enable row-level security", "Enable row-level security", false),
		),
		Div(
			Class("flex flex-col gap-3"),
			P(Class("text-xs text-[var(--fgColor-muted)]"), Text("Radio + Toggle")),
			core.Radio("run-manual", "run-mode", "Manual run", "Manual run", true),
			core.Radio("run-scheduled", "run-mode", "Scheduled run", "Scheduled run", false),
			core.Toggle("notifications", "notifications", "Notify on failed runs", true),
		),
	)

	actionBarShowcase := actionBar()

	toolbarShowcase := Div(
		Class("flex flex-col gap-3"),
		pageToolbar("/ui/components", "New component"),
		quickFilterCardWithValue("Filter by name, type, or owner", "asset"),
	)

	tableShowcase := core.TableContainer("",
		core.DataTable("",
			THead(
				Tr(
					Th(Text("Name")),
					Th(Text("Type")),
					Th(Text("Status")),
				),
			),
			TBody(
				Tr(Td(Text("orders.daily")), Td(core.Badge("table", "accent")), Td(core.Badge("healthy", "success"))),
				Tr(Td(Text("finance.revenue")), Td(core.Badge("view", "attention")), Td(core.Badge("warning", "attention"))),
				Tr(Td(Text("billing.settlements")), Td(core.Badge("table", "accent")), Td(core.Badge("failed", "severe"))),
			),
		),
	)

	dropdownShowcase := Div(
		Class("flex flex-wrap items-center gap-3"),
		core.ActionMenu(
			"Actions",
			actionMenuLink("/ui/components", "Open"),
			actionMenuLink("/ui/components", "Edit"),
			actionMenuPost("/ui/components", "Delete", func() Node { return Input(Type("hidden"), Name("csrf_token"), Value("preview")) }, true),
		),
	)

	feedbackShowcase := Div(
		Class("flex flex-col gap-3"),
		banner("info", "Info", "Use tokenized primitives for all new UI surfaces and controls."),
		banner("success", "Success", "Catalog sync completed and materializations are up to date."),
		banner("attention", "Attention", "Backfill service is configured but currently paused."),
		banner("danger", "Error", "Failed to apply policy changes. Review validation details."),
	)

	metricsShowcase := Div(
		Class("grid gap-3 md:grid-cols-2 xl:grid-cols-3"),
		metricCard("Active Models", "142", "+12 this week", "accent"),
		metricCard("Successful Runs", "98.7%", "7-day average", "success"),
		metricCard("Queued Backfills", "8", "2 high priority", "attention"),
	)

	emptyAndPaginationShowcase := Div(
		Class("flex flex-col gap-3"),
		emptyStateCard("No semantic models yet.", "Create model", "/ui/models/new"),
		paginationCard("/ui/components", domain.PageRequest{MaxResults: 10}, 42),
	)

	tabShowcase := segmentedTabs([]segmentedTabItem{
		{Label: "Overview", Active: true},
		{Label: "Schema"},
		{Label: "Lineage"},
		{Label: "History"},
	})

	treeShowcase := treeView([]treeViewItem{
		{
			Label: "analytics",
			Icon:  "database",
			Href:  "/ui/catalogs/analytics",
			Open:  true,
			Children: []treeViewItem{
				{
					Label: "core",
					Icon:  "folder",
					Href:  "/ui/catalogs/analytics/schemas/core",
					Open:  true,
					Children: []treeViewItem{
						{Label: "orders", Icon: "table", Href: "/ui/catalogs/analytics/schemas/core/orders"},
						{Label: "customers", Icon: "table", Href: "/ui/catalogs/analytics/schemas/core/customers", Active: true},
					},
				},
				{
					Label: "marts",
					Icon:  "folder",
					Href:  "/ui/catalogs/analytics/schemas/marts",
					Children: []treeViewItem{
						{Label: "revenue_daily", Icon: "file-chart-column", Href: "/ui/models/revenue_daily"},
					},
				},
			},
		},
	})

	limitedFormShowcase := Form(
		Class("flex max-w-xl flex-col gap-3"),
		formField(formFieldConfig{
			Label:        "Model name",
			Name:         "model_name",
			Placeholder:  "revenue_daily",
			Required:     true,
			ErrorMessage: "Model name is required.",
			Invalid:      true,
		}),
		formField(formFieldConfig{
			Label:       "Owner",
			Name:        "owner",
			Value:       "data-platform",
			Placeholder: "team-or-user",
			HelpText:    "Use the owning team slug for routing and alerts.",
		}),
		formField(formFieldConfig{
			Label:       "SLA minutes",
			Name:        "sla_minutes",
			Type:        "number",
			Value:       "60",
			Placeholder: "60",
			HelpText:    "Set expected freshness target in minutes.",
		}),
		Div(Class("mt-1 flex flex-wrap items-center gap-2 [&_form]:m-0 [&_form]:inline-flex"), core.PrimaryButton("", Type("button"), Text("Save")), core.SecondaryButton("", Type("button"), Text("Cancel"))),
	)

	loadingShowcase := Div(
		Class("flex max-w-xl flex-col gap-3"),
		Div(Class("flex items-center gap-3"), spinner(), P(Text("Loading metadata from metastore..."))),
		progressBar(68, 100),
	)

	tokenSwatches := Div(
		Class("grid gap-3 sm:grid-cols-2 xl:grid-cols-5"),
		Div(Class("flex flex-col gap-3 rounded-xl border border-[var(--borderColor-default)] bg-[var(--bgColor-muted)] p-4"), Div(Class("h-16 rounded-lg bg-[var(--bgColor-accent-emphasis)]")), Code(Class("text-xs"), Text("--color-accent"))),
		Div(Class("flex flex-col gap-3 rounded-xl border border-[var(--borderColor-default)] bg-[var(--bgColor-muted)] p-4"), Div(Class("h-16 rounded-lg bg-[var(--bgColor-success-emphasis)]")), Code(Class("text-xs"), Text("--color-success"))),
		Div(Class("flex flex-col gap-3 rounded-xl border border-[var(--borderColor-default)] bg-[var(--bgColor-muted)] p-4"), Div(Class("h-16 rounded-lg bg-[var(--bgColor-attention-emphasis)]")), Code(Class("text-xs"), Text("--color-warning"))),
		Div(Class("flex flex-col gap-3 rounded-xl border border-[var(--borderColor-default)] bg-[var(--bgColor-muted)] p-4"), Div(Class("h-16 rounded-lg bg-[var(--bgColor-danger-emphasis)]")), Code(Class("text-xs"), Text("--color-danger"))),
		Div(Class("flex flex-col gap-3 rounded-xl border border-[var(--borderColor-default)] bg-[var(--bgColor-muted)] p-4"), Div(Class("h-16 rounded-lg bg-[var(--bgColor-muted)]")), Code(Class("text-xs"), Text("--color-surface-muted"))),
	)

	return core.AppPage(
		"Components",
		"components",
		principal,
		Div(
			Class("grid gap-8"),
			componentCard("Buttons", "Shared buttons and link-buttons for primary flows, secondary actions, and icon affordances.", buttonShowcase, buttonGroupShowcase),
			Div(Class("grid gap-8 xl:grid-cols-2"),
				componentCard("Forms", "Shared inputs, selects, field wrappers, and real selection controls from core.", formShowcase, twoColumnFormShowcase, selectionControlShowcase, limitedFormShowcase),
				componentCard("Menus & Tables", "Shared action menus and data tables for list and detail views.", dropdownShowcase, tableShowcase, toolbarShowcase, actionBarShowcase),
			),
			Div(Class("grid gap-8 xl:grid-cols-2"),
				componentCard("States & Signals", "Empty states, banners, loading indicators, pagination, and semantic badges.", labelShowcase, feedbackShowcase, loadingShowcase, emptyAndPaginationShowcase),
				componentCard("Navigation & Identity", "Breadcrumbs, tree navigation, avatars, and segmented tabs.", breadcrumbShowcase, treeShowcase, avatarShowcase, tabShowcase),
			),
			Div(Class("grid gap-8 xl:grid-cols-[minmax(0,1.3fr)_minmax(18rem,0.9fr)]"),
				componentCard("Metrics", "Shared stat treatments used across dashboards, runtime assets, and products.", metricsShowcase),
				componentCard("Theme Tokens", "Semantic theme aliases used by the component library in light and dark mode.", tokenSwatches),
			),
		),
	)
}

func componentCard(title, copy string, body ...Node) Node {
	nodes := []Node{
		Div(
			Class("flex flex-col gap-2 border-l-2 border-[var(--borderColor-accent-emphasis)] pl-4"),
			H2(Class("m-0 text-xl font-semibold tracking-tight text-[var(--fgColor-default)]"), Text(title)),
			P(Class("m-0 max-w-2xl text-sm leading-6 text-[var(--fgColor-muted)]"), Text(copy)),
		),
	}
	nodes = append(nodes, body...)
	return Section(Class("flex flex-col gap-5 border-t border-[var(--borderColor-default)] pt-6 first:border-t-0 first:pt-0"), Group(nodes))
}
