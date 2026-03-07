package ui

import (
	"duck-demo/internal/domain"

	. "maragu.dev/gomponents"
	. "maragu.dev/gomponents/html"
)

func componentsPage(principal domain.ContextPrincipal) Node {
	buttonShowcase := Div(
		Class("component-row"),
		Button(Type("button"), Class("btn"), Text("Default")),
		Button(Type("button"), Class("btn btn-primary"), Text("Primary")),
		Button(Type("button"), Class("btn btn-danger"), Text("Danger")),
		Button(Type("button"), Class("btn btn-sm"), Text("Small")),
		Button(Type("button"), Class("btn btn-sm btn-icon"), Attr("aria-label", "Settings"), I(Class("btn-icon-glyph"), Attr("data-lucide", "settings"), Attr("aria-hidden", "true"))),
	)

	buttonGroupShowcase := Div(
		Class("component-row"),
		Div(
			Class("BtnGroup"),
			Button(Type("button"), Class("btn btn-sm"), Text("Run")),
			Button(Type("button"), Class("btn btn-sm"), Text("Explain")),
			Button(Type("button"), Class("btn btn-sm"), Text("History")),
		),
		Div(
			Class("BtnGroup"),
			Button(Type("button"), Class("btn btn-sm btn-icon"), Attr("aria-label", "Refresh"), I(Class("btn-icon-glyph"), Attr("data-lucide", "refresh-cw"), Attr("aria-hidden", "true"))),
			Button(Type("button"), Class("btn btn-sm btn-icon"), Attr("aria-label", "Download"), I(Class("btn-icon-glyph"), Attr("data-lucide", "download"), Attr("aria-hidden", "true"))),
			Button(Type("button"), Class("btn btn-sm btn-icon"), Attr("aria-label", "Share"), I(Class("btn-icon-glyph"), Attr("data-lucide", "share-2"), Attr("aria-hidden", "true"))),
		),
	)

	labelShowcase := Div(
		Class("component-row"),
		statusLabel("Default", ""),
		statusLabel("Accent", "accent"),
		statusLabel("Success", "success"),
		statusLabel("Attention", "attention"),
		statusLabel("Severe", "severe"),
	)

	breadcrumbShowcase := breadcrumbs([]breadcrumbItem{
		{Label: "Catalogs", Href: "/ui/catalogs"},
		{Label: "analytics", Href: "/ui/catalogs/analytics"},
		{Label: "core", Href: "/ui/catalogs/analytics/schemas/core", Active: true},
	})

	avatarShowcase := Div(
		Class("component-row"),
		avatar(avatarConfig{Label: "Data Platform", Tone: "accent", Size: "small"}),
		avatar(avatarConfig{Label: "Analytics Team", Tone: "success", Size: "medium"}),
		avatar(avatarConfig{Label: "Operations", Tone: "attention", Size: "large"}),
	)

	formShowcase := Div(
		Class("component-stack"),
		Label(Text("Search assets")),
		Input(Type("search"), Class("form-control"), Placeholder("orders.daily")),
		Label(Text("Catalog")),
		Select(Class("form-select"), Option(Text("analytics")), Option(Text("warehouse"))),
		Label(Text("Description")),
		Textarea(Placeholder("Add details for teammates")),
		Div(Class("button-row"), Button(Type("button"), Class(primaryButtonClass()), Text("Save")), Button(Type("button"), Class(secondaryButtonClass()), Text("Cancel"))),
	)

	twoColumnFormShowcase := Div(
		Class("component-columns"),
		Div(
			Class("component-stack"),
			Label(Text("Model name")),
			Input(Type("text"), Class("form-control"), Value("revenue_daily")),
		),
		Div(
			Class("component-stack"),
			Label(Text("Owner")),
			Input(Type("text"), Class("form-control"), Value("data-platform")),
		),
	)

	selectionControlShowcase := Div(
		Class("component-columns"),
		Div(
			Class("component-stack"),
			P(Class(mutedClass()), Text("Checkbox Group")),
			checkboxOption("feature-audit", "feature", "Enable audit logs", true),
			checkboxOption("feature-masking", "feature", "Enable column masking", true),
			checkboxOption("feature-rls", "feature", "Enable row-level security", false),
		),
		Div(
			Class("component-stack"),
			P(Class(mutedClass()), Text("Radio + Toggle")),
			radioOption("run-manual", "run-mode", "Manual run", true),
			radioOption("run-scheduled", "run-mode", "Scheduled run", false),
			toggleSwitch("notifications", "notifications", "Notify on failed runs", true),
		),
	)

	actionBarShowcase := actionBar()

	toolbarShowcase := Div(
		Class("component-stack"),
		pageToolbar("/ui/components", "New component"),
		quickFilterCardWithValue("Filter by name, type, or owner", "asset"),
	)

	tableShowcase := Div(
		Class("table-wrap"),
		Table(
			Class("data-table"),
			THead(
				Tr(
					Th(Text("Name")),
					Th(Text("Type")),
					Th(Text("Status")),
				),
			),
			TBody(
				Tr(Td(Text("orders.daily")), Td(statusLabel("table", "accent")), Td(statusLabel("healthy", "success"))),
				Tr(Td(Text("finance.revenue")), Td(statusLabel("view", "attention")), Td(statusLabel("warning", "attention"))),
				Tr(Td(Text("billing.settlements")), Td(statusLabel("table", "accent")), Td(statusLabel("failed", "severe"))),
			),
		),
	)

	dropdownShowcase := Div(
		Class("component-row"),
		actionMenu(
			"Actions",
			actionMenuLink("/ui/components", "Open"),
			actionMenuLink("/ui/components", "Edit"),
			actionMenuPost("/ui/components", "Delete", func() Node { return Input(Type("hidden"), Name("csrf_token"), Value("preview")) }, true),
		),
	)

	feedbackShowcase := Div(
		Class("component-stack"),
		banner("info", "Info", "Use tokenized primitives for all new UI surfaces and controls."),
		banner("success", "Success", "Catalog sync completed and materializations are up to date."),
		banner("attention", "Attention", "Backfill service is configured but currently paused."),
		banner("danger", "Error", "Failed to apply policy changes. Review validation details."),
	)

	metricsShowcase := Div(
		Class("metric-grid"),
		metricCard("Active Models", "142", "+12 this week", "accent"),
		metricCard("Successful Runs", "98.7%", "7-day average", "success"),
		metricCard("Queued Backfills", "8", "2 high priority", "attention"),
	)

	emptyAndPaginationShowcase := Div(
		Class("component-stack"),
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
		Class("component-stack component-form-limited"),
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
		Div(Class("button-row"), Button(Type("button"), Class(primaryButtonClass()), Text("Save")), Button(Type("button"), Class(secondaryButtonClass()), Text("Cancel"))),
	)

	loadingShowcase := Div(
		Class("component-stack component-form-limited"),
		Div(Class("component-row"), spinner(), P(Text("Loading metadata from metastore..."))),
		progressBar(68, 100),
	)

	tokenSwatches := Div(
		Class("token-grid"),
		Div(Class("token-swatch-card"), Div(Class("token-swatch token-swatch-accent")), Code(Text("--bgColor-accent-emphasis"))),
		Div(Class("token-swatch-card"), Div(Class("token-swatch token-swatch-success")), Code(Text("--bgColor-success-emphasis"))),
		Div(Class("token-swatch-card"), Div(Class("token-swatch token-swatch-attention")), Code(Text("--bgColor-attention-emphasis"))),
		Div(Class("token-swatch-card"), Div(Class("token-swatch token-swatch-danger")), Code(Text("--bgColor-danger-emphasis"))),
		Div(Class("token-swatch-card"), Div(Class("token-swatch token-swatch-surface")), Code(Text("--bgColor-muted"))),
	)

	return appPage(
		"Components",
		"components",
		principal,
		Div(
			Class("components-showcase"),
			Div(
				Class(cardClass("component-section")),
				H2(Text("Buttons")),
				P(Class(mutedClass()), Text("Shared button variants for primary flows, secondary actions, and compact icon controls.")),
				buttonShowcase,
				buttonGroupShowcase,
			),
			Div(
				Class(cardClass("component-section")),
				H2(Text("Labels")),
				P(Class(mutedClass()), Text("Semantic status labels driven by shared tone tokens.")),
				labelShowcase,
			),
			Div(
				Class(cardClass("component-section")),
				H2(Text("Breadcrumbs")),
				P(Class(mutedClass()), Text("Path navigation for hierarchical resources and detail pages.")),
				breadcrumbShowcase,
			),
			Div(
				Class(cardClass("component-section")),
				H2(Text("Action Bar")),
				P(Class(mutedClass()), Text("Search, sort, and actions in a single reusable row with DataStar signals.")),
				actionBarShowcase,
			),
			Div(
				Class(cardClass("component-section")),
				H2(Text("Forms")),
				P(Class(mutedClass()), Text("Input, select, and textarea controls using tokenized surfaces, borders, and focus states.")),
				formShowcase,
				twoColumnFormShowcase,
				selectionControlShowcase,
			),
			Div(
				Class(cardClass("component-section")),
				H2(Text("Toolbar & Filters")),
				P(Class(mutedClass()), Text("Reusable toolbar and quick-filter card for list pages.")),
				Div(Class("component-embed-flat"), toolbarShowcase),
			),
			Div(
				Class(cardClass("component-section")),
				H2(Text("Tables")),
				P(Class(mutedClass()), Text("Data table pattern for list pages with semantic labels.")),
				tableShowcase,
			),
			Div(
				Class(cardClass("component-section")),
				H2(Text("Menus")),
				P(Class(mutedClass()), Text("Dropdown action menu used across list and detail views.")),
				dropdownShowcase,
			),
			Div(
				Class(cardClass("component-section")),
				H2(Text("Avatar")),
				P(Class(mutedClass()), Text("Compact identity markers with semantic tone support.")),
				avatarShowcase,
			),
			Div(
				Class(cardClass("component-section")),
				H2(Text("Feedback")),
				P(Class(mutedClass()), Text("Inline banners for informational, success, warning, and error states.")),
				feedbackShowcase,
			),
			Div(
				Class(cardClass("component-section")),
				H2(Text("Loading")),
				P(Class(mutedClass()), Text("Spinner and progress indicators for async operations and background processing.")),
				loadingShowcase,
			),
			Div(
				Class(cardClass("component-section")),
				H2(Text("Cards & Metrics")),
				P(Class(mutedClass()), Text("Stat cards for dashboards and overview pages.")),
				metricsShowcase,
			),
			Div(
				Class(cardClass("component-section")),
				H2(Text("Tabs")),
				P(Class(mutedClass()), Text("Segmented tabs for switching related detail panels without nested boxes.")),
				tabShowcase,
			),
			Div(
				Class(cardClass("component-section")),
				H2(Text("Tree View")),
				P(Class(mutedClass()), Text("Reusable hierarchical tree with icons, active leaf styling, and disclosure behavior used in catalog navigation.")),
				treeShowcase,
			),
			Div(
				Class(cardClass("component-section")),
				H2(Text("Form Fields")),
				P(Class(mutedClass()), Text("Reusable field builder with required indicator, help text, and validation message.")),
				limitedFormShowcase,
			),
			Div(
				Class(cardClass("component-section")),
				H2(Text("Empty & Pagination")),
				P(Class(mutedClass()), Text("Consistent empty state and pagination components for collection pages.")),
				emptyAndPaginationShowcase,
			),
			Div(
				Class(cardClass("component-section")),
				H2(Text("Theme Tokens")),
				P(Class(mutedClass()), Text("Functional theme tokens are the source of truth for semantic colors in light and dark mode.")),
				tokenSwatches,
			),
		),
	)
}
