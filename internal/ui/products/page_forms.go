package products

import (
	"duck-demo/internal/domain"
	"duck-demo/internal/ui/core"

	. "maragu.dev/gomponents"
	. "maragu.dev/gomponents/html"
)

func productNewPage(principal domain.ContextPrincipal, csrfField func() Node) Node {
	return core.AppPage(
		"New Product",
		"products",
		principal,
		Div(Class("flex flex-col gap-4"),
			Div(Class("flex flex-col gap-4 rounded-xl border border-[var(--borderColor-default)] bg-[var(--bgColor-default)] p-4 shadow-xs"),
				Div(Class("flex flex-wrap items-start justify-between gap-3"),
					Div(Class("flex min-w-0 flex-col gap-1"),
						H2(Class("m-0 text-lg font-semibold"), Text("Create product")),
						P(Class("text-sm text-[var(--fgColor-muted)]"), Text("Create a draft product, normalize ownership into domain/team records, and optionally link a primary runtime asset.")),
					),
				),
				Form(Method("post"), Action("/ui/products"),
					csrfField(),
					Div(Class("grid gap-4 md:grid-cols-2"),
						productFormField("slug", "Slug", "daily-orders"),
						productFormField("name", "Name", "Daily Orders"),
						productFormField("domain_name", "Domain", "Revenue"),
						productFormField("team_name", "Owner team", "Analytics Engineering"),
						productFormField("steward_principal", "Steward principal", "alice"),
						productFormField("contact_channel", "Contact channel", "#data-products"),
						productFormField("visibility", "Visibility", "internal"),
						productFormField("consumer_audience", "Audience", "analytics"),
						productFormField("docs_url", "Docs URL", "https://docs.example.com/products/daily-orders"),
						productFormField("access_request_path", "Access request path", "/access/daily-orders"),
						productFormField("data_grain", "Data grain", "one row per order"),
						productFormField("update_cadence", "Update cadence", "hourly"),
						productFormField("retention_window", "Retention window", "365d"),
						productFormField("freshness_slo", "Freshness SLO", "60m"),
						productFormField("latency_slo", "Latency SLO", "5m"),
						productFormField("breaking_change_policy", "Breaking change policy", "new version required"),
						productFormField("primary_asset_key", "Primary asset key", "main.analytics.daily_orders"),
						productFormField("semantic_model_refs", "Semantic model refs", "analytics.orders"),
					),
					Div(Class("flex flex-col gap-1 mt-4"),
						Label(For("description"), Text("Description")),
						core.TextareaControl("", Name("description"), ID("description"), Rows("5")),
					),
					Div(Class("mt-3 flex gap-2"),
						core.PrimaryButton("", Type("submit"), Text("Create draft product")),
						core.SecondaryLink("/ui/products", "", Text("Cancel")),
					),
				),
			),
		),
	)
}

func productFormField(name, label, placeholder string) Node {
	return Div(Class("flex flex-col gap-1"),
		Label(For(name), Text(label)),
		core.InputControl("", Type("text"), Name(name), ID(name), Placeholder(placeholder)),
	)
}
