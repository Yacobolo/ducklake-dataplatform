package ui

import (
	"fmt"
	"net/url"
	"strconv"
	"strings"

	"duck-demo/internal/domain"

	. "maragu.dev/gomponents"
	data "maragu.dev/gomponents-datastar"
	. "maragu.dev/gomponents/html"
)

func productsListPage(principal domain.ContextPrincipal, items []domain.DataProductListItem, page domain.PageRequest, total int64, filterValue string) Node {
	published := 0
	certified := 0
	linked := 0
	cards := make([]Node, 0, len(items))
	for i := range items {
		item := items[i]
		if item.Status != nil && strings.EqualFold(item.Status.PublicationState, domain.ProductReleaseStatePublished) {
			published++
		}
		if item.Status != nil && strings.EqualFold(item.Status.CertificationState, domain.CertificationCertified) {
			certified++
		}
		if item.PrimaryOutput != nil {
			linked++
		}

		filter := strings.Join([]string{
			item.Product.Slug,
			item.Product.Name,
			item.Domain.Name,
			item.OwnerTeam.Name,
			item.Product.Description,
			item.Product.StewardPrincipal,
		}, " ")
		versionLabel := "No versions"
		if item.LatestVersion != nil {
			versionLabel = fmt.Sprintf("v%d", item.LatestVersion.Version)
		}
		healthLabel := "Status pending"
		healthTone := "attention"
		if item.Status != nil {
			healthLabel = fallbackString(item.Status.QualityStatus, "Status pending")
			healthTone = productHealthTone(item.Status.QualityStatus)
		}
		outputLabel := "No primary output"
		if item.PrimaryOutput != nil {
			outputLabel = item.PrimaryOutput.AssetKey
		}

		cards = append(cards,
			A(
				Href("/ui/products/"+url.PathEscape(item.Product.Slug)),
				Class("assets-showcase-card"),
				data.Show(containsExpr(filter)),
				Div(Class("assets-showcase-head"),
					Div(
						P(Class("assets-showcase-key"), Text(item.Product.Name)),
						P(Class("assets-showcase-owner"), Text(item.Domain.Name+" / "+item.OwnerTeam.Name)),
					),
					statusLabel(versionLabel, "accent"),
				),
				P(Class("assets-showcase-description"), Text(fallbackString(item.Product.Description, "No product description yet."))),
				Div(Class("assets-badge-row"),
					statusLabel(fallbackString(productListItemPublicationState(item), domain.ProductReleaseStateDraft), productPublicationTone(productListItemPublicationState(item))),
					statusLabel(fallbackString(productListItemCertificationState(item), domain.CertificationDraft), productCertificationTone(productListItemCertificationState(item))),
					statusLabel(healthLabel, healthTone),
				),
				P(Class("color-fg-muted text-small mb-1"), Text("Primary output: "+outputLabel)),
				P(Class("color-fg-muted text-small mb-0"), Text("Steward: "+fallbackString(item.Product.StewardPrincipal, "unassigned"))),
			),
		)
	}

	return appPage(
		"Products",
		"products",
		principal,
		Div(Class("assets-shell"),
			Div(Class("assets-hero"),
				Div(Class("assets-hero-copy"),
					P(Class("assets-kicker"), Text("Product control plane")),
					H2(Class("assets-hero-title"), Text("Products are the governed interface consumers should discover first.")),
					P(Class("assets-hero-text"), Text("Use products to package ownership, contract, runtime outputs, and trust state around the datasets your platform publishes.")),
					Div(Class("assets-hero-actions"),
						A(Href("/ui/products/new"), Class(primaryButtonClass()), Text("New product")),
						A(Href("/ui/assets"), Class(secondaryButtonClass()), Text("Open runtime assets")),
					),
				),
			),
			quickFilterCardWithValue("Filter by product, domain, team, or steward", filterValue),
			Div(Class("assets-metrics-grid"),
				productMetricCard("Total products", total),
				productMetricCard("Published", int64(published)),
				productMetricCard("Certified", int64(certified)),
				productMetricCard("Linked outputs", int64(linked)),
			),
			Div(Class(cardClass("assets-type-band")),
				Div(Class("assets-section-head"),
					H2(Text("Published catalog")),
					P(Class(mutedClass()), Text("Products link business ownership to runtime assets and semantic entrypoints.")),
				),
				func() Node {
					if len(cards) == 0 {
						return emptyStateCard("No products defined yet.", "Create product", "/ui/products/new")
					}
					return Div(Class("assets-showcase-grid"), Group(cards))
				}(),
			),
			paginationCard("/ui/products", page, total),
		),
	)
}

func productMetricCard(label string, value int64) Node {
	return Div(Class("assets-metric-card"),
		P(Class("assets-metric-label"), Text(label)),
		P(Class("assets-metric-value"), Text(strconv.FormatInt(value, 10))),
	)
}

func productDetailPage(principal domain.ContextPrincipal, detail *domain.DataProductDetail) Node {
	versionBadges := []Node{statusLabel("No versions", "attention")}
	if len(detail.Versions) > 0 {
		versionBadges = make([]Node, 0, len(detail.Versions))
		for i := range detail.Versions {
			versionBadges = append(versionBadges, statusLabel(
				fmt.Sprintf("v%d %s", detail.Versions[i].Version, detail.Versions[i].ReleaseState),
				productPublicationTone(detail.Versions[i].ReleaseState),
			))
		}
	}

	outputs := make([]Node, 0, len(detail.Outputs))
	for i := range detail.Outputs {
		output := detail.Outputs[i]
		label := output.AssetKey
		if output.IsPrimary {
			label += " (primary)"
		}
		outputs = append(outputs, Li(Class("asset-link-list-item"), A(Href("/ui/assets/"+url.PathEscape(output.AssetKey)), Text(label))))
	}
	if len(outputs) == 0 {
		outputs = append(outputs, Li(Class("asset-link-list-item"), Text("No outputs linked yet.")))
	}

	semanticEntrypoints := make([]Node, 0, len(detail.SemanticEntrypoints))
	for i := range detail.SemanticEntrypoints {
		semanticEntrypoints = append(semanticEntrypoints,
			Li(Class("asset-link-list-item"), Text(detail.SemanticEntrypoints[i].ProjectName+"."+detail.SemanticEntrypoints[i].ModelName)),
		)
	}
	if len(semanticEntrypoints) == 0 {
		semanticEntrypoints = append(semanticEntrypoints, Li(Class("asset-link-list-item"), Text("No semantic entrypoints linked yet.")))
	}

	dependencies := make([]Node, 0, len(detail.Dependencies))
	for i := range detail.Dependencies {
		dependencies = append(dependencies,
			Li(Class("asset-link-list-item"),
				A(Href("/ui/products/"+url.PathEscape(detail.Dependencies[i].Product.Slug)), Text(detail.Dependencies[i].Product.Name)),
			),
		)
	}
	if len(dependencies) == 0 {
		dependencies = append(dependencies, Li(Class("asset-link-list-item"), Text("No product dependencies linked yet.")))
	}

	subscriptions := make([]Node, 0, len(detail.Subscriptions))
	for i := range detail.Subscriptions {
		subscriptions = append(subscriptions,
			Li(Class("asset-link-list-item"),
				Text(detail.Subscriptions[i].PrincipalName+" • "+detail.Subscriptions[i].EventType+" • "+detail.Subscriptions[i].Channel),
			),
		)
	}
	if len(subscriptions) == 0 {
		subscriptions = append(subscriptions, Li(Class("asset-link-list-item"), Text("No subscribers yet.")))
	}

	events := make([]Node, 0, len(detail.Events))
	for i := range detail.Events {
		event := detail.Events[i]
		events = append(events,
			Li(Class("asset-link-list-item"),
				Strong(Text(event.Title+" ")),
				Text(event.EventType+" • "+formatTime(event.CreatedAt)),
			),
		)
	}
	if len(events) == 0 {
		events = append(events, Li(Class("asset-link-list-item"), Text("No product events recorded yet.")))
	}

	status := detail.Status
	publicationState := detail.Product.PublicationIntent
	certificationState := domain.CertificationDraft
	freshness := "UNKNOWN"
	quality := "UNKNOWN"
	lastSuccess := "-"
	adoptionScore := "0"
	downstreamCount := "0"
	subscriberCount := "0"
	if status != nil {
		publicationState = fallbackString(status.PublicationState, publicationState)
		certificationState = fallbackString(status.CertificationState, certificationState)
		freshness = fallbackString(status.FreshnessStatus, freshness)
		quality = fallbackString(status.QualityStatus, quality)
		lastSuccess = formatTimePtr(status.LastSuccessfulUpdateAt)
		adoptionScore = metricDisplay(status.AdoptionMetrics["adoption_score"])
		downstreamCount = metricDisplay(status.AdoptionMetrics["downstream_product_count"])
		subscriberCount = metricDisplay(status.AdoptionMetrics["subscription_count"])
	}

	return appPage(
		"Product: "+detail.Product.Name,
		"products",
		principal,
		Div(Class("asset-detail-shell"),
			Div(Class("asset-detail-hero"),
				Div(Class("asset-detail-hero-copy"),
					P(Class("assets-kicker"), Text("Published interface")),
					Div(Class("asset-detail-title-row"),
						H2(Class("asset-detail-title"), Text(detail.Product.Name)),
						statusLabel(publicationState, productPublicationTone(publicationState)),
					),
					P(Class("asset-detail-description"), Text(fallbackString(detail.Product.Description, "No description provided yet."))),
					Div(Class("assets-badge-row"),
						statusLabel(certificationState, productCertificationTone(certificationState)),
						statusLabel(freshness, productHealthTone(freshness)),
						statusLabel(quality, productHealthTone(quality)),
					),
				),
				Div(Class("asset-detail-hero-meta"),
					assetDetailMetaRow("Slug", detail.Product.Slug),
					assetDetailMetaRow("Domain", detail.Domain.Name),
					assetDetailMetaRow("Owner team", detail.OwnerTeam.Name),
					assetDetailMetaRow("Steward", fallbackString(detail.Product.StewardPrincipal, "unassigned")),
				),
			),
			Div(Class("asset-detail-metrics"),
				assetDetailMetricCard("Versions", strconv.Itoa(len(detail.Versions)), "Immutable release records"),
				assetDetailMetricCard("Outputs", strconv.Itoa(len(detail.Outputs)), "Runtime assets linked to the latest release"),
				assetDetailMetricCard("Freshness", freshness, "Computed from linked runtime state"),
				assetDetailMetricCard("Last success", lastSuccess, "Latest successful update across linked outputs"),
				assetDetailMetricCard("Adoption score", adoptionScore, "Ranked from current control-plane usage signals"),
				assetDetailMetricCard("Downstream products", downstreamCount, "Products depending on this product"),
				assetDetailMetricCard("Subscribers", subscriberCount, "Consumers following product events"),
			),
			Div(Class("asset-detail-layout"),
				Div(Class("asset-detail-main"),
					Div(Class(cardClass("asset-detail-section")),
						Div(Class("assets-section-head"), H2(Text("Contract")), P(Class(mutedClass()), Text("Consumer-facing contract and SLO expectations."))),
						assetFactList([][2]string{
							{"Data grain", fallbackString(detail.Product.Contract.DataGrain, "-")},
							{"Update cadence", fallbackString(detail.Product.Contract.UpdateCadence, "-")},
							{"Retention", fallbackString(detail.Product.Contract.RetentionWindow, "-")},
							{"Freshness SLO", fallbackString(detail.Product.SLO.FreshnessSLO, "-")},
							{"Latency SLO", fallbackString(detail.Product.SLO.LatencySLO, "-")},
							{"Change policy", fallbackString(detail.Product.Contract.BreakingChangePolicy, "-")},
						}),
					),
					Div(Class(cardClass("asset-detail-section")),
						Div(Class("assets-section-head"), H2(Text("Latest outputs")), P(Class(mutedClass()), Text("Runtime resources linked to the current product release."))),
						Ul(Class("asset-link-list"), Group(outputs)),
					),
					Div(Class(cardClass("asset-detail-section")),
						Div(Class("assets-section-head"), H2(Text("Semantic entrypoints")), P(Class(mutedClass()), Text("Consumer-facing semantic models linked to the current product release."))),
						Ul(Class("asset-link-list"), Group(semanticEntrypoints)),
					),
					Div(Class(cardClass("asset-detail-section")),
						Div(Class("assets-section-head"), H2(Text("Version history")), P(Class(mutedClass()), Text("Published state is versioned even when the UI is still minimal."))),
						Div(Class("assets-badge-row"), Group(versionBadges)),
						Ul(Class("asset-link-list mt-3"), Group(productVersionLinks(detail))),
					),
					Div(Class(cardClass("asset-detail-section")),
						Div(Class("assets-section-head"), H2(Text("Dependencies")), P(Class(mutedClass()), Text("Upstream products that affect this product's trust state."))),
						Ul(Class("asset-link-list"), Group(dependencies)),
					),
					Div(Class(cardClass("asset-detail-section")),
						Div(Class("assets-section-head"), H2(Text("Subscriptions")), P(Class(mutedClass()), Text("Consumers following product change events."))),
						Ul(Class("asset-link-list"), Group(subscriptions)),
					),
					Div(Class(cardClass("asset-detail-section")),
						Div(Class("assets-section-head"), H2(Text("Recent events")), P(Class(mutedClass()), Text("Durable product lifecycle and health events."))),
						Ul(Class("asset-link-list"), Group(events)),
					),
				),
				Div(Class("asset-detail-rail"),
					Div(Class(cardClass("asset-rail-card")),
						H2(Text("Ownership")),
						assetFactList([][2]string{
							{"Domain", detail.Domain.Name},
							{"Team", detail.OwnerTeam.Name},
							{"Contact", fallbackString(detail.Product.ContactChannel, "-")},
							{"Docs", fallbackString(detail.Product.DocsURL, "-")},
							{"Access path", fallbackString(detail.Product.AccessRequestPath, "-")},
						}),
					),
					Div(Class(cardClass("asset-rail-card")),
						H2(Text("Lifecycle")),
						Form(Method("post"), Action("/ui/products/"+url.PathEscape(detail.Product.Slug)+"/publish"), Class("stack-form"),
							Div(Class("form-group"),
								Label(For("publish-version"), Text("Publish version")),
								Input(Type("number"), Name("version"), ID("publish-version"), Value(defaultVersionValue(detail.Versions)), Min("1"), Class("form-control")),
							),
							Button(Type("submit"), Class(primaryButtonClass()), Text("Publish")),
						),
						Form(Method("post"), Action("/ui/products/"+url.PathEscape(detail.Product.Slug)+"/deprecate"), Class("stack-form mt-3"),
							Div(Class("form-group"),
								Label(For("deprecate-version"), Text("Deprecate version")),
								Input(Type("number"), Name("version"), ID("deprecate-version"), Value(defaultVersionValue(detail.Versions)), Min("1"), Class("form-control")),
							),
							Div(Class("form-group"),
								Label(For("replacement-slug"), Text("Replacement product slug")),
								Input(Type("text"), Name("replacement_slug"), ID("replacement-slug"), Class("form-control"), Placeholder("replacement-product")),
							),
							Button(Type("submit"), Class(secondaryButtonClass()), Text("Deprecate")),
						),
						Form(Method("post"), Action("/ui/products/"+url.PathEscape(detail.Product.Slug)+"/retire"), Class("stack-form mt-3"),
							Div(Class("form-group"),
								Label(For("retire-version"), Text("Retire version")),
								Input(Type("number"), Name("version"), ID("retire-version"), Value(defaultVersionValue(detail.Versions)), Min("1"), Class("form-control")),
							),
							Button(Type("submit"), Class(secondaryButtonClass()), Text("Retire")),
						),
					),
					Div(Class(cardClass("asset-rail-card")),
						H2(Text("New Version")),
						Form(Method("post"), Action("/ui/products/"+url.PathEscape(detail.Product.Slug)+"/versions"), Class("stack-form"),
							productFormField("compatibility_level", "Compatibility", domain.ProductCompatibilityBackwardCompatible),
							productFormField("data_grain", "Data grain", detail.Product.Contract.DataGrain),
							productFormField("update_cadence", "Update cadence", detail.Product.Contract.UpdateCadence),
							productFormField("retention_window", "Retention window", detail.Product.Contract.RetentionWindow),
							productFormField("freshness_slo", "Freshness SLO", detail.Product.SLO.FreshnessSLO),
							productFormField("latency_slo", "Latency SLO", detail.Product.SLO.LatencySLO),
							productFormField("breaking_change_policy", "Breaking change policy", detail.Product.Contract.BreakingChangePolicy),
							productFormField("docs_url", "Docs URL", detail.Product.DocsURL),
							productFormField("access_request_path", "Access path", detail.Product.AccessRequestPath),
							productFormField("output_asset_keys", "Output asset keys", joinOutputAssetKeys(detail.Outputs)),
							productFormField("semantic_model_refs", "Semantic model refs", joinSemanticModelRefs(detail.SemanticEntrypoints)),
							Button(Type("submit"), Class(primaryButtonClass()), Text("Create draft version")),
						),
					),
					Div(Class(cardClass("asset-rail-card")),
						H2(Text("Dependency")),
						Form(Method("post"), Action("/ui/products/"+url.PathEscape(detail.Product.Slug)+"/dependencies"), Class("stack-form"),
							productFormField("depends_on_slug", "Depends on product slug", "upstream-product"),
							Button(Type("submit"), Class(secondaryButtonClass()), Text("Add dependency")),
						),
					),
					Div(Class(cardClass("asset-rail-card")),
						H2(Text("Subscribe")),
						Form(Method("post"), Action("/ui/products/"+url.PathEscape(detail.Product.Slug)+"/subscriptions"), Class("stack-form"),
							productFormField("event_type", "Event type", "freshness_breach"),
							productFormField("channel", "Channel", "inbox"),
							Button(Type("submit"), Class(secondaryButtonClass()), Text("Subscribe")),
						),
					),
				),
			),
		),
	)
}

func productVersionPage(principal domain.ContextPrincipal, detail *domain.DataProductDetail, versionDetail *domain.DataProductVersionDetail) Node {
	version := versionDetail.Version

	outputs := make([]Node, 0, len(versionDetail.Outputs))
	for i := range versionDetail.Outputs {
		output := versionDetail.Outputs[i]
		outputs = append(outputs,
			Li(Class("asset-link-list-item"),
				A(Href("/ui/assets/"+url.PathEscape(output.AssetKey)), Text(output.AssetKey)),
			),
		)
	}
	if len(outputs) == 0 {
		outputs = append(outputs, Li(Class("asset-link-list-item"), Text("No outputs linked to this version.")))
	}

	semanticEntrypoints := make([]Node, 0, len(versionDetail.SemanticEntrypoints))
	for i := range versionDetail.SemanticEntrypoints {
		semanticEntrypoints = append(semanticEntrypoints,
			Li(Class("asset-link-list-item"), Text(versionDetail.SemanticEntrypoints[i].ProjectName+"."+versionDetail.SemanticEntrypoints[i].ModelName)),
		)
	}
	if len(semanticEntrypoints) == 0 {
		semanticEntrypoints = append(semanticEntrypoints, Li(Class("asset-link-list-item"), Text("No semantic entrypoints linked to this version.")))
	}

	return appPage(
		fmt.Sprintf("Product %s v%d", detail.Product.Name, version.Version),
		"products",
		principal,
		Div(Class("asset-detail-shell"),
			Div(Class("asset-detail-hero"),
				Div(Class("asset-detail-hero-copy"),
					P(Class("assets-kicker"), Text("Version detail")),
					Div(Class("asset-detail-title-row"),
						H2(Class("asset-detail-title"), Text(detail.Product.Name+" v"+strconv.Itoa(version.Version))),
						statusLabel(version.ReleaseState, productPublicationTone(version.ReleaseState)),
					),
					P(Class("asset-detail-description"), Text("Immutable contract snapshot for the selected product release.")),
					Div(Class("assets-badge-row"),
						statusLabel(version.CompatibilityLevel, "accent"),
						statusLabel(fallbackString(detail.Product.Visibility, "internal"), "accent"),
					),
				),
				Div(Class("asset-detail-hero-meta"),
					assetDetailMetaRow("Product", detail.Product.Name),
					assetDetailMetaRow("Slug", detail.Product.Slug),
					assetDetailMetaRow("Domain", detail.Domain.Name),
					assetDetailMetaRow("Owner team", detail.OwnerTeam.Name),
				),
			),
			Div(Class("asset-detail-layout"),
				Div(Class("asset-detail-main"),
					Div(Class(cardClass("asset-detail-section")),
						Div(Class("assets-section-head"), H2(Text("Contract snapshot")), P(Class(mutedClass()), Text("Versioned consumer-facing contract fields."))),
						assetFactList([][2]string{
							{"Data grain", fallbackString(version.Contract.DataGrain, "-")},
							{"Update cadence", fallbackString(version.Contract.UpdateCadence, "-")},
							{"Retention", fallbackString(version.Contract.RetentionWindow, "-")},
							{"Freshness SLO", fallbackString(version.SLO.FreshnessSLO, "-")},
							{"Latency SLO", fallbackString(version.SLO.LatencySLO, "-")},
							{"Change policy", fallbackString(version.Contract.BreakingChangePolicy, "-")},
						}),
					),
					Div(Class(cardClass("asset-detail-section")),
						Div(Class("assets-section-head"), H2(Text("Outputs")), P(Class(mutedClass()), Text("Runtime assets currently linked to this release snapshot."))),
						Ul(Class("asset-link-list"), Group(outputs)),
					),
					Div(Class(cardClass("asset-detail-section")),
						Div(Class("assets-section-head"), H2(Text("Semantic entrypoints")), P(Class(mutedClass()), Text("Semantic entrypoints exposed by this release."))),
						Ul(Class("asset-link-list"), Group(semanticEntrypoints)),
					),
				),
				Div(Class("asset-detail-rail"),
					Div(Class(cardClass("asset-rail-card")),
						H2(Text("Release metadata")),
						assetFactList([][2]string{
							{"Release state", version.ReleaseState},
							{"Compatibility", version.CompatibilityLevel},
							{"Docs", fallbackString(version.DocsURL, "-")},
							{"Access path", fallbackString(version.AccessRequestPath, "-")},
							{"Created by", fallbackString(version.CreatedBy, "-")},
							{"Created at", formatTime(version.CreatedAt)},
						}),
					),
					Div(Class(cardClass("asset-rail-card")),
						H2(Text("Navigate")),
						P(Class(mutedClass()), Text("Inspect the full product control plane or switch versions.")),
						A(Href("/ui/products/"+url.PathEscape(detail.Product.Slug)), Class(secondaryButtonClass()), Text("Back to product")),
						Ul(Class("asset-link-list mt-3"), Group(productVersionLinks(detail))),
					),
				),
			),
		),
	)
}

func productVersionLinks(detail *domain.DataProductDetail) []Node {
	links := make([]Node, 0, len(detail.Versions))
	for i := range detail.Versions {
		version := detail.Versions[i]
		links = append(links,
			Li(Class("asset-link-list-item"),
				A(Href("/ui/products/"+url.PathEscape(detail.Product.Slug)+"/versions/"+strconv.Itoa(version.Version)),
					Text(fmt.Sprintf("v%d %s", version.Version, version.ReleaseState)),
				),
			),
		)
	}
	if len(links) == 0 {
		links = append(links, Li(Class("asset-link-list-item"), Text("No versions created yet.")))
	}
	return links
}

func productNewPage(principal domain.ContextPrincipal, csrfField func() Node) Node {
	return appPage(
		"New Product",
		"products",
		principal,
		Div(Class("asset-detail-shell"),
			Div(Class(cardClass("asset-detail-section")),
				Div(Class("assets-section-head"),
					H2(Text("Create product")),
					P(Class(mutedClass()), Text("Create a draft product, normalize ownership into domain/team records, and optionally link a primary runtime asset.")),
				),
				Form(Method("post"), Action("/ui/products"),
					csrfField(),
					Div(Class("form-grid"),
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
					Div(Class("form-group"),
						Label(For("description"), Text("Description")),
						Textarea(Name("description"), ID("description"), Class("form-control"), Rows("5")),
					),
					Div(Class("mt-3 d-flex gap-2"),
						Button(Type("submit"), Class(primaryButtonClass()), Text("Create draft product")),
						A(Href("/ui/products"), Class(secondaryButtonClass()), Text("Cancel")),
					),
				),
			),
		),
	)
}

func productFormField(name, label, placeholder string) Node {
	return Div(Class("form-group"),
		Label(For(name), Text(label)),
		Input(Type("text"), Name(name), ID(name), Class("form-control"), Placeholder(placeholder)),
	)
}

func productPublicationTone(state string) string {
	switch strings.ToUpper(strings.TrimSpace(state)) {
	case domain.ProductReleaseStatePublished:
		return "success"
	case domain.ProductReleaseStateDeprecated:
		return "attention"
	case domain.ProductReleaseStateRetired:
		return "severe"
	default:
		return "accent"
	}
}

func productCertificationTone(state string) string {
	switch strings.ToUpper(strings.TrimSpace(state)) {
	case domain.CertificationCertified:
		return "success"
	case domain.CertificationDeprecated:
		return "attention"
	default:
		return "accent"
	}
}

func metricDisplay(value any) string {
	switch v := value.(type) {
	case int:
		return strconv.Itoa(v)
	case int32:
		return strconv.FormatInt(int64(v), 10)
	case int64:
		return strconv.FormatInt(v, 10)
	case float64:
		return strconv.FormatInt(int64(v), 10)
	case string:
		if strings.TrimSpace(v) != "" {
			return v
		}
	}
	return "0"
}

func productHealthTone(state string) string {
	switch strings.ToUpper(strings.TrimSpace(state)) {
	case "HEALTHY", "GOOD":
		return "success"
	case "STALE", "DEGRADED", "WARNING":
		return "attention"
	case "FAILED", "ERROR":
		return "severe"
	default:
		return "accent"
	}
}

func productListItemPublicationState(item domain.DataProductListItem) string {
	if item.Status == nil {
		return item.Product.PublicationIntent
	}
	return item.Status.PublicationState
}

func productListItemCertificationState(item domain.DataProductListItem) string {
	if item.Status == nil {
		return domain.CertificationDraft
	}
	return item.Status.CertificationState
}

func defaultVersionValue(versions []domain.DataProductVersion) string {
	if len(versions) == 0 {
		return "1"
	}
	return strconv.Itoa(versions[0].Version)
}

func joinOutputAssetKeys(outputs []domain.ProductOutput) string {
	if len(outputs) == 0 {
		return ""
	}
	keys := make([]string, 0, len(outputs))
	for i := range outputs {
		keys = append(keys, outputs[i].AssetKey)
	}
	return strings.Join(keys, ", ")
}

func joinSemanticModelRefs(entrypoints []domain.ProductSemanticEntrypoint) string {
	if len(entrypoints) == 0 {
		return ""
	}
	refs := make([]string, 0, len(entrypoints))
	for i := range entrypoints {
		refs = append(refs, entrypoints[i].ProjectName+"."+entrypoints[i].ModelName)
	}
	return strings.Join(refs, ", ")
}
