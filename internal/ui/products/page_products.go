package products

import (
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"

	"duck-demo/internal/domain"
	"duck-demo/internal/ui/core"

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
				Class(assetShowcaseCardClass()),
				data.Show(containsExpr(filter)),
				Div(Class(assetShowcaseHeadClass()),
					Div(
						P(Class(assetShowcaseKeyClass()), Text(item.Product.Name)),
						P(Class(assetShowcaseOwnerClass()), Text(item.Domain.Name+" / "+item.OwnerTeam.Name)),
					),
					statusLabel(versionLabel, "accent"),
				),
				P(Class(assetShowcaseDescriptionClass()), Text(fallbackString(item.Product.Description, "No product description yet."))),
				Div(Class(assetBadgeRowClass()),
					statusLabel(fallbackString(productListItemPublicationState(item), domain.ProductReleaseStateDraft), productPublicationTone(productListItemPublicationState(item))),
					statusLabel(fallbackString(productListItemCertificationState(item), domain.CertificationDraft), productCertificationTone(productListItemCertificationState(item))),
					statusLabel(healthLabel, healthTone),
				),
				P(Class("mb-1 text-xs text-[var(--fgColor-muted)]"), Text("Primary output: "+outputLabel)),
				P(Class("m-0 text-xs text-[var(--fgColor-muted)]"), Text("Steward: "+fallbackString(item.Product.StewardPrincipal, "unassigned"))),
			),
		)
	}

	return core.AppPage(
		"Products",
		"products",
		principal,
		Div(Class(assetShellClass()),
			Div(Class(assetHeroClass()),
				Div(Class(assetHeroCopyClass()),
					P(Class(assetKickerClass()), Text("Product control plane")),
					H2(Class(assetTitleClass()), Text("Products are the governed interface consumers should discover first.")),
					P(Class(assetDescriptionClass()), Text("Use products to package ownership, contract, runtime outputs, and trust state around the datasets your platform publishes.")),
					Div(Class(assetHeroActionsClass()),
						core.PrimaryLink("/ui/products/new", "", Text("New product")),
						core.SecondaryLink("/ui/assets", "", Text("Open runtime assets")),
					),
				),
			),
			quickFilterCardWithValue("Filter by product, domain, team, or steward", filterValue),
			Div(Class(assetMetricsGridClass()),
				productMetricCard("Total products", total),
				productMetricCard("Published", int64(published)),
				productMetricCard("Certified", int64(certified)),
				productMetricCard("Linked outputs", int64(linked)),
			),
			Div(Class(assetTypeBandClass()),
				Div(Class(assetSectionHeadClass()),
					H2(Text("Published catalog")),
					P(Class("text-sm text-[var(--fgColor-muted)]"), Text("Products link business ownership to runtime assets and semantic entrypoints.")),
				),
				func() Node {
					if len(cards) == 0 {
						return emptyStateCard("No products defined yet.", "Create product", "/ui/products/new")
					}
					return Div(Class(assetShowcaseGridClass()), Group(cards))
				}(),
			),
			paginationCard("/ui/products", page, total),
		),
	)
}

func productMetricCard(label string, value int64) Node {
	return core.MetricCard(label, strconv.FormatInt(value, 10), "")
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
		outputs = append(outputs, core.ItemListEntry(core.TextLink("/ui/assets/"+url.PathEscape(output.AssetKey), Text(label))))
	}
	if len(outputs) == 0 {
		outputs = append(outputs, core.ItemListEntry(Text("No outputs linked yet.")))
	}

	semanticEntrypoints := make([]Node, 0, len(detail.SemanticEntrypoints))
	for i := range detail.SemanticEntrypoints {
		semanticEntrypoints = append(semanticEntrypoints, core.ItemListEntry(Text(detail.SemanticEntrypoints[i].ProjectName+"."+detail.SemanticEntrypoints[i].ModelName)))
	}
	if len(semanticEntrypoints) == 0 {
		semanticEntrypoints = append(semanticEntrypoints, core.ItemListEntry(Text("No semantic entrypoints linked yet.")))
	}

	dependencies := make([]Node, 0, len(detail.Dependencies))
	for i := range detail.Dependencies {
		dependencies = append(dependencies, core.ItemListEntry(core.TextLink("/ui/products/"+url.PathEscape(detail.Dependencies[i].Product.Slug), Text(detail.Dependencies[i].Product.Name))))
	}
	if len(dependencies) == 0 {
		dependencies = append(dependencies, core.ItemListEntry(Text("No product dependencies linked yet.")))
	}

	subscriptions := make([]Node, 0, len(detail.Subscriptions))
	for i := range detail.Subscriptions {
		subscriptions = append(subscriptions, core.ItemListEntry(Text(detail.Subscriptions[i].PrincipalName+" • "+detail.Subscriptions[i].EventType+" • "+detail.Subscriptions[i].Channel)))
	}
	if len(subscriptions) == 0 {
		subscriptions = append(subscriptions, core.ItemListEntry(Text("No subscribers yet.")))
	}

	events := make([]Node, 0, len(detail.Events))
	for i := range detail.Events {
		event := detail.Events[i]
		events = append(events, core.ItemListEntry(Strong(Text(event.Title+" ")), Text(event.EventType+" • "+formatTime(event.CreatedAt))))
	}
	if len(events) == 0 {
		events = append(events, core.ItemListEntry(Text("No product events recorded yet.")))
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

	return core.AppPage(
		"Product: "+detail.Product.Name,
		"products",
		principal,
		core.DetailShell(
			core.DetailHero(
				core.DetailHeroCopy(
					core.Kicker("Published interface"),
					core.DetailTitleRow(
						core.DetailTitle(detail.Product.Name),
						statusLabel(publicationState, productPublicationTone(publicationState)),
					),
					core.DetailDescription(fallbackString(detail.Product.Description, "No description provided yet.")),
					core.BadgeRow(
						statusLabel(certificationState, productCertificationTone(certificationState)),
						statusLabel(freshness, productHealthTone(freshness)),
						statusLabel(quality, productHealthTone(quality)),
					),
				),
				core.DetailHeroMeta(
					core.MetaItem("Slug", detail.Product.Slug),
					core.MetaItem("Domain", detail.Domain.Name),
					core.MetaItem("Owner team", detail.OwnerTeam.Name),
					core.MetaItem("Steward", fallbackString(detail.Product.StewardPrincipal, "unassigned")),
				),
			),
			core.MetricsGrid(
				core.MetricCard("Versions", strconv.Itoa(len(detail.Versions)), "Immutable release records"),
				core.MetricCard("Outputs", strconv.Itoa(len(detail.Outputs)), "Runtime assets linked to the latest release"),
				core.MetricCard("Freshness", freshness, "Computed from linked runtime state"),
				core.MetricCard("Last success", lastSuccess, "Latest successful update across linked outputs"),
				core.MetricCard("Adoption score", adoptionScore, "Ranked from current control-plane usage signals"),
				core.MetricCard("Downstream products", downstreamCount, "Products depending on this product"),
				core.MetricCard("Subscribers", subscriberCount, "Consumers following product events"),
			),
			core.DetailLayout(
				core.DetailMain(
					Div(Class(assetSectionCardClass()),
						Div(Class(assetSectionHeadClass()), H2(Class(sectionTitleClass()), Text("Contract")), P(Class(sectionCopyClass()), Text("Consumer-facing contract and SLO expectations."))),
						core.FactList([][2]string{
							{"Data grain", fallbackString(detail.Product.Contract.DataGrain, "-")},
							{"Update cadence", fallbackString(detail.Product.Contract.UpdateCadence, "-")},
							{"Retention", fallbackString(detail.Product.Contract.RetentionWindow, "-")},
							{"Freshness SLO", fallbackString(detail.Product.SLO.FreshnessSLO, "-")},
							{"Latency SLO", fallbackString(detail.Product.SLO.LatencySLO, "-")},
							{"Change policy", fallbackString(detail.Product.Contract.BreakingChangePolicy, "-")},
						}),
					),
					Div(Class(assetSectionCardClass()),
						Div(Class(assetSectionHeadClass()), H2(Class(sectionTitleClass()), Text("Latest outputs")), P(Class(sectionCopyClass()), Text("Runtime resources linked to the current product release."))),
						core.ItemList("", Group(outputs)),
					),
					Div(Class(assetSectionCardClass()),
						Div(Class(assetSectionHeadClass()), H2(Class(sectionTitleClass()), Text("Semantic entrypoints")), P(Class(sectionCopyClass()), Text("Consumer-facing semantic models linked to the current product release."))),
						core.ItemList("", Group(semanticEntrypoints)),
					),
					Div(Class(assetSectionCardClass()),
						Div(Class(assetSectionHeadClass()), H2(Class(sectionTitleClass()), Text("Version history")), P(Class(sectionCopyClass()), Text("Published state is versioned even when the UI is still minimal."))),
						core.BadgeRow(Group(versionBadges)),
						core.ItemList("mt-3", Group(productVersionLinks(detail))),
					),
					Div(Class(assetSectionCardClass()),
						Div(Class(assetSectionHeadClass()), H2(Class(sectionTitleClass()), Text("Dependencies")), P(Class(sectionCopyClass()), Text("Upstream products that affect this product's trust state."))),
						core.ItemList("", Group(dependencies)),
					),
					Div(Class(assetSectionCardClass()),
						Div(Class(assetSectionHeadClass()), H2(Class(sectionTitleClass()), Text("Subscriptions")), P(Class(sectionCopyClass()), Text("Consumers following product change events."))),
						core.ItemList("", Group(subscriptions)),
					),
					Div(Class(assetSectionCardClass()),
						Div(Class(assetSectionHeadClass()), H2(Class(sectionTitleClass()), Text("Recent events")), P(Class(sectionCopyClass()), Text("Durable product lifecycle and health events."))),
						core.ItemList("", Group(events)),
					),
				),
				core.DetailRail(
					Div(Class(assetSectionCardClass()),
						H2(Class(sectionTitleClass()), Text("Ownership")),
						core.FactList([][2]string{
							{"Domain", detail.Domain.Name},
							{"Team", detail.OwnerTeam.Name},
							{"Contact", fallbackString(detail.Product.ContactChannel, "-")},
							{"Docs", fallbackString(detail.Product.DocsURL, "-")},
							{"Access path", fallbackString(detail.Product.AccessRequestPath, "-")},
						}),
					),
					Div(Class(assetSectionCardClass()),
						H2(Class(sectionTitleClass()), Text("Lifecycle")),
						Form(Method("post"), Action("/ui/products/"+url.PathEscape(detail.Product.Slug)+"/publish"), Class("stack-form [&>:not(label):not(.form-actions)]:mb-3 [&>:last-child]:mb-0"),
							Div(Class("form-group"),
								Label(For("publish-version"), Text("Publish version")),
								core.InputControl("", Type("number"), Name("version"), ID("publish-version"), Value(defaultVersionValue(detail.Versions)), Min("1")),
							),
							core.PrimaryButton("", Type("submit"), Text("Publish")),
						),
						Form(Method("post"), Action("/ui/products/"+url.PathEscape(detail.Product.Slug)+"/deprecate"), Class("stack-form [&>:not(label):not(.form-actions)]:mb-3 [&>:last-child]:mb-0 mt-3"),
							Div(Class("form-group"),
								Label(For("deprecate-version"), Text("Deprecate version")),
								core.InputControl("", Type("number"), Name("version"), ID("deprecate-version"), Value(defaultVersionValue(detail.Versions)), Min("1")),
							),
							Div(Class("form-group"),
								Label(For("replacement-slug"), Text("Replacement product slug")),
								core.InputControl("", Type("text"), Name("replacement_slug"), ID("replacement-slug"), Placeholder("replacement-product")),
							),
							core.SecondaryButton("", Type("submit"), Text("Deprecate")),
						),
						Form(Method("post"), Action("/ui/products/"+url.PathEscape(detail.Product.Slug)+"/retire"), Class("stack-form [&>:not(label):not(.form-actions)]:mb-3 [&>:last-child]:mb-0 mt-3"),
							Div(Class("form-group"),
								Label(For("retire-version"), Text("Retire version")),
								core.InputControl("", Type("number"), Name("version"), ID("retire-version"), Value(defaultVersionValue(detail.Versions)), Min("1")),
							),
							core.SecondaryButton("", Type("submit"), Text("Retire")),
						),
					),
					Div(Class(assetSectionCardClass()),
						H2(Class(sectionTitleClass()), Text("New Version")),
						Form(Method("post"), Action("/ui/products/"+url.PathEscape(detail.Product.Slug)+"/versions"), Class("stack-form [&>:not(label):not(.form-actions)]:mb-3 [&>:last-child]:mb-0"),
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
							core.PrimaryButton("", Type("submit"), Text("Create draft version")),
						),
					),
					Div(Class(assetSectionCardClass()),
						H2(Class(sectionTitleClass()), Text("Dependency")),
						Form(Method("post"), Action("/ui/products/"+url.PathEscape(detail.Product.Slug)+"/dependencies"), Class("stack-form [&>:not(label):not(.form-actions)]:mb-3 [&>:last-child]:mb-0"),
							productFormField("depends_on_slug", "Depends on product slug", "upstream-product"),
							core.SecondaryButton("", Type("submit"), Text("Add dependency")),
						),
					),
					Div(Class(assetSectionCardClass()),
						H2(Class(sectionTitleClass()), Text("Subscribe")),
						Form(Method("post"), Action("/ui/products/"+url.PathEscape(detail.Product.Slug)+"/subscriptions"), Class("stack-form [&>:not(label):not(.form-actions)]:mb-3 [&>:last-child]:mb-0"),
							productFormField("event_type", "Event type", "freshness_breach"),
							productFormField("channel", "Channel", "inbox"),
							core.SecondaryButton("", Type("submit"), Text("Subscribe")),
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
		outputs = append(outputs, core.ItemListEntry(core.TextLink("/ui/assets/"+url.PathEscape(output.AssetKey), Text(output.AssetKey))))
	}
	if len(outputs) == 0 {
		outputs = append(outputs, core.ItemListEntry(Text("No outputs linked to this version.")))
	}

	semanticEntrypoints := make([]Node, 0, len(versionDetail.SemanticEntrypoints))
	for i := range versionDetail.SemanticEntrypoints {
		semanticEntrypoints = append(semanticEntrypoints, core.ItemListEntry(Text(versionDetail.SemanticEntrypoints[i].ProjectName+"."+versionDetail.SemanticEntrypoints[i].ModelName)))
	}
	if len(semanticEntrypoints) == 0 {
		semanticEntrypoints = append(semanticEntrypoints, core.ItemListEntry(Text("No semantic entrypoints linked to this version.")))
	}

	return core.AppPage(
		fmt.Sprintf("Product %s v%d", detail.Product.Name, version.Version),
		"products",
		principal,
		core.DetailShell(
			core.DetailHero(
				core.DetailHeroCopy(
					core.Kicker("Version detail"),
					core.DetailTitleRow(
						core.DetailTitle(detail.Product.Name+" v"+strconv.Itoa(version.Version)),
						statusLabel(version.ReleaseState, productPublicationTone(version.ReleaseState)),
					),
					core.DetailDescription("Immutable contract snapshot for the selected product release."),
					core.BadgeRow(
						statusLabel(version.CompatibilityLevel, "accent"),
						statusLabel(fallbackString(detail.Product.Visibility, "internal"), "accent"),
					),
				),
				core.DetailHeroMeta(
					core.MetaItem("Product", detail.Product.Name),
					core.MetaItem("Slug", detail.Product.Slug),
					core.MetaItem("Domain", detail.Domain.Name),
					core.MetaItem("Owner team", detail.OwnerTeam.Name),
				),
			),
			core.DetailLayout(
				core.DetailMain(
					Div(Class(assetSectionCardClass()),
						Div(Class(assetSectionHeadClass()), H2(Class(sectionTitleClass()), Text("Contract snapshot")), P(Class(sectionCopyClass()), Text("Versioned consumer-facing contract fields."))),
						core.FactList([][2]string{
							{"Data grain", fallbackString(version.Contract.DataGrain, "-")},
							{"Update cadence", fallbackString(version.Contract.UpdateCadence, "-")},
							{"Retention", fallbackString(version.Contract.RetentionWindow, "-")},
							{"Freshness SLO", fallbackString(version.SLO.FreshnessSLO, "-")},
							{"Latency SLO", fallbackString(version.SLO.LatencySLO, "-")},
							{"Change policy", fallbackString(version.Contract.BreakingChangePolicy, "-")},
						}),
					),
					Div(Class(assetSectionCardClass()),
						Div(Class(assetSectionHeadClass()), H2(Class(sectionTitleClass()), Text("Outputs")), P(Class(sectionCopyClass()), Text("Runtime assets currently linked to this release snapshot."))),
						core.ItemList("", Group(outputs)),
					),
					Div(Class(assetSectionCardClass()),
						Div(Class(assetSectionHeadClass()), H2(Class(sectionTitleClass()), Text("Semantic entrypoints")), P(Class(sectionCopyClass()), Text("Semantic entrypoints exposed by this release."))),
						core.ItemList("", Group(semanticEntrypoints)),
					),
				),
				core.DetailRail(
					Div(Class(assetSectionCardClass()),
						H2(Class(sectionTitleClass()), Text("Release metadata")),
						core.FactList([][2]string{
							{"Release state", version.ReleaseState},
							{"Compatibility", version.CompatibilityLevel},
							{"Docs", fallbackString(version.DocsURL, "-")},
							{"Access path", fallbackString(version.AccessRequestPath, "-")},
							{"Created by", fallbackString(version.CreatedBy, "-")},
							{"Created at", formatTime(version.CreatedAt)},
						}),
					),
					Div(Class(assetSectionCardClass()),
						H2(Class(sectionTitleClass()), Text("Navigate")),
						P(Class("text-sm text-[var(--fgColor-muted)]"), Text("Inspect the full product control plane or switch versions.")),
						core.SecondaryLink("/ui/products/"+url.PathEscape(detail.Product.Slug), "", Text("Back to product")),
						core.ItemList("mt-3", Group(productVersionLinks(detail))),
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
		links = append(links, core.ItemListEntry(core.TextLink("/ui/products/"+url.PathEscape(detail.Product.Slug)+"/versions/"+strconv.Itoa(version.Version), Text(fmt.Sprintf("v%d %s", version.Version, version.ReleaseState)))))
	}
	if len(links) == 0 {
		links = append(links, core.ItemListEntry(Text("No versions created yet.")))
	}
	return links
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

func fallbackString(value, fallback string) string {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return fallback
	}
	return trimmed
}

func formatTime(ts time.Time) string {
	if ts.IsZero() {
		return "-"
	}
	return ts.Format(time.RFC3339)
}

func formatTimePtr(ts *time.Time) string {
	if ts == nil || ts.IsZero() {
		return "-"
	}
	return ts.Format(time.RFC3339)
}

func containsExpr(value string) string {
	lower := strings.ToLower(value)
	return "$q === '' || " + strconv.Quote(lower) + ".includes($q.toLowerCase())"
}

func quickFilterCardWithValue(placeholder, initialValue string, extraControls ...Node) Node {
	controls := []Node{
		Div(
			Class("flex min-w-[min(20rem,100%)] flex-1 flex-col gap-1"),
			Label(Class("sr-only"), Text("Quick filter")),
			core.InputControl("", Type("search"), Name("q"), Placeholder(placeholder), data.Bind("q"), AutoComplete("off"), Attr("data-quick-filter-input", "true")),
		),
	}
	controls = append(controls, extraControls...)
	syncScript := `(function(){
  var input=document.querySelector('[data-quick-filter-input="true"]');
  if(!(input instanceof HTMLInputElement)){ return; }

  function syncURL(value){
    var url=new URL(window.location.href);
    if(value){
      url.searchParams.set('q', value);
    } else {
      url.searchParams.delete('q');
    }
    url.searchParams.delete('page_token');
    var next=url.pathname;
    var query=url.searchParams.toString();
    if(query){ next+='?'+query; }
    if(next!==window.location.pathname+window.location.search){
      window.history.replaceState({}, '', next);
    }
  }

  input.addEventListener('input', function(){
    syncURL(input.value.trim());
  });
})();`

	return Div(
		Class("rounded-xl border border-[var(--borderColor-default)] bg-[var(--bgColor-default)] p-4 shadow-xs"),
		data.Signals(map[string]any{"q": initialValue}),
		Div(Class("flex flex-wrap items-center gap-3"), Group(controls)),
		Script(Raw(syncScript)),
	)
}

func emptyStateCard(message, ctaLabel, ctaHref string) Node {
	cta := Node(nil)
	if ctaLabel != "" && ctaHref != "" {
		cta = core.PrimaryLink(ctaHref, "", Text(ctaLabel))
	}
	return Div(
		Class("rounded-xl border border-[var(--borderColor-default)] bg-[var(--bgColor-default)] p-4 text-center shadow-xs"),
		Div(Class("mx-auto mb-3 flex h-12 w-12 items-center justify-center rounded-full bg-[var(--bgColor-muted)] text-[var(--fgColor-accent)]"),
			core.Icon("inbox", Class(core.NavIconClass())),
		),
		Div(Class("flex flex-col items-center gap-2 text-center"),
			P(Class("m-0 text-lg font-semibold"), Text("No results yet")),
			P(Class("m-0 text-sm text-[var(--fgColor-muted)]"), Text(message)),
			cta,
		),
	)
}

func paginationCard(basePath string, page domain.PageRequest, total int64) Node {
	shown := min(page.Limit(), int(total))
	summary := fmt.Sprintf("Showing %d of %d entries.", shown, total)
	nextToken := domain.NextPageToken(page.Offset(), page.Limit(), total)
	if nextToken == "" {
		return Div(
			Class("rounded-xl border border-[var(--borderColor-default)] bg-[var(--bgColor-default)] p-4 shadow-xs"),
			Div(
				Class("flex items-center justify-between gap-3 max-sm:flex-col max-sm:items-start"),
				Div(Class("flex min-w-0 flex-col gap-1"),
					P(Class("m-0 text-sm font-semibold text-[var(--fgColor-default)]"), Text("Pagination")),
					P(Class("m-0 text-xs text-[var(--fgColor-muted)]"), Text(summary)),
				),
				Span(Class("inline-flex min-h-8 items-center justify-center rounded-lg border border-[var(--borderColor-default)] px-3 text-sm font-medium text-[var(--fgColor-default)] opacity-60 pointer-events-none"), Attr("aria-disabled", "true"), Text("Next")),
			),
		)
	}
	u := fmt.Sprintf("%s?max_results=%d&page_token=%s", basePath, page.Limit(), nextToken)
	return Div(
		Class("rounded-xl border border-[var(--borderColor-default)] bg-[var(--bgColor-default)] p-4 shadow-xs"),
		Div(
			Class("flex items-center justify-between gap-3 max-sm:flex-col max-sm:items-start"),
			Div(Class("flex min-w-0 flex-col gap-1"),
				P(Class("m-0 text-sm font-semibold text-[var(--fgColor-default)]"), Text("Pagination")),
				P(Class("m-0 text-xs text-[var(--fgColor-muted)]"), Text(summary)),
			),
			core.SecondaryLink(u, "small", Text("Next page")),
		),
	)
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func assetShellClass() string { return "flex flex-col gap-4" }
func assetHeroClass() string {
	return "grid gap-4 rounded-2xl border border-[var(--borderColor-default)] bg-[linear-gradient(135deg,var(--bgColor-muted)_0%,var(--bgColor-default)_65%)] p-5 shadow-sm lg:grid-cols-[minmax(0,1.5fr)_minmax(16rem,0.8fr)]"
}
func assetHeroCopyClass() string { return "flex min-w-0 flex-col gap-3" }
func assetKickerClass() string {
	return "m-0 text-[11px] font-semibold uppercase tracking-[0.08em] text-[var(--fgColor-muted)]"
}
func assetTitleClass() string {
	return "m-0 text-3xl font-semibold leading-tight text-[var(--fgColor-default)]"
}
func assetDescriptionClass() string {
	return "m-0 max-w-3xl text-sm leading-6 text-[var(--fgColor-muted)]"
}
func assetBadgeRowClass() string    { return "flex flex-wrap items-center gap-2" }
func assetHeroActionsClass() string { return "flex flex-wrap items-center gap-3" }
func assetMetricsGridClass() string { return "grid gap-3 sm:grid-cols-2 xl:grid-cols-4" }
func assetTypeBandClass() string {
	return "flex flex-col gap-4 rounded-xl border border-[var(--borderColor-default)] bg-[var(--bgColor-default)] p-4 shadow-xs"
}
func assetShowcaseGridClass() string { return "grid gap-3 lg:grid-cols-2 2xl:grid-cols-3" }
func assetShowcaseCardClass() string {
	return "flex h-full flex-col gap-3 rounded-xl border border-[var(--borderColor-default)] bg-[var(--bgColor-muted)] p-4 text-inherit no-underline shadow-xs transition-colors hover:border-[var(--borderColor-accent-emphasis)] hover:bg-[var(--bgColor-muted)]"
}
func assetShowcaseHeadClass() string { return "flex items-start justify-between gap-3" }
func assetShowcaseKeyClass() string {
	return "m-0 text-base font-semibold text-[var(--fgColor-default)]"
}
func assetShowcaseOwnerClass() string { return "mt-1 mb-0 text-xs text-[var(--fgColor-muted)]" }
func assetShowcaseDescriptionClass() string {
	return "m-0 text-sm leading-6 text-[var(--fgColor-muted)]"
}
func assetSectionHeadClass() string { return "flex flex-wrap items-start justify-between gap-2" }
func assetSectionClass() string     { return "flex flex-col gap-4" }
func assetSectionCardClass() string {
	return "flex flex-col gap-4 border-t border-[var(--borderColor-default)] pt-4 first:border-t-0 first:pt-0"
}
func sectionTitleClass() string { return "m-0 text-lg font-semibold text-[var(--fgColor-default)]" }
func sectionCopyClass() string  { return "m-0 text-sm text-[var(--fgColor-muted)]" }

func labelClass(tone string) string {
	base := "inline-flex items-center rounded-full border border-transparent px-2 py-0.5 text-xs font-medium"
	switch tone {
	case "accent":
		return core.ClassNames(base, "bg-[var(--bgColor-accent-muted)] text-[var(--fgColor-accent)]")
	case "attention":
		return core.ClassNames(base, "bg-[var(--bgColor-attention-muted)] text-[var(--fgColor-attention)]")
	case "success":
		return core.ClassNames(base, "bg-[var(--bgColor-success-muted)] text-[var(--fgColor-success)]")
	case "severe":
		return core.ClassNames(base, "bg-[var(--bgColor-danger-muted)] text-[var(--fgColor-danger)]")
	default:
		return core.ClassNames(base, "bg-[var(--bgColor-muted)] text-[var(--fgColor-default)]")
	}
}

func statusLabel(text, tone string) Node {
	return Span(Class(labelClass(tone)), Text(text))
}
