package security

import (
	"strconv"
	"strings"
	"time"

	"duck-demo/internal/domain"
	"duck-demo/internal/ui/core"

	. "maragu.dev/gomponents"
	data "maragu.dev/gomponents-datastar"
	. "maragu.dev/gomponents/html"
)

type securityCardData struct {
	Title       string
	Description string
	Href        string
	LinkLabel   string
}

func securityHomePage(principal domain.ContextPrincipal, cards []securityCardData) Node {
	nodes := make([]Node, 0, len(cards))
	for i := range cards {
		card := cards[i]
		nodes = append(nodes, core.Card(H2(Text(card.Title)), P(Text(card.Description)), core.TextLink(card.Href, Text(card.LinkLabel))))
	}
	return core.AppPage("Security", "security", principal, Div(Class("grid gap-3 md:grid-cols-2 xl:grid-cols-3"), Group(nodes)))
}

func securitySectionNav(active string) Node {
	return core.SectionTabs([]core.SectionTab{
		{Label: "Principals", Href: "/ui/security/principals", Active: active == "principals"},
		{Label: "Groups", Href: "/ui/security/groups", Active: active == "groups"},
		{Label: "Grants", Href: "/ui/security/grants", Active: active == "grants"},
		{Label: "Row Filters", Href: "/ui/security/row-filters", Active: active == "row-filters"},
		{Label: "Column Masks", Href: "/ui/security/column-masks", Active: active == "column-masks"},
		{Label: "API Keys", Href: "/ui/security/api-keys", Active: active == "api-keys"},
	})
}

type securityPrincipalRowData struct {
	Filter    string
	ID        string
	Name      string
	Type      string
	IsAdmin   bool
	CreatedAt string
	DetailURL string
}

func securityPrincipalsListPage(principal domain.ContextPrincipal, rows []securityPrincipalRowData, page domain.PageRequest, total int64) Node {
	tableRows := make([]Node, 0, len(rows))
	for i := range rows {
		row := rows[i]
		admin := statusLabel("user", "muted")
		if row.IsAdmin {
			admin = statusLabel("admin", "accent")
		}
		tableRows = append(tableRows, Tr(data.Show(containsExpr(row.Filter)), Td(core.TextLink(row.DetailURL, Text(row.Name))), Td(Text(row.Type)), Td(admin), Td(Text(row.CreatedAt))))
	}
	tableNode := Node(emptyStateCard("No principals found.", "New principal", "/ui/security/principals/new"))
	if len(tableRows) > 0 {
		tableNode = core.TableContainer("", core.DataTable("", THead(Tr(Th(Text("Name")), Th(Text("Type")), Th(Text("Role")), Th(Text("Created")))), TBody(Group(tableRows))))
	}

	return core.AppPage("Security: Principals", "security", principal,
		securitySectionNav("principals"),
		core.PageHeader("Operate", "Security principals", "Browse identities first, then drill into grants and API credentials from the detail workspace.", core.PrimaryLink("/ui/security/principals/new", "", Text("New principal"))),
		quickFilterCard("Filter by principal name or type"),
		tableNode,
		paginationCard("/ui/security/principals", page, total),
	)
}

type securityPrincipalDetailPageData struct {
	Principal         domain.ContextPrincipal
	Item              *domain.Principal
	Grants            []domain.PrivilegeGrant
	APIKeys           []domain.APIKey
	CSRFFieldProvider func() Node
}

func securityPrincipalDetailPage(d securityPrincipalDetailPageData) Node {
	grantRows := make([]Node, 0, len(d.Grants))
	for i := range d.Grants {
		grant := d.Grants[i]
		grantRows = append(grantRows, Tr(Td(Text(grant.Privilege)), Td(Text(grant.SecurableType)), Td(Text(grant.SecurableID)), Td(Text(formatTime(grant.GrantedAt))), Td(Class("text-right"), core.ActionMenu("Actions", actionMenuPost("/ui/security/grants/"+grant.ID+"/delete", "Delete grant", d.CSRFFieldProvider, true)))))
	}

	keyRows := make([]Node, 0, len(d.APIKeys))
	for i := range d.APIKeys {
		key := d.APIKeys[i]
		keyRows = append(keyRows, Tr(Td(Text(key.Name)), Td(Text(key.KeyPrefix)), Td(Text(formatTimePtr(key.ExpiresAt))), Td(Text(formatTime(key.CreatedAt))), Td(Class("text-right"), core.ActionMenu("Actions", actionMenuPost("/ui/security/api-keys/"+key.ID+"/delete", "Delete API key", d.CSRFFieldProvider, true)))))
	}

	roleLabel := statusLabel("user", "muted")
	adminButtonLabel := "Grant admin"
	adminValue := "true"
	if d.Item.IsAdmin {
		roleLabel = statusLabel("admin", "accent")
		adminButtonLabel = "Revoke admin"
		adminValue = "false"
	}

	return core.AppPage(
		"Security: "+d.Item.Name,
		"security",
		d.Principal,
		securitySectionNav("principals"),
		core.DetailShell(
			core.DetailHero(
				core.DetailHeroCopy(
					core.Kicker("Identity summary"),
					core.DetailTitle(d.Item.Name),
					core.DetailDescription("Start with who this principal is, then inspect the grants and programmatic access attached to it."),
					core.BadgeRow(roleLabel, core.Badge(d.Item.Type, "")),
				),
				core.DetailHeroMeta(
					core.MetaItem("Principal ID", d.Item.ID),
					core.MetaItem("Created", formatTime(d.Item.CreatedAt)),
					core.MetaItem("External subject", strOrDash(d.Item.ExternalID)),
					core.MetaItem("External issuer", strOrDash(d.Item.ExternalIssuer)),
				),
			),
			core.DetailLayout(
				core.DetailMain(
					core.SectionSurface(
						core.SectionHeader("Privilege grants", "Effective privileges assigned to this principal."),
						core.TableContainer("", core.DataTable("", THead(Tr(Th(Text("Privilege")), Th(Text("Securable type")), Th(Text("Securable ID")), Th(Text("Granted")), Th(Class("text-right"), Text("Actions")))), TBody(Group(grantRows)))),
					),
					core.SectionSurface(
						core.SectionHeader("API keys", "Programmatic access credentials owned by this principal."),
						core.TableContainer("", core.DataTable("", THead(Tr(Th(Text("Name")), Th(Text("Prefix")), Th(Text("Expires")), Th(Text("Created")), Th(Class("text-right"), Text("Actions")))), TBody(Group(keyRows)))),
					),
				),
				core.DetailRail(
					core.SectionSurface(
						core.SectionHeader("Actions", "Administrative changes stay separate from the record tables."),
						core.DetailSummaryList([][2]string{
							{"Type", d.Item.Type},
							{"Role", func() string {
								if d.Item.IsAdmin {
									return "admin"
								}
								return "user"
							}()},
						}),
						Div(Class("flex flex-wrap items-center gap-3 [&_form]:m-0 [&_form]:inline-flex"),
							Form(Method("post"), Action("/ui/security/principals/"+d.Item.ID+"/admin"), d.CSRFFieldProvider(), Input(Type("hidden"), Name("is_admin"), Value(adminValue)), core.PrimaryButton("", Type("submit"), Text(adminButtonLabel))),
							Form(Method("post"), Action("/ui/security/principals/"+d.Item.ID+"/delete"), d.CSRFFieldProvider(), core.DangerButton("", Type("submit"), Text("Delete principal"))),
							core.SecondaryLink("/ui/security/api-keys?principal_id="+d.Item.ID, "", Text("View API keys")),
						),
					),
				),
			),
		),
	)
}

func securityPrincipalFormPage(principal domain.ContextPrincipal, csrfFieldProvider func() Node) Node {
	return formPage(principal, "New Principal", "security", "/ui/security/principals", csrfFieldProvider,
		core.FieldLabel("Name"),
		core.InputControl("", Name("name"), Required()),
		core.FieldLabel("Type"),
		core.SelectControl("", Name("type"), Option(Value("user"), Text("user")), Option(Value("service_principal"), Text("service_principal"))),
		core.FieldLabel("Admin"),
		core.Checkbox("principal-is-admin", "is_admin", "true", "Create with admin access", false),
	)
}

type securityGroupRowData struct {
	Filter    string
	ID        string
	Name      string
	Members   string
	CreatedAt string
	DetailURL string
}

func securityGroupsListPage(principal domain.ContextPrincipal, rows []securityGroupRowData, page domain.PageRequest, total int64) Node {
	tableRows := make([]Node, 0, len(rows))
	for i := range rows {
		row := rows[i]
		tableRows = append(tableRows, Tr(data.Show(containsExpr(row.Filter)), Td(core.TextLink(row.DetailURL, Text(row.Name))), Td(Text(row.Members)), Td(Text(row.CreatedAt))))
	}
	tableNode := Node(emptyStateCard("No groups found.", "New group", "/ui/security/groups/new"))
	if len(tableRows) > 0 {
		tableNode = core.TableContainer("", core.DataTable("", THead(Tr(Th(Text("Name")), Th(Text("Members")), Th(Text("Created")))), TBody(Group(tableRows))))
	}
	return core.AppPage("Security: Groups", "security", principal,
		securitySectionNav("groups"),
		core.PageHeader("Operate", "Security groups", "Manage memberships and group-level grants from the same operational surface.", core.PrimaryLink("/ui/security/groups/new", "", Text("New group"))),
		quickFilterCard("Filter by group name"),
		tableNode,
		paginationCard("/ui/security/groups", page, total),
	)
}

type securityGroupMemberRowData struct {
	GroupID    string
	MemberID   string
	MemberType string
	CSRFField  func() Node
}

type securityGroupDetailPageData struct {
	Principal         domain.ContextPrincipal
	Item              *domain.Group
	Members           []securityGroupMemberRowData
	Grants            []domain.PrivilegeGrant
	CSRFFieldProvider func() Node
}

func securityGroupDetailPage(d securityGroupDetailPageData) Node {
	memberRows := make([]Node, 0, len(d.Members))
	for i := range d.Members {
		member := d.Members[i]
		memberRows = append(memberRows, Tr(Td(Text(member.MemberID)), Td(Text(member.MemberType)), Td(Class("text-right"), Form(Method("post"), Action("/ui/security/groups/"+member.GroupID+"/members/delete"), member.CSRFField(), Input(Type("hidden"), Name("member_id"), Value(member.MemberID)), Input(Type("hidden"), Name("member_type"), Value(member.MemberType)), core.DangerButton("", Type("submit"), Text("Remove"))))))
	}

	grantRows := make([]Node, 0, len(d.Grants))
	for i := range d.Grants {
		grant := d.Grants[i]
		grantRows = append(grantRows, Tr(Td(Text(grant.Privilege)), Td(Text(grant.SecurableType)), Td(Text(grant.SecurableID)), Td(Class("text-right"), core.ActionMenu("Actions", actionMenuPost("/ui/security/grants/"+grant.ID+"/delete", "Delete grant", d.CSRFFieldProvider, true)))))
	}

	return core.AppPage(
		"Security: "+d.Item.Name,
		"security",
		d.Principal,
		securitySectionNav("groups"),
		core.DetailShell(
			core.DetailHero(
				core.DetailHeroCopy(
					core.Kicker("Group summary"),
					core.DetailTitle(d.Item.Name),
					core.DetailDescription("Use the main column for membership and grants, with group-level actions and member creation kept in the side rail."),
				),
				core.DetailHeroMeta(
					core.MetaItem("Group ID", d.Item.ID),
					core.MetaItem("Created", formatTime(d.Item.CreatedAt)),
					core.MetaItem("Description", dashIfEmpty(d.Item.Description)),
				),
			),
			core.DetailLayout(
				core.DetailMain(
					core.SectionSurface(
						core.SectionHeader("Members", "Current group membership."),
						core.TableContainer("", core.DataTable("", THead(Tr(Th(Text("Member ID")), Th(Text("Type")), Th(Class("text-right"), Text("Actions")))), TBody(Group(memberRows)))),
					),
					core.SectionSurface(
						core.SectionHeader("Privilege grants", "Privileges inherited through this group."),
						core.TableContainer("", core.DataTable("", THead(Tr(Th(Text("Privilege")), Th(Text("Securable type")), Th(Text("Securable ID")), Th(Class("text-right"), Text("Actions")))), TBody(Group(grantRows)))),
					),
				),
				core.DetailRail(
					core.SectionSurface(
						core.SectionHeader("Actions", "Administrative controls stay separate from the member and grant tables."),
						Div(Class("flex flex-wrap items-center gap-3 [&_form]:m-0 [&_form]:inline-flex"),
							Form(Method("post"), Action("/ui/security/groups/"+d.Item.ID+"/delete"), d.CSRFFieldProvider(), core.DangerButton("", Type("submit"), Text("Delete group"))),
						),
					),
					core.SectionSurface(
						core.SectionHeader("Add member", "Add a user or another group to this membership set."),
						Form(Class("stack-form [&>:not(label):not(.form-actions)]:mb-3 [&>:last-child]:mb-0"), Method("post"), Action("/ui/security/groups/"+d.Item.ID+"/members"), d.CSRFFieldProvider(),
							Label(Text("Member type")),
							core.SelectControl("", Name("member_type"), Option(Value("user"), Text("user")), Option(Value("group"), Text("group"))),
							Label(Text("Member ID")),
							core.InputControl("", Name("member_id"), Required()),
							Div(Class("form-actions mt-2"), core.PrimaryButton("", Type("submit"), Text("Add member"))),
						),
					),
				),
			),
		),
	)
}

func securityGroupFormPage(principal domain.ContextPrincipal, csrfFieldProvider func() Node) Node {
	return formPage(principal, "New Group", "security", "/ui/security/groups", csrfFieldProvider,
		core.FieldLabel("Name"),
		core.InputControl("", Name("name"), Required()),
		core.FieldLabel("Description"),
		core.TextareaControl("", Name("description")),
	)
}

func formPage(principal domain.ContextPrincipal, title, active, action string, csrfFieldProvider func() Node, fields ...Node) Node {
	nodes := []Node{csrfFieldProvider()}
	nodes = append(nodes, fields...)
	return core.AppPage(title, active, principal,
		core.SectionSurface(
			core.SectionHeader(title, "Create or update the selected security record."),
			Form(Class("stack-form [&>:not(label):not(.form-actions)]:mb-3 [&>:last-child]:mb-0"), Method("post"), Action(action), Group(nodes), Div(Class("form-actions mt-2"), core.PrimaryButton("", Type("submit"), Text("Save")))),
		),
	)
}

func pageToolbar(newHref, newLabel string) Node {
	return Div(Class("flex flex-wrap items-center justify-between gap-3 border-b border-[var(--borderColor-default)] pb-3"),
		Div(Class("flex min-w-0 flex-col gap-1"), Span(Class(labelClass("")), Text("Workspace")), P(Class("m-0 text-xs text-[var(--fgColor-muted)]"), Text("Browse and manage resources."))),
		core.PrimaryLink(newHref, "", Text(newLabel)),
	)
}

func quickFilterCard(placeholder string, extraControls ...Node) Node {
	return quickFilterCardWithValue(placeholder, "", extraControls...)
}

func quickFilterCardWithValue(placeholder, initialValue string, extraControls ...Node) Node {
	controls := []Node{Div(Class("flex min-w-[min(20rem,100%)] flex-1 flex-col gap-1"), Label(Class("sr-only"), Text("Quick filter")), core.InputControl("", Type("search"), Name("q"), Placeholder(placeholder), data.Bind("q"), AutoComplete("off"), Attr("data-quick-filter-input", "true")))}
	controls = append(controls, extraControls...)
	syncScript := `(function(){var input=document.querySelector('[data-quick-filter-input="true"]');if(!(input instanceof HTMLInputElement)){ return; }function syncURL(value){var url=new URL(window.location.href);if(value){url.searchParams.set('q', value);} else {url.searchParams.delete('q');}url.searchParams.delete('page_token');var next=url.pathname;var query=url.searchParams.toString();if(query){ next+='?'+query; }if(next!==window.location.pathname+window.location.search){window.history.replaceState({}, '', next);}}input.addEventListener('input', function(){syncURL(input.value.trim());});})();`
	return Div(Class("grid gap-3 border-b border-[var(--borderColor-default)] pb-3"), data.Signals(map[string]any{"q": initialValue}), Div(Class("flex flex-wrap items-center gap-3"), Group(controls)), Script(Raw(syncScript)))
}

func paginationCard(basePath string, page domain.PageRequest, total int64) Node {
	shown := min(page.Limit(), int(total))
	summary := "Showing " + strconv.Itoa(shown) + " of " + strconv.FormatInt(total, 10) + " entries."
	nextToken := domain.NextPageToken(page.Offset(), page.Limit(), total)
	if nextToken == "" {
		return Div(Class("flex items-center justify-between gap-3 border-t border-[var(--borderColor-default)] pt-3 max-sm:flex-col max-sm:items-start"), Div(Class("flex min-w-0 flex-col gap-1"), P(Class("m-0 text-sm font-semibold text-[var(--fgColor-default)]"), Text("Pagination")), P(Class("m-0 text-xs text-[var(--fgColor-muted)]"), Text(summary))), Span(Class("inline-flex min-h-8 items-center justify-center rounded-lg border border-[var(--borderColor-default)] px-3 text-sm font-medium text-[var(--fgColor-default)] opacity-60 pointer-events-none"), Attr("aria-disabled", "true"), Text("Next")))
	}
	u := basePath + "?max_results=" + strconv.Itoa(page.Limit()) + "&page_token=" + nextToken
	return Div(Class("flex items-center justify-between gap-3 border-t border-[var(--borderColor-default)] pt-3 max-sm:flex-col max-sm:items-start"), Div(Class("flex min-w-0 flex-col gap-1"), P(Class("m-0 text-sm font-semibold text-[var(--fgColor-default)]"), Text("Pagination")), P(Class("m-0 text-xs text-[var(--fgColor-muted)]"), Text(summary))), core.SecondaryLink(u, "small", Text("Next page")))
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func labelClass(tone string) string {
	base := "inline-flex items-center rounded-full border border-transparent px-2 py-0.5 text-xs font-medium"
	switch tone {
	case "accent":
		return core.ClassNames(base, "bg-[var(--bgColor-accent-muted)] text-[var(--fgColor-accent)]")
	case "muted":
		return core.ClassNames(base, "bg-[var(--bgColor-muted)] text-[var(--fgColor-default)]")
	case "success":
		return core.ClassNames(base, "bg-[var(--bgColor-success-muted)] text-[var(--fgColor-success)]")
	case "attention":
		return core.ClassNames(base, "bg-[var(--bgColor-attention-muted)] text-[var(--fgColor-attention)]")
	case "severe":
		return core.ClassNames(base, "bg-[var(--bgColor-danger-muted)] text-[var(--fgColor-danger)]")
	default:
		return core.ClassNames(base, "bg-[var(--bgColor-muted)] text-[var(--fgColor-default)]")
	}
}

func statusLabel(text, tone string) Node { return Span(Class(labelClass(tone)), Text(text)) }

func actionMenuPost(action, label string, csrfField func() Node, danger bool) Node {
	return core.ActionMenuPost(action, label, csrfField, danger)
}

func emptyStateCard(message, ctaLabel, ctaHref string) Node {
	cta := Node(nil)
	if ctaLabel != "" && ctaHref != "" {
		cta = core.PrimaryLink(ctaHref, "", Text(ctaLabel))
	}
	return core.EmptyState("inbox", "No results yet", message, cta)
}

func containsExpr(value string) string {
	lower := strings.ToLower(value)
	return "$q === '' || " + strconv.Quote(lower) + ".includes($q.toLowerCase())"
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

func dashIfEmpty(v string) string {
	if v == "" {
		return "-"
	}
	return v
}

func strOrDash(v *string) string {
	if v == nil || *v == "" {
		return "-"
	}
	return *v
}
