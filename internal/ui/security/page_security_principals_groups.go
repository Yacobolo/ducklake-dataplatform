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
		nodes = append(nodes, Div(Class(core.CardClass()), H2(Text(card.Title)), P(Text(card.Description)), A(Href(card.Href), Text(card.LinkLabel))))
	}
	return core.AppPage("Security", "security", principal, Div(Class("grid gap-3 md:grid-cols-2 xl:grid-cols-3"), Group(nodes)))
}

func securitySectionNav(active string) Node {
	links := []struct {
		key   string
		label string
		href  string
	}{
		{key: "principals", label: "Principals", href: "/ui/security/principals"},
		{key: "groups", label: "Groups", href: "/ui/security/groups"},
		{key: "grants", label: "Grants", href: "/ui/security/grants"},
		{key: "row-filters", label: "Row Filters", href: "/ui/security/row-filters"},
		{key: "column-masks", label: "Column Masks", href: "/ui/security/column-masks"},
		{key: "api-keys", label: "API Keys", href: "/ui/security/api-keys"},
	}
	nodes := make([]Node, 0, len(links))
	for i := range links {
		link := links[i]
		className := core.SecondaryButtonClass()
		if link.key == active {
			className = core.PrimaryButtonClass()
		}
		nodes = append(nodes, A(Href(link.href), Class(className), Text(link.label)))
	}
	return Div(Class(core.CardClass()), Div(Class("flex flex-wrap gap-2"), Group(nodes)))
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
		tableRows = append(tableRows, Tr(data.Show(containsExpr(row.Filter)), Td(A(Href(row.DetailURL), Text(row.Name))), Td(Text(row.Type)), Td(admin), Td(Text(row.CreatedAt))))
	}
	tableNode := Node(emptyStateCard("No principals found.", "New principal", "/ui/security/principals/new"))
	if len(tableRows) > 0 {
		tableNode = Div(Class(core.CardClass("overflow-x-auto")), Table(Class(dataTableClass()), THead(Tr(Th(Text("Name")), Th(Text("Type")), Th(Text("Role")), Th(Text("Created")))), TBody(Group(tableRows))))
	}

	return core.AppPage("Security: Principals", "security", principal, securitySectionNav("principals"), pageToolbar("/ui/security/principals/new", "New principal"), quickFilterCard("Filter by principal name or type"), tableNode, paginationCard("/ui/security/principals", page, total))
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
		grantRows = append(grantRows, Tr(Td(Text(grant.Privilege)), Td(Text(grant.SecurableType)), Td(Text(grant.SecurableID)), Td(Text(formatTime(grant.GrantedAt))), Td(Class("text-right"), actionMenu("Actions", actionMenuPost("/ui/security/grants/"+grant.ID+"/delete", "Delete grant", d.CSRFFieldProvider, true)))))
	}

	keyRows := make([]Node, 0, len(d.APIKeys))
	for i := range d.APIKeys {
		key := d.APIKeys[i]
		keyRows = append(keyRows, Tr(Td(Text(key.Name)), Td(Text(key.KeyPrefix)), Td(Text(formatTimePtr(key.ExpiresAt))), Td(Text(formatTime(key.CreatedAt))), Td(Class("text-right"), actionMenu("Actions", actionMenuPost("/ui/security/api-keys/"+key.ID+"/delete", "Delete API key", d.CSRFFieldProvider, true)))))
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
		Div(Class(core.CardClass()),
			P(Text("Principal ID: "+d.Item.ID)),
			P(Text("Type: "+d.Item.Type)),
			P(Text("Role: "), roleLabel),
			P(Text("External subject: "+strOrDash(d.Item.ExternalID))),
			P(Text("External issuer: "+strOrDash(d.Item.ExternalIssuer))),
			P(Text("Created: "+formatTime(d.Item.CreatedAt))),
			Div(Class(core.ButtonRowClass()),
				Form(Method("post"), Action("/ui/security/principals/"+d.Item.ID+"/admin"), d.CSRFFieldProvider(), Input(Type("hidden"), Name("is_admin"), Value(adminValue)), Button(Type("submit"), Class(core.PrimaryButtonClass()), Text(adminButtonLabel))),
				Form(Method("post"), Action("/ui/security/principals/"+d.Item.ID+"/delete"), d.CSRFFieldProvider(), Button(Type("submit"), Class(core.DangerButtonClass()), Text("Delete principal"))),
				A(Href("/ui/security/api-keys?principal_id="+d.Item.ID), Class(core.SecondaryButtonClass()), Text("View API keys")),
			),
		),
		Div(Class(core.CardClass("overflow-x-auto")), H2(Text("Privilege grants")), Table(Class(dataTableClass()), THead(Tr(Th(Text("Privilege")), Th(Text("Securable type")), Th(Text("Securable ID")), Th(Text("Granted")), Th(Class("text-right"), Text("Actions")))), TBody(Group(grantRows)))),
		Div(Class(core.CardClass("overflow-x-auto")), H2(Text("API keys")), Table(Class(dataTableClass()), THead(Tr(Th(Text("Name")), Th(Text("Prefix")), Th(Text("Expires")), Th(Text("Created")), Th(Class("text-right"), Text("Actions")))), TBody(Group(keyRows)))),
	)
}

func securityPrincipalFormPage(principal domain.ContextPrincipal, csrfFieldProvider func() Node) Node {
	return formPage(principal, "New Principal", "security", "/ui/security/principals", csrfFieldProvider,
		Label(Text("Name")),
		Input(Name("name"), Required()),
		Label(Text("Type")),
		Select(Name("type"), Option(Value("user"), Text("user")), Option(Value("service_principal"), Text("service_principal"))),
		Label(Text("Admin")),
		Label(Input(Type("checkbox"), Name("is_admin")), Text(" Create with admin access")),
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
		tableRows = append(tableRows, Tr(data.Show(containsExpr(row.Filter)), Td(A(Href(row.DetailURL), Text(row.Name))), Td(Text(row.Members)), Td(Text(row.CreatedAt))))
	}
	tableNode := Node(emptyStateCard("No groups found.", "New group", "/ui/security/groups/new"))
	if len(tableRows) > 0 {
		tableNode = Div(Class(core.CardClass("overflow-x-auto")), Table(Class(dataTableClass()), THead(Tr(Th(Text("Name")), Th(Text("Members")), Th(Text("Created")))), TBody(Group(tableRows))))
	}
	return core.AppPage("Security: Groups", "security", principal, securitySectionNav("groups"), pageToolbar("/ui/security/groups/new", "New group"), quickFilterCard("Filter by group name"), tableNode, paginationCard("/ui/security/groups", page, total))
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
		memberRows = append(memberRows, Tr(Td(Text(member.MemberID)), Td(Text(member.MemberType)), Td(Class("text-right"), Form(Method("post"), Action("/ui/security/groups/"+member.GroupID+"/members/delete"), member.CSRFField(), Input(Type("hidden"), Name("member_id"), Value(member.MemberID)), Input(Type("hidden"), Name("member_type"), Value(member.MemberType)), Button(Type("submit"), Class(core.DangerButtonClass()), Text("Remove"))))))
	}

	grantRows := make([]Node, 0, len(d.Grants))
	for i := range d.Grants {
		grant := d.Grants[i]
		grantRows = append(grantRows, Tr(Td(Text(grant.Privilege)), Td(Text(grant.SecurableType)), Td(Text(grant.SecurableID)), Td(Class("text-right"), actionMenu("Actions", actionMenuPost("/ui/security/grants/"+grant.ID+"/delete", "Delete grant", d.CSRFFieldProvider, true)))))
	}

	return core.AppPage(
		"Security: "+d.Item.Name,
		"security",
		d.Principal,
		securitySectionNav("groups"),
		Div(Class(core.CardClass()),
			P(Text("Group ID: "+d.Item.ID)),
			P(Text("Description: "+dashIfEmpty(d.Item.Description))),
			P(Text("Created: "+formatTime(d.Item.CreatedAt))),
			Div(Class(core.ButtonRowClass()),
				Form(Method("post"), Action("/ui/security/groups/"+d.Item.ID+"/delete"), d.CSRFFieldProvider(), Button(Type("submit"), Class(core.DangerButtonClass()), Text("Delete group"))),
			),
		),
		Div(Class(core.CardClass()),
			H2(Text("Add member")),
			Form(Class("stack-form [&>:not(label):not(.form-actions)]:mb-3 [&>:last-child]:mb-0"), Method("post"), Action("/ui/security/groups/"+d.Item.ID+"/members"), d.CSRFFieldProvider(),
				Label(Text("Member type")),
				Select(Name("member_type"), Option(Value("user"), Text("user")), Option(Value("group"), Text("group"))),
				Label(Text("Member ID")),
				Input(Name("member_id"), Required()),
				Div(Class("form-actions mt-2"), Button(Type("submit"), Class(core.PrimaryButtonClass()), Text("Add member"))),
			),
		),
		Div(Class(core.CardClass("overflow-x-auto")), H2(Text("Members")), Table(Class(dataTableClass()), THead(Tr(Th(Text("Member ID")), Th(Text("Type")), Th(Class("text-right"), Text("Actions")))), TBody(Group(memberRows)))),
		Div(Class(core.CardClass("overflow-x-auto")), H2(Text("Privilege grants")), Table(Class(dataTableClass()), THead(Tr(Th(Text("Privilege")), Th(Text("Securable type")), Th(Text("Securable ID")), Th(Class("text-right"), Text("Actions")))), TBody(Group(grantRows)))),
	)
}

func securityGroupFormPage(principal domain.ContextPrincipal, csrfFieldProvider func() Node) Node {
	return formPage(principal, "New Group", "security", "/ui/security/groups", csrfFieldProvider,
		Label(Text("Name")),
		Input(Name("name"), Required()),
		Label(Text("Description")),
		Textarea(Name("description")),
	)
}

func formPage(principal domain.ContextPrincipal, title, active, action string, csrfFieldProvider func() Node, fields ...Node) Node {
	nodes := []Node{csrfFieldProvider()}
	nodes = append(nodes, fields...)
	return core.AppPage(title, active, principal,
		Div(Class(core.CardClass()),
			Form(Class("stack-form [&>:not(label):not(.form-actions)]:mb-3 [&>:last-child]:mb-0"), Method("post"), Action(action), Group(nodes), Div(Class("form-actions mt-2"), Button(Type("submit"), Class(core.PrimaryButtonClass()), Text("Save")))),
		),
	)
}

func dataTableClass(extra ...string) string {
	return core.ClassNames("min-w-full border-collapse overflow-hidden rounded-xl border border-[var(--borderColor-default)] bg-[var(--bgColor-default)] [&_tbody_tr:hover]:bg-[var(--control-bgColor-hover)] [&_td]:border-b [&_td]:border-[var(--borderColor-default)] [&_td]:px-4 [&_td]:py-3 [&_td]:align-top [&_td]:text-[0.8125rem] [&_th]:sticky [&_th]:top-0 [&_th]:z-[1] [&_th]:border-b [&_th]:border-[var(--borderColor-default)] [&_th]:bg-[var(--bgColor-muted)] [&_th]:px-4 [&_th]:py-3 [&_th]:text-left [&_th]:text-[0.8125rem] [&_th]:font-semibold [&_th]:uppercase [&_th]:tracking-[0.02em] [&_th]:text-[var(--fgColor-muted)]", strings.Join(extra, " "))
}

func pageToolbar(newHref, newLabel string) Node {
	return Div(Class(core.CardClass()), Div(Class("flex flex-wrap items-center justify-between gap-3"),
		Div(Class("flex min-w-0 flex-col gap-1"), Span(Class(labelClass("")), Text("Workspace")), P(Class("m-0 text-xs text-[var(--fgColor-muted)]"), Text("Browse and manage resources."))),
		A(Href(newHref), Class(core.PrimaryButtonClass()), Text(newLabel)),
	))
}

func quickFilterCard(placeholder string, extraControls ...Node) Node {
	return quickFilterCardWithValue(placeholder, "", extraControls...)
}

func quickFilterCardWithValue(placeholder, initialValue string, extraControls ...Node) Node {
	controls := []Node{Div(Class("flex min-w-[min(20rem,100%)] flex-1 flex-col gap-1"), Label(Class("sr-only"), Text("Quick filter")), Input(Type("search"), Class(core.FormControlClass()), Name("q"), Placeholder(placeholder), data.Bind("q"), AutoComplete("off"), Attr("data-quick-filter-input", "true")))}
	controls = append(controls, extraControls...)
	syncScript := `(function(){var input=document.querySelector('[data-quick-filter-input="true"]');if(!(input instanceof HTMLInputElement)){ return; }function syncURL(value){var url=new URL(window.location.href);if(value){url.searchParams.set('q', value);} else {url.searchParams.delete('q');}url.searchParams.delete('page_token');var next=url.pathname;var query=url.searchParams.toString();if(query){ next+='?'+query; }if(next!==window.location.pathname+window.location.search){window.history.replaceState({}, '', next);}}input.addEventListener('input', function(){syncURL(input.value.trim());});})();`
	return Div(Class(core.CardClass()), data.Signals(map[string]any{"q": initialValue}), Div(Class("flex flex-wrap items-center gap-3"), Group(controls)), Script(Raw(syncScript)))
}

func paginationCard(basePath string, page domain.PageRequest, total int64) Node {
	shown := min(page.Limit(), int(total))
	summary := "Showing " + strconv.Itoa(shown) + " of " + strconv.FormatInt(total, 10) + " entries."
	nextToken := domain.NextPageToken(page.Offset(), page.Limit(), total)
	if nextToken == "" {
		return Div(Class(core.CardClass()), Div(Class("flex items-center justify-between gap-3 max-sm:flex-col max-sm:items-start"), Div(Class("flex min-w-0 flex-col gap-1"), P(Class("m-0 text-sm font-semibold text-[var(--fgColor-default)]"), Text("Pagination")), P(Class("m-0 text-xs text-[var(--fgColor-muted)]"), Text(summary))), Span(Class(core.ClassNames(core.SecondaryButtonClass("small"), "pointer-events-none opacity-60")), Attr("aria-disabled", "true"), Text("Next"))))
	}
	u := basePath + "?max_results=" + strconv.Itoa(page.Limit()) + "&page_token=" + nextToken
	return Div(Class(core.CardClass()), Div(Class("flex items-center justify-between gap-3 max-sm:flex-col max-sm:items-start"), Div(Class("flex min-w-0 flex-col gap-1"), P(Class("m-0 text-sm font-semibold text-[var(--fgColor-default)]"), Text("Pagination")), P(Class("m-0 text-xs text-[var(--fgColor-muted)]"), Text(summary))), A(Href(u), Class(core.SecondaryButtonClass("small")), Text("Next page"))))
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
		return core.ClassNames(base, "bg-[var(--label-blue-bgColor-rest)] text-[var(--label-blue-fgColor-rest)]")
	case "muted":
		return core.ClassNames(base, "bg-[var(--label-gray-bgColor-rest)] text-[var(--label-gray-fgColor-rest)]")
	case "success":
		return core.ClassNames(base, "bg-[var(--label-green-bgColor-rest)] text-[var(--label-green-fgColor-rest)]")
	case "attention":
		return core.ClassNames(base, "bg-[var(--label-yellow-bgColor-rest)] text-[var(--label-yellow-fgColor-rest)]")
	case "severe":
		return core.ClassNames(base, "bg-[var(--label-orange-bgColor-rest)] text-[var(--label-orange-fgColor-rest)]")
	default:
		return core.ClassNames(base, "bg-[var(--label-gray-bgColor-rest)] text-[var(--label-gray-fgColor-rest)]")
	}
}

func statusLabel(text, tone string) Node { return Span(Class(labelClass(tone)), Text(text)) }

func actionMenu(label string, items ...Node) Node {
	summaryClass := core.SecondaryButtonClass("small")
	summaryContent := Node(Text(label))
	if label == "More" || label == "Actions" {
		summaryClass = core.IconButtonClass("small")
		summaryContent = Group([]Node{I(Class(core.IconGlyphClass()), Attr("data-lucide", "ellipsis"), Attr("aria-hidden", "true")), Span(Class("sr-only"), Text(label))})
	}
	return Details(Class("relative inline-block"), Summary(Class(core.ClassNames("list-none [&::-webkit-details-marker]:hidden", summaryClass)), Title(label), Attr("aria-label", label), summaryContent), Div(Class("absolute right-0 top-full z-20 mt-1 min-w-[var(--overlay-width-xsmall)] rounded-xl border border-[var(--overlay-borderColor)] bg-[var(--overlay-bgColor)] p-1 shadow-[var(--shadow-floating-small)]"), Group(items)))
}

func actionMenuPost(action, label string, csrfField func() Node, danger bool) Node {
	btnClass := "flex w-full items-center gap-2 rounded-lg px-3 py-2 text-left text-[0.8125rem] no-underline hover:bg-[var(--control-bgColor-hover)]"
	if danger {
		btnClass += " text-[var(--fgColor-danger)] hover:bg-[var(--bgColor-danger-muted)]"
	} else {
		btnClass += " text-[var(--fgColor-default)]"
	}
	button := Form(Method("post"), Action(action), csrfField(), Button(Type("submit"), Class(btnClass), Span(Text(label))))
	if danger {
		return Group([]Node{Div(Class("dropdown-divider my-1 border-t border-[var(--borderColor-muted)]")), button})
	}
	return button
}

func emptyStateCard(message, ctaLabel, ctaHref string) Node {
	cta := Node(nil)
	if ctaLabel != "" && ctaHref != "" {
		cta = A(Href(ctaHref), Class(core.PrimaryButtonClass()), Text(ctaLabel))
	}
	return Div(Class(core.CardClass("text-center")), Div(Class("mx-auto mb-3 flex h-12 w-12 items-center justify-center rounded-full bg-[var(--bgColor-muted)] text-[var(--fgColor-accent)]"), I(Class(core.NavIconClass()), Attr("data-lucide", "inbox"), Attr("aria-hidden", "true"))), Div(Class("flex flex-col items-center gap-2 text-center"), P(Class("m-0 text-lg font-semibold"), Text("No results yet")), P(Class("m-0 text-sm text-[var(--fgColor-muted)]"), Text(message)), cta))
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
