package ui

import (
	"fmt"
	"net/url"

	"duck-demo/internal/domain"

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
		nodes = append(nodes,
			Div(
				Class(cardClass()),
				H2(Text(card.Title)),
				P(Text(card.Description)),
				A(Href(card.Href), Text(card.LinkLabel)),
			),
		)
	}
	return appPage("Security", "security", principal, Div(Class("grid"), Group(nodes)))
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
		{key: "api-keys", label: "API Keys", href: "/ui/security/api-keys"},
	}
	nodes := make([]Node, 0, len(links))
	for i := range links {
		link := links[i]
		className := secondaryButtonClass()
		if link.key == active {
			className = primaryButtonClass()
		}
		nodes = append(nodes, A(Href(link.href), Class(className), Text(link.label)))
	}
	return Div(Class(cardClass("toolbar")), Div(Class("d-flex flex-wrap gap-2"), Group(nodes)))
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
		tableRows = append(tableRows,
			Tr(
				data.Show(containsExpr(row.Filter)),
				Td(A(Href(row.DetailURL), Text(row.Name))),
				Td(Text(row.Type)),
				Td(admin),
				Td(Text(row.CreatedAt)),
			),
		)
	}
	tableNode := Node(emptyStateCard("No principals found.", "New principal", "/ui/security/principals/new"))
	if len(tableRows) > 0 {
		tableNode = Div(
			Class(cardClass("table-wrap")),
			Table(
				Class("data-table"),
				THead(Tr(Th(Text("Name")), Th(Text("Type")), Th(Text("Role")), Th(Text("Created")))),
				TBody(Group(tableRows)),
			),
		)
	}

	return appPage(
		"Security: Principals",
		"security",
		principal,
		securitySectionNav("principals"),
		pageToolbar("/ui/security/principals/new", "New principal"),
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
		grantRows = append(grantRows,
			Tr(
				Td(Text(grant.Privilege)),
				Td(Text(grant.SecurableType)),
				Td(Text(grant.SecurableID)),
				Td(Text(formatTime(grant.GrantedAt))),
				Td(Class("text-right"), actionMenu("Actions", actionMenuPost("/ui/security/grants/"+grant.ID+"/delete", "Delete grant", d.CSRFFieldProvider, true))),
			),
		)
	}

	keyRows := make([]Node, 0, len(d.APIKeys))
	for i := range d.APIKeys {
		key := d.APIKeys[i]
		keyRows = append(keyRows,
			Tr(
				Td(Text(key.Name)),
				Td(Text(key.KeyPrefix)),
				Td(Text(formatTimePtr(key.ExpiresAt))),
				Td(Text(formatTime(key.CreatedAt))),
				Td(Class("text-right"), actionMenu("Actions", actionMenuPost("/ui/security/api-keys/"+key.ID+"/delete", "Delete API key", d.CSRFFieldProvider, true))),
			),
		)
	}

	roleLabel := statusLabel("user", "muted")
	adminButtonLabel := "Grant admin"
	adminValue := "true"
	if d.Item.IsAdmin {
		roleLabel = statusLabel("admin", "accent")
		adminButtonLabel = "Revoke admin"
		adminValue = "false"
	}

	return appPage(
		"Security: "+d.Item.Name,
		"security",
		d.Principal,
		securitySectionNav("principals"),
		Div(
			Class(cardClass()),
			P(Text("Principal ID: "+d.Item.ID)),
			P(Text("Type: "+d.Item.Type)),
			P(Text("Role: "), roleLabel),
			P(Text("External subject: "+strOrDash(d.Item.ExternalID))),
			P(Text("External issuer: "+strOrDash(d.Item.ExternalIssuer))),
			P(Text("Created: "+formatTime(d.Item.CreatedAt))),
			Div(
				Class("BtnGroup"),
				Form(
					Method("post"),
					Action("/ui/security/principals/"+d.Item.ID+"/admin"),
					d.CSRFFieldProvider(),
					Input(Type("hidden"), Name("is_admin"), Value(adminValue)),
					Button(Type("submit"), Class(primaryButtonClass()), Text(adminButtonLabel)),
				),
				Form(
					Method("post"),
					Action("/ui/security/principals/"+d.Item.ID+"/delete"),
					d.CSRFFieldProvider(),
					Button(Type("submit"), Class("btn btn-danger"), Text("Delete principal")),
				),
				A(Href("/ui/security/api-keys?principal_id="+d.Item.ID), Class(secondaryButtonClass()), Text("View API keys")),
			),
		),
		Div(
			Class(cardClass("table-wrap")),
			H2(Text("Privilege grants")),
			Table(
				Class("data-table"),
				THead(Tr(Th(Text("Privilege")), Th(Text("Securable type")), Th(Text("Securable ID")), Th(Text("Granted")), Th(Class("text-right"), Text("Actions")))),
				TBody(Group(grantRows)),
			),
		),
		Div(
			Class(cardClass("table-wrap")),
			H2(Text("API keys")),
			Table(
				Class("data-table"),
				THead(Tr(Th(Text("Name")), Th(Text("Prefix")), Th(Text("Expires")), Th(Text("Created")), Th(Class("text-right"), Text("Actions")))),
				TBody(Group(keyRows)),
			),
		),
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
		tableRows = append(tableRows,
			Tr(
				data.Show(containsExpr(row.Filter)),
				Td(A(Href(row.DetailURL), Text(row.Name))),
				Td(Text(row.Members)),
				Td(Text(row.CreatedAt)),
			),
		)
	}
	tableNode := Node(emptyStateCard("No groups found.", "New group", "/ui/security/groups/new"))
	if len(tableRows) > 0 {
		tableNode = Div(
			Class(cardClass("table-wrap")),
			Table(
				Class("data-table"),
				THead(Tr(Th(Text("Name")), Th(Text("Members")), Th(Text("Created")))),
				TBody(Group(tableRows)),
			),
		)
	}

	return appPage(
		"Security: Groups",
		"security",
		principal,
		securitySectionNav("groups"),
		pageToolbar("/ui/security/groups/new", "New group"),
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
		memberRows = append(memberRows,
			Tr(
				Td(Text(member.MemberID)),
				Td(Text(member.MemberType)),
				Td(
					Class("text-right"),
					Form(
						Method("post"),
						Action("/ui/security/groups/"+member.GroupID+"/members/delete"),
						member.CSRFField(),
						Input(Type("hidden"), Name("member_id"), Value(member.MemberID)),
						Input(Type("hidden"), Name("member_type"), Value(member.MemberType)),
						Button(Type("submit"), Class("btn btn-danger"), Text("Remove")),
					),
				),
			),
		)
	}

	grantRows := make([]Node, 0, len(d.Grants))
	for i := range d.Grants {
		grant := d.Grants[i]
		grantRows = append(grantRows,
			Tr(
				Td(Text(grant.Privilege)),
				Td(Text(grant.SecurableType)),
				Td(Text(grant.SecurableID)),
				Td(Class("text-right"), actionMenu("Actions", actionMenuPost("/ui/security/grants/"+grant.ID+"/delete", "Delete grant", d.CSRFFieldProvider, true))),
			),
		)
	}

	return appPage(
		"Security: "+d.Item.Name,
		"security",
		d.Principal,
		securitySectionNav("groups"),
		Div(
			Class(cardClass()),
			P(Text("Group ID: "+d.Item.ID)),
			P(Text("Description: "+dashIfEmpty(d.Item.Description))),
			P(Text("Created: "+formatTime(d.Item.CreatedAt))),
			Div(
				Class("BtnGroup"),
				Form(
					Method("post"),
					Action("/ui/security/groups/"+d.Item.ID+"/delete"),
					d.CSRFFieldProvider(),
					Button(Type("submit"), Class("btn btn-danger"), Text("Delete group")),
				),
			),
		),
		Div(
			Class(cardClass()),
			H2(Text("Add member")),
			Form(
				Class("stack-form"),
				Method("post"),
				Action("/ui/security/groups/"+d.Item.ID+"/members"),
				d.CSRFFieldProvider(),
				Label(Text("Member type")),
				Select(Name("member_type"), Option(Value("user"), Text("user")), Option(Value("group"), Text("group"))),
				Label(Text("Member ID")),
				Input(Name("member_id"), Required()),
				Div(Class("form-actions"), Button(Type("submit"), Class(primaryButtonClass()), Text("Add member"))),
			),
		),
		Div(
			Class(cardClass("table-wrap")),
			H2(Text("Members")),
			Table(
				Class("data-table"),
				THead(Tr(Th(Text("Member ID")), Th(Text("Type")), Th(Class("text-right"), Text("Actions")))),
				TBody(Group(memberRows)),
			),
		),
		Div(
			Class(cardClass("table-wrap")),
			H2(Text("Privilege grants")),
			Table(
				Class("data-table"),
				THead(Tr(Th(Text("Privilege")), Th(Text("Securable type")), Th(Text("Securable ID")), Th(Class("text-right"), Text("Actions")))),
				TBody(Group(grantRows)),
			),
		),
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

type securityGrantRowData struct {
	ID            string
	Filter        string
	PrincipalID   string
	PrincipalType string
	SecurableType string
	SecurableID   string
	Privilege     string
	GrantedAt     string
}

type securityGrantsPageData struct {
	Principal         domain.ContextPrincipal
	Rows              []securityGrantRowData
	Page              domain.PageRequest
	Total             int64
	PrincipalID       string
	PrincipalType     string
	SecurableType     string
	SecurableID       string
	CSRFFieldProvider func() Node
}

func securityGrantsPage(d securityGrantsPageData) Node {
	tableRows := make([]Node, 0, len(d.Rows))
	for i := range d.Rows {
		row := d.Rows[i]
		tableRows = append(tableRows,
			Tr(
				data.Show(containsExpr(row.Filter)),
				Td(Text(row.PrincipalID)),
				Td(Text(row.PrincipalType)),
				Td(Text(row.Privilege)),
				Td(Text(row.SecurableType)),
				Td(Text(row.SecurableID)),
				Td(Text(row.GrantedAt)),
				Td(Class("text-right"), actionMenu("Actions", actionMenuPost("/ui/security/grants/"+row.ID+"/delete", "Delete grant", d.CSRFFieldProvider, true))),
			),
		)
	}
	tableNode := Node(emptyStateCard("No grants found for the current filter.", "", ""))
	if len(tableRows) > 0 {
		tableNode = Div(
			Class(cardClass("table-wrap")),
			Table(
				Class("data-table"),
				THead(Tr(Th(Text("Principal ID")), Th(Text("Type")), Th(Text("Privilege")), Th(Text("Securable type")), Th(Text("Securable ID")), Th(Text("Granted")), Th(Class("text-right"), Text("Actions")))),
				TBody(Group(tableRows)),
			),
		)
	}

	return appPage(
		"Security: Grants",
		"security",
		d.Principal,
		securitySectionNav("grants"),
		Div(
			Class(cardClass()),
			H2(Text("Grant privilege")),
			Form(
				Class("stack-form"),
				Method("post"),
				Action("/ui/security/grants"),
				d.CSRFFieldProvider(),
				Label(Text("Principal ID")),
				Input(Name("principal_id"), Value(d.PrincipalID), Required()),
				Label(Text("Principal type")),
				Select(Name("principal_type"), optionSelected("user", d.PrincipalType), optionSelected("group", d.PrincipalType)),
				Label(Text("Privilege")),
				Input(Name("privilege"), Required()),
				Label(Text("Securable type")),
				Input(Name("securable_type"), Value(d.SecurableType), Required()),
				Label(Text("Securable ID")),
				Input(Name("securable_id"), Value(d.SecurableID), Required()),
				Div(Class("form-actions"), Button(Type("submit"), Class(primaryButtonClass()), Text("Grant privilege"))),
			),
		),
		quickFilterCard("Filter by privilege, principal ID, or securable ID"),
		tableNode,
		paginationCard(grantsPagePath(d.PrincipalID, d.PrincipalType, d.SecurableType, d.SecurableID), d.Page, d.Total),
	)
}

func grantsPagePath(principalID, principalType, securableType, securableID string) string {
	path := "/ui/security/grants"
	query := ""
	switch {
	case principalID != "" && principalType != "":
		query = fmt.Sprintf("?principal_id=%s&principal_type=%s", url.QueryEscape(principalID), url.QueryEscape(principalType))
	case securableID != "" && securableType != "":
		query = fmt.Sprintf("?securable_type=%s&securable_id=%s", url.QueryEscape(securableType), url.QueryEscape(securableID))
	}
	return path + query
}

type securityAPIKeyRowData struct {
	ID          string
	Filter      string
	Name        string
	PrincipalID string
	KeyPrefix   string
	ExpiresAt   string
	CreatedAt   string
}

type securityAPIKeysPageData struct {
	Principal         domain.ContextPrincipal
	Rows              []securityAPIKeyRowData
	Page              domain.PageRequest
	Total             int64
	SelectedPrincipal string
	CSRFFieldProvider func() Node
}

func securityAPIKeysPage(d securityAPIKeysPageData) Node {
	tableRows := make([]Node, 0, len(d.Rows))
	for i := range d.Rows {
		row := d.Rows[i]
		tableRows = append(tableRows,
			Tr(
				data.Show(containsExpr(row.Filter)),
				Td(Text(row.Name)),
				Td(Text(row.PrincipalID)),
				Td(Text(row.KeyPrefix)),
				Td(Text(row.ExpiresAt)),
				Td(Text(row.CreatedAt)),
				Td(Class("text-right"), actionMenu("Actions", actionMenuPost("/ui/security/api-keys/"+row.ID+"/delete", "Delete API key", d.CSRFFieldProvider, true))),
			),
		)
	}
	tableNode := Node(emptyStateCard("No API keys found.", "", ""))
	if len(tableRows) > 0 {
		tableNode = Div(
			Class(cardClass("table-wrap")),
			Table(
				Class("data-table"),
				THead(Tr(Th(Text("Name")), Th(Text("Principal ID")), Th(Text("Prefix")), Th(Text("Expires")), Th(Text("Created")), Th(Class("text-right"), Text("Actions")))),
				TBody(Group(tableRows)),
			),
		)
	}

	createPrincipalID := d.SelectedPrincipal
	if createPrincipalID == "" {
		createPrincipalID = d.Principal.ID
	}

	createFormNodes := []Node{
		Label(Text("Principal ID")),
		Input(Name("principal_id"), Value(createPrincipalID), Required()),
		Label(Text("Name")),
		Input(Name("name"), Required()),
		Label(Text("Expires at")),
		Input(Name("expires_at"), Placeholder("YYYY-MM-DD or RFC3339")),
	}
	if !d.Principal.IsAdmin {
		createFormNodes = []Node{
			Input(Type("hidden"), Name("principal_id"), Value(createPrincipalID)),
			Label(Text("Name")),
			Input(Name("name"), Required()),
			Label(Text("Expires at")),
			Input(Name("expires_at"), Placeholder("YYYY-MM-DD or RFC3339")),
		}
	}

	cleanupNode := Node(nil)
	if d.Principal.IsAdmin {
		cleanupNode = Form(
			Method("post"),
			Action("/ui/security/api-keys/cleanup"),
			d.CSRFFieldProvider(),
			Button(Type("submit"), Class(secondaryButtonClass()), Text("Cleanup expired keys")),
		)
	}

	return appPage(
		"Security: API Keys",
		"security",
		d.Principal,
		securitySectionNav("api-keys"),
		Div(
			Class(cardClass()),
			H2(Text("Create API key")),
			Form(
				Class("stack-form"),
				Method("post"),
				Action("/ui/security/api-keys"),
				d.CSRFFieldProvider(),
				Group(createFormNodes),
				Div(Class("form-actions"), Button(Type("submit"), Class(primaryButtonClass()), Text("Create API key")), cleanupNode),
			),
		),
		quickFilterCard("Filter by key name, prefix, or principal ID"),
		tableNode,
		paginationCard(apiKeysPagePath(d.SelectedPrincipal), d.Page, d.Total),
	)
}

func apiKeysPagePath(principalID string) string {
	if principalID == "" {
		return "/ui/security/api-keys"
	}
	return "/ui/security/api-keys?principal_id=" + url.QueryEscape(principalID)
}

func securityAPIKeyCreatedPage(principal domain.ContextPrincipal, principalID, keyName, rawKey string) Node {
	return appPage(
		"API Key Created",
		"security",
		principal,
		securitySectionNav("api-keys"),
		Div(
			Class(cardClass()),
			H2(Text("API key created")),
			P(Text("Principal ID: "+principalID)),
			P(Text("Name: "+keyName)),
			P(Text("Copy this value now. It will not be shown again.")),
			Pre(Text(rawKey)),
			A(Href(apiKeysPagePath(principalID)), Class(primaryButtonClass()), Text("Back to API keys")),
		),
	)
}
