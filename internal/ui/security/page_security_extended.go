package security

import (
	"fmt"
	"net/url"
	"strings"

	"duck-demo/internal/domain"
	"duck-demo/internal/ui/core"

	. "maragu.dev/gomponents"
	data "maragu.dev/gomponents-datastar"
	. "maragu.dev/gomponents/html"
)

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
				Td(Class("text-right"), core.ActionMenu("Actions", actionMenuPost("/ui/security/grants/"+row.ID+"/delete", "Delete grant", d.CSRFFieldProvider, true))),
			),
		)
	}
	tableNode := Node(emptyStateCard("No grants found for the current filter.", "", ""))
	if len(tableRows) > 0 {
		tableNode = core.TableContainer("",
			core.DataTable("",
				THead(Tr(Th(Text("Principal ID")), Th(Text("Type")), Th(Text("Privilege")), Th(Text("Securable type")), Th(Text("Securable ID")), Th(Text("Granted")), Th(Class("text-right"), Text("Actions")))),
				TBody(Group(tableRows)),
			),
		)
	}

	return coreAppPage(
		"Security: Grants",
		d.Principal,
		securitySectionNav("grants"),
		Div(
			Class(coreCardClass()),
			H2(Text("Grant privilege")),
			Form(
				Class(stackFormClass()),
				Method("post"),
				Action("/ui/security/grants"),
				d.CSRFFieldProvider(),
				core.FieldLabel("Principal ID"),
				core.InputControl("", Name("principal_id"), Value(d.PrincipalID), Required()),
				core.FieldLabel("Principal type"),
				core.SelectControl("", Name("principal_type"), optionSelected("user", d.PrincipalType), optionSelected("group", d.PrincipalType)),
				core.FieldLabel("Privilege"),
				core.InputControl("", Name("privilege"), Required()),
				core.FieldLabel("Securable type"),
				core.InputControl("", Name("securable_type"), Value(d.SecurableType), Required()),
				core.FieldLabel("Securable ID"),
				core.InputControl("", Name("securable_id"), Value(d.SecurableID), Required()),
				Div(Class(formActionsClass()), core.PrimaryButton("", Type("submit"), Text("Grant privilege"))),
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
				Td(Class("text-right"), core.ActionMenu("Actions", actionMenuPost("/ui/security/api-keys/"+row.ID+"/delete", "Delete API key", d.CSRFFieldProvider, true))),
			),
		)
	}
	tableNode := Node(emptyStateCard("No API keys found.", "", ""))
	if len(tableRows) > 0 {
		tableNode = core.TableContainer("",
			core.DataTable("",
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
		core.FieldLabel("Principal ID"),
		core.InputControl("", Name("principal_id"), Value(createPrincipalID), Required()),
		core.FieldLabel("Name"),
		core.InputControl("", Name("name"), Required()),
		core.FieldLabel("Expires at"),
		core.InputControl("", Name("expires_at"), Placeholder("YYYY-MM-DD or RFC3339")),
	}
	if !d.Principal.IsAdmin {
		createFormNodes = []Node{
			Input(Type("hidden"), Name("principal_id"), Value(createPrincipalID)),
			core.FieldLabel("Name"),
			core.InputControl("", Name("name"), Required()),
			core.FieldLabel("Expires at"),
			core.InputControl("", Name("expires_at"), Placeholder("YYYY-MM-DD or RFC3339")),
		}
	}

	cleanupNode := Node(nil)
	if d.Principal.IsAdmin {
		cleanupNode = Form(
			Method("post"),
			Action("/ui/security/api-keys/cleanup"),
			d.CSRFFieldProvider(),
			core.SecondaryButton("", Type("submit"), Text("Cleanup expired keys")),
		)
	}

	return coreAppPage(
		"Security: API Keys",
		d.Principal,
		securitySectionNav("api-keys"),
		Div(
			Class(coreCardClass()),
			H2(Text("Create API key")),
			Form(
				Class(stackFormClass()),
				Method("post"),
				Action("/ui/security/api-keys"),
				d.CSRFFieldProvider(),
				Group(createFormNodes),
				Div(Class(formActionsClass()), core.PrimaryButton("", Type("submit"), Text("Create API key")), cleanupNode),
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
	return coreAppPage(
		"API Key Created",
		principal,
		securitySectionNav("api-keys"),
		Div(
			Class(coreCardClass()),
			H2(Text("API key created")),
			P(Text("Principal ID: "+principalID)),
			P(Text("Name: "+keyName)),
			P(Text("Copy this value now. It will not be shown again.")),
			Pre(Text(rawKey)),
			core.PrimaryLink(apiKeysPagePath(principalID), "", Text("Back to API keys")),
		),
	)
}

type securityRowFilterPageData struct {
	Principal         domain.ContextPrincipal
	TableID           string
	Rows              []securityRowFilterRowData
	CSRFFieldProvider func() Node
}

type securityRowFilterRowData struct {
	ID          string
	TableID     string
	FilterSQL   string
	Description string
	CreatedAt   string
	Bindings    []domain.RowFilterBinding
}

func securityRowFiltersPage(d securityRowFilterPageData) Node {
	rows := make([]Node, 0, len(d.Rows))
	for i := range d.Rows {
		row := d.Rows[i]
		bindings := "-"
		if len(row.Bindings) > 0 {
			parts := make([]string, 0, len(row.Bindings))
			for j := range row.Bindings {
				parts = append(parts, row.Bindings[j].PrincipalType+":"+row.Bindings[j].PrincipalID)
			}
			bindings = stringsJoin(parts)
		}
		rows = append(rows,
			Tr(
				Td(Text(row.TableID)),
				Td(Pre(Text(row.FilterSQL))),
				Td(Text(dashIfEmpty(row.Description))),
				Td(Text(bindings)),
				Td(Text(row.CreatedAt)),
				Td(Class("text-right"), core.ActionMenu("Actions", actionMenuPost("/ui/security/row-filters/"+row.ID+"/delete", "Delete row filter", d.CSRFFieldProvider, true))),
			),
		)
	}

	tableNode := Node(emptyStateCard("No row filters found for the selected table.", "", ""))
	if len(rows) > 0 {
		tableNode = core.TableContainer("",
			core.DataTable("",
				THead(Tr(Th(Text("Table ID")), Th(Text("Filter SQL")), Th(Text("Description")), Th(Text("Bindings")), Th(Text("Created")), Th(Class("text-right"), Text("Actions")))),
				TBody(Group(rows)),
			),
		)
	}

	return coreAppPage(
		"Security: Row Filters",
		d.Principal,
		securitySectionNav("row-filters"),
		Div(Class(coreCardClass()),
			H2(Text("Create row filter")),
			Form(Class(stackFormClass()), Method("post"), Action("/ui/security/row-filters"),
				d.CSRFFieldProvider(),
				core.FieldLabel("Table ID"),
				core.InputControl("", Name("table_id"), Value(d.TableID), Required()),
				core.FieldLabel("Description"),
				core.InputControl("", Name("description")),
				core.FieldLabel("Filter SQL"),
				core.TextareaControl("", Name("filter_sql"), Required()),
				Div(Class(formActionsClass()), core.PrimaryButton("", Type("submit"), Text("Create row filter"))),
			),
		),
		Div(Class(coreCardClass()),
			H2(Text("Filter")),
			Form(Class(stackFormClass()), Method("get"), Action("/ui/security/row-filters"),
				core.FieldLabel("Table ID"),
				core.InputControl("", Name("table_id"), Value(d.TableID)),
				Div(Class(formActionsClass()), core.SecondaryButton("", Type("submit"), Text("Apply filter"))),
			),
		),
		Div(Class(coreCardClass()),
			H2(Text("Bind row filter")),
			Form(Class(stackFormClass()), Method("post"), Action("/ui/security/row-filters/bindings"),
				d.CSRFFieldProvider(),
				core.FieldLabel("Row Filter ID"),
				core.InputControl("", Name("row_filter_id"), Required()),
				core.FieldLabel("Principal type"),
				core.SelectControl("", Name("principal_type"), Option(Value("user"), Text("user")), Option(Value("group"), Text("group"))),
				core.FieldLabel("Principal ID"),
				core.InputControl("", Name("principal_id"), Required()),
				Div(Class(formActionsClass()), core.SecondaryButton("", Type("submit"), Text("Bind filter"))),
			),
		),
		Div(Class(coreCardClass()),
			H2(Text("Unbind row filter")),
			Form(Class(stackFormClass()), Method("post"), Action("/ui/security/row-filters/bindings/delete"),
				d.CSRFFieldProvider(),
				core.FieldLabel("Row Filter ID"),
				core.InputControl("", Name("row_filter_id"), Required()),
				core.FieldLabel("Principal type"),
				core.SelectControl("", Name("principal_type"), Option(Value("user"), Text("user")), Option(Value("group"), Text("group"))),
				core.FieldLabel("Principal ID"),
				core.InputControl("", Name("principal_id"), Required()),
				Div(Class(formActionsClass()), core.DangerButton("", Type("submit"), Text("Unbind filter"))),
			),
		),
		tableNode,
	)
}

type securityColumnMaskPageData struct {
	Principal         domain.ContextPrincipal
	TableID           string
	Rows              []securityColumnMaskRowData
	CSRFFieldProvider func() Node
}

type securityColumnMaskRowData struct {
	ID             string
	TableID        string
	ColumnName     string
	MaskExpression string
	Description    string
	CreatedAt      string
	Bindings       []domain.ColumnMaskBinding
}

func securityColumnMasksPage(d securityColumnMaskPageData) Node {
	rows := make([]Node, 0, len(d.Rows))
	for i := range d.Rows {
		row := d.Rows[i]
		bindings := "-"
		if len(row.Bindings) > 0 {
			parts := make([]string, 0, len(row.Bindings))
			for j := range row.Bindings {
				entry := row.Bindings[j].PrincipalType + ":" + row.Bindings[j].PrincipalID
				if row.Bindings[j].SeeOriginal {
					entry += " (see original)"
				}
				parts = append(parts, entry)
			}
			bindings = stringsJoin(parts)
		}
		rows = append(rows,
			Tr(
				Td(Text(row.TableID)),
				Td(Text(row.ColumnName)),
				Td(Pre(Text(row.MaskExpression))),
				Td(Text(dashIfEmpty(row.Description))),
				Td(Text(bindings)),
				Td(Text(row.CreatedAt)),
				Td(Class("text-right"), core.ActionMenu("Actions", actionMenuPost("/ui/security/column-masks/"+row.ID+"/delete", "Delete column mask", d.CSRFFieldProvider, true))),
			),
		)
	}

	tableNode := Node(emptyStateCard("No column masks found for the selected table.", "", ""))
	if len(rows) > 0 {
		tableNode = core.TableContainer("",
			core.DataTable("",
				THead(Tr(Th(Text("Table ID")), Th(Text("Column")), Th(Text("Mask Expression")), Th(Text("Description")), Th(Text("Bindings")), Th(Text("Created")), Th(Class("text-right"), Text("Actions")))),
				TBody(Group(rows)),
			),
		)
	}

	return coreAppPage(
		"Security: Column Masks",
		d.Principal,
		securitySectionNav("column-masks"),
		Div(Class(coreCardClass()),
			H2(Text("Create column mask")),
			Form(Class(stackFormClass()), Method("post"), Action("/ui/security/column-masks"),
				d.CSRFFieldProvider(),
				core.FieldLabel("Table ID"),
				core.InputControl("", Name("table_id"), Value(d.TableID), Required()),
				core.FieldLabel("Column name"),
				core.InputControl("", Name("column_name"), Required()),
				core.FieldLabel("Description"),
				core.InputControl("", Name("description")),
				core.FieldLabel("Mask expression"),
				core.TextareaControl("", Name("mask_expression"), Required()),
				Div(Class(formActionsClass()), core.PrimaryButton("", Type("submit"), Text("Create column mask"))),
			),
		),
		Div(Class(coreCardClass()),
			H2(Text("Filter")),
			Form(Class(stackFormClass()), Method("get"), Action("/ui/security/column-masks"),
				core.FieldLabel("Table ID"),
				core.InputControl("", Name("table_id"), Value(d.TableID)),
				Div(Class(formActionsClass()), core.SecondaryButton("", Type("submit"), Text("Apply filter"))),
			),
		),
		Div(Class(coreCardClass()),
			H2(Text("Bind column mask")),
			Form(Class(stackFormClass()), Method("post"), Action("/ui/security/column-masks/bindings"),
				d.CSRFFieldProvider(),
				core.FieldLabel("Column Mask ID"),
				core.InputControl("", Name("column_mask_id"), Required()),
				core.FieldLabel("Principal type"),
				core.SelectControl("", Name("principal_type"), Option(Value("user"), Text("user")), Option(Value("group"), Text("group"))),
				core.FieldLabel("Principal ID"),
				core.InputControl("", Name("principal_id"), Required()),
				core.Checkbox("column-mask-see-original", "see_original", "true", "Allow original values", false),
				Div(Class(formActionsClass()), core.SecondaryButton("", Type("submit"), Text("Bind mask"))),
			),
		),
		Div(Class(coreCardClass()),
			H2(Text("Unbind column mask")),
			Form(Class(stackFormClass()), Method("post"), Action("/ui/security/column-masks/bindings/delete"),
				d.CSRFFieldProvider(),
				core.FieldLabel("Column Mask ID"),
				core.InputControl("", Name("column_mask_id"), Required()),
				core.FieldLabel("Principal type"),
				core.SelectControl("", Name("principal_type"), Option(Value("user"), Text("user")), Option(Value("group"), Text("group"))),
				core.FieldLabel("Principal ID"),
				core.InputControl("", Name("principal_id"), Required()),
				Div(Class(formActionsClass()), core.DangerButton("", Type("submit"), Text("Unbind mask"))),
			),
		),
		tableNode,
	)
}

func stringsJoin(values []string) string {
	return joinStrings(values, ", ")
}

func joinStrings(values []string, separator string) string {
	if len(values) == 0 {
		return ""
	}
	result := values[0]
	for _, value := range values[1:] {
		result += separator + value
	}
	return result
}

func coreAppPage(title string, principal domain.ContextPrincipal, body ...Node) Node {
	return core.AppPage(title, "security", principal, body...)
}

func coreCardClass(extra ...string) string {
	return core.ClassNames("rounded-xl border border-[var(--borderColor-default)] bg-[var(--bgColor-default)] p-4 shadow-xs", strings.Join(extra, " "))
}

func stackFormClass() string {
	return "stack-form [&>:not(label):not(.form-actions)]:mb-3 [&>:last-child]:mb-0"
}

func formActionsClass() string {
	return "form-actions mt-2"
}

func optionSelected(value, selected string) Node {
	if value == selected {
		return Option(Value(value), Selected(), Text(value))
	}
	return Option(Value(value), Text(value))
}
