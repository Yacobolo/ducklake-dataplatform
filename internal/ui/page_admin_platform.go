package ui

import (
	"fmt"
	"net/url"

	"duck-demo/internal/domain"

	. "maragu.dev/gomponents"
	data "maragu.dev/gomponents-datastar"
	. "maragu.dev/gomponents/html"
)

func sectionHomePage(title, active string, principal domain.ContextPrincipal, cards []securityCardData) Node {
	nodes := make([]Node, 0, len(cards))
	for i := range cards {
		card := cards[i]
		nodes = append(nodes, Div(Class(cardClass()), H2(Text(card.Title)), P(Text(card.Description)), A(Href(card.Href), Text(card.LinkLabel))))
	}
	return appPage(title, active, principal, Div(Class("grid gap-3 md:grid-cols-2 xl:grid-cols-3"), Group(nodes)))
}

func storageSectionNav(active string) Node {
	return adminSectionNav(active, "storage", []navItem{
		{Key: "credentials", Label: "Credentials", Href: "/ui/storage/credentials"},
		{Key: "locations", Label: "Locations", Href: "/ui/storage/locations"},
		{Key: "volumes", Label: "Volumes", Href: "/ui/storage/volumes"},
	})
}

func computeSectionNav(active string) Node {
	return adminSectionNav(active, "compute", []navItem{
		{Key: "endpoints", Label: "Endpoints", Href: "/ui/compute/endpoints"},
	})
}

func governanceSectionNav(active string) Node {
	return adminSectionNav(active, "governance", []navItem{
		{Key: "search", Label: "Search", Href: "/ui/governance/search"},
		{Key: "tags", Label: "Tags", Href: "/ui/governance/tags"},
		{Key: "lineage", Label: "Lineage", Href: "/ui/governance/lineage"},
		{Key: "audit", Label: "Audit Logs", Href: "/ui/governance/audit-logs"},
		{Key: "history", Label: "Query History", Href: "/ui/governance/query-history"},
		{Key: "manifest", Label: "Manifest", Href: "/ui/governance/manifest"},
	})
}

func adminSectionNav(active, _ string, items []navItem) Node {
	nodes := make([]Node, 0, len(items))
	for i := range items {
		item := items[i]
		className := secondaryButtonClass()
		if item.Key == active {
			className = primaryButtonClass()
		}
		nodes = append(nodes, A(Href(item.Href), Class(className), Text(item.Label)))
	}
	return Div(Class(cardClass()), Div(Class("flex flex-wrap gap-2"), Group(nodes)))
}

type adminTableRow struct {
	Filter string
	Cells  []Node
}

func adminTablePage(title, active string, principal domain.ContextPrincipal, nav Node, newHref, newLabel, filterPlaceholder, basePath string, page domain.PageRequest, total int64, headers []string, rows []adminTableRow, extra ...Node) Node {
	tableRows := make([]Node, 0, len(rows))
	for i := range rows {
		row := rows[i]
		tableRows = append(tableRows, Tr(append([]Node{data.Show(containsExpr(row.Filter))}, row.Cells...)...))
	}
	tableNode := Node(emptyStateCard("No entries found.", "", ""))
	if len(rows) > 0 {
		ths := make([]Node, 0, len(headers))
		for i := range headers {
			ths = append(ths, Th(Text(headers[i])))
		}
		tableNode = Div(Class(cardClass(tableWrapClass())), Table(Class(dataTableClass()), THead(Tr(Group(ths))), TBody(Group(tableRows))))
	}

	nodes := []Node{nav}
	if newHref != "" && newLabel != "" {
		nodes = append(nodes, pageToolbar(newHref, newLabel))
	}
	if filterPlaceholder != "" {
		nodes = append(nodes, quickFilterCard(filterPlaceholder))
	}
	nodes = append(nodes, extra...)
	nodes = append(nodes, tableNode, paginationCard(basePath, page, total))
	return appPage(title, active, principal, Group(nodes))
}

func storageCredentialFormPage(principal domain.ContextPrincipal, title, action string, item *domain.StorageCredential, csrfFieldProvider func() Node) Node {
	credentialType := string(domain.CredentialTypeS3)
	if item != nil {
		credentialType = string(item.CredentialType)
	}
	return formPage(principal, title, "storage", action, csrfFieldProvider,
		Label(Text("Name")),
		Input(Name("name"), Value(optionalCredentialName(item)), Required()),
		Label(Text("Credential type")),
		Select(Name("credential_type"),
			optionSelected(string(domain.CredentialTypeS3), credentialType),
			optionSelected(string(domain.CredentialTypeAzure), credentialType),
			optionSelected(string(domain.CredentialTypeGCS), credentialType),
		),
		Label(Text("Comment")),
		Textarea(Name("comment"), Text(optionalCredentialComment(item))),
		Label(Text("S3 Key ID")),
		Input(Name("key_id"), Value(optionalCredentialValue(item, "key_id"))),
		Label(Text("S3 Secret")),
		Input(Name("secret"), Value(optionalCredentialValue(item, "secret"))),
		Label(Text("S3 Endpoint")),
		Input(Name("endpoint"), Value(optionalCredentialValue(item, "endpoint"))),
		Label(Text("S3 Region")),
		Input(Name("region"), Value(optionalCredentialValue(item, "region"))),
		Label(Text("S3 URL Style")),
		Input(Name("url_style"), Value(optionalCredentialValue(item, "url_style"))),
		Label(Text("Azure Account Name")),
		Input(Name("azure_account_name"), Value(optionalCredentialValue(item, "azure_account_name"))),
		Label(Text("Azure Account Key")),
		Input(Name("azure_account_key"), Value(optionalCredentialValue(item, "azure_account_key"))),
		Label(Text("Azure Client ID")),
		Input(Name("azure_client_id"), Value(optionalCredentialValue(item, "azure_client_id"))),
		Label(Text("Azure Tenant ID")),
		Input(Name("azure_tenant_id"), Value(optionalCredentialValue(item, "azure_tenant_id"))),
		Label(Text("Azure Client Secret")),
		Input(Name("azure_client_secret"), Value(optionalCredentialValue(item, "azure_client_secret"))),
		Label(Text("GCS Key File Path")),
		Input(Name("gcs_key_file_path"), Value(optionalCredentialValue(item, "gcs"))),
	)
}

func optionalCredentialName(item *domain.StorageCredential) string {
	if item == nil {
		return ""
	}
	return item.Name
}

func optionalCredentialComment(item *domain.StorageCredential) string {
	if item == nil {
		return ""
	}
	return item.Comment
}

func optionalCredentialValue(item *domain.StorageCredential, key string) string {
	if item == nil {
		return ""
	}
	switch key {
	case "key_id":
		return item.KeyID
	case "secret":
		return item.Secret
	case "endpoint":
		return item.Endpoint
	case "region":
		return item.Region
	case "url_style":
		return item.URLStyle
	case "azure_account_name":
		return item.AzureAccountName
	case "azure_account_key":
		return item.AzureAccountKey
	case "azure_client_id":
		return item.AzureClientID
	case "azure_tenant_id":
		return item.AzureTenantID
	case "azure_client_secret":
		return item.AzureClientSecret
	case "gcs":
		return item.GCSKeyFilePath
	default:
		return ""
	}
}

func storageLocationFormPage(principal domain.ContextPrincipal, title, action string, item *domain.ExternalLocation, csrfFieldProvider func() Node) Node {
	storageType := string(domain.StorageTypeS3)
	if item != nil {
		storageType = string(item.StorageType)
	}
	return formPage(principal, title, "storage", action, csrfFieldProvider,
		Label(Text("Name")),
		Input(Name("name"), Value(optionalLocationName(item)), Required()),
		Label(Text("URL")),
		Input(Name("url"), Value(optionalLocationURL(item)), Required()),
		Label(Text("Credential name")),
		Input(Name("credential_name"), Value(optionalLocationCredential(item)), Required()),
		Label(Text("Storage type")),
		Select(Name("storage_type"),
			optionSelected(string(domain.StorageTypeS3), storageType),
			optionSelected(string(domain.StorageTypeAzure), storageType),
			optionSelected(string(domain.StorageTypeGCS), storageType),
		),
		Label(Text("Comment")),
		Textarea(Name("comment"), Text(optionalLocationComment(item))),
		Label(Input(Type("checkbox"), Name("read_only"), checkedIf(item != nil && item.ReadOnly)), Text(" Read only")),
	)
}

func checkedIf(v bool) Node {
	if v {
		return Checked()
	}
	return nil
}

func optionalLocationName(item *domain.ExternalLocation) string {
	if item == nil {
		return ""
	}
	return item.Name
}

func optionalLocationURL(item *domain.ExternalLocation) string {
	if item == nil {
		return ""
	}
	return item.URL
}

func optionalLocationCredential(item *domain.ExternalLocation) string {
	if item == nil {
		return ""
	}
	return item.CredentialName
}

func optionalLocationComment(item *domain.ExternalLocation) string {
	if item == nil {
		return ""
	}
	return item.Comment
}

func storageVolumeFormPage(principal domain.ContextPrincipal, title, action string, item *domain.Volume, csrfFieldProvider func() Node) Node {
	volumeType := domain.VolumeTypeManaged
	if item != nil && item.VolumeType != "" {
		volumeType = item.VolumeType
	}
	return formPage(principal, title, "storage", action, csrfFieldProvider,
		Label(Text("Catalog name")),
		Input(Name("catalog_name"), Value(optionalVolumeCatalog(item)), Required()),
		Label(Text("Schema name")),
		Input(Name("schema_name"), Value(optionalVolumeSchema(item)), Required()),
		Label(Text("Name")),
		Input(Name("name"), Value(optionalVolumeName(item)), Required()),
		Label(Text("Volume type")),
		Select(Name("volume_type"), optionSelected(domain.VolumeTypeManaged, volumeType), optionSelected(domain.VolumeTypeExternal, volumeType)),
		Label(Text("Storage location")),
		Input(Name("storage_location"), Value(optionalVolumeLocation(item))),
		Label(Text("Comment")),
		Textarea(Name("comment"), Text(optionalVolumeComment(item))),
	)
}

func optionalVolumeCatalog(item *domain.Volume) string {
	if item == nil {
		return ""
	}
	return item.CatalogName
}
func optionalVolumeSchema(item *domain.Volume) string {
	if item == nil {
		return ""
	}
	return item.SchemaName
}
func optionalVolumeName(item *domain.Volume) string {
	if item == nil {
		return ""
	}
	return item.Name
}
func optionalVolumeLocation(item *domain.Volume) string {
	if item == nil {
		return ""
	}
	return item.StorageLocation
}
func optionalVolumeComment(item *domain.Volume) string {
	if item == nil {
		return ""
	}
	return item.Comment
}

func computeEndpointFormPage(principal domain.ContextPrincipal, title, action string, item *domain.ComputeEndpoint, csrfFieldProvider func() Node) Node {
	endpointType := "REMOTE"
	size := "MEDIUM"
	if item != nil {
		if item.Type != "" {
			endpointType = item.Type
		}
		if item.Size != "" {
			size = item.Size
		}
	}
	return formPage(principal, title, "compute", action, csrfFieldProvider,
		Label(Text("Name")),
		Input(Name("name"), Value(optionalEndpointName(item)), Required()),
		Label(Text("URL")),
		Input(Name("url"), Value(optionalEndpointURL(item)), Required()),
		Label(Text("Type")),
		Select(Name("type"), optionSelected("LOCAL", endpointType), optionSelected("REMOTE", endpointType)),
		Label(Text("Size")),
		Select(Name("size"), optionSelected("SMALL", size), optionSelected("MEDIUM", size), optionSelected("LARGE", size)),
		Label(Text("Max memory (GB)")),
		Input(Name("max_memory_gb"), Value(optionalEndpointMemory(item))),
		Label(Text("Auth token")),
		Input(Name("auth_token"), Value(optionalEndpointToken(item))),
		Label(Text("Status")),
		Input(Name("status"), Value(optionalEndpointStatus(item))),
	)
}

func optionalEndpointName(item *domain.ComputeEndpoint) string {
	if item == nil {
		return ""
	}
	return item.Name
}
func optionalEndpointURL(item *domain.ComputeEndpoint) string {
	if item == nil {
		return ""
	}
	return item.URL
}
func optionalEndpointToken(item *domain.ComputeEndpoint) string {
	if item == nil {
		return ""
	}
	return item.AuthToken
}
func optionalEndpointStatus(item *domain.ComputeEndpoint) string {
	if item == nil {
		return ""
	}
	return item.Status
}
func optionalEndpointMemory(item *domain.ComputeEndpoint) string {
	if item == nil || item.MaxMemoryGB == nil {
		return ""
	}
	return fmt.Sprintf("%d", *item.MaxMemoryGB)
}

type governanceSearchPageData struct {
	Principal   domain.ContextPrincipal
	Query       string
	ObjectType  string
	CatalogName string
	Rows        []domain.SearchResult
}

func governanceSearchPage(d governanceSearchPageData) Node {
	rows := make([]Node, 0, len(d.Rows))
	for i := range d.Rows {
		row := d.Rows[i]
		target := "/ui/catalogs"
		if row.SchemaName != nil && row.Type == "schema" {
			target = "/ui/catalogs?schema=" + url.QueryEscape(*row.SchemaName) + "&type=schema"
		}
		if row.SchemaName != nil && row.TableName != nil {
			target = "/ui/catalogs?schema=" + url.QueryEscape(*row.SchemaName) + "&type=" + url.QueryEscape(row.Type) + "&name=" + url.QueryEscape(*row.TableName)
		}
		rows = append(rows, Tr(
			Td(Text(row.Type)),
			Td(A(Href(target), Text(row.Name))),
			Td(Text(strOrDash(row.SchemaName))),
			Td(Text(strOrDash(row.Comment))),
			Td(Text(row.MatchField)),
		))
	}
	tableNode := Node(emptyStateCard("No search results yet.", "", ""))
	if len(rows) > 0 {
		tableNode = Div(Class(cardClass(tableWrapClass())), Table(Class(dataTableClass()), THead(Tr(Th(Text("Type")), Th(Text("Name")), Th(Text("Schema")), Th(Text("Comment")), Th(Text("Match")))), TBody(Group(rows))))
	}
	return appPage("Governance: Search", "governance", d.Principal,
		governanceSectionNav("search"),
		Div(Class(cardClass()),
			Form(Class(stackFormClass()), Method("get"), Action("/ui/governance/search"),
				Label(Text("Query")),
				Input(Name("q"), Value(d.Query)),
				Label(Text("Object type")),
				Input(Name("object_type"), Value(d.ObjectType)),
				Label(Text("Catalog")),
				Input(Name("catalog"), Value(d.CatalogName)),
				Div(Class(formActionsClass()), Button(Type("submit"), Class(primaryButtonClass()), Text("Search"))),
			),
		),
		tableNode,
	)
}
