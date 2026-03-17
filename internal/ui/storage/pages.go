package storage

import (
	"net/url"
	"strconv"

	"duck-demo/internal/domain"
	"duck-demo/internal/ui/core"

	. "maragu.dev/gomponents"
	. "maragu.dev/gomponents/html"
)

type storageCredentialRowData struct {
	Name    string
	URL     string
	Type    string
	Owner   string
	Updated string
}

type storageLocationRowData struct {
	Name           string
	URL            string
	StorageURL     string
	CredentialName string
	ReadOnly       bool
}

type storageVolumeRowData struct {
	Name            string
	URL             string
	VolumeType      string
	StorageLocation string
	Owner           string
}

func storageHomePage(principal domain.ContextPrincipal) Node {
	return core.AppPage("Storage", "storage", principal,
		storageSectionNav(""),
		Div(Class("grid gap-3 md:grid-cols-2 xl:grid-cols-3"),
			storageCard("Credentials", "Create and manage governed cloud storage credentials.", "/ui/storage/credentials"),
			storageCard("Locations", "Manage external locations backed by storage credentials.", "/ui/storage/locations"),
			storageCard("Volumes", "Create and manage governed volumes in catalog schemas.", "/ui/storage/volumes"),
		),
	)
}

func storageCredentialsListPage(principal domain.ContextPrincipal, rows []storageCredentialRowData, page domain.PageRequest, total int64) Node {
	table := Node(P(Class("text-xs text-muted"), Text("No storage credentials found.")))
	if len(rows) > 0 {
		tableRows := make([]Node, 0, len(rows))
		for i := range rows {
			row := rows[i]
			tableRows = append(tableRows, Tr(
				Td(A(Href(row.URL), Class("font-medium text-accent"), Text(row.Name))),
				Td(Text(row.Type)),
				Td(Text(row.Owner)),
				Td(Text(row.Updated)),
			))
		}
		table = Div(Class("overflow-x-auto"),
			Table(Class("min-w-full text-left text-sm"),
				THead(Tr(Th(Text("Name")), Th(Text("Type")), Th(Text("Owner")), Th(Text("Updated")))),
				TBody(Group(tableRows)),
			),
		)
	}

	return core.AppPage("Storage: Credentials", "storage", principal,
		storageSectionNav("credentials"),
		sectionHeader("Storage credentials", "Create and manage governed cloud storage credentials.", "/ui/storage/credentials/new", "New credential"),
		Div(Class("rounded-xl border border-border bg-background p-4 shadow-xs"),
			table,
			P(Class("mt-4 text-sm text-muted"), Text("Showing up to "+strconv.Itoa(page.MaxResults)+" credentials. Total: "+strconv.FormatInt(total, 10))),
		),
	)
}

func storageLocationsListPage(principal domain.ContextPrincipal, rows []storageLocationRowData, page domain.PageRequest, total int64) Node {
	table := Node(P(Class("text-xs text-muted"), Text("No external locations found.")))
	if len(rows) > 0 {
		tableRows := make([]Node, 0, len(rows))
		for i := range rows {
			row := rows[i]
			tableRows = append(tableRows, Tr(
				Td(A(Href(row.URL), Class("font-medium text-accent"), Text(row.Name))),
				Td(Text(row.StorageURL)),
				Td(Text(row.CredentialName)),
				Td(func() Node {
					if row.ReadOnly {
						return statusPill("true", "attention")
					}
					return statusPill("false", "success")
				}()),
			))
		}
		table = Div(Class("overflow-x-auto"),
			Table(Class("min-w-full text-left text-sm"),
				THead(Tr(Th(Text("Name")), Th(Text("URL")), Th(Text("Credential")), Th(Text("Read Only")))),
				TBody(Group(tableRows)),
			),
		)
	}

	return core.AppPage("Storage: Locations", "storage", principal,
		storageSectionNav("locations"),
		sectionHeader("External locations", "Manage external storage locations backed by named credentials.", "/ui/storage/locations/new", "New location"),
		Div(Class("rounded-xl border border-border bg-background p-4 shadow-xs"),
			table,
			P(Class("mt-4 text-sm text-muted"), Text("Showing up to "+strconv.Itoa(page.MaxResults)+" locations. Total: "+strconv.FormatInt(total, 10))),
		),
	)
}

func storageVolumesListPage(principal domain.ContextPrincipal, catalogName, schemaName string, rows []storageVolumeRowData, page domain.PageRequest, total int64) Node {
	table := Node(P(Class("text-xs text-muted"), Text("Choose a catalog and schema to load volumes.")))
	if catalogName != "" && schemaName != "" {
		table = P(Class("text-xs text-muted"), Text("No volumes found for that catalog and schema."))
	}
	if len(rows) > 0 {
		tableRows := make([]Node, 0, len(rows))
		for i := range rows {
			row := rows[i]
			tableRows = append(tableRows, Tr(
				Td(A(Href(row.URL), Class("font-medium text-accent"), Text(row.Name))),
				Td(Text(row.VolumeType)),
				Td(Text(row.StorageLocation)),
				Td(Text(row.Owner)),
			))
		}
		table = Div(Class("overflow-x-auto"),
			Table(Class("min-w-full text-left text-sm"),
				THead(Tr(Th(Text("Name")), Th(Text("Type")), Th(Text("Location")), Th(Text("Owner")))),
				TBody(Group(tableRows)),
			),
		)
	}

	return core.AppPage("Storage: Volumes", "storage", principal,
		storageSectionNav("volumes"),
		sectionHeader("Volumes", "Create and manage governed storage volumes within catalog schemas.", "/ui/storage/volumes/new", "New volume"),
		Div(Class("rounded-xl border border-border bg-background p-4 shadow-xs"),
			Form(Class("grid gap-3 md:grid-cols-2 md:items-end"), Method("get"), Action("/ui/storage/volumes"),
				Div(Label(Text("Catalog")), core.InputControl("", Name("catalog"), Value(catalogName))),
				Div(Label(Text("Schema")), core.InputControl("", Name("schema"), Value(schemaName))),
				Div(Class("md:col-span-2"), core.SecondaryButton("", Type("submit"), Text("Load volumes"))),
			),
		),
		Div(Class("rounded-xl border border-border bg-background p-4 shadow-xs"),
			table,
			P(Class("mt-4 text-sm text-muted"), Text("Showing up to "+strconv.Itoa(page.MaxResults)+" volumes. Total: "+strconv.FormatInt(total, 10))),
		),
	)
}

func storageCredentialDetailPage(principal domain.ContextPrincipal, item *domain.StorageCredential, csrfFieldProvider func() Node) Node {
	return core.AppPage("Storage Credential: "+item.Name, "storage", principal,
		storageSectionNav("credentials"),
		Div(Class("rounded-xl border border-border bg-background p-4 shadow-xs"),
			sectionTitle("Credential details"),
			detailMeta("Type", string(item.CredentialType)),
			detailMeta("Owner", fallbackString(item.Owner, "unknown")),
			detailMeta("Comment", fallbackString(item.Comment, "-")),
			detailMeta("Updated", formatTime(item.UpdatedAt)),
			Div(Class("mt-1 flex flex-wrap items-center gap-2 [&_form]:m-0 [&_form]:inline-flex"),
				core.SecondaryLink("/ui/storage/credentials/"+url.PathEscape(item.Name)+"/edit", "", Text("Edit")),
				Form(Method("post"), Action("/ui/storage/credentials/"+url.PathEscape(item.Name)+"/delete"), csrfFieldProvider(), core.DangerButton("", Type("submit"), Text("Delete"))),
			),
		),
	)
}

func storageLocationDetailPage(principal domain.ContextPrincipal, item *domain.ExternalLocation, csrfFieldProvider func() Node) Node {
	return core.AppPage("External Location: "+item.Name, "storage", principal,
		storageSectionNav("locations"),
		Div(Class("rounded-xl border border-border bg-background p-4 shadow-xs"),
			sectionTitle("Location details"),
			detailMeta("URL", item.URL),
			detailMeta("Credential", item.CredentialName),
			detailMeta("Type", string(item.StorageType)),
			detailMeta("Read only", strconv.FormatBool(item.ReadOnly)),
			detailMeta("Owner", fallbackString(item.Owner, "unknown")),
			detailMeta("Comment", fallbackString(item.Comment, "-")),
			Div(Class("mt-1 flex flex-wrap items-center gap-2 [&_form]:m-0 [&_form]:inline-flex"),
				core.SecondaryLink("/ui/storage/locations/"+url.PathEscape(item.Name)+"/edit", "", Text("Edit")),
				Form(Method("post"), Action("/ui/storage/locations/"+url.PathEscape(item.Name)+"/delete"), csrfFieldProvider(), core.DangerButton("", Type("submit"), Text("Delete"))),
			),
		),
	)
}

func storageVolumeDetailPage(principal domain.ContextPrincipal, item *domain.Volume, csrfFieldProvider func() Node) Node {
	catalogPath := url.PathEscape(item.CatalogName)
	schemaPath := url.PathEscape(item.SchemaName)
	namePath := url.PathEscape(item.Name)

	return core.AppPage("Volume: "+item.Name, "storage", principal,
		storageSectionNav("volumes"),
		Div(Class("rounded-xl border border-border bg-background p-4 shadow-xs"),
			sectionTitle("Volume details"),
			detailMeta("Catalog", item.CatalogName),
			detailMeta("Schema", item.SchemaName),
			detailMeta("Type", item.VolumeType),
			detailMeta("Location", item.StorageLocation),
			detailMeta("Owner", fallbackString(item.Owner, "unknown")),
			detailMeta("Comment", fallbackString(item.Comment, "-")),
			Div(Class("mt-1 flex flex-wrap items-center gap-2 [&_form]:m-0 [&_form]:inline-flex"),
				core.SecondaryLink("/ui/storage/volumes/"+catalogPath+"/"+schemaPath+"/"+namePath+"/edit", "", Text("Edit")),
				Form(Method("post"), Action("/ui/storage/volumes/"+catalogPath+"/"+schemaPath+"/"+namePath+"/delete"), csrfFieldProvider(), core.DangerButton("", Type("submit"), Text("Delete"))),
			),
		),
	)
}

func storageCredentialFormPage(principal domain.ContextPrincipal, title, action string, item *domain.StorageCredential, csrfFieldProvider func() Node) Node {
	credentialType := string(domain.CredentialTypeS3)
	if item != nil {
		credentialType = string(item.CredentialType)
	}
	return storageFormPage(principal, title, action, csrfFieldProvider,
		Label(Text("Name")),
		core.InputControl("", Name("name"), Value(optionalCredentialName(item)), Required()),
		Label(Text("Credential type")),
		core.SelectControl("", Name("credential_type"),
			optionSelected(string(domain.CredentialTypeS3), credentialType),
			optionSelected(string(domain.CredentialTypeAzure), credentialType),
			optionSelected(string(domain.CredentialTypeGCS), credentialType),
		),
		Label(Text("Comment")),
		core.TextareaControl("min-h-24", Name("comment"), Text(optionalCredentialComment(item))),
		Label(Text("S3 Key ID")),
		core.InputControl("", Name("key_id"), Value(optionalCredentialValue(item, "key_id"))),
		Label(Text("S3 Secret")),
		core.InputControl("", Name("secret"), Value(optionalCredentialValue(item, "secret"))),
		Label(Text("S3 Endpoint")),
		core.InputControl("", Name("endpoint"), Value(optionalCredentialValue(item, "endpoint"))),
		Label(Text("S3 Region")),
		core.InputControl("", Name("region"), Value(optionalCredentialValue(item, "region"))),
		Label(Text("S3 URL Style")),
		core.InputControl("", Name("url_style"), Value(optionalCredentialValue(item, "url_style"))),
		Label(Text("Azure Account Name")),
		core.InputControl("", Name("azure_account_name"), Value(optionalCredentialValue(item, "azure_account_name"))),
		Label(Text("Azure Account Key")),
		core.InputControl("", Name("azure_account_key"), Value(optionalCredentialValue(item, "azure_account_key"))),
		Label(Text("Azure Client ID")),
		core.InputControl("", Name("azure_client_id"), Value(optionalCredentialValue(item, "azure_client_id"))),
		Label(Text("Azure Tenant ID")),
		core.InputControl("", Name("azure_tenant_id"), Value(optionalCredentialValue(item, "azure_tenant_id"))),
		Label(Text("Azure Client Secret")),
		core.InputControl("", Name("azure_client_secret"), Value(optionalCredentialValue(item, "azure_client_secret"))),
		Label(Text("GCS Key File Path")),
		core.InputControl("", Name("gcs_key_file_path"), Value(optionalCredentialValue(item, "gcs"))),
	)
}

func storageLocationFormPage(principal domain.ContextPrincipal, title, action string, item *domain.ExternalLocation, csrfFieldProvider func() Node) Node {
	storageType := string(domain.StorageTypeS3)
	if item != nil {
		storageType = string(item.StorageType)
	}
	readOnly := []Node{Type("checkbox"), Name("read_only"), Class("h-4 w-4")}
	if item != nil && item.ReadOnly {
		readOnly = append(readOnly, Checked())
	}

	return storageFormPage(principal, title, action, csrfFieldProvider,
		Label(Text("Name")),
		core.InputControl("", Name("name"), Value(optionalLocationName(item)), Required()),
		Label(Text("URL")),
		core.InputControl("", Name("url"), Value(optionalLocationURL(item)), Required()),
		Label(Text("Credential name")),
		core.InputControl("", Name("credential_name"), Value(optionalLocationCredential(item)), Required()),
		Label(Text("Storage type")),
		core.SelectControl("", Name("storage_type"),
			optionSelected(string(domain.StorageTypeS3), storageType),
			optionSelected(string(domain.StorageTypeAzure), storageType),
			optionSelected(string(domain.StorageTypeGCS), storageType),
		),
		Label(Text("Comment")),
		core.TextareaControl("min-h-24", Name("comment"), Text(optionalLocationComment(item))),
		Label(Class("inline-flex items-center gap-2"), Input(readOnly...), Span(Text("Read only"))),
	)
}

func storageVolumeFormPage(principal domain.ContextPrincipal, title, action string, item *domain.Volume, csrfFieldProvider func() Node) Node {
	volumeType := domain.VolumeTypeManaged
	if item != nil {
		volumeType = item.VolumeType
	}
	return storageFormPage(principal, title, action, csrfFieldProvider,
		Label(Text("Catalog name")),
		core.InputControl("", Name("catalog_name"), Value(optionalVolumeCatalog(item)), Required()),
		Label(Text("Schema name")),
		core.InputControl("", Name("schema_name"), Value(optionalVolumeSchema(item)), Required()),
		Label(Text("Name")),
		core.InputControl("", Name("name"), Value(optionalVolumeName(item)), Required()),
		Label(Text("Volume type")),
		core.SelectControl("", Name("volume_type"),
			optionSelected(domain.VolumeTypeManaged, volumeType),
			optionSelected(domain.VolumeTypeExternal, volumeType),
		),
		Label(Text("Storage location")),
		core.InputControl("", Name("storage_location"), Value(optionalVolumeLocation(item))),
		Label(Text("Comment")),
		core.TextareaControl("min-h-24", Name("comment"), Text(optionalVolumeComment(item))),
	)
}

func storageFormPage(principal domain.ContextPrincipal, title, action string, csrfFieldProvider func() Node, fields ...Node) Node {
	nodes := []Node{csrfFieldProvider()}
	nodes = append(nodes, fields...)
	nodes = append(nodes, Div(Class("mt-4"), core.PrimaryButton("", Type("submit"), Text("Save"))))

	return core.AppPage(title, "storage", principal,
		storageSectionNav(""),
		core.Card(
			Form(Class("grid gap-3"), Method("post"), Action(action), Group(nodes)),
		),
	)
}

func storageSectionNav(active string) Node {
	return Div(Class("rounded-xl border border-border bg-background p-4 shadow-xs"),
		Div(Class("flex flex-wrap gap-2"),
			navButton("Credentials", "/ui/storage/credentials", active == "credentials"),
			navButton("Locations", "/ui/storage/locations", active == "locations"),
			navButton("Volumes", "/ui/storage/volumes", active == "volumes"),
		),
	)
}

func navButton(label, href string, active bool) Node {
	if active {
		return core.PrimaryLink(href, "", Text(label))
	}
	return core.SecondaryLink(href, "", Text(label))
}

func sectionHeader(title, copy, href, action string) Node {
	return Div(Class("rounded-xl border border-border bg-background p-4 shadow-xs"),
		Div(Class("flex flex-wrap items-start justify-between gap-3"),
			Div(H2(Class("m-0 text-xl font-semibold"), Text(title)), P(Class("m-0 text-sm text-muted"), Text(copy))),
			core.PrimaryLink(href, "", Text(action)),
		),
	)
}

func storageCard(title, copy, href string) Node {
	return Div(Class("rounded-xl border border-border bg-background p-4 shadow-xs"),
		H2(Class("mt-0 text-lg font-semibold"), Text(title)),
		P(Class("text-sm text-muted"), Text(copy)),
		core.SecondaryLink(href, "", Text("Open")),
	)
}

func sectionTitle(value string) Node {
	return H2(Class("mt-0 text-lg font-semibold"), Text(value))
}

func detailMeta(label, value string) Node {
	return P(Class("m-0 text-sm"), Strong(Text(label+": ")), Text(value))
}

func statusPill(text, tone string) Node {
	className := "inline-flex items-center rounded-full px-2.5 py-1 text-xs font-medium"
	switch tone {
	case "success":
		className += " bg-success-muted text-success-text"
	case "attention":
		className += " bg-warning-muted text-warning-text"
	default:
		className += " bg-surface-muted text-foreground"
	}
	return Span(Class(className), Text(text))
}

func optionSelected(value, current string) Node {
	if value == current {
		return Option(Value(value), Selected(), Text(value))
	}
	return Option(Value(value), Text(value))
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
