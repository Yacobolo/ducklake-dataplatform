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
	table := Node(P(Class(core.MutedClass()), Text("No storage credentials found.")))
	if len(rows) > 0 {
		tableRows := make([]Node, 0, len(rows))
		for i := range rows {
			row := rows[i]
			tableRows = append(tableRows, Tr(
				Td(A(Href(row.URL), Class("font-medium text-[var(--fgColor-accent)]"), Text(row.Name))),
				Td(Text(row.Type)),
				Td(Text(row.Owner)),
				Td(Text(row.Updated)),
			))
		}
		table = Div(Class(core.TableWrapClass()),
			Table(Class("min-w-full text-left text-sm"),
				THead(Tr(Th(Text("Name")), Th(Text("Type")), Th(Text("Owner")), Th(Text("Updated")))),
				TBody(Group(tableRows)),
			),
		)
	}

	return core.AppPage("Storage: Credentials", "storage", principal,
		storageSectionNav("credentials"),
		sectionHeader("Storage credentials", "Create and manage governed cloud storage credentials.", "/ui/storage/credentials/new", "New credential"),
		Div(Class(core.CardClass()),
			table,
			P(Class("mt-4 text-sm text-[var(--fgColor-muted)]"), Text("Showing up to "+strconv.Itoa(page.MaxResults)+" credentials. Total: "+strconv.FormatInt(total, 10))),
		),
	)
}

func storageLocationsListPage(principal domain.ContextPrincipal, rows []storageLocationRowData, page domain.PageRequest, total int64) Node {
	table := Node(P(Class(core.MutedClass()), Text("No external locations found.")))
	if len(rows) > 0 {
		tableRows := make([]Node, 0, len(rows))
		for i := range rows {
			row := rows[i]
			tableRows = append(tableRows, Tr(
				Td(A(Href(row.URL), Class("font-medium text-[var(--fgColor-accent)]"), Text(row.Name))),
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
		table = Div(Class(core.TableWrapClass()),
			Table(Class("min-w-full text-left text-sm"),
				THead(Tr(Th(Text("Name")), Th(Text("URL")), Th(Text("Credential")), Th(Text("Read Only")))),
				TBody(Group(tableRows)),
			),
		)
	}

	return core.AppPage("Storage: Locations", "storage", principal,
		storageSectionNav("locations"),
		sectionHeader("External locations", "Manage external storage locations backed by named credentials.", "/ui/storage/locations/new", "New location"),
		Div(Class(core.CardClass()),
			table,
			P(Class("mt-4 text-sm text-[var(--fgColor-muted)]"), Text("Showing up to "+strconv.Itoa(page.MaxResults)+" locations. Total: "+strconv.FormatInt(total, 10))),
		),
	)
}

func storageVolumesListPage(principal domain.ContextPrincipal, catalogName, schemaName string, rows []storageVolumeRowData, page domain.PageRequest, total int64) Node {
	table := Node(P(Class(core.MutedClass()), Text("Choose a catalog and schema to load volumes.")))
	if catalogName != "" && schemaName != "" {
		table = P(Class(core.MutedClass()), Text("No volumes found for that catalog and schema."))
	}
	if len(rows) > 0 {
		tableRows := make([]Node, 0, len(rows))
		for i := range rows {
			row := rows[i]
			tableRows = append(tableRows, Tr(
				Td(A(Href(row.URL), Class("font-medium text-[var(--fgColor-accent)]"), Text(row.Name))),
				Td(Text(row.VolumeType)),
				Td(Text(row.StorageLocation)),
				Td(Text(row.Owner)),
			))
		}
		table = Div(Class(core.TableWrapClass()),
			Table(Class("min-w-full text-left text-sm"),
				THead(Tr(Th(Text("Name")), Th(Text("Type")), Th(Text("Location")), Th(Text("Owner")))),
				TBody(Group(tableRows)),
			),
		)
	}

	return core.AppPage("Storage: Volumes", "storage", principal,
		storageSectionNav("volumes"),
		sectionHeader("Volumes", "Create and manage governed storage volumes within catalog schemas.", "/ui/storage/volumes/new", "New volume"),
		Div(Class(core.CardClass()),
			Form(Class("grid gap-3 md:grid-cols-2 md:items-end"), Method("get"), Action("/ui/storage/volumes"),
				Div(Label(Text("Catalog")), Input(Name("catalog"), Value(catalogName), Class(core.FormControlClass()))),
				Div(Label(Text("Schema")), Input(Name("schema"), Value(schemaName), Class(core.FormControlClass()))),
				Div(Class("md:col-span-2"), Button(Type("submit"), Class(core.SecondaryButtonClass()), Text("Load volumes"))),
			),
		),
		Div(Class(core.CardClass()),
			table,
			P(Class("mt-4 text-sm text-[var(--fgColor-muted)]"), Text("Showing up to "+strconv.Itoa(page.MaxResults)+" volumes. Total: "+strconv.FormatInt(total, 10))),
		),
	)
}

func storageCredentialDetailPage(principal domain.ContextPrincipal, item *domain.StorageCredential, csrfFieldProvider func() Node) Node {
	return core.AppPage("Storage Credential: "+item.Name, "storage", principal,
		storageSectionNav("credentials"),
		Div(Class(core.CardClass()),
			sectionTitle("Credential details"),
			detailMeta("Type", string(item.CredentialType)),
			detailMeta("Owner", fallbackString(item.Owner, "unknown")),
			detailMeta("Comment", fallbackString(item.Comment, "-")),
			detailMeta("Updated", formatTime(item.UpdatedAt)),
			Div(Class(core.ButtonRowClass()),
				A(Href("/ui/storage/credentials/"+url.PathEscape(item.Name)+"/edit"), Class(core.SecondaryButtonClass()), Text("Edit")),
				Form(Method("post"), Action("/ui/storage/credentials/"+url.PathEscape(item.Name)+"/delete"), csrfFieldProvider(), Button(Type("submit"), Class(core.DangerButtonClass()), Text("Delete"))),
			),
		),
	)
}

func storageLocationDetailPage(principal domain.ContextPrincipal, item *domain.ExternalLocation, csrfFieldProvider func() Node) Node {
	return core.AppPage("External Location: "+item.Name, "storage", principal,
		storageSectionNav("locations"),
		Div(Class(core.CardClass()),
			sectionTitle("Location details"),
			detailMeta("URL", item.URL),
			detailMeta("Credential", item.CredentialName),
			detailMeta("Type", string(item.StorageType)),
			detailMeta("Read only", strconv.FormatBool(item.ReadOnly)),
			detailMeta("Owner", fallbackString(item.Owner, "unknown")),
			detailMeta("Comment", fallbackString(item.Comment, "-")),
			Div(Class(core.ButtonRowClass()),
				A(Href("/ui/storage/locations/"+url.PathEscape(item.Name)+"/edit"), Class(core.SecondaryButtonClass()), Text("Edit")),
				Form(Method("post"), Action("/ui/storage/locations/"+url.PathEscape(item.Name)+"/delete"), csrfFieldProvider(), Button(Type("submit"), Class(core.DangerButtonClass()), Text("Delete"))),
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
		Div(Class(core.CardClass()),
			sectionTitle("Volume details"),
			detailMeta("Catalog", item.CatalogName),
			detailMeta("Schema", item.SchemaName),
			detailMeta("Type", item.VolumeType),
			detailMeta("Location", item.StorageLocation),
			detailMeta("Owner", fallbackString(item.Owner, "unknown")),
			detailMeta("Comment", fallbackString(item.Comment, "-")),
			Div(Class(core.ButtonRowClass()),
				A(Href("/ui/storage/volumes/"+catalogPath+"/"+schemaPath+"/"+namePath+"/edit"), Class(core.SecondaryButtonClass()), Text("Edit")),
				Form(Method("post"), Action("/ui/storage/volumes/"+catalogPath+"/"+schemaPath+"/"+namePath+"/delete"), csrfFieldProvider(), Button(Type("submit"), Class(core.DangerButtonClass()), Text("Delete"))),
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
		Input(Name("name"), Value(optionalCredentialName(item)), Required(), Class(core.FormControlClass())),
		Label(Text("Credential type")),
		Select(Name("credential_type"), Class(core.FormControlClass()),
			optionSelected(string(domain.CredentialTypeS3), credentialType),
			optionSelected(string(domain.CredentialTypeAzure), credentialType),
			optionSelected(string(domain.CredentialTypeGCS), credentialType),
		),
		Label(Text("Comment")),
		Textarea(Name("comment"), Class(core.FormControlClass("min-h-24")), Text(optionalCredentialComment(item))),
		Label(Text("S3 Key ID")),
		Input(Name("key_id"), Value(optionalCredentialValue(item, "key_id")), Class(core.FormControlClass())),
		Label(Text("S3 Secret")),
		Input(Name("secret"), Value(optionalCredentialValue(item, "secret")), Class(core.FormControlClass())),
		Label(Text("S3 Endpoint")),
		Input(Name("endpoint"), Value(optionalCredentialValue(item, "endpoint")), Class(core.FormControlClass())),
		Label(Text("S3 Region")),
		Input(Name("region"), Value(optionalCredentialValue(item, "region")), Class(core.FormControlClass())),
		Label(Text("S3 URL Style")),
		Input(Name("url_style"), Value(optionalCredentialValue(item, "url_style")), Class(core.FormControlClass())),
		Label(Text("Azure Account Name")),
		Input(Name("azure_account_name"), Value(optionalCredentialValue(item, "azure_account_name")), Class(core.FormControlClass())),
		Label(Text("Azure Account Key")),
		Input(Name("azure_account_key"), Value(optionalCredentialValue(item, "azure_account_key")), Class(core.FormControlClass())),
		Label(Text("Azure Client ID")),
		Input(Name("azure_client_id"), Value(optionalCredentialValue(item, "azure_client_id")), Class(core.FormControlClass())),
		Label(Text("Azure Tenant ID")),
		Input(Name("azure_tenant_id"), Value(optionalCredentialValue(item, "azure_tenant_id")), Class(core.FormControlClass())),
		Label(Text("Azure Client Secret")),
		Input(Name("azure_client_secret"), Value(optionalCredentialValue(item, "azure_client_secret")), Class(core.FormControlClass())),
		Label(Text("GCS Key File Path")),
		Input(Name("gcs_key_file_path"), Value(optionalCredentialValue(item, "gcs")), Class(core.FormControlClass())),
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
		Input(Name("name"), Value(optionalLocationName(item)), Required(), Class(core.FormControlClass())),
		Label(Text("URL")),
		Input(Name("url"), Value(optionalLocationURL(item)), Required(), Class(core.FormControlClass())),
		Label(Text("Credential name")),
		Input(Name("credential_name"), Value(optionalLocationCredential(item)), Required(), Class(core.FormControlClass())),
		Label(Text("Storage type")),
		Select(Name("storage_type"), Class(core.FormControlClass()),
			optionSelected(string(domain.StorageTypeS3), storageType),
			optionSelected(string(domain.StorageTypeAzure), storageType),
			optionSelected(string(domain.StorageTypeGCS), storageType),
		),
		Label(Text("Comment")),
		Textarea(Name("comment"), Class(core.FormControlClass("min-h-24")), Text(optionalLocationComment(item))),
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
		Input(Name("catalog_name"), Value(optionalVolumeCatalog(item)), Required(), Class(core.FormControlClass())),
		Label(Text("Schema name")),
		Input(Name("schema_name"), Value(optionalVolumeSchema(item)), Required(), Class(core.FormControlClass())),
		Label(Text("Name")),
		Input(Name("name"), Value(optionalVolumeName(item)), Required(), Class(core.FormControlClass())),
		Label(Text("Volume type")),
		Select(Name("volume_type"), Class(core.FormControlClass()),
			optionSelected(domain.VolumeTypeManaged, volumeType),
			optionSelected(domain.VolumeTypeExternal, volumeType),
		),
		Label(Text("Storage location")),
		Input(Name("storage_location"), Value(optionalVolumeLocation(item)), Class(core.FormControlClass())),
		Label(Text("Comment")),
		Textarea(Name("comment"), Class(core.FormControlClass("min-h-24")), Text(optionalVolumeComment(item))),
	)
}

func storageFormPage(principal domain.ContextPrincipal, title, action string, csrfFieldProvider func() Node, fields ...Node) Node {
	nodes := []Node{csrfFieldProvider()}
	nodes = append(nodes, fields...)
	nodes = append(nodes, Div(Class("mt-4"), Button(Type("submit"), Class(core.PrimaryButtonClass()), Text("Save"))))

	return core.AppPage(title, "storage", principal,
		storageSectionNav(""),
		Div(Class(core.CardClass()),
			Form(Class("grid gap-3"), Method("post"), Action(action), Group(nodes)),
		),
	)
}

func storageSectionNav(active string) Node {
	return Div(Class(core.CardClass()),
		Div(Class("flex flex-wrap gap-2"),
			navButton("Credentials", "/ui/storage/credentials", active == "credentials"),
			navButton("Locations", "/ui/storage/locations", active == "locations"),
			navButton("Volumes", "/ui/storage/volumes", active == "volumes"),
		),
	)
}

func navButton(label, href string, active bool) Node {
	className := core.SecondaryButtonClass()
	if active {
		className = core.PrimaryButtonClass()
	}
	return A(Href(href), Class(className), Text(label))
}

func sectionHeader(title, copy, href, action string) Node {
	return Div(Class(core.CardClass()),
		Div(Class("flex flex-wrap items-start justify-between gap-3"),
			Div(H2(Class("m-0 text-xl font-semibold"), Text(title)), P(Class("m-0 text-sm text-[var(--fgColor-muted)]"), Text(copy))),
			A(Href(href), Class(core.PrimaryButtonClass()), Text(action)),
		),
	)
}

func storageCard(title, copy, href string) Node {
	return Div(Class(core.CardClass()),
		H2(Class("mt-0 text-lg font-semibold"), Text(title)),
		P(Class("text-sm text-[var(--fgColor-muted)]"), Text(copy)),
		A(Href(href), Class(core.SecondaryButtonClass()), Text("Open")),
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
		className += " bg-[var(--bgColor-success-muted)] text-[var(--fgColor-success)]"
	case "attention":
		className += " bg-[var(--bgColor-attention-muted)] text-[var(--fgColor-attention)]"
	default:
		className += " bg-[var(--bgColor-muted)] text-[var(--fgColor-default)]"
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
