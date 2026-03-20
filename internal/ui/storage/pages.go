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
		core.ListPageLayout(
			core.PageHeader("Operate", "Storage workspaces", "Manage storage credentials, external locations, and governed volumes from the same operating surface."),
			core.SectionSurface(
				core.SectionHeader("Choose a storage workspace", "Each area maps to a concrete operational job instead of a hub of equal-weight cards."),
				core.ItemList("md:grid-cols-3",
					core.ItemListEntry(
						Div(Class("flex flex-wrap items-center justify-between gap-3"),
							Div(Class("grid gap-1"),
								Strong(Text("Credentials")),
								P(Class("m-0 text-sm text-[var(--fgColor-muted)]"), Text("Create and manage governed cloud storage credentials.")),
							),
							core.SecondaryLink("/ui/storage/credentials", "small", Text("Open")),
						),
					),
					core.ItemListEntry(
						Div(Class("flex flex-wrap items-center justify-between gap-3"),
							Div(Class("grid gap-1"),
								Strong(Text("Locations")),
								P(Class("m-0 text-sm text-[var(--fgColor-muted)]"), Text("Manage external locations backed by named credentials.")),
							),
							core.SecondaryLink("/ui/storage/locations", "small", Text("Open")),
						),
					),
					core.ItemListEntry(
						Div(Class("flex flex-wrap items-center justify-between gap-3"),
							Div(Class("grid gap-1"),
								Strong(Text("Volumes")),
								P(Class("m-0 text-sm text-[var(--fgColor-muted)]"), Text("Create and manage governed storage volumes in catalog schemas.")),
							),
							core.SecondaryLink("/ui/storage/volumes", "small", Text("Open")),
						),
					),
				),
			),
		),
	)
}

func storageCredentialsListPage(principal domain.ContextPrincipal, rows []storageCredentialRowData, page domain.PageRequest, total int64) Node {
	table := Node(P(Class("text-xs text-[var(--fgColor-muted)]"), Text("No storage credentials found.")))
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
		table = core.TableContainer("",
			core.DataTable("",
				THead(Tr(Th(Scope("col"), Text("Name")), Th(Scope("col"), Text("Type")), Th(Scope("col"), Text("Owner")), Th(Scope("col"), Text("Updated")))),
				TBody(Group(tableRows)),
			),
		)
	}

	return core.AppPage("Storage: Credentials", "storage", principal,
		storageSectionNav("credentials"),
		core.ListPageLayout(
			core.ListPageHeader("Storage credentials", "Create and manage governed cloud storage credentials.", core.PrimaryLink("/ui/storage/credentials/new", "", Text("New credential"))),
			core.ListPageBody(
				table,
				core.ListPagination("/ui/storage/credentials", page, total),
			),
		),
	)
}

func storageLocationsListPage(principal domain.ContextPrincipal, rows []storageLocationRowData, page domain.PageRequest, total int64) Node {
	table := Node(P(Class("text-xs text-[var(--fgColor-muted)]"), Text("No external locations found.")))
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
		table = core.TableContainer("",
			core.DataTable("",
				THead(Tr(Th(Scope("col"), Text("Name")), Th(Scope("col"), Text("URL")), Th(Scope("col"), Text("Credential")), Th(Scope("col"), Text("Read Only")))),
				TBody(Group(tableRows)),
			),
		)
	}

	return core.AppPage("Storage: Locations", "storage", principal,
		storageSectionNav("locations"),
		core.ListPageLayout(
			core.ListPageHeader("External locations", "Manage external storage locations backed by named credentials.", core.PrimaryLink("/ui/storage/locations/new", "", Text("New location"))),
			core.ListPageBody(
				table,
				core.ListPagination("/ui/storage/locations", page, total),
			),
		),
	)
}

func storageVolumesListPage(principal domain.ContextPrincipal, catalogName, schemaName string, rows []storageVolumeRowData, page domain.PageRequest, total int64) Node {
	table := Node(P(Class("text-xs text-[var(--fgColor-muted)]"), Text("Choose a catalog and schema to load volumes.")))
	if catalogName != "" && schemaName != "" {
		table = P(Class("text-xs text-[var(--fgColor-muted)]"), Text("No volumes found for that catalog and schema."))
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
		table = core.TableContainer("",
			core.DataTable("",
				THead(Tr(Th(Scope("col"), Text("Name")), Th(Scope("col"), Text("Type")), Th(Scope("col"), Text("Location")), Th(Scope("col"), Text("Owner")))),
				TBody(Group(tableRows)),
			),
		)
	}

	return core.AppPage("Storage: Volumes", "storage", principal,
		storageSectionNav("volumes"),
		core.ListPageLayout(
			core.ListPageHeader("Volumes", "Create and manage governed storage volumes within catalog schemas.", core.PrimaryLink("/ui/storage/volumes/new", "", Text("New volume"))),
			core.SectionSurface(
				core.SectionHeader("Scope volumes", "Load the volume workspace for a specific catalog and schema before inspecting records."),
				Form(Class("grid gap-3 md:grid-cols-2 md:items-end"), Method("get"), Action("/ui/storage/volumes"),
					Div(Label(Text("Catalog")), core.InputControl("", Name("catalog"), Value(catalogName))),
					Div(Label(Text("Schema")), core.InputControl("", Name("schema"), Value(schemaName))),
					Div(Class("md:col-span-2"), core.SecondaryButton("", Type("submit"), Text("Load volumes"))),
				),
			),
			core.ListPageBody(
				table,
				core.ListPagination("/ui/storage/volumes", page, total),
			),
		),
	)
}

func storageCredentialDetailPage(principal domain.ContextPrincipal, item *domain.StorageCredential, csrfFieldProvider func() Node) Node {
	return core.AppPage("Storage Credential: "+item.Name, "storage", principal,
		storageSectionNav("credentials"),
		core.DetailShell(
			core.DetailHero(
				core.DetailHeroCopy(
					core.Kicker("Operate"),
					core.DetailTitle(item.Name),
					core.DetailDescription("Storage credentials hold the cloud auth and connection details that downstream locations rely on."),
				),
				core.DetailHeroMeta(
					core.BadgeRow(statusPill(string(item.CredentialType), "success")),
					core.DetailSummaryList([][2]string{
						{"Owner", fallbackString(item.Owner, "unknown")},
						{"Updated", formatTime(item.UpdatedAt)},
					}),
				),
			),
			core.DetailLayout(
				core.DetailMain(
					core.SectionSurface(
						core.SectionHeader("Credential details", "Review the persisted metadata before editing or deleting the credential."),
						core.KeyValueGrid([][2]string{
							{"Type", string(item.CredentialType)},
							{"Owner", fallbackString(item.Owner, "unknown")},
							{"Comment", fallbackString(item.Comment, "-")},
							{"Updated", formatTime(item.UpdatedAt)},
						}),
					),
				),
				core.DetailRail(
					core.DetailRailCard("Actions", "Use the rail for changes so the main column stays focused on current state.",
						core.ButtonGroup("",
							core.SecondaryLink("/ui/storage/credentials/"+url.PathEscape(item.Name)+"/edit", "", Text("Edit")),
							Form(Method("post"), Action("/ui/storage/credentials/"+url.PathEscape(item.Name)+"/delete"), csrfFieldProvider(), core.DangerButton("", Type("submit"), Text("Delete"))),
						),
					),
				),
			),
		),
	)
}

func storageLocationDetailPage(principal domain.ContextPrincipal, item *domain.ExternalLocation, csrfFieldProvider func() Node) Node {
	return core.AppPage("External Location: "+item.Name, "storage", principal,
		storageSectionNav("locations"),
		core.DetailShell(
			core.DetailHero(
				core.DetailHeroCopy(
					core.Kicker("Operate"),
					core.DetailTitle(item.Name),
					core.DetailDescription("External locations map a named credential onto a concrete storage URL for governed access."),
				),
				core.DetailHeroMeta(
					core.BadgeRow(statusPill(string(item.StorageType), "accent"), statusPill(strconv.FormatBool(item.ReadOnly), readOnlyTone(item.ReadOnly))),
					core.DetailSummaryList([][2]string{
						{"Credential", item.CredentialName},
						{"Owner", fallbackString(item.Owner, "unknown")},
					}),
				),
			),
			core.DetailLayout(
				core.DetailMain(
					core.SectionSurface(
						core.SectionHeader("Location details", "Keep metadata and configuration in the main column; move editing into the rail."),
						core.KeyValueGrid([][2]string{
							{"URL", item.URL},
							{"Credential", item.CredentialName},
							{"Type", string(item.StorageType)},
							{"Read only", strconv.FormatBool(item.ReadOnly)},
							{"Owner", fallbackString(item.Owner, "unknown")},
							{"Comment", fallbackString(item.Comment, "-")},
						}),
					),
				),
				core.DetailRail(
					core.DetailRailCard("Actions", "Editing and deletion stay secondary to the current configuration view.",
						core.ButtonGroup("",
							core.SecondaryLink("/ui/storage/locations/"+url.PathEscape(item.Name)+"/edit", "", Text("Edit")),
							Form(Method("post"), Action("/ui/storage/locations/"+url.PathEscape(item.Name)+"/delete"), csrfFieldProvider(), core.DangerButton("", Type("submit"), Text("Delete"))),
						),
					),
				),
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
		core.DetailShell(
			core.DetailHero(
				core.DetailHeroCopy(
					core.Kicker("Operate"),
					core.DetailTitle(item.Name),
					core.DetailDescription("Volumes package catalog storage into a governed object that teams can manage without scanning a long card stack."),
				),
				core.DetailHeroMeta(
					core.BadgeRow(statusPill(item.VolumeType, "success")),
					core.DetailSummaryList([][2]string{
						{"Catalog", item.CatalogName},
						{"Schema", item.SchemaName},
						{"Owner", fallbackString(item.Owner, "unknown")},
					}),
				),
			),
			core.DetailLayout(
				core.DetailMain(
					core.SectionSurface(
						core.SectionHeader("Volume details", "The primary column is reserved for the current storage configuration."),
						core.KeyValueGrid([][2]string{
							{"Catalog", item.CatalogName},
							{"Schema", item.SchemaName},
							{"Type", item.VolumeType},
							{"Location", item.StorageLocation},
							{"Owner", fallbackString(item.Owner, "unknown")},
							{"Comment", fallbackString(item.Comment, "-")},
						}),
					),
				),
				core.DetailRail(
					core.DetailRailCard("Actions", "Mutations stay in the rail to keep the page easy to scan.",
						core.ButtonGroup("",
							core.SecondaryLink("/ui/storage/volumes/"+catalogPath+"/"+schemaPath+"/"+namePath+"/edit", "", Text("Edit")),
							Form(Method("post"), Action("/ui/storage/volumes/"+catalogPath+"/"+schemaPath+"/"+namePath+"/delete"), csrfFieldProvider(), core.DangerButton("", Type("submit"), Text("Delete"))),
						),
					),
				),
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
	nodes = append(nodes, Div(Class("mt-2"), core.PrimaryButton("", Type("submit"), Text("Save"))))

	return core.AppPage(title, "storage", principal,
		storageSectionNav(""),
		core.FormPageLayout("Operate", title, "Use a single, focused form surface for storage configuration instead of stacking several generic cards.",
			Form(Class("grid gap-3"), Method("post"), Action(action), Group(nodes)),
		),
	)
}

func storageSectionNav(active string) Node {
	return core.SectionTabs([]core.SectionTab{
		{Label: "Credentials", Href: "/ui/storage/credentials", Active: active == "credentials"},
		{Label: "Locations", Href: "/ui/storage/locations", Active: active == "locations"},
		{Label: "Volumes", Href: "/ui/storage/volumes", Active: active == "volumes"},
	})
}

func statusPill(text, tone string) Node {
	className := "inline-flex items-center rounded-full px-2.5 py-1 text-xs font-medium"
	switch tone {
	case "success":
		className += " bg-[var(--bgColor-success-muted)] text-[var(--fgColor-success)]"
	case "attention":
		className += " bg-[var(--bgColor-attention-muted)] text-[var(--fgColor-attention)]"
	case "accent":
		className += " bg-[var(--bgColor-accent-muted)] text-[var(--fgColor-accent)]"
	default:
		className += " bg-[var(--bgColor-muted)] text-[var(--fgColor-default)]"
	}
	return Span(Class(className), Text(text))
}

func readOnlyTone(readOnly bool) string {
	if readOnly {
		return "attention"
	}
	return "success"
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
