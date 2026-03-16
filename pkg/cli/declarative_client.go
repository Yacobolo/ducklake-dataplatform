package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"reflect"
	"sort"
	"strings"

	"duck-demo/internal/declarative"
	"duck-demo/internal/domain"
	"duck-demo/pkg/cli/apiruntime"
)

// StateReader fetches current state from the server API.
type StateReader interface {
	ReadState(ctx context.Context) (*declarative.DesiredState, error)
}

// StateWriter applies changes to the server API.
type StateWriter interface {
	Execute(ctx context.Context, action declarative.Action) error
}

// CapabilityCompatibilityMode controls how tolerant the declarative client is
// of optional endpoint drift when reading state from older servers.
type CapabilityCompatibilityMode string

const (
	// CapabilityCompatibilityLegacy tolerates optional endpoint failures.
	CapabilityCompatibilityLegacy CapabilityCompatibilityMode = "legacy"
	// CapabilityCompatibilityStrict requires optional endpoints to succeed.
	CapabilityCompatibilityStrict CapabilityCompatibilityMode = "strict"
)

// APIStateClientOptions configures compatibility behavior for state reads.
type APIStateClientOptions struct {
	CompatibilityMode CapabilityCompatibilityMode
}

func normalizeCompatibilityMode(mode CapabilityCompatibilityMode) CapabilityCompatibilityMode {
	if mode == CapabilityCompatibilityStrict {
		return mode
	}
	return CapabilityCompatibilityLegacy
}

// resourceIndex maps human-readable names to API UUIDs. It is populated during
// ReadState and consumed by Execute methods that must resolve names to IDs.
type resourceIndex struct {
	principalIDByName     map[string]string // "alice" → UUID
	groupIDByName         map[string]string // "admins" → UUID
	grantIDByIdentity     map[string]string // effective grant identity → UUID
	catalogIDByName       map[string]string // "demo" → UUID
	semanticModelIDByPath map[string]string // "project.model" → UUID
	schemaIDByPath        map[string]string // "catalog.schema" → UUID
	tableIDByPath         map[string]string // "catalog.schema.table" → UUID
	volumeIDByPath        map[string]string // "catalog.schema.volume" → UUID
	locationIDByName      map[string]string // "external_location_name" → UUID
	credentialIDByName    map[string]string // "storage_credential_name" → UUID
	computeIDByName       map[string]string // "local" → UUID
	computeAssignIDByKey  map[string]string // "endpoint/principalType/principal" → UUID
	tagIDByKey            map[string]string // "pii" or "pii:value" → UUID
	rowFilterIDByPath     map[string]string // "cat.sch.tbl/filterName" → UUID
	columnMaskIDByPath    map[string]string // "cat.sch.tbl/maskName" → UUID
	notebookIDByName      map[string]string // "kpi_walkthrough" → UUID
	productDomainNameByID map[string]string // product domain UUID -> domain name
}

func newResourceIndex() *resourceIndex {
	return &resourceIndex{
		principalIDByName:     make(map[string]string),
		groupIDByName:         make(map[string]string),
		grantIDByIdentity:     make(map[string]string),
		catalogIDByName:       make(map[string]string),
		semanticModelIDByPath: make(map[string]string),
		schemaIDByPath:        make(map[string]string),
		tableIDByPath:         make(map[string]string),
		volumeIDByPath:        make(map[string]string),
		locationIDByName:      make(map[string]string),
		credentialIDByName:    make(map[string]string),
		computeIDByName:       make(map[string]string),
		computeAssignIDByKey:  make(map[string]string),
		tagIDByKey:            make(map[string]string),
		rowFilterIDByPath:     make(map[string]string),
		columnMaskIDByPath:    make(map[string]string),
		notebookIDByName:      make(map[string]string),
		productDomainNameByID: make(map[string]string),
	}
}

// APIStateClient implements both StateReader and StateWriter using the CLI runtime client.
type APIStateClient struct {
	client               *apiruntime.Client
	index                *resourceIndex
	compatibilityMode    CapabilityCompatibilityMode
	optionalReadWarnings []string
}

// Compile-time interface checks.
var (
	_ StateReader = (*APIStateClient)(nil)
	_ StateWriter = (*APIStateClient)(nil)
)

// NewAPIStateClient creates a new client adapter.
func NewAPIStateClient(client *apiruntime.Client) *APIStateClient {
	return NewAPIStateClientWithOptions(client, APIStateClientOptions{})
}

// NewAPIStateClientWithOptions creates a new client adapter with behavior options.
func NewAPIStateClientWithOptions(client *apiruntime.Client, options APIStateClientOptions) *APIStateClient {
	return &APIStateClient{
		client:            client,
		compatibilityMode: normalizeCompatibilityMode(options.CompatibilityMode),
	}
}

// listResponse is the generic JSON envelope for paginated list endpoints.
type listResponse struct {
	Data          json.RawMessage `json:"data"`
	NextPageToken string          `json:"next_page_token"`
}

// fetchAllPages fetches all pages from a paginated list endpoint.
// The dataKey param selects between the standard "data" key and alternate keys.
func (c *APIStateClient) fetchAllPages(_ context.Context, path string) ([]json.RawMessage, error) {
	var all []json.RawMessage
	pageToken := ""

	for {
		q := url.Values{}
		q.Set("max_results", "1000")
		if pageToken != "" {
			q.Set("page_token", pageToken)
		}

		resp, err := c.client.Do(http.MethodGet, path, q, nil)
		if err != nil {
			return nil, fmt.Errorf("GET %s: %w", path, err)
		}

		body, err := apiruntime.ReadBody(resp)
		if err != nil {
			return nil, fmt.Errorf("read GET %s: %w", path, err)
		}

		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, fmt.Errorf("GET %s: HTTP %d: %s", path, resp.StatusCode, string(body))
		}

		var lr listResponse
		if err := json.Unmarshal(body, &lr); err != nil {
			return nil, fmt.Errorf("parse GET %s: %w", path, err)
		}

		if len(lr.Data) > 0 && string(lr.Data) != "null" {
			all = append(all, lr.Data)
		}

		if lr.NextPageToken == "" {
			break
		}
		pageToken = lr.NextPageToken
	}

	return all, nil
}

// mergePages concatenates multiple JSON arrays into a single slice.
func mergePages(pages []json.RawMessage, target interface{}) error {
	// Build one big JSON array from all pages.
	var combined []json.RawMessage
	for _, page := range pages {
		var items []json.RawMessage
		if err := json.Unmarshal(page, &items); err != nil {
			return fmt.Errorf("merge pages: %w", err)
		}
		combined = append(combined, items...)
	}

	data, err := json.Marshal(combined)
	if err != nil {
		return fmt.Errorf("marshal merged pages: %w", err)
	}
	return json.Unmarshal(data, target)
}

// === ReadState ===

// ReadState fetches the current server state and returns it as a DesiredState.
// It also populates the internal resource index for name→ID resolution during Execute.
func (c *APIStateClient) ReadState(ctx context.Context) (*declarative.DesiredState, error) {
	c.index = newResourceIndex()
	c.optionalReadWarnings = nil
	state := &declarative.DesiredState{}

	if err := c.readPrincipals(ctx, state); err != nil {
		return nil, fmt.Errorf("read principals: %w", err)
	}
	if err := c.readGroups(ctx, state); err != nil {
		return nil, fmt.Errorf("read groups: %w", err)
	}
	if err := c.readAPIKeys(ctx, state); err != nil {
		return nil, fmt.Errorf("read api keys: %w", err)
	}
	if err := c.readCatalogs(ctx, state); err != nil {
		return nil, fmt.Errorf("read catalogs: %w", err)
	}
	if err := c.readStorageCredentials(ctx, state); err != nil {
		return nil, fmt.Errorf("read storage credentials: %w", err)
	}
	if err := c.readExternalLocations(ctx, state); err != nil {
		return nil, fmt.Errorf("read external locations: %w", err)
	}
	if err := c.readGrants(ctx, state); err != nil {
		return nil, fmt.Errorf("read grants: %w", err)
	}
	if err := c.readComputeEndpoints(ctx, state); err != nil {
		return nil, fmt.Errorf("read compute endpoints: %w", err)
	}
	if err := c.readTags(ctx, state); err != nil {
		return nil, fmt.Errorf("read tags: %w", err)
	}
	if err := c.readNotebooks(ctx, state); err != nil {
		return nil, fmt.Errorf("read notebooks: %w", err)
	}
	if err := c.readDomains(ctx, state); err != nil {
		if !c.isOptionalReadError(err) {
			return nil, fmt.Errorf("read domains: %w", err)
		}
	} else {
		if err := c.readTeams(ctx, state); err != nil {
			if !c.isOptionalReadError(err) {
				return nil, fmt.Errorf("read teams: %w", err)
			}
		}
		if err := c.readDataProducts(ctx, state); err != nil {
			if !c.isOptionalReadError(err) {
				return nil, fmt.Errorf("read data products: %w", err)
			}
		}
	}
	if err := c.readAssets(ctx, state); err != nil {
		if !c.isOptionalReadError(err) {
			return nil, fmt.Errorf("read assets: %w", err)
		}
		c.addOptionalReadWarning("assets", err)
	}
	if err := c.readMacros(ctx, state); err != nil {
		if !c.isOptionalReadError(err) {
			return nil, fmt.Errorf("read macros: %w", err)
		}
		c.addOptionalReadWarning("macros", err)
	}
	if err := c.readModels(ctx, state); err != nil {
		if !c.isOptionalReadError(err) {
			return nil, fmt.Errorf("read models: %w", err)
		}
		c.addOptionalReadWarning("models", err)
	}
	if err := c.readSemanticModels(ctx, state); err != nil {
		return nil, fmt.Errorf("read semantic models: %w", err)
	}

	return state, nil
}

// --- Security resources ---

type apiPrincipal struct {
	ID      string `json:"id"`
	Name    string `json:"name"`
	Type    string `json:"type"`
	IsAdmin bool   `json:"is_admin"`
}

func (c *APIStateClient) readPrincipals(ctx context.Context, state *declarative.DesiredState) error {
	pages, err := c.fetchAllPages(ctx, "/principals")
	if err != nil {
		return err
	}
	if len(pages) == 0 {
		return nil
	}

	var items []apiPrincipal
	if err := mergePages(pages, &items); err != nil {
		return err
	}

	for _, p := range items {
		state.Principals = append(state.Principals, declarative.PrincipalSpec{
			Name:    p.Name,
			Type:    p.Type,
			IsAdmin: p.IsAdmin,
		})
		if p.ID != "" && c.index != nil {
			c.index.principalIDByName[p.Name] = p.ID
		}
	}
	return nil
}

type apiGroup struct {
	ID          string `json:"id"`
	Name        string `json:"name"`
	Description string `json:"description"`
}

type apiGroupMember struct {
	MemberID   string `json:"member_id"`
	MemberType string `json:"member_type"`
}

func (c *APIStateClient) readGroups(ctx context.Context, state *declarative.DesiredState) error {
	pages, err := c.fetchAllPages(ctx, "/groups")
	if err != nil {
		return err
	}
	if len(pages) == 0 {
		return nil
	}

	var items []apiGroup
	if err := mergePages(pages, &items); err != nil {
		return err
	}

	for _, g := range items {
		spec := declarative.GroupSpec{
			Name:        g.Name,
			Description: g.Description,
		}

		// Fetch members for this group.
		memberPages, err := c.fetchAllPages(ctx, "/groups/"+g.ID+"/members")
		if err != nil {
			return fmt.Errorf("group %q members: %w", g.Name, err)
		}
		if len(memberPages) > 0 {
			var members []apiGroupMember
			if err := mergePages(memberPages, &members); err != nil {
				return fmt.Errorf("group %q members parse: %w", g.Name, err)
			}
			for _, m := range members {
				name := c.reverseLookupPrincipalName(m.MemberID, m.MemberType)
				if name == "" {
					resolvedName, err := c.lookupMemberNameByID(ctx, m.MemberID, m.MemberType)
					if err != nil {
						return fmt.Errorf("group %q member %s (%s): %w", g.Name, m.MemberID, m.MemberType, err)
					}
					name = resolvedName
				}
				spec.Members = append(spec.Members, declarative.MemberRef{
					Name:     name,
					Type:     m.MemberType,
					MemberID: m.MemberID,
				})
			}
		}

		state.Groups = append(state.Groups, spec)
		if g.ID != "" && c.index != nil {
			c.index.groupIDByName[g.Name] = g.ID
		}
	}
	return nil
}

type apiGrant struct {
	ID            string `json:"id"`
	PrincipalID   string `json:"principal_id"`
	PrincipalType string `json:"principal_type"`
	SecurableType string `json:"securable_type"`
	SecurableID   string `json:"securable_id"`
	Privilege     string `json:"privilege"`
}

func (c *APIStateClient) readGrants(ctx context.Context, state *declarative.DesiredState) error {
	pages, err := c.fetchAllPages(ctx, "/grants")
	if err != nil {
		return err
	}
	if len(pages) == 0 {
		return nil
	}

	var items []apiGrant
	if err := mergePages(pages, &items); err != nil {
		return err
	}

	unresolved := make([]string, 0)

	for _, g := range items {
		principalName := c.reverseLookupPrincipalName(g.PrincipalID, g.PrincipalType)
		if principalName == "" {
			resolvedName, lookupErr := c.lookupMemberNameByID(ctx, g.PrincipalID, g.PrincipalType)
			if lookupErr != nil {
				unresolved = append(unresolved, fmt.Sprintf("principal_id=%s principal_type=%s (lookup failed)", g.PrincipalID, g.PrincipalType))
				continue
			}
			principalName = resolvedName
		}

		securablePath := c.reverseLookupSecurablePath(g.SecurableType, g.SecurableID)
		if securablePath == "" {
			unresolved = append(unresolved, fmt.Sprintf("securable_type=%s securable_id=%s", g.SecurableType, g.SecurableID))
			continue
		}

		state.Grants = append(state.Grants, declarative.GrantSpec{
			Principal:     principalName,
			PrincipalType: g.PrincipalType,
			SecurableType: g.SecurableType,
			Securable:     securablePath,
			Privilege:     g.Privilege,
		})
		if g.ID != "" && c.index != nil {
			c.index.grantIDByIdentity[grantIdentity(declarative.GrantSpec{
				Principal:     principalName,
				PrincipalType: g.PrincipalType,
				SecurableType: g.SecurableType,
				Securable:     securablePath,
				Privilege:     g.Privilege,
			})] = g.ID
		}
	}

	_ = unresolved

	return nil
}

type apiAPIKey struct {
	ID          string  `json:"id"`
	Name        string  `json:"name"`
	Principal   string  `json:"principal"`
	PrincipalID string  `json:"principal_id"`
	KeyPrefix   string  `json:"key_prefix"`
	ExpiresAt   *string `json:"expires_at"`
}

func (c *APIStateClient) readAPIKeys(ctx context.Context, state *declarative.DesiredState) error {
	pages, err := c.fetchAllPages(ctx, "/api-keys")
	if err != nil {
		return err
	}
	if len(pages) == 0 {
		return nil
	}

	var items []apiAPIKey
	if err := mergePages(pages, &items); err != nil {
		return err
	}

	for _, k := range items {
		principal := k.Principal
		if principal == "" && k.PrincipalID != "" {
			principal = c.reverseLookupPrincipalName(k.PrincipalID, "user")
		}
		if principal == "" {
			principal = k.PrincipalID
		}

		state.APIKeys = append(state.APIKeys, declarative.APIKeySpec{
			Name:      k.Name,
			Principal: principal,
			ExpiresAt: k.ExpiresAt,
		})
	}
	return nil
}

// --- Catalog resources ---

type apiCatalog struct {
	ID            string `json:"id"`
	Name          string `json:"name"`
	MetastoreType string `json:"metastore_type"`
	DSN           string `json:"dsn"`
	DataPath      string `json:"data_path"`
	IsDefault     bool   `json:"is_default"`
	Comment       string `json:"comment"`
}

func (c *APIStateClient) readCatalogs(ctx context.Context, state *declarative.DesiredState) error {
	pages, err := c.fetchAllPages(ctx, "/catalogs")
	if err != nil {
		return err
	}

	var items []apiCatalog
	if err := mergePages(pages, &items); err != nil {
		return fmt.Errorf("parse catalogs: %w", err)
	}

	for _, cat := range items {
		state.Catalogs = append(state.Catalogs, declarative.CatalogResource{
			CatalogName: cat.Name,
			Spec: declarative.CatalogSpec{
				MetastoreType: cat.MetastoreType,
				DSN:           cat.DSN,
				DataPath:      cat.DataPath,
				IsDefault:     cat.IsDefault,
				Comment:       cat.Comment,
			},
		})

		if cat.ID != "" && c.index != nil {
			c.index.catalogIDByName[cat.Name] = cat.ID
		}

		// Fetch schemas for this catalog.
		if err := c.readSchemas(ctx, cat.Name, state); err != nil {
			return fmt.Errorf("catalog %q schemas: %w", cat.Name, err)
		}
	}
	return nil
}

type apiSchema struct {
	ID           string            `json:"id"`
	Name         string            `json:"name"`
	Comment      string            `json:"comment"`
	Owner        string            `json:"owner"`
	LocationName string            `json:"location_name"`
	Properties   map[string]string `json:"properties"`
}

func (c *APIStateClient) readSchemas(ctx context.Context, catalogName string, state *declarative.DesiredState) error {
	pages, err := c.fetchAllPages(ctx, "/catalogs/"+catalogName+"/schemas")
	if err != nil {
		return err
	}
	if len(pages) == 0 {
		return nil
	}

	var items []apiSchema
	if err := mergePages(pages, &items); err != nil {
		return err
	}

	for _, s := range items {
		state.Schemas = append(state.Schemas, declarative.SchemaResource{
			CatalogName: catalogName,
			SchemaName:  s.Name,
			Spec: declarative.SchemaSpec{
				Comment:      s.Comment,
				Owner:        s.Owner,
				LocationName: s.LocationName,
				Properties:   s.Properties,
			},
		})
		if s.ID != "" && c.index != nil {
			c.index.schemaIDByPath[catalogName+"."+s.Name] = s.ID
		}

		// Fetch tables, views, volumes for this schema.
		if err := c.readTables(ctx, catalogName, s.Name, state); err != nil {
			return fmt.Errorf("schema %s.%s tables: %w", catalogName, s.Name, err)
		}
		if err := c.readViews(ctx, catalogName, s.Name, state); err != nil {
			return fmt.Errorf("schema %s.%s views: %w", catalogName, s.Name, err)
		}
		if err := c.readVolumes(ctx, catalogName, s.Name, state); err != nil {
			return fmt.Errorf("schema %s.%s volumes: %w", catalogName, s.Name, err)
		}
	}
	return nil
}

type apiTable struct {
	ID           string            `json:"id"`
	Name         string            `json:"name"`
	TableType    string            `json:"table_type"`
	Comment      string            `json:"comment"`
	Owner        string            `json:"owner"`
	Columns      []apiColumn       `json:"columns"`
	Properties   map[string]string `json:"properties"`
	SourcePath   string            `json:"source_path"`
	FileFormat   string            `json:"file_format"`
	LocationName string            `json:"location_name"`
}

type apiColumn struct {
	Name    string `json:"name"`
	Type    string `json:"type"`
	Comment string `json:"comment"`
}

func (c *APIStateClient) readTables(ctx context.Context, catalogName, schemaName string, state *declarative.DesiredState) error {
	path := "/catalogs/" + catalogName + "/schemas/" + schemaName + "/tables"
	pages, err := c.fetchAllPages(ctx, path)
	if err != nil {
		return err
	}
	if len(pages) == 0 {
		return nil
	}

	var items []apiTable
	if err := mergePages(pages, &items); err != nil {
		return err
	}

	for _, t := range items {
		var cols []declarative.ColumnDef
		for _, col := range t.Columns {
			cols = append(cols, declarative.ColumnDef{
				Name:    col.Name,
				Type:    col.Type,
				Comment: col.Comment,
			})
		}

		state.Tables = append(state.Tables, declarative.TableResource{
			CatalogName: catalogName,
			SchemaName:  schemaName,
			TableName:   t.Name,
			Spec: declarative.TableSpec{
				TableType:    t.TableType,
				Comment:      t.Comment,
				Owner:        t.Owner,
				Columns:      cols,
				Properties:   t.Properties,
				SourcePath:   t.SourcePath,
				FileFormat:   t.FileFormat,
				LocationName: t.LocationName,
			},
		})
		if t.ID != "" && c.index != nil {
			c.index.tableIDByPath[catalogName+"."+schemaName+"."+t.Name] = t.ID
		}
	}
	return nil
}

type apiView struct {
	Name           string            `json:"name"`
	ViewDefinition string            `json:"view_definition"`
	Comment        string            `json:"comment"`
	Owner          string            `json:"owner"`
	Properties     map[string]string `json:"properties"`
}

func (c *APIStateClient) readViews(ctx context.Context, catalogName, schemaName string, state *declarative.DesiredState) error {
	path := "/catalogs/" + catalogName + "/schemas/" + schemaName + "/views"
	pages, err := c.fetchAllPages(ctx, path)
	if err != nil {
		return err
	}
	if len(pages) == 0 {
		return nil
	}

	var items []apiView
	if err := mergePages(pages, &items); err != nil {
		return err
	}

	for _, v := range items {
		state.Views = append(state.Views, declarative.ViewResource{
			CatalogName: catalogName,
			SchemaName:  schemaName,
			ViewName:    v.Name,
			Spec: declarative.ViewSpec{
				ViewDefinition: v.ViewDefinition,
				Comment:        v.Comment,
				Owner:          v.Owner,
				Properties:     v.Properties,
			},
		})
	}
	return nil
}

type apiVolume struct {
	ID              string `json:"id"`
	Name            string `json:"name"`
	VolumeType      string `json:"volume_type"`
	StorageLocation string `json:"storage_location"`
	Comment         string `json:"comment"`
	Owner           string `json:"owner"`
}

func (c *APIStateClient) readVolumes(ctx context.Context, catalogName, schemaName string, state *declarative.DesiredState) error {
	path := "/catalogs/" + catalogName + "/schemas/" + schemaName + "/volumes"
	pages, err := c.fetchAllPages(ctx, path)
	if err != nil {
		return err
	}
	if len(pages) == 0 {
		return nil
	}

	var items []apiVolume
	if err := mergePages(pages, &items); err != nil {
		return err
	}

	for _, v := range items {
		state.Volumes = append(state.Volumes, declarative.VolumeResource{
			CatalogName: catalogName,
			SchemaName:  schemaName,
			VolumeName:  v.Name,
			Spec: declarative.VolumeSpec{
				VolumeType:      v.VolumeType,
				StorageLocation: v.StorageLocation,
				Comment:         v.Comment,
				Owner:           v.Owner,
			},
		})
		if v.ID != "" && c.index != nil {
			c.index.volumeIDByPath[catalogName+"."+schemaName+"."+v.Name] = v.ID
		}
	}
	return nil
}

// --- Storage resources ---

type apiStorageCredential struct {
	ID             string `json:"id"`
	Name           string `json:"name"`
	CredentialType string `json:"credential_type"`
	Comment        string `json:"comment"`
	Endpoint       string `json:"endpoint"`
	Region         string `json:"region"`
	URLStyle       string `json:"url_style"`
}

func (c *APIStateClient) readStorageCredentials(ctx context.Context, state *declarative.DesiredState) error {
	pages, err := c.fetchAllPages(ctx, "/storage-credentials")
	if err != nil {
		return err
	}
	if len(pages) == 0 {
		return nil
	}

	var items []apiStorageCredential
	if err := mergePages(pages, &items); err != nil {
		return err
	}

	for _, sc := range items {
		spec := declarative.StorageCredentialSpec{
			Name:           sc.Name,
			CredentialType: sc.CredentialType,
			Comment:        sc.Comment,
		}
		switch sc.CredentialType {
		case "S3":
			spec.S3 = &declarative.S3CredentialSpec{
				KeyIDFromEnv:  "REPLACE_ME",
				SecretFromEnv: "REPLACE_ME",
				Endpoint:      sc.Endpoint,
				Region:        sc.Region,
				URLStyle:      sc.URLStyle,
			}
		case "AZURE":
			spec.Azure = &declarative.AzureCredentialSpec{
				AccountNameFromEnv: "REPLACE_ME",
			}
		case "GCS":
			spec.GCS = &declarative.GCSCredentialSpec{
				KeyFilePath: "REPLACE_ME",
			}
		}
		state.StorageCredentials = append(state.StorageCredentials, spec)
		if sc.ID != "" && c.index != nil {
			c.index.credentialIDByName[sc.Name] = sc.ID
		}
	}
	return nil
}

type apiExternalLocation struct {
	ID             string `json:"id"`
	Name           string `json:"name"`
	URL            string `json:"url"`
	CredentialName string `json:"credential_name"`
	StorageType    string `json:"storage_type"`
	Comment        string `json:"comment"`
	ReadOnly       bool   `json:"read_only"`
}

func (c *APIStateClient) readExternalLocations(ctx context.Context, state *declarative.DesiredState) error {
	pages, err := c.fetchAllPages(ctx, "/external-locations")
	if err != nil {
		return err
	}
	if len(pages) == 0 {
		return nil
	}

	var items []apiExternalLocation
	if err := mergePages(pages, &items); err != nil {
		return err
	}

	for _, el := range items {
		state.ExternalLocations = append(state.ExternalLocations, declarative.ExternalLocationSpec{
			Name:           el.Name,
			URL:            el.URL,
			CredentialName: el.CredentialName,
			StorageType:    el.StorageType,
			Comment:        el.Comment,
			ReadOnly:       el.ReadOnly,
		})
		if el.ID != "" && c.index != nil {
			c.index.locationIDByName[el.Name] = el.ID
		}
	}
	return nil
}

// --- Compute resources ---

type apiComputeEndpoint struct {
	ID                         string `json:"id"`
	Name                       string `json:"name"`
	URL                        string `json:"url"`
	Type                       string `json:"type"`
	SelectionPolicy            string `json:"selection_policy"`
	WorkloadClass              string `json:"workload_class"`
	ReadinessStatus            string `json:"readiness_status"`
	Size                       string `json:"size"`
	MaxMemoryGB                *int   `json:"max_memory_gb"`
	MaxConcurrency             *int   `json:"max_concurrency"`
	MaxResultSizeMB            *int   `json:"max_result_size_mb"`
	RecommendedForLargeQueries bool   `json:"recommended_for_large_queries"`
	IsDraining                 bool   `json:"is_draining"`
}

type apiComputeAssignment struct {
	ID            string `json:"id"`
	Endpoint      string `json:"endpoint"`
	Principal     string `json:"principal"`
	PrincipalType string `json:"principal_type"`
	IsDefault     bool   `json:"is_default"`
	FallbackLocal bool   `json:"fallback_local"`
}

func computeAssignmentKey(endpoint, principalType, principal string) string {
	return endpoint + "|" + principalType + "|" + principal
}

func (c *APIStateClient) readComputeEndpoints(ctx context.Context, state *declarative.DesiredState) error {
	pages, err := c.fetchAllPages(ctx, "/compute-endpoints")
	if err != nil {
		return err
	}
	if len(pages) == 0 {
		return nil
	}

	var items []apiComputeEndpoint
	if err := mergePages(pages, &items); err != nil {
		return err
	}

	for _, ep := range items {
		state.ComputeEndpoints = append(state.ComputeEndpoints, declarative.ComputeEndpointSpec{
			Name:                       ep.Name,
			URL:                        ep.URL,
			Type:                       ep.Type,
			SelectionPolicy:            ep.SelectionPolicy,
			WorkloadClass:              ep.WorkloadClass,
			ReadinessStatus:            ep.ReadinessStatus,
			Size:                       ep.Size,
			MaxMemoryGB:                ep.MaxMemoryGB,
			MaxConcurrency:             ep.MaxConcurrency,
			MaxResultSizeMB:            ep.MaxResultSizeMB,
			RecommendedForLargeQueries: ep.RecommendedForLargeQueries,
			IsDraining:                 ep.IsDraining,
		})
		if ep.ID != "" && c.index != nil {
			c.index.computeIDByName[ep.Name] = ep.ID
		}

		// Fetch assignments for this endpoint.
		assignPages, err := c.fetchAllPages(ctx, "/compute-endpoints/"+ep.Name+"/assignments")
		if err != nil {
			return fmt.Errorf("endpoint %q assignments: %w", ep.Name, err)
		}
		if len(assignPages) > 0 {
			var assignments []apiComputeAssignment
			if err := mergePages(assignPages, &assignments); err != nil {
				return fmt.Errorf("endpoint %q assignments parse: %w", ep.Name, err)
			}
			for _, a := range assignments {
				state.ComputeAssignments = append(state.ComputeAssignments, declarative.ComputeAssignmentSpec{
					Endpoint:      ep.Name,
					Principal:     a.Principal,
					PrincipalType: a.PrincipalType,
					IsDefault:     a.IsDefault,
					FallbackLocal: a.FallbackLocal,
				})
				if a.ID != "" && c.index != nil {
					c.index.computeAssignIDByKey[computeAssignmentKey(ep.Name, a.PrincipalType, a.Principal)] = a.ID
				}
			}
		}
	}

	defaultsPage, err := c.client.Do(http.MethodGet, "/compute-defaults", nil, nil)
	if err == nil && defaultsPage.StatusCode >= 200 && defaultsPage.StatusCode < 300 {
		var defaults struct {
			InteractiveMode string `json:"interactive_mode"`
			ScheduledMode   string `json:"scheduled_mode"`
			NotebookMode    string `json:"notebook_mode"`
		}
		body, readErr := apiruntime.ReadBody(defaultsPage)
		if readErr == nil && json.Unmarshal(body, &defaults) == nil {
			state.ComputeDefaults = &declarative.ComputeRoutingDefaultsSpec{
				InteractiveMode: defaults.InteractiveMode,
				ScheduledMode:   defaults.ScheduledMode,
				NotebookMode:    defaults.NotebookMode,
			}
		}
	}
	return nil
}

// --- Governance resources ---

type apiTag struct {
	ID    string  `json:"id"`
	Key   string  `json:"key"`
	Value *string `json:"value"`
}

func (c *APIStateClient) readTags(ctx context.Context, state *declarative.DesiredState) error {
	pages, err := c.fetchAllPages(ctx, "/tags")
	if err != nil {
		return err
	}
	if len(pages) == 0 {
		return nil
	}

	var items []apiTag
	if err := mergePages(pages, &items); err != nil {
		return err
	}

	for _, t := range items {
		state.Tags = append(state.Tags, declarative.TagSpec{
			Key:   t.Key,
			Value: t.Value,
		})
		if t.ID != "" && c.index != nil {
			c.index.tagIDByKey[tagKey(t.Key, t.Value)] = t.ID
		}
	}
	return nil
}

// tagKey returns the canonical key for a tag: "key" or "key:value".
func tagKey(key string, value *string) string {
	if value != nil {
		return key + ":" + *value
	}
	return key
}

// --- Workflow resources (simplified) ---

type apiNotebook struct {
	ID          string `json:"id"`
	Name        string `json:"name"`
	Description string `json:"description"`
	Owner       string `json:"owner"`
}

type apiNotebookCell struct {
	ID         string               `json:"id"`
	CellType   string               `json:"cell_type"`
	Name       string               `json:"name"`
	Role       string               `json:"role"`
	Disabled   bool                 `json:"disabled"`
	Test       *apiNotebookCellTest `json:"test"`
	VisualSpec *domain.VisualSpec   `json:"visual_spec"`
	Content    string               `json:"content"`
	Position   int                  `json:"position"`
}

type apiNotebookCellTest struct {
	Severity string `json:"severity"`
}

type apiNotebookPublishModel struct {
	ProjectName     string `json:"project_name"`
	Name            string `json:"name"`
	Materialization string `json:"materialization"`
	OutputCellID    string `json:"output_cell_id"`
}

type apiNotebookDetail struct {
	Notebook     apiNotebook              `json:"notebook"`
	Cells        []apiNotebookCell        `json:"cells"`
	PublishModel *apiNotebookPublishModel `json:"publish_model"`
}

type apiAsset struct {
	AssetKey     string   `json:"asset_key"`
	AssetType    string   `json:"asset_type"`
	Owner        string   `json:"owner"`
	Description  string   `json:"description"`
	Tags         []string `json:"tags"`
	IOProfile    string   `json:"io_profile"`
	IsActive     bool     `json:"is_active"`
	CronSchedule string   `json:"cron_schedule"`
}

type apiAssetGraph struct {
	AssetKey          string   `json:"asset_key"`
	UpstreamAssetKeys []string `json:"upstream_asset_keys"`
}

type apiAssetCheck struct {
	Name      string `json:"name"`
	CheckType string `json:"check_type"`
	Severity  string `json:"severity"`
	Enabled   *bool  `json:"enabled"`
}

func (c *APIStateClient) readNotebooks(ctx context.Context, state *declarative.DesiredState) error {
	pages, err := c.fetchAllPages(ctx, "/notebooks")
	if err != nil {
		return err
	}
	if len(pages) == 0 {
		return nil
	}

	var items []apiNotebook
	if err := mergePages(pages, &items); err != nil {
		return err
	}

	for _, nb := range items {
		if nb.ID != "" && c.index != nil {
			c.index.notebookIDByName[nb.Name] = nb.ID
		}

		cells := make([]declarative.CellSpec, 0)
		if nb.ID != "" {
			detail, err := c.readNotebookDetail(ctx, nb.ID)
			if err != nil {
				return fmt.Errorf("notebook %q detail: %w", nb.Name, err)
			}
			sort.Slice(detail.Cells, func(i, j int) bool {
				return detail.Cells[i].Position < detail.Cells[j].Position
			})
			cells = make([]declarative.CellSpec, 0, len(detail.Cells))
			publish := buildNotebookPublishSpec(detail.PublishModel, detail.Cells)
			for _, cell := range detail.Cells {
				name := cell.Name
				if publish != nil && publish.Model != nil && publish.Model.OutputCell == cell.ID && name == "" {
					name = cell.ID
				}
				spec := declarative.CellSpec{
					Type:    cell.CellType,
					Content: cell.Content,
				}
				if name != "" {
					spec.Name = name
				}
				if cell.Role != "" && cell.Role != defaultNotebookCellRole(cell.CellType) {
					spec.Role = cell.Role
				}
				if cell.Disabled {
					spec.Disabled = true
				}
				if cell.Test != nil {
					spec.Test = toDeclarativeNotebookTest(cell.Test)
				}
				if cell.VisualSpec != nil {
					spec.VisualSpec = cell.VisualSpec
				}
				cells = append(cells, spec)
			}
			state.Notebooks = append(state.Notebooks, declarative.NotebookResource{
				Name: nb.Name,
				Spec: declarative.NotebookSpec{
					Description: nb.Description,
					Owner:       nb.Owner,
					Cells:       cells,
					Publish:     publish,
				},
			})
			continue
		}

		state.Notebooks = append(state.Notebooks, declarative.NotebookResource{
			Name: nb.Name,
			Spec: declarative.NotebookSpec{
				Description: nb.Description,
				Owner:       nb.Owner,
				Cells:       cells,
			},
		})
	}
	return nil
}

type apiProductDomain struct {
	ID          string `json:"id"`
	Name        string `json:"name"`
	Description string `json:"description"`
}

type apiProductTeam struct {
	ID             string `json:"id"`
	DomainID       string `json:"domain_id"`
	Name           string `json:"name"`
	ContactChannel string `json:"contact_channel"`
}

type apiProductContract struct {
	DataGrain            string   `json:"data_grain"`
	PrimaryKeys          []string `json:"primary_keys"`
	JoinKeys             []string `json:"join_keys"`
	Dimensions           []string `json:"dimensions"`
	Measures             []string `json:"measures"`
	RetentionWindow      string   `json:"retention_window"`
	UpdateCadence        string   `json:"update_cadence"`
	QualityExpectations  []string `json:"quality_expectations"`
	BreakingChangePolicy string   `json:"breaking_change_policy"`
	SampleQueries        []string `json:"sample_queries"`
}

type apiProductSLO struct {
	FreshnessSLO string `json:"freshness_slo"`
	LatencySLO   string `json:"latency_slo"`
}

type apiDataProduct struct {
	ID                  string             `json:"id"`
	Slug                string             `json:"slug"`
	Name                string             `json:"name"`
	Description         string             `json:"description"`
	StewardPrincipal    string             `json:"steward_principal"`
	ContactChannel      string             `json:"contact_channel"`
	Visibility          string             `json:"visibility"`
	ConsumerAudience    string             `json:"consumer_audience"`
	DocsURL             string             `json:"docs_url"`
	AccessRequestPath   string             `json:"access_request_path"`
	BusinessDefinitions map[string]string  `json:"business_definitions"`
	Contract            apiProductContract `json:"contract"`
	SLO                 apiProductSLO      `json:"slo"`
	PublicationIntent   string             `json:"publication_intent"`
}

type apiDataProductVersion struct {
	Version            int                `json:"version"`
	ReleaseState       string             `json:"release_state"`
	CompatibilityLevel string             `json:"compatibility_level"`
	Contract           apiProductContract `json:"contract"`
	SLO                apiProductSLO      `json:"slo"`
	DocsURL            string             `json:"docs_url"`
	AccessRequestPath  string             `json:"access_request_path"`
}

type apiProductOutput struct {
	AssetKey  string `json:"asset_key"`
	IsPrimary bool   `json:"is_primary"`
}

type apiProductSemanticEntrypoint struct {
	ProjectName string `json:"project_name"`
	ModelName   string `json:"model_name"`
}

type apiDataProductListItem struct {
	Product apiDataProduct `json:"product"`
}

type apiDataProductDetail struct {
	Product             apiDataProduct                 `json:"product"`
	Domain              apiProductDomain               `json:"domain"`
	OwnerTeam           apiProductTeam                 `json:"owner_team"`
	Versions            []apiDataProductVersion        `json:"versions"`
	Outputs             []apiProductOutput             `json:"outputs"`
	SemanticEntrypoints []apiProductSemanticEntrypoint `json:"semantic_entrypoints"`
	Dependencies        []apiDataProductListItem       `json:"dependencies"`
}

type apiDataProductVersionDetail struct {
	Version             apiDataProductVersion          `json:"version"`
	Outputs             []apiProductOutput             `json:"outputs"`
	SemanticEntrypoints []apiProductSemanticEntrypoint `json:"semantic_entrypoints"`
}

func (c *APIStateClient) readDomains(ctx context.Context, state *declarative.DesiredState) error {
	pages, err := c.fetchAllPages(ctx, "/domains")
	if err != nil {
		return err
	}
	if len(pages) == 0 {
		return nil
	}

	var items []apiProductDomain
	if err := mergePages(pages, &items); err != nil {
		return err
	}

	for _, item := range items {
		state.Domains = append(state.Domains, declarative.DomainResource{
			Name: item.Name,
			Spec: declarative.DomainSpec{Description: item.Description},
		})
		if c.index != nil && item.ID != "" {
			c.index.productDomainNameByID[item.ID] = item.Name
		}
	}
	return nil
}

func (c *APIStateClient) readTeams(ctx context.Context, state *declarative.DesiredState) error {
	pages, err := c.fetchAllPages(ctx, "/teams")
	if err != nil {
		return err
	}
	if len(pages) == 0 {
		return nil
	}

	var items []apiProductTeam
	if err := mergePages(pages, &items); err != nil {
		return err
	}

	for _, item := range items {
		domainRef := c.lookupDomainNameFromState(state, item.DomainID)
		if domainRef == "" {
			domainRef = item.DomainID
		}
		state.Teams = append(state.Teams, declarative.TeamResource{
			Name: item.Name,
			Spec: declarative.TeamSpec{
				DomainRef:      domainRef,
				ContactChannel: item.ContactChannel,
			},
		})
	}
	return nil
}

func (c *APIStateClient) readDataProducts(ctx context.Context, state *declarative.DesiredState) error {
	pages, err := c.fetchAllPages(ctx, "/data-products")
	if err != nil {
		return err
	}
	if len(pages) == 0 {
		return nil
	}

	var items []apiDataProductListItem
	if err := mergePages(pages, &items); err != nil {
		return err
	}

	for _, item := range items {
		if item.Product.Slug == "" {
			continue
		}
		detail, err := c.readDataProductDetail(ctx, item.Product.Slug)
		if err != nil {
			return fmt.Errorf("read data product %q: %w", item.Product.Slug, err)
		}
		spec := declarative.DataProductSpec{
			Name:                detail.Product.Name,
			Description:         detail.Product.Description,
			DomainRef:           detail.Domain.Name,
			OwnerTeamRef:        detail.OwnerTeam.Name,
			StewardPrincipal:    detail.Product.StewardPrincipal,
			ContactChannel:      detail.Product.ContactChannel,
			Visibility:          detail.Product.Visibility,
			ConsumerAudience:    detail.Product.ConsumerAudience,
			DocsURL:             detail.Product.DocsURL,
			AccessRequestPath:   detail.Product.AccessRequestPath,
			BusinessDefinitions: cloneStringMap(detail.Product.BusinessDefinitions),
			Contract:            declarativeProductContract(detail.Product.Contract),
			SLO:                 declarativeProductSLO(detail.Product.SLO),
			Outputs:             productOutputKeys(detail.Outputs),
			SemanticEntrypoints: semanticEntrypointRefs(detail.SemanticEntrypoints),
			Dependencies:        productDependencySlugs(detail.Dependencies),
			PublicationIntent:   detail.Product.PublicationIntent,
		}

		for _, version := range detail.Versions {
			versionDetail, err := c.readDataProductVersionDetail(ctx, item.Product.Slug, version.Version)
			if err != nil {
				return fmt.Errorf("read data product %q version %d: %w", item.Product.Slug, version.Version, err)
			}
			spec.Versions = append(spec.Versions, declarative.DataProductVersionSpec{
				Version:             version.Version,
				ReleaseState:        version.ReleaseState,
				CompatibilityLevel:  version.CompatibilityLevel,
				Contract:            declarativeProductContract(version.Contract),
				SLO:                 declarativeProductSLO(version.SLO),
				DocsURL:             version.DocsURL,
				AccessRequestPath:   version.AccessRequestPath,
				Outputs:             productOutputKeys(versionDetail.Outputs),
				SemanticEntrypoints: semanticEntrypointRefs(versionDetail.SemanticEntrypoints),
			})
		}
		sort.Slice(spec.Versions, func(i, j int) bool {
			return spec.Versions[i].Version < spec.Versions[j].Version
		})
		if len(spec.Versions) == 1 && isImplicitProductVersion(spec, spec.Versions[0]) {
			spec.Versions = nil
		}

		state.DataProducts = append(state.DataProducts, declarative.DataProductResource{
			Slug: item.Product.Slug,
			Spec: spec,
		})
	}
	return nil
}

func (c *APIStateClient) readDataProductDetail(_ context.Context, slug string) (*apiDataProductDetail, error) {
	resp, err := c.client.Do(http.MethodGet, "/data-products/"+slug, nil, nil)
	if err != nil {
		return nil, err
	}
	body, err := apiruntime.ReadBody(resp)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("GET /data-products/%s: HTTP %d: %s", slug, resp.StatusCode, string(body))
	}
	var detail apiDataProductDetail
	if err := json.Unmarshal(body, &detail); err != nil {
		return nil, err
	}
	return &detail, nil
}

func (c *APIStateClient) readDataProductVersionDetail(_ context.Context, slug string, version int) (*apiDataProductVersionDetail, error) {
	resp, err := c.client.Do(http.MethodGet, fmt.Sprintf("/data-products/%s/versions/%d", slug, version), nil, nil)
	if err != nil {
		return nil, err
	}
	body, err := apiruntime.ReadBody(resp)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("GET /data-products/%s/versions/%d: HTTP %d: %s", slug, version, resp.StatusCode, string(body))
	}
	var detail apiDataProductVersionDetail
	if err := json.Unmarshal(body, &detail); err != nil {
		return nil, err
	}
	return &detail, nil
}

func declarativeProductContract(item apiProductContract) declarative.ProductContractSpec {
	return declarative.ProductContractSpec{
		DataGrain:            item.DataGrain,
		PrimaryKeys:          append([]string(nil), item.PrimaryKeys...),
		JoinKeys:             append([]string(nil), item.JoinKeys...),
		Dimensions:           append([]string(nil), item.Dimensions...),
		Measures:             append([]string(nil), item.Measures...),
		RetentionWindow:      item.RetentionWindow,
		UpdateCadence:        item.UpdateCadence,
		QualityExpectations:  append([]string(nil), item.QualityExpectations...),
		BreakingChangePolicy: item.BreakingChangePolicy,
		SampleQueries:        append([]string(nil), item.SampleQueries...),
	}
}

func declarativeProductSLO(item apiProductSLO) declarative.ProductSLOSpec {
	return declarative.ProductSLOSpec{
		FreshnessSLO: item.FreshnessSLO,
		LatencySLO:   item.LatencySLO,
	}
}

func productOutputKeys(outputs []apiProductOutput) []string {
	keys := make([]string, 0, len(outputs))
	for _, output := range outputs {
		if output.AssetKey != "" {
			keys = append(keys, output.AssetKey)
		}
	}
	sort.Strings(keys)
	return keys
}

func semanticEntrypointRefs(entrypoints []apiProductSemanticEntrypoint) []string {
	refs := make([]string, 0, len(entrypoints))
	for _, entrypoint := range entrypoints {
		if entrypoint.ProjectName == "" || entrypoint.ModelName == "" {
			continue
		}
		refs = append(refs, semanticModelPath(entrypoint.ProjectName, entrypoint.ModelName))
	}
	sort.Strings(refs)
	return refs
}

func productDependencySlugs(items []apiDataProductListItem) []string {
	slugs := make([]string, 0, len(items))
	for _, item := range items {
		if item.Product.Slug != "" {
			slugs = append(slugs, item.Product.Slug)
		}
	}
	sort.Strings(slugs)
	return slugs
}

func cloneStringMap(values map[string]string) map[string]string {
	if len(values) == 0 {
		return nil
	}
	cloned := make(map[string]string, len(values))
	for key, value := range values {
		cloned[key] = value
	}
	return cloned
}

func isImplicitProductVersion(spec declarative.DataProductSpec, version declarative.DataProductVersionSpec) bool {
	return version.Version == 1 &&
		version.ReleaseState == domain.ProductReleaseStateDraft &&
		(version.CompatibilityLevel == "" || version.CompatibilityLevel == domain.ProductCompatibilityBackwardCompatible) &&
		reflect.DeepEqual(spec.Contract, version.Contract) &&
		reflect.DeepEqual(spec.SLO, version.SLO) &&
		spec.DocsURL == version.DocsURL &&
		spec.AccessRequestPath == version.AccessRequestPath &&
		reflect.DeepEqual(spec.Outputs, version.Outputs) &&
		reflect.DeepEqual(spec.SemanticEntrypoints, version.SemanticEntrypoints)
}

func (c *APIStateClient) lookupDomainNameFromState(state *declarative.DesiredState, domainID string) string {
	if domainID == "" {
		return ""
	}
	if c.index != nil {
		if name := c.index.productDomainNameByID[domainID]; name != "" {
			return name
		}
	}
	for _, item := range state.Domains {
		if item.Name == domainID {
			return item.Name
		}
	}
	return ""
}

func (c *APIStateClient) readAssets(ctx context.Context, state *declarative.DesiredState) error {
	pages, err := c.fetchAllPages(ctx, "/assets")
	if err != nil {
		return err
	}
	if len(pages) == 0 {
		return nil
	}

	var items []apiAsset
	if err := mergePages(pages, &items); err != nil {
		return err
	}

	productRefByAssetKey := map[string]string{}
	for _, product := range state.DataProducts {
		for _, assetKey := range product.Spec.Outputs {
			productRefByAssetKey[assetKey] = product.Slug
		}
		for _, version := range product.Spec.Versions {
			for _, assetKey := range version.Outputs {
				productRefByAssetKey[assetKey] = product.Slug
			}
		}
	}

	for _, asset := range items {
		spec := declarative.AssetSpec{
			AssetType:    asset.AssetType,
			ProductRef:   productRefByAssetKey[asset.AssetKey],
			Owner:        asset.Owner,
			Description:  asset.Description,
			Tags:         append([]string(nil), asset.Tags...),
			IOProfile:    asset.IOProfile,
			CronSchedule: asset.CronSchedule,
		}

		if graph, graphErr := c.readAssetGraph(ctx, asset.AssetKey); graphErr == nil {
			spec.DependsOn = append([]string(nil), graph.UpstreamAssetKeys...)
		}

		checks, checksErr := c.readAssetChecks(ctx, asset.AssetKey)
		if checksErr == nil {
			spec.CheckDefinitions = checks
		}

		state.Assets = append(state.Assets, declarative.AssetResource{
			Name: asset.AssetKey,
			Spec: spec,
		})
	}

	return nil
}

func (c *APIStateClient) readAssetGraph(_ context.Context, assetKey string) (*apiAssetGraph, error) {
	resp, err := c.client.Do(http.MethodGet, "/assets/"+assetKey+"/graph", nil, nil)
	if err != nil {
		return nil, err
	}
	body, err := apiruntime.ReadBody(resp)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("GET /assets/%s/graph: HTTP %d: %s", assetKey, resp.StatusCode, string(body))
	}
	var graph apiAssetGraph
	if err := json.Unmarshal(body, &graph); err != nil {
		return nil, err
	}
	return &graph, nil
}

func (c *APIStateClient) readAssetChecks(ctx context.Context, assetKey string) ([]declarative.AssetCheckSpec, error) {
	pages, err := c.fetchAllPages(ctx, "/assets/"+assetKey+"/checks")
	if err != nil {
		return nil, err
	}
	if len(pages) == 0 {
		return nil, nil
	}

	var checks []apiAssetCheck
	if err := mergePages(pages, &checks); err != nil {
		return nil, err
	}

	out := make([]declarative.AssetCheckSpec, 0, len(checks))
	for _, check := range checks {
		out = append(out, declarative.AssetCheckSpec{
			Name:      check.Name,
			CheckType: check.CheckType,
			Severity:  check.Severity,
			Enabled:   check.Enabled,
		})
	}
	return out, nil
}

func (c *APIStateClient) readNotebookDetail(_ context.Context, notebookID string) (*apiNotebookDetail, error) {
	resp, err := c.client.Do(http.MethodGet, "/notebooks/"+notebookID, nil, nil)
	if err != nil {
		return nil, err
	}
	body, err := apiruntime.ReadBody(resp)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("GET /notebooks/%s: HTTP %d: %s", notebookID, resp.StatusCode, string(body))
	}
	var detail apiNotebookDetail
	if err := json.Unmarshal(body, &detail); err != nil {
		return nil, err
	}
	return &detail, nil
}

type apiMacro struct {
	Name        string            `json:"name"`
	MacroType   string            `json:"macro_type"`
	Parameters  []string          `json:"parameters"`
	Body        string            `json:"body"`
	Description string            `json:"description"`
	CatalogName string            `json:"catalog_name"`
	ProjectName string            `json:"project_name"`
	Visibility  string            `json:"visibility"`
	Owner       string            `json:"owner"`
	Properties  map[string]string `json:"properties"`
	Tags        []string          `json:"tags"`
	Status      string            `json:"status"`
}

func (c *APIStateClient) readMacros(ctx context.Context, state *declarative.DesiredState) error {
	pages, err := c.fetchAllPages(ctx, "/macros")
	if err != nil {
		return err
	}
	if len(pages) == 0 {
		return nil
	}

	var items []apiMacro
	if err := mergePages(pages, &items); err != nil {
		return err
	}

	for _, m := range items {
		state.Macros = append(state.Macros, declarative.MacroResource{
			Name: m.Name,
			Spec: declarative.MacroSpec{
				MacroType:   m.MacroType,
				Parameters:  m.Parameters,
				Body:        m.Body,
				Description: m.Description,
				CatalogName: m.CatalogName,
				ProjectName: m.ProjectName,
				Visibility:  m.Visibility,
				Owner:       m.Owner,
				Properties:  m.Properties,
				Tags:        m.Tags,
				Status:      m.Status,
			},
		})
	}

	return nil
}

type apiModelConfig struct {
	UniqueKey           []string `json:"unique_key"`
	IncrementalStrategy string   `json:"incremental_strategy"`
	OnSchemaChange      string   `json:"on_schema_change"`
}

type apiModelContractColumn struct {
	Name     string `json:"name"`
	Type     string `json:"type"`
	Nullable bool   `json:"nullable"`
}

type apiModelContract struct {
	Enforce bool                     `json:"enforce"`
	Columns []apiModelContractColumn `json:"columns"`
}

type apiModelFreshness struct {
	MaxLagSeconds int64  `json:"max_lag_seconds"`
	CronSchedule  string `json:"cron_schedule"`
}

type apiModel struct {
	ProjectName     string             `json:"project_name"`
	Name            string             `json:"name"`
	SQL             string             `json:"sql"`
	Materialization string             `json:"materialization"`
	Description     string             `json:"description"`
	Tags            []string           `json:"tags"`
	Config          *apiModelConfig    `json:"config"`
	Contract        *apiModelContract  `json:"contract"`
	FreshnessPolicy *apiModelFreshness `json:"freshness_policy"`
}

type apiSemanticModel struct {
	ID                   string   `json:"id"`
	ProjectName          string   `json:"project_name"`
	Name                 string   `json:"name"`
	Description          string   `json:"description"`
	BaseModelRef         string   `json:"base_model_ref"`
	DefaultTimeDimension string   `json:"default_time_dimension"`
	Tags                 []string `json:"tags"`
}

type apiSemanticMetric struct {
	Name               string `json:"name"`
	Description        string `json:"description"`
	MetricType         string `json:"metric_type"`
	ExpressionMode     string `json:"expression_mode"`
	Expression         string `json:"expression"`
	DefaultTimeGrain   string `json:"default_time_grain"`
	Format             string `json:"format"`
	CertificationState string `json:"certification_state"`
}

type apiSemanticRelationship struct {
	Name             string `json:"name"`
	FromSemanticID   string `json:"from_semantic_id"`
	ToSemanticID     string `json:"to_semantic_id"`
	RelationshipType string `json:"relationship_type"`
	JoinSQL          string `json:"join_sql"`
	IsDefault        bool   `json:"is_default"`
	Cost             int    `json:"cost"`
	MaxHops          int    `json:"max_hops"`
}

type apiSemanticPreAggregation struct {
	Name           string   `json:"name"`
	MetricSet      []string `json:"metric_set"`
	DimensionSet   []string `json:"dimension_set"`
	Grain          string   `json:"grain"`
	TargetRelation string   `json:"target_relation"`
	RefreshPolicy  string   `json:"refresh_policy"`
}

type apiModelTestConfig struct {
	Values    []string `json:"values"`
	ToModel   string   `json:"to_model"`
	ToColumn  string   `json:"to_column"`
	CustomSQL string   `json:"custom_sql"`
}

type apiModelTest struct {
	ID       string              `json:"id"`
	Name     string              `json:"name"`
	TestType string              `json:"test_type"`
	Column   string              `json:"column"`
	Config   *apiModelTestConfig `json:"config"`
}

func (c *APIStateClient) listModelTests(ctx context.Context, projectName, modelName string) ([]apiModelTest, bool, error) {
	pages, err := c.fetchAllPages(ctx, "/models/"+projectName+"/"+modelName+"/tests")
	if err != nil {
		if c.isOptionalReadError(err) {
			return nil, false, nil
		}
		return nil, false, err
	}
	if len(pages) == 0 {
		return nil, true, nil
	}

	var tests []apiModelTest
	if err := mergePages(pages, &tests); err != nil {
		return nil, false, err
	}
	return tests, true, nil
}

func toDeclarativeTestSpec(test apiModelTest) declarative.TestSpec {
	result := declarative.TestSpec{
		Name:   test.Name,
		Type:   test.TestType,
		Column: test.Column,
	}
	if test.Config == nil {
		return result
	}
	result.Values = append([]string(nil), test.Config.Values...)
	result.ToModel = test.Config.ToModel
	result.ToColumn = test.Config.ToColumn
	result.SQL = test.Config.CustomSQL
	return result
}

func toDeclarativeContract(contract *apiModelContract) *declarative.ContractSpec {
	if contract == nil {
		return nil
	}
	columns := make([]declarative.ContractColumnSpec, len(contract.Columns))
	for i, col := range contract.Columns {
		columns[i] = declarative.ContractColumnSpec{
			Name:     col.Name,
			Type:     col.Type,
			Nullable: col.Nullable,
		}
	}
	return &declarative.ContractSpec{Enforce: contract.Enforce, Columns: columns}
}

func toDeclarativeConfig(config *apiModelConfig) *declarative.ModelConfigSpec {
	if config == nil {
		return nil
	}
	return &declarative.ModelConfigSpec{
		UniqueKey:           append([]string(nil), config.UniqueKey...),
		IncrementalStrategy: config.IncrementalStrategy,
		OnSchemaChange:      config.OnSchemaChange,
	}
}

func toDeclarativeFreshness(freshness *apiModelFreshness) *declarative.FreshnessSpecYAML {
	if freshness == nil {
		return nil
	}
	return &declarative.FreshnessSpecYAML{
		MaxLagSeconds: freshness.MaxLagSeconds,
		CronSchedule:  freshness.CronSchedule,
	}
}

func toDeclarativeNotebookTest(test *apiNotebookCellTest) *declarative.NotebookTestSpec {
	if test == nil {
		return nil
	}
	return &declarative.NotebookTestSpec{Severity: test.Severity}
}

func buildNotebookPublishSpec(model *apiNotebookPublishModel, cells []apiNotebookCell) *declarative.NotebookPublishSpec {
	if model == nil {
		return nil
	}
	outputCellRef := model.OutputCellID
	for _, cell := range cells {
		if cell.ID != model.OutputCellID {
			continue
		}
		if cell.Name != "" {
			outputCellRef = cell.Name
		}
		break
	}
	return &declarative.NotebookPublishSpec{
		Model: &declarative.NotebookPublishModelSpec{
			Project:         model.ProjectName,
			Name:            model.Name,
			Materialization: model.Materialization,
			OutputCell:      outputCellRef,
		},
	}
}

func defaultNotebookCellRole(cellType string) string {
	if cellType == "markdown" {
		return "markdown"
	}
	return "transform"
}

func (c *APIStateClient) readModels(ctx context.Context, state *declarative.DesiredState) error {
	pages, err := c.fetchAllPages(ctx, "/models")
	if err != nil {
		return err
	}
	if len(pages) == 0 {
		return nil
	}

	var items []apiModel
	if err := mergePages(pages, &items); err != nil {
		return err
	}

	publishedNotebookModels := make(map[string]struct{}, len(state.Notebooks))
	for _, notebook := range state.Notebooks {
		if notebook.Spec.Publish == nil || notebook.Spec.Publish.Model == nil {
			continue
		}
		publishedNotebookModels[modelPath(notebook.Spec.Publish.Model.Project, notebook.Spec.Publish.Model.Name)] = struct{}{}
	}

	for _, m := range items {
		if _, published := publishedNotebookModels[modelPath(m.ProjectName, m.Name)]; published {
			continue
		}
		tests, _, err := c.listModelTests(ctx, m.ProjectName, m.Name)
		if err != nil {
			return fmt.Errorf("list tests for model %s.%s: %w", m.ProjectName, m.Name, err)
		}

		testSpecs := make([]declarative.TestSpec, len(tests))
		for i, test := range tests {
			testSpecs[i] = toDeclarativeTestSpec(test)
		}

		state.Models = append(state.Models, declarative.ModelResource{
			ProjectName: m.ProjectName,
			ModelName:   m.Name,
			Spec: declarative.ModelSpec{
				Materialization: m.Materialization,
				Description:     m.Description,
				Tags:            m.Tags,
				SQL:             m.SQL,
				Config:          toDeclarativeConfig(m.Config),
				Contract:        toDeclarativeContract(m.Contract),
				Tests:           testSpecs,
				Freshness:       toDeclarativeFreshness(m.FreshnessPolicy),
			},
		})
	}

	return nil
}

func (c *APIStateClient) readSemanticModels(ctx context.Context, state *declarative.DesiredState) error {
	pages, err := c.fetchAllPages(ctx, "/semantic-models")
	if err != nil {
		return err
	}
	if len(pages) == 0 {
		return nil
	}

	var items []apiSemanticModel
	if err := mergePages(pages, &items); err != nil {
		return err
	}

	modelIndexByID := make(map[string]int, len(items))
	modelNameByID := make(map[string]string, len(items))
	for _, m := range items {
		state.SemanticModels = append(state.SemanticModels, declarative.SemanticModelResource{
			ProjectName: m.ProjectName,
			ModelName:   m.Name,
			Spec: declarative.SemanticModelSpec{
				Description:          m.Description,
				BaseModelRef:         m.BaseModelRef,
				DefaultTimeDimension: m.DefaultTimeDimension,
				Tags:                 append([]string(nil), m.Tags...),
			},
		})

		idx := len(state.SemanticModels) - 1
		if m.ID != "" {
			modelIndexByID[m.ID] = idx
			modelNameByID[m.ID] = semanticModelPath(m.ProjectName, m.Name)
			if c.index != nil {
				c.index.semanticModelIDByPath[semanticModelPath(m.ProjectName, m.Name)] = m.ID
			}
		}
	}

	for i := range state.SemanticModels {
		model := &state.SemanticModels[i]
		metrics, err := c.listSemanticMetrics(ctx, model.ProjectName, model.ModelName)
		if err != nil {
			return fmt.Errorf("list semantic metrics for %s.%s: %w", model.ProjectName, model.ModelName, err)
		}
		for _, metric := range metrics {
			model.Spec.Metrics = append(model.Spec.Metrics, declarative.SemanticMetricSpec{
				Name:               metric.Name,
				Description:        metric.Description,
				MetricType:         metric.MetricType,
				ExpressionMode:     metric.ExpressionMode,
				Expression:         metric.Expression,
				DefaultTimeGrain:   metric.DefaultTimeGrain,
				Format:             metric.Format,
				CertificationState: metric.CertificationState,
			})
		}

		preAggs, err := c.listSemanticPreAggregations(ctx, model.ProjectName, model.ModelName)
		if err != nil {
			return fmt.Errorf("list semantic pre-aggregations for %s.%s: %w", model.ProjectName, model.ModelName, err)
		}
		for _, preAgg := range preAggs {
			model.Spec.PreAggregations = append(model.Spec.PreAggregations, declarative.SemanticPreAggSpec{
				Name:           preAgg.Name,
				MetricSet:      append([]string(nil), preAgg.MetricSet...),
				DimensionSet:   append([]string(nil), preAgg.DimensionSet...),
				Grain:          preAgg.Grain,
				TargetRelation: preAgg.TargetRelation,
				RefreshPolicy:  preAgg.RefreshPolicy,
			})
		}
	}

	relationships, err := c.listSemanticRelationships(ctx)
	if err != nil {
		return fmt.Errorf("list semantic relationships: %w", err)
	}
	for _, rel := range relationships {
		fromIdx, ok := modelIndexByID[rel.FromSemanticID]
		if !ok {
			return fmt.Errorf("semantic relationship %q references unknown from_semantic_id %q", rel.Name, rel.FromSemanticID)
		}
		toModel := modelNameByID[rel.ToSemanticID]
		if toModel == "" {
			toModel = rel.ToSemanticID
		}

		state.SemanticModels[fromIdx].Spec.Relationships = append(state.SemanticModels[fromIdx].Spec.Relationships, declarative.SemanticRelationshipSpec{
			Name:             rel.Name,
			ToModel:          toModel,
			RelationshipType: rel.RelationshipType,
			JoinSQL:          rel.JoinSQL,
			IsDefault:        rel.IsDefault,
			Cost:             rel.Cost,
			MaxHops:          rel.MaxHops,
		})
	}

	return nil
}

func (c *APIStateClient) listSemanticMetrics(ctx context.Context, projectName, modelName string) ([]apiSemanticMetric, error) {
	pages, err := c.fetchAllPages(ctx, "/semantic-models/"+projectName+"/"+modelName+"/metrics")
	if err != nil {
		return nil, err
	}
	if len(pages) == 0 {
		return nil, nil
	}

	var metrics []apiSemanticMetric
	if err := mergePages(pages, &metrics); err != nil {
		return nil, err
	}
	return metrics, nil
}

func (c *APIStateClient) listSemanticPreAggregations(ctx context.Context, projectName, modelName string) ([]apiSemanticPreAggregation, error) {
	pages, err := c.fetchAllPages(ctx, "/semantic-models/"+projectName+"/"+modelName+"/pre-aggregations")
	if err != nil {
		return nil, err
	}
	if len(pages) == 0 {
		return nil, nil
	}

	var preAggs []apiSemanticPreAggregation
	if err := mergePages(pages, &preAggs); err != nil {
		return nil, err
	}
	return preAggs, nil
}

func (c *APIStateClient) listSemanticRelationships(ctx context.Context) ([]apiSemanticRelationship, error) {
	pages, err := c.fetchAllPages(ctx, "/semantic-relationships")
	if err != nil {
		return nil, err
	}
	if len(pages) == 0 {
		return nil, nil
	}

	var relationships []apiSemanticRelationship
	if err := mergePages(pages, &relationships); err != nil {
		return nil, err
	}
	return relationships, nil
}

// === Name-to-ID Resolution ===

// reverseLookupPrincipalName finds the principal name for a given ID by
// iterating the index maps. Returns "" if the ID is not found.
func (c *APIStateClient) reverseLookupPrincipalName(id, memberType string) string {
	if c.index == nil {
		return ""
	}
	source := c.index.principalIDByName
	if memberType == "group" {
		source = c.index.groupIDByName
	}
	for name, storedID := range source {
		if storedID == id {
			return name
		}
	}
	return ""
}

func reverseLookupByID(source map[string]string, id string) string {
	for name, storedID := range source {
		if storedID == id {
			return name
		}
	}
	return ""
}

func (c *APIStateClient) reverseLookupSecurablePath(securableType, id string) string {
	if c.index == nil {
		return ""
	}
	if id == "" {
		return ""
	}

	switch securableType {
	case "catalog":
		return reverseLookupByID(c.index.catalogIDByName, id)
	case "schema":
		return reverseLookupByID(c.index.schemaIDByPath, id)
	case "table":
		return reverseLookupByID(c.index.tableIDByPath, id)
	case "volume":
		return reverseLookupByID(c.index.volumeIDByPath, id)
	case "external_location":
		return reverseLookupByID(c.index.locationIDByName, id)
	case "storage_credential":
		return reverseLookupByID(c.index.credentialIDByName, id)
	default:
		return ""
	}
}

func (c *APIStateClient) lookupMemberNameByID(_ context.Context, id, memberType string) (string, error) {
	path := "/principals/" + id
	if memberType == "group" {
		path = "/groups/" + id
	}
	resp, err := c.client.Do(http.MethodGet, path, nil, nil)
	if err != nil {
		return "", err
	}
	body, err := apiruntime.ReadBody(resp)
	if err != nil {
		return "", err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(body))
	}
	var parsed struct {
		Name string `json:"name"`
	}
	if err := json.Unmarshal(body, &parsed); err != nil {
		return "", err
	}
	if parsed.Name == "" {
		return "", fmt.Errorf("empty name in response")
	}
	return parsed.Name, nil
}

// resolvePrincipalID looks up a principal or group UUID by name.
func (c *APIStateClient) resolvePrincipalID(name, principalType string) (string, error) {
	if c.index == nil {
		return "", fmt.Errorf("resource index not populated; call ReadState first")
	}
	if principalType == "group" {
		id, ok := c.index.groupIDByName[name]
		if !ok {
			return "", fmt.Errorf("group %q not found in index", name)
		}
		return id, nil
	}
	id, ok := c.index.principalIDByName[name]
	if !ok {
		return "", fmt.Errorf("principal %q not found in index", name)
	}
	return id, nil
}

func grantIdentity(grant declarative.GrantSpec) string {
	return strings.Join([]string{
		grant.Principal,
		grant.PrincipalType,
		grant.SecurableType,
		grant.Securable,
		grant.Privilege,
	}, "|")
}

func (c *APIStateClient) resolveGrantID(ctx context.Context, grant declarative.GrantSpec) (string, error) {
	if c.index != nil {
		if id, ok := c.index.grantIDByIdentity[grantIdentity(grant)]; ok {
			return id, nil
		}
	}
	principalID, err := c.resolvePrincipalID(grant.Principal, grant.PrincipalType)
	if err != nil {
		return "", err
	}
	securableID, err := c.resolveSecurableID(ctx, grant.SecurableType, grant.Securable)
	if err != nil {
		return "", err
	}
	pages, err := c.fetchAllPages(ctx, "/grants?principal_id="+url.QueryEscape(principalID)+"&principal_type="+url.QueryEscape(grant.PrincipalType))
	if err != nil {
		return "", err
	}
	var items []apiGrant
	if err := mergePages(pages, &items); err != nil {
		return "", err
	}
	for _, item := range items {
		if item.SecurableType != grant.SecurableType || item.SecurableID != securableID || item.Privilege != grant.Privilege {
			continue
		}
		if item.ID == "" {
			continue
		}
		if c.index != nil {
			c.index.grantIDByIdentity[grantIdentity(grant)] = item.ID
		}
		return item.ID, nil
	}
	return "", fmt.Errorf("grant %q not found", grantIdentity(grant))
}

// resolveSecurableID looks up a securable UUID by type and dot-path.
// It falls back to direct API lookups for schemas/tables when missing in the index.
func (c *APIStateClient) resolveSecurableID(ctx context.Context, securableType, path string) (string, error) {
	if c.index == nil {
		return "", fmt.Errorf("resource index not populated; call ReadState first")
	}
	switch securableType {
	case "catalog":
		if id, ok := c.index.catalogIDByName[path]; ok {
			return id, nil
		}
	case "schema":
		if id, ok := c.index.schemaIDByPath[path]; ok {
			return id, nil
		}
		parts := strings.SplitN(path, ".", 2)
		if len(parts) == 2 {
			id, err := c.lookupSchemaIDByPath(ctx, parts[0], parts[1])
			if err == nil && id != "" {
				c.index.schemaIDByPath[path] = id
				return id, nil
			}
		}
	case "table":
		if id, ok := c.index.tableIDByPath[path]; ok {
			return id, nil
		}
		parts := strings.SplitN(path, ".", 3)
		if len(parts) == 3 {
			id, err := c.lookupTableIDByPath(ctx, parts[0], parts[1], parts[2])
			if err == nil && id != "" {
				c.index.tableIDByPath[path] = id
				return id, nil
			}
		}
	case "volume":
		if id, ok := c.index.volumeIDByPath[path]; ok {
			return id, nil
		}
	case "external_location":
		if id, ok := c.index.locationIDByName[path]; ok {
			return id, nil
		}
	case "storage_credential":
		if id, ok := c.index.credentialIDByName[path]; ok {
			return id, nil
		}
	default:
		// For other types (volume, external_location, etc.) try all maps.
		if id, ok := c.index.volumeIDByPath[path]; ok {
			return id, nil
		}
		if id, ok := c.index.locationIDByName[path]; ok {
			return id, nil
		}
		if id, ok := c.index.credentialIDByName[path]; ok {
			return id, nil
		}
		if id, ok := c.index.tableIDByPath[path]; ok {
			return id, nil
		}
		if id, ok := c.index.schemaIDByPath[path]; ok {
			return id, nil
		}
		if id, ok := c.index.catalogIDByName[path]; ok {
			return id, nil
		}
		parts := strings.SplitN(path, ".", 3)
		if len(parts) == 3 {
			id, err := c.lookupTableIDByPath(ctx, parts[0], parts[1], parts[2])
			if err == nil && id != "" {
				c.index.tableIDByPath[path] = id
				return id, nil
			}
		}
		parts = strings.SplitN(path, ".", 2)
		if len(parts) == 2 {
			id, err := c.lookupSchemaIDByPath(ctx, parts[0], parts[1])
			if err == nil && id != "" {
				c.index.schemaIDByPath[path] = id
				return id, nil
			}
		}
	}
	return "", fmt.Errorf("%s %q not found in index", securableType, path)
}

// resolveTagID looks up a tag UUID by key or "key:value" string.
func (c *APIStateClient) resolveTagID(keyOrKeyValue string) (string, error) {
	if c.index == nil {
		return "", fmt.Errorf("resource index not populated; call ReadState first")
	}
	id, ok := c.index.tagIDByKey[keyOrKeyValue]
	if !ok {
		return "", fmt.Errorf("tag %q not found in index", keyOrKeyValue)
	}
	return id, nil
}

// resolveRowFilterID looks up a row filter UUID by "catalog.schema.table/filterName" path.
func (c *APIStateClient) resolveRowFilterID(resourceName string) (string, error) {
	if c.index == nil {
		return "", fmt.Errorf("resource index not populated; call ReadState first")
	}
	id, ok := c.index.rowFilterIDByPath[resourceName]
	if !ok {
		return "", fmt.Errorf("row filter %q not found in index", resourceName)
	}
	return id, nil
}

// resolveColumnMaskID looks up a column mask UUID by "catalog.schema.table/maskName" path.
func (c *APIStateClient) resolveColumnMaskID(resourceName string) (string, error) {
	if c.index == nil {
		return "", fmt.Errorf("resource index not populated; call ReadState first")
	}
	id, ok := c.index.columnMaskIDByPath[resourceName]
	if !ok {
		return "", fmt.Errorf("column mask %q not found in index", resourceName)
	}
	return id, nil
}

// checkCreateResponse reads the response body, checks for errors, and extracts the created resource ID.
func (c *APIStateClient) checkCreateResponse(resp *http.Response) (string, error) {
	body, err := apiruntime.ReadBody(resp)
	if err != nil {
		return "", err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		var apiErr struct {
			Code    interface{} `json:"code"`
			Message string      `json:"message"`
		}
		if json.Unmarshal(body, &apiErr) == nil && apiErr.Message != "" {
			return "", fmt.Errorf("API error (HTTP %d): %s", resp.StatusCode, apiErr.Message)
		}
		return "", fmt.Errorf("API error (HTTP %d): %s", resp.StatusCode, string(body))
	}
	var created struct {
		ID       string `json:"id"`
		SchemaID string `json:"schema_id"`
		TableID  string `json:"table_id"`
	}
	_ = json.Unmarshal(body, &created)
	id := created.ID
	if id == "" {
		id = created.SchemaID
	}
	if id == "" {
		id = created.TableID
	}
	return id, nil
}

func (c *APIStateClient) lookupSchemaIDByPath(_ context.Context, catalogName, schemaName string) (string, error) {
	resp, err := c.client.Do(http.MethodGet, "/catalogs/"+catalogName+"/schemas/"+schemaName, nil, nil)
	if err != nil {
		return "", err
	}
	id, err := c.checkCreateResponse(resp)
	if err != nil {
		return "", err
	}
	if id == "" {
		return "", fmt.Errorf("schema %q has no id in API response", catalogName+"."+schemaName)
	}
	return id, nil
}

func (c *APIStateClient) lookupTableIDByPath(_ context.Context, catalogName, schemaName, tableName string) (string, error) {
	resp, err := c.client.Do(http.MethodGet, "/catalogs/"+catalogName+"/schemas/"+schemaName+"/tables/"+tableName, nil, nil)
	if err != nil {
		return "", err
	}
	id, err := c.checkCreateResponse(resp)
	if err != nil {
		return "", err
	}
	if id == "" {
		return "", fmt.Errorf("table %q has no id in API response", catalogName+"."+schemaName+"."+tableName)
	}
	return id, nil
}

type apiColumnMask struct {
	ID             string `json:"id"`
	ColumnName     string `json:"column_name"`
	MaskExpression string `json:"mask_expression"`
	Description    string `json:"description"`
}

func (c *APIStateClient) lookupColumnMaskIDBySpec(ctx context.Context, tablePath string, mask declarative.ColumnMaskSpec) (string, error) {
	tableID, err := c.resolveSecurableID(ctx, "table", tablePath)
	if err != nil {
		return "", fmt.Errorf("resolve table for column mask lookup: %w", err)
	}

	pages, err := c.fetchAllPages(ctx, "/tables/"+tableID+"/column-masks")
	if err != nil {
		return "", err
	}

	var items []apiColumnMask
	if err := mergePages(pages, &items); err != nil {
		return "", err
	}

	for _, item := range items {
		if item.ColumnName != mask.ColumnName {
			continue
		}
		if item.MaskExpression != mask.MaskExpression {
			continue
		}
		if mask.Description != "" && item.Description != mask.Description {
			continue
		}
		if item.ID == "" {
			continue
		}
		return item.ID, nil
	}

	return "", fmt.Errorf("column mask %q not found in table %q", mask.Name, tablePath)
}

// === Execute ===

// Execute applies a single planned action to the server via the API.
func (c *APIStateClient) Execute(ctx context.Context, action declarative.Action) error {
	switch action.ResourceKind {
	case declarative.KindStorageCredential:
		return c.executeStorageCredential(ctx, action)
	case declarative.KindPrincipal:
		return c.executePrincipal(ctx, action)
	case declarative.KindGroup:
		return c.executeGroup(ctx, action)
	case declarative.KindExternalLocation:
		return c.executeExternalLocation(ctx, action)
	case declarative.KindComputeEndpoint:
		return c.executeComputeEndpoint(ctx, action)
	case declarative.KindComputeRoutingDefaults:
		return c.executeComputeRoutingDefaults(ctx, action)
	case declarative.KindComputeAssignment:
		return c.executeComputeAssignment(ctx, action)
	case declarative.KindGroupMembership:
		return c.executeGroupMembership(ctx, action)
	case declarative.KindPrivilegeGrant:
		return c.executeGrant(ctx, action)
	case declarative.KindDomain:
		return c.executeDomain(ctx, action)
	case declarative.KindTeam:
		return c.executeTeam(ctx, action)
	case declarative.KindCatalogRegistration:
		return c.executeCatalog(ctx, action)
	case declarative.KindSchema:
		return c.executeSchema(ctx, action)
	case declarative.KindTable:
		return c.executeTable(ctx, action)
	case declarative.KindView:
		return c.executeView(ctx, action)
	case declarative.KindVolume:
		return c.executeVolume(ctx, action)
	case declarative.KindTag:
		return c.executeTag(ctx, action)
	case declarative.KindTagAssignment:
		return c.executeTagAssignment(ctx, action)
	case declarative.KindRowFilter:
		return c.executeRowFilter(ctx, action)
	case declarative.KindRowFilterBinding:
		return c.executeRowFilterBinding(ctx, action)
	case declarative.KindColumnMask:
		return c.executeColumnMask(ctx, action)
	case declarative.KindColumnMaskBinding:
		return c.executeColumnMaskBinding(ctx, action)
	case declarative.KindAPIKey:
		return c.executeAPIKey(ctx, action)
	case declarative.KindNotebook:
		return c.executeNotebook(ctx, action)
	case declarative.KindDataProduct:
		return c.executeDataProduct(ctx, action)
	case declarative.KindAsset:
		return c.executeAsset(ctx, action)
	case declarative.KindMacro:
		return c.executeMacro(ctx, action)
	case declarative.KindModel:
		return c.executeModel(ctx, action)
	case declarative.KindSemanticModel:
		return c.executeSemanticModel(ctx, action)
	default:
		return fmt.Errorf("execute %s %s: resource kind not yet implemented", action.Operation, action.ResourceKind)
	}
}

func semanticModelPath(projectName, modelName string) string {
	return projectName + "." + modelName
}

func modelPath(projectName, modelName string) string {
	return projectName + "." + modelName
}

func (c *APIStateClient) resolveSemanticModelRefID(_ context.Context, defaultProject, modelRef string) (string, error) {
	projectName := defaultProject
	modelName := modelRef
	if strings.Contains(modelRef, ".") {
		parts := strings.SplitN(modelRef, ".", 2)
		projectName = parts[0]
		modelName = parts[1]
	}

	path := semanticModelPath(projectName, modelName)
	if c.index != nil {
		if id, ok := c.index.semanticModelIDByPath[path]; ok {
			return id, nil
		}
	}

	resp, err := c.client.Do(http.MethodGet, "/semantic-models/"+projectName+"/"+modelName, nil, nil)
	if err != nil {
		return "", err
	}
	id, err := c.checkCreateResponse(resp)
	if err != nil {
		return "", err
	}
	if id == "" {
		return "", fmt.Errorf("semantic model %q has no id in API response", path)
	}
	if c.index != nil {
		c.index.semanticModelIDByPath[path] = id
	}
	return id, nil
}

func (c *APIStateClient) reconcileSemanticMetrics(ctx context.Context, projectName, modelName string, desired []declarative.SemanticMetricSpec) error {
	actual, err := c.listSemanticMetrics(ctx, projectName, modelName)
	if err != nil {
		return err
	}

	actualByName := make(map[string]apiSemanticMetric, len(actual))
	for _, metric := range actual {
		actualByName[metric.Name] = metric
	}

	seen := make(map[string]struct{}, len(desired))
	for _, metric := range desired {
		seen[metric.Name] = struct{}{}
		body := map[string]interface{}{
			"description":         metric.Description,
			"metric_type":         metric.MetricType,
			"expression_mode":     metric.ExpressionMode,
			"expression":          metric.Expression,
			"default_time_grain":  metric.DefaultTimeGrain,
			"format":              metric.Format,
			"certification_state": metric.CertificationState,
		}

		if _, exists := actualByName[metric.Name]; exists {
			resp, err := c.client.Do(http.MethodPatch, "/semantic-models/"+projectName+"/"+modelName+"/metrics/"+metric.Name, nil, body)
			if err != nil {
				return err
			}
			if err := apiruntime.CheckError(resp); err != nil {
				return err
			}
			continue
		}

		body["name"] = metric.Name
		resp, err := c.client.Do(http.MethodPost, "/semantic-models/"+projectName+"/"+modelName+"/metrics", nil, body)
		if err != nil {
			return err
		}
		if err := apiruntime.CheckError(resp); err != nil {
			return err
		}
	}

	for name := range actualByName {
		if _, ok := seen[name]; ok {
			continue
		}
		resp, err := c.client.Do(http.MethodDelete, "/semantic-models/"+projectName+"/"+modelName+"/metrics/"+name, nil, nil)
		if err != nil {
			return err
		}
		if err := apiruntime.CheckError(resp); err != nil {
			return err
		}
	}

	return nil
}

func (c *APIStateClient) reconcileSemanticPreAggregations(ctx context.Context, projectName, modelName string, desired []declarative.SemanticPreAggSpec) error {
	actual, err := c.listSemanticPreAggregations(ctx, projectName, modelName)
	if err != nil {
		return err
	}

	actualByName := make(map[string]apiSemanticPreAggregation, len(actual))
	for _, preAgg := range actual {
		actualByName[preAgg.Name] = preAgg
	}

	seen := make(map[string]struct{}, len(desired))
	for _, preAgg := range desired {
		seen[preAgg.Name] = struct{}{}
		body := map[string]interface{}{
			"metric_set":      preAgg.MetricSet,
			"dimension_set":   preAgg.DimensionSet,
			"grain":           preAgg.Grain,
			"target_relation": preAgg.TargetRelation,
			"refresh_policy":  preAgg.RefreshPolicy,
		}

		if _, exists := actualByName[preAgg.Name]; exists {
			resp, err := c.client.Do(http.MethodPatch, "/semantic-models/"+projectName+"/"+modelName+"/pre-aggregations/"+preAgg.Name, nil, body)
			if err != nil {
				return err
			}
			if err := apiruntime.CheckError(resp); err != nil {
				return err
			}
			continue
		}

		body["name"] = preAgg.Name
		resp, err := c.client.Do(http.MethodPost, "/semantic-models/"+projectName+"/"+modelName+"/pre-aggregations", nil, body)
		if err != nil {
			return err
		}
		if err := apiruntime.CheckError(resp); err != nil {
			return err
		}
	}

	for name := range actualByName {
		if _, ok := seen[name]; ok {
			continue
		}
		resp, err := c.client.Do(http.MethodDelete, "/semantic-models/"+projectName+"/"+modelName+"/pre-aggregations/"+name, nil, nil)
		if err != nil {
			return err
		}
		if err := apiruntime.CheckError(resp); err != nil {
			return err
		}
	}

	return nil
}

func (c *APIStateClient) reconcileSemanticRelationships(ctx context.Context, modelID, projectName string, desired []declarative.SemanticRelationshipSpec) error {
	actual, err := c.listSemanticRelationships(ctx)
	if err != nil {
		return err
	}

	actualByName := make(map[string]apiSemanticRelationship)
	for _, rel := range actual {
		if rel.FromSemanticID == modelID {
			actualByName[rel.Name] = rel
		}
	}

	seen := make(map[string]struct{}, len(desired))
	for _, rel := range desired {
		seen[rel.Name] = struct{}{}
		toModelID, err := c.resolveSemanticModelRefID(ctx, projectName, rel.ToModel)
		if err != nil {
			return fmt.Errorf("resolve to_model %q for relationship %q: %w", rel.ToModel, rel.Name, err)
		}

		body := map[string]interface{}{
			"relationship_type": rel.RelationshipType,
			"join_sql":          rel.JoinSQL,
			"is_default":        rel.IsDefault,
			"cost":              rel.Cost,
			"max_hops":          rel.MaxHops,
		}

		if _, exists := actualByName[rel.Name]; exists {
			resp, err := c.client.Do(http.MethodPatch, "/semantic-relationships/"+rel.Name, nil, body)
			if err != nil {
				return err
			}
			if err := apiruntime.CheckError(resp); err != nil {
				return err
			}
			continue
		}

		body["name"] = rel.Name
		body["from_semantic_id"] = modelID
		body["to_semantic_id"] = toModelID
		resp, err := c.client.Do(http.MethodPost, "/semantic-relationships", nil, body)
		if err != nil {
			return err
		}
		if err := apiruntime.CheckError(resp); err != nil {
			return err
		}
	}

	for name := range actualByName {
		if _, ok := seen[name]; ok {
			continue
		}
		resp, err := c.client.Do(http.MethodDelete, "/semantic-relationships/"+name, nil, nil)
		if err != nil {
			return err
		}
		if err := apiruntime.CheckError(resp); err != nil {
			return err
		}
	}

	return nil
}

func (c *APIStateClient) reconcileSemanticChildren(ctx context.Context, modelID string, model declarative.SemanticModelResource) error {
	if err := c.reconcileSemanticMetrics(ctx, model.ProjectName, model.ModelName, model.Spec.Metrics); err != nil {
		return fmt.Errorf("reconcile semantic metrics: %w", err)
	}
	if err := c.reconcileSemanticPreAggregations(ctx, model.ProjectName, model.ModelName, model.Spec.PreAggregations); err != nil {
		return fmt.Errorf("reconcile semantic pre-aggregations: %w", err)
	}
	if err := c.reconcileSemanticRelationships(ctx, modelID, model.ProjectName, model.Spec.Relationships); err != nil {
		return fmt.Errorf("reconcile semantic relationships: %w", err)
	}
	return nil
}

func (c *APIStateClient) executeSemanticModel(ctx context.Context, action declarative.Action) error {
	switch action.Operation {
	case declarative.OpCreate:
		model := action.Desired.(declarative.SemanticModelResource)
		body := map[string]interface{}{
			"project_name":   model.ProjectName,
			"name":           model.ModelName,
			"description":    model.Spec.Description,
			"base_model_ref": model.Spec.BaseModelRef,
			"tags":           model.Spec.Tags,
		}
		if model.Spec.DefaultTimeDimension != "" {
			body["default_time_dimension"] = model.Spec.DefaultTimeDimension
		}

		resp, err := c.client.Do(http.MethodPost, "/semantic-models", nil, body)
		if err != nil {
			return err
		}
		id, err := c.checkCreateResponse(resp)
		if err != nil {
			return err
		}
		if id == "" {
			id, err = c.resolveSemanticModelRefID(ctx, model.ProjectName, model.ModelName)
			if err != nil {
				return err
			}
		}
		if c.index != nil {
			c.index.semanticModelIDByPath[semanticModelPath(model.ProjectName, model.ModelName)] = id
		}
		return c.reconcileSemanticChildren(ctx, id, model)

	case declarative.OpUpdate:
		model := action.Desired.(declarative.SemanticModelResource)
		body := map[string]interface{}{
			"description":    model.Spec.Description,
			"base_model_ref": model.Spec.BaseModelRef,
			"tags":           model.Spec.Tags,
		}
		if model.Spec.DefaultTimeDimension != "" {
			body["default_time_dimension"] = model.Spec.DefaultTimeDimension
		}
		resp, err := c.client.Do(http.MethodPatch, "/semantic-models/"+model.ProjectName+"/"+model.ModelName, nil, body)
		if err != nil {
			return err
		}
		if err := apiruntime.CheckError(resp); err != nil {
			return err
		}

		id, err := c.resolveSemanticModelRefID(ctx, model.ProjectName, model.ModelName)
		if err != nil {
			return err
		}
		return c.reconcileSemanticChildren(ctx, id, model)

	case declarative.OpDelete:
		parts := strings.SplitN(action.ResourceName, ".", 2)
		if len(parts) != 2 {
			return fmt.Errorf("invalid semantic model resource name: %s", action.ResourceName)
		}
		resp, err := c.client.Do(http.MethodDelete, "/semantic-models/"+parts[0]+"/"+parts[1], nil, nil)
		if err != nil {
			return err
		}
		return apiruntime.CheckError(resp)

	default:
		return fmt.Errorf("unsupported operation %s for semantic model", action.Operation)
	}
}

func (c *APIStateClient) executeAPIKey(ctx context.Context, action declarative.Action) error {
	switch action.Operation {
	case declarative.OpCreate:
		spec := action.Desired.(declarative.APIKeySpec)
		principalID, err := c.resolvePrincipalID(spec.Principal, "user")
		if err != nil {
			return fmt.Errorf("resolve principal for api key create: %w", err)
		}
		body := map[string]interface{}{
			"principal_id": principalID,
			"name":         spec.Name,
		}
		if spec.ExpiresAt != nil {
			body["expires_at"] = *spec.ExpiresAt
		}
		resp, err := c.client.Do(http.MethodPost, "/api-keys", nil, body)
		if err != nil {
			return err
		}
		return apiruntime.CheckError(resp)

	case declarative.OpUpdate:
		actual := action.Actual.(declarative.APIKeySpec)
		id, err := c.lookupAPIKeyID(ctx, actual)
		if err != nil {
			return fmt.Errorf("resolve api key for update: %w", err)
		}
		resp, err := c.client.Do(http.MethodDelete, "/api-keys/"+id, nil, nil)
		if err != nil {
			return err
		}
		if err := apiruntime.CheckError(resp); err != nil {
			return err
		}
		create := declarative.Action{Operation: declarative.OpCreate, Desired: action.Desired}
		return c.executeAPIKey(ctx, create)

	case declarative.OpDelete:
		spec := action.Actual.(declarative.APIKeySpec)
		id, err := c.lookupAPIKeyID(ctx, spec)
		if err != nil {
			return fmt.Errorf("resolve api key for delete: %w", err)
		}
		resp, err := c.client.Do(http.MethodDelete, "/api-keys/"+id, nil, nil)
		if err != nil {
			return err
		}
		return apiruntime.CheckError(resp)

	default:
		return fmt.Errorf("unsupported operation %s for api-key", action.Operation)
	}
}

func (c *APIStateClient) executeNotebook(ctx context.Context, action declarative.Action) error {
	switch action.Operation {
	case declarative.OpCreate:
		nb := action.Desired.(declarative.NotebookResource)
		body := map[string]interface{}{
			"name": nb.Name,
		}
		if nb.Spec.Description != "" {
			body["description"] = nb.Spec.Description
		}
		resp, err := c.client.Do(http.MethodPost, "/notebooks", nil, body)
		if err != nil {
			return err
		}
		var notebookID string
		if resp.StatusCode == http.StatusConflict {
			notebookID, err = c.lookupNotebookIDByName(ctx, nb.Name)
			if err != nil {
				return fmt.Errorf("notebook already exists and lookup failed: %w", err)
			}
		} else {
			notebookID, err = c.checkCreateResponse(resp)
			if err != nil {
				return err
			}
			if notebookID == "" {
				notebookID, err = c.lookupNotebookIDByName(ctx, nb.Name)
				if err != nil {
					return fmt.Errorf("lookup created notebook %q: %w", nb.Name, err)
				}
			}
		}
		if c.index != nil {
			c.index.notebookIDByName[nb.Name] = notebookID
		}
		if err := c.syncNotebookCells(ctx, notebookID, nb.Spec.Cells); err != nil {
			return err
		}
		return c.reconcileNotebookPublish(ctx, notebookID, nb.Spec)

	case declarative.OpUpdate:
		nb := action.Desired.(declarative.NotebookResource)
		notebookID, err := c.resolveNotebookID(ctx, nb.Name)
		if err != nil {
			return fmt.Errorf("resolve notebook for update: %w", err)
		}
		body := map[string]interface{}{
			"name":        nb.Name,
			"description": nb.Spec.Description,
		}
		resp, err := c.client.Do(http.MethodPatch, "/notebooks/"+notebookID, nil, body)
		if err != nil {
			return err
		}
		if err := apiruntime.CheckError(resp); err != nil {
			return err
		}
		if err := c.syncNotebookCells(ctx, notebookID, nb.Spec.Cells); err != nil {
			return err
		}
		return c.reconcileNotebookPublish(ctx, notebookID, nb.Spec)

	case declarative.OpDelete:
		notebookName := action.ResourceName
		if actual, ok := action.Actual.(declarative.NotebookResource); ok && actual.Name != "" {
			notebookName = actual.Name
		}
		notebookID, err := c.resolveNotebookID(ctx, notebookName)
		if err != nil {
			return fmt.Errorf("resolve notebook for delete: %w", err)
		}
		resp, err := c.client.Do(http.MethodDelete, "/notebooks/"+notebookID, nil, nil)
		if err != nil {
			return err
		}
		if err := apiruntime.CheckError(resp); err != nil {
			return err
		}
		if c.index != nil {
			delete(c.index.notebookIDByName, notebookName)
		}
		return nil

	default:
		return fmt.Errorf("unsupported operation %s for notebook", action.Operation)
	}
}

func (c *APIStateClient) executeDomain(_ context.Context, action declarative.Action) error {
	switch action.Operation {
	case declarative.OpCreate:
		item := action.Desired.(declarative.DomainResource)
		resp, err := c.client.Do(http.MethodPost, "/domains", nil, map[string]interface{}{
			"name":        item.Name,
			"description": item.Spec.Description,
		})
		if err != nil {
			return err
		}
		if resp.StatusCode == http.StatusConflict {
			return nil
		}
		return apiruntime.CheckError(resp)
	case declarative.OpUpdate:
		item := action.Desired.(declarative.DomainResource)
		resp, err := c.client.Do(http.MethodPut, "/domains/"+item.Name, nil, map[string]interface{}{
			"description": item.Spec.Description,
		})
		if err != nil {
			return err
		}
		return apiruntime.CheckError(resp)
	case declarative.OpDelete:
		name := action.ResourceName
		if actual, ok := action.Actual.(declarative.DomainResource); ok && actual.Name != "" {
			name = actual.Name
		}
		resp, err := c.client.Do(http.MethodDelete, "/domains/"+name, nil, nil)
		if err != nil {
			return err
		}
		return apiruntime.CheckError(resp)
	default:
		return fmt.Errorf("unsupported operation %s for domain", action.Operation)
	}
}

func (c *APIStateClient) executeTeam(_ context.Context, action declarative.Action) error {
	switch action.Operation {
	case declarative.OpCreate:
		item := action.Desired.(declarative.TeamResource)
		resp, err := c.client.Do(http.MethodPost, "/teams", nil, map[string]interface{}{
			"domain_name":     item.Spec.DomainRef,
			"name":            item.Name,
			"contact_channel": item.Spec.ContactChannel,
		})
		if err != nil {
			return err
		}
		if resp.StatusCode == http.StatusConflict {
			return nil
		}
		return apiruntime.CheckError(resp)
	case declarative.OpUpdate:
		item := action.Desired.(declarative.TeamResource)
		resp, err := c.client.Do(http.MethodPut, "/teams/"+item.Spec.DomainRef+"/"+item.Name, nil, map[string]interface{}{
			"contact_channel": item.Spec.ContactChannel,
		})
		if err != nil {
			return err
		}
		return apiruntime.CheckError(resp)
	case declarative.OpDelete:
		item := action.Actual.(declarative.TeamResource)
		resp, err := c.client.Do(http.MethodDelete, "/teams/"+item.Spec.DomainRef+"/"+item.Name, nil, nil)
		if err != nil {
			return err
		}
		return apiruntime.CheckError(resp)
	default:
		return fmt.Errorf("unsupported operation %s for team", action.Operation)
	}
}

func (c *APIStateClient) executeDataProduct(ctx context.Context, action declarative.Action) error {
	switch action.Operation {
	case declarative.OpCreate:
		return c.applyDataProductCreate(ctx, action.Desired.(declarative.DataProductResource))
	case declarative.OpUpdate:
		return c.applyDataProductUpdate(ctx, action.Desired.(declarative.DataProductResource), action.Actual.(declarative.DataProductResource))
	case declarative.OpDelete:
		slug := action.ResourceName
		if actual, ok := action.Actual.(declarative.DataProductResource); ok && actual.Slug != "" {
			slug = actual.Slug
		}
		resp, err := c.client.Do(http.MethodDelete, "/data-products/"+slug, nil, nil)
		if err != nil {
			return err
		}
		return apiruntime.CheckError(resp)
	default:
		return fmt.Errorf("unsupported operation %s for data-product", action.Operation)
	}
}

func (c *APIStateClient) applyDataProductCreate(ctx context.Context, item declarative.DataProductResource) error {
	createSpec, topLevelPatch, err := c.prepareCreateDataProduct(item)
	if err != nil {
		return err
	}
	resp, err := c.client.Do(http.MethodPost, "/data-products", nil, createSpec)
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusConflict {
		if err := apiruntime.CheckError(resp); err != nil {
			return err
		}
	}
	if len(topLevelPatch) > 0 {
		resp, err = c.client.Do(http.MethodPut, "/data-products/"+item.Slug, nil, topLevelPatch)
		if err != nil {
			return err
		}
		if err := apiruntime.CheckError(resp); err != nil {
			return err
		}
	}
	for _, version := range sortedProductVersions(item.Spec.Versions) {
		if version.Version == 1 {
			if err := c.applyProductVersionState(ctx, item.Slug, domain.ProductReleaseStateDraft, version.ReleaseState, version.Version); err != nil {
				return err
			}
			continue
		}
		resp, err := c.client.Do(http.MethodPost, "/data-products/"+item.Slug+"/versions", nil, dataProductVersionBody(version))
		if err != nil {
			return err
		}
		if err := apiruntime.CheckError(resp); err != nil {
			return err
		}
		if err := c.applyProductVersionState(ctx, item.Slug, domain.ProductReleaseStateDraft, version.ReleaseState, version.Version); err != nil {
			return err
		}
	}
	for _, dependency := range item.Spec.Dependencies {
		resp, err := c.client.Do(http.MethodPost, "/data-products/"+item.Slug+"/dependencies", nil, map[string]interface{}{
			"depends_on_slug": dependency,
		})
		if err != nil {
			return err
		}
		if resp.StatusCode == http.StatusConflict {
			continue
		}
		if err := apiruntime.CheckError(resp); err != nil {
			return err
		}
	}
	if len(item.Spec.Versions) == 0 && item.Spec.PublicationIntent == domain.ProductPublicationIntentPublished {
		if err := c.applyProductVersionState(ctx, item.Slug, domain.ProductReleaseStateDraft, domain.ProductReleaseStatePublished, 1); err != nil {
			return err
		}
	}
	return nil
}

func (c *APIStateClient) applyDataProductUpdate(ctx context.Context, desired, actual declarative.DataProductResource) error {
	resp, err := c.client.Do(http.MethodPut, "/data-products/"+desired.Slug, nil, dataProductUpdateBody(desired.Slug, desired.Spec))
	if err != nil {
		return err
	}
	if err := apiruntime.CheckError(resp); err != nil {
		return err
	}

	if len(desired.Spec.Versions) > 0 {
		actualVersions := map[int]declarative.DataProductVersionSpec{}
		for _, version := range actual.Spec.Versions {
			actualVersions[version.Version] = version
		}
		for _, version := range sortedProductVersions(desired.Spec.Versions) {
			current, exists := actualVersions[version.Version]
			if !exists {
				resp, err := c.client.Do(http.MethodPost, "/data-products/"+desired.Slug+"/versions", nil, dataProductVersionBody(version))
				if err != nil {
					return err
				}
				if err := apiruntime.CheckError(resp); err != nil {
					return err
				}
				if err := c.applyProductVersionState(ctx, desired.Slug, domain.ProductReleaseStateDraft, version.ReleaseState, version.Version); err != nil {
					return err
				}
				continue
			}
			if !reflect.DeepEqual(normalizeProductVersionForMutation(current), normalizeProductVersionForMutation(version)) {
				return fmt.Errorf("data product %q version %d is immutable; create a new version instead of modifying it", desired.Slug, version.Version)
			}
			if err := c.applyProductVersionState(ctx, desired.Slug, current.ReleaseState, version.ReleaseState, version.Version); err != nil {
				return err
			}
			delete(actualVersions, version.Version)
		}
		if len(actualVersions) > 0 {
			extra := make([]int, 0, len(actualVersions))
			for version := range actualVersions {
				extra = append(extra, version)
			}
			sort.Ints(extra)
			return fmt.Errorf("data product %q has existing versions %v that cannot be removed declaratively", desired.Slug, extra)
		}
	} else if desired.Spec.PublicationIntent == domain.ProductPublicationIntentPublished {
		if versionOne, ok := findProductVersion(actual.Spec.Versions, 1); ok {
			if err := c.applyProductVersionState(ctx, desired.Slug, versionOne.ReleaseState, domain.ProductReleaseStatePublished, 1); err != nil {
				return err
			}
		}
	}

	desiredDeps := make(map[string]struct{}, len(desired.Spec.Dependencies))
	for _, dependency := range desired.Spec.Dependencies {
		desiredDeps[dependency] = struct{}{}
	}
	actualDeps := make(map[string]struct{}, len(actual.Spec.Dependencies))
	for _, dependency := range actual.Spec.Dependencies {
		actualDeps[dependency] = struct{}{}
	}
	for dependency := range desiredDeps {
		if _, ok := actualDeps[dependency]; ok {
			continue
		}
		resp, err := c.client.Do(http.MethodPost, "/data-products/"+desired.Slug+"/dependencies", nil, map[string]interface{}{
			"depends_on_slug": dependency,
		})
		if err != nil {
			return err
		}
		if resp.StatusCode == http.StatusConflict {
			continue
		}
		if err := apiruntime.CheckError(resp); err != nil {
			return err
		}
	}
	var removedDeps []string
	for dependency := range actualDeps {
		if _, ok := desiredDeps[dependency]; !ok {
			removedDeps = append(removedDeps, dependency)
		}
	}
	if len(removedDeps) > 0 {
		sort.Strings(removedDeps)
		return fmt.Errorf("data product %q dependencies %v cannot be removed with the current public API", desired.Slug, removedDeps)
	}
	return nil
}

func (c *APIStateClient) applyProductVersionState(_ context.Context, slug, actualState, desiredState string, version int) error {
	if desiredState == "" || desiredState == actualState {
		return nil
	}
	switch desiredState {
	case domain.ProductReleaseStateDraft:
		if actualState == "" || actualState == domain.ProductReleaseStateDraft {
			return nil
		}
		return fmt.Errorf("data product %q version %d cannot transition back to DRAFT", slug, version)
	case domain.ProductReleaseStatePublished:
		switch actualState {
		case "", domain.ProductReleaseStateDraft:
			resp, err := c.client.Do(http.MethodPatch, "/data-products/"+slug+"/publish", nil, map[string]interface{}{"version": version})
			if err != nil {
				return err
			}
			return apiruntime.CheckError(resp)
		case domain.ProductReleaseStatePublished:
			return nil
		default:
			return fmt.Errorf("data product %q version %d cannot transition from %s to PUBLISHED", slug, version, actualState)
		}
	case domain.ProductReleaseStateDeprecated:
		switch actualState {
		case "", domain.ProductReleaseStateDraft, domain.ProductReleaseStatePublished:
			resp, err := c.client.Do(http.MethodPatch, "/data-products/"+slug+"/deprecate", nil, map[string]interface{}{"version": version})
			if err != nil {
				return err
			}
			return apiruntime.CheckError(resp)
		case domain.ProductReleaseStateDeprecated:
			return nil
		default:
			return fmt.Errorf("data product %q version %d cannot transition from %s to DEPRECATED", slug, version, actualState)
		}
	case domain.ProductReleaseStateRetired:
		if actualState == domain.ProductReleaseStateRetired {
			return nil
		}
		resp, err := c.client.Do(http.MethodPatch, "/data-products/"+slug+"/retire", nil, map[string]interface{}{"version": version})
		if err != nil {
			return err
		}
		return apiruntime.CheckError(resp)
	default:
		return fmt.Errorf("unsupported product release state %q for %s version %d", desiredState, slug, version)
	}
}

func (c *APIStateClient) prepareCreateDataProduct(item declarative.DataProductResource) (map[string]interface{}, map[string]interface{}, error) {
	name := item.Spec.Name
	if name == "" {
		name = item.Slug
	}
	body := dataProductCreateBody(item.Slug, item.Spec)
	topLevelPatch := map[string]interface{}{}
	if versionOne, ok := findProductVersion(item.Spec.Versions, 1); ok {
		body["contract"] = versionOne.Contract
		body["slo"] = versionOne.SLO
		body["docs_url"] = versionOne.DocsURL
		body["access_request_path"] = versionOne.AccessRequestPath
		body["semantic_model_refs"] = versionOne.SemanticEntrypoints
		delete(body, "primary_asset_key")
		if len(versionOne.Outputs) > 0 {
			body["primary_asset_key"] = versionOne.Outputs[0]
		}

		if !reflect.DeepEqual(versionOne.Contract, item.Spec.Contract) ||
			!reflect.DeepEqual(versionOne.SLO, item.Spec.SLO) ||
			versionOne.DocsURL != item.Spec.DocsURL ||
			versionOne.AccessRequestPath != item.Spec.AccessRequestPath {
			topLevelPatch = dataProductUpdateBody(item.Slug, item.Spec)
		}
	}
	body["name"] = name
	return body, topLevelPatch, nil
}

func dataProductCreateBody(slug string, spec declarative.DataProductSpec) map[string]interface{} {
	name := spec.Name
	if name == "" {
		name = slug
	}
	body := map[string]interface{}{
		"slug":                 slug,
		"name":                 name,
		"description":          spec.Description,
		"domain_name":          spec.DomainRef,
		"team_name":            spec.OwnerTeamRef,
		"steward_principal":    spec.StewardPrincipal,
		"contact_channel":      spec.ContactChannel,
		"visibility":           spec.Visibility,
		"consumer_audience":    spec.ConsumerAudience,
		"docs_url":             spec.DocsURL,
		"access_request_path":  spec.AccessRequestPath,
		"business_definitions": spec.BusinessDefinitions,
		"contract":             spec.Contract,
		"slo":                  spec.SLO,
		"semantic_model_refs":  spec.SemanticEntrypoints,
		"created_by":           "declarative",
	}
	if len(spec.Outputs) > 0 {
		body["primary_asset_key"] = spec.Outputs[0]
	}
	return body
}

func dataProductUpdateBody(slug string, spec declarative.DataProductSpec) map[string]interface{} {
	name := spec.Name
	if name == "" {
		name = slug
	}
	return map[string]interface{}{
		"name":                 name,
		"description":          spec.Description,
		"domain_name":          spec.DomainRef,
		"team_name":            spec.OwnerTeamRef,
		"steward_principal":    spec.StewardPrincipal,
		"contact_channel":      spec.ContactChannel,
		"visibility":           spec.Visibility,
		"consumer_audience":    spec.ConsumerAudience,
		"docs_url":             spec.DocsURL,
		"access_request_path":  spec.AccessRequestPath,
		"business_definitions": spec.BusinessDefinitions,
		"contract":             spec.Contract,
		"slo":                  spec.SLO,
		"publication_intent":   spec.PublicationIntent,
	}
}

func dataProductVersionBody(spec declarative.DataProductVersionSpec) map[string]interface{} {
	return map[string]interface{}{
		"compatibility_level": spec.CompatibilityLevel,
		"contract":            spec.Contract,
		"slo":                 spec.SLO,
		"docs_url":            spec.DocsURL,
		"access_request_path": spec.AccessRequestPath,
		"output_asset_keys":   spec.Outputs,
		"semantic_model_refs": spec.SemanticEntrypoints,
		"created_by":          "declarative",
	}
}

func sortedProductVersions(versions []declarative.DataProductVersionSpec) []declarative.DataProductVersionSpec {
	cloned := append([]declarative.DataProductVersionSpec(nil), versions...)
	sort.Slice(cloned, func(i, j int) bool {
		return cloned[i].Version < cloned[j].Version
	})
	return cloned
}

func findProductVersion(versions []declarative.DataProductVersionSpec, version int) (declarative.DataProductVersionSpec, bool) {
	for _, item := range versions {
		if item.Version == version {
			return item, true
		}
	}
	return declarative.DataProductVersionSpec{}, false
}

func normalizeProductVersionForMutation(spec declarative.DataProductVersionSpec) declarative.DataProductVersionSpec {
	spec.ReleaseState = ""
	return spec
}

func (c *APIStateClient) executeAsset(_ context.Context, action declarative.Action) error {
	toAssetBody := func(asset declarative.AssetResource) map[string]interface{} {
		body := map[string]interface{}{
			"asset_key":     asset.Name,
			"asset_type":    asset.Spec.AssetType,
			"product_slug":  asset.Spec.ProductRef,
			"owner":         asset.Spec.Owner,
			"description":   asset.Spec.Description,
			"tags":          asset.Spec.Tags,
			"io_profile":    asset.Spec.IOProfile,
			"is_active":     true,
			"cron_schedule": asset.Spec.CronSchedule,
		}
		if len(asset.Spec.DependsOn) > 0 {
			body["depends_on"] = asset.Spec.DependsOn
		}
		if asset.Spec.PartitionDefinition != nil {
			body["partition_definition"] = asset.Spec.PartitionDefinition
		}
		if asset.Spec.AutoMaterializePolicy != nil {
			body["auto_materialize_policy"] = asset.Spec.AutoMaterializePolicy
		}
		if asset.Spec.FreshnessPolicy != nil {
			body["freshness_policy"] = asset.Spec.FreshnessPolicy
		}
		if asset.Spec.MaterializationPolicy != nil {
			body["materialization_policy"] = asset.Spec.MaterializationPolicy
		}
		if asset.Spec.PartitionType != "" {
			body["partition_type"] = asset.Spec.PartitionType
		}
		if asset.Spec.MaxLagSeconds != nil {
			body["max_lag_seconds"] = *asset.Spec.MaxLagSeconds
		}
		if len(asset.Spec.CheckDefinitions) > 0 {
			body["checks"] = asset.Spec.CheckDefinitions
		}
		if len(asset.Spec.Properties) > 0 {
			body["properties"] = asset.Spec.Properties
		}
		return body
	}

	switch action.Operation {
	case declarative.OpCreate:
		asset := action.Desired.(declarative.AssetResource)
		resp, err := c.client.Do(http.MethodPost, "/assets", nil, toAssetBody(asset))
		if err != nil {
			return err
		}
		if resp.StatusCode == http.StatusConflict {
			return nil
		}
		return apiruntime.CheckError(resp)
	case declarative.OpUpdate:
		asset := action.Desired.(declarative.AssetResource)
		resp, err := c.client.Do(http.MethodPut, "/assets/"+asset.Name, nil, toAssetBody(asset))
		if err != nil {
			return err
		}
		return apiruntime.CheckError(resp)
	case declarative.OpDelete:
		key := action.ResourceName
		if actual, ok := action.Actual.(declarative.AssetResource); ok && actual.Name != "" {
			key = actual.Name
		}
		resp, err := c.client.Do(http.MethodDelete, "/assets/"+key, nil, nil)
		if err != nil {
			return err
		}
		return apiruntime.CheckError(resp)
	default:
		return fmt.Errorf("unsupported operation %s for asset", action.Operation)
	}
}

func (c *APIStateClient) syncNotebookCells(ctx context.Context, notebookID string, desired []declarative.CellSpec) error {
	detail, err := c.readNotebookDetail(ctx, notebookID)
	if err != nil {
		return err
	}
	sort.Slice(detail.Cells, func(i, j int) bool {
		return detail.Cells[i].Position < detail.Cells[j].Position
	})
	protectedOutputCellID := ""
	if detail.PublishModel != nil {
		protectedOutputCellID = detail.PublishModel.OutputCellID
	}

	for i, existing := range detail.Cells {
		if i >= len(desired) {
			if existing.ID == protectedOutputCellID {
				return fmt.Errorf("cannot delete published output cell %q from notebook %q during declarative sync", existing.ID, notebookID)
			}
			continue
		}
		if existing.CellType != desired[i].Type && existing.ID == protectedOutputCellID {
			return fmt.Errorf("cannot replace published output cell %q in notebook %q because cell_type changed", existing.ID, notebookID)
		}
	}

	for i := 0; i < len(detail.Cells) && i < len(desired); i++ {
		existing := detail.Cells[i]
		cell := desired[i]
		if existing.CellType != cell.Type {
			continue
		}
		resp, err := c.client.Do(http.MethodPatch, "/notebooks/"+notebookID+"/cells/"+existing.ID, nil, notebookCellBody(cell, i, false))
		if err != nil {
			return err
		}
		if err := apiruntime.CheckError(resp); err != nil {
			return err
		}
	}

	for i := len(detail.Cells); i < len(desired); i++ {
		resp, err := c.client.Do(http.MethodPost, "/notebooks/"+notebookID+"/cells", nil, notebookCellBody(desired[i], i, true))
		if err != nil {
			return err
		}
		if err := apiruntime.CheckError(resp); err != nil {
			return err
		}
	}

	for i := len(detail.Cells) - 1; i >= len(desired); i-- {
		cell := detail.Cells[i]
		resp, err := c.client.Do(http.MethodDelete, "/notebooks/"+notebookID+"/cells/"+cell.ID, nil, nil)
		if err != nil {
			return err
		}
		if err := apiruntime.CheckError(resp); err != nil {
			return err
		}
	}

	for i := 0; i < len(detail.Cells) && i < len(desired); i++ {
		existing := detail.Cells[i]
		cell := desired[i]
		if existing.CellType == cell.Type {
			continue
		}
		resp, err := c.client.Do(http.MethodDelete, "/notebooks/"+notebookID+"/cells/"+existing.ID, nil, nil)
		if err != nil {
			return err
		}
		if err := apiruntime.CheckError(resp); err != nil {
			return err
		}
		resp, err = c.client.Do(http.MethodPost, "/notebooks/"+notebookID+"/cells", nil, notebookCellBody(cell, i, true))
		if err != nil {
			return err
		}
		if err := apiruntime.CheckError(resp); err != nil {
			return err
		}
	}

	return nil
}

func notebookCellBody(cell declarative.CellSpec, position int, includeCellType bool) map[string]interface{} {
	body := map[string]interface{}{
		"content":  cell.Content,
		"position": position,
	}
	if includeCellType {
		body["cell_type"] = cell.Type
	}
	if cell.Name != "" {
		body["name"] = cell.Name
	}
	if cell.Role != "" {
		body["role"] = cell.Role
	}
	if cell.Disabled {
		body["disabled"] = true
	}
	if cell.Test != nil {
		body["test"] = map[string]interface{}{
			"severity": cell.Test.Severity,
		}
	}
	if cell.VisualSpec != nil {
		body["visual_spec"] = cell.VisualSpec
	}
	return body
}

func (c *APIStateClient) reconcileNotebookPublish(_ context.Context, notebookID string, spec declarative.NotebookSpec) error {
	if spec.Publish == nil || spec.Publish.Model == nil {
		return nil
	}
	outputIndex := -1
	for i, cell := range spec.Cells {
		if cell.Name == spec.Publish.Model.OutputCell {
			outputIndex = i
			break
		}
	}
	if outputIndex < 0 {
		return fmt.Errorf("published output cell %q not found in notebook %q", spec.Publish.Model.OutputCell, notebookID)
	}
	body := map[string]interface{}{
		"notebook_id":  notebookID,
		"cell_index":   outputIndex,
		"project_name": spec.Publish.Model.Project,
		"name":         spec.Publish.Model.Name,
	}
	if spec.Publish.Model.Materialization != "" {
		body["materialization"] = spec.Publish.Model.Materialization
	}
	resp, err := c.client.Do(http.MethodPost, "/models/from-notebook", nil, body)
	if err != nil {
		return err
	}
	return apiruntime.CheckError(resp)
}

func (c *APIStateClient) resolveNotebookID(ctx context.Context, notebookName string) (string, error) {
	if c.index != nil {
		if id, ok := c.index.notebookIDByName[notebookName]; ok {
			return id, nil
		}
	}
	return c.lookupNotebookIDByName(ctx, notebookName)
}

func (c *APIStateClient) lookupNotebookIDByName(ctx context.Context, notebookName string) (string, error) {
	pages, err := c.fetchAllPages(ctx, "/notebooks")
	if err != nil {
		return "", err
	}
	if len(pages) == 0 {
		return "", fmt.Errorf("notebook %q not found", notebookName)
	}
	var notebooks []apiNotebook
	if err := mergePages(pages, &notebooks); err != nil {
		return "", err
	}
	for _, notebook := range notebooks {
		if notebook.Name == notebookName {
			if notebook.ID == "" {
				return "", fmt.Errorf("notebook %q has empty id", notebookName)
			}
			if c.index != nil {
				c.index.notebookIDByName[notebookName] = notebook.ID
			}
			return notebook.ID, nil
		}
	}
	return "", fmt.Errorf("notebook %q not found", notebookName)
}

// ValidateNoSelfAPIKeyDeletion fails fast when a plan would delete the API key
// currently used by the CLI for authentication.
func (c *APIStateClient) ValidateNoSelfAPIKeyDeletion(ctx context.Context, actions []declarative.Action) error {
	authPrefix := c.currentAPIKeyPrefix()
	if authPrefix == "" {
		return nil
	}

	needsCheck := false
	for _, action := range actions {
		if action.ResourceKind != declarative.KindAPIKey {
			continue
		}
		if action.Operation == declarative.OpDelete || action.Operation == declarative.OpUpdate {
			needsCheck = true
			break
		}
	}
	if !needsCheck {
		return nil
	}

	items, err := c.listAPIKeys(ctx)
	if err != nil {
		return fmt.Errorf("list api keys: %w", err)
	}

	for _, action := range actions {
		if action.ResourceKind != declarative.KindAPIKey {
			continue
		}
		if action.Operation != declarative.OpDelete && action.Operation != declarative.OpUpdate {
			continue
		}

		spec, ok := action.Actual.(declarative.APIKeySpec)
		if !ok {
			return fmt.Errorf("invalid api-key action payload for %s", action.Operation)
		}

		item, err := c.lookupAPIKeyFromList(items, spec)
		if err != nil {
			return fmt.Errorf("resolve api key %q for %s: %w", action.ResourceName, action.Operation, err)
		}

		if item.KeyPrefix != "" && item.KeyPrefix == authPrefix {
			return fmt.Errorf("plan %s api-key %q would revoke the currently-authenticated API key; rerun with a different API key or --token", action.Operation, spec.Name)
		}
	}

	return nil
}

func (c *APIStateClient) currentAPIKeyPrefix() string {
	if c == nil || c.client == nil || c.client.APIKey == "" {
		return ""
	}
	if len(c.client.APIKey) <= 8 {
		return c.client.APIKey
	}
	return c.client.APIKey[:8]
}

func (c *APIStateClient) listAPIKeys(ctx context.Context) ([]apiAPIKey, error) {
	pages, err := c.fetchAllPages(ctx, "/api-keys")
	if err != nil {
		return nil, err
	}
	var items []apiAPIKey
	if err := mergePages(pages, &items); err != nil {
		return nil, err
	}
	return items, nil
}

func (c *APIStateClient) lookupAPIKeyFromList(items []apiAPIKey, spec declarative.APIKeySpec) (*apiAPIKey, error) {
	for i := range items {
		item := &items[i]
		if item.Name != spec.Name {
			continue
		}
		if spec.Principal != "" {
			principal := item.Principal
			if principal == "" && item.PrincipalID != "" {
				principal = c.reverseLookupPrincipalName(item.PrincipalID, "user")
			}
			if principal != "" && principal != spec.Principal {
				continue
			}
		}
		if item.ID == "" {
			return nil, fmt.Errorf("api key %q has no id in API response", spec.Name)
		}
		return item, nil
	}
	if spec.Principal != "" {
		return nil, fmt.Errorf("api key %q for principal %q not found", spec.Name, spec.Principal)
	}
	return nil, fmt.Errorf("api key %q not found", spec.Name)
}

func (c *APIStateClient) lookupAPIKeyID(ctx context.Context, spec declarative.APIKeySpec) (string, error) {
	items, err := c.listAPIKeys(ctx)
	if err != nil {
		return "", err
	}
	item, err := c.lookupAPIKeyFromList(items, spec)
	if err != nil {
		return "", err
	}
	return item.ID, nil
}

func canonicalIncrementalStrategy(value string) string {
	v := strings.ToLower(strings.TrimSpace(value))
	if v == "delete+insert" {
		return "delete_insert"
	}
	return v
}

func canonicalOnSchemaChange(value string) string {
	return strings.ToLower(strings.TrimSpace(value))
}

func modelConfigBody(config *declarative.ModelConfigSpec) map[string]interface{} {
	if config == nil {
		return nil
	}
	body := map[string]interface{}{}
	if len(config.UniqueKey) > 0 {
		body["unique_key"] = config.UniqueKey
	}
	if value := canonicalIncrementalStrategy(config.IncrementalStrategy); value != "" {
		body["incremental_strategy"] = value
	}
	if value := canonicalOnSchemaChange(config.OnSchemaChange); value != "" {
		body["on_schema_change"] = value
	}
	if len(body) == 0 {
		return nil
	}
	return body
}

func modelContractBody(contract *declarative.ContractSpec) map[string]interface{} {
	if contract == nil {
		return nil
	}
	body := map[string]interface{}{
		"enforce": contract.Enforce,
	}
	if len(contract.Columns) > 0 {
		columns := make([]map[string]interface{}, len(contract.Columns))
		for i, col := range contract.Columns {
			columns[i] = map[string]interface{}{
				"name":     col.Name,
				"type":     col.Type,
				"nullable": col.Nullable,
			}
		}
		body["columns"] = columns
	}
	return body
}

func modelFreshnessBody(freshness *declarative.FreshnessSpecYAML) map[string]interface{} {
	if freshness == nil {
		return nil
	}
	body := map[string]interface{}{}
	if freshness.MaxLagSeconds > 0 {
		body["max_lag_seconds"] = freshness.MaxLagSeconds
	}
	if freshness.CronSchedule != "" {
		body["cron_schedule"] = freshness.CronSchedule
	}
	if len(body) == 0 {
		return nil
	}
	return body
}

func toModelTestBody(test declarative.TestSpec) map[string]interface{} {
	body := map[string]interface{}{
		"name":      test.Name,
		"test_type": test.Type,
	}
	if test.Column != "" {
		body["column"] = test.Column
	}

	config := map[string]interface{}{}
	if len(test.Values) > 0 {
		config["values"] = test.Values
	}
	if test.ToModel != "" {
		config["to_model"] = test.ToModel
	}
	if test.ToColumn != "" {
		config["to_column"] = test.ToColumn
	}
	if test.SQL != "" {
		config["custom_sql"] = test.SQL
	}
	if len(config) > 0 {
		body["config"] = config
	}

	return body
}

func testsEquivalent(desired declarative.TestSpec, actual apiModelTest) bool {
	if desired.Name != actual.Name || desired.Type != actual.TestType || desired.Column != actual.Column {
		return false
	}

	var actualValues []string
	actualToModel := ""
	actualToColumn := ""
	actualSQL := ""
	if actual.Config != nil {
		actualValues = append(actualValues, actual.Config.Values...)
		actualToModel = actual.Config.ToModel
		actualToColumn = actual.Config.ToColumn
		actualSQL = actual.Config.CustomSQL
	}

	if strings.Join(desired.Values, "\x00") != strings.Join(actualValues, "\x00") {
		return false
	}
	if desired.ToModel != actualToModel || desired.ToColumn != actualToColumn || desired.SQL != actualSQL {
		return false
	}

	return true
}

func (c *APIStateClient) reconcileModelTests(ctx context.Context, projectName, modelName string, desired []declarative.TestSpec) error {
	actual, supported, err := c.listModelTests(ctx, projectName, modelName)
	if err != nil {
		return err
	}
	if !supported {
		return nil
	}

	actualByName := make(map[string]apiModelTest, len(actual))
	for _, test := range actual {
		actualByName[test.Name] = test
	}

	seen := make(map[string]struct{}, len(desired))
	for _, wanted := range desired {
		seen[wanted.Name] = struct{}{}
		current, exists := actualByName[wanted.Name]
		if exists {
			if testsEquivalent(wanted, current) {
				continue
			}
			if current.ID == "" {
				return fmt.Errorf("model test %q missing id for replace", wanted.Name)
			}
			resp, err := c.client.Do(http.MethodDelete, "/models/"+projectName+"/"+modelName+"/tests/"+current.ID, nil, nil)
			if err != nil {
				return err
			}
			if err := apiruntime.CheckError(resp); err != nil {
				return err
			}
		}

		resp, err := c.client.Do(http.MethodPost, "/models/"+projectName+"/"+modelName+"/tests", nil, toModelTestBody(wanted))
		if err != nil {
			return err
		}
		if err := apiruntime.CheckError(resp); err != nil {
			return err
		}
	}

	for name, current := range actualByName {
		if _, ok := seen[name]; ok {
			continue
		}
		if current.ID == "" {
			return fmt.Errorf("model test %q missing id for delete", current.Name)
		}
		resp, err := c.client.Do(http.MethodDelete, "/models/"+projectName+"/"+modelName+"/tests/"+current.ID, nil, nil)
		if err != nil {
			return err
		}
		if err := apiruntime.CheckError(resp); err != nil {
			return err
		}
	}

	return nil
}

func (c *APIStateClient) executeMacro(_ context.Context, action declarative.Action) error {
	switch action.Operation {
	case declarative.OpCreate:
		macro := action.Desired.(declarative.MacroResource)
		body := map[string]interface{}{
			"name": macro.Name,
			"body": macro.Spec.Body,
		}
		if macro.Spec.MacroType != "" {
			body["macro_type"] = macro.Spec.MacroType
		}
		if len(macro.Spec.Parameters) > 0 {
			body["parameters"] = macro.Spec.Parameters
		}
		if macro.Spec.Description != "" {
			body["description"] = macro.Spec.Description
		}
		if macro.Spec.CatalogName != "" {
			body["catalog_name"] = macro.Spec.CatalogName
		}
		if macro.Spec.ProjectName != "" {
			body["project_name"] = macro.Spec.ProjectName
		}
		if macro.Spec.Visibility != "" {
			body["visibility"] = macro.Spec.Visibility
		}
		if macro.Spec.Owner != "" {
			body["owner"] = macro.Spec.Owner
		}
		if len(macro.Spec.Properties) > 0 {
			body["properties"] = macro.Spec.Properties
		}
		if len(macro.Spec.Tags) > 0 {
			body["tags"] = macro.Spec.Tags
		}
		if macro.Spec.Status != "" {
			body["status"] = macro.Spec.Status
		}

		resp, err := c.client.Do(http.MethodPost, "/macros", nil, body)
		if err != nil {
			return err
		}
		return apiruntime.CheckError(resp)

	case declarative.OpUpdate:
		macro := action.Desired.(declarative.MacroResource)
		body := map[string]interface{}{}
		if macro.Spec.Body != "" {
			body["body"] = macro.Spec.Body
		}
		body["parameters"] = macro.Spec.Parameters
		body["description"] = macro.Spec.Description
		if macro.Spec.CatalogName != "" {
			body["catalog_name"] = macro.Spec.CatalogName
		}
		if macro.Spec.ProjectName != "" {
			body["project_name"] = macro.Spec.ProjectName
		}
		if macro.Spec.Visibility != "" {
			body["visibility"] = macro.Spec.Visibility
		}
		if macro.Spec.Owner != "" {
			body["owner"] = macro.Spec.Owner
		}
		body["properties"] = macro.Spec.Properties
		body["tags"] = macro.Spec.Tags
		if macro.Spec.Status != "" {
			body["status"] = macro.Spec.Status
		}

		resp, err := c.client.Do(http.MethodPatch, "/macros/"+macro.Name, nil, body)
		if err != nil {
			return err
		}
		return apiruntime.CheckError(resp)

	case declarative.OpDelete:
		resp, err := c.client.Do(http.MethodDelete, "/macros/"+action.ResourceName, nil, nil)
		if err != nil {
			return err
		}
		return apiruntime.CheckError(resp)

	default:
		return fmt.Errorf("unsupported operation %s for macro", action.Operation)
	}
}

func (c *APIStateClient) executeModel(ctx context.Context, action declarative.Action) error {
	switch action.Operation {
	case declarative.OpCreate:
		model := action.Desired.(declarative.ModelResource)
		body := map[string]interface{}{
			"project_name": model.ProjectName,
			"name":         model.ModelName,
			"sql":          model.Spec.SQL,
		}
		if model.Spec.Materialization != "" {
			body["materialization"] = model.Spec.Materialization
		}
		if model.Spec.Description != "" {
			body["description"] = model.Spec.Description
		}
		if len(model.Spec.Tags) > 0 {
			body["tags"] = model.Spec.Tags
		}
		if config := modelConfigBody(model.Spec.Config); config != nil {
			body["config"] = config
		}
		if contract := modelContractBody(model.Spec.Contract); contract != nil {
			body["contract"] = contract
		}
		if freshness := modelFreshnessBody(model.Spec.Freshness); freshness != nil {
			body["freshness_policy"] = freshness
		}

		resp, err := c.client.Do(http.MethodPost, "/models", nil, body)
		if err != nil {
			return err
		}
		if err := apiruntime.CheckError(resp); err != nil {
			return err
		}
		return c.reconcileModelTests(ctx, model.ProjectName, model.ModelName, model.Spec.Tests)

	case declarative.OpUpdate:
		model := action.Desired.(declarative.ModelResource)
		body := map[string]interface{}{
			"sql": model.Spec.SQL,
		}
		if model.Spec.Materialization != "" {
			body["materialization"] = model.Spec.Materialization
		}
		body["description"] = model.Spec.Description
		body["tags"] = model.Spec.Tags
		body["config"] = modelConfigBody(model.Spec.Config)
		body["contract"] = modelContractBody(model.Spec.Contract)
		body["freshness_policy"] = modelFreshnessBody(model.Spec.Freshness)

		path := "/models/" + model.ProjectName + "/" + model.ModelName
		resp, err := c.client.Do(http.MethodPatch, path, nil, body)
		if err != nil {
			return err
		}
		if err := apiruntime.CheckError(resp); err != nil {
			return err
		}
		return c.reconcileModelTests(ctx, model.ProjectName, model.ModelName, model.Spec.Tests)

	case declarative.OpDelete:
		parts := strings.SplitN(action.ResourceName, ".", 2)
		if len(parts) != 2 {
			return fmt.Errorf("invalid model resource name: %s", action.ResourceName)
		}
		resp, err := c.client.Do(http.MethodDelete, "/models/"+parts[0]+"/"+parts[1], nil, nil)
		if err != nil {
			return err
		}
		return apiruntime.CheckError(resp)

	default:
		return fmt.Errorf("unsupported operation %s for model", action.Operation)
	}
}

func (c *APIStateClient) executeStorageCredential(_ context.Context, action declarative.Action) error {
	switch action.Operation {
	case declarative.OpCreate:
		spec := action.Desired.(declarative.StorageCredentialSpec)
		body := map[string]interface{}{
			"name":            spec.Name,
			"credential_type": spec.CredentialType,
		}
		if spec.Comment != "" {
			body["comment"] = spec.Comment
		}
		resp, err := c.client.Do(http.MethodPost, "/storage-credentials", nil, body)
		if err != nil {
			return err
		}
		if resp.StatusCode == http.StatusConflict {
			return nil
		}
		id, err := c.checkCreateResponse(resp)
		if err != nil {
			return err
		}
		if id != "" && c.index != nil {
			c.index.credentialIDByName[spec.Name] = id
		}
		return nil
	case declarative.OpUpdate:
		spec := action.Desired.(declarative.StorageCredentialSpec)
		body := map[string]interface{}{
			"comment": spec.Comment,
		}
		resp, err := c.client.Do(http.MethodPatch, "/storage-credentials/"+spec.Name, nil, body)
		if err != nil {
			return err
		}
		return apiruntime.CheckError(resp)
	case declarative.OpDelete:
		name := action.ResourceName
		if actual, ok := action.Actual.(declarative.StorageCredentialSpec); ok && actual.Name != "" {
			name = actual.Name
		}
		resp, err := c.client.Do(http.MethodDelete, "/storage-credentials/"+name, nil, nil)
		if err != nil {
			return err
		}
		return apiruntime.CheckError(resp)
	default:
		return fmt.Errorf("unsupported operation %s for storage-credential", action.Operation)
	}
}

func (c *APIStateClient) executeExternalLocation(_ context.Context, action declarative.Action) error {
	switch action.Operation {
	case declarative.OpCreate:
		spec := action.Desired.(declarative.ExternalLocationSpec)
		body := map[string]interface{}{
			"name":            spec.Name,
			"url":             spec.URL,
			"credential_name": spec.CredentialName,
		}
		if spec.StorageType != "" {
			body["storage_type"] = spec.StorageType
		}
		if spec.Comment != "" {
			body["comment"] = spec.Comment
		}
		body["read_only"] = spec.ReadOnly
		resp, err := c.client.Do(http.MethodPost, "/external-locations", nil, body)
		if err != nil {
			return err
		}
		if resp.StatusCode == http.StatusConflict {
			return nil
		}
		id, err := c.checkCreateResponse(resp)
		if err != nil {
			return err
		}
		if id != "" && c.index != nil {
			c.index.locationIDByName[spec.Name] = id
		}
		return nil
	case declarative.OpUpdate:
		spec := action.Desired.(declarative.ExternalLocationSpec)
		body := map[string]interface{}{
			"url":             spec.URL,
			"credential_name": spec.CredentialName,
			"storage_type":    spec.StorageType,
			"comment":         spec.Comment,
			"read_only":       spec.ReadOnly,
		}
		resp, err := c.client.Do(http.MethodPatch, "/external-locations/"+spec.Name, nil, body)
		if err != nil {
			return err
		}
		return apiruntime.CheckError(resp)
	case declarative.OpDelete:
		name := action.ResourceName
		if actual, ok := action.Actual.(declarative.ExternalLocationSpec); ok && actual.Name != "" {
			name = actual.Name
		}
		resp, err := c.client.Do(http.MethodDelete, "/external-locations/"+name, nil, nil)
		if err != nil {
			return err
		}
		return apiruntime.CheckError(resp)
	default:
		return fmt.Errorf("unsupported operation %s for external-location", action.Operation)
	}
}

func (c *APIStateClient) executeComputeEndpoint(_ context.Context, action declarative.Action) error {
	switch action.Operation {
	case declarative.OpCreate:
		spec := action.Desired.(declarative.ComputeEndpointSpec)
		body := map[string]interface{}{
			"name": spec.Name,
			"type": spec.Type,
		}
		if spec.URL != "" {
			body["url"] = spec.URL
		}
		if spec.Size != "" {
			body["size"] = spec.Size
		}
		if spec.SelectionPolicy != "" {
			body["selection_policy"] = spec.SelectionPolicy
		}
		if spec.WorkloadClass != "" {
			body["workload_class"] = spec.WorkloadClass
		}
		if spec.ReadinessStatus != "" {
			body["readiness_status"] = spec.ReadinessStatus
		}
		if spec.MaxMemoryGB != nil {
			body["max_memory_gb"] = *spec.MaxMemoryGB
		}
		if spec.MaxConcurrency != nil {
			body["max_concurrency"] = *spec.MaxConcurrency
		}
		if spec.MaxResultSizeMB != nil {
			body["max_result_size_mb"] = *spec.MaxResultSizeMB
		}
		if spec.RecommendedForLargeQueries {
			body["recommended_for_large_queries"] = spec.RecommendedForLargeQueries
		}
		if spec.IsDraining {
			body["is_draining"] = spec.IsDraining
		}
		resp, err := c.client.Do(http.MethodPost, "/compute-endpoints", nil, body)
		if err != nil {
			return err
		}
		if resp.StatusCode == http.StatusConflict {
			return nil
		}
		id, err := c.checkCreateResponse(resp)
		if err != nil {
			return err
		}
		if id != "" && c.index != nil {
			c.index.computeIDByName[spec.Name] = id
		}
		return nil
	case declarative.OpUpdate:
		spec := action.Desired.(declarative.ComputeEndpointSpec)
		body := map[string]interface{}{
			"url":  spec.URL,
			"size": spec.Size,
		}
		if spec.SelectionPolicy != "" {
			body["selection_policy"] = spec.SelectionPolicy
		}
		if spec.WorkloadClass != "" {
			body["workload_class"] = spec.WorkloadClass
		}
		if spec.ReadinessStatus != "" {
			body["readiness_status"] = spec.ReadinessStatus
		}
		if spec.MaxMemoryGB != nil {
			body["max_memory_gb"] = *spec.MaxMemoryGB
		}
		if spec.MaxConcurrency != nil {
			body["max_concurrency"] = *spec.MaxConcurrency
		}
		if spec.MaxResultSizeMB != nil {
			body["max_result_size_mb"] = *spec.MaxResultSizeMB
		}
		body["recommended_for_large_queries"] = spec.RecommendedForLargeQueries
		body["is_draining"] = spec.IsDraining
		resp, err := c.client.Do(http.MethodPatch, "/compute-endpoints/"+spec.Name, nil, body)
		if err != nil {
			return err
		}
		return apiruntime.CheckError(resp)
	case declarative.OpDelete:
		name := action.ResourceName
		if actual, ok := action.Actual.(declarative.ComputeEndpointSpec); ok && actual.Name != "" {
			name = actual.Name
		}
		resp, err := c.client.Do(http.MethodDelete, "/compute-endpoints/"+name, nil, nil)
		if err != nil {
			return err
		}
		return apiruntime.CheckError(resp)
	default:
		return fmt.Errorf("unsupported operation %s for compute-endpoint", action.Operation)
	}
}

func (c *APIStateClient) executeComputeRoutingDefaults(_ context.Context, action declarative.Action) error {
	switch action.Operation {
	case declarative.OpCreate, declarative.OpUpdate:
		spec := action.Desired.(declarative.ComputeRoutingDefaultsSpec)
		body := map[string]interface{}{}
		if spec.InteractiveMode != "" {
			body["interactive_mode"] = spec.InteractiveMode
		}
		if spec.ScheduledMode != "" {
			body["scheduled_mode"] = spec.ScheduledMode
		}
		if spec.NotebookMode != "" {
			body["notebook_mode"] = spec.NotebookMode
		}
		resp, err := c.client.Do(http.MethodPatch, "/compute-defaults", nil, body)
		if err != nil {
			return err
		}
		return apiruntime.CheckError(resp)
	case declarative.OpDelete:
		resp, err := c.client.Do(http.MethodPatch, "/compute-defaults", nil, map[string]interface{}{
			"interactive_mode": "BYOC_LOCAL",
			"scheduled_mode":   "SHARED_ENDPOINT",
			"notebook_mode":    "SHARED_ENDPOINT",
		})
		if err != nil {
			return err
		}
		return apiruntime.CheckError(resp)
	default:
		return fmt.Errorf("unsupported operation %s for compute-routing-defaults", action.Operation)
	}
}

func (c *APIStateClient) executeComputeAssignment(_ context.Context, action declarative.Action) error {
	switch action.Operation {
	case declarative.OpCreate:
		spec := action.Desired.(declarative.ComputeAssignmentSpec)
		principalID, err := c.resolvePrincipalID(spec.Principal, spec.PrincipalType)
		if err != nil {
			return fmt.Errorf("resolve principal for compute assignment: %w", err)
		}
		body := map[string]interface{}{
			"principal_id":   principalID,
			"principal_type": spec.PrincipalType,
			"is_default":     spec.IsDefault,
			"fallback_local": spec.FallbackLocal,
		}
		resp, err := c.client.Do(http.MethodPost, "/compute-endpoints/"+spec.Endpoint+"/assignments", nil, body)
		if err != nil {
			return err
		}
		if resp.StatusCode == http.StatusConflict {
			return nil
		}
		id, err := c.checkCreateResponse(resp)
		if err != nil {
			return err
		}
		if id != "" && c.index != nil {
			c.index.computeAssignIDByKey[computeAssignmentKey(spec.Endpoint, spec.PrincipalType, spec.Principal)] = id
		}
		return nil
	case declarative.OpUpdate:
		spec := action.Desired.(declarative.ComputeAssignmentSpec)
		actual := action.Actual.(declarative.ComputeAssignmentSpec)
		deleteAction := declarative.Action{Operation: declarative.OpDelete, Actual: actual, ResourceName: action.ResourceName}
		if err := c.executeComputeAssignment(context.TODO(), deleteAction); err != nil {
			return err
		}
		createAction := declarative.Action{Operation: declarative.OpCreate, Desired: spec, ResourceName: action.ResourceName}
		return c.executeComputeAssignment(context.TODO(), createAction)
	case declarative.OpDelete:
		spec := action.Actual.(declarative.ComputeAssignmentSpec)
		assignmentID := spec.AssignmentID
		if assignmentID == "" && c.index != nil {
			assignmentID = c.index.computeAssignIDByKey[computeAssignmentKey(spec.Endpoint, spec.PrincipalType, spec.Principal)]
		}
		if assignmentID == "" {
			return fmt.Errorf("compute assignment %s has no assignment id", action.ResourceName)
		}
		resp, err := c.client.Do(http.MethodDelete, "/compute-endpoints/"+spec.Endpoint+"/assignments/"+assignmentID, nil, nil)
		if err != nil {
			return err
		}
		return apiruntime.CheckError(resp)
	default:
		return fmt.Errorf("unsupported operation %s for compute-assignment", action.Operation)
	}
}

func (c *APIStateClient) executeVolume(_ context.Context, action declarative.Action) error {
	switch action.Operation {
	case declarative.OpCreate:
		volume := action.Desired.(declarative.VolumeResource)
		body := map[string]interface{}{
			"name": volume.VolumeName,
		}
		if volume.Spec.VolumeType != "" {
			body["volume_type"] = volume.Spec.VolumeType
		}
		if volume.Spec.StorageLocation != "" {
			body["storage_location"] = volume.Spec.StorageLocation
		}
		if volume.Spec.Comment != "" {
			body["comment"] = volume.Spec.Comment
		}
		resp, err := c.client.Do(http.MethodPost, "/catalogs/"+volume.CatalogName+"/schemas/"+volume.SchemaName+"/volumes", nil, body)
		if err != nil {
			return err
		}
		if resp.StatusCode == http.StatusConflict {
			return nil
		}
		id, err := c.checkCreateResponse(resp)
		if err != nil {
			return err
		}
		if id != "" && c.index != nil {
			c.index.volumeIDByPath[volume.CatalogName+"."+volume.SchemaName+"."+volume.VolumeName] = id
		}
		return nil
	case declarative.OpUpdate:
		volume := action.Desired.(declarative.VolumeResource)
		body := map[string]interface{}{
			"comment": volume.Spec.Comment,
		}
		resp, err := c.client.Do(http.MethodPatch, "/catalogs/"+volume.CatalogName+"/schemas/"+volume.SchemaName+"/volumes/"+volume.VolumeName, nil, body)
		if err != nil {
			return err
		}
		return apiruntime.CheckError(resp)
	case declarative.OpDelete:
		parts := strings.SplitN(action.ResourceName, ".", 3)
		if len(parts) != 3 {
			return fmt.Errorf("invalid volume resource name: %s", action.ResourceName)
		}
		resp, err := c.client.Do(http.MethodDelete, "/catalogs/"+parts[0]+"/schemas/"+parts[1]+"/volumes/"+parts[2], nil, nil)
		if err != nil {
			return err
		}
		return apiruntime.CheckError(resp)
	default:
		return fmt.Errorf("unsupported operation %s for volume", action.Operation)
	}
}

// --- Security resource execution ---

func (c *APIStateClient) executePrincipal(_ context.Context, action declarative.Action) error {
	switch action.Operation {
	case declarative.OpCreate:
		spec := action.Desired.(declarative.PrincipalSpec)
		body := map[string]interface{}{
			"name":     spec.Name,
			"type":     spec.Type,
			"is_admin": spec.IsAdmin,
		}
		resp, err := c.client.Do(http.MethodPost, "/principals", nil, body)
		if err != nil {
			return err
		}
		id, err := c.checkCreateResponse(resp)
		if err != nil {
			return err
		}
		if id != "" && c.index != nil {
			c.index.principalIDByName[spec.Name] = id
		}
		return nil

	case declarative.OpUpdate:
		spec := action.Desired.(declarative.PrincipalSpec)
		id, err := c.resolvePrincipalID(spec.Name, spec.Type)
		if err != nil {
			return fmt.Errorf("resolve principal for update: %w", err)
		}
		body := map[string]interface{}{
			"is_admin": spec.IsAdmin,
		}
		resp, err := c.client.Do(http.MethodPut, "/principals/"+id+"/admin", nil, body)
		if err != nil {
			return err
		}
		return apiruntime.CheckError(resp)

	case declarative.OpDelete:
		spec := action.Actual.(declarative.PrincipalSpec)
		id, err := c.resolvePrincipalID(spec.Name, spec.Type)
		if err != nil {
			return fmt.Errorf("resolve principal for delete: %w", err)
		}
		resp, err := c.client.Do(http.MethodDelete, "/principals/"+id, nil, nil)
		if err != nil {
			return err
		}
		return apiruntime.CheckError(resp)

	default:
		return fmt.Errorf("unsupported operation %s for principal", action.Operation)
	}
}

func (c *APIStateClient) executeGroup(_ context.Context, action declarative.Action) error {
	switch action.Operation {
	case declarative.OpCreate:
		spec := action.Desired.(declarative.GroupSpec)
		body := map[string]interface{}{
			"name": spec.Name,
		}
		if spec.Description != "" {
			body["description"] = spec.Description
		}
		resp, err := c.client.Do(http.MethodPost, "/groups", nil, body)
		if err != nil {
			return err
		}
		id, err := c.checkCreateResponse(resp)
		if err != nil {
			return err
		}
		if id != "" && c.index != nil {
			c.index.groupIDByName[spec.Name] = id
		}
		return nil

	case declarative.OpUpdate:
		resp, err := c.client.Do(http.MethodPatch, "/groups/"+action.ResourceName, nil, action.Desired)
		if err != nil {
			return err
		}
		return apiruntime.CheckError(resp)

	case declarative.OpDelete:
		groupName := action.ResourceName
		if actual, ok := action.Actual.(declarative.GroupSpec); ok && actual.Name != "" {
			groupName = actual.Name
		}
		groupID, err := c.resolvePrincipalID(groupName, "group")
		if err != nil {
			return fmt.Errorf("resolve group for delete: %w", err)
		}
		resp, err := c.client.Do(http.MethodDelete, "/groups/"+groupID, nil, nil)
		if err != nil {
			return err
		}
		return apiruntime.CheckError(resp)

	default:
		return fmt.Errorf("unsupported operation %s for group", action.Operation)
	}
}

func (c *APIStateClient) executeGrant(ctx context.Context, action declarative.Action) error {
	switch action.Operation {
	case declarative.OpCreate:
		grant := action.Desired.(declarative.GrantSpec)
		principalID, err := c.resolvePrincipalID(grant.Principal, grant.PrincipalType)
		if err != nil {
			return fmt.Errorf("resolve principal for grant: %w", err)
		}
		securableID, err := c.resolveSecurableID(ctx, grant.SecurableType, grant.Securable)
		if err != nil {
			return fmt.Errorf("resolve securable for grant: %w", err)
		}
		body := map[string]interface{}{
			"principal_id":   principalID,
			"principal_type": grant.PrincipalType,
			"securable_id":   securableID,
			"securable_type": grant.SecurableType,
			"privilege":      grant.Privilege,
		}
		resp, err := c.client.Do(http.MethodPost, "/grants", nil, body)
		if err != nil {
			return err
		}
		return apiruntime.CheckError(resp)

	case declarative.OpDelete:
		grant := action.Actual.(declarative.GrantSpec)
		grantID, err := c.resolveGrantID(ctx, grant)
		if err != nil {
			return fmt.Errorf("resolve grant for delete: %w", err)
		}
		resp, err := c.client.Do(http.MethodDelete, "/grants/"+grantID, nil, nil)
		if err != nil {
			return err
		}
		return apiruntime.CheckError(resp)

	default:
		// Grants are typically immutable — recreate via delete+create.
		return fmt.Errorf("unsupported operation %s for grant (grants are immutable, delete and recreate)", action.Operation)
	}
}

// --- Catalog resource execution ---

func (c *APIStateClient) executeCatalog(_ context.Context, action declarative.Action) error {
	switch action.Operation {
	case declarative.OpCreate:
		cat := action.Desired.(declarative.CatalogResource)
		body := map[string]interface{}{
			"name":           cat.CatalogName,
			"metastore_type": cat.Spec.MetastoreType,
			"dsn":            cat.Spec.DSN,
			"data_path":      cat.Spec.DataPath,
		}
		if cat.Spec.IsDefault {
			body["is_default"] = true
		}
		if cat.Spec.Comment != "" {
			body["comment"] = cat.Spec.Comment
		}
		resp, err := c.client.Do(http.MethodPost, "/catalogs", nil, body)
		if err != nil {
			return err
		}
		id, err := c.checkCreateResponse(resp)
		if err != nil {
			return err
		}
		if id != "" && c.index != nil {
			c.index.catalogIDByName[cat.CatalogName] = id
		}
		return nil

	case declarative.OpUpdate:
		cat := action.Desired.(declarative.CatalogResource)
		body := map[string]interface{}{
			"metastore_type": cat.Spec.MetastoreType,
			"dsn":            cat.Spec.DSN,
			"data_path":      cat.Spec.DataPath,
			"is_default":     cat.Spec.IsDefault,
		}
		if cat.Spec.Comment != "" {
			body["comment"] = cat.Spec.Comment
		}
		resp, err := c.client.Do(http.MethodPatch, "/catalogs/"+action.ResourceName, nil, body)
		if err != nil {
			return err
		}
		return apiruntime.CheckError(resp)

	case declarative.OpDelete:
		resp, err := c.client.Do(http.MethodDelete, "/catalogs/"+action.ResourceName, nil, nil)
		if err != nil {
			return err
		}
		return apiruntime.CheckError(resp)

	default:
		return fmt.Errorf("unsupported operation %s for catalog", action.Operation)
	}
}

func (c *APIStateClient) executeSchema(ctx context.Context, action declarative.Action) error {
	// ResourceName is "catalog.schema" format.
	switch action.Operation {
	case declarative.OpCreate:
		schema := action.Desired.(declarative.SchemaResource)
		body := map[string]interface{}{
			"name": schema.SchemaName,
		}
		if schema.Spec.Comment != "" {
			body["comment"] = schema.Spec.Comment
		}
		if schema.Spec.LocationName != "" {
			body["location_name"] = schema.Spec.LocationName
		}
		if len(schema.Spec.Properties) > 0 {
			body["properties"] = schema.Spec.Properties
		}
		resp, err := c.client.Do(http.MethodPost, "/catalogs/"+schema.CatalogName+"/schemas", nil, body)
		if err != nil {
			return err
		}
		if resp.StatusCode == http.StatusConflict {
			id, lookupErr := c.lookupSchemaIDByPath(ctx, schema.CatalogName, schema.SchemaName)
			if lookupErr != nil {
				return fmt.Errorf("schema already exists and lookup failed: %w", lookupErr)
			}
			if c.index != nil {
				c.index.schemaIDByPath[schema.CatalogName+"."+schema.SchemaName] = id
			}
			return nil
		}
		id, err := c.checkCreateResponse(resp)
		if err != nil {
			return err
		}
		if id != "" && c.index != nil {
			c.index.schemaIDByPath[schema.CatalogName+"."+schema.SchemaName] = id
		}
		return nil

	case declarative.OpUpdate:
		schema := action.Desired.(declarative.SchemaResource)
		body := map[string]interface{}{}
		if schema.Spec.Comment != "" {
			body["comment"] = schema.Spec.Comment
		}
		if len(schema.Spec.Properties) > 0 {
			body["properties"] = schema.Spec.Properties
		}
		resp, err := c.client.Do(http.MethodPatch, "/catalogs/"+schema.CatalogName+"/schemas/"+schema.SchemaName, nil, body)
		if err != nil {
			return err
		}
		return apiruntime.CheckError(resp)

	case declarative.OpDelete:
		parts := strings.SplitN(action.ResourceName, ".", 2)
		if len(parts) != 2 {
			return fmt.Errorf("invalid schema resource name: %s", action.ResourceName)
		}
		resp, err := c.client.Do(http.MethodDelete, "/catalogs/"+parts[0]+"/schemas/"+parts[1], nil, nil)
		if err != nil {
			return err
		}
		return apiruntime.CheckError(resp)

	default:
		return fmt.Errorf("unsupported operation %s for schema", action.Operation)
	}
}

func (c *APIStateClient) executeTable(ctx context.Context, action declarative.Action) error {
	// ResourceName is "catalog.schema.table" format.
	switch action.Operation {
	case declarative.OpCreate:
		tbl := action.Desired.(declarative.TableResource)
		body := map[string]interface{}{
			"name": tbl.TableName,
		}
		if tbl.Spec.TableType != "" {
			body["table_type"] = tbl.Spec.TableType
		}
		if tbl.Spec.Comment != "" {
			body["comment"] = tbl.Spec.Comment
		}
		if len(tbl.Spec.Columns) > 0 {
			cols := make([]map[string]interface{}, len(tbl.Spec.Columns))
			for i, col := range tbl.Spec.Columns {
				c := map[string]interface{}{
					"name": col.Name,
					"type": col.Type,
				}
				if col.Comment != "" {
					c["comment"] = col.Comment
				}
				cols[i] = c
			}
			body["columns"] = cols
		}
		if tbl.Spec.SourcePath != "" {
			body["source_path"] = tbl.Spec.SourcePath
		}
		if tbl.Spec.FileFormat != "" {
			body["file_format"] = tbl.Spec.FileFormat
		}
		if tbl.Spec.LocationName != "" {
			body["location_name"] = tbl.Spec.LocationName
		}
		basePath := "/catalogs/" + tbl.CatalogName + "/schemas/" + tbl.SchemaName + "/tables"
		resp, err := c.client.Do(http.MethodPost, basePath, nil, body)
		if err != nil {
			return err
		}
		if resp.StatusCode == http.StatusConflict {
			id, lookupErr := c.lookupTableIDByPath(ctx, tbl.CatalogName, tbl.SchemaName, tbl.TableName)
			if lookupErr != nil {
				return fmt.Errorf("table already exists and lookup failed: %w", lookupErr)
			}
			if c.index != nil {
				c.index.tableIDByPath[tbl.CatalogName+"."+tbl.SchemaName+"."+tbl.TableName] = id
			}
			return nil
		}
		id, err := c.checkCreateResponse(resp)
		if err != nil {
			return err
		}
		if id != "" && c.index != nil {
			c.index.tableIDByPath[tbl.CatalogName+"."+tbl.SchemaName+"."+tbl.TableName] = id
		}
		return nil

	case declarative.OpUpdate:
		tbl := action.Desired.(declarative.TableResource)
		body := map[string]interface{}{}
		if tbl.Spec.Comment != "" {
			body["comment"] = tbl.Spec.Comment
		}
		basePath := "/catalogs/" + tbl.CatalogName + "/schemas/" + tbl.SchemaName + "/tables/" + tbl.TableName
		resp, err := c.client.Do(http.MethodPatch, basePath, nil, body)
		if err != nil {
			return err
		}
		return apiruntime.CheckError(resp)

	case declarative.OpDelete:
		parts := strings.SplitN(action.ResourceName, ".", 3)
		if len(parts) != 3 {
			return fmt.Errorf("invalid table resource name: %s", action.ResourceName)
		}
		basePath := "/catalogs/" + parts[0] + "/schemas/" + parts[1] + "/tables/" + parts[2]
		resp, err := c.client.Do(http.MethodDelete, basePath, nil, nil)
		if err != nil {
			return err
		}
		return apiruntime.CheckError(resp)

	default:
		return fmt.Errorf("unsupported operation %s for table", action.Operation)
	}
}

func (c *APIStateClient) executeView(_ context.Context, action declarative.Action) error {
	// ResourceName is "catalog.schema.view" format.
	switch action.Operation {
	case declarative.OpCreate:
		vw := action.Desired.(declarative.ViewResource)
		body := map[string]interface{}{
			"name":            vw.ViewName,
			"view_definition": vw.Spec.ViewDefinition,
		}
		if vw.Spec.Comment != "" {
			body["comment"] = vw.Spec.Comment
		}
		if len(vw.Spec.Properties) > 0 {
			body["properties"] = vw.Spec.Properties
		}
		basePath := "/catalogs/" + vw.CatalogName + "/schemas/" + vw.SchemaName + "/views"
		resp, err := c.client.Do(http.MethodPost, basePath, nil, body)
		if err != nil {
			return err
		}
		return apiruntime.CheckError(resp)

	case declarative.OpUpdate:
		vw := action.Desired.(declarative.ViewResource)
		body := map[string]interface{}{}
		if vw.Spec.ViewDefinition != "" {
			body["view_definition"] = vw.Spec.ViewDefinition
		}
		if vw.Spec.Comment != "" {
			body["comment"] = vw.Spec.Comment
		}
		if len(vw.Spec.Properties) > 0 {
			body["properties"] = vw.Spec.Properties
		}
		basePath := "/catalogs/" + vw.CatalogName + "/schemas/" + vw.SchemaName + "/views/" + vw.ViewName
		resp, err := c.client.Do(http.MethodPatch, basePath, nil, body)
		if err != nil {
			return err
		}
		return apiruntime.CheckError(resp)

	case declarative.OpDelete:
		parts := strings.SplitN(action.ResourceName, ".", 3)
		if len(parts) != 3 {
			return fmt.Errorf("invalid view resource name: %s", action.ResourceName)
		}
		basePath := "/catalogs/" + parts[0] + "/schemas/" + parts[1] + "/views/" + parts[2]
		resp, err := c.client.Do(http.MethodDelete, basePath, nil, nil)
		if err != nil {
			return err
		}
		return apiruntime.CheckError(resp)

	default:
		return fmt.Errorf("unsupported operation %s for view", action.Operation)
	}
}

// --- Group membership execution ---

func (c *APIStateClient) executeGroupMembership(_ context.Context, action declarative.Action) error {
	// ResourceName format: "groupName/memberName(memberType)"
	slashIdx := strings.Index(action.ResourceName, "/")
	if slashIdx < 0 {
		return fmt.Errorf("invalid group membership resource name: %s", action.ResourceName)
	}
	groupName := action.ResourceName[:slashIdx]
	groupID, err := c.resolvePrincipalID(groupName, "group")
	if err != nil {
		return fmt.Errorf("resolve group for membership: %w", err)
	}

	switch action.Operation {
	case declarative.OpCreate:
		member := action.Desired.(declarative.MemberRef)
		memberID, err := c.resolvePrincipalID(member.Name, member.Type)
		if err != nil {
			return fmt.Errorf("resolve member for group membership: %w", err)
		}
		body := map[string]interface{}{
			"member_id":   memberID,
			"member_type": member.Type,
		}
		resp, err := c.client.Do(http.MethodPost, "/groups/"+groupID+"/members", nil, body)
		if err != nil {
			return err
		}
		return apiruntime.CheckError(resp)

	case declarative.OpDelete:
		member := action.Actual.(declarative.MemberRef)
		var memberID string
		switch {
		case member.MemberID != "":
			memberID = member.MemberID
		case member.Name != "":
			resolved, err := c.resolvePrincipalID(member.Name, member.Type)
			if err != nil {
				return fmt.Errorf("resolve member for group membership delete: %w", err)
			}
			memberID = resolved
		default:
			return fmt.Errorf("cannot delete group membership: member has neither ID nor name")
		}
		q := url.Values{}
		q.Set("member_id", memberID)
		q.Set("member_type", member.Type)
		resp, err := c.client.Do(http.MethodDelete, "/groups/"+groupID+"/members", q, nil)
		if err != nil {
			return err
		}
		return apiruntime.CheckError(resp)

	default:
		return fmt.Errorf("unsupported operation %s for group-membership", action.Operation)
	}
}

// --- Tag execution ---

func (c *APIStateClient) executeTag(_ context.Context, action declarative.Action) error {
	switch action.Operation {
	case declarative.OpCreate:
		tag := action.Desired.(declarative.TagSpec)
		body := map[string]interface{}{
			"key": tag.Key,
		}
		if tag.Value != nil {
			body["value"] = *tag.Value
		}
		resp, err := c.client.Do(http.MethodPost, "/tags", nil, body)
		if err != nil {
			return err
		}
		id, err := c.checkCreateResponse(resp)
		if err != nil {
			return err
		}
		if id != "" && c.index != nil {
			c.index.tagIDByKey[tagKey(tag.Key, tag.Value)] = id
		}
		return nil

	case declarative.OpDelete:
		tagID, err := c.resolveTagID(action.ResourceName)
		if err != nil {
			return fmt.Errorf("resolve tag for delete: %w", err)
		}
		resp, err := c.client.Do(http.MethodDelete, "/tags/"+tagID, nil, nil)
		if err != nil {
			return err
		}
		return apiruntime.CheckError(resp)

	default:
		return fmt.Errorf("unsupported operation %s for tag", action.Operation)
	}
}

// --- Tag assignment execution ---

func (c *APIStateClient) executeTagAssignment(ctx context.Context, action declarative.Action) error {
	switch action.Operation {
	case declarative.OpCreate:
		assignment := action.Desired.(declarative.TagAssignmentSpec)
		tagID, err := c.resolveTagID(assignment.Tag)
		if err != nil {
			return fmt.Errorf("resolve tag for assignment: %w", err)
		}
		securableID, err := c.resolveSecurableID(ctx, assignment.SecurableType, assignment.Securable)
		if err != nil {
			return fmt.Errorf("resolve securable for tag assignment: %w", err)
		}
		body := map[string]interface{}{
			"securable_id":   securableID,
			"securable_type": assignment.SecurableType,
		}
		if assignment.ColumnName != "" {
			body["column_name"] = assignment.ColumnName
		}
		resp, err := c.client.Do(http.MethodPost, "/tags/"+tagID+"/assignments", nil, body)
		if err != nil {
			return err
		}
		return apiruntime.CheckError(resp)

	case declarative.OpDelete:
		// Tag assignment deletes require the assignment ID. Since we don't
		// track assignment IDs during ReadState, we use the tag ID and
		// attempt deletion via the composite endpoint.
		assignment := action.Actual.(declarative.TagAssignmentSpec)
		tagID, err := c.resolveTagID(assignment.Tag)
		if err != nil {
			return fmt.Errorf("resolve tag for assignment delete: %w", err)
		}
		securableID, err := c.resolveSecurableID(ctx, assignment.SecurableType, assignment.Securable)
		if err != nil {
			return fmt.Errorf("resolve securable for tag assignment delete: %w", err)
		}
		q := url.Values{}
		q.Set("tag_id", tagID)
		q.Set("securable_id", securableID)
		q.Set("securable_type", assignment.SecurableType)
		if assignment.ColumnName != "" {
			q.Set("column_name", assignment.ColumnName)
		}
		resp, err := c.client.Do(http.MethodDelete, "/tag-assignments", q, nil)
		if err != nil {
			return err
		}
		return apiruntime.CheckError(resp)

	default:
		return fmt.Errorf("unsupported operation %s for tag-assignment", action.Operation)
	}
}

// --- Row filter execution ---

func (c *APIStateClient) executeRowFilter(ctx context.Context, action declarative.Action) error {
	switch action.Operation {
	case declarative.OpCreate:
		filter := action.Desired.(declarative.RowFilterSpec)
		// ResourceName is "catalog.schema.table/filterName" — extract table path.
		parts := strings.SplitN(action.ResourceName, "/", 2)
		tablePath := parts[0]
		tableID, err := c.resolveSecurableID(ctx, "table", tablePath)
		if err != nil {
			return fmt.Errorf("resolve table for row filter: %w", err)
		}
		body := map[string]interface{}{
			"filter_sql": filter.FilterSQL,
		}
		if filter.Description != "" {
			body["description"] = filter.Description
		}
		resp, err := c.client.Do(http.MethodPost, "/tables/"+tableID+"/row-filters", nil, body)
		if err != nil {
			return err
		}
		id, err := c.checkCreateResponse(resp)
		if err != nil {
			return err
		}
		if id != "" && c.index != nil {
			c.index.rowFilterIDByPath[action.ResourceName] = id
		}
		return nil

	case declarative.OpDelete:
		filterID, err := c.resolveRowFilterID(action.ResourceName)
		if err != nil {
			return fmt.Errorf("resolve row filter for delete: %w", err)
		}
		resp, err := c.client.Do(http.MethodDelete, "/row-filters/"+filterID, nil, nil)
		if err != nil {
			return err
		}
		return apiruntime.CheckError(resp)

	default:
		return fmt.Errorf("unsupported operation %s for row-filter", action.Operation)
	}
}

// --- Row filter binding execution ---

func (c *APIStateClient) executeRowFilterBinding(_ context.Context, action declarative.Action) error {
	// ResourceName format: "catalog.schema.table/filterName->principalType:principalName"
	parts := strings.SplitN(action.ResourceName, "->", 2)
	filterPath := parts[0]
	filterID, err := c.resolveRowFilterID(filterPath)
	if err != nil {
		return fmt.Errorf("resolve row filter for binding: %w", err)
	}

	switch action.Operation {
	case declarative.OpCreate:
		binding := action.Desired.(declarative.FilterBindingRef)
		principalID, err := c.resolvePrincipalID(binding.Principal, binding.PrincipalType)
		if err != nil {
			return fmt.Errorf("resolve principal for row filter binding: %w", err)
		}
		body := map[string]interface{}{
			"principal_id":   principalID,
			"principal_type": binding.PrincipalType,
		}
		resp, err := c.client.Do(http.MethodPost, "/row-filters/"+filterID+"/bindings", nil, body)
		if err != nil {
			return err
		}
		return apiruntime.CheckError(resp)

	case declarative.OpDelete:
		binding := action.Actual.(declarative.FilterBindingRef)
		principalID, err := c.resolvePrincipalID(binding.Principal, binding.PrincipalType)
		if err != nil {
			return fmt.Errorf("resolve principal for row filter binding delete: %w", err)
		}
		q := url.Values{}
		q.Set("principal_id", principalID)
		q.Set("principal_type", binding.PrincipalType)
		resp, err := c.client.Do(http.MethodDelete, "/row-filters/"+filterID+"/bindings", q, nil)
		if err != nil {
			return err
		}
		return apiruntime.CheckError(resp)

	default:
		return fmt.Errorf("unsupported operation %s for row-filter-binding", action.Operation)
	}
}

// --- Column mask execution ---

func (c *APIStateClient) executeColumnMask(ctx context.Context, action declarative.Action) error {
	switch action.Operation {
	case declarative.OpCreate:
		mask := action.Desired.(declarative.ColumnMaskSpec)
		// ResourceName is "catalog.schema.table/maskName" — extract table path.
		parts := strings.SplitN(action.ResourceName, "/", 2)
		tablePath := parts[0]
		tableID, err := c.resolveSecurableID(ctx, "table", tablePath)
		if err != nil {
			return fmt.Errorf("resolve table for column mask: %w", err)
		}
		body := map[string]interface{}{
			"column_name":     mask.ColumnName,
			"mask_expression": mask.MaskExpression,
		}
		if mask.Description != "" {
			body["description"] = mask.Description
		}
		resp, err := c.client.Do(http.MethodPost, "/tables/"+tableID+"/column-masks", nil, body)
		if err != nil {
			return err
		}
		id, err := c.checkCreateResponse(resp)
		if err != nil {
			if strings.Contains(strings.ToLower(err.Error()), "resource already exists") {
				id, lookupErr := c.lookupColumnMaskIDBySpec(ctx, tablePath, mask)
				if lookupErr != nil {
					return fmt.Errorf("column mask already exists and lookup failed: %w", lookupErr)
				}
				if c.index != nil {
					c.index.columnMaskIDByPath[action.ResourceName] = id
				}
				return nil
			}
			return err
		}
		if id != "" && c.index != nil {
			c.index.columnMaskIDByPath[action.ResourceName] = id
		}
		return nil

	case declarative.OpDelete:
		maskID, err := c.resolveColumnMaskID(action.ResourceName)
		if err != nil {
			return fmt.Errorf("resolve column mask for delete: %w", err)
		}
		resp, err := c.client.Do(http.MethodDelete, "/column-masks/"+maskID, nil, nil)
		if err != nil {
			return err
		}
		return apiruntime.CheckError(resp)

	default:
		return fmt.Errorf("unsupported operation %s for column-mask", action.Operation)
	}
}

// --- Column mask binding execution ---

func (c *APIStateClient) executeColumnMaskBinding(_ context.Context, action declarative.Action) error {
	// ResourceName format: "catalog.schema.table/maskName->principalType:principalName"
	parts := strings.SplitN(action.ResourceName, "->", 2)
	maskPath := parts[0]
	maskID, err := c.resolveColumnMaskID(maskPath)
	if err != nil {
		return fmt.Errorf("resolve column mask for binding: %w", err)
	}

	switch action.Operation {
	case declarative.OpCreate:
		binding := action.Desired.(declarative.MaskBindingRef)
		principalID, err := c.resolvePrincipalID(binding.Principal, binding.PrincipalType)
		if err != nil {
			return fmt.Errorf("resolve principal for column mask binding: %w", err)
		}
		body := map[string]interface{}{
			"principal_id":   principalID,
			"principal_type": binding.PrincipalType,
			"see_original":   binding.SeeOriginal,
		}
		resp, err := c.client.Do(http.MethodPost, "/column-masks/"+maskID+"/bindings", nil, body)
		if err != nil {
			return err
		}
		return apiruntime.CheckError(resp)

	case declarative.OpDelete:
		binding := action.Actual.(declarative.MaskBindingRef)
		principalID, err := c.resolvePrincipalID(binding.Principal, binding.PrincipalType)
		if err != nil {
			return fmt.Errorf("resolve principal for column mask binding delete: %w", err)
		}
		q := url.Values{}
		q.Set("principal_id", principalID)
		q.Set("principal_type", binding.PrincipalType)
		resp, err := c.client.Do(http.MethodDelete, "/column-masks/"+maskID+"/bindings", q, nil)
		if err != nil {
			return err
		}
		return apiruntime.CheckError(resp)

	default:
		return fmt.Errorf("unsupported operation %s for column-mask-binding", action.Operation)
	}
}
