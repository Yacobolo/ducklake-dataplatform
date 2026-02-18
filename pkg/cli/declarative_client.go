package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"duck-demo/internal/declarative"
	"duck-demo/pkg/cli/gen"
)

// StateReader fetches current state from the server API.
type StateReader interface {
	ReadState(ctx context.Context) (*declarative.DesiredState, error)
}

// StateWriter applies changes to the server API.
type StateWriter interface {
	Execute(ctx context.Context, action declarative.Action) error
}

// resourceIndex maps human-readable names to API UUIDs. It is populated during
// ReadState and consumed by Execute methods that must resolve names to IDs.
type resourceIndex struct {
	principalIDByName  map[string]string // "alice" → UUID
	groupIDByName      map[string]string // "admins" → UUID
	catalogIDByName    map[string]string // "demo" → UUID
	schemaIDByPath     map[string]string // "catalog.schema" → UUID
	tableIDByPath      map[string]string // "catalog.schema.table" → UUID
	tagIDByKey         map[string]string // "pii" or "pii:value" → UUID
	rowFilterIDByPath  map[string]string // "cat.sch.tbl/filterName" → UUID
	columnMaskIDByPath map[string]string // "cat.sch.tbl/maskName" → UUID
}

func newResourceIndex() *resourceIndex {
	return &resourceIndex{
		principalIDByName:  make(map[string]string),
		groupIDByName:      make(map[string]string),
		catalogIDByName:    make(map[string]string),
		schemaIDByPath:     make(map[string]string),
		tableIDByPath:      make(map[string]string),
		tagIDByKey:         make(map[string]string),
		rowFilterIDByPath:  make(map[string]string),
		columnMaskIDByPath: make(map[string]string),
	}
}

// APIStateClient implements both StateReader and StateWriter using the gen.Client.
type APIStateClient struct {
	client *gen.Client
	index  *resourceIndex
}

// Compile-time interface checks.
var (
	_ StateReader = (*APIStateClient)(nil)
	_ StateWriter = (*APIStateClient)(nil)
)

// NewAPIStateClient creates a new client adapter.
func NewAPIStateClient(client *gen.Client) *APIStateClient {
	return &APIStateClient{client: client}
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

		body, err := gen.ReadBody(resp)
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
	state := &declarative.DesiredState{}

	if err := c.readPrincipals(ctx, state); err != nil {
		return nil, fmt.Errorf("read principals: %w", err)
	}
	if err := c.readGroups(ctx, state); err != nil {
		return nil, fmt.Errorf("read groups: %w", err)
	}
	if err := c.readGrants(ctx, state); err != nil {
		return nil, fmt.Errorf("read grants: %w", err)
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
	if err := c.readComputeEndpoints(ctx, state); err != nil {
		return nil, fmt.Errorf("read compute endpoints: %w", err)
	}
	if err := c.readTags(ctx, state); err != nil {
		return nil, fmt.Errorf("read tags: %w", err)
	}
	if err := c.readNotebooks(ctx, state); err != nil {
		return nil, fmt.Errorf("read notebooks: %w", err)
	}
	if err := c.readPipelines(ctx, state); err != nil {
		return nil, fmt.Errorf("read pipelines: %w", err)
	}
	if err := c.readMacros(ctx, state); err != nil {
		if !isOptionalReadError(err) {
			return nil, fmt.Errorf("read macros: %w", err)
		}
	}
	if err := c.readModels(ctx, state); err != nil {
		if !isOptionalReadError(err) {
			return nil, fmt.Errorf("read models: %w", err)
		}
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

func (c *APIStateClient) readGrants(_ context.Context, _ *declarative.DesiredState) error {
	// The grants API returns ID-based references (principal_id, securable_id) but
	// the declarative config uses names (principal, securable dot-path). Full grant
	// reconciliation requires an ID→name resolver which is not yet implemented.
	// Grants are skipped during ReadState — they are handled separately by the
	// declarative differ when both desired and actual states include grants.
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
	}
	return nil
}

// --- Storage resources ---

type apiStorageCredential struct {
	Name           string `json:"name"`
	CredentialType string `json:"credential_type"`
	Comment        string `json:"comment"`
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
		state.StorageCredentials = append(state.StorageCredentials, declarative.StorageCredentialSpec{
			Name:           sc.Name,
			CredentialType: sc.CredentialType,
			Comment:        sc.Comment,
		})
	}
	return nil
}

type apiExternalLocation struct {
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
	}
	return nil
}

// --- Compute resources ---

type apiComputeEndpoint struct {
	Name string `json:"name"`
	URL  string `json:"url"`
	Type string `json:"type"`
	Size string `json:"size"`
}

type apiComputeAssignment struct {
	Endpoint      string `json:"endpoint"`
	Principal     string `json:"principal"`
	PrincipalType string `json:"principal_type"`
	IsDefault     bool   `json:"is_default"`
	FallbackLocal bool   `json:"fallback_local"`
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
			Name: ep.Name,
			URL:  ep.URL,
			Type: ep.Type,
			Size: ep.Size,
		})

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
	Name        string `json:"name"`
	Description string `json:"description"`
	Owner       string `json:"owner"`
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
		state.Notebooks = append(state.Notebooks, declarative.NotebookResource{
			Name: nb.Name,
			Spec: declarative.NotebookSpec{
				Description: nb.Description,
				Owner:       nb.Owner,
				// Cells omitted for simplicity — would require per-notebook fetch.
			},
		})
	}
	return nil
}

type apiPipeline struct {
	Name         string `json:"name"`
	Description  string `json:"description"`
	ScheduleCron string `json:"schedule_cron"`
	IsPaused     bool   `json:"is_paused"`
}

func (c *APIStateClient) readPipelines(ctx context.Context, state *declarative.DesiredState) error {
	pages, err := c.fetchAllPages(ctx, "/pipelines")
	if err != nil {
		return err
	}
	if len(pages) == 0 {
		return nil
	}

	var items []apiPipeline
	if err := mergePages(pages, &items); err != nil {
		return err
	}

	for _, pl := range items {
		state.Pipelines = append(state.Pipelines, declarative.PipelineResource{
			Name: pl.Name,
			Spec: declarative.PipelineSpec{
				Description:  pl.Description,
				ScheduleCron: pl.ScheduleCron,
				IsPaused:     pl.IsPaused,
				// Jobs omitted for simplicity — would require per-pipeline fetch.
			},
		})
	}
	return nil
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

func (c *APIStateClient) listModelTests(ctx context.Context, projectName, modelName string) ([]apiModelTest, error) {
	pages, err := c.fetchAllPages(ctx, "/models/"+projectName+"/"+modelName+"/tests")
	if err != nil {
		if isOptionalReadError(err) {
			return nil, nil
		}
		return nil, err
	}
	if len(pages) == 0 {
		return nil, nil
	}

	var tests []apiModelTest
	if err := mergePages(pages, &tests); err != nil {
		return nil, err
	}
	return tests, nil
}

func isOptionalReadError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	if strings.Contains(msg, "http 404") || strings.Contains(msg, "http 405") || strings.Contains(msg, "http 501") {
		return true
	}
	if strings.Contains(msg, "eof") || strings.Contains(msg, "connection reset by peer") || strings.Contains(msg, "broken pipe") {
		return true
	}
	return false
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

	for _, m := range items {
		tests, err := c.listModelTests(ctx, m.ProjectName, m.Name)
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

func (c *APIStateClient) lookupMemberNameByID(_ context.Context, id, memberType string) (string, error) {
	path := "/principals/" + id
	if memberType == "group" {
		path = "/groups/" + id
	}
	resp, err := c.client.Do(http.MethodGet, path, nil, nil)
	if err != nil {
		return "", err
	}
	body, err := gen.ReadBody(resp)
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

// resolveSecurableID looks up a securable UUID by type and dot-path.
func (c *APIStateClient) resolveSecurableID(securableType, path string) (string, error) {
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
	case "table":
		if id, ok := c.index.tableIDByPath[path]; ok {
			return id, nil
		}
	default:
		// For other types (volume, external_location, etc.) try all maps.
		if id, ok := c.index.tableIDByPath[path]; ok {
			return id, nil
		}
		if id, ok := c.index.schemaIDByPath[path]; ok {
			return id, nil
		}
		if id, ok := c.index.catalogIDByName[path]; ok {
			return id, nil
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
	body, err := gen.ReadBody(resp)
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

// === Execute ===

// Execute applies a single planned action to the server via the API.
func (c *APIStateClient) Execute(ctx context.Context, action declarative.Action) error {
	switch action.ResourceKind {
	case declarative.KindPrincipal:
		return c.executePrincipal(ctx, action)
	case declarative.KindGroup:
		return c.executeGroup(ctx, action)
	case declarative.KindGroupMembership:
		return c.executeGroupMembership(ctx, action)
	case declarative.KindPrivilegeGrant:
		return c.executeGrant(ctx, action)
	case declarative.KindCatalogRegistration:
		return c.executeCatalog(ctx, action)
	case declarative.KindSchema:
		return c.executeSchema(ctx, action)
	case declarative.KindTable:
		return c.executeTable(ctx, action)
	case declarative.KindView:
		return c.executeView(ctx, action)
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
	case declarative.KindMacro:
		return c.executeMacro(ctx, action)
	case declarative.KindModel:
		return c.executeModel(ctx, action)
	default:
		return fmt.Errorf("execute %s %s: resource kind not yet implemented", action.Operation, action.ResourceKind)
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
		return gen.CheckError(resp)

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
		if err := gen.CheckError(resp); err != nil {
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
		return gen.CheckError(resp)

	default:
		return fmt.Errorf("unsupported operation %s for api-key", action.Operation)
	}
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
	actual, err := c.listModelTests(ctx, projectName, modelName)
	if err != nil {
		return err
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
			if err := gen.CheckError(resp); err != nil {
				return err
			}
		}

		resp, err := c.client.Do(http.MethodPost, "/models/"+projectName+"/"+modelName+"/tests", nil, toModelTestBody(wanted))
		if err != nil {
			return err
		}
		if err := gen.CheckError(resp); err != nil {
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
		if err := gen.CheckError(resp); err != nil {
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
		return gen.CheckError(resp)

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
		return gen.CheckError(resp)

	case declarative.OpDelete:
		resp, err := c.client.Do(http.MethodDelete, "/macros/"+action.ResourceName, nil, nil)
		if err != nil {
			return err
		}
		return gen.CheckError(resp)

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
		if err := gen.CheckError(resp); err != nil {
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
		if err := gen.CheckError(resp); err != nil {
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
		return gen.CheckError(resp)

	default:
		return fmt.Errorf("unsupported operation %s for model", action.Operation)
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
		return gen.CheckError(resp)

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
		return gen.CheckError(resp)

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
		return gen.CheckError(resp)

	case declarative.OpDelete:
		resp, err := c.client.Do(http.MethodDelete, "/groups/"+action.ResourceName, nil, nil)
		if err != nil {
			return err
		}
		return gen.CheckError(resp)

	default:
		return fmt.Errorf("unsupported operation %s for group", action.Operation)
	}
}

func (c *APIStateClient) executeGrant(_ context.Context, action declarative.Action) error {
	switch action.Operation {
	case declarative.OpCreate:
		grant := action.Desired.(declarative.GrantSpec)
		principalID, err := c.resolvePrincipalID(grant.Principal, grant.PrincipalType)
		if err != nil {
			return fmt.Errorf("resolve principal for grant: %w", err)
		}
		securableID, err := c.resolveSecurableID(grant.SecurableType, grant.Securable)
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
		return gen.CheckError(resp)

	case declarative.OpDelete:
		grant := action.Actual.(declarative.GrantSpec)
		principalID, err := c.resolvePrincipalID(grant.Principal, grant.PrincipalType)
		if err != nil {
			return fmt.Errorf("resolve principal for grant delete: %w", err)
		}
		securableID, err := c.resolveSecurableID(grant.SecurableType, grant.Securable)
		if err != nil {
			return fmt.Errorf("resolve securable for grant delete: %w", err)
		}
		body := map[string]interface{}{
			"principal_id":   principalID,
			"principal_type": grant.PrincipalType,
			"securable_id":   securableID,
			"securable_type": grant.SecurableType,
			"privilege":      grant.Privilege,
		}
		resp, err := c.client.Do(http.MethodDelete, "/grants", nil, body)
		if err != nil {
			return err
		}
		return gen.CheckError(resp)

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
		return gen.CheckError(resp)

	case declarative.OpDelete:
		resp, err := c.client.Do(http.MethodDelete, "/catalogs/"+action.ResourceName, nil, nil)
		if err != nil {
			return err
		}
		return gen.CheckError(resp)

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
		return gen.CheckError(resp)

	case declarative.OpDelete:
		parts := strings.SplitN(action.ResourceName, ".", 2)
		if len(parts) != 2 {
			return fmt.Errorf("invalid schema resource name: %s", action.ResourceName)
		}
		resp, err := c.client.Do(http.MethodDelete, "/catalogs/"+parts[0]+"/schemas/"+parts[1], nil, nil)
		if err != nil {
			return err
		}
		return gen.CheckError(resp)

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
		return gen.CheckError(resp)

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
		return gen.CheckError(resp)

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
		return gen.CheckError(resp)

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
		return gen.CheckError(resp)

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
		return gen.CheckError(resp)

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
		return gen.CheckError(resp)

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
		return gen.CheckError(resp)

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
		return gen.CheckError(resp)

	default:
		return fmt.Errorf("unsupported operation %s for tag", action.Operation)
	}
}

// --- Tag assignment execution ---

func (c *APIStateClient) executeTagAssignment(_ context.Context, action declarative.Action) error {
	switch action.Operation {
	case declarative.OpCreate:
		assignment := action.Desired.(declarative.TagAssignmentSpec)
		tagID, err := c.resolveTagID(assignment.Tag)
		if err != nil {
			return fmt.Errorf("resolve tag for assignment: %w", err)
		}
		securableID, err := c.resolveSecurableID(assignment.SecurableType, assignment.Securable)
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
		return gen.CheckError(resp)

	case declarative.OpDelete:
		// Tag assignment deletes require the assignment ID. Since we don't
		// track assignment IDs during ReadState, we use the tag ID and
		// attempt deletion via the composite endpoint.
		assignment := action.Actual.(declarative.TagAssignmentSpec)
		tagID, err := c.resolveTagID(assignment.Tag)
		if err != nil {
			return fmt.Errorf("resolve tag for assignment delete: %w", err)
		}
		securableID, err := c.resolveSecurableID(assignment.SecurableType, assignment.Securable)
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
		return gen.CheckError(resp)

	default:
		return fmt.Errorf("unsupported operation %s for tag-assignment", action.Operation)
	}
}

// --- Row filter execution ---

func (c *APIStateClient) executeRowFilter(_ context.Context, action declarative.Action) error {
	switch action.Operation {
	case declarative.OpCreate:
		filter := action.Desired.(declarative.RowFilterSpec)
		// ResourceName is "catalog.schema.table/filterName" — extract table path.
		parts := strings.SplitN(action.ResourceName, "/", 2)
		tablePath := parts[0]
		tableID, err := c.resolveSecurableID("table", tablePath)
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
		return gen.CheckError(resp)

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
		return gen.CheckError(resp)

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
		return gen.CheckError(resp)

	default:
		return fmt.Errorf("unsupported operation %s for row-filter-binding", action.Operation)
	}
}

// --- Column mask execution ---

func (c *APIStateClient) executeColumnMask(_ context.Context, action declarative.Action) error {
	switch action.Operation {
	case declarative.OpCreate:
		mask := action.Desired.(declarative.ColumnMaskSpec)
		// ResourceName is "catalog.schema.table/maskName" — extract table path.
		parts := strings.SplitN(action.ResourceName, "/", 2)
		tablePath := parts[0]
		tableID, err := c.resolveSecurableID("table", tablePath)
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
		return gen.CheckError(resp)

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
		return gen.CheckError(resp)

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
		return gen.CheckError(resp)

	default:
		return fmt.Errorf("unsupported operation %s for column-mask-binding", action.Operation)
	}
}
