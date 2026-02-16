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
			// Connection error (e.g., server panic) — treat as unavailable.
			return nil, nil //nolint:nilerr // intentional: unavailable endpoint returns empty
		}

		body, err := gen.ReadBody(resp)
		if err != nil {
			return nil, fmt.Errorf("read GET %s: %w", path, err)
		}

		if resp.StatusCode == http.StatusNotFound || resp.StatusCode == http.StatusBadRequest {
			// Endpoint may not exist or requires unsupported filter params — return empty.
			return nil, nil
		}
		if resp.StatusCode >= 500 {
			// Server error (e.g., service not wired) — treat as empty.
			return nil, nil
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
	Name string `json:"name"`
	Type string `json:"type"`
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
				spec.Members = append(spec.Members, declarative.MemberRef{
					Name: m.Name,
					Type: m.Type,
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
	// The grants API requires principal_id+principal_type or securable_type+securable_id
	// filter parameters and returns ID-based references (not name-based). Full grant
	// reconciliation requires a name→ID resolver which is not yet implemented.
	// Grants are skipped during ReadState — they are handled separately by the
	// declarative differ when both desired and actual states include grants.
	return nil
}

type apiAPIKey struct {
	Name      string  `json:"name"`
	Principal string  `json:"principal"`
	ExpiresAt *string `json:"expires_at"`
}

func (c *APIStateClient) readAPIKeys(ctx context.Context, state *declarative.DesiredState) error {
	pages, err := c.fetchAllPages(ctx, "/api-keys")
	if err != nil {
		// The api-keys endpoint may not be available (service not wired) or may
		// require a principal_id filter. Silently skip.
		return nil //nolint:nilerr // intentional: unavailable endpoint is silently skipped
	}
	if len(pages) == 0 {
		return nil
	}

	var items []apiAPIKey
	if err := mergePages(pages, &items); err != nil {
		return err
	}

	for _, k := range items {
		state.APIKeys = append(state.APIKeys, declarative.APIKeySpec{
			Name:      k.Name,
			Principal: k.Principal,
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

// === Name-to-ID Resolution ===

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
		ID string `json:"id"`
	}
	_ = json.Unmarshal(body, &created)
	return created.ID, nil
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
	default:
		return fmt.Errorf("execute %s %s: resource kind not yet implemented", action.Operation, action.ResourceKind)
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

func (c *APIStateClient) executeSchema(_ context.Context, action declarative.Action) error {
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

func (c *APIStateClient) executeTable(_ context.Context, action declarative.Action) error {
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
		memberID, err := c.resolvePrincipalID(member.Name, member.Type)
		if err != nil {
			return fmt.Errorf("resolve member for group membership delete: %w", err)
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
