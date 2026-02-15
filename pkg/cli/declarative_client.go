package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"

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

// APIStateClient implements both StateReader and StateWriter using the gen.Client.
type APIStateClient struct {
	client *gen.Client
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
func (c *APIStateClient) ReadState(ctx context.Context) (*declarative.DesiredState, error) {
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

		// Fetch schemas for this catalog.
		if err := c.readSchemas(ctx, cat.Name, state); err != nil {
			return fmt.Errorf("catalog %q schemas: %w", cat.Name, err)
		}
	}
	return nil
}

type apiSchema struct {
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
	}
	return nil
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

// === Execute ===

// Execute applies a single planned action to the server via the API.
func (c *APIStateClient) Execute(ctx context.Context, action declarative.Action) error {
	switch action.ResourceKind {
	case declarative.KindPrincipal:
		return c.executePrincipal(ctx, action)
	case declarative.KindGroup:
		return c.executeGroup(ctx, action)
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
	default:
		return fmt.Errorf("execute %s %s: resource kind not yet implemented", action.Operation, action.ResourceKind)
	}
}

// --- Security resource execution ---

func (c *APIStateClient) executePrincipal(_ context.Context, action declarative.Action) error {
	switch action.Operation {
	case declarative.OpCreate:
		resp, err := c.client.Do(http.MethodPost, "/principals", nil, action.Desired)
		if err != nil {
			return err
		}
		return gen.CheckError(resp)

	case declarative.OpUpdate:
		resp, err := c.client.Do(http.MethodPatch, "/principals/"+action.ResourceName, nil, action.Desired)
		if err != nil {
			return err
		}
		return gen.CheckError(resp)

	case declarative.OpDelete:
		resp, err := c.client.Do(http.MethodDelete, "/principals/"+action.ResourceName, nil, nil)
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
		resp, err := c.client.Do(http.MethodPost, "/groups", nil, action.Desired)
		if err != nil {
			return err
		}
		return gen.CheckError(resp)

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
		resp, err := c.client.Do(http.MethodPost, "/grants", nil, action.Desired)
		if err != nil {
			return err
		}
		return gen.CheckError(resp)

	case declarative.OpDelete:
		resp, err := c.client.Do(http.MethodDelete, "/grants", nil, action.Actual)
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
		resp, err := c.client.Do(http.MethodPost, "/catalogs", nil, action.Desired)
		if err != nil {
			return err
		}
		return gen.CheckError(resp)

	case declarative.OpUpdate:
		resp, err := c.client.Do(http.MethodPatch, "/catalogs/"+action.ResourceName, nil, action.Desired)
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
		resp, err := c.client.Do(http.MethodPost, "/schemas", nil, action.Desired)
		if err != nil {
			return err
		}
		return gen.CheckError(resp)

	case declarative.OpUpdate:
		resp, err := c.client.Do(http.MethodPatch, "/schemas/"+action.ResourceName, nil, action.Desired)
		if err != nil {
			return err
		}
		return gen.CheckError(resp)

	case declarative.OpDelete:
		resp, err := c.client.Do(http.MethodDelete, "/schemas/"+action.ResourceName, nil, nil)
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
		resp, err := c.client.Do(http.MethodPost, "/tables", nil, action.Desired)
		if err != nil {
			return err
		}
		return gen.CheckError(resp)

	case declarative.OpUpdate:
		resp, err := c.client.Do(http.MethodPatch, "/tables/"+action.ResourceName, nil, action.Desired)
		if err != nil {
			return err
		}
		return gen.CheckError(resp)

	case declarative.OpDelete:
		resp, err := c.client.Do(http.MethodDelete, "/tables/"+action.ResourceName, nil, nil)
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
		resp, err := c.client.Do(http.MethodPost, "/views", nil, action.Desired)
		if err != nil {
			return err
		}
		return gen.CheckError(resp)

	case declarative.OpUpdate:
		resp, err := c.client.Do(http.MethodPatch, "/views/"+action.ResourceName, nil, action.Desired)
		if err != nil {
			return err
		}
		return gen.CheckError(resp)

	case declarative.OpDelete:
		resp, err := c.client.Do(http.MethodDelete, "/views/"+action.ResourceName, nil, nil)
		if err != nil {
			return err
		}
		return gen.CheckError(resp)

	default:
		return fmt.Errorf("unsupported operation %s for view", action.Operation)
	}
}
