package cli

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"duck-demo/pkg/cli/apiruntime"
)

func TestBuildDesiredState_WithSecurity(t *testing.T) {
	t.Parallel()

	state := buildDesiredState(initOptions{
		env:            "staging",
		catalogName:    "lake",
		metastoreType:  "sqlite",
		metastoreDSN:   "./ducklake_lake.sqlite",
		credentialName: "staging-default-s3",
		prefix:         "staging",
		bucket:         "demo-bucket",
		endpoint:       "s3.example.com",
		region:         "us-east-1",
		keyID:          "key",
		secret:         "secret",
		urlStyle:       "path",
		withSecurity:   true,
	})

	assert.Equal(t, []string{"landing", "bronze", "silver", "gold"}, state.Schemas)
	assert.Equal(t, "s3://demo-bucket/staging/", state.DataPath)
	assert.Equal(t, "staging-landing", state.SchemaLocation["landing"])
	assert.Equal(t, "staging-default-s3", state.Credential.Name)
	assert.Len(t, state.Locations, 4)
	assert.Contains(t, state.Groups, "analytics")
	assert.Contains(t, state.Principals, "svc-transform")
	assert.NotEmpty(t, state.Memberships)
	assert.NotEmpty(t, state.SchemaGrants)
	assert.NotEmpty(t, state.ServiceGrants)
}

func TestBuildDesiredState_WithoutSecurity(t *testing.T) {
	t.Parallel()

	state := buildDesiredState(initOptions{
		env:            "staging",
		catalogName:    "lake",
		metastoreType:  "sqlite",
		metastoreDSN:   "./ducklake_lake.sqlite",
		credentialName: "staging-default-s3",
		prefix:         "staging",
		bucket:         "demo-bucket",
		endpoint:       "s3.example.com",
		region:         "us-east-1",
		keyID:          "key",
		secret:         "secret",
		urlStyle:       "path",
	})

	assert.Empty(t, state.Groups)
	assert.Empty(t, state.Principals)
	assert.Empty(t, state.Memberships)
	assert.Empty(t, state.SchemaGrants)
	assert.Empty(t, state.ServiceGrants)
}

func TestComputeInitPlan_MissingResources(t *testing.T) {
	t.Parallel()

	desired := buildDesiredState(initOptions{
		env:            "staging",
		catalogName:    "lake",
		metastoreType:  "sqlite",
		metastoreDSN:   "./ducklake_lake.sqlite",
		credentialName: "staging-default-s3",
		prefix:         "staging",
		bucket:         "demo-bucket",
		endpoint:       "s3.example.com",
		region:         "us-east-1",
		keyID:          "key",
		secret:         "secret",
		urlStyle:       "path",
		withSecurity:   true,
	})

	plan := computeInitPlan(desired, initExistingState{
		Credentials: map[string]bool{},
		Locations:   map[string]bool{},
		Catalogs:    map[string]bool{},
		Schemas:     map[string]string{},
		Groups:      map[string]string{},
		Principals:  map[string]string{},
		Memberships: map[string]map[string]bool{},
		Grants:      map[string]bool{},
	})

	assert.Contains(t, plan.Creates, `storage credential "staging-default-s3"`)
	assert.Contains(t, plan.Creates, `storage location "staging-landing"`)
	assert.Contains(t, plan.Creates, `catalog "lake"`)
	assert.Contains(t, plan.Creates, `schema "landing"`)
	assert.Contains(t, plan.Creates, `group "analytics"`)
	assert.Contains(t, plan.Creates, `principal "svc-bi"`)
	assert.Contains(t, plan.Creates, `membership "service-accounts" <- "svc-ingest"`)
	assert.Contains(t, plan.Creates, `grant USAGE on schema "gold" to group "analytics"`)
	assert.Empty(t, plan.Exists)
}

func TestComputeInitPlan_ExistingResources(t *testing.T) {
	t.Parallel()

	desired := buildDesiredState(initOptions{
		env:            "staging",
		catalogName:    "lake",
		metastoreType:  "sqlite",
		metastoreDSN:   "./ducklake_lake.sqlite",
		credentialName: "staging-default-s3",
		prefix:         "staging",
		bucket:         "demo-bucket",
		endpoint:       "s3.example.com",
		region:         "us-east-1",
		keyID:          "key",
		secret:         "secret",
		urlStyle:       "path",
		withSecurity:   true,
	})

	existing := initExistingState{
		Credentials: map[string]bool{"staging-default-s3": true},
		Locations: map[string]bool{
			"staging-landing": true,
			"staging-bronze":  true,
			"staging-silver":  true,
			"staging-gold":    true,
		},
		Catalogs: map[string]bool{"lake": true},
		Schemas: map[string]string{
			"landing": "schema-landing",
			"bronze":  "schema-bronze",
			"silver":  "schema-silver",
			"gold":    "schema-gold",
		},
		Groups: map[string]string{
			"platform-admins":  "group-platform-admins",
			"data-engineers":   "group-data-engineers",
			"analytics":        "group-analytics",
			"service-accounts": "group-service-accounts",
		},
		Principals: map[string]string{
			"svc-ingest":    "principal-svc-ingest",
			"svc-transform": "principal-svc-transform",
			"svc-bi":        "principal-svc-bi",
		},
		Memberships: map[string]map[string]bool{
			"group-service-accounts": {
				"principal-svc-ingest":    true,
				"principal-svc-transform": true,
				"principal-svc-bi":        true,
			},
			"group-data-engineers": {
				"principal-svc-ingest":    true,
				"principal-svc-transform": true,
			},
			"group-analytics": {
				"principal-svc-bi": true,
			},
		},
		Grants: map[string]bool{
			grantKey("group-data-engineers", "group", "schema-landing", "schema", "USAGE"):   true,
			grantKey("group-data-engineers", "group", "schema-bronze", "schema", "USAGE"):    true,
			grantKey("group-data-engineers", "group", "schema-silver", "schema", "USAGE"):    true,
			grantKey("group-data-engineers", "group", "schema-gold", "schema", "USAGE"):      true,
			grantKey("group-analytics", "group", "schema-gold", "schema", "USAGE"):           true,
			grantKey("group-service-accounts", "group", "schema-landing", "schema", "USAGE"): true,
			grantKey("group-service-accounts", "group", "schema-bronze", "schema", "USAGE"):  true,
			grantKey("group-service-accounts", "group", "schema-silver", "schema", "USAGE"):  true,
			grantKey("group-service-accounts", "group", "schema-gold", "schema", "USAGE"):    true,
			grantKey("principal-svc-ingest", "user", "schema-landing", "schema", "USAGE"):    true,
			grantKey("principal-svc-transform", "user", "schema-landing", "schema", "USAGE"): true,
			grantKey("principal-svc-ingest", "user", "schema-bronze", "schema", "USAGE"):     true,
			grantKey("principal-svc-transform", "user", "schema-bronze", "schema", "USAGE"):  true,
			grantKey("principal-svc-transform", "user", "schema-silver", "schema", "USAGE"):  true,
			grantKey("principal-svc-transform", "user", "schema-gold", "schema", "USAGE"):    true,
			grantKey("principal-svc-bi", "user", "schema-gold", "schema", "USAGE"):           true,
		},
	}

	plan := computeInitPlan(desired, existing)

	assert.Empty(t, plan.Creates)
	assert.Contains(t, plan.Exists, `storage credential "staging-default-s3"`)
	assert.Contains(t, plan.Exists, `storage location "staging-gold"`)
	assert.Contains(t, plan.Exists, `catalog "lake"`)
	assert.Contains(t, plan.Exists, `schema "gold"`)
	assert.Contains(t, plan.Exists, `group "analytics"`)
	assert.Contains(t, plan.Exists, `principal "svc-bi"`)
	assert.Contains(t, plan.Exists, `membership "analytics" <- "svc-bi"`)
	assert.Contains(t, plan.Exists, `grant USAGE on schema "gold" to user "svc-bi"`)
}

func TestApplyDesiredState_RetryResumesAfterPartialSchemaFailure(t *testing.T) {
	t.Parallel()

	desired := buildDesiredState(initOptions{
		env:            "staging",
		catalogName:    "lake",
		metastoreType:  "sqlite",
		metastoreDSN:   "./ducklake_lake.sqlite",
		credentialName: "staging-default-s3",
		prefix:         "staging",
		bucket:         "demo-bucket",
		endpoint:       "s3.example.com",
		region:         "us-east-1",
		keyID:          "key",
		secret:         "secret",
		urlStyle:       "path",
		withSecurity:   false,
	})

	fakeAPI := newFakeInitAPI("bronze")
	server := httptest.NewServer(fakeAPI)
	t.Cleanup(server.Close)

	client := apiruntime.NewClient(server.URL, "test-api-key", "")

	existing, err := fetchExistingState(client, desired)
	require.NoError(t, err)

	err = applyDesiredState(client, desired, existing)
	require.Error(t, err)
	require.ErrorContains(t, err, `create schema "bronze"`)

	assert.True(t, fakeAPI.credentials[desired.Credential.Name])
	assert.True(t, fakeAPI.locations["staging-landing"])
	assert.True(t, fakeAPI.catalogs[desired.CatalogName])
	assert.Contains(t, fakeAPI.schemas, "landing")
	assert.NotContains(t, fakeAPI.schemas, "bronze")

	existing, err = fetchExistingState(client, desired)
	require.NoError(t, err)

	err = applyDesiredState(client, desired, existing)
	require.NoError(t, err)

	assert.Len(t, fakeAPI.schemas, len(desired.Schemas))
	assert.Equal(t, 1, fakeAPI.createCounts["credential:"+desired.Credential.Name])
	assert.Equal(t, 1, fakeAPI.createCounts["catalog:"+desired.CatalogName])
	assert.Equal(t, 1, fakeAPI.createCounts["schema:landing"])
	assert.Equal(t, 2, fakeAPI.createCounts["schema:bronze"])
	assert.Equal(t, 1, fakeAPI.createCounts["schema:silver"])
	assert.Equal(t, 1, fakeAPI.createCounts["schema:gold"])
	for _, loc := range desired.Locations {
		assert.Equal(t, 1, fakeAPI.createCounts["location:"+loc.Name])
	}
}

type fakeInitAPI struct {
	credentials       map[string]bool
	locations         map[string]bool
	catalogs          map[string]bool
	schemas           map[string]string
	createCounts      map[string]int
	failSchemaName    string
	failSchemaTrigger bool
}

func newFakeInitAPI(failSchemaName string) *fakeInitAPI {
	return &fakeInitAPI{
		credentials:       map[string]bool{},
		locations:         map[string]bool{},
		catalogs:          map[string]bool{},
		schemas:           map[string]string{},
		createCounts:      map[string]int{},
		failSchemaName:    failSchemaName,
		failSchemaTrigger: true,
	}
}

func (f *fakeInitAPI) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch {
	case r.Method == http.MethodGet && r.URL.Path == "/v1/storage-credentials":
		writeInitListJSON(w, "data", sortedNames(f.credentials))
	case r.Method == http.MethodGet && r.URL.Path == "/v1/external-locations":
		writeInitListJSON(w, "data", sortedNames(f.locations))
	case r.Method == http.MethodGet && r.URL.Path == "/v1/catalogs":
		writeInitListJSON(w, "data", sortedNames(f.catalogs))
	case r.Method == http.MethodGet && r.URL.Path == "/v1/catalogs/lake/schemas":
		names := make([]string, 0, len(f.schemas))
		for name := range f.schemas {
			names = append(names, name)
		}
		sort.Strings(names)
		rows := make([]map[string]string, 0, len(names))
		for _, name := range names {
			rows = append(rows, map[string]string{"id": f.schemas[name], "name": name})
		}
		writeJSON(w, http.StatusOK, map[string]any{"data": rows})
	case r.Method == http.MethodPost && r.URL.Path == "/v1/storage-credentials":
		name := decodeName(r)
		f.createCounts["credential:"+name]++
		if f.credentials[name] {
			writeJSON(w, http.StatusConflict, map[string]any{"code": 409, "message": "already exists"})
			return
		}
		f.credentials[name] = true
		writeJSON(w, http.StatusCreated, map[string]any{"name": name})
	case r.Method == http.MethodPost && r.URL.Path == "/v1/external-locations":
		name := decodeName(r)
		f.createCounts["location:"+name]++
		if f.locations[name] {
			writeJSON(w, http.StatusConflict, map[string]any{"code": 409, "message": "already exists"})
			return
		}
		f.locations[name] = true
		writeJSON(w, http.StatusCreated, map[string]any{"name": name})
	case r.Method == http.MethodPost && r.URL.Path == "/v1/catalogs":
		name := decodeName(r)
		f.createCounts["catalog:"+name]++
		if f.catalogs[name] {
			writeJSON(w, http.StatusConflict, map[string]any{"code": 409, "message": "already exists"})
			return
		}
		f.catalogs[name] = true
		writeJSON(w, http.StatusCreated, map[string]any{"name": name})
	case r.Method == http.MethodPost && r.URL.Path == "/v1/catalogs/lake/schemas":
		name := decodeName(r)
		f.createCounts["schema:"+name]++
		if f.failSchemaTrigger && name == f.failSchemaName {
			f.failSchemaTrigger = false
			writeJSON(w, http.StatusInternalServerError, map[string]any{"code": 500, "message": "synthetic schema create failure"})
			return
		}
		if _, ok := f.schemas[name]; ok {
			writeJSON(w, http.StatusConflict, map[string]any{"code": 409, "message": "already exists"})
			return
		}
		f.schemas[name] = "schema-" + name
		writeJSON(w, http.StatusCreated, map[string]any{"id": f.schemas[name], "name": name})
	default:
		writeJSON(w, http.StatusNotFound, map[string]any{"code": 404, "message": "not found"})
	}
}

func sortedNames(items map[string]bool) []string {
	names := make([]string, 0, len(items))
	for name := range items {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func writeInitListJSON(w http.ResponseWriter, field string, names []string) {
	rows := make([]map[string]string, 0, len(names))
	for _, name := range names {
		rows = append(rows, map[string]string{"name": name})
	}
	writeJSON(w, http.StatusOK, map[string]any{field: rows})
}

func decodeName(r *http.Request) string {
	var body map[string]any
	_ = json.NewDecoder(r.Body).Decode(&body)
	name, _ := body["name"].(string)
	return name
}

func writeJSON(w http.ResponseWriter, status int, body map[string]any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}
