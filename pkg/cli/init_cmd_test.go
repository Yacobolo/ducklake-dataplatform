package cli

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
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

func TestRunInitWorkflow_RefreshesAfterMarkedPhase(t *testing.T) {
	t.Parallel()

	createdCatalog := false
	sawRefreshedState := false

	initial := initExistingState{
		Credentials: map[string]bool{},
		Locations:   map[string]bool{},
		Catalogs:    map[string]bool{},
		Schemas:     map[string]string{},
		Groups:      map[string]string{},
		Principals:  map[string]string{},
		Memberships: map[string]map[string]bool{},
		Grants:      map[string]bool{},
		GrantIDs:    map[string]string{},
	}

	err := runInitWorkflow(func() (initExistingState, error) {
		next := initial
		next.Catalogs = map[string]bool{}
		if createdCatalog {
			next.Catalogs["lake"] = true
		}
		return next, nil
	}, initial, []initWorkflowPhase{
		{
			name:         "ensure_catalog",
			refreshAfter: true,
			run: func(current initExistingState) error {
				assert.False(t, current.Catalogs["lake"])
				createdCatalog = true
				return nil
			},
		},
		{
			name: "ensure_schemas",
			run: func(current initExistingState) error {
				sawRefreshedState = current.Catalogs["lake"]
				return nil
			},
		},
	})

	require.NoError(t, err)
	assert.True(t, sawRefreshedState)
}

func TestRunInitWorkflow_StopsAfterPhaseFailure(t *testing.T) {
	t.Parallel()

	ranLaterPhase := false

	err := runInitWorkflow(func() (initExistingState, error) {
		return initExistingState{}, nil
	}, initExistingState{}, []initWorkflowPhase{
		{
			name: "ensure_catalog",
			run: func(current initExistingState) error {
				return nil
			},
		},
		{
			name: "ensure_schemas",
			run: func(current initExistingState) error {
				return errors.New("boom")
			},
		},
		{
			name: "ensure_security_identities",
			run: func(current initExistingState) error {
				ranLaterPhase = true
				return nil
			},
		},
	})

	require.Error(t, err)
	require.ErrorContains(t, err, "phase ensure_schemas")
	require.ErrorContains(t, err, "boom")
	assert.False(t, ranLaterPhase)
}

func TestRunInitWorkflow_RefreshFailureNamesPhase(t *testing.T) {
	t.Parallel()

	err := runInitWorkflow(func() (initExistingState, error) {
		return initExistingState{}, errors.New("refresh failed")
	}, initExistingState{}, []initWorkflowPhase{
		{
			name:         "ensure_catalog",
			refreshAfter: true,
			run: func(current initExistingState) error {
				return nil
			},
		},
	})

	require.Error(t, err)
	require.ErrorContains(t, err, "phase ensure_catalog")
	require.ErrorContains(t, err, "refresh state")
	require.ErrorContains(t, err, "refresh failed")
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

func TestDestroyDesiredState_RetryResumesAfterPartialSchemaDeleteFailure(t *testing.T) {
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

	fakeAPI := newFakeInitAPI("")
	fakeAPI.seedDesiredState(desired)
	fakeAPI.failDeleteSchemaName = "silver"
	fakeAPI.failDeleteSchemaTrigger = true

	server := httptest.NewServer(fakeAPI)
	t.Cleanup(server.Close)

	client := apiruntime.NewClient(server.URL, "test-api-key", "")

	existing, err := fetchExistingState(client, desired)
	require.NoError(t, err)

	err = destroyDesiredState(client, desired, existing)
	require.Error(t, err)
	require.ErrorContains(t, err, `delete schema "silver"`)

	assert.Empty(t, fakeAPI.grants)
	assert.Empty(t, fakeAPI.groups)
	assert.Empty(t, fakeAPI.principals)
	assert.Equal(t, 0, membershipCount(fakeAPI.memberships))
	assert.True(t, fakeAPI.catalogs[desired.CatalogName])
	assert.True(t, fakeAPI.credentials[desired.Credential.Name])
	assert.Contains(t, fakeAPI.schemas, "silver")
	assert.NotContains(t, fakeAPI.schemas, "gold")

	existing, err = fetchExistingState(client, desired)
	require.NoError(t, err)

	err = destroyDesiredState(client, desired, existing)
	require.NoError(t, err)

	assert.Empty(t, fakeAPI.credentials)
	assert.Empty(t, fakeAPI.locations)
	assert.Empty(t, fakeAPI.catalogs)
	assert.Empty(t, fakeAPI.schemas)
	assert.Empty(t, fakeAPI.groups)
	assert.Empty(t, fakeAPI.principals)
	assert.Equal(t, 0, membershipCount(fakeAPI.memberships))
	assert.Empty(t, fakeAPI.grants)

	for _, group := range desired.Groups {
		assert.Equal(t, 1, fakeAPI.deleteCounts["group:"+group])
	}
	for _, principal := range desired.Principals {
		assert.Equal(t, 1, fakeAPI.deleteCounts["principal:"+principal])
	}
	for _, membership := range desired.Memberships {
		assert.Equal(t, 1, fakeAPI.deleteCounts["membership:"+membership.GroupName+":"+membership.PrincipalName])
	}
	for _, grant := range append([]initGrantSpec{}, append(desired.SchemaGrants, desired.ServiceGrants...)...) {
		key := "grant:" + grant.PrincipalType + ":" + grant.PrincipalName + ":" + grant.SchemaName + ":" + grant.Privilege
		assert.Equal(t, 1, fakeAPI.deleteCounts[key])
	}
	for _, loc := range desired.Locations {
		assert.Equal(t, 1, fakeAPI.deleteCounts["location:"+loc.Name])
	}
	assert.Equal(t, 1, fakeAPI.deleteCounts["credential:"+desired.Credential.Name])
	assert.Equal(t, 1, fakeAPI.deleteCounts["catalog:"+desired.CatalogName])
	assert.Equal(t, 1, fakeAPI.deleteCounts["schema:gold"])
	assert.Equal(t, 2, fakeAPI.deleteCounts["schema:silver"])
	assert.Equal(t, 1, fakeAPI.deleteCounts["schema:bronze"])
	assert.Equal(t, 1, fakeAPI.deleteCounts["schema:landing"])
}

type fakeGrant struct {
	ID            string
	PrincipalID   string
	PrincipalType string
	SecurableID   string
	SecurableType string
	Privilege     string
}

type fakeInitAPI struct {
	credentials             map[string]bool
	locations               map[string]bool
	catalogs                map[string]bool
	schemas                 map[string]string
	groups                  map[string]string
	principals              map[string]string
	memberships             map[string]map[string]bool
	grants                  map[string]fakeGrant
	createCounts            map[string]int
	deleteCounts            map[string]int
	nextID                  int
	failSchemaName          string
	failSchemaTrigger       bool
	failDeleteSchemaName    string
	failDeleteSchemaTrigger bool
}

func newFakeInitAPI(failSchemaName string) *fakeInitAPI {
	return &fakeInitAPI{
		credentials:       map[string]bool{},
		locations:         map[string]bool{},
		catalogs:          map[string]bool{},
		schemas:           map[string]string{},
		groups:            map[string]string{},
		principals:        map[string]string{},
		memberships:       map[string]map[string]bool{},
		grants:            map[string]fakeGrant{},
		createCounts:      map[string]int{},
		deleteCounts:      map[string]int{},
		failSchemaName:    failSchemaName,
		failSchemaTrigger: true,
	}
}

func (f *fakeInitAPI) seedDesiredState(desired initDesiredState) {
	f.credentials[desired.Credential.Name] = true
	for _, loc := range desired.Locations {
		f.locations[loc.Name] = true
	}
	f.catalogs[desired.CatalogName] = true
	for _, schema := range desired.Schemas {
		f.schemas[schema] = "schema-" + schema
	}
	for _, group := range desired.Groups {
		f.groups[group] = "group-" + group
	}
	for _, principal := range desired.Principals {
		f.principals[principal] = "principal-" + principal
	}
	for _, membership := range desired.Memberships {
		groupID := f.groups[membership.GroupName]
		memberID := f.principals[membership.PrincipalName]
		if f.memberships[groupID] == nil {
			f.memberships[groupID] = map[string]bool{}
		}
		f.memberships[groupID][memberID] = true
	}
	for _, grant := range append([]initGrantSpec{}, append(desired.SchemaGrants, desired.ServiceGrants...)...) {
		principalID := f.principals[grant.PrincipalName]
		if grant.PrincipalType == "group" {
			principalID = f.groups[grant.PrincipalName]
		}
		schemaID := f.schemas[grant.SchemaName]
		key := grantKey(principalID, grant.PrincipalType, schemaID, "schema", grant.Privilege)
		f.grants[key] = fakeGrant{
			ID:            f.newID("grant"),
			PrincipalID:   principalID,
			PrincipalType: grant.PrincipalType,
			SecurableID:   schemaID,
			SecurableType: "schema",
			Privilege:     grant.Privilege,
		}
	}
}

func (f *fakeInitAPI) newID(prefix string) string {
	f.nextID++
	return fmt.Sprintf("%s-%d", prefix, f.nextID)
}

func (f *fakeInitAPI) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch {
	case r.Method == http.MethodGet && r.URL.Path == "/v1/storage-credentials":
		writeInitListJSON(w, "data", sortedNames(f.credentials))
	case r.Method == http.MethodGet && r.URL.Path == "/v1/external-locations":
		writeInitListJSON(w, "data", sortedNames(f.locations))
	case r.Method == http.MethodGet && r.URL.Path == "/v1/catalogs":
		writeInitListJSON(w, "data", sortedNames(f.catalogs))
	case r.Method == http.MethodGet && r.URL.Path == "/v1/groups":
		writeIDListJSON(w, sortedNamedIDs(f.groups))
	case r.Method == http.MethodGet && r.URL.Path == "/v1/principals":
		writeIDListJSON(w, sortedNamedIDs(f.principals))
	case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/v1/groups/") && strings.HasSuffix(r.URL.Path, "/members"):
		groupID := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/v1/groups/"), "/members")
		rows := make([]map[string]string, 0)
		for _, memberID := range sortedMemberIDs(f.memberships[groupID]) {
			rows = append(rows, map[string]string{"member_id": memberID})
		}
		writeJSON(w, http.StatusOK, map[string]any{"data": rows})
	case r.Method == http.MethodGet && r.URL.Path == "/v1/grants":
		rows := make([]map[string]string, 0, len(f.grants))
		for _, grant := range sortedGrants(f.grants) {
			rows = append(rows, map[string]string{
				"id":             grant.ID,
				"principal_id":   grant.PrincipalID,
				"principal_type": grant.PrincipalType,
				"securable_id":   grant.SecurableID,
				"securable_type": grant.SecurableType,
				"privilege":      grant.Privilege,
			})
		}
		writeJSON(w, http.StatusOK, map[string]any{"data": rows})
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
	case r.Method == http.MethodPost && r.URL.Path == "/v1/groups":
		name := decodeName(r)
		f.createCounts["group:"+name]++
		if _, ok := f.groups[name]; ok {
			writeJSON(w, http.StatusConflict, map[string]any{"code": 409, "message": "already exists"})
			return
		}
		f.groups[name] = "group-" + name
		writeJSON(w, http.StatusCreated, map[string]any{"id": f.groups[name], "name": name})
	case r.Method == http.MethodPost && r.URL.Path == "/v1/principals":
		name := decodeName(r)
		f.createCounts["principal:"+name]++
		if _, ok := f.principals[name]; ok {
			writeJSON(w, http.StatusConflict, map[string]any{"code": 409, "message": "already exists"})
			return
		}
		f.principals[name] = "principal-" + name
		writeJSON(w, http.StatusCreated, map[string]any{"id": f.principals[name], "name": name})
	case r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/v1/groups/") && strings.HasSuffix(r.URL.Path, "/members"):
		groupID := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/v1/groups/"), "/members")
		var body map[string]string
		_ = json.NewDecoder(r.Body).Decode(&body)
		memberID := body["member_id"]
		if f.memberships[groupID] == nil {
			f.memberships[groupID] = map[string]bool{}
		}
		f.memberships[groupID][memberID] = true
		writeJSON(w, http.StatusCreated, map[string]any{"member_id": memberID})
	case r.Method == http.MethodPost && r.URL.Path == "/v1/grants":
		var body map[string]string
		_ = json.NewDecoder(r.Body).Decode(&body)
		key := grantKey(body["principal_id"], body["principal_type"], body["securable_id"], body["securable_type"], body["privilege"])
		if _, ok := f.grants[key]; ok {
			writeJSON(w, http.StatusConflict, map[string]any{"code": 409, "message": "already exists"})
			return
		}
		grant := fakeGrant{
			ID:            f.newID("grant"),
			PrincipalID:   body["principal_id"],
			PrincipalType: body["principal_type"],
			SecurableID:   body["securable_id"],
			SecurableType: body["securable_type"],
			Privilege:     body["privilege"],
		}
		f.grants[key] = grant
		writeJSON(w, http.StatusCreated, map[string]any{"id": grant.ID})
	case r.Method == http.MethodDelete && strings.HasPrefix(r.URL.Path, "/v1/grants/"):
		grantID := strings.TrimPrefix(r.URL.Path, "/v1/grants/")
		if !f.deleteGrant(grantID) {
			writeJSON(w, http.StatusNotFound, map[string]any{"code": 404, "message": "not found"})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"status": "deleted"})
	case r.Method == http.MethodDelete && strings.HasPrefix(r.URL.Path, "/v1/groups/") && strings.HasSuffix(r.URL.Path, "/members"):
		groupID := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/v1/groups/"), "/members")
		memberID := r.URL.Query().Get("member_id")
		if f.memberships[groupID] == nil || !f.memberships[groupID][memberID] {
			writeJSON(w, http.StatusNotFound, map[string]any{"code": 404, "message": "not found"})
			return
		}
		delete(f.memberships[groupID], memberID)
		if len(f.memberships[groupID]) == 0 {
			delete(f.memberships, groupID)
		}
		f.deleteCounts["membership:"+f.groupNameForID(groupID)+":"+f.principalNameForID(memberID)]++
		writeJSON(w, http.StatusOK, map[string]any{"status": "deleted"})
	case r.Method == http.MethodDelete && strings.HasPrefix(r.URL.Path, "/v1/principals/"):
		principalID := strings.TrimPrefix(r.URL.Path, "/v1/principals/")
		name, ok := f.principalNameByID(principalID)
		if !ok {
			writeJSON(w, http.StatusNotFound, map[string]any{"code": 404, "message": "not found"})
			return
		}
		delete(f.principals, name)
		f.deleteCounts["principal:"+name]++
		writeJSON(w, http.StatusOK, map[string]any{"status": "deleted"})
	case r.Method == http.MethodDelete && strings.HasPrefix(r.URL.Path, "/v1/groups/"):
		groupID := strings.TrimPrefix(r.URL.Path, "/v1/groups/")
		name, ok := f.groupNameByID(groupID)
		if !ok {
			writeJSON(w, http.StatusNotFound, map[string]any{"code": 404, "message": "not found"})
			return
		}
		delete(f.groups, name)
		delete(f.memberships, groupID)
		f.deleteCounts["group:"+name]++
		writeJSON(w, http.StatusOK, map[string]any{"status": "deleted"})
	case r.Method == http.MethodDelete && strings.HasPrefix(r.URL.Path, "/v1/catalogs/lake/schemas/"):
		schemaName := strings.TrimPrefix(r.URL.Path, "/v1/catalogs/lake/schemas/")
		f.deleteCounts["schema:"+schemaName]++
		if f.failDeleteSchemaTrigger && schemaName == f.failDeleteSchemaName {
			f.failDeleteSchemaTrigger = false
			writeJSON(w, http.StatusInternalServerError, map[string]any{"code": 500, "message": "synthetic schema delete failure"})
			return
		}
		if _, ok := f.schemas[schemaName]; !ok {
			writeJSON(w, http.StatusNotFound, map[string]any{"code": 404, "message": "not found"})
			return
		}
		delete(f.schemas, schemaName)
		writeJSON(w, http.StatusOK, map[string]any{"status": "deleted"})
	case r.Method == http.MethodDelete && r.URL.Path == "/v1/catalogs/lake":
		if !f.catalogs["lake"] {
			writeJSON(w, http.StatusNotFound, map[string]any{"code": 404, "message": "not found"})
			return
		}
		delete(f.catalogs, "lake")
		f.deleteCounts["catalog:lake"]++
		writeJSON(w, http.StatusOK, map[string]any{"status": "deleted"})
	case r.Method == http.MethodDelete && strings.HasPrefix(r.URL.Path, "/v1/external-locations/"):
		name := strings.TrimPrefix(r.URL.Path, "/v1/external-locations/")
		if !f.locations[name] {
			writeJSON(w, http.StatusNotFound, map[string]any{"code": 404, "message": "not found"})
			return
		}
		delete(f.locations, name)
		f.deleteCounts["location:"+name]++
		writeJSON(w, http.StatusOK, map[string]any{"status": "deleted"})
	case r.Method == http.MethodDelete && strings.HasPrefix(r.URL.Path, "/v1/storage-credentials/"):
		name := strings.TrimPrefix(r.URL.Path, "/v1/storage-credentials/")
		if !f.credentials[name] {
			writeJSON(w, http.StatusNotFound, map[string]any{"code": 404, "message": "not found"})
			return
		}
		delete(f.credentials, name)
		f.deleteCounts["credential:"+name]++
		writeJSON(w, http.StatusOK, map[string]any{"status": "deleted"})
	default:
		writeJSON(w, http.StatusNotFound, map[string]any{"code": 404, "message": "not found"})
	}
}

func (f *fakeInitAPI) deleteGrant(grantID string) bool {
	for key, grant := range f.grants {
		if grant.ID != grantID {
			continue
		}
		delete(f.grants, key)
		name := f.principalNameForGrant(grant)
		schemaName := f.schemaNameByID(grant.SecurableID)
		f.deleteCounts["grant:"+grant.PrincipalType+":"+name+":"+schemaName+":"+grant.Privilege]++
		return true
	}
	return false
}

func (f *fakeInitAPI) principalNameForGrant(grant fakeGrant) string {
	if grant.PrincipalType == "group" {
		return f.groupNameForID(grant.PrincipalID)
	}
	return f.principalNameForID(grant.PrincipalID)
}

func (f *fakeInitAPI) groupNameForID(id string) string {
	name, _ := f.groupNameByID(id)
	return name
}

func (f *fakeInitAPI) principalNameForID(id string) string {
	name, _ := f.principalNameByID(id)
	return name
}

func (f *fakeInitAPI) groupNameByID(id string) (string, bool) {
	for name, groupID := range f.groups {
		if groupID == id {
			return name, true
		}
	}
	return "", false
}

func (f *fakeInitAPI) principalNameByID(id string) (string, bool) {
	for name, principalID := range f.principals {
		if principalID == id {
			return name, true
		}
	}
	return "", false
}

func (f *fakeInitAPI) schemaNameByID(id string) string {
	for name, schemaID := range f.schemas {
		if schemaID == id {
			return name
		}
	}
	return ""
}

func sortedNames(items map[string]bool) []string {
	names := make([]string, 0, len(items))
	for name := range items {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func sortedNamedIDs(items map[string]string) []map[string]string {
	names := make([]string, 0, len(items))
	for name := range items {
		names = append(names, name)
	}
	sort.Strings(names)
	rows := make([]map[string]string, 0, len(names))
	for _, name := range names {
		rows = append(rows, map[string]string{"id": items[name], "name": name})
	}
	return rows
}

func sortedMemberIDs(items map[string]bool) []string {
	memberIDs := make([]string, 0, len(items))
	for memberID := range items {
		memberIDs = append(memberIDs, memberID)
	}
	sort.Strings(memberIDs)
	return memberIDs
}

func sortedGrants(items map[string]fakeGrant) []fakeGrant {
	keys := make([]string, 0, len(items))
	for key := range items {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	grants := make([]fakeGrant, 0, len(keys))
	for _, key := range keys {
		grants = append(grants, items[key])
	}
	return grants
}

func membershipCount(items map[string]map[string]bool) int {
	total := 0
	for _, members := range items {
		total += len(members)
	}
	return total
}

func writeInitListJSON(w http.ResponseWriter, field string, names []string) {
	rows := make([]map[string]string, 0, len(names))
	for _, name := range names {
		rows = append(rows, map[string]string{"name": name})
	}
	writeJSON(w, http.StatusOK, map[string]any{field: rows})
}

func writeIDListJSON(w http.ResponseWriter, rows []map[string]string) {
	writeJSON(w, http.StatusOK, map[string]any{"data": rows})
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
