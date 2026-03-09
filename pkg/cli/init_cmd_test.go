package cli

import (
	"testing"

	"github.com/stretchr/testify/assert"
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
