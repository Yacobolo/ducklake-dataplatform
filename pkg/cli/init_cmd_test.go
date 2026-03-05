package cli

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBuildDesiredState_IncludesSandboxAndShowcase(t *testing.T) {
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

	assert.Contains(t, state.Schemas, "sandbox")
	assert.Equal(t, "rides_demo", state.Showcase.PipelineName)
	assert.Equal(t, "rides_raw", state.Showcase.RawTableName)
	assert.Equal(t, "rides_gold_daily_metrics", state.Showcase.GoldViewName)
	assert.Equal(t, "sandbox_getting_started", state.Showcase.SandboxSmokeTable)
}

func TestComputeInitPlan_ShowcaseAndPipelineMissing(t *testing.T) {
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
		Credentials: map[string]bool{desired.Credential.Name: true},
		Locations:   map[string]bool{},
		Catalogs:    map[string]bool{desired.CatalogName: true},
		Schemas: map[string]string{
			"landing": "1",
			"bronze":  "2",
			"silver":  "3",
			"gold":    "4",
			"sandbox": "5",
		},
		Tables:      map[string]map[string]bool{},
		Views:       map[string]map[string]bool{},
		Pipelines:   map[string]bool{},
		Groups:      map[string]string{},
		Principals:  map[string]string{},
		Memberships: map[string]map[string]bool{},
		Grants:      map[string]bool{},
		GrantIDs:    map[string]string{},
	}

	plan := computeInitPlan(desired, existing)
	require.NotEmpty(t, plan.Creates)
	assert.Contains(t, plan.Creates, `showcase table "landing"."rides_raw"`)
	assert.Contains(t, plan.Creates, `showcase view "gold"."rides_gold_daily_metrics"`)
	assert.Contains(t, plan.Creates, `pipeline "rides_demo"`)
}
