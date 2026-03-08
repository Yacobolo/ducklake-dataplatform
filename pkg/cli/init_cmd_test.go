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
	assert.Equal(t, "rides_raw", state.Showcase.RawTableName)
	assert.Equal(t, "rides_gold_daily_metrics", state.Showcase.GoldTableName)
	assert.Equal(t, "sandbox_getting_started", state.Showcase.SandboxSmokeTable)
	assert.Len(t, state.Showcase.Assets, 6)
	assert.Equal(t, "showcase.rides.gold", state.Showcase.Assets[3].AssetKey)
	assert.Equal(t, []string{"showcase.rides.silver"}, state.Showcase.Assets[3].UpstreamAssetKey)
	assert.Len(t, state.Showcase.Assets[3].Checks, 1)
}

func TestComputeInitPlan_ShowcaseAssetsMissing(t *testing.T) {
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
		Assets:      map[string]initAssetState{},
		Groups:      map[string]string{},
		Principals:  map[string]string{},
		Memberships: map[string]map[string]bool{},
		Grants:      map[string]bool{},
		GrantIDs:    map[string]string{},
	}

	plan := computeInitPlan(desired, existing)
	require.NotEmpty(t, plan.Creates)
	assert.Contains(t, plan.Creates, `showcase table "landing"."rides_raw"`)
	assert.Contains(t, plan.Creates, `showcase table "gold"."rides_gold_daily_metrics"`)
	assert.Contains(t, plan.Creates, `asset "showcase.rides.gold"`)
	assert.Contains(t, plan.Updates, `asset graph "showcase.rides.gold"`)
	assert.Contains(t, plan.Updates, `asset checks "showcase.rides.gold"`)
}

func TestComputeInitPlan_ShowcaseAssetDrift(t *testing.T) {
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
		Schemas:     map[string]string{},
		Tables:      map[string]map[string]bool{},
		Views:       map[string]map[string]bool{},
		Assets: map[string]initAssetState{
			"showcase.rides.gold": {
				AssetType:        "TABLE",
				Owner:            "analytics",
				Description:      "stale description",
				Tags:             []string{"gold", "rides", "showcase"},
				IOProfile:        "duckdb",
				IsActive:         true,
				UpstreamAssetKey: []string{"showcase.rides.silver"},
				Checks: []initAssetCheckSpec{{
					Name:      "gold_non_empty",
					CheckType: "SQL_ASSERT",
					Severity:  "WARN",
					Enabled:   true,
				}},
			},
		},
		Groups:      map[string]string{},
		Principals:  map[string]string{},
		Memberships: map[string]map[string]bool{},
		Grants:      map[string]bool{},
		GrantIDs:    map[string]string{},
	}

	plan := computeInitPlan(desired, existing)
	assert.Contains(t, plan.Updates, `asset "showcase.rides.gold"`)
	assert.Contains(t, plan.Updates, `asset checks "showcase.rides.gold"`)
}

func TestAssetHealthIssues_ReportsMetadataGraphAndChecksDrift(t *testing.T) {
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

	issues := assetHealthIssues(desired.Showcase.Assets, map[string]initAssetState{
		"showcase.rides.gold": {
			AssetType:        "TABLE",
			Owner:            "analytics",
			Description:      "wrong",
			Tags:             []string{"gold", "rides", "showcase"},
			IOProfile:        "duckdb",
			IsActive:         true,
			UpstreamAssetKey: []string{"showcase.rides.bronze"},
			Checks: []initAssetCheckSpec{{
				Name:      "gold_non_empty",
				CheckType: "SQL_ASSERT",
				Severity:  "WARN",
				Enabled:   true,
			}},
		},
	})

	assert.Contains(t, issues, `asset "showcase.rides.raw" is missing`)
	assert.Contains(t, issues, `asset "showcase.rides.gold" metadata drifted`)
	assert.Contains(t, issues, `asset "showcase.rides.gold" graph drifted`)
	assert.Contains(t, issues, `asset "showcase.rides.gold" checks drifted`)
}
