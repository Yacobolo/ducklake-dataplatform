package devseed

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"duck-demo/internal/declarative"
)

func TestEnsureDatasets_CachesDownloads(t *testing.T) {
	t.Parallel()

	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		_, _ = w.Write([]byte("seed-data"))
	}))
	defer server.Close()

	cacheDir := t.TempDir()
	specs := []DatasetSpec{{
		Name:     "taxi_trips",
		URL:      server.URL + "/yellow.parquet",
		FileName: "yellow.parquet",
	}}

	paths, err := EnsureDatasets(context.Background(), cacheDir, server.Client(), specs)
	require.NoError(t, err)
	require.FileExists(t, paths["taxi_trips"])

	pathsAgain, err := EnsureDatasets(context.Background(), cacheDir, server.Client(), specs)
	require.NoError(t, err)
	assert.Equal(t, paths["taxi_trips"], pathsAgain["taxi_trips"])
	assert.Equal(t, 1, requests)
}

func TestRenderDirectory_ReplacesPlaceholders(t *testing.T) {
	t.Parallel()

	inputDir := t.TempDir()
	outputDir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(inputDir, "nested"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(inputDir, "nested", "config.cue"), []byte("path: \"__PLACEHOLDER__\"\n"), 0o644))

	err := RenderDirectory(inputDir, outputDir, map[string]string{"__PLACEHOLDER__": "/tmp/data.parquet"})
	require.NoError(t, err)

	rendered, err := os.ReadFile(filepath.Join(outputDir, "nested", "config.cue"))
	require.NoError(t, err)
	assert.Equal(t, "path: \"/tmp/data.parquet\"\n", string(rendered))
}

func TestPrepare_RendersRepoDuckConfig(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch filepath.Base(r.URL.Path) {
		case "yellow_tripdata_2024-01.parquet":
			_, _ = w.Write([]byte("PAR1"))
		case "taxi_zone_lookup.csv":
			_, _ = w.Write([]byte("LocationID,Borough,Zone,service_zone\n1,Manhattan,Test,Yellow\n"))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	_, thisFile, _, _ := runtime.Caller(0)
	repoRoot := filepath.Join(filepath.Dir(thisFile), "..", "..")
	inputDir := filepath.Join(repoRoot, "duck-config")
	outputDir := filepath.Join(t.TempDir(), "rendered")
	cacheDir := filepath.Join(t.TempDir(), "cache")
	sampleCatalogDir := filepath.Join(t.TempDir(), "catalog")

	prepared, err := Prepare(context.Background(), PrepareOptions{
		InputDir:            inputDir,
		OutputDir:           outputDir,
		CacheDir:            cacheDir,
		SampleMetastorePath: filepath.Join(sampleCatalogDir, "ducklake_sample_data.sqlite"),
		SampleDataDir:       filepath.Join(sampleCatalogDir, "data"),
		BootstrapPrincipal:  "dev_admin",
		TaxiTripsURL:        server.URL + "/yellow_tripdata_2024-01.parquet",
		TaxiZonesURL:        server.URL + "/taxi_zone_lookup.csv",
		HTTPClient:          server.Client(),
	})
	require.NoError(t, err)
	require.DirExists(t, prepared.OutputDir)

	state, err := declarative.LoadDirectory(prepared.OutputDir)
	require.NoError(t, err)
	assert.Empty(t, declarative.Validate(state))
}
