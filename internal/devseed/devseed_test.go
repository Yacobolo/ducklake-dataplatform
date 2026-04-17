package devseed

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	apitypes "github.com/Yacobolo/quackstack/internal/api"
	"github.com/Yacobolo/quackstack/internal/declarative"
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
	inputDir := filepath.Join(repoRoot, "quackstack-config")
	outputDir := filepath.Join(t.TempDir(), "rendered")
	cacheDir := filepath.Join(t.TempDir(), "cache")
	sampleCatalogDir := filepath.Join(t.TempDir(), "catalog")

	prepared, err := Prepare(context.Background(), PrepareOptions{
		InputDir:            inputDir,
		OutputDir:           outputDir,
		CacheDir:            cacheDir,
		SampleMetastorePath: filepath.Join(sampleCatalogDir, "quackstack_sample_data.sqlite"),
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

func TestSeedPipelines_CreatesDemoPipelineAndInitialRun(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "config.yaml")
	t.Setenv("QUACK_CONFIG_FILE", configPath)

	require.NoError(t, os.WriteFile(configPath, []byte(`
current-profile: seeded
profiles:
  seeded:
    host: __HOST__
    token: seed-token
`), 0o600))

	notebookID := "nb-seeded"
	folderID := "folder-analytics"
	pipelineID := "pipe-seeded"
	jobID := "job-seeded"
	runID := "run-seeded"

	pipelineCreated := false
	jobCreated := false
	runCreated := false

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !assert.Equal(t, "Bearer seed-token", r.Header.Get("Authorization")) {
			http.Error(w, "unexpected authorization header", http.StatusUnauthorized)
			return
		}

		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/notebooks":
			writeJSON(t, w, apitypes.PaginatedNotebooks{
				Data: []apitypes.Notebook{{
					Id:       strPtr(notebookID),
					Name:     strPtr("nyc_taxi_explore"),
					Owner:    strPtr("dev_admin"),
					FolderId: strPtr(folderID),
				}},
			})
		case r.Method == http.MethodGet && r.URL.Path == "/v1/pipelines/nyc_taxi_demo":
			if !pipelineCreated {
				w.WriteHeader(http.StatusNotFound)
				writeJSON(t, w, map[string]any{"code": 404, "message": "not found"})
				return
			}
			writeJSON(t, w, apitypes.Pipeline{
				Id:       strPtr(pipelineID),
				Name:     strPtr("nyc_taxi_demo"),
				FolderId: strPtr(folderID),
			})
		case r.Method == http.MethodPost && r.URL.Path == "/v1/pipelines":
			var req apitypes.CreatePipelineRequest
			if !assert.NoError(t, json.NewDecoder(r.Body).Decode(&req)) {
				http.Error(w, "invalid pipeline payload", http.StatusBadRequest)
				return
			}
			if !assert.Equal(t, "nyc_taxi_demo", req.Name) {
				http.Error(w, "unexpected pipeline name", http.StatusBadRequest)
				return
			}
			if !assert.Equal(t, folderID, derefString(req.FolderId)) {
				http.Error(w, "unexpected pipeline folder", http.StatusBadRequest)
				return
			}
			pipelineCreated = true
			writeJSON(t, w, apitypes.Pipeline{
				Id:       strPtr(pipelineID),
				Name:     strPtr(req.Name),
				FolderId: req.FolderId,
			})
		case r.Method == http.MethodGet && r.URL.Path == "/v1/pipelines/nyc_taxi_demo/jobs":
			if !jobCreated {
				writeJSON(t, w, apitypes.PipelineJobList{Data: []apitypes.PipelineJob{}})
				return
			}
			writeJSON(t, w, apitypes.PipelineJobList{Data: []apitypes.PipelineJob{{
				Id:         strPtr(jobID),
				Name:       strPtr("run_nyc_taxi_explore"),
				NotebookId: strPtr(notebookID),
			}}})
		case r.Method == http.MethodPost && r.URL.Path == "/v1/pipelines/nyc_taxi_demo/jobs":
			var req apitypes.CreatePipelineJobRequest
			if !assert.NoError(t, json.NewDecoder(r.Body).Decode(&req)) {
				http.Error(w, "invalid pipeline job payload", http.StatusBadRequest)
				return
			}
			if !assert.Equal(t, "run_nyc_taxi_explore", req.Name) {
				http.Error(w, "unexpected pipeline job name", http.StatusBadRequest)
				return
			}
			if !assert.Equal(t, notebookID, derefString(req.NotebookId)) {
				http.Error(w, "unexpected pipeline notebook", http.StatusBadRequest)
				return
			}
			jobCreated = true
			writeJSON(t, w, apitypes.PipelineJob{
				Id:         strPtr(jobID),
				Name:       strPtr(req.Name),
				NotebookId: req.NotebookId,
			})
		case r.Method == http.MethodGet && r.URL.Path == "/v1/pipelines/nyc_taxi_demo/runs":
			if !runCreated {
				writeJSON(t, w, apitypes.PaginatedPipelineRuns{Data: []apitypes.PipelineRun{}})
				return
			}
			writeJSON(t, w, apitypes.PaginatedPipelineRuns{Data: []apitypes.PipelineRun{{
				Id: strPtr(runID),
			}}})
		case r.Method == http.MethodPost && r.URL.Path == "/v1/pipelines/nyc_taxi_demo/runs":
			runCreated = true
			writeJSON(t, w, apitypes.PipelineRun{Id: strPtr(runID)})
		default:
			t.Fatalf("unexpected %s %s", r.Method, r.URL.String())
		}
	}))
	defer server.Close()

	configContents, err := os.ReadFile(configPath)
	require.NoError(t, err)
	configContents = []byte(strings.ReplaceAll(string(configContents), "__HOST__", server.URL))
	require.NoError(t, os.WriteFile(configPath, configContents, 0o600))

	seeded, err := SeedPipelines(context.Background(), SeedPipelinesOptions{
		Profile:    "seeded",
		TriggerRun: true,
	})
	require.NoError(t, err)
	assert.Equal(t, notebookID, seeded.NotebookID)
	assert.Equal(t, pipelineID, seeded.PipelineID)
	assert.Equal(t, jobID, seeded.JobID)
	assert.Equal(t, runID, seeded.RunID)
	assert.True(t, pipelineCreated)
	assert.True(t, jobCreated)
	assert.True(t, runCreated)
}

func writeJSON(t *testing.T, w http.ResponseWriter, payload any) {
	t.Helper()
	w.Header().Set("Content-Type", "application/json")
	require.NoError(t, json.NewEncoder(w).Encode(payload))
}

func derefString(v *string) string {
	if v == nil {
		return ""
	}
	return *v
}

func strPtr(v string) *string {
	return &v
}
