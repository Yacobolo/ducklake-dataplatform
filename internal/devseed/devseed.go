// Package devseed downloads local dev datasets and renders the seeded config.
package devseed

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

const (
	// DefaultTaxiTripsURL points to the public TLC parquet used in seeded local dev.
	DefaultTaxiTripsURL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet"
	// DefaultTaxiZonesURL points to the public TLC taxi zone lookup CSV.
	DefaultTaxiZonesURL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"

	// PlaceholderSampleMetastore is replaced with the rendered local metastore path.
	PlaceholderSampleMetastore = "__QUACK_DEV_SAMPLE_METASTORE__"
	// PlaceholderSampleDataDir is replaced with the rendered local data directory path.
	PlaceholderSampleDataDir = "__QUACK_DEV_SAMPLE_DATA_DIR__"
	// PlaceholderTaxiTripsPath is replaced with the cached taxi trips parquet path.
	PlaceholderTaxiTripsPath = "__QUACK_DEV_TAXI_TRIPS__"
	// PlaceholderTaxiZonesPath is replaced with the cached taxi zone CSV path.
	PlaceholderTaxiZonesPath = "__QUACK_DEV_TAXI_ZONES__"
	// PlaceholderBootstrapUser is replaced with the bootstrap admin principal.
	PlaceholderBootstrapUser = "__QUACK_DEV_BOOTSTRAP_PRINCIPAL__"
)

// DatasetSpec defines one downloaded seed input.
type DatasetSpec struct {
	Name     string
	URL      string
	FileName string
}

// PrepareOptions configures local seed rendering.
type PrepareOptions struct {
	InputDir            string
	OutputDir           string
	CacheDir            string
	SampleMetastorePath string
	SampleDataDir       string
	BootstrapPrincipal  string
	TaxiTripsURL        string
	TaxiZonesURL        string
	HTTPClient          *http.Client
}

// PreparedConfig describes the rendered config and cached datasets.
type PreparedConfig struct {
	OutputDir           string
	SampleMetastorePath string
	SampleDataDir       string
	DatasetPaths        map[string]string
}

// Prepare renders the portable checked-in config into a local applyable directory.
func Prepare(ctx context.Context, opts PrepareOptions) (*PreparedConfig, error) {
	if strings.TrimSpace(opts.InputDir) == "" {
		return nil, fmt.Errorf("input dir is required")
	}
	if strings.TrimSpace(opts.OutputDir) == "" {
		return nil, fmt.Errorf("output dir is required")
	}
	if strings.TrimSpace(opts.CacheDir) == "" {
		return nil, fmt.Errorf("cache dir is required")
	}
	if strings.TrimSpace(opts.SampleMetastorePath) == "" {
		return nil, fmt.Errorf("sample metastore path is required")
	}
	if strings.TrimSpace(opts.SampleDataDir) == "" {
		return nil, fmt.Errorf("sample data dir is required")
	}
	if strings.TrimSpace(opts.BootstrapPrincipal) == "" {
		return nil, fmt.Errorf("bootstrap principal is required")
	}

	tripsURL := strings.TrimSpace(opts.TaxiTripsURL)
	if tripsURL == "" {
		tripsURL = DefaultTaxiTripsURL
	}
	zonesURL := strings.TrimSpace(opts.TaxiZonesURL)
	if zonesURL == "" {
		zonesURL = DefaultTaxiZonesURL
	}

	client := opts.HTTPClient
	if client == nil {
		client = http.DefaultClient
	}

	datasetPaths, err := EnsureDatasets(ctx, opts.CacheDir, client, []DatasetSpec{
		{Name: "taxi_trips", URL: tripsURL, FileName: "yellow_tripdata_2024-01.parquet"},
		{Name: "taxi_zones", URL: zonesURL, FileName: "taxi_zone_lookup.csv"},
	})
	if err != nil {
		return nil, err
	}

	if err := os.RemoveAll(opts.OutputDir); err != nil {
		return nil, fmt.Errorf("reset output dir: %w", err)
	}
	if err := os.MkdirAll(opts.OutputDir, 0o750); err != nil {
		return nil, fmt.Errorf("create output dir: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(opts.SampleMetastorePath), 0o750); err != nil {
		return nil, fmt.Errorf("create sample metastore parent: %w", err)
	}
	if err := os.MkdirAll(opts.SampleDataDir, 0o750); err != nil {
		return nil, fmt.Errorf("create sample data dir: %w", err)
	}

	replacements := map[string]string{
		PlaceholderSampleMetastore: filepath.ToSlash(opts.SampleMetastorePath),
		PlaceholderSampleDataDir:   filepath.ToSlash(opts.SampleDataDir),
		PlaceholderTaxiTripsPath:   filepath.ToSlash(datasetPaths["taxi_trips"]),
		PlaceholderTaxiZonesPath:   filepath.ToSlash(datasetPaths["taxi_zones"]),
		PlaceholderBootstrapUser:   opts.BootstrapPrincipal,
	}
	if err := RenderDirectory(opts.InputDir, opts.OutputDir, replacements); err != nil {
		return nil, err
	}

	return &PreparedConfig{
		OutputDir:           opts.OutputDir,
		SampleMetastorePath: opts.SampleMetastorePath,
		SampleDataDir:       opts.SampleDataDir,
		DatasetPaths:        datasetPaths,
	}, nil
}

// EnsureDatasets downloads datasets once into a worktree-local cache.
func EnsureDatasets(ctx context.Context, cacheDir string, client *http.Client, specs []DatasetSpec) (map[string]string, error) {
	if strings.TrimSpace(cacheDir) == "" {
		return nil, fmt.Errorf("cache dir is required")
	}
	if client == nil {
		client = http.DefaultClient
	}
	if err := os.MkdirAll(cacheDir, 0o750); err != nil {
		return nil, fmt.Errorf("create cache dir: %w", err)
	}

	paths := make(map[string]string, len(specs))
	for _, spec := range specs {
		if strings.TrimSpace(spec.Name) == "" {
			return nil, fmt.Errorf("dataset name is required")
		}
		if strings.TrimSpace(spec.URL) == "" {
			return nil, fmt.Errorf("dataset %q url is required", spec.Name)
		}
		fileName := strings.TrimSpace(spec.FileName)
		if fileName == "" {
			fileName = filepath.Base(spec.URL)
		}
		dstDir := filepath.Join(cacheDir, spec.Name)
		if err := os.MkdirAll(dstDir, 0o750); err != nil {
			return nil, fmt.Errorf("create dataset dir %q: %w", spec.Name, err)
		}
		dstPath := filepath.Join(dstDir, fileName)
		if _, err := os.Stat(dstPath); err == nil {
			absPath, absErr := filepath.Abs(dstPath)
			if absErr != nil {
				return nil, fmt.Errorf("resolve cached dataset path %q: %w", spec.Name, absErr)
			}
			paths[spec.Name] = absPath
			continue
		}

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, spec.URL, nil)
		if err != nil {
			return nil, fmt.Errorf("build request for dataset %q: %w", spec.Name, err)
		}
		resp, err := client.Do(req)
		if err != nil {
			return nil, fmt.Errorf("download dataset %q: %w", spec.Name, err)
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			_ = resp.Body.Close()
			return nil, fmt.Errorf("download dataset %q: unexpected status %s", spec.Name, resp.Status)
		}

		tmpPath := dstPath + ".tmp"
		// #nosec G304 -- tmpPath is derived from the managed cache directory and file name.
		file, err := os.Create(tmpPath)
		if err != nil {
			_ = resp.Body.Close()
			return nil, fmt.Errorf("create temp file for dataset %q: %w", spec.Name, err)
		}
		_, copyErr := io.Copy(file, resp.Body)
		closeErr := file.Close()
		bodyCloseErr := resp.Body.Close()
		if copyErr != nil {
			_ = os.Remove(tmpPath)
			return nil, fmt.Errorf("write dataset %q: %w", spec.Name, copyErr)
		}
		if closeErr != nil {
			_ = os.Remove(tmpPath)
			return nil, fmt.Errorf("close dataset %q: %w", spec.Name, closeErr)
		}
		if bodyCloseErr != nil {
			_ = os.Remove(tmpPath)
			return nil, fmt.Errorf("close response body for dataset %q: %w", spec.Name, bodyCloseErr)
		}
		if err := os.Rename(tmpPath, dstPath); err != nil {
			_ = os.Remove(tmpPath)
			return nil, fmt.Errorf("finalize dataset %q: %w", spec.Name, err)
		}

		absPath, err := filepath.Abs(dstPath)
		if err != nil {
			return nil, fmt.Errorf("resolve dataset path %q: %w", spec.Name, err)
		}
		paths[spec.Name] = absPath
	}

	return paths, nil
}

// RenderDirectory copies a config tree and replaces placeholder tokens.
func RenderDirectory(inputDir, outputDir string, replacements map[string]string) error {
	info, err := os.Stat(inputDir)
	if err != nil {
		return fmt.Errorf("stat input dir: %w", err)
	}
	if !info.IsDir() {
		return fmt.Errorf("input dir %q is not a directory", inputDir)
	}

	keys := make([]string, 0, len(replacements))
	for key := range replacements {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	return filepath.Walk(inputDir, func(path string, info os.FileInfo, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		relPath, err := filepath.Rel(inputDir, path)
		if err != nil {
			return fmt.Errorf("rel path for %q: %w", path, err)
		}
		if relPath == "." {
			return nil
		}
		dstPath := filepath.Join(outputDir, relPath)

		if info.IsDir() {
			if err := os.MkdirAll(dstPath, info.Mode().Perm()); err != nil {
				return fmt.Errorf("create dir %q: %w", dstPath, err)
			}
			return nil
		}

		// #nosec G304 -- path comes from walking the checked-in/local config tree.
		content, err := os.ReadFile(path)
		if err != nil {
			return fmt.Errorf("read %q: %w", path, err)
		}
		rendered := string(content)
		for _, key := range keys {
			rendered = strings.ReplaceAll(rendered, key, replacements[key])
		}
		if err := os.MkdirAll(filepath.Dir(dstPath), 0o750); err != nil {
			return fmt.Errorf("create parent dir for %q: %w", dstPath, err)
		}
		if err := os.WriteFile(dstPath, []byte(rendered), info.Mode().Perm()); err != nil {
			return fmt.Errorf("write %q: %w", dstPath, err)
		}
		return nil
	})
}
