// Package main renders the checked-in dev seed config into a local applyable tree.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/Yacobolo/quackstack/internal/devseed"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "fatal: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	if len(os.Args) < 2 {
		return fmt.Errorf("usage: devseed prepare [flags]")
	}

	switch os.Args[1] {
	case "prepare":
		return runPrepare(os.Args[2:])
	default:
		return fmt.Errorf("unknown command %q", os.Args[1])
	}
}

func runPrepare(args []string) error {
	fs := flag.NewFlagSet("prepare", flag.ContinueOnError)
	inputDir := fs.String("input-dir", "./quackstack-config", "Path to the checked-in seed config")
	outputDir := fs.String("output-dir", "", "Path to the rendered config directory")
	cacheDir := fs.String("cache-dir", "", "Path to the dataset cache directory")
	sampleMetastorePath := fs.String("sample-metastore-path", "", "Path to the seed catalog metastore sqlite file")
	sampleDataDir := fs.String("sample-data-dir", "", "Path to the seed catalog data directory")
	bootstrapPrincipal := fs.String("bootstrap-principal", "dev_admin", "Bootstrap principal name used in the seed config")
	taxiTripsURL := fs.String("taxi-trips-url", devseed.DefaultTaxiTripsURL, "NYC taxi parquet download URL")
	taxiZonesURL := fs.String("taxi-zones-url", devseed.DefaultTaxiZonesURL, "NYC taxi zones CSV download URL")
	fs.SetOutput(os.Stderr)
	if err := fs.Parse(args); err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()

	prepared, err := devseed.Prepare(ctx, devseed.PrepareOptions{
		InputDir:            *inputDir,
		OutputDir:           *outputDir,
		CacheDir:            *cacheDir,
		SampleMetastorePath: *sampleMetastorePath,
		SampleDataDir:       *sampleDataDir,
		BootstrapPrincipal:  *bootstrapPrincipal,
		TaxiTripsURL:        *taxiTripsURL,
		TaxiZonesURL:        *taxiZonesURL,
	})
	if err != nil {
		return err
	}

	fmt.Printf("rendered_config_dir=%s\n", prepared.OutputDir)
	fmt.Printf("sample_metastore_path=%s\n", prepared.SampleMetastorePath)
	fmt.Printf("sample_data_dir=%s\n", prepared.SampleDataDir)
	for name, path := range prepared.DatasetPaths {
		fmt.Printf("dataset_%s=%s\n", name, path)
	}
	return nil
}
