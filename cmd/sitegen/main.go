package main

import (
	"flag"
	"fmt"
	"log"
	"os"

	"duck-demo/internal/sitegen"
)

func main() {
	contentDir := flag.String("content-dir", "site/content", "path to site content")
	configPath := flag.String("config", "site/config/site.yaml", "path to site config")
	navPath := flag.String("nav", "site/config/navigation.yaml", "path to site navigation config")
	templatesDir := flag.String("templates-dir", "site/templates", "path to site templates")
	assetsDir := flag.String("assets-dir", "site/assets", "path to site assets")
	outDir := flag.String("out-dir", "dist/site", "path to static site output")
	baseURL := flag.String("base-url", "", "public base URL for sitemap")
	serve := flag.Bool("serve", false, "serve the generated site locally")
	addr := flag.String("addr", ":4080", "address for local site server")
	flag.Parse()

	builder := sitegen.Builder{
		ContentDir:   *contentDir,
		ConfigPath:   *configPath,
		NavPath:      *navPath,
		TemplatesDir: *templatesDir,
		AssetsDir:    *assetsDir,
		OutDir:       *outDir,
		BaseURL:      *baseURL,
	}

	if err := builder.Build(); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "error: build site: %v\n", err)
		os.Exit(1)
	}

	if !*serve {
		return
	}

	log.Printf("serving site at http://localhost%s", *addr)
	if err := sitegen.Serve(*addr, *outDir); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "error: serve site: %v\n", err)
		os.Exit(1)
	}
}
