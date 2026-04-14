package cli

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"

	"github.com/spf13/cobra"

	"github.com/Yacobolo/quackstack/internal/declarative"
	"github.com/Yacobolo/quackstack/pkg/cli/apiruntime"
)

var declarativeSlugPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]*$`)

type declarativeInitOptions struct {
	name      string
	workspace string
	owner     string
	template  string
	outputDir string
	module    string
	force     bool
}

func newDeclarativeInitCmd() *cobra.Command {
	opts := declarativeInitOptions{
		name:      "analytics",
		workspace: "main",
		owner:     "changeme",
		template:  "analytics",
		outputDir: "./quackstack-config",
	}

	cmd := &cobra.Command{
		Use:   "init",
		Short: "Generate a starter declarative CUE module",
		Long:  "Creates a starter declarative CUE module with a browsable workspace/project layout and validates it immediately.",
		RunE: func(cmd *cobra.Command, _ []string) error {
			resolved, err := resolveDeclarativeInitOptions(opts)
			if err != nil {
				return err
			}

			files, err := renderDeclarativeTemplate(resolved)
			if err != nil {
				return err
			}

			if err := ensureDeclarativeInitTarget(resolved.outputDir, resolved.force); err != nil {
				return err
			}

			if err := writeDeclarativeFiles(resolved.outputDir, files); err != nil {
				return err
			}

			desired, err := declarative.LoadDirectory(resolved.outputDir)
			if err != nil {
				return fmt.Errorf("validate generated scaffold: %w", err)
			}
			if validationErrs := declarative.Validate(desired); len(validationErrs) > 0 {
				errs := make([]string, 0, len(validationErrs))
				for _, validationErr := range validationErrs {
					errs = append(errs, validationErr.Error())
				}
				return fmt.Errorf("validate generated scaffold: %s", strings.Join(errs, "; "))
			}

			absPath, err := filepath.Abs(resolved.outputDir)
			if err != nil {
				absPath = resolved.outputDir
			}

			payload := map[string]any{
				"status":    "ok",
				"path":      absPath,
				"template":  resolved.template,
				"name":      resolved.name,
				"workspace": resolved.workspace,
				"module":    resolved.module,
			}
			if getOutputFormat(cmd) == "json" {
				return apiruntime.PrintJSON(os.Stdout, payload)
			}

			_, _ = fmt.Fprintf(os.Stdout, "Wrote declarative scaffold to %s (%s template)\n", absPath, resolved.template)
			return nil
		},
	}

	cmd.Flags().StringVar(&opts.name, "name", opts.name, "Project name slug")
	cmd.Flags().StringVar(&opts.workspace, "workspace", opts.workspace, "Workspace name slug")
	cmd.Flags().StringVar(&opts.owner, "owner", opts.owner, "Owner principal name")
	cmd.Flags().StringVar(&opts.template, "template", opts.template, "Template to generate (minimal, analytics)")
	cmd.Flags().StringVar(&opts.outputDir, "output-dir", opts.outputDir, "Output directory")
	cmd.Flags().StringVar(&opts.module, "module", opts.module, "CUE module name (defaults to quackstack.local/<output-or-name>)")
	cmd.Flags().BoolVar(&opts.force, "force", false, "Overwrite output directory if it exists")

	return cmd
}

func resolveDeclarativeInitOptions(opts declarativeInitOptions) (declarativeInitOptions, error) {
	resolved := opts
	resolved.name = strings.TrimSpace(strings.ToLower(resolved.name))
	resolved.workspace = strings.TrimSpace(strings.ToLower(resolved.workspace))
	resolved.owner = strings.TrimSpace(resolved.owner)
	resolved.template = strings.TrimSpace(strings.ToLower(resolved.template))
	resolved.outputDir = strings.TrimSpace(resolved.outputDir)
	resolved.module = strings.TrimSpace(resolved.module)

	if resolved.outputDir == "" {
		return declarativeInitOptions{}, fmt.Errorf("--output-dir is required")
	}
	if !declarativeSlugPattern.MatchString(resolved.name) {
		return declarativeInitOptions{}, fmt.Errorf("invalid --name %q: use lowercase letters, numbers, hyphen, or underscore", resolved.name)
	}
	if !declarativeSlugPattern.MatchString(resolved.workspace) {
		return declarativeInitOptions{}, fmt.Errorf("invalid --workspace %q: use lowercase letters, numbers, hyphen, or underscore", resolved.workspace)
	}
	if strings.TrimSpace(resolved.owner) == "" {
		return declarativeInitOptions{}, fmt.Errorf("--owner is required")
	}

	switch resolved.template {
	case "minimal", "analytics":
	default:
		return declarativeInitOptions{}, fmt.Errorf("invalid --template %q (expected minimal or analytics)", resolved.template)
	}

	if resolved.module == "" {
		moduleSeed := resolved.name
		if base := filepath.Base(resolved.outputDir); base != "" && base != "." && base != string(filepath.Separator) {
			moduleSeed = strings.ToLower(base)
		}
		moduleSeed = strings.ReplaceAll(moduleSeed, " ", "-")
		moduleSeed = strings.Trim(moduleSeed, "/")
		if !declarativeSlugPattern.MatchString(moduleSeed) {
			moduleSeed = resolved.name
		}
		resolved.module = "quackstack.local/" + moduleSeed
	}

	return resolved, nil
}

func ensureDeclarativeInitTarget(outputDir string, force bool) error {
	info, err := os.Stat(outputDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("stat output directory: %w", err)
	}
	if !info.IsDir() {
		return fmt.Errorf("%s already exists and is not a directory", outputDir)
	}

	entries, err := os.ReadDir(outputDir)
	if err != nil {
		return fmt.Errorf("read output directory: %w", err)
	}
	if len(entries) == 0 {
		return nil
	}
	if !force {
		return fmt.Errorf("%s already exists and is not empty; use --force to overwrite", outputDir)
	}
	if err := os.RemoveAll(outputDir); err != nil {
		return fmt.Errorf("remove %s: %w", outputDir, err)
	}
	return nil
}

func writeDeclarativeFiles(outputDir string, files map[string]string) error {
	paths := make([]string, 0, len(files))
	for path := range files {
		paths = append(paths, path)
	}
	sort.Strings(paths)

	for _, relPath := range paths {
		targetPath := filepath.Join(outputDir, relPath)
		if err := os.MkdirAll(filepath.Dir(targetPath), 0o750); err != nil {
			return fmt.Errorf("create %s: %w", filepath.Dir(targetPath), err)
		}
		if err := os.WriteFile(targetPath, []byte(files[relPath]), 0o600); err != nil {
			return fmt.Errorf("write %s: %w", targetPath, err)
		}
	}
	return nil
}

func renderDeclarativeTemplate(opts declarativeInitOptions) (map[string]string, error) {
	switch opts.template {
	case "minimal":
		return renderMinimalDeclarativeTemplate(opts), nil
	case "analytics":
		return renderAnalyticsDeclarativeTemplate(opts), nil
	default:
		return nil, fmt.Errorf("unsupported template %q", opts.template)
	}
}

func renderMinimalDeclarativeTemplate(opts declarativeInitOptions) map[string]string {
	projectRef := opts.workspace + "/" + opts.name
	envRef := projectRef + "/dev"

	return map[string]string{
		"README.md": fmt.Sprintf("# Declarative Scaffold\n\n"+
			"This module was generated by `quack declarative init`.\n\n"+
			"Starter conventions:\n\n"+
			"- `workspaces/` owns folders, notebooks, and dashboards\n"+
			"- `projects/` owns environments, models, macros, and semantic models\n"+
			"- refs stay explicit even when the path implies ownership\n\n"+
			"Quick start:\n\n"+
			"```bash\n"+
			"quack validate --config-dir %s\n"+
			"quack plan --config-dir %s\n"+
			"```\n",
			opts.outputDir, opts.outputDir),
		"cue.mod/module.cue": fmt.Sprintf("module: %q\nlanguage: {\n\tversion: \"v0.14.0\"\n}\n", opts.module),
		filepath.Join("workspaces", opts.workspace, "workspace.cue"): fmt.Sprintf(`package duckconfig

platform: workspaces: %q: {
	kind:                    "personal"
	owner_principal:         %q
	default_project_ref:     %q
	default_environment_ref: %q
}
`, opts.workspace, opts.owner, projectRef, envRef),
		filepath.Join("projects", opts.name, "project.cue"): fmt.Sprintf(`package duckconfig

platform: projects: %q: {
	workspace_ref:  %q
	kind:           "personal"
	description:    "Starter declarative project"
	default_branch: "main"
	environments: dev: {
		project_ref:      %q
		kind:             "development"
		description:      "Starter development environment"
		target_catalog:   "main"
		target_schema:    "default"
	}
}
`, opts.name, opts.workspace, projectRef),
	}
}

func renderAnalyticsDeclarativeTemplate(opts declarativeInitOptions) map[string]string {
	projectRef := opts.workspace + "/" + opts.name
	envRef := projectRef + "/dev"
	notebookName := opts.name + "_explore"
	modelName := opts.name + "_daily_summary"
	dashboardName := opts.name + "-ops"

	return map[string]string{
		"README.md": fmt.Sprintf("# Declarative Analytics Scaffold\n\n"+
			"This module was generated by `quack declarative init --template analytics`.\n\n"+
			"Layout:\n\n"+
			"- `workspaces/%s` contains namespace-facing content\n"+
			"- `projects/%s` contains execution and modeling content\n\n"+
			"Quick start:\n\n"+
			"```bash\n"+
			"quack validate --config-dir %s\n"+
			"quack plan --config-dir %s\n"+
			"quack apply --config-dir %s --auto-approve\n"+
			"```\n",
			opts.workspace, opts.name, opts.outputDir, opts.outputDir, opts.outputDir),
		"cue.mod/module.cue": fmt.Sprintf("module: %q\nlanguage: {\n\tversion: \"v0.14.0\"\n}\n", opts.module),
		filepath.Join("workspaces", opts.workspace, "defs.cue"): fmt.Sprintf(`package duckconfig

#Owner:        %q
#ProjectRef:   %q
#EnvRef:       %q
#NotebookName: %q
`, opts.owner, projectRef, envRef, notebookName),
		filepath.Join("workspaces", opts.workspace, "workspace.cue"): fmt.Sprintf(`package duckconfig

platform: workspaces: %q: {
	kind:                    "personal"
	owner_principal:         #Owner
	default_project_ref:     #ProjectRef
	default_environment_ref: #EnvRef
}
`, opts.workspace),
		filepath.Join("workspaces", opts.workspace, "folders.cue"): fmt.Sprintf(`package duckconfig

platform: workspaces: %q: folders: analysis: {
	default_project_ref:     #ProjectRef
	default_environment_ref: #EnvRef
	notebooks: {
		%q: {
			description:     "Starter analytics notebook"
			owner:           #Owner
			project_ref:     #ProjectRef
			environment_ref: #EnvRef
			cells: [{
				type: "markdown"
				content: """
					# Starter Analytics Notebook

					Replace this scaffold with your team's exploratory workflow.
					"""
			}, {
				type: "sql"
				name: "daily_summary"
				role: "output"
				content: """
					SELECT
					  DATE '2026-01-01' AS service_date,
					  42 AS metric_value
					"""
			}]
		}
	}
}
`, opts.workspace, notebookName),
		filepath.Join("workspaces", opts.workspace, "dashboards.cue"): fmt.Sprintf(`package duckconfig

platform: workspaces: %q: dashboards: %q: {
	description:           "Starter analytics dashboard"
	owner:                 #Owner
	semantic_project_name: %q
	semantic_model_name:   %q
	compute: mode: "AUTO"
	widgets: [{
		key:       "daily-summary"
		page_name: "Overview"
		name:      "Daily Summary"
		source: {
			kind: "notebook_cell"
			notebook_cell: {
				notebook_name: #NotebookName
				cell_name:     "daily_summary"
			}
		}
		layout: {
			x: 0
			y: 0
			w: 6
			h: 4
		}
	}]
}
`, opts.workspace, dashboardName, opts.name, modelName),
		filepath.Join("projects", opts.name, "defs.cue"): fmt.Sprintf(`package duckconfig

#WorkspaceRef: %q
#ProjectRef:   %q
#TargetCatalog: "main"
#TargetSchema:  "default"
`, opts.workspace, projectRef),
		filepath.Join("projects", opts.name, "project.cue"): fmt.Sprintf(`package duckconfig

platform: projects: %q: {
	workspace_ref:  #WorkspaceRef
	kind:           "personal"
	description:    "Starter analytics project"
	default_branch: "main"
	environments: dev: {
		project_ref:      #ProjectRef
		kind:             "development"
		description:      "Starter development environment"
		target_catalog:   #TargetCatalog
		target_schema:    #TargetSchema
	}
	macros: safe_divide: {
		macro_type: "SCALAR"
		parameters: [
			"numerator",
			"denominator",
		]
		body:        "CASE WHEN denominator = 0 THEN NULL ELSE numerator / denominator END"
		description: "Starter defensive SQL macro"
		owner:       %q
	}
}
`, opts.name, opts.owner),
		filepath.Join("projects", opts.name, "models.cue"): fmt.Sprintf(`package duckconfig

platform: projects: %q: {
	models: %q: {
		materialization: "VIEW"
		description:     "Starter model for semantic and dashboard wiring"
		tags: [
			"starter",
			"analytics",
		]
		sql: """
			SELECT
			  DATE '2026-01-01' AS service_date,
			  'all' AS segment,
			  42 AS metric_value
			"""
	}
	semantic_models: %q: {
		description:            "Starter semantic model"
		base_model_ref:         %q
		default_time_dimension: "service_date"
		tags: [
			"starter",
			"semantic",
		]
		metrics: [{
			name:            "metric_value"
			metric_type:     "SUM"
			expression_mode: "DSL"
			expression:      "metric_value"
		}]
	}
}
`, opts.name, modelName, modelName, modelName),
	}
}
