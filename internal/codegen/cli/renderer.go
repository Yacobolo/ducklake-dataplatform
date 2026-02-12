package cli

import (
	"bytes"
	"embed"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"text/template"

	"golang.org/x/tools/imports"
)

//go:embed templates/*.tmpl
var templateFS embed.FS

// Render generates all Go source files from the model into outDir.
func Render(groups []GroupModel, cfg *CLIConfig, outDir string) error {
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return fmt.Errorf("create output dir: %w", err)
	}

	funcMap := template.FuncMap{
		"toKebab":    toKebabCase,
		"toPascal":   toPascalCase,
		"toCamel":    toCamelCase,
		"toLower":    strings.ToLower,
		"toUpper":    strings.ToUpper,
		"title":      strings.Title, //nolint:staticcheck
		"join":       strings.Join,
		"contains":   strings.Contains,
		"hasPrefix":  strings.HasPrefix,
		"hasSuffix":  strings.HasSuffix,
		"trimPrefix": strings.TrimPrefix,
		"trimSuffix": strings.TrimSuffix,
		"replace":    strings.ReplaceAll,
		"quote":      func(s string) string { return fmt.Sprintf("%q", s) },
		"add":        func(a, b int) int { return a + b },
		"sub":        func(a, b int) int { return a - b },
		"seq": func(n int) []int {
			s := make([]int, n)
			for i := range s {
				s[i] = i
			}
			return s
		},
		"last": func(i int, s interface{}) bool {
			switch v := s.(type) {
			case []CommandModel:
				return i == len(v)-1
			case []FlagModel:
				return i == len(v)-1
			case []string:
				return i == len(v)-1
			case []GroupModel:
				return i == len(v)-1
			default:
				return false
			}
		},
		"isPaginatedList":  func(p ResponsePattern) bool { return p == PaginatedList },
		"isSingleResource": func(p ResponsePattern) bool { return p == SingleResource },
		"isNoContent":      func(p ResponsePattern) bool { return p == NoContent },
		"isCustomResult":   func(p ResponsePattern) bool { return p == CustomResult },
		"defaultValue": func(f FlagModel) string {
			switch f.CobraType {
			case "Bool":
				if f.Default == "true" {
					return "true"
				}
				return "false"
			case "Int64":
				if f.Default != "" {
					return f.Default
				}
				return "0"
			case "Float64":
				if f.Default != "" {
					return f.Default
				}
				return "0"
			default:
				return f.Default
			}
		},
		"pathTemplate": func(urlPath string) string {
			// Convert /catalog/schemas/{schemaName} to fmt.Sprintf pattern
			result := urlPath
			return result
		},
		"groupCommands": func(commands []CommandModel) map[string][]CommandModel {
			m := make(map[string][]CommandModel)
			for _, cmd := range commands {
				key := ""
				if len(cmd.CommandPath) > 0 {
					key = cmd.CommandPath[0]
				}
				m[key] = append(m[key], cmd)
			}
			return m
		},
		"uniqueSubcommands": func(commands []CommandModel) []string {
			seen := map[string]bool{}
			var result []string
			for _, cmd := range commands {
				if len(cmd.CommandPath) > 0 {
					key := cmd.CommandPath[0]
					if !seen[key] {
						seen[key] = true
						result = append(result, key)
					}
				}
			}
			sort.Strings(result)
			return result
		},
	}

	tmpl, err := template.New("").Funcs(funcMap).ParseFS(templateFS, "templates/*.tmpl")
	if err != nil {
		return fmt.Errorf("parse templates: %w", err)
	}

	type renderJob struct {
		tmplName string
		fileName string
		data     interface{}
	}

	jobs := []renderJob{
		{"overrides.go.tmpl", "overrides.gen.go", nil},
		{"client.go.tmpl", "client.gen.go", nil},
		{"output.go.tmpl", "output.gen.go", cfg},
		{"pagination.go.tmpl", "pagination.gen.go", nil},
		{"root.go.tmpl", "root.gen.go", groups},
	}

	// One file per group
	for _, g := range groups {
		jobs = append(jobs, renderJob{
			tmplName: "group.go.tmpl",
			fileName: g.Name + ".gen.go",
			data:     g,
		})
	}

	for _, job := range jobs {
		var buf bytes.Buffer
		if err := tmpl.ExecuteTemplate(&buf, job.tmplName, job.data); err != nil {
			return fmt.Errorf("execute template %s: %w", job.tmplName, err)
		}

		// Run goimports
		formatted, err := imports.Process(job.fileName, buf.Bytes(), nil)
		if err != nil {
			// Write unformatted for debugging
			outPath := filepath.Join(outDir, job.fileName)
			_ = os.WriteFile(outPath, buf.Bytes(), 0o644)
			return fmt.Errorf("goimports %s: %w", job.fileName, err)
		}

		outPath := filepath.Join(outDir, job.fileName)
		if err := os.WriteFile(outPath, formatted, 0o644); err != nil {
			return fmt.Errorf("write %s: %w", outPath, err)
		}
	}

	return nil
}
