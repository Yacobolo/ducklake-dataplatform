package sitegen

import (
	"fmt"
	"html/template"
	"strings"
	"unicode"
)

type directiveRenderer struct {
	templates *template.Template
}

func newDirectiveRenderer(templates templateSet) directiveRenderer {
	return directiveRenderer{templates: templates.base}
}

func (r directiveRenderer) render(name string, data any) (string, error) {
	if r.templates == nil {
		return "", fmt.Errorf("directive renderer not configured")
	}
	templateName := "directive_" + strings.ReplaceAll(name, "-", "_")
	return executeNamedTemplate(r.templates, templateName, data)
}

type directiveCalloutData struct {
	Title    string
	BodyHTML template.HTML
}

type directiveDetailsData struct {
	Title    string
	BodyHTML template.HTML
}

type directiveStepsData struct {
	BodyHTML template.HTML
}

type directiveCardData struct {
	Title    string
	Href     string
	BodyHTML template.HTML
}

type directiveCardGroupData struct {
	BodyHTML template.HTML
}

func transformDirectives(source string, renderer directiveRenderer) (string, string, error) {
	lines := strings.Split(source, "\n")
	htmlOut, mirrorOut, next, err := parseDirectiveLines(lines, 0, renderer)
	if err != nil {
		return "", "", err
	}
	if next != len(lines) {
		return "", "", fmt.Errorf("unexpected directive parse stop")
	}
	return strings.Join(htmlOut, "\n"), strings.Join(mirrorOut, "\n"), nil
}

func parseDirectiveLines(lines []string, start int, renderer directiveRenderer) ([]string, []string, int, error) {
	htmlOut := make([]string, 0)
	mirrorOut := make([]string, 0)

	for i := start; i < len(lines); i++ {
		trimmed := strings.TrimSpace(lines[i])
		if trimmed == ":::" {
			return htmlOut, mirrorOut, i + 1, nil
		}
		if strings.HasPrefix(trimmed, ":::") {
			name, attrs := parseDirectiveHeader(strings.TrimPrefix(trimmed, ":::"))
			childHTMLLines, childMirrorLines, next, err := parseDirectiveLines(lines, i+1, renderer)
			if err != nil {
				return nil, nil, 0, err
			}
			childHTML, err := renderMarkdown(strings.Join(childHTMLLines, "\n"))
			if err != nil {
				return nil, nil, 0, err
			}
			renderedHTML, renderedMirror, err := renderDirective(renderer, name, attrs, childHTML, strings.Join(childMirrorLines, "\n"))
			if err != nil {
				return nil, nil, 0, err
			}
			htmlOut = append(htmlOut, renderedHTML)
			mirrorOut = append(mirrorOut, renderedMirror)
			i = next - 1
			continue
		}
		htmlOut = append(htmlOut, lines[i])
		mirrorOut = append(mirrorOut, lines[i])
	}

	return htmlOut, mirrorOut, len(lines), nil
}

func parseDirectiveHeader(header string) (string, map[string]string) {
	attrs := map[string]string{}
	header = strings.TrimSpace(header)
	if header == "" {
		return "", attrs
	}

	name := header
	if idx := strings.IndexFunc(header, unicode.IsSpace); idx >= 0 {
		name = header[:idx]
		attrSource := strings.TrimSpace(header[idx+1:])
		attrRE := directiveAttrRE
		for _, match := range attrRE.FindAllStringSubmatch(attrSource, -1) {
			if len(match) != 3 {
				continue
			}
			attrs[match[1]] = strings.Trim(match[2], `"'`)
		}
	}
	return name, attrs
}

func renderDirective(renderer directiveRenderer, name string, attrs map[string]string, childHTML, childMirror string) (string, string, error) {
	switch name {
	case "callout":
		title := attrs["title"]
		if title == "" {
			title = directiveKindTitle(attrs["kind"])
		}
		htmlBlock, err := renderer.render(name, directiveCalloutData{
			Title: title,
			// #nosec G203 -- childHTML is rendered from repository-owned markdown content and directive bodies.
			BodyHTML: template.HTML(childHTML),
		})
		if err != nil {
			return "", "", err
		}
		return htmlBlock, renderTitledMirror(title, childMirror, ""), nil
	case "details":
		title := attrs["title"]
		if title == "" {
			title = "Details"
		}
		htmlBlock, err := renderer.render(name, directiveDetailsData{
			Title: title,
			// #nosec G203 -- childHTML is rendered from repository-owned markdown content and directive bodies.
			BodyHTML: template.HTML(childHTML),
		})
		if err != nil {
			return "", "", err
		}
		return htmlBlock, renderTitledMirror(title, childMirror, ""), nil
	case "steps":
		htmlBlock, err := renderer.render(name, directiveStepsData{
			// #nosec G203 -- childHTML is rendered from repository-owned markdown content and directive bodies.
			BodyHTML: template.HTML(childHTML),
		})
		if err != nil {
			return "", "", err
		}
		return htmlBlock, strings.TrimSpace(childMirror), nil
	case "card":
		title := attrs["title"]
		href := attrs["href"]
		htmlBlock, err := renderer.render(name, directiveCardData{
			Title: title,
			Href:  href,
			// #nosec G203 -- childHTML is rendered from repository-owned markdown content and directive bodies.
			BodyHTML: template.HTML(childHTML),
		})
		if err != nil {
			return "", "", err
		}
		extra := ""
		if href != "" {
			extra = "Link: " + href
		}
		return htmlBlock, renderTitledMirror(title, childMirror, extra), nil
	case "card-group":
		htmlBlock, err := renderer.render(name, directiveCardGroupData{
			// #nosec G203 -- childHTML is rendered from repository-owned markdown content and directive bodies.
			BodyHTML: template.HTML(childHTML),
		})
		if err != nil {
			return "", "", err
		}
		return htmlBlock, strings.TrimSpace(childMirror), nil
	default:
		return childHTML, childMirror, nil
	}
}

func renderTitledMirror(title, childMirror, trailing string) string {
	var out strings.Builder
	if title != "" {
		out.WriteString("## ")
		out.WriteString(title)
		out.WriteString("\n\n")
	}
	out.WriteString(strings.TrimSpace(childMirror))
	if trailing != "" {
		if out.Len() > 0 {
			out.WriteString("\n\n")
		}
		out.WriteString(trailing)
	}
	return out.String()
}
