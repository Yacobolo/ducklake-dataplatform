package notebook

import (
	"fmt"
	"strings"

	"duck-demo/internal/domain"
)

// ParseMarkdown parses a Markdown file into a notebook name and cells.
// Convention:
//   - First # heading becomes the notebook name
//   - ```sql blocks become SQL cells
//   - Everything else becomes Markdown cells
//   - Empty markdown between SQL blocks is omitted
func ParseMarkdown(content string) (name string, description string, cells []domain.Cell) {
	lines := strings.Split(content, "\n")
	name = "Untitled Notebook"

	var currentContent strings.Builder
	inCodeBlock := false
	codeBlockLang := ""
	position := 0
	foundTitle := false

	flushMarkdown := func() {
		text := strings.TrimSpace(currentContent.String())
		if text != "" {
			if !foundTitle {
				// First non-empty markdown before any code block is the description
				description = text
				foundTitle = true
			} else {
				cells = append(cells, domain.Cell{
					CellType: domain.CellTypeMarkdown,
					Content:  text,
					Position: position,
				})
				position++
			}
		}
		currentContent.Reset()
	}

	for _, line := range lines {
		trimmed := strings.TrimSpace(line)

		// Check for title (first # heading)
		if !foundTitle && strings.HasPrefix(trimmed, "# ") && !inCodeBlock {
			name = strings.TrimPrefix(trimmed, "# ")
			foundTitle = true
			continue
		}

		if strings.HasPrefix(trimmed, "```") && !inCodeBlock {
			// Start of code block
			flushMarkdown()
			inCodeBlock = true
			codeBlockLang = strings.TrimPrefix(trimmed, "```")
			codeBlockLang = strings.TrimSpace(codeBlockLang)
			continue
		}

		if trimmed == "```" && inCodeBlock {
			// End of code block
			text := strings.TrimSpace(currentContent.String())
			cellType := domain.CellTypeMarkdown
			if strings.EqualFold(codeBlockLang, "sql") {
				cellType = domain.CellTypeSQL
			}
			if text != "" || cellType == domain.CellTypeSQL {
				cells = append(cells, domain.Cell{
					CellType: cellType,
					Content:  text,
					Position: position,
				})
				position++
			}
			currentContent.Reset()
			inCodeBlock = false
			codeBlockLang = ""
			continue
		}

		currentContent.WriteString(line)
		currentContent.WriteString("\n")
	}

	// Flush remaining content
	if !inCodeBlock {
		flushMarkdown()
	}

	return name, description, cells
}

// SerializeMarkdown converts a notebook and its cells to Markdown format.
func SerializeMarkdown(nb *domain.Notebook, cells []domain.Cell) string {
	var b strings.Builder

	b.WriteString(fmt.Sprintf("# %s\n\n", nb.Name))

	if nb.Description != nil && *nb.Description != "" {
		b.WriteString(*nb.Description)
		b.WriteString("\n\n")
	}

	for _, cell := range cells {
		switch cell.CellType {
		case domain.CellTypeSQL:
			b.WriteString("```sql\n")
			b.WriteString(cell.Content)
			b.WriteString("\n```\n\n")
		case domain.CellTypeMarkdown:
			b.WriteString(cell.Content)
			b.WriteString("\n\n")
		}
	}

	return strings.TrimRight(b.String(), "\n") + "\n"
}
