package notebook

import (
	"testing"

	"duck-demo/internal/domain"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseMarkdown_FullDocument(t *testing.T) {
	content := `# My Notebook

This is the description.

` + "```sql" + `
SELECT * FROM users
` + "```" + `

Some notes between queries.

` + "```sql" + `
SELECT count(*) FROM orders
` + "```" + `
`

	name, desc, cells := ParseMarkdown(content)

	assert.Equal(t, "My Notebook", name)
	// When # heading is present, foundTitle is set by the heading,
	// so subsequent markdown paragraphs become cells, not description.
	assert.Empty(t, desc)
	require.Len(t, cells, 4)

	assert.Equal(t, domain.CellTypeMarkdown, cells[0].CellType)
	assert.Equal(t, "This is the description.", cells[0].Content)
	assert.Equal(t, 0, cells[0].Position)

	assert.Equal(t, domain.CellTypeSQL, cells[1].CellType)
	assert.Equal(t, "SELECT * FROM users", cells[1].Content)
	assert.Equal(t, 1, cells[1].Position)

	assert.Equal(t, domain.CellTypeMarkdown, cells[2].CellType)
	assert.Equal(t, "Some notes between queries.", cells[2].Content)
	assert.Equal(t, 2, cells[2].Position)

	assert.Equal(t, domain.CellTypeSQL, cells[3].CellType)
	assert.Equal(t, "SELECT count(*) FROM orders", cells[3].Content)
	assert.Equal(t, 3, cells[3].Position)
}

func TestParseMarkdown_NoTitle(t *testing.T) {
	content := "```sql\nSELECT 1\n```\n"

	name, desc, cells := ParseMarkdown(content)

	assert.Equal(t, "Untitled Notebook", name)
	assert.Empty(t, desc)
	require.Len(t, cells, 1)
	assert.Equal(t, domain.CellTypeSQL, cells[0].CellType)
	assert.Equal(t, "SELECT 1", cells[0].Content)
}

func TestParseMarkdown_OnlyTitle(t *testing.T) {
	content := "# Just a Title\n"

	name, desc, cells := ParseMarkdown(content)

	assert.Equal(t, "Just a Title", name)
	assert.Empty(t, desc)
	assert.Empty(t, cells)
}

func TestParseMarkdown_EmptyContent(t *testing.T) {
	name, desc, cells := ParseMarkdown("")

	assert.Equal(t, "Untitled Notebook", name)
	assert.Empty(t, desc)
	assert.Empty(t, cells)
}

func TestParseMarkdown_EmptySQLBlock(t *testing.T) {
	content := "# NB\n\n```sql\n```\n"

	name, _, cells := ParseMarkdown(content)

	assert.Equal(t, "NB", name)
	// Empty SQL blocks are still created (Content is "")
	require.Len(t, cells, 1)
	assert.Equal(t, domain.CellTypeSQL, cells[0].CellType)
	assert.Equal(t, "", cells[0].Content)
}

func TestParseMarkdown_DescriptionBeforeCode(t *testing.T) {
	// When a `# heading` is present, foundTitle is already set, so
	// subsequent markdown paragraphs become cells (not the description).
	content := "# NB\n\nParagraph one.\n\nParagraph two.\n\n```sql\nSELECT 1\n```\n"

	name, desc, cells := ParseMarkdown(content)

	assert.Equal(t, "NB", name)
	assert.Empty(t, desc) // description is only set when there's no heading
	require.Len(t, cells, 2)
	assert.Equal(t, domain.CellTypeMarkdown, cells[0].CellType)
	assert.Equal(t, "Paragraph one.\n\nParagraph two.", cells[0].Content)
	assert.Equal(t, domain.CellTypeSQL, cells[1].CellType)
}

func TestSerializeMarkdown_Full(t *testing.T) {
	desc := "A description"
	nb := &domain.Notebook{
		Name:        "Test Notebook",
		Description: &desc,
	}
	cells := []domain.Cell{
		{CellType: domain.CellTypeSQL, Content: "SELECT 1"},
		{CellType: domain.CellTypeMarkdown, Content: "Some notes"},
		{CellType: domain.CellTypeSQL, Content: "SELECT 2"},
	}

	result := SerializeMarkdown(nb, cells)

	expected := "# Test Notebook\n\nA description\n\n```sql\nSELECT 1\n```\n\nSome notes\n\n```sql\nSELECT 2\n```\n"
	assert.Equal(t, expected, result)
}

func TestSerializeMarkdown_NoDescription(t *testing.T) {
	nb := &domain.Notebook{
		Name: "No Desc",
	}
	cells := []domain.Cell{
		{CellType: domain.CellTypeSQL, Content: "SELECT 42"},
	}

	result := SerializeMarkdown(nb, cells)

	expected := "# No Desc\n\n```sql\nSELECT 42\n```\n"
	assert.Equal(t, expected, result)
}

func TestSerializeMarkdown_NoCells(t *testing.T) {
	nb := &domain.Notebook{Name: "Empty"}
	result := SerializeMarkdown(nb, nil)

	assert.Equal(t, "# Empty\n", result)
}

func TestSerializeMarkdown_RoundTrip(t *testing.T) {
	// Round-trip: serialize then parse back.
	// Note: when a description is serialized as a paragraph after the `# title`,
	// the parser treats it as a markdown cell (since foundTitle is already set by the heading).
	// This means the description does not survive a full round-trip — it becomes a cell.
	// We test both the lossy path (with desc) and lossless path (no desc).
	t.Run("without description", func(t *testing.T) {
		nb := &domain.Notebook{Name: "Roundtrip"}
		cells := []domain.Cell{
			{CellType: domain.CellTypeSQL, Content: "CREATE TABLE t (id INT)"},
			{CellType: domain.CellTypeMarkdown, Content: "Insert some data:"},
			{CellType: domain.CellTypeSQL, Content: "INSERT INTO t VALUES (1), (2)"},
		}

		md := SerializeMarkdown(nb, cells)
		parsedName, _, parsedCells := ParseMarkdown(md)

		assert.Equal(t, "Roundtrip", parsedName)
		require.Len(t, parsedCells, 3)

		assert.Equal(t, domain.CellTypeSQL, parsedCells[0].CellType)
		assert.Equal(t, "CREATE TABLE t (id INT)", parsedCells[0].Content)

		assert.Equal(t, domain.CellTypeMarkdown, parsedCells[1].CellType)
		assert.Equal(t, "Insert some data:", parsedCells[1].Content)

		assert.Equal(t, domain.CellTypeSQL, parsedCells[2].CellType)
		assert.Equal(t, "INSERT INTO t VALUES (1), (2)", parsedCells[2].Content)
	})

	t.Run("with description becomes extra cell", func(t *testing.T) {
		desc := "Round-trip test"
		nb := &domain.Notebook{
			Name:        "Roundtrip",
			Description: &desc,
		}
		cells := []domain.Cell{
			{CellType: domain.CellTypeSQL, Content: "SELECT 1"},
		}

		md := SerializeMarkdown(nb, cells)
		parsedName, _, parsedCells := ParseMarkdown(md)

		assert.Equal(t, "Roundtrip", parsedName)
		// Description paragraph becomes a markdown cell on re-parse
		require.Len(t, parsedCells, 2)
		assert.Equal(t, domain.CellTypeMarkdown, parsedCells[0].CellType)
		assert.Contains(t, parsedCells[0].Content, "Round-trip test")
		assert.Equal(t, domain.CellTypeSQL, parsedCells[1].CellType)
	})
}
