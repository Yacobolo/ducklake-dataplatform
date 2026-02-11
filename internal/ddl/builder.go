package ddl

import (
	"fmt"
	"strings"
)

// ColumnDef describes a column for CREATE TABLE.
type ColumnDef struct {
	Name string
	Type string
}

// CreateSchema returns a DuckDB DDL statement: CREATE SCHEMA lake."<name>".
func CreateSchema(name string) (string, error) {
	if err := ValidateIdentifier(name); err != nil {
		return "", fmt.Errorf("invalid schema name: %w", err)
	}
	return fmt.Sprintf("CREATE SCHEMA lake.%s", QuoteIdentifier(name)), nil
}

// DropSchema returns a DuckDB DDL statement: DROP SCHEMA lake."<name>" [CASCADE].
func DropSchema(name string, cascade bool) (string, error) {
	if err := ValidateIdentifier(name); err != nil {
		return "", fmt.Errorf("invalid schema name: %w", err)
	}
	stmt := fmt.Sprintf("DROP SCHEMA lake.%s", QuoteIdentifier(name))
	if cascade {
		stmt += " CASCADE"
	}
	return stmt, nil
}

// CreateTable returns a DuckDB DDL statement:
// CREATE TABLE lake."<schema>"."<table>" ("<col1>" TYPE1, "<col2>" TYPE2, ...).
func CreateTable(schema, table string, columns []ColumnDef) (string, error) {
	if err := ValidateIdentifier(schema); err != nil {
		return "", fmt.Errorf("invalid schema name: %w", err)
	}
	if err := ValidateIdentifier(table); err != nil {
		return "", fmt.Errorf("invalid table name: %w", err)
	}
	if len(columns) == 0 {
		return "", fmt.Errorf("at least one column is required")
	}

	var colDefs []string
	for _, c := range columns {
		if err := ValidateIdentifier(c.Name); err != nil {
			return "", fmt.Errorf("invalid column name %q: %w", c.Name, err)
		}
		if err := ValidateColumnType(c.Type); err != nil {
			return "", fmt.Errorf("invalid column type for %q: %w", c.Name, err)
		}
		colDefs = append(colDefs, fmt.Sprintf("%s %s", QuoteIdentifier(c.Name), c.Type))
	}

	return fmt.Sprintf("CREATE TABLE lake.%s.%s (%s)",
		QuoteIdentifier(schema),
		QuoteIdentifier(table),
		strings.Join(colDefs, ", "),
	), nil
}

// DropTable returns a DuckDB DDL statement: DROP TABLE lake."<schema>"."<table>".
func DropTable(schema, table string) (string, error) {
	if err := ValidateIdentifier(schema); err != nil {
		return "", fmt.Errorf("invalid schema name: %w", err)
	}
	if err := ValidateIdentifier(table); err != nil {
		return "", fmt.Errorf("invalid table name: %w", err)
	}
	return fmt.Sprintf("DROP TABLE lake.%s.%s",
		QuoteIdentifier(schema),
		QuoteIdentifier(table),
	), nil
}

// CreateS3Secret returns a DuckDB DDL statement to create an S3 secret.
func CreateS3Secret(name, keyID, secret, endpoint, region, urlStyle string) (string, error) {
	if name == "" {
		return "", fmt.Errorf("secret name is required")
	}
	return fmt.Sprintf(`CREATE SECRET %s (
	TYPE S3,
	KEY_ID %s,
	SECRET %s,
	ENDPOINT %s,
	REGION %s,
	URL_STYLE %s
)`,
		QuoteIdentifier(name),
		QuoteLiteral(keyID),
		QuoteLiteral(secret),
		QuoteLiteral(endpoint),
		QuoteLiteral(region),
		QuoteLiteral(urlStyle),
	), nil
}

// DropS3Secret returns a DuckDB DDL statement: DROP SECRET IF EXISTS "<name>".
func DropS3Secret(name string) (string, error) {
	if name == "" {
		return "", fmt.Errorf("secret name is required")
	}
	return fmt.Sprintf("DROP SECRET IF EXISTS %s", QuoteIdentifier(name)), nil
}

// AttachDuckLake returns a DuckDB DDL statement to attach a DuckLake catalog.
// Both metaDBPath and dataPath are properly escaped as SQL string literals.
func AttachDuckLake(metaDBPath, dataPath string) (string, error) {
	if metaDBPath == "" {
		return "", fmt.Errorf("metastore path is required")
	}
	if dataPath == "" {
		return "", fmt.Errorf("data path is required")
	}
	// The ATTACH connection string format is: 'ducklake:sqlite:<path>'
	// Both the metaDBPath and dataPath need proper escaping within single-quoted literals.
	connStr := QuoteLiteral("ducklake:sqlite:" + metaDBPath)
	return fmt.Sprintf("ATTACH %s AS lake (\n\tDATA_PATH %s\n)",
		connStr,
		QuoteLiteral(dataPath),
	), nil
}
