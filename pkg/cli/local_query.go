package cli

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/cobra"

	// Register the embedded DuckDB driver used by CLI local BYOC execution.
	_ "github.com/duckdb/duckdb-go/v2"

	"duck-demo/pkg/cli/apiruntime"
)

const (
	computeModeAuto           = "AUTO"
	computeModeBYOCLocal      = "BYOC_LOCAL"
	computeModeSharedEndpoint = "SHARED_ENDPOINT"
)

// The browser runtime will use the same DuckDB + duck_access manifest contract.
type localQueryExecutor interface {
	Execute(ctx context.Context, cfg localQueryConfig, sqlQuery string) (*localQueryResult, error)
}

type localQueryConfig struct {
	APIBaseURL    string
	APIKey        string
	ExtensionPath string
}

type localQueryResult struct {
	Columns  []string        `json:"columns"`
	Rows     [][]interface{} `json:"rows"`
	RowCount int             `json:"row_count"`
}

type embeddedDuckDBLocalExecutor struct{}

var cliLocalQueryExecutor localQueryExecutor = embeddedDuckDBLocalExecutor{}

func shouldExecuteLocally(cmd *cobra.Command) bool {
	mode, _ := cmd.Flags().GetString("compute-mode")
	mode = strings.ToUpper(strings.TrimSpace(mode))

	host, _ := cmd.Root().PersistentFlags().GetString("host")
	if strings.TrimSpace(host) != "" && mode != computeModeBYOCLocal {
		return false
	}

	endpoint, _ := cmd.Flags().GetString("compute-endpoint")
	if strings.TrimSpace(endpoint) != "" {
		return false
	}

	switch mode {
	case computeModeBYOCLocal:
		return true
	case "", computeModeAuto:
		return strings.TrimSpace(host) == ""
	case computeModeSharedEndpoint:
		return false
	default:
		return false
	}
}

func executeLocalQuery(cmd *cobra.Command, client *apiruntime.Client, sqlQuery string) error {
	cfg, err := resolveLocalQueryConfig(cmd, client)
	if err != nil {
		return err
	}

	result, err := cliLocalQueryExecutor.Execute(cmd.Context(), cfg, sqlQuery)
	if err != nil {
		return err
	}
	return printLocalQueryResult(cmd, result)
}

func resolveLocalQueryConfig(cmd *cobra.Command, client *apiruntime.Client) (localQueryConfig, error) {
	if strings.TrimSpace(client.APIKey) == "" {
		return localQueryConfig{}, fmt.Errorf("local BYOC execution requires an API key because duck_access secrets do not support bearer tokens")
	}

	extensionPath, err := resolveDuckAccessExtensionPath(cmd)
	if err != nil {
		return localQueryConfig{}, err
	}

	return localQueryConfig{
		APIBaseURL:    strings.TrimRight(client.BaseURL, "/") + "/v1",
		APIKey:        client.APIKey,
		ExtensionPath: extensionPath,
	}, nil
}

func resolveDuckAccessExtensionPath(cmd *cobra.Command) (string, error) {
	if value, _ := cmd.Root().PersistentFlags().GetString("duck-access-extension-path"); strings.TrimSpace(value) != "" {
		return validateDuckAccessExtensionPath(strings.TrimSpace(value))
	}
	if value := strings.TrimSpace(os.Getenv("DUCK_ACCESS_EXTENSION_PATH")); value != "" {
		return validateDuckAccessExtensionPath(value)
	}

	exePath, err := os.Executable()
	if err == nil {
		exeDir := filepath.Dir(exePath)
		candidates := []string{
			filepath.Join(exeDir, "duck_access.duckdb_extension"),
			filepath.Join(exeDir, "lib", "duck_access.duckdb_extension"),
		}
		for _, candidate := range candidates {
			if path, ok := existingDuckAccessExtensionPath(candidate); ok {
				return path, nil
			}
		}
	}

	devPath := filepath.Join("extension", "duck_access", "build", "release", "duck_access.duckdb_extension")
	if path, ok := existingDuckAccessExtensionPath(devPath); ok {
		return path, nil
	}

	return "", fmt.Errorf("duck_access extension not found; set --duck-access-extension-path or DUCK_ACCESS_EXTENSION_PATH")
}

func existingDuckAccessExtensionPath(path string) (string, bool) {
	if _, err := os.Stat(path); err != nil {
		return "", false
	}
	absPath, err := filepath.Abs(path)
	if err != nil {
		return path, true
	}
	return absPath, true
}

func validateDuckAccessExtensionPath(path string) (string, error) {
	if resolvedPath, ok := existingDuckAccessExtensionPath(path); ok {
		return resolvedPath, nil
	}
	return "", fmt.Errorf("duck_access extension not found at %q", path)
}

func (embeddedDuckDBLocalExecutor) Execute(ctx context.Context, cfg localQueryConfig, sqlQuery string) (*localQueryResult, error) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return nil, fmt.Errorf("open embedded DuckDB: %w", err)
	}
	defer func() { _ = db.Close() }()

	statements := []string{
		"SET autoinstall_known_extensions=true",
		"SET autoload_known_extensions=true",
		fmt.Sprintf("LOAD '%s'", escapeDuckDBString(cfg.ExtensionPath)),
		fmt.Sprintf(
			"CREATE SECRET my_platform (TYPE duck_access, API_URL '%s', API_KEY '%s')",
			escapeDuckDBString(cfg.APIBaseURL),
			escapeDuckDBString(cfg.APIKey),
		),
	}

	for _, statement := range statements {
		if _, err := db.ExecContext(ctx, statement); err != nil {
			return nil, fmt.Errorf("prepare local DuckDB runtime: %w", err)
		}
	}

	rows, err := db.QueryContext(ctx, sqlQuery)
	if err != nil {
		return nil, fmt.Errorf("execute local query: %w", err)
	}
	defer func() { _ = rows.Close() }()

	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("read local query columns: %w", err)
	}

	resultRows := make([][]interface{}, 0)
	for rows.Next() {
		values := make([]interface{}, len(columns))
		pointers := make([]interface{}, len(columns))
		for i := range values {
			pointers[i] = &values[i]
		}
		if err := rows.Scan(pointers...); err != nil {
			return nil, fmt.Errorf("scan local query row: %w", err)
		}
		row := make([]interface{}, len(values))
		for i, value := range values {
			if bytes, ok := value.([]byte); ok {
				row[i] = string(bytes)
			} else {
				row[i] = value
			}
		}
		resultRows = append(resultRows, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read local query rows: %w", err)
	}

	return &localQueryResult{
		Columns:  columns,
		Rows:     resultRows,
		RowCount: len(resultRows),
	}, nil
}

func escapeDuckDBString(value string) string {
	return strings.ReplaceAll(value, "'", "''")
}
