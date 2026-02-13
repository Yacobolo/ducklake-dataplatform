package ddl

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateSchema(t *testing.T) {
	tests := []struct {
		name    string
		catalog string
		schema  string
		want    string
		wantErr string
	}{
		{
			name:    "valid",
			catalog: "lake",
			schema:  "analytics",
			want:    `CREATE SCHEMA "lake"."analytics"`,
		},
		{
			name:    "custom_catalog",
			catalog: "mycat",
			schema:  "analytics",
			want:    `CREATE SCHEMA "mycat"."analytics"`,
		},
		{
			name:    "empty_catalog",
			catalog: "",
			schema:  "analytics",
			wantErr: "invalid catalog name",
		},
		{
			name:    "empty_name",
			catalog: "lake",
			schema:  "",
			wantErr: "invalid schema name",
		},
		{
			name:    "invalid_name",
			catalog: "lake",
			schema:  "my-schema",
			wantErr: "invalid schema name",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := CreateSchema(tt.catalog, tt.schema)
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestDropSchema(t *testing.T) {
	tests := []struct {
		name    string
		catalog string
		schema  string
		cascade bool
		want    string
		wantErr string
	}{
		{
			name:    "without_cascade",
			catalog: "lake",
			schema:  "analytics",
			want:    `DROP SCHEMA "lake"."analytics"`,
		},
		{
			name:    "with_cascade",
			catalog: "lake",
			schema:  "analytics",
			cascade: true,
			want:    `DROP SCHEMA "lake"."analytics" CASCADE`,
		},
		{
			name:    "empty_name",
			catalog: "lake",
			schema:  "",
			wantErr: "invalid schema name",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := DropSchema(tt.catalog, tt.schema, tt.cascade)
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestCreateTable(t *testing.T) {
	tests := []struct {
		name    string
		catalog string
		schema  string
		table   string
		columns []ColumnDef
		want    string
		wantErr string
	}{
		{
			name:    "single_column",
			catalog: "lake",
			schema:  "analytics",
			table:   "events",
			columns: []ColumnDef{
				{Name: "id", Type: "INTEGER"},
			},
			want: `CREATE TABLE "lake"."analytics"."events" ("id" INTEGER)`,
		},
		{
			name:    "multiple_columns",
			catalog: "lake",
			schema:  "analytics",
			table:   "events",
			columns: []ColumnDef{
				{Name: "id", Type: "INTEGER"},
				{Name: "name", Type: "VARCHAR(255)"},
				{Name: "amount", Type: "DECIMAL(10,2)"},
			},
			want: `CREATE TABLE "lake"."analytics"."events" ("id" INTEGER, "name" VARCHAR(255), "amount" DECIMAL(10,2))`,
		},
		{
			name:    "array_type",
			catalog: "lake",
			schema:  "main",
			table:   "data",
			columns: []ColumnDef{
				{Name: "tags", Type: "VARCHAR[]"},
			},
			want: `CREATE TABLE "lake"."main"."data" ("tags" VARCHAR[])`,
		},
		{
			name:    "empty_schema",
			catalog: "lake",
			schema:  "",
			table:   "events",
			columns: []ColumnDef{{Name: "id", Type: "INTEGER"}},
			wantErr: "invalid schema name",
		},
		{
			name:    "empty_table",
			catalog: "lake",
			schema:  "analytics",
			table:   "",
			columns: []ColumnDef{{Name: "id", Type: "INTEGER"}},
			wantErr: "invalid table name",
		},
		{
			name:    "no_columns",
			catalog: "lake",
			schema:  "analytics",
			table:   "events",
			columns: nil,
			wantErr: "at least one column is required",
		},
		{
			name:    "invalid_column_name",
			catalog: "lake",
			schema:  "analytics",
			table:   "events",
			columns: []ColumnDef{
				{Name: "my-col", Type: "INTEGER"},
			},
			wantErr: "invalid column name",
		},
		{
			name:    "sql_injection_in_type",
			catalog: "lake",
			schema:  "analytics",
			table:   "events",
			columns: []ColumnDef{
				{Name: "id", Type: "INTEGER); DROP TABLE foo; --"},
			},
			wantErr: "invalid column type",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := CreateTable(tt.catalog, tt.schema, tt.table, tt.columns)
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestDropTable(t *testing.T) {
	tests := []struct {
		name    string
		catalog string
		schema  string
		table   string
		want    string
		wantErr string
	}{
		{
			name:    "valid",
			catalog: "lake",
			schema:  "analytics",
			table:   "events",
			want:    `DROP TABLE "lake"."analytics"."events"`,
		},
		{
			name:    "empty_schema",
			catalog: "lake",
			schema:  "",
			table:   "events",
			wantErr: "invalid schema name",
		},
		{
			name:    "empty_table",
			catalog: "lake",
			schema:  "analytics",
			table:   "",
			wantErr: "invalid table name",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := DropTable(tt.catalog, tt.schema, tt.table)
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestCreateS3Secret(t *testing.T) {
	tests := []struct {
		name     string
		secName  string
		keyID    string
		secret   string
		endpoint string
		region   string
		urlStyle string
		wantErr  string
		contains []string
	}{
		{
			name:     "valid",
			secName:  "my_secret",
			keyID:    "AKIAIOSFODNN7EXAMPLE",
			secret:   "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
			endpoint: "s3.amazonaws.com",
			region:   "us-east-1",
			urlStyle: "path",
			contains: []string{
				`CREATE SECRET "my_secret"`,
				`KEY_ID 'AKIAIOSFODNN7EXAMPLE'`,
				`SECRET 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY'`,
				`ENDPOINT 's3.amazonaws.com'`,
				`REGION 'us-east-1'`,
				`URL_STYLE 'path'`,
			},
		},
		{
			name:     "escapes_quotes_in_values",
			secName:  `sec"ret`,
			keyID:    "key'id",
			secret:   "sec'ret",
			endpoint: "end'point",
			region:   "reg'ion",
			urlStyle: "url'style",
			contains: []string{
				`CREATE SECRET "sec""ret"`,
				`KEY_ID 'key''id'`,
				`SECRET 'sec''ret'`,
				`ENDPOINT 'end''point'`,
				`REGION 'reg''ion'`,
				`URL_STYLE 'url''style'`,
			},
		},
		{
			name:    "empty_name",
			secName: "",
			wantErr: "secret name is required",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := CreateS3Secret(tt.secName, tt.keyID, tt.secret, tt.endpoint, tt.region, tt.urlStyle)
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
				return
			}
			require.NoError(t, err)
			for _, s := range tt.contains {
				assert.Contains(t, got, s)
			}
		})
	}
}

func TestCreateAzureSecret(t *testing.T) {
	tests := []struct {
		name             string
		secName          string
		accountName      string
		accountKey       string
		connectionString string
		wantErr          string
		contains         []string
	}{
		{
			name:        "with_account_key",
			secName:     "azure_cred",
			accountName: "mystorageaccount",
			accountKey:  "base64encodedkey==",
			contains: []string{
				`CREATE SECRET "azure_cred"`,
				"TYPE AZURE",
				`ACCOUNT_NAME 'mystorageaccount'`,
				`ACCOUNT_KEY 'base64encodedkey=='`,
			},
		},
		{
			name:             "with_connection_string",
			secName:          "azure_cred",
			connectionString: "DefaultEndpointsProtocol=https;AccountName=myacct;AccountKey=mykey==;EndpointSuffix=core.windows.net",
			contains: []string{
				`CREATE SECRET "azure_cred"`,
				"TYPE AZURE",
				"CONNECTION_STRING",
			},
		},
		{
			name:    "empty_name",
			secName: "",
			wantErr: "secret name is required",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := CreateAzureSecret(tt.secName, tt.accountName, tt.accountKey, tt.connectionString)
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
				return
			}
			require.NoError(t, err)
			for _, s := range tt.contains {
				assert.Contains(t, got, s)
			}
		})
	}
}

func TestCreateGCSSecret(t *testing.T) {
	tests := []struct {
		name        string
		secName     string
		keyFilePath string
		wantErr     string
		contains    []string
	}{
		{
			name:        "valid",
			secName:     "gcs_cred",
			keyFilePath: "/path/to/keyfile.json",
			contains: []string{
				`CREATE SECRET "gcs_cred"`,
				"TYPE GCS",
				`KEY_FILE_PATH '/path/to/keyfile.json'`,
			},
		},
		{
			name:    "empty_name",
			secName: "",
			wantErr: "secret name is required",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := CreateGCSSecret(tt.secName, tt.keyFilePath)
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
				return
			}
			require.NoError(t, err)
			for _, s := range tt.contains {
				assert.Contains(t, got, s)
			}
		})
	}
}

func TestDropSecret(t *testing.T) {
	tests := []struct {
		name    string
		secName string
		want    string
		wantErr string
	}{
		{
			name:    "valid",
			secName: "my_secret",
			want:    `DROP SECRET IF EXISTS "my_secret"`,
		},
		{
			name:    "escapes_quotes",
			secName: `sec"ret`,
			want:    `DROP SECRET IF EXISTS "sec""ret"`,
		},
		{
			name:    "empty_name",
			secName: "",
			wantErr: "secret name is required",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := DropSecret(tt.secName)
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestDropS3Secret(t *testing.T) {
	tests := []struct {
		name    string
		secName string
		want    string
		wantErr string
	}{
		{
			name:    "valid",
			secName: "my_secret",
			want:    `DROP SECRET IF EXISTS "my_secret"`,
		},
		{
			name:    "escapes_quotes",
			secName: `sec"ret`,
			want:    `DROP SECRET IF EXISTS "sec""ret"`,
		},
		{
			name:    "empty_name",
			secName: "",
			wantErr: "secret name is required",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := DropS3Secret(tt.secName)
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestCreateExternalTableView(t *testing.T) {
	tests := []struct {
		name       string
		catalog    string
		schema     string
		table      string
		sourcePath string
		fileFormat string
		want       string
		wantErr    string
	}{
		{
			name:       "parquet",
			catalog:    "lake",
			schema:     "analytics",
			table:      "events",
			sourcePath: "s3://bucket/data/events.parquet",
			fileFormat: "parquet",
			want:       `CREATE VIEW "lake"."analytics"."events" AS SELECT * FROM read_parquet(['s3://bucket/data/events.parquet'])`,
		},
		{
			name:       "csv",
			catalog:    "lake",
			schema:     "analytics",
			table:      "logs",
			sourcePath: "s3://bucket/data/logs.csv",
			fileFormat: "csv",
			want:       `CREATE VIEW "lake"."analytics"."logs" AS SELECT * FROM read_csv(['s3://bucket/data/logs.csv'])`,
		},
		{
			name:       "default_format",
			catalog:    "lake",
			schema:     "analytics",
			table:      "data",
			sourcePath: "s3://bucket/data.parquet",
			fileFormat: "",
			want:       `CREATE VIEW "lake"."analytics"."data" AS SELECT * FROM read_parquet(['s3://bucket/data.parquet'])`,
		},
		{
			name:       "invalid_format",
			catalog:    "lake",
			schema:     "analytics",
			table:      "data",
			sourcePath: "s3://bucket/data",
			fileFormat: "json",
			wantErr:    "unsupported file format",
		},
		{
			name:       "missing_path",
			catalog:    "lake",
			schema:     "analytics",
			table:      "data",
			sourcePath: "",
			fileFormat: "parquet",
			wantErr:    "source path is required",
		},
		{
			name:       "invalid_schema",
			catalog:    "lake",
			schema:     "",
			table:      "data",
			sourcePath: "s3://bucket/data.parquet",
			fileFormat: "parquet",
			wantErr:    "invalid schema name",
		},
		{
			name:       "invalid_table",
			catalog:    "lake",
			schema:     "analytics",
			table:      "",
			sourcePath: "s3://bucket/data.parquet",
			fileFormat: "parquet",
			wantErr:    "invalid table name",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := CreateExternalTableView(tt.catalog, tt.schema, tt.table, tt.sourcePath, tt.fileFormat)
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestDropView(t *testing.T) {
	tests := []struct {
		name    string
		catalog string
		schema  string
		table   string
		want    string
		wantErr string
	}{
		{
			name:    "valid",
			catalog: "lake",
			schema:  "analytics",
			table:   "events",
			want:    `DROP VIEW IF EXISTS "lake"."analytics"."events"`,
		},
		{
			name:    "empty_schema",
			catalog: "lake",
			schema:  "",
			table:   "events",
			wantErr: "invalid schema name",
		},
		{
			name:    "empty_table",
			catalog: "lake",
			schema:  "analytics",
			table:   "",
			wantErr: "invalid table name",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := DropView(tt.catalog, tt.schema, tt.table)
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestDiscoverColumnsSQL(t *testing.T) {
	tests := []struct {
		name       string
		sourcePath string
		fileFormat string
		want       string
		wantErr    string
	}{
		{
			name:       "parquet",
			sourcePath: "s3://bucket/data.parquet",
			fileFormat: "parquet",
			want:       `DESCRIBE SELECT * FROM read_parquet(['s3://bucket/data.parquet']) LIMIT 0`,
		},
		{
			name:       "csv",
			sourcePath: "s3://bucket/data.csv",
			fileFormat: "csv",
			want:       `DESCRIBE SELECT * FROM read_csv(['s3://bucket/data.csv']) LIMIT 0`,
		},
		{
			name:       "invalid_format",
			sourcePath: "s3://bucket/data",
			fileFormat: "xml",
			wantErr:    "unsupported file format",
		},
		{
			name:       "empty_path",
			sourcePath: "",
			fileFormat: "parquet",
			wantErr:    "source path is required",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := DiscoverColumnsSQL(tt.sourcePath, tt.fileFormat)
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestAttachDuckLake(t *testing.T) {
	tests := []struct {
		name        string
		catalogName string
		metaDBPath  string
		dataPath    string
		wantErr     string
		contains    []string
	}{
		{
			name:        "valid",
			catalogName: "lake",
			metaDBPath:  "/tmp/meta.db",
			dataPath:    "s3://bucket/data",
			contains: []string{
				"ATTACH 'ducklake:sqlite:/tmp/meta.db'",
				`AS "lake"`,
				"DATA_PATH 's3://bucket/data'",
			},
		},
		{
			name:        "custom_catalog_name",
			catalogName: "mycat",
			metaDBPath:  "/tmp/meta.db",
			dataPath:    "s3://bucket/data",
			contains: []string{
				`AS "mycat"`,
			},
		},
		{
			name:        "escapes_metadb_path",
			catalogName: "lake",
			metaDBPath:  "/tmp/it's here/meta.db",
			dataPath:    "s3://bucket/data",
			contains: []string{
				"ATTACH 'ducklake:sqlite:/tmp/it''s here/meta.db'",
			},
		},
		{
			name:        "escapes_data_path",
			catalogName: "lake",
			metaDBPath:  "/tmp/meta.db",
			dataPath:    "s3://bucket/it's/data",
			contains: []string{
				"DATA_PATH 's3://bucket/it''s/data'",
			},
		},
		{
			name:        "empty_meta_path",
			catalogName: "lake",
			metaDBPath:  "",
			dataPath:    "s3://bucket/data",
			wantErr:     "metastore path is required",
		},
		{
			name:        "empty_data_path",
			catalogName: "lake",
			metaDBPath:  "/tmp/meta.db",
			dataPath:    "",
			wantErr:     "data path is required",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := AttachDuckLake(tt.catalogName, tt.metaDBPath, tt.dataPath)
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
				return
			}
			require.NoError(t, err)
			for _, s := range tt.contains {
				assert.Contains(t, got, s)
			}
		})
	}
}

func TestDetachCatalog(t *testing.T) {
	tests := []struct {
		name        string
		catalogName string
		want        string
		wantErr     string
	}{
		{
			name:        "valid",
			catalogName: "lake",
			want:        `DETACH "lake"`,
		},
		{
			name:        "empty_name",
			catalogName: "",
			wantErr:     "invalid catalog name",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := DetachCatalog(tt.catalogName)
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestSetDefaultCatalog(t *testing.T) {
	tests := []struct {
		name        string
		catalogName string
		want        string
		wantErr     string
	}{
		{
			name:        "valid",
			catalogName: "lake",
			want:        `USE "lake"`,
		},
		{
			name:        "empty_name",
			catalogName: "",
			wantErr:     "invalid catalog name",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := SetDefaultCatalog(tt.catalogName)
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}
