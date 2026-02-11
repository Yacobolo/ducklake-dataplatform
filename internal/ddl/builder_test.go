package ddl

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateSchema(t *testing.T) {
	tests := []struct {
		name    string
		schema  string
		want    string
		wantErr string
	}{
		{
			name:   "valid",
			schema: "analytics",
			want:   `CREATE SCHEMA lake."analytics"`,
		},
		{
			name:    "empty_name",
			schema:  "",
			wantErr: "invalid schema name",
		},
		{
			name:    "invalid_name",
			schema:  "my-schema",
			wantErr: "invalid schema name",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := CreateSchema(tt.schema)
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
		schema  string
		cascade bool
		want    string
		wantErr string
	}{
		{
			name:   "without_cascade",
			schema: "analytics",
			want:   `DROP SCHEMA lake."analytics"`,
		},
		{
			name:    "with_cascade",
			schema:  "analytics",
			cascade: true,
			want:    `DROP SCHEMA lake."analytics" CASCADE`,
		},
		{
			name:    "empty_name",
			schema:  "",
			wantErr: "invalid schema name",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := DropSchema(tt.schema, tt.cascade)
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
		schema  string
		table   string
		columns []ColumnDef
		want    string
		wantErr string
	}{
		{
			name:   "single_column",
			schema: "analytics",
			table:  "events",
			columns: []ColumnDef{
				{Name: "id", Type: "INTEGER"},
			},
			want: `CREATE TABLE lake."analytics"."events" ("id" INTEGER)`,
		},
		{
			name:   "multiple_columns",
			schema: "analytics",
			table:  "events",
			columns: []ColumnDef{
				{Name: "id", Type: "INTEGER"},
				{Name: "name", Type: "VARCHAR(255)"},
				{Name: "amount", Type: "DECIMAL(10,2)"},
			},
			want: `CREATE TABLE lake."analytics"."events" ("id" INTEGER, "name" VARCHAR(255), "amount" DECIMAL(10,2))`,
		},
		{
			name:   "array_type",
			schema: "main",
			table:  "data",
			columns: []ColumnDef{
				{Name: "tags", Type: "VARCHAR[]"},
			},
			want: `CREATE TABLE lake."main"."data" ("tags" VARCHAR[])`,
		},
		{
			name:    "empty_schema",
			schema:  "",
			table:   "events",
			columns: []ColumnDef{{Name: "id", Type: "INTEGER"}},
			wantErr: "invalid schema name",
		},
		{
			name:    "empty_table",
			schema:  "analytics",
			table:   "",
			columns: []ColumnDef{{Name: "id", Type: "INTEGER"}},
			wantErr: "invalid table name",
		},
		{
			name:    "no_columns",
			schema:  "analytics",
			table:   "events",
			columns: nil,
			wantErr: "at least one column is required",
		},
		{
			name:   "invalid_column_name",
			schema: "analytics",
			table:  "events",
			columns: []ColumnDef{
				{Name: "my-col", Type: "INTEGER"},
			},
			wantErr: "invalid column name",
		},
		{
			name:   "sql_injection_in_type",
			schema: "analytics",
			table:  "events",
			columns: []ColumnDef{
				{Name: "id", Type: "INTEGER); DROP TABLE foo; --"},
			},
			wantErr: "invalid column type",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := CreateTable(tt.schema, tt.table, tt.columns)
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
		schema  string
		table   string
		want    string
		wantErr string
	}{
		{
			name:   "valid",
			schema: "analytics",
			table:  "events",
			want:   `DROP TABLE lake."analytics"."events"`,
		},
		{
			name:    "empty_schema",
			schema:  "",
			table:   "events",
			wantErr: "invalid schema name",
		},
		{
			name:    "empty_table",
			schema:  "analytics",
			table:   "",
			wantErr: "invalid table name",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := DropTable(tt.schema, tt.table)
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

func TestAttachDuckLake(t *testing.T) {
	tests := []struct {
		name       string
		metaDBPath string
		dataPath   string
		wantErr    string
		contains   []string
	}{
		{
			name:       "valid",
			metaDBPath: "/tmp/meta.db",
			dataPath:   "s3://bucket/data",
			contains: []string{
				"ATTACH 'ducklake:sqlite:/tmp/meta.db'",
				"DATA_PATH 's3://bucket/data'",
			},
		},
		{
			name:       "escapes_metadb_path",
			metaDBPath: "/tmp/it's here/meta.db",
			dataPath:   "s3://bucket/data",
			contains: []string{
				"ATTACH 'ducklake:sqlite:/tmp/it''s here/meta.db'",
			},
		},
		{
			name:       "escapes_data_path",
			metaDBPath: "/tmp/meta.db",
			dataPath:   "s3://bucket/it's/data",
			contains: []string{
				"DATA_PATH 's3://bucket/it''s/data'",
			},
		},
		{
			name:       "empty_meta_path",
			metaDBPath: "",
			dataPath:   "s3://bucket/data",
			wantErr:    "metastore path is required",
		},
		{
			name:       "empty_data_path",
			metaDBPath: "/tmp/meta.db",
			dataPath:   "",
			wantErr:    "data path is required",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := AttachDuckLake(tt.metaDBPath, tt.dataPath)
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
