package model

import (
	"testing"

	"duck-demo/internal/domain"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGenerateTestSQL(t *testing.T) {
	tests := []struct {
		name    string
		test    domain.ModelTest
		schema  string
		model   string
		wantSQL string
		wantErr bool
	}{
		{
			name:    "not_null",
			test:    domain.ModelTest{TestType: "not_null", Column: "id"},
			schema:  "analytics",
			model:   "orders",
			wantSQL: `SELECT * FROM "analytics"."orders" WHERE "id" IS NULL LIMIT 1`,
		},
		{
			name:    "unique",
			test:    domain.ModelTest{TestType: "unique", Column: "email"},
			schema:  "analytics",
			model:   "users",
			wantSQL: `SELECT "email", COUNT(*) AS cnt FROM "analytics"."users" GROUP BY "email" HAVING cnt > 1 LIMIT 1`,
		},
		{
			name: "accepted_values",
			test: domain.ModelTest{
				TestType: "accepted_values", Column: "status",
				Config: domain.ModelTestConfig{Values: []string{"active", "inactive"}},
			},
			schema:  "analytics",
			model:   "orders",
			wantSQL: `SELECT * FROM "analytics"."orders" WHERE "status" NOT IN ('active', 'inactive') LIMIT 1`,
		},
		{
			name: "relationships",
			test: domain.ModelTest{
				TestType: "relationships", Column: "customer_id",
				Config: domain.ModelTestConfig{ToModel: "customers", ToColumn: "id"},
			},
			schema:  "analytics",
			model:   "orders",
			wantSQL: `SELECT a."customer_id" FROM "analytics"."orders" a LEFT JOIN "analytics"."customers" b ON a."customer_id" = b."id" WHERE b."id" IS NULL LIMIT 1`,
		},
		{
			name: "custom_sql",
			test: domain.ModelTest{
				TestType: "custom_sql",
				Config:   domain.ModelTestConfig{SQL: "SELECT 1 WHERE 1=0"},
			},
			schema:  "analytics",
			model:   "orders",
			wantSQL: "SELECT 1 WHERE 1=0",
		},
		{
			name:    "unknown type",
			test:    domain.ModelTest{TestType: "invalid"},
			schema:  "analytics",
			model:   "orders",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sql, err := generateTestSQL(tt.test, tt.schema, tt.model)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.wantSQL, sql)
		})
	}
}
