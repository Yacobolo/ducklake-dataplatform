//go:build integration

package integration

import (
	"io"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type semanticModelResp struct {
	ID          string `json:"id"`
	ProjectName string `json:"project_name"`
	Name        string `json:"name"`
}

type semanticMetricResp struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

type semanticPreAggResp struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

type semanticRelationshipResp struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

func TestSemanticAPI_CRUDAndExplain(t *testing.T) {
	t.Parallel()

	env := setupHTTPServer(t, httpTestOpts{WithSemantic: true})

	createSalesResp := doRequest(t, http.MethodPost, env.Server.URL+"/v1/semantic-models", env.Keys.Admin, map[string]interface{}{
		"project_name":   "analytics",
		"name":           "sales",
		"base_model_ref": "analytics.fct_sales",
	})
	require.Equal(t, http.StatusCreated, createSalesResp.StatusCode, responseBodyOnStatusMismatch(t, createSalesResp, http.StatusCreated))
	var salesModel semanticModelResp
	decodeJSON(t, createSalesResp, &salesModel)
	require.NotEmpty(t, salesModel.ID)

	createCustomersResp := doRequest(t, http.MethodPost, env.Server.URL+"/v1/semantic-models", env.Keys.Admin, map[string]interface{}{
		"project_name":   "analytics",
		"name":           "customers",
		"base_model_ref": "analytics.dim_customers",
	})
	require.Equal(t, http.StatusCreated, createCustomersResp.StatusCode, responseBodyOnStatusMismatch(t, createCustomersResp, http.StatusCreated))
	var customersModel semanticModelResp
	decodeJSON(t, createCustomersResp, &customersModel)
	require.NotEmpty(t, customersModel.ID)

	createMetricResp := doRequest(t, http.MethodPost, env.Server.URL+"/v1/semantic-models/analytics/sales/metrics", env.Keys.Admin, map[string]interface{}{
		"name":                "total_revenue",
		"metric_type":         "SUM",
		"expression_mode":     "SQL",
		"expression":          "SUM(amount)",
		"certification_state": "CERTIFIED",
	})
	require.Equal(t, http.StatusCreated, createMetricResp.StatusCode, responseBodyOnStatusMismatch(t, createMetricResp, http.StatusCreated))
	var metric semanticMetricResp
	decodeJSON(t, createMetricResp, &metric)
	require.NotEmpty(t, metric.ID)

	createPreAggResp := doRequest(t, http.MethodPost, env.Server.URL+"/v1/semantic-models/analytics/sales/pre-aggregations", env.Keys.Admin, map[string]interface{}{
		"name":            "daily_sales",
		"metric_set":      []string{"total_revenue"},
		"dimension_set":   []string{"order_date"},
		"target_relation": "analytics.agg_daily_sales",
	})
	require.Equal(t, http.StatusCreated, createPreAggResp.StatusCode, responseBodyOnStatusMismatch(t, createPreAggResp, http.StatusCreated))
	var preAgg semanticPreAggResp
	decodeJSON(t, createPreAggResp, &preAgg)
	require.NotEmpty(t, preAgg.ID)

	createRelationshipResp := doRequest(t, http.MethodPost, env.Server.URL+"/v1/semantic-relationships", env.Keys.Admin, map[string]interface{}{
		"name":              "sales_to_customers",
		"from_semantic_id":  salesModel.ID,
		"to_semantic_id":    customersModel.ID,
		"relationship_type": "MANY_TO_ONE",
		"join_sql":          "sales.customer_id = customers.id",
	})
	require.Equal(t, http.StatusCreated, createRelationshipResp.StatusCode, responseBodyOnStatusMismatch(t, createRelationshipResp, http.StatusCreated))
	var relationship semanticRelationshipResp
	decodeJSON(t, createRelationshipResp, &relationship)
	require.NotEmpty(t, relationship.ID)

	listModelsResp := doRequest(t, http.MethodGet, env.Server.URL+"/v1/semantic-models?project_name=analytics", env.Keys.Admin, nil)
	require.Equal(t, http.StatusOK, listModelsResp.StatusCode, responseBodyOnStatusMismatch(t, listModelsResp, http.StatusOK))
	var listed struct {
		Data []semanticModelResp `json:"data"`
	}
	decodeJSON(t, listModelsResp, &listed)
	require.Len(t, listed.Data, 2)

	explainResp := doRequest(t, http.MethodPost, env.Server.URL+"/v1/metric-queries:explain", env.Keys.Admin, map[string]interface{}{
		"project_name":        "analytics",
		"semantic_model_name": "sales",
		"metrics":             []string{"total_revenue"},
	})
	require.Equal(t, http.StatusOK, explainResp.StatusCode, responseBodyOnStatusMismatch(t, explainResp, http.StatusOK))
	var explainBody struct {
		Plan struct {
			BaseModelName string `json:"base_model_name"`
			GeneratedSQL  string `json:"generated_sql"`
		} `json:"plan"`
	}
	decodeJSON(t, explainResp, &explainBody)
	assert.Equal(t, "sales", explainBody.Plan.BaseModelName)
	assert.NotEmpty(t, explainBody.Plan.GeneratedSQL)
}

func TestSemanticAPI_RunMetricQuery(t *testing.T) {
	t.Parallel()

	env := setupHTTPServer(t, httpTestOpts{WithSemantic: true, WithComputeEndpoints: true, SeedDuckLakeMetadata: true})
	require.NotNil(t, env.DuckDB)

	_, err := env.DuckDB.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS titanic (Fare DOUBLE)`)
	require.NoError(t, err)
	_, err = env.DuckDB.ExecContext(ctx, `DELETE FROM titanic`)
	require.NoError(t, err)
	_, err = env.DuckDB.ExecContext(ctx, `INSERT INTO titanic VALUES (10.5), (20.0)`)
	require.NoError(t, err)

	createModelResp := doRequest(t, http.MethodPost, env.Server.URL+"/v1/semantic-models", env.Keys.Admin, map[string]interface{}{
		"project_name":   "analytics",
		"name":           "sales_runtime",
		"base_model_ref": "main.titanic",
	})
	require.Equal(t, http.StatusCreated, createModelResp.StatusCode, responseBodyOnStatusMismatch(t, createModelResp, http.StatusCreated))

	createMetricResp := doRequest(t, http.MethodPost, env.Server.URL+"/v1/semantic-models/analytics/sales_runtime/metrics", env.Keys.Admin, map[string]interface{}{
		"name":                "total_amount",
		"metric_type":         "SUM",
		"expression_mode":     "SQL",
		"expression":          "SUM(Fare)",
		"certification_state": "CERTIFIED",
	})
	require.Equal(t, http.StatusCreated, createMetricResp.StatusCode, responseBodyOnStatusMismatch(t, createMetricResp, http.StatusCreated))

	runResp := doRequest(t, http.MethodPost, env.Server.URL+"/v1/metric-queries:run", env.Keys.Admin, map[string]interface{}{
		"project_name":        "analytics",
		"semantic_model_name": "sales_runtime",
		"metrics":             []string{"total_amount"},
	})
	require.Equal(t, http.StatusOK, runResp.StatusCode, responseBodyOnStatusMismatch(t, runResp, http.StatusOK))

	var runBody struct {
		Plan struct {
			GeneratedSQL string `json:"generated_sql"`
		} `json:"plan"`
		Result struct {
			Columns  []string        `json:"columns"`
			Rows     [][]interface{} `json:"rows"`
			RowCount int64           `json:"row_count"`
		} `json:"result"`
	}
	decodeJSON(t, runResp, &runBody)

	assert.NotEmpty(t, runBody.Plan.GeneratedSQL)
	assert.EqualValues(t, 1, runBody.Result.RowCount)
	require.GreaterOrEqual(t, len(runBody.Result.Columns), 1)
	require.Len(t, runBody.Result.Rows, 1)
}

func responseBodyOnStatusMismatch(t *testing.T, resp *http.Response, expected int) string {
	t.Helper()
	if resp.StatusCode == expected {
		return ""
	}
	defer resp.Body.Close() //nolint:errcheck
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return "<failed to read body>"
	}
	return string(b)
}
