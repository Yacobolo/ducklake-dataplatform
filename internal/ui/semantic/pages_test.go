package semantic

import (
	"bytes"
	"strings"
	"testing"

	"duck-demo/internal/domain"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	gomponents "maragu.dev/gomponents"
)

func TestSemanticModelDetailPage_RendersReadOnlyOverview(t *testing.T) {
	page := semanticModelDetailPage(semanticModelDetailPageData{
		Principal:           domain.ContextPrincipal{Name: "alice", Type: "user"},
		ProjectName:         "analytics",
		ModelName:           "sales",
		BaseModelRef:        "analytics.fct_sales",
		DefaultTimeDim:      "sales.order_date",
		Description:         "Sales semantic model",
		EditURL:             "/ui/semantic/models/analytics/sales/edit",
		DeleteURL:           "/ui/semantic/models/analytics/sales/delete",
		GraphNodesJSON:      `[{"id":"sales","label":"analytics.sales","role":"current","baseModelRef":"analytics.fct_sales","defaultTimeDimension":"sales.order_date","fields":[{"id":"sales_customer_id","label":"customer_id","meta":"sales","kind":"join","sortable":true},{"id":"sales_order_date","label":"order_date","meta":"sales","kind":"time","sortable":true}],"position":{"x":380,"y":140}},{"id":"customers","label":"analytics.customers","role":"outgoing","baseModelRef":"analytics.dim_customers","fields":[{"id":"customers_customer_id","label":"customer_id","meta":"customers","kind":"join","sortable":true}],"position":{"x":720,"y":40}}]`,
		GraphEdgesJSON:      `[{"id":"sales_to_customers","source":"sales","target":"customers","sourceHandle":"source:sales_customer_id","targetHandle":"target:customers_customer_id","name":"sales_to_customers","cardinality":"N:1","typeLabel":"MANY_TO_ONE","joinLabel":"customer_id = customer_id","sourceField":"customer_id","targetField":"customer_id","sourceFieldId":"sales_customer_id","targetFieldId":"customers_customer_id"}]`,
		RelationshipCount:   1,
		ConnectedModelCount: 1,
		RelatedRelationships: []semanticRelatedRelationshipRowData{
			{
				Name:            "sales_to_customers",
				RelatedRelation: "analytics.customers",
				Type:            "MANY_TO_ONE",
				Cardinality:     "N:1",
				JoinLabel:       "customer_id = customer_id",
				JoinSQL:         "sales.customer_id = customers.customer_id",
				EditURL:         "/ui/semantic/models/analytics/sales/edit",
			},
		},
		Metrics: []semanticMetricRowData{
			{Name: "total_revenue", Type: "SUM", Expression: "SUM(sales.amount)", RelationshipNames: []string{"sales_to_customers"}, Status: "CERTIFIED"},
		},
		CSRFFieldProvider: func() gomponents.Node { return nil },
	})

	var buf bytes.Buffer
	require.NoError(t, page.Render(&buf))
	html := buf.String()

	assert.Contains(t, html, "<semantic-model-flow")
	assert.Contains(t, html, "/ui/static/js/semantic-model-flow")
	assert.Contains(t, html, "Join paths")
	assert.Contains(t, html, "Metrics")
	assert.Contains(t, html, "Edit semantic model")
	assert.Contains(t, html, "Base relation analytics.fct_sales")
	assert.Contains(t, html, "1 connected relations")
	assert.Contains(t, html, "Join path")
	assert.Contains(t, html, "Related relation")
	assert.Contains(t, html, "sales_to_customers")
	assert.Contains(t, html, "analytics.customers")
	assert.Contains(t, html, "N:1")
	assert.NotContains(t, html, "New relationship")
	assert.NotContains(t, html, "New metric")
	assert.NotContains(t, html, "Advanced tools")
	assert.NotContains(t, html, "/ui/semantic/relationships")
	assert.NotContains(t, html, ">Relationships</a>")

	titleIndex := strings.Index(html, "analytics.sales")
	modelMapIndex := strings.Index(html, "Semantic model map")
	relationshipsIndex := strings.Index(html, "Join paths")
	metricsIndex := strings.Index(html, "Metrics")
	require.NotEqual(t, -1, titleIndex)
	require.NotEqual(t, -1, modelMapIndex)
	require.NotEqual(t, -1, relationshipsIndex)
	require.NotEqual(t, -1, metricsIndex)
	assert.Less(t, titleIndex, modelMapIndex)
	assert.Less(t, modelMapIndex, relationshipsIndex)
	assert.Less(t, relationshipsIndex, metricsIndex)
}

func TestSemanticModelEditPage_RendersModelScopedAuthoring(t *testing.T) {
	page := semanticModelEditPage(semanticModelEditPageData{
		Principal:             domain.ContextPrincipal{Name: "alice", Type: "user"},
		ProjectName:           "analytics",
		ModelName:             "sales",
		Description:           "Sales semantic model",
		BaseModelRef:          "analytics.fct_sales",
		DefaultTimeDim:        "sales.order_date",
		TagsCSV:               "finance,core",
		UpdateURL:             "/ui/semantic/models/analytics/sales/update",
		DeleteURL:             "/ui/semantic/models/analytics/sales/delete",
		RelationshipCreateURL: "/ui/semantic/models/analytics/sales/relationships",
		MetricsCreateURL:      "/ui/semantic/models/analytics/sales/metrics",
		PreAggCreateURL:       "/ui/semantic/models/analytics/sales/pre-aggregations",
		QueryExplainURL:       "/ui/semantic/query/explain",
		QueryRunURL:           "/ui/semantic/query/run",
		RelatedModelOptions: []semanticOptionData{
			{Value: "sm-customers", Label: "analytics.customers"},
		},
		Relationships: []semanticEditableRelationshipRowData{
			{
				Name:            "sales_to_customers",
				RelatedRelation: "analytics.customers",
				Type:            "MANY_TO_ONE",
				Cardinality:     "N:1",
				JoinSQL:         "sales.customer_id = customers.customer_id",
				Cost:            0,
				MaxHops:         0,
				UpdateURL:       "/ui/semantic/models/analytics/sales/relationships/sales_to_customers/update",
				DeleteURL:       "/ui/semantic/models/analytics/sales/relationships/sales_to_customers/delete",
			},
		},
		Metrics: []semanticMetricRowData{
			{Name: "total_revenue", Type: "SUM", Expression: "SUM(sales.amount)", RelationshipNames: []string{"sales_to_customers"}, Status: "CERTIFIED", EditURL: "/edit", DeleteURL: "/delete"},
		},
		PreAggregations: []semanticPreAggRowData{
			{Name: "daily_sales_summary", Grain: "day", Target: "analytics.daily_sales_summary", EditURL: "/edit", DeleteURL: "/delete"},
		},
		CSRFFieldProvider: func() gomponents.Node { return nil },
	})

	var buf bytes.Buffer
	require.NoError(t, page.Render(&buf))
	html := buf.String()

	assert.Contains(t, html, "Back to overview")
	assert.Contains(t, html, "Semantic model metadata")
	assert.Contains(t, html, "Join paths")
	assert.Contains(t, html, "New join path")
	assert.Contains(t, html, "Base relation reference")
	assert.Contains(t, html, "Related relation")
	assert.Contains(t, html, "Create join path")
	assert.Contains(t, html, "Join paths (comma separated)")
	assert.Contains(t, html, "Metrics")
	assert.Contains(t, html, "New metric")
	assert.Contains(t, html, "Advanced tools")
	assert.Contains(t, html, "Pre-aggregations and query tools")
	assert.Contains(t, html, "Create pre-aggregation")
	assert.NotContains(t, html, "/ui/semantic/relationships")
	assert.NotContains(t, html, ">Relationships</a>")

	metadataIndex := strings.Index(html, "Semantic model metadata")
	relationshipsIndex := strings.Index(html, "Join paths")
	metricsIndex := strings.Index(html, "Metrics")
	advancedIndex := strings.Index(html, "Advanced tools")
	require.NotEqual(t, -1, metadataIndex)
	require.NotEqual(t, -1, relationshipsIndex)
	require.NotEqual(t, -1, metricsIndex)
	require.NotEqual(t, -1, advancedIndex)
	assert.Less(t, metadataIndex, relationshipsIndex)
	assert.Less(t, relationshipsIndex, metricsIndex)
	assert.Less(t, metricsIndex, advancedIndex)
}
