package semantic

import (
	"testing"

	"duck-demo/internal/domain"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBuildSemanticModelFlowData_NoRelationships(t *testing.T) {
	current := semanticTestModel("model-sales", "sales")

	flow, rows := buildSemanticModelFlowData(current, nil, nil)

	require.Len(t, flow.Nodes, 1)
	assert.Equal(t, current.ID, flow.Nodes[0].ID)
	assert.Equal(t, "current", flow.Nodes[0].Role)
	require.Len(t, flow.Nodes[0].Fields, 1)
	assert.Equal(t, "No join columns yet", flow.Nodes[0].Fields[0].Label)
	assert.Equal(t, "empty", flow.Nodes[0].Fields[0].Kind)
	assert.Empty(t, flow.Edges)
	assert.Empty(t, rows)
}

func TestBuildSemanticModelFlowData_DirectRelationshipsOnly(t *testing.T) {
	current := semanticTestModel("model-sales", "sales")
	customers := semanticTestModel("model-customers", "customers")
	regions := semanticTestModel("model-regions", "regions")
	campaigns := semanticTestModel("model-campaigns", "campaigns")

	flow, rows := buildSemanticModelFlowData(current,
		[]domain.SemanticModel{customers, regions, campaigns},
		[]domain.SemanticRelationship{
			{
				Name:             "sales_to_customers",
				FromSemanticID:   current.ID,
				ToSemanticID:     customers.ID,
				RelationshipType: domain.RelationshipTypeManyToOne,
				JoinSQL:          "sales.customer_id = customers.customer_id",
			},
			{
				Name:             "regions_to_sales",
				FromSemanticID:   regions.ID,
				ToSemanticID:     current.ID,
				RelationshipType: domain.RelationshipTypeOneToMany,
				JoinSQL:          "regions.region_id = sales.region_id",
			},
			{
				Name:             "customers_to_campaigns",
				FromSemanticID:   customers.ID,
				ToSemanticID:     campaigns.ID,
				RelationshipType: domain.RelationshipTypeOneToOne,
				JoinSQL:          "customers.id = campaigns.customer_id",
			},
		},
	)

	require.Len(t, flow.Nodes, 2)
	assert.ElementsMatch(t, []string{current.ID, customers.ID}, semanticNodeIDs(flow.Nodes))
	require.Len(t, flow.Edges, 1)
	assert.ElementsMatch(t, []string{"sales_to_customers"}, semanticEdgeIDs(flow.Edges))
	assert.Equal(t, "source:sales_customer_id", semanticEdgeByID(flow.Edges, "sales_to_customers").SourceHandle)
	assert.Equal(t, "target:customers_customer_id", semanticEdgeByID(flow.Edges, "sales_to_customers").TargetHandle)
	assert.Contains(t, semanticNodeFieldLabels(flow.Nodes, current.ID), "customer_id")
	assert.Contains(t, semanticNodeFieldLabels(flow.Nodes, customers.ID), "customer_id")

	require.Len(t, rows, 1)
	assert.Equal(t, "sales_to_customers", rows[0].Name)
	assert.Equal(t, "customers", rows[0].RelatedRelation)
	assert.Equal(t, "N:1", rows[0].Cardinality)
	assert.Equal(t, "customer_id = customer_id", rows[0].JoinLabel)
	assert.Equal(t, "customer_id", rows[0].SourceField)
	assert.Equal(t, "customer_id", rows[0].TargetField)
}

func TestBuildSemanticModelFlowData_AssignsOutgoingRoles(t *testing.T) {
	current := semanticTestModel("model-sales", "sales")
	customers := semanticTestModel("model-customers", "customers")
	regions := semanticTestModel("model-regions", "regions")
	products := semanticTestModel("model-products", "products")

	flow, _ := buildSemanticModelFlowData(current,
		[]domain.SemanticModel{customers, regions, products},
		[]domain.SemanticRelationship{
			{
				Name:             "sales_to_customers",
				FromSemanticID:   current.ID,
				ToSemanticID:     customers.ID,
				RelationshipType: domain.RelationshipTypeManyToOne,
				JoinSQL:          "sales.customer_id = customers.customer_id",
			},
			{
				Name:             "sales_to_products",
				FromSemanticID:   current.ID,
				ToSemanticID:     products.ID,
				RelationshipType: domain.RelationshipTypeManyToOne,
				JoinSQL:          "sales.product_id = products.product_id",
			},
		},
	)

	assert.Equal(t, "outgoing", semanticNodeRole(flow.Nodes, customers.ID))
	assert.Empty(t, semanticNodeRole(flow.Nodes, regions.ID))
	assert.Equal(t, "outgoing", semanticNodeRole(flow.Nodes, products.ID))
}

func TestSemanticJoinLabel_SimpleJoinWhitespace(t *testing.T) {
	assert.Equal(t, "customer_id = customer_id", semanticJoinLabel("  sales.customer_id   =   customers.customer_id  "))
}

func TestSemanticJoinLabel_FallsBackToCompactSQL(t *testing.T) {
	assert.Equal(t, "sales.customer_id = customers.customer_id AND customers.is_active = true", semanticJoinLabel(" sales.customer_id = customers.customer_id\n AND customers.is_active = true "))
}

func TestParseSemanticJoinOperands_SimpleJoin(t *testing.T) {
	source, target, ok := parseSemanticJoinOperands(" sales.customer_id = customers.customer_id ")

	assert.True(t, ok)
	assert.Equal(t, "sales.customer_id", source)
	assert.Equal(t, "customers.customer_id", target)
}

func TestParseSemanticJoinOperands_RejectsComplexJoin(t *testing.T) {
	_, _, ok := parseSemanticJoinOperands("sales.customer_id = customers.customer_id AND customers.is_active = true")

	assert.False(t, ok)
}

func semanticTestModel(id, name string) domain.SemanticModel {
	return domain.SemanticModel{
		ID:   id,
		Name: name,
	}
}

func semanticNodeIDs(nodes []semanticFlowNodeData) []string {
	ids := make([]string, 0, len(nodes))
	for i := range nodes {
		ids = append(ids, nodes[i].ID)
	}
	return ids
}

func semanticEdgeIDs(edges []semanticFlowEdgeData) []string {
	ids := make([]string, 0, len(edges))
	for i := range edges {
		ids = append(ids, edges[i].ID)
	}
	return ids
}

func semanticNodeRole(nodes []semanticFlowNodeData, id string) string {
	for i := range nodes {
		if nodes[i].ID == id {
			return nodes[i].Role
		}
	}
	return ""
}

func semanticNodeFieldLabels(nodes []semanticFlowNodeData, id string) []string {
	for i := range nodes {
		if nodes[i].ID != id {
			continue
		}
		labels := make([]string, 0, len(nodes[i].Fields))
		for j := range nodes[i].Fields {
			labels = append(labels, nodes[i].Fields[j].Label)
		}
		return labels
	}
	return nil
}

func semanticEdgeByID(edges []semanticFlowEdgeData, id string) semanticFlowEdgeData {
	for i := range edges {
		if edges[i].ID == id {
			return edges[i]
		}
	}
	return semanticFlowEdgeData{}
}
