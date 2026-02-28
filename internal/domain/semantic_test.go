package domain

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCreateSemanticModelRequest_Validate(t *testing.T) {
	req := CreateSemanticModelRequest{
		ProjectName:  "analytics",
		Name:         "sales",
		BaseModelRef: "analytics.fct_sales",
	}

	require.NoError(t, req.Validate())
}

func TestCreateSemanticMetricRequest_Validate_Defaults(t *testing.T) {
	req := CreateSemanticMetricRequest{
		SemanticModelID: "semantic-id",
		Name:            "total_revenue",
		MetricType:      MetricTypeSum,
		Expression:      "sum(amount)",
	}

	require.NoError(t, req.Validate())
	require.Equal(t, MetricExpressionModeDSL, req.ExpressionMode)
	require.Equal(t, CertificationDraft, req.CertificationState)
}

func TestCreateSemanticRelationshipRequest_Validate(t *testing.T) {
	req := CreateSemanticRelationshipRequest{
		Name:             "sales_to_customers",
		FromSemanticID:   "from-id",
		ToSemanticID:     "to-id",
		RelationshipType: RelationshipTypeManyToOne,
		JoinSQL:          "sales.customer_id = customers.id",
		Cost:             1,
		MaxHops:          2,
	}

	require.NoError(t, req.Validate())
}
