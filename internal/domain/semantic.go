package domain

import (
	"time"
	"unicode/utf8"
)

const (
	MaxSemanticNameLength = 255

	MetricExpressionModeDSL = "DSL"
	MetricExpressionModeSQL = "SQL"

	MetricTypeSum           = "SUM"
	MetricTypeCount         = "COUNT"
	MetricTypeCountDistinct = "COUNT_DISTINCT"
	MetricTypeAverage       = "AVG"
	MetricTypeMin           = "MIN"
	MetricTypeMax           = "MAX"
	MetricTypeRatio         = "RATIO"

	RelationshipTypeOneToOne  = "ONE_TO_ONE"
	RelationshipTypeOneToMany = "ONE_TO_MANY"
	RelationshipTypeManyToOne = "MANY_TO_ONE"
	RelationshipTypeManyMany  = "MANY_TO_MANY"

	CertificationDraft      = "DRAFT"
	CertificationCertified  = "CERTIFIED"
	CertificationDeprecated = "DEPRECATED"
)

// SemanticModel defines business-facing semantic metadata anchored to a base model.
type SemanticModel struct {
	ID                   string
	ProjectName          string
	Name                 string
	Description          string
	Owner                string
	BaseModelRef         string
	DefaultTimeDimension string
	Tags                 []string
	CreatedBy            string
	CreatedAt            time.Time
	UpdatedAt            time.Time
}

// CreateSemanticModelRequest holds parameters for creating a semantic model.
type CreateSemanticModelRequest struct {
	ProjectName          string
	Name                 string
	Description          string
	BaseModelRef         string
	DefaultTimeDimension string
	Tags                 []string
}

// Validate checks that the request is well-formed.
func (r *CreateSemanticModelRequest) Validate() error {
	if r.ProjectName == "" {
		return ErrValidation("project_name is required")
	}
	if r.Name == "" {
		return ErrValidation("name is required")
	}
	if utf8.RuneCountInString(r.Name) > MaxSemanticNameLength {
		return ErrValidation("name must be <= %d characters", MaxSemanticNameLength)
	}
	if r.BaseModelRef == "" {
		return ErrValidation("base_model_ref is required")
	}
	return nil
}

// UpdateSemanticModelRequest holds partial-update parameters.
type UpdateSemanticModelRequest struct {
	Description          *string
	Owner                *string
	BaseModelRef         *string
	DefaultTimeDimension *string
	Tags                 []string // nil = no change, empty = clear
}

// SemanticMetric defines a business metric attached to a semantic model.
type SemanticMetric struct {
	ID                 string
	SemanticModelID    string
	Name               string
	Description        string
	MetricType         string
	ExpressionMode     string
	Expression         string
	DefaultTimeGrain   string
	Format             string
	Owner              string
	CertificationState string
	CreatedBy          string
	CreatedAt          time.Time
	UpdatedAt          time.Time
}

// CreateSemanticMetricRequest holds parameters for creating a metric.
type CreateSemanticMetricRequest struct {
	SemanticModelID    string
	Name               string
	Description        string
	MetricType         string
	ExpressionMode     string
	Expression         string
	DefaultTimeGrain   string
	Format             string
	CertificationState string
}

// Validate checks that the request is well-formed.
func (r *CreateSemanticMetricRequest) Validate() error {
	if r.SemanticModelID == "" {
		return ErrValidation("semantic_model_id is required")
	}
	if r.Name == "" {
		return ErrValidation("name is required")
	}
	if utf8.RuneCountInString(r.Name) > MaxSemanticNameLength {
		return ErrValidation("name must be <= %d characters", MaxSemanticNameLength)
	}
	validMetricTypes := map[string]bool{
		MetricTypeSum: true, MetricTypeCount: true, MetricTypeCountDistinct: true,
		MetricTypeAverage: true, MetricTypeMin: true, MetricTypeMax: true, MetricTypeRatio: true,
	}
	if !validMetricTypes[r.MetricType] {
		return ErrValidation("metric_type must be one of SUM, COUNT, COUNT_DISTINCT, AVG, MIN, MAX, RATIO")
	}
	if r.Expression == "" {
		return ErrValidation("expression is required")
	}
	if r.ExpressionMode == "" {
		r.ExpressionMode = MetricExpressionModeDSL
	}
	if r.ExpressionMode != MetricExpressionModeDSL && r.ExpressionMode != MetricExpressionModeSQL {
		return ErrValidation("expression_mode must be DSL or SQL")
	}
	if r.CertificationState == "" {
		r.CertificationState = CertificationDraft
	}
	if r.CertificationState != CertificationDraft && r.CertificationState != CertificationCertified && r.CertificationState != CertificationDeprecated {
		return ErrValidation("certification_state must be DRAFT, CERTIFIED, or DEPRECATED")
	}
	return nil
}

// UpdateSemanticMetricRequest holds partial-update parameters.
type UpdateSemanticMetricRequest struct {
	Description        *string
	MetricType         *string
	ExpressionMode     *string
	Expression         *string
	DefaultTimeGrain   *string
	Format             *string
	Owner              *string
	CertificationState *string
}

// SemanticRelationship defines a join edge between semantic models.
type SemanticRelationship struct {
	ID               string
	Name             string
	FromSemanticID   string
	ToSemanticID     string
	RelationshipType string
	JoinSQL          string
	IsDefault        bool
	Cost             int
	MaxHops          int
	CreatedBy        string
	CreatedAt        time.Time
	UpdatedAt        time.Time
}

// CreateSemanticRelationshipRequest holds parameters for creating a relationship.
type CreateSemanticRelationshipRequest struct {
	Name             string
	FromSemanticID   string
	ToSemanticID     string
	RelationshipType string
	JoinSQL          string
	IsDefault        bool
	Cost             int
	MaxHops          int
}

// Validate checks that the request is well-formed.
func (r *CreateSemanticRelationshipRequest) Validate() error {
	if r.Name == "" {
		return ErrValidation("name is required")
	}
	if r.FromSemanticID == "" {
		return ErrValidation("from_semantic_id is required")
	}
	if r.ToSemanticID == "" {
		return ErrValidation("to_semantic_id is required")
	}
	if r.JoinSQL == "" {
		return ErrValidation("join_sql is required")
	}
	validTypes := map[string]bool{
		RelationshipTypeOneToOne:  true,
		RelationshipTypeOneToMany: true,
		RelationshipTypeManyToOne: true,
		RelationshipTypeManyMany:  true,
	}
	if !validTypes[r.RelationshipType] {
		return ErrValidation("relationship_type must be ONE_TO_ONE, ONE_TO_MANY, MANY_TO_ONE, or MANY_TO_MANY")
	}
	if r.Cost < 0 {
		return ErrValidation("cost must be >= 0")
	}
	if r.MaxHops < 0 {
		return ErrValidation("max_hops must be >= 0")
	}
	return nil
}

// UpdateSemanticRelationshipRequest holds partial-update parameters.
type UpdateSemanticRelationshipRequest struct {
	RelationshipType *string
	JoinSQL          *string
	IsDefault        *bool
	Cost             *int
	MaxHops          *int
}

// SemanticPreAggregation maps metric/dimension sets to pre-aggregated relations.
type SemanticPreAggregation struct {
	ID              string
	SemanticModelID string
	Name            string
	MetricSet       []string
	DimensionSet    []string
	Grain           string
	TargetRelation  string
	RefreshPolicy   string
	CreatedBy       string
	CreatedAt       time.Time
	UpdatedAt       time.Time
}

// CreateSemanticPreAggregationRequest holds parameters for creating pre-aggregations.
type CreateSemanticPreAggregationRequest struct {
	SemanticModelID string
	Name            string
	MetricSet       []string
	DimensionSet    []string
	Grain           string
	TargetRelation  string
	RefreshPolicy   string
}

// Validate checks that the request is well-formed.
func (r *CreateSemanticPreAggregationRequest) Validate() error {
	if r.SemanticModelID == "" {
		return ErrValidation("semantic_model_id is required")
	}
	if r.Name == "" {
		return ErrValidation("name is required")
	}
	if r.TargetRelation == "" {
		return ErrValidation("target_relation is required")
	}
	return nil
}

// UpdateSemanticPreAggregationRequest holds partial-update parameters.
type UpdateSemanticPreAggregationRequest struct {
	MetricSet      []string
	DimensionSet   []string
	Grain          *string
	TargetRelation *string
	RefreshPolicy  *string
}
