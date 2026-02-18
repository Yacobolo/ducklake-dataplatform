package repository

import (
	"encoding/json"
	"log/slog"
	"time"

	"duck-demo/internal/db/dbstore"
	"duck-demo/internal/domain"
)

const dbTimeLayout = "2006-01-02 15:04:05"

func parseDBTime(value string, field string) time.Time {
	ts, err := time.Parse(dbTimeLayout, value)
	if err != nil {
		slog.Default().Warn("failed to parse db timestamp", "field", field, "value", value, "error", err)
	}
	return ts
}

func semanticModelFromDB(row dbstore.SemanticModel) *domain.SemanticModel {
	var tags []string
	if row.Tags != "" {
		if err := json.Unmarshal([]byte(row.Tags), &tags); err != nil {
			slog.Default().Warn("failed to unmarshal semantic model tags", "value", row.Tags, "error", err)
		}
	}
	return &domain.SemanticModel{
		ID:                   row.ID,
		ProjectName:          row.ProjectName,
		Name:                 row.Name,
		Description:          row.Description,
		Owner:                row.Owner,
		BaseModelRef:         row.BaseModelRef,
		DefaultTimeDimension: row.DefaultTimeDimension,
		Tags:                 tags,
		CreatedBy:            row.CreatedBy,
		CreatedAt:            parseDBTime(row.CreatedAt, "semantic_models.created_at"),
		UpdatedAt:            parseDBTime(row.UpdatedAt, "semantic_models.updated_at"),
	}
}

func semanticMetricFromDB(row dbstore.SemanticMetric) *domain.SemanticMetric {
	return &domain.SemanticMetric{
		ID:                 row.ID,
		SemanticModelID:    row.SemanticModelID,
		Name:               row.Name,
		Description:        row.Description,
		MetricType:         row.MetricType,
		ExpressionMode:     row.ExpressionMode,
		Expression:         row.Expression,
		DefaultTimeGrain:   row.DefaultTimeGrain,
		Format:             row.Format,
		Owner:              row.Owner,
		CertificationState: row.CertificationState,
		CreatedBy:          row.CreatedBy,
		CreatedAt:          parseDBTime(row.CreatedAt, "semantic_metrics.created_at"),
		UpdatedAt:          parseDBTime(row.UpdatedAt, "semantic_metrics.updated_at"),
	}
}

func semanticRelationshipFromDB(row dbstore.SemanticRelationship) *domain.SemanticRelationship {
	return &domain.SemanticRelationship{
		ID:               row.ID,
		Name:             row.Name,
		FromSemanticID:   row.FromSemanticID,
		ToSemanticID:     row.ToSemanticID,
		RelationshipType: row.RelationshipType,
		JoinSQL:          row.JoinSql,
		IsDefault:        row.IsDefault == 1,
		Cost:             int(row.Cost),
		MaxHops:          int(row.MaxHops),
		CreatedBy:        row.CreatedBy,
		CreatedAt:        parseDBTime(row.CreatedAt, "semantic_relationships.created_at"),
		UpdatedAt:        parseDBTime(row.UpdatedAt, "semantic_relationships.updated_at"),
	}
}

func semanticPreAggregationFromDB(row dbstore.SemanticPreAggregation) *domain.SemanticPreAggregation {
	var metrics []string
	if row.MetricSet != "" {
		if err := json.Unmarshal([]byte(row.MetricSet), &metrics); err != nil {
			slog.Default().Warn("failed to unmarshal pre-aggregation metric_set", "value", row.MetricSet, "error", err)
		}
	}
	var dimensions []string
	if row.DimensionSet != "" {
		if err := json.Unmarshal([]byte(row.DimensionSet), &dimensions); err != nil {
			slog.Default().Warn("failed to unmarshal pre-aggregation dimension_set", "value", row.DimensionSet, "error", err)
		}
	}

	return &domain.SemanticPreAggregation{
		ID:              row.ID,
		SemanticModelID: row.SemanticModelID,
		Name:            row.Name,
		MetricSet:       metrics,
		DimensionSet:    dimensions,
		Grain:           row.Grain,
		TargetRelation:  row.TargetRelation,
		RefreshPolicy:   row.RefreshPolicy,
		CreatedBy:       row.CreatedBy,
		CreatedAt:       parseDBTime(row.CreatedAt, "semantic_pre_aggregations.created_at"),
		UpdatedAt:       parseDBTime(row.UpdatedAt, "semantic_pre_aggregations.updated_at"),
	}
}
