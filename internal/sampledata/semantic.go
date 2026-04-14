package sampledata

import (
	"context"
	"errors"

	"github.com/Yacobolo/quackstack/internal/domain"
	semanticsvc "github.com/Yacobolo/quackstack/internal/service/semantic"
)

const sampleDashboardSemanticDescription = "Semantic layer entrypoint for the built-in NYC taxi dashboard review surface."

func ensureSampleDashboardSemanticModel(ctx context.Context, semanticSvc *semanticsvc.Service, workspaces domain.WorkspaceRepository) error {
	if semanticSvc == nil {
		return nil
	}
	if workspaces == nil {
		return domain.ErrValidation("workspace repository is required")
	}
	workspace, err := workspaces.GetPersonalByPrincipal(ctx, sampleDashboardOwner)
	if err != nil {
		var notFoundErr *domain.NotFoundError
		if !errors.As(err, &notFoundErr) {
			return err
		}
		workspace, err = workspaces.Create(ctx, &domain.Workspace{
			Name:           sampleDashboardOwner + " workspace",
			Kind:           domain.WorkspaceKindPersonal,
			OwnerPrincipal: strPtr(sampleDashboardOwner),
			CreatedBy:      sampleDashboardOwner,
		})
		if err != nil {
			return err
		}
	}

	model, err := semanticSvc.GetSemanticModelByName(ctx, workspace.ID, sampleDashboardSemanticModel)
	if err != nil {
		var notFoundErr *domain.NotFoundError
		if !errors.As(err, &notFoundErr) {
			return err
		}
		model, err = semanticSvc.CreateSemanticModel(ctx, sampleDashboardOwner, domain.CreateSemanticModelRequest{
			WorkspaceID:          workspace.ID,
			Name:                 sampleDashboardSemanticModel,
			Description:          sampleDashboardSemanticDescription,
			BaseRelationRef:      "sample_data.nyc_taxi.dashboard_metrics",
			DefaultTimeDimension: "pickup_date",
		})
		if err != nil {
			return err
		}
	} else if model.Description != sampleDashboardSemanticDescription || model.BaseRelationRef != "sample_data.nyc_taxi.dashboard_metrics" || model.DefaultTimeDimension != "pickup_date" {
		model, err = semanticSvc.UpdateSemanticModel(ctx, workspace.ID, model.ID, domain.UpdateSemanticModelRequest{
			Description:          strPtr(sampleDashboardSemanticDescription),
			BaseRelationRef:      strPtr("sample_data.nyc_taxi.dashboard_metrics"),
			DefaultTimeDimension: strPtr("pickup_date"),
		})
		if err != nil {
			return err
		}
	}

	return ensureSampleDashboardSemanticMetrics(ctx, semanticSvc, model)
}

func ensureSampleDashboardSemanticMetrics(ctx context.Context, semanticSvc *semanticsvc.Service, model *domain.SemanticModel) error {
	existing, err := semanticSvc.ListMetrics(ctx, model.WorkspaceID, model.ID)
	if err != nil {
		return err
	}

	byName := make(map[string]domain.SemanticMetric, len(existing))
	for _, metric := range existing {
		byName[metric.Name] = metric
	}

	for _, desired := range []domain.CreateSemanticMetricRequest{
		{
			SemanticModelID:    model.ID,
			Name:               "gross_revenue",
			Description:        "Sum of gross revenue at the dashboard review grain.",
			Label:              "Gross Revenue",
			MetricType:         domain.MetricTypeSum,
			ExpressionMode:     domain.MetricExpressionModeSQL,
			Expression:         "SUM(gross_revenue)",
			DefaultTimeGrain:   "day",
			CertificationState: domain.CertificationCertified,
		},
		{
			SemanticModelID:    model.ID,
			Name:               "trip_count",
			Description:        "Sum of trip volume at the dashboard review grain.",
			Label:              "Trip Count",
			MetricType:         domain.MetricTypeSum,
			ExpressionMode:     domain.MetricExpressionModeSQL,
			Expression:         "SUM(trip_count)",
			DefaultTimeGrain:   "day",
			CertificationState: domain.CertificationCertified,
		},
	} {
		current, ok := byName[desired.Name]
		if !ok {
			if _, err := semanticSvc.CreateMetric(ctx, sampleDashboardOwner, model.WorkspaceID, model.ID, desired); err != nil {
				return err
			}
			continue
		}
		if current.Description == desired.Description &&
			current.Label == desired.Label &&
			current.MetricType == desired.MetricType &&
			current.ExpressionMode == desired.ExpressionMode &&
			current.Expression == desired.Expression &&
			current.DefaultTimeGrain == desired.DefaultTimeGrain &&
			current.CertificationState == desired.CertificationState {
			continue
		}
		if _, err := semanticSvc.UpdateMetric(ctx, model.WorkspaceID, model.ID, desired.Name, domain.UpdateSemanticMetricRequest{
			Description:        strPtr(desired.Description),
			Label:              strPtr(desired.Label),
			MetricType:         strPtr(desired.MetricType),
			ExpressionMode:     strPtr(desired.ExpressionMode),
			Expression:         strPtr(desired.Expression),
			DefaultTimeGrain:   strPtr(desired.DefaultTimeGrain),
			CertificationState: strPtr(desired.CertificationState),
		}); err != nil {
			return err
		}
	}

	return nil
}
