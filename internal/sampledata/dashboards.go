package sampledata

import (
	"context"
	"reflect"

	"duck-demo/internal/domain"
	"duck-demo/internal/service/dashboard"
)

const (
	sampleDashboardOwner = "dev-admin"
	sampleDashboardName  = "NYC Taxi Ops Overview"
)

func ensureSampleDashboards(ctx context.Context, dashboardSvc *dashboard.Service) error {
	if dashboardSvc == nil {
		return nil
	}

	item, err := ensureSampleDashboard(ctx, dashboardSvc)
	if err != nil {
		return err
	}

	for _, widget := range sampleDashboardWidgets(item.ID) {
		if err := ensureSampleDashboardWidget(ctx, dashboardSvc, item, widget); err != nil {
			return err
		}
	}

	return nil
}

type sampleAuditRepo struct{}

func (sampleAuditRepo) Insert(context.Context, *domain.AuditEntry) error { return nil }

func (sampleAuditRepo) List(context.Context, domain.AuditFilter) ([]domain.AuditEntry, int64, error) {
	return nil, 0, nil
}

func ensureSampleDashboard(ctx context.Context, dashboardSvc *dashboard.Service) (*domain.Dashboard, error) {
	items, _, err := dashboardSvc.ListDashboards(ctx, nil, domain.PageRequest{MaxResults: domain.MaxMaxResults})
	if err != nil {
		return nil, err
	}

	for _, item := range items {
		if item.Name == sampleDashboardName && item.Owner == sampleDashboardOwner {
			if item.Description != sampleDashboardDescription {
				updated, err := dashboardSvc.UpdateDashboard(ctx, sampleDashboardOwner, true, item.ID, domain.UpdateDashboardRequest{
					Description: strPtr(sampleDashboardDescription),
				})
				if err != nil {
					return nil, err
				}
				return updated, nil
			}
			return &item, nil
		}
	}

	return dashboardSvc.CreateDashboard(ctx, sampleDashboardOwner, domain.CreateDashboardRequest{
		Name:        sampleDashboardName,
		Description: sampleDashboardDescription,
	})
}

func ensureSampleDashboardWidget(ctx context.Context, dashboardSvc *dashboard.Service, item *domain.Dashboard, desired sampleDashboardWidget) error {
	_, widgets, err := dashboardSvc.GetDashboard(ctx, item.ID)
	if err != nil {
		return err
	}

	for _, widget := range widgets {
		if widget.Name != desired.Name {
			continue
		}
		if widget.Description == desired.Description &&
			reflect.DeepEqual(widget.Source, desired.Source) &&
			reflect.DeepEqual(widget.VisualSpec, desired.VisualSpec) &&
			reflect.DeepEqual(widget.Layout, desired.Layout) {
			return nil
		}

		name := desired.Name
		description := desired.Description
		source := desired.Source
		visual := desired.VisualSpec
		layout := desired.Layout
		_, err := dashboardSvc.UpdateWidget(ctx, sampleDashboardOwner, true, widget.ID, domain.UpdateDashboardWidgetRequest{
			Name:        &name,
			Description: &description,
			Source:      &source,
			VisualSpec:  visual,
			Layout:      &layout,
		})
		return err
	}

	_, err = dashboardSvc.CreateWidget(ctx, sampleDashboardOwner, true, item.ID, domain.CreateDashboardWidgetRequest{
		Name:        desired.Name,
		Description: desired.Description,
		Source:      desired.Source,
		VisualSpec:  desired.VisualSpec,
		Layout:      desired.Layout,
	})
	return err
}

type sampleDashboardWidget struct {
	Name        string
	Description string
	Source      domain.DashboardWidgetSource
	VisualSpec  *domain.VisualSpec
	Layout      domain.DashboardWidgetLayout
}

const sampleDashboardDescription = "Seeded from the built-in NYC taxi sample catalog so local dashboard reviews start with charts, KPIs, and ranking views instead of an empty state."

func sampleDashboardWidgets(_ string) []sampleDashboardWidget {
	lineChart := domain.VisualChartLine
	barChart := domain.VisualChartBar
	doughnutChart := domain.VisualChartDoughnut

	return []sampleDashboardWidget{
		{
			Name:        "Total Revenue",
			Description: "Gross revenue summed across the full sample period.",
			Source: domain.DashboardWidgetSource{
				Kind: domain.DashboardWidgetSourceSQLQuery,
				SQLQuery: &domain.DashboardSQLQuerySource{
					SQL: `SELECT ROUND(SUM(gross_revenue), 2) AS total_revenue
FROM sample_data.nyc_taxi.daily_metrics`,
				},
			},
			VisualSpec: &domain.VisualSpec{
				Kind:  domain.VisualOutputMetric,
				Title: "Gross Revenue",
				Encodings: domain.VisualEncodings{
					Value: &domain.VisualFieldBinding{Field: "total_revenue"},
				},
			},
			Layout: domain.DashboardWidgetLayout{X: 0, Y: 0, W: 3, H: 2},
		},
		{
			Name:        "Trips by Day",
			Description: "Daily ride volume across the seeded January taxi sample.",
			Source: domain.DashboardWidgetSource{
				Kind: domain.DashboardWidgetSourceSQLQuery,
				SQLQuery: &domain.DashboardSQLQuerySource{
					SQL: `SELECT pickup_date, trip_count
FROM sample_data.nyc_taxi.daily_metrics
ORDER BY pickup_date`,
				},
			},
			VisualSpec: &domain.VisualSpec{
				Kind:      domain.VisualOutputChart,
				ChartType: &lineChart,
				Title:     "Trips by Day",
				Encodings: domain.VisualEncodings{
					X: &domain.VisualFieldBinding{Field: "pickup_date"},
					Y: &domain.VisualFieldBinding{Field: "trip_count"},
				},
			},
			Layout: domain.DashboardWidgetLayout{X: 3, Y: 0, W: 9, H: 4},
		},
		{
			Name:        "Revenue by Borough",
			Description: "Which pickup boroughs contribute the most revenue.",
			Source: domain.DashboardWidgetSource{
				Kind: domain.DashboardWidgetSourceSQLQuery,
				SQLQuery: &domain.DashboardSQLQuerySource{
					SQL: `SELECT borough, ROUND(SUM(gross_revenue), 2) AS gross_revenue
FROM sample_data.nyc_taxi.zone_metrics
GROUP BY borough
ORDER BY gross_revenue DESC`,
				},
			},
			VisualSpec: &domain.VisualSpec{
				Kind:      domain.VisualOutputChart,
				ChartType: &barChart,
				Title:     "Revenue by Borough",
				Encodings: domain.VisualEncodings{
					X: &domain.VisualFieldBinding{Field: "borough"},
					Y: &domain.VisualFieldBinding{Field: "gross_revenue"},
				},
			},
			Layout: domain.DashboardWidgetLayout{X: 0, Y: 4, W: 6, H: 4},
		},
		{
			Name:        "Top Pickup Zones",
			Description: "Highest-revenue pickup zones for a quick ranking cut.",
			Source: domain.DashboardWidgetSource{
				Kind: domain.DashboardWidgetSourceSQLQuery,
				SQLQuery: &domain.DashboardSQLQuerySource{
					SQL: `SELECT pickup_zone, gross_revenue
FROM sample_data.nyc_taxi.zone_metrics
ORDER BY gross_revenue DESC
LIMIT 8`,
				},
			},
			VisualSpec: &domain.VisualSpec{
				Kind:      domain.VisualOutputChart,
				ChartType: &doughnutChart,
				Title:     "Top Pickup Zones",
				Encodings: domain.VisualEncodings{
					Label: &domain.VisualFieldBinding{Field: "pickup_zone"},
					Value: &domain.VisualFieldBinding{Field: "gross_revenue"},
				},
			},
			Layout: domain.DashboardWidgetLayout{X: 6, Y: 4, W: 6, H: 4},
		},
	}
}
