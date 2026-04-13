package sampledata

import (
	"context"
	"reflect"

	"duck-demo/internal/domain"
	"duck-demo/internal/service/dashboard"
)

const (
	sampleDashboardOwner         = "dev-admin"
	sampleDashboardName          = "NYC Taxi Ops Overview"
	sampleChartLabDashboardName  = "NYC Taxi Chart Lab"
	sampleDashboardSemanticProj  = "sample_data"
	sampleDashboardSemanticModel = "dashboard_metrics"
)

func ensureSampleDashboards(ctx context.Context, dashboardSvc *dashboard.Service) error {
	if dashboardSvc == nil {
		return nil
	}

	for _, definition := range sampleDashboardDefinitions() {
		item, err := ensureSampleDashboard(ctx, dashboardSvc, definition.Name, definition.Description)
		if err != nil {
			return err
		}

		for _, widget := range definition.Widgets(item.ID) {
			if err := ensureSampleDashboardWidget(ctx, dashboardSvc, item, widget); err != nil {
				return err
			}
		}
	}
	return nil
}

type sampleAuditRepo struct{}

func (sampleAuditRepo) Insert(context.Context, *domain.AuditEntry) error { return nil }

func (sampleAuditRepo) List(context.Context, domain.AuditFilter) ([]domain.AuditEntry, int64, error) {
	return nil, 0, nil
}

func ensureSampleDashboard(ctx context.Context, dashboardSvc *dashboard.Service, name, description string) (*domain.Dashboard, error) {
	items, _, err := dashboardSvc.ListDashboards(ctx, nil, domain.PageRequest{MaxResults: domain.MaxMaxResults})
	if err != nil {
		return nil, err
	}

	for _, item := range items {
		if item.Name == name && item.Owner == sampleDashboardOwner {
			if item.Description != description ||
				item.SemanticProjectName != sampleDashboardSemanticProj ||
				item.SemanticModelName != sampleDashboardSemanticModel {
				updated, err := dashboardSvc.UpdateDashboard(ctx, sampleDashboardOwner, true, item.ID, domain.UpdateDashboardRequest{
					Description:         strPtr(description),
					SemanticProjectName: strPtr(sampleDashboardSemanticProj),
					SemanticModelName:   strPtr(sampleDashboardSemanticModel),
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
		Name:                name,
		Description:         description,
		SemanticProjectName: sampleDashboardSemanticProj,
		SemanticModelName:   sampleDashboardSemanticModel,
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
			domain.NormalizeDashboardPageName(widget.PageName) == domain.NormalizeDashboardPageName(desired.PageName) &&
			reflect.DeepEqual(widget.Source, desired.Source) &&
			reflect.DeepEqual(widget.VisualSpec, desired.VisualSpec) &&
			reflect.DeepEqual(widget.Layout, desired.Layout) {
			return nil
		}

		name := desired.Name
		pageName := desired.PageName
		description := desired.Description
		source := desired.Source
		visual := desired.VisualSpec
		layout := desired.Layout
		_, err := dashboardSvc.UpdateWidget(ctx, sampleDashboardOwner, true, widget.ID, domain.UpdateDashboardWidgetRequest{
			PageName:    &pageName,
			Name:        &name,
			Description: &description,
			Source:      &source,
			VisualSpec:  visual,
			Layout:      &layout,
		})
		return err
	}

	_, err = dashboardSvc.CreateWidget(ctx, sampleDashboardOwner, true, item.ID, domain.CreateDashboardWidgetRequest{
		PageName:    desired.PageName,
		Name:        desired.Name,
		Description: desired.Description,
		Source:      desired.Source,
		VisualSpec:  desired.VisualSpec,
		Layout:      desired.Layout,
	})
	return err
}

type sampleDashboardWidget struct {
	PageName    string
	Name        string
	Description string
	Source      domain.DashboardWidgetSource
	VisualSpec  *domain.VisualSpec
	Layout      domain.DashboardWidgetLayout
}

const sampleDashboardDescription = "Seeded from the built-in NYC taxi sample catalog so local dashboard reviews start with charts, KPIs, and ranking views instead of an empty state."
const sampleChartLabDashboardDescription = "Validation-first widget gallery covering every supported chart renderer plus the interactive detail table."

type sampleDashboardDefinition struct {
	Name        string
	Description string
	Widgets     func(string) []sampleDashboardWidget
}

func sampleDashboardDefinitions() []sampleDashboardDefinition {
	return []sampleDashboardDefinition{
		{
			Name:        sampleDashboardName,
			Description: sampleDashboardDescription,
			Widgets:     sampleDashboardWidgets,
		},
		{
			Name:        sampleChartLabDashboardName,
			Description: sampleChartLabDashboardDescription,
			Widgets:     sampleChartLabDashboardWidgets,
		},
	}
}

func sampleDashboardWidgets(_ string) []sampleDashboardWidget {
	lineChart := domain.VisualChartLine
	barChart := domain.VisualChartBar
	doughnutChart := domain.VisualChartDoughnut

	return []sampleDashboardWidget{
		{
			PageName:    "Overview",
			Name:        "Total Revenue",
			Description: "Gross revenue summed across the full sample period.",
			Source: domain.DashboardWidgetSource{
				Kind: domain.DashboardWidgetSourceSemanticQuery,
				SemanticQuery: &domain.DashboardSemanticQuerySource{
					ProjectName:       sampleDashboardSemanticProj,
					SemanticModelName: sampleDashboardSemanticModel,
					Metrics:           []string{"gross_revenue"},
				},
			},
			VisualSpec: &domain.VisualSpec{
				Kind:  domain.VisualOutputMetric,
				Title: "Gross Revenue",
				Encodings: domain.VisualEncodings{
					Value: &domain.VisualFieldBinding{Field: "gross_revenue"},
				},
			},
			Layout: domain.DashboardWidgetLayout{X: 0, Y: 0, W: 3, H: 2},
		},
		{
			PageName:    "Overview",
			Name:        "Trips by Day",
			Description: "Daily ride volume across the seeded January taxi sample.",
			Source: domain.DashboardWidgetSource{
				Kind: domain.DashboardWidgetSourceSemanticQuery,
				SemanticQuery: &domain.DashboardSemanticQuerySource{
					ProjectName:       sampleDashboardSemanticProj,
					SemanticModelName: sampleDashboardSemanticModel,
					Metrics:           []string{"trip_count"},
					Dimensions:        []string{"pickup_date"},
					OrderBy:           []string{"pickup_date"},
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
			PageName:    "Geography",
			Name:        "Revenue by Borough",
			Description: "Which pickup boroughs contribute the most revenue.",
			Source: domain.DashboardWidgetSource{
				Kind: domain.DashboardWidgetSourceSemanticQuery,
				SemanticQuery: &domain.DashboardSemanticQuerySource{
					ProjectName:       sampleDashboardSemanticProj,
					SemanticModelName: sampleDashboardSemanticModel,
					Metrics:           []string{"gross_revenue"},
					Dimensions:        []string{"borough"},
					OrderBy:           []string{"gross_revenue DESC"},
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
			PageName:    "Geography",
			Name:        "Top Pickup Zones",
			Description: "Highest-revenue pickup zones for a quick ranking cut.",
			Source: domain.DashboardWidgetSource{
				Kind: domain.DashboardWidgetSourceSemanticQuery,
				SemanticQuery: &domain.DashboardSemanticQuerySource{
					ProjectName:       sampleDashboardSemanticProj,
					SemanticModelName: sampleDashboardSemanticModel,
					Metrics:           []string{"gross_revenue"},
					Dimensions:        []string{"pickup_zone"},
					OrderBy:           []string{"gross_revenue DESC"},
					Limit:             intPtr(8),
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
		{
			PageName:    "Geography",
			Name:        "Zone Revenue Detail",
			Description: "Tabular ranking of the top pickup zones with borough context, trip volume, and revenue.",
			Source: domain.DashboardWidgetSource{
				Kind: domain.DashboardWidgetSourceSemanticQuery,
				SemanticQuery: &domain.DashboardSemanticQuerySource{
					ProjectName:       sampleDashboardSemanticProj,
					SemanticModelName: sampleDashboardSemanticModel,
					Metrics:           []string{"gross_revenue", "trip_count"},
					Dimensions:        []string{"pickup_zone", "borough"},
					OrderBy:           []string{"gross_revenue DESC"},
				},
			},
			VisualSpec: &domain.VisualSpec{
				Kind:  domain.VisualOutputTable,
				Title: "Zone Revenue Detail",
			},
			Layout: domain.DashboardWidgetLayout{X: 0, Y: 8, W: 12, H: 4},
		},
	}
}

func sampleChartLabDashboardWidgets(_ string) []sampleDashboardWidget {
	lineChart := domain.VisualChartLine
	areaChart := domain.VisualChartArea
	barChart := domain.VisualChartBar
	stackedBarChart := domain.VisualChartStackedBar
	pieChart := domain.VisualChartPie
	doughnutChart := domain.VisualChartDoughnut
	scatterChart := domain.VisualChartScatter

	return []sampleDashboardWidget{
		{
			PageName:    "Gallery",
			Name:        "Trips by Day (Line)",
			Description: "Line chart sanity check for the daily trip trend.",
			Source: domain.DashboardWidgetSource{
				Kind: domain.DashboardWidgetSourceSemanticQuery,
				SemanticQuery: &domain.DashboardSemanticQuerySource{
					ProjectName:       sampleDashboardSemanticProj,
					SemanticModelName: sampleDashboardSemanticModel,
					Metrics:           []string{"trip_count"},
					Dimensions:        []string{"pickup_date"},
					OrderBy:           []string{"pickup_date"},
				},
			},
			VisualSpec: &domain.VisualSpec{
				Kind:      domain.VisualOutputChart,
				ChartType: &lineChart,
				Title:     "Trips by Day (Line)",
				Encodings: domain.VisualEncodings{
					X: &domain.VisualFieldBinding{Field: "pickup_date"},
					Y: &domain.VisualFieldBinding{Field: "trip_count"},
				},
			},
			Layout: domain.DashboardWidgetLayout{X: 0, Y: 0, W: 6, H: 4},
		},
		{
			PageName:    "Gallery",
			Name:        "Revenue by Day (Area)",
			Description: "Area chart check for daily revenue rendering.",
			Source: domain.DashboardWidgetSource{
				Kind: domain.DashboardWidgetSourceSemanticQuery,
				SemanticQuery: &domain.DashboardSemanticQuerySource{
					ProjectName:       sampleDashboardSemanticProj,
					SemanticModelName: sampleDashboardSemanticModel,
					Metrics:           []string{"gross_revenue"},
					Dimensions:        []string{"pickup_date"},
					OrderBy:           []string{"pickup_date"},
				},
			},
			VisualSpec: &domain.VisualSpec{
				Kind:      domain.VisualOutputChart,
				ChartType: &areaChart,
				Title:     "Revenue by Day (Area)",
				Encodings: domain.VisualEncodings{
					X: &domain.VisualFieldBinding{Field: "pickup_date"},
					Y: &domain.VisualFieldBinding{Field: "gross_revenue"},
				},
			},
			Layout: domain.DashboardWidgetLayout{X: 6, Y: 0, W: 6, H: 4},
		},
		{
			PageName:    "Gallery",
			Name:        "Revenue by Borough (Bar)",
			Description: "Bar chart check for categorical revenue comparison.",
			Source: domain.DashboardWidgetSource{
				Kind: domain.DashboardWidgetSourceSemanticQuery,
				SemanticQuery: &domain.DashboardSemanticQuerySource{
					ProjectName:       sampleDashboardSemanticProj,
					SemanticModelName: sampleDashboardSemanticModel,
					Metrics:           []string{"gross_revenue"},
					Dimensions:        []string{"borough"},
					OrderBy:           []string{"gross_revenue DESC"},
				},
			},
			VisualSpec: &domain.VisualSpec{
				Kind:      domain.VisualOutputChart,
				ChartType: &barChart,
				Title:     "Revenue by Borough (Bar)",
				Encodings: domain.VisualEncodings{
					X: &domain.VisualFieldBinding{Field: "borough"},
					Y: &domain.VisualFieldBinding{Field: "gross_revenue"},
				},
			},
			Layout: domain.DashboardWidgetLayout{X: 0, Y: 4, W: 6, H: 4},
		},
		{
			PageName:    "Gallery",
			Name:        "Trips by Day and Borough (Stacked)",
			Description: "Stacked bar validation across time and borough segments.",
			Source: domain.DashboardWidgetSource{
				Kind: domain.DashboardWidgetSourceSemanticQuery,
				SemanticQuery: &domain.DashboardSemanticQuerySource{
					ProjectName:       sampleDashboardSemanticProj,
					SemanticModelName: sampleDashboardSemanticModel,
					Metrics:           []string{"trip_count"},
					Dimensions:        []string{"pickup_date", "borough"},
					OrderBy:           []string{"pickup_date", "borough"},
				},
			},
			VisualSpec: &domain.VisualSpec{
				Kind:      domain.VisualOutputChart,
				ChartType: &stackedBarChart,
				Title:     "Trips by Day and Borough (Stacked)",
				Encodings: domain.VisualEncodings{
					X:      &domain.VisualFieldBinding{Field: "pickup_date"},
					Y:      &domain.VisualFieldBinding{Field: "trip_count"},
					Series: &domain.VisualFieldBinding{Field: "borough"},
				},
			},
			Layout: domain.DashboardWidgetLayout{X: 6, Y: 4, W: 6, H: 4},
		},
		{
			PageName:    "Gallery",
			Name:        "Revenue Share by Borough (Pie)",
			Description: "Pie chart validation for share-style categorical totals.",
			Source: domain.DashboardWidgetSource{
				Kind: domain.DashboardWidgetSourceSemanticQuery,
				SemanticQuery: &domain.DashboardSemanticQuerySource{
					ProjectName:       sampleDashboardSemanticProj,
					SemanticModelName: sampleDashboardSemanticModel,
					Metrics:           []string{"gross_revenue"},
					Dimensions:        []string{"borough"},
					OrderBy:           []string{"gross_revenue DESC"},
				},
			},
			VisualSpec: &domain.VisualSpec{
				Kind:      domain.VisualOutputChart,
				ChartType: &pieChart,
				Title:     "Revenue Share by Borough (Pie)",
				Encodings: domain.VisualEncodings{
					Label: &domain.VisualFieldBinding{Field: "borough"},
					Value: &domain.VisualFieldBinding{Field: "gross_revenue"},
				},
			},
			Layout: domain.DashboardWidgetLayout{X: 0, Y: 8, W: 4, H: 4},
		},
		{
			PageName:    "Gallery",
			Name:        "Top Pickup Zones (Doughnut)",
			Description: "Doughnut chart validation for ranked categorical slices.",
			Source: domain.DashboardWidgetSource{
				Kind: domain.DashboardWidgetSourceSemanticQuery,
				SemanticQuery: &domain.DashboardSemanticQuerySource{
					ProjectName:       sampleDashboardSemanticProj,
					SemanticModelName: sampleDashboardSemanticModel,
					Metrics:           []string{"gross_revenue"},
					Dimensions:        []string{"pickup_zone"},
					OrderBy:           []string{"gross_revenue DESC"},
					Limit:             intPtr(8),
				},
			},
			VisualSpec: &domain.VisualSpec{
				Kind:      domain.VisualOutputChart,
				ChartType: &doughnutChart,
				Title:     "Top Pickup Zones (Doughnut)",
				Encodings: domain.VisualEncodings{
					Label: &domain.VisualFieldBinding{Field: "pickup_zone"},
					Value: &domain.VisualFieldBinding{Field: "gross_revenue"},
				},
			},
			Layout: domain.DashboardWidgetLayout{X: 4, Y: 8, W: 4, H: 4},
		},
		{
			PageName:    "Gallery",
			Name:        "Zone Revenue vs Trips (Scatter)",
			Description: "Scatter plot check for two-metric zone comparisons with pickup-zone selection.",
			Source: domain.DashboardWidgetSource{
				Kind: domain.DashboardWidgetSourceSemanticQuery,
				SemanticQuery: &domain.DashboardSemanticQuerySource{
					ProjectName:       sampleDashboardSemanticProj,
					SemanticModelName: sampleDashboardSemanticModel,
					Metrics:           []string{"trip_count", "gross_revenue"},
					Dimensions:        []string{"pickup_zone"},
					OrderBy:           []string{"gross_revenue DESC"},
					Limit:             intPtr(20),
				},
			},
			VisualSpec: &domain.VisualSpec{
				Kind:      domain.VisualOutputChart,
				ChartType: &scatterChart,
				Title:     "Zone Revenue vs Trips (Scatter)",
				Encodings: domain.VisualEncodings{
					Label: &domain.VisualFieldBinding{Field: "pickup_zone"},
					X: &domain.VisualFieldBinding{Field: "trip_count"},
					Y: &domain.VisualFieldBinding{Field: "gross_revenue"},
				},
			},
			Layout: domain.DashboardWidgetLayout{X: 8, Y: 8, W: 4, H: 4},
		},
		{
			PageName:    "Gallery",
			Name:        "Zone Revenue Detail (Table)",
			Description: "Table validation for selection, sorting, and paging behavior.",
			Source: domain.DashboardWidgetSource{
				Kind: domain.DashboardWidgetSourceSemanticQuery,
				SemanticQuery: &domain.DashboardSemanticQuerySource{
					ProjectName:       sampleDashboardSemanticProj,
					SemanticModelName: sampleDashboardSemanticModel,
					Metrics:           []string{"gross_revenue", "trip_count"},
					Dimensions:        []string{"pickup_zone", "borough"},
					OrderBy:           []string{"gross_revenue DESC"},
				},
			},
			VisualSpec: &domain.VisualSpec{
				Kind:  domain.VisualOutputTable,
				Title: "Zone Revenue Detail (Table)",
			},
			Layout: domain.DashboardWidgetLayout{X: 0, Y: 12, W: 12, H: 5},
		},
	}
}
