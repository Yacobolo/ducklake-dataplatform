package repository

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	internaldb "github.com/Yacobolo/quackstack/internal/db"
	"github.com/Yacobolo/quackstack/internal/domain"
)

func TestDataProductRepo_CreateListAndGetBySlug(t *testing.T) {
	writeDB, _ := internaldb.OpenTestSQLite(t)
	ctx := context.Background()

	domainRepo := NewDomainRepo(writeDB)
	teamRepo := NewTeamRepo(writeDB)
	productRepo := NewDataProductRepo(writeDB)
	assetRepo := NewDataAssetRepo(writeDB)

	domainItem, err := domainRepo.Create(ctx, &domain.Domain{Name: "Revenue", Description: "Commercial reporting"})
	require.NoError(t, err)

	teamItem, err := teamRepo.Create(ctx, &domain.Team{DomainID: domainItem.ID, Name: "Analytics Engineering", ContactChannel: "#rev-data"})
	require.NoError(t, err)

	asset, err := assetRepo.Create(ctx, &domain.DataAsset{
		AssetKey:  "main.analytics.daily_orders",
		AssetType: domain.AssetTypeTable,
		Owner:     "analytics-engineering",
		CreatedBy: "alice",
		IsActive:  true,
	})
	require.NoError(t, err)

	productItem, err := productRepo.Create(ctx, &domain.DataProduct{
		Slug:              "daily-orders",
		Name:              "Daily Orders",
		Description:       "Orders served to analytics and finance",
		DomainID:          domainItem.ID,
		OwnerTeamID:       teamItem.ID,
		StewardPrincipal:  "alice",
		ContactChannel:    "#rev-data",
		PublicationIntent: domain.ProductPublicationIntentDraft,
		CreatedBy:         "alice",
		Contract: domain.ProductContract{
			DataGrain:            "one row per order",
			UpdateCadence:        "hourly",
			BreakingChangePolicy: "new version required",
		},
		SLO: domain.ProductSLO{FreshnessSLO: "60m"},
	})
	require.NoError(t, err)

	version, err := productRepo.CreateVersion(ctx, &domain.DataProductVersion{
		ProductID:          productItem.ID,
		Version:            1,
		ReleaseState:       domain.ProductReleaseStateDraft,
		CompatibilityLevel: domain.ProductCompatibilityBackwardCompatible,
		Contract:           productItem.Contract,
		SLO:                productItem.SLO,
		CreatedBy:          "alice",
	})
	require.NoError(t, err)

	require.NoError(t, productRepo.UpsertStatus(ctx, &domain.DataProductStatus{
		ProductID:          productItem.ID,
		PublicationState:   domain.ProductReleaseStateDraft,
		CertificationState: domain.CertificationDraft,
		FreshnessStatus:    "UNKNOWN",
		QualityStatus:      "UNKNOWN",
		AdoptionMetrics:    map[string]any{},
		OpenWarnings:       []string{"Draft product has not been published"},
	}))
	require.NoError(t, productRepo.AddOutput(ctx, &domain.ProductOutput{
		ProductVersionID: version.ID,
		AssetID:          asset.ID,
		IsPrimary:        true,
	}))
	_, err = productRepo.AddEvent(ctx, &domain.ProductEvent{
		ProductID:   productItem.ID,
		EventType:   "publication",
		Title:       "Draft created",
		Description: "Initial draft version was created.",
		Metadata:    map[string]any{"version": 1},
	})
	require.NoError(t, err)

	items, total, err := productRepo.List(ctx, domain.DataProductFilter{Page: domain.PageRequest{MaxResults: 10}})
	require.NoError(t, err)
	require.Equal(t, int64(1), total)
	require.Len(t, items, 1)
	assert.Equal(t, "daily-orders", items[0].Product.Slug)
	require.NotNil(t, items[0].PrimaryOutput)
	assert.Equal(t, "main.analytics.daily_orders", items[0].PrimaryOutput.AssetKey)

	detail, err := productRepo.GetBySlug(ctx, "daily-orders")
	require.NoError(t, err)
	assert.Equal(t, "Revenue", detail.Domain.Name)
	assert.Equal(t, "Analytics Engineering", detail.OwnerTeam.Name)
	require.Len(t, detail.Versions, 1)
	require.Len(t, detail.Outputs, 1)
	assert.Equal(t, "main.analytics.daily_orders", detail.Outputs[0].AssetKey)
	require.NotNil(t, detail.Status)
	assert.Equal(t, domain.ProductReleaseStateDraft, detail.Status.PublicationState)
	require.Len(t, detail.Events, 1)
	assert.Equal(t, "publication", detail.Events[0].EventType)
}
