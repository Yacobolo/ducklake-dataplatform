package pipeline

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"duck-demo/internal/domain"
)

func TestBuildPipelineAssetGraph(t *testing.T) {
	p := &domain.Pipeline{Name: "daily", CreatedBy: "admin"}
	jobs := []domain.PipelineJob{
		{ID: "j1", Name: "extract", JobType: domain.PipelineJobTypeNotebook},
		{ID: "j2", Name: "transform", JobType: domain.PipelineJobTypeModelRun, DependsOn: []string{"extract"}},
	}

	adapted, err := BuildPipelineAssetGraph(p, jobs)
	require.NoError(t, err)
	require.Len(t, adapted.Assets, 2)
	require.Len(t, adapted.Dependencies, 1)

	assert.Equal(t, "pipeline.daily.extract", adapted.Assets[0].AssetKey)
	assert.Equal(t, domain.AssetTypeNotebook, adapted.Assets[0].AssetType)
	assert.Equal(t, domain.AssetTypeModel, adapted.Assets[1].AssetType)

	dep := adapted.Dependencies[0]
	assert.Equal(t, "j2", dep.AssetID)
	assert.Equal(t, "j1", dep.UpstreamAssetID)
	assert.Equal(t, domain.DependencyTypeHard, dep.DependencyType)
}

func TestBuildPipelineAssetGraph_InvalidDependency(t *testing.T) {
	p := &domain.Pipeline{Name: "daily", CreatedBy: "admin"}
	jobs := []domain.PipelineJob{{ID: "j1", Name: "extract", DependsOn: []string{"missing"}}}

	_, err := BuildPipelineAssetGraph(p, jobs)
	require.Error(t, err)
	var validationErr *domain.ValidationError
	assert.ErrorAs(t, err, &validationErr)
}
