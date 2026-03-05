//nolint:revive // orchestration components are exported for app wiring and tests.
package orchestration

import (
	"context"
	"fmt"

	"duck-demo/internal/domain"
)

type AssetRunPlan struct {
	RootAssetID string
	Levels      [][]string
}

type AssetScheduler struct {
	assets       domain.DataAssetRepository
	dependencies domain.AssetDependencyRepository
	runs         domain.AssetRunRepository
}

func NewAssetScheduler(
	assets domain.DataAssetRepository,
	dependencies domain.AssetDependencyRepository,
	runs domain.AssetRunRepository,
) *AssetScheduler {
	return &AssetScheduler{assets: assets, dependencies: dependencies, runs: runs}
}

func (s *AssetScheduler) BuildPlan(ctx context.Context, rootAssetID string) (*AssetRunPlan, error) {
	if _, err := s.assets.GetByID(ctx, rootAssetID); err != nil {
		return nil, fmt.Errorf("get root asset: %w", err)
	}

	adj := map[string][]string{}
	inDegree := map[string]int{}
	queue := []string{rootAssetID}
	seen := map[string]bool{rootAssetID: true}
	inDegree[rootAssetID] = 0

	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]

		downstream, err := s.dependencies.ListDownstream(ctx, current)
		if err != nil {
			return nil, fmt.Errorf("list downstream dependencies: %w", err)
		}
		for _, dep := range downstream {
			adj[current] = append(adj[current], dep.AssetID)
			inDegree[dep.AssetID]++
			if !seen[dep.AssetID] {
				seen[dep.AssetID] = true
				queue = append(queue, dep.AssetID)
			}
		}
	}

	levels := make([][]string, 0)
	currentLevel := make([]string, 0)
	for node := range seen {
		if inDegree[node] == 0 {
			currentLevel = append(currentLevel, node)
		}
	}

	processed := 0
	for len(currentLevel) > 0 {
		nextLevel := make([]string, 0)
		levels = append(levels, currentLevel)
		for _, node := range currentLevel {
			processed++
			for _, child := range adj[node] {
				inDegree[child]--
				if inDegree[child] == 0 {
					nextLevel = append(nextLevel, child)
				}
			}
		}
		currentLevel = nextLevel
	}

	if processed != len(seen) {
		return nil, domain.ErrValidation("asset dependency graph contains cycle")
	}

	return &AssetRunPlan{RootAssetID: rootAssetID, Levels: levels}, nil
}
