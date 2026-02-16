// Package model provides transformation model management including
// dependency extraction, DAG resolution, and materialization execution.
package model

import (
	"sort"

	"duck-demo/internal/domain"
)

// DAGNode represents a model in the execution graph.
type DAGNode struct {
	Model *domain.Model
	Tier  int
}

// ResolveDAG computes execution tiers using Kahn's algorithm.
func ResolveDAG(models []domain.Model) ([][]DAGNode, error) {
	if len(models) == 0 {
		return nil, nil
	}

	nameToIdx := make(map[string]int, len(models))
	for i, m := range models {
		nameToIdx[m.QualifiedName()] = i
	}

	inDegree := make([]int, len(models))
	dependents := make(map[int][]int)

	for i, m := range models {
		for _, dep := range m.DependsOn {
			depIdx, ok := nameToIdx[dep]
			if !ok {
				continue // physical table dep, not a model
			}
			if depIdx == i {
				return nil, domain.ErrValidation("self dependency: %s", m.QualifiedName())
			}
			dependents[depIdx] = append(dependents[depIdx], i)
			inDegree[i]++
		}
	}

	var tiers [][]DAGNode
	var queue []int
	for i, deg := range inDegree {
		if deg == 0 {
			queue = append(queue, i)
		}
	}

	processed := 0
	tierNum := 0
	for len(queue) > 0 {
		sort.Ints(queue) // deterministic ordering
		tier := make([]DAGNode, 0, len(queue))
		for _, idx := range queue {
			tier = append(tier, DAGNode{Model: &models[idx], Tier: tierNum})
		}
		tiers = append(tiers, tier)
		processed += len(queue)

		var next []int
		for _, idx := range queue {
			for _, dep := range dependents[idx] {
				inDegree[dep]--
				if inDegree[dep] == 0 {
					next = append(next, dep)
				}
			}
		}
		queue = next
		tierNum++
	}

	if processed != len(models) {
		return nil, domain.ErrValidation("cycle detected in model dependencies")
	}
	return tiers, nil
}
