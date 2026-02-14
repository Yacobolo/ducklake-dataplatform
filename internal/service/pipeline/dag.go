// Package pipeline implements the pipeline orchestration service.
package pipeline

import "duck-demo/internal/domain"

// ResolveExecutionOrder computes a topological ordering of pipeline jobs
// using Kahn's algorithm. Returns levels of job IDs where each level
// can execute in parallel. Returns an error if cycles or unknown deps exist.
func ResolveExecutionOrder(jobs []domain.PipelineJob) ([][]string, error) {
	if len(jobs) == 0 {
		return nil, nil
	}

	// Build name → job mapping and adjacency.
	nameToID := make(map[string]string, len(jobs))
	inDegree := make(map[string]int, len(jobs))
	dependents := make(map[string][]string) // dep name → list of job IDs that depend on it

	for _, j := range jobs {
		nameToID[j.Name] = j.ID
		inDegree[j.ID] = 0
	}

	for _, j := range jobs {
		for _, dep := range j.DependsOn {
			depID, ok := nameToID[dep]
			if !ok {
				return nil, domain.ErrValidation("unknown dependency: %s", dep)
			}
			if depID == j.ID {
				return nil, domain.ErrValidation("self dependency: %s", j.Name)
			}
			dependents[depID] = append(dependents[depID], j.ID)
			inDegree[j.ID]++
		}
	}

	// Kahn's algorithm — process by levels.
	var levels [][]string
	var queue []string
	for id, deg := range inDegree {
		if deg == 0 {
			queue = append(queue, id)
		}
	}

	processed := 0
	for len(queue) > 0 {
		level := make([]string, len(queue))
		copy(level, queue)
		levels = append(levels, level)
		processed += len(level)

		var next []string
		for _, id := range queue {
			for _, dep := range dependents[id] {
				inDegree[dep]--
				if inDegree[dep] == 0 {
					next = append(next, dep)
				}
			}
		}
		queue = next
	}

	if processed != len(jobs) {
		return nil, domain.ErrValidation("cycle detected in job dependencies")
	}
	return levels, nil
}
