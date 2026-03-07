//nolint:revive // orchestration components are exported for app wiring and tests.
package orchestration

import (
	"context"
	"fmt"

	"duck-demo/internal/domain"
)

type AssetRunStateMachine struct{}

func NewAssetRunStateMachine() *AssetRunStateMachine {
	return &AssetRunStateMachine{}
}

func (m *AssetRunStateMachine) CanTransition(from, to string) bool {
	if from == to {
		return true
	}
	allowed := map[string]map[string]bool{
		domain.AssetRunStatusQueued: {
			domain.AssetRunStatusPlanning:  true,
			domain.AssetRunStatusRunning:   true,
			domain.AssetRunStatusCancelled: true,
			domain.AssetRunStatusSkipped:   true,
		},
		domain.AssetRunStatusPlanning: {
			domain.AssetRunStatusRunning:   true,
			domain.AssetRunStatusRetrying:  true,
			domain.AssetRunStatusFailed:    true,
			domain.AssetRunStatusCancelled: true,
		},
		domain.AssetRunStatusRunning: {
			domain.AssetRunStatusRetrying:  true,
			domain.AssetRunStatusSuccess:   true,
			domain.AssetRunStatusFailed:    true,
			domain.AssetRunStatusCancelled: true,
			domain.AssetRunStatusStale:     true,
		},
		domain.AssetRunStatusRetrying: {
			domain.AssetRunStatusRunning:   true,
			domain.AssetRunStatusFailed:    true,
			domain.AssetRunStatusCancelled: true,
		},
		domain.AssetRunStatusStale: {
			domain.AssetRunStatusQueued: true,
		},
	}
	return allowed[from][to]
}

func (m *AssetRunStateMachine) Transition(ctx context.Context, repo domain.AssetRunRepository, runID, from, to string, attempt int, errMsg *string) error {
	if !m.CanTransition(from, to) {
		return domain.ErrValidation("invalid asset run transition: %s -> %s", from, to)
	}

	switch to {
	case domain.AssetRunStatusRunning:
		if err := repo.UpdateRunStarted(ctx, runID); err != nil {
			return fmt.Errorf("update run started: %w", err)
		}
	case domain.AssetRunStatusRetrying:
		if err := repo.UpdateRunRetrying(ctx, runID, attempt, errMsg); err != nil {
			return fmt.Errorf("update run retrying: %w", err)
		}
	default:
		if err := repo.UpdateRunFinished(ctx, runID, to, errMsg); err != nil {
			return fmt.Errorf("update run finished: %w", err)
		}
	}
	return nil
}
