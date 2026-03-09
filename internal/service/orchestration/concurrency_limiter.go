//nolint:revive // orchestration components are exported for app wiring and tests.
package orchestration

import (
	"context"
	"sync"
)

type ConcurrencyLimiter struct {
	global sem

	mu          sync.Mutex
	perAsset    map[string]sem
	perAssetCap int
}

func NewConcurrencyLimiter(globalCap, perAssetCap int) *ConcurrencyLimiter {
	if globalCap <= 0 {
		globalCap = 1
	}
	if perAssetCap <= 0 {
		perAssetCap = 1
	}
	return &ConcurrencyLimiter{
		global:      newSem(globalCap),
		perAsset:    map[string]sem{},
		perAssetCap: perAssetCap,
	}
}

func (l *ConcurrencyLimiter) Acquire(ctx context.Context, assetID string) error {
	if err := l.global.acquire(ctx); err != nil {
		return err
	}

	s := l.assetSem(assetID)
	if err := s.acquire(ctx); err != nil {
		l.global.release()
		return err
	}
	return nil
}

func (l *ConcurrencyLimiter) Release(assetID string) {
	l.global.release()
	l.assetSem(assetID).release()
}

func (l *ConcurrencyLimiter) assetSem(assetID string) sem {
	l.mu.Lock()
	defer l.mu.Unlock()
	s, ok := l.perAsset[assetID]
	if !ok {
		s = newSem(l.perAssetCap)
		l.perAsset[assetID] = s
	}
	return s
}

type sem chan struct{}

func newSem(size int) sem {
	return make(chan struct{}, size)
}

func (s sem) acquire(ctx context.Context) error {
	select {
	case s <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s sem) release() {
	select {
	case <-s:
	default:
	}
}
