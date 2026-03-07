package orchestration

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestConcurrencyLimiter_GlobalCapBlocksUntilRelease(t *testing.T) {
	limiter := NewConcurrencyLimiter(1, 1)
	require.NoError(t, limiter.Acquire(context.Background(), "asset-a"))

	started := make(chan struct{})
	errCh := make(chan error, 1)
	go func() {
		close(started)
		errCh <- limiter.Acquire(context.Background(), "asset-b")
	}()

	<-started
	select {
	case err := <-errCh:
		require.Failf(t, "acquire unexpectedly completed", "err=%v", err)
	default:
	}

	limiter.Release("asset-a")

	select {
	case err := <-errCh:
		require.NoError(t, err)
	case <-time.After(1 * time.Second):
		require.Fail(t, "acquire did not unblock after release")
	}
	limiter.Release("asset-b")
}

func TestConcurrencyLimiter_PerAssetIsolationAndCap(t *testing.T) {
	limiter := NewConcurrencyLimiter(3, 1)
	require.NoError(t, limiter.Acquire(context.Background(), "asset-a"))

	sameAssetErrCh := make(chan error, 1)
	go func() {
		sameAssetErrCh <- limiter.Acquire(context.Background(), "asset-a")
	}()

	otherAssetErrCh := make(chan error, 1)
	go func() {
		otherAssetErrCh <- limiter.Acquire(context.Background(), "asset-b")
	}()

	select {
	case err := <-otherAssetErrCh:
		require.NoError(t, err)
	case <-time.After(1 * time.Second):
		require.Fail(t, "different asset should not be blocked")
	}
	limiter.Release("asset-b")

	select {
	case err := <-sameAssetErrCh:
		require.Failf(t, "same asset unexpectedly acquired", "err=%v", err)
	default:
	}

	limiter.Release("asset-a")

	select {
	case err := <-sameAssetErrCh:
		require.NoError(t, err)
	case <-time.After(1 * time.Second):
		require.Fail(t, "same asset acquire did not unblock after release")
	}
	limiter.Release("asset-a")
}

func TestConcurrencyLimiter_ContextCancellationWhileWaitingReleasesGlobalSlot(t *testing.T) {
	limiter := NewConcurrencyLimiter(2, 1)
	require.NoError(t, limiter.Acquire(context.Background(), "asset-a"))

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		errCh <- limiter.Acquire(ctx, "asset-a")
	}()

	cancel()

	select {
	case err := <-errCh:
		require.Error(t, err)
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(1 * time.Second):
		require.Fail(t, "canceled acquire did not return")
	}

	acquireCtx, acquireCancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer acquireCancel()
	require.NoError(t, limiter.Acquire(acquireCtx, "asset-b"))
	limiter.Release("asset-b")
	limiter.Release("asset-a")
}
