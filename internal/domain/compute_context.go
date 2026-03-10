package domain

import "context"

type computeExecutionRequestKey struct{}
type computeResolutionKey struct{}

// ComputeResolution records how a query was ultimately routed.
type ComputeResolution struct {
	RequestedMode     string
	RequestedEndpoint string
	ResolvedMode      string
	ResolvedEndpoint  string
}

// WithComputeExecutionRequest attaches a compute routing preference to context.
func WithComputeExecutionRequest(ctx context.Context, req ComputeExecutionRequest) context.Context {
	return context.WithValue(ctx, computeExecutionRequestKey{}, req.Normalize())
}

// ComputeExecutionRequestFromContext extracts a compute routing preference.
func ComputeExecutionRequestFromContext(ctx context.Context) (ComputeExecutionRequest, bool) {
	req, ok := ctx.Value(computeExecutionRequestKey{}).(ComputeExecutionRequest)
	if !ok {
		return ComputeExecutionRequest{}, false
	}
	return req.Normalize(), true
}

// WithComputeResolutionTracker installs a mutable resolution tracker into context.
func WithComputeResolutionTracker(ctx context.Context) (context.Context, *ComputeResolution) {
	tracker := &ComputeResolution{}
	return context.WithValue(ctx, computeResolutionKey{}, tracker), tracker
}

// RecordComputeResolution updates the tracker when one is installed on context.
func RecordComputeResolution(ctx context.Context, resolution ComputeResolution) {
	tracker, ok := ctx.Value(computeResolutionKey{}).(*ComputeResolution)
	if !ok || tracker == nil {
		return
	}
	*tracker = resolution
}
