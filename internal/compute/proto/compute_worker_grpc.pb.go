package computeproto

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	ComputeWorker_Execute_FullMethodName           = "/duckdemo.compute.v1.ComputeWorker/Execute"
	ComputeWorker_SubmitQuery_FullMethodName       = "/duckdemo.compute.v1.ComputeWorker/SubmitQuery"
	ComputeWorker_GetQueryStatus_FullMethodName    = "/duckdemo.compute.v1.ComputeWorker/GetQueryStatus"
	ComputeWorker_FetchQueryResults_FullMethodName = "/duckdemo.compute.v1.ComputeWorker/FetchQueryResults"
	ComputeWorker_CancelQuery_FullMethodName       = "/duckdemo.compute.v1.ComputeWorker/CancelQuery"
	ComputeWorker_DeleteQuery_FullMethodName       = "/duckdemo.compute.v1.ComputeWorker/DeleteQuery"
	ComputeWorker_Health_FullMethodName            = "/duckdemo.compute.v1.ComputeWorker/Health"
)

type ComputeWorkerClient interface {
	Execute(ctx context.Context, in *ExecuteRequest, opts ...grpc.CallOption) (*ExecuteResponse, error)
	SubmitQuery(ctx context.Context, in *SubmitQueryRequest, opts ...grpc.CallOption) (*SubmitQueryResponse, error)
	GetQueryStatus(ctx context.Context, in *GetQueryStatusRequest, opts ...grpc.CallOption) (*QueryStatusResponse, error)
	FetchQueryResults(ctx context.Context, in *FetchQueryResultsRequest, opts ...grpc.CallOption) (*FetchQueryResultsResponse, error)
	CancelQuery(ctx context.Context, in *CancelQueryRequest, opts ...grpc.CallOption) (*CancelQueryResponse, error)
	DeleteQuery(ctx context.Context, in *DeleteQueryRequest, opts ...grpc.CallOption) (*DeleteQueryResponse, error)
	Health(ctx context.Context, in *HealthRequest, opts ...grpc.CallOption) (*HealthResponse, error)
}

type computeWorkerClient struct {
	cc grpc.ClientConnInterface
}

func NewComputeWorkerClient(cc grpc.ClientConnInterface) ComputeWorkerClient {
	return &computeWorkerClient{cc: cc}
}

func (c *computeWorkerClient) Execute(ctx context.Context, in *ExecuteRequest, opts ...grpc.CallOption) (*ExecuteResponse, error) {
	out := new(ExecuteResponse)
	if err := c.cc.Invoke(ctx, ComputeWorker_Execute_FullMethodName, in, out, opts...); err != nil {
		return nil, err
	}
	return out, nil
}

func (c *computeWorkerClient) SubmitQuery(ctx context.Context, in *SubmitQueryRequest, opts ...grpc.CallOption) (*SubmitQueryResponse, error) {
	out := new(SubmitQueryResponse)
	if err := c.cc.Invoke(ctx, ComputeWorker_SubmitQuery_FullMethodName, in, out, opts...); err != nil {
		return nil, err
	}
	return out, nil
}

func (c *computeWorkerClient) GetQueryStatus(ctx context.Context, in *GetQueryStatusRequest, opts ...grpc.CallOption) (*QueryStatusResponse, error) {
	out := new(QueryStatusResponse)
	if err := c.cc.Invoke(ctx, ComputeWorker_GetQueryStatus_FullMethodName, in, out, opts...); err != nil {
		return nil, err
	}
	return out, nil
}

func (c *computeWorkerClient) FetchQueryResults(ctx context.Context, in *FetchQueryResultsRequest, opts ...grpc.CallOption) (*FetchQueryResultsResponse, error) {
	out := new(FetchQueryResultsResponse)
	if err := c.cc.Invoke(ctx, ComputeWorker_FetchQueryResults_FullMethodName, in, out, opts...); err != nil {
		return nil, err
	}
	return out, nil
}

func (c *computeWorkerClient) CancelQuery(ctx context.Context, in *CancelQueryRequest, opts ...grpc.CallOption) (*CancelQueryResponse, error) {
	out := new(CancelQueryResponse)
	if err := c.cc.Invoke(ctx, ComputeWorker_CancelQuery_FullMethodName, in, out, opts...); err != nil {
		return nil, err
	}
	return out, nil
}

func (c *computeWorkerClient) DeleteQuery(ctx context.Context, in *DeleteQueryRequest, opts ...grpc.CallOption) (*DeleteQueryResponse, error) {
	out := new(DeleteQueryResponse)
	if err := c.cc.Invoke(ctx, ComputeWorker_DeleteQuery_FullMethodName, in, out, opts...); err != nil {
		return nil, err
	}
	return out, nil
}

func (c *computeWorkerClient) Health(ctx context.Context, in *HealthRequest, opts ...grpc.CallOption) (*HealthResponse, error) {
	out := new(HealthResponse)
	if err := c.cc.Invoke(ctx, ComputeWorker_Health_FullMethodName, in, out, opts...); err != nil {
		return nil, err
	}
	return out, nil
}

type ComputeWorkerServer interface {
	Execute(context.Context, *ExecuteRequest) (*ExecuteResponse, error)
	SubmitQuery(context.Context, *SubmitQueryRequest) (*SubmitQueryResponse, error)
	GetQueryStatus(context.Context, *GetQueryStatusRequest) (*QueryStatusResponse, error)
	FetchQueryResults(context.Context, *FetchQueryResultsRequest) (*FetchQueryResultsResponse, error)
	CancelQuery(context.Context, *CancelQueryRequest) (*CancelQueryResponse, error)
	DeleteQuery(context.Context, *DeleteQueryRequest) (*DeleteQueryResponse, error)
	Health(context.Context, *HealthRequest) (*HealthResponse, error)
	mustEmbedUnimplementedComputeWorkerServer()
}

type UnimplementedComputeWorkerServer struct{}

func (UnimplementedComputeWorkerServer) Execute(context.Context, *ExecuteRequest) (*ExecuteResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Execute not implemented")
}
func (UnimplementedComputeWorkerServer) SubmitQuery(context.Context, *SubmitQueryRequest) (*SubmitQueryResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method SubmitQuery not implemented")
}
func (UnimplementedComputeWorkerServer) GetQueryStatus(context.Context, *GetQueryStatusRequest) (*QueryStatusResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetQueryStatus not implemented")
}
func (UnimplementedComputeWorkerServer) FetchQueryResults(context.Context, *FetchQueryResultsRequest) (*FetchQueryResultsResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method FetchQueryResults not implemented")
}
func (UnimplementedComputeWorkerServer) CancelQuery(context.Context, *CancelQueryRequest) (*CancelQueryResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method CancelQuery not implemented")
}
func (UnimplementedComputeWorkerServer) DeleteQuery(context.Context, *DeleteQueryRequest) (*DeleteQueryResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method DeleteQuery not implemented")
}
func (UnimplementedComputeWorkerServer) Health(context.Context, *HealthRequest) (*HealthResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Health not implemented")
}
func (UnimplementedComputeWorkerServer) mustEmbedUnimplementedComputeWorkerServer() {}

func RegisterComputeWorkerServer(s grpc.ServiceRegistrar, srv ComputeWorkerServer) {
	s.RegisterService(&ComputeWorker_ServiceDesc, srv)
}

func _ComputeWorker_Execute_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(ExecuteRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ComputeWorkerServer).Execute(ctx, in)
	}
	info := &grpc.UnaryServerInfo{Server: srv, FullMethod: ComputeWorker_Execute_FullMethodName}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ComputeWorkerServer).Execute(ctx, req.(*ExecuteRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _ComputeWorker_SubmitQuery_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(SubmitQueryRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ComputeWorkerServer).SubmitQuery(ctx, in)
	}
	info := &grpc.UnaryServerInfo{Server: srv, FullMethod: ComputeWorker_SubmitQuery_FullMethodName}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ComputeWorkerServer).SubmitQuery(ctx, req.(*SubmitQueryRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _ComputeWorker_GetQueryStatus_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(GetQueryStatusRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ComputeWorkerServer).GetQueryStatus(ctx, in)
	}
	info := &grpc.UnaryServerInfo{Server: srv, FullMethod: ComputeWorker_GetQueryStatus_FullMethodName}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ComputeWorkerServer).GetQueryStatus(ctx, req.(*GetQueryStatusRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _ComputeWorker_FetchQueryResults_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(FetchQueryResultsRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ComputeWorkerServer).FetchQueryResults(ctx, in)
	}
	info := &grpc.UnaryServerInfo{Server: srv, FullMethod: ComputeWorker_FetchQueryResults_FullMethodName}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ComputeWorkerServer).FetchQueryResults(ctx, req.(*FetchQueryResultsRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _ComputeWorker_CancelQuery_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(CancelQueryRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ComputeWorkerServer).CancelQuery(ctx, in)
	}
	info := &grpc.UnaryServerInfo{Server: srv, FullMethod: ComputeWorker_CancelQuery_FullMethodName}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ComputeWorkerServer).CancelQuery(ctx, req.(*CancelQueryRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _ComputeWorker_DeleteQuery_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(DeleteQueryRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ComputeWorkerServer).DeleteQuery(ctx, in)
	}
	info := &grpc.UnaryServerInfo{Server: srv, FullMethod: ComputeWorker_DeleteQuery_FullMethodName}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ComputeWorkerServer).DeleteQuery(ctx, req.(*DeleteQueryRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _ComputeWorker_Health_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(HealthRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ComputeWorkerServer).Health(ctx, in)
	}
	info := &grpc.UnaryServerInfo{Server: srv, FullMethod: ComputeWorker_Health_FullMethodName}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ComputeWorkerServer).Health(ctx, req.(*HealthRequest))
	}
	return interceptor(ctx, in, info, handler)
}

var ComputeWorker_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "duckdemo.compute.v1.ComputeWorker",
	HandlerType: (*ComputeWorkerServer)(nil),
	Methods: []grpc.MethodDesc{
		{MethodName: "Execute", Handler: _ComputeWorker_Execute_Handler},
		{MethodName: "SubmitQuery", Handler: _ComputeWorker_SubmitQuery_Handler},
		{MethodName: "GetQueryStatus", Handler: _ComputeWorker_GetQueryStatus_Handler},
		{MethodName: "FetchQueryResults", Handler: _ComputeWorker_FetchQueryResults_Handler},
		{MethodName: "CancelQuery", Handler: _ComputeWorker_CancelQuery_Handler},
		{MethodName: "DeleteQuery", Handler: _ComputeWorker_DeleteQuery_Handler},
		{MethodName: "Health", Handler: _ComputeWorker_Health_Handler},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "internal/compute/proto/compute_worker.proto",
}
