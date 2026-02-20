package compute

import (
	"encoding/json"
	"sync"

	"google.golang.org/grpc/encoding"
)

const grpcJSONCodecName = "json"

var registerCodecOnce sync.Once

type grpcJSONCodec struct{}

func (c *grpcJSONCodec) Name() string {
	return grpcJSONCodecName
}

func (c *grpcJSONCodec) Marshal(v interface{}) ([]byte, error) {
	return json.Marshal(v)
}

func (c *grpcJSONCodec) Unmarshal(data []byte, v interface{}) error {
	return json.Unmarshal(data, v)
}

// EnsureGRPCJSONCodec registers the JSON codec used for internal gRPC transport.
func EnsureGRPCJSONCodec() {
	registerCodecOnce.Do(func() {
		encoding.RegisterCodec(&grpcJSONCodec{})
	})
}
