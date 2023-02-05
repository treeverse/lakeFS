package plugins

import (
	"context"

	"github.com/hashicorp/go-plugin"
	"google.golang.org/grpc"
)

type PingPongSound int32

const (
	PING PingPongSound = iota
	PONG
	POING
	BOOM
)

type PingPongStub interface {
	Play(PingPongSound) PingPongSound
}
type PingPongPlayer struct{}

// Play is the actual implementation of the function that the server will use
func (*PingPongPlayer) Play(pps PingPongSound) PingPongSound {
	switch pps % 3 {
	case 0:
		return PONG
	case 1:
		return PING
	default:
		return POING
	}
}

// TestGRPCServer is the implementation of our GRPC service.
// It should implement the Protobuf service server's interface (PingPongServer).
type TestGRPCServer struct {
	Impl PingPongStub
}

func (t *TestGRPCServer) Ping(ctx context.Context, request *PingRequest) (*PongResponse, error) {
	return &PongResponse{Sound: Sound(t.Impl.Play(PingPongSound(request.Sound)))}, nil
}
func (t *TestGRPCServer) mustEmbedUnimplementedPingPongServer() {
	panic("implement me")
}

// TestGRPCClient is the implementation of our GRPC client.
// It should implement the PingPongStub interface and use the Protobuf service client (PingPongClient) to perform the
// call to the server.
type TestGRPCClient struct {
	client PingPongClient
}

func (t *TestGRPCClient) Play(sound PingPongSound) PingPongSound {
	ping, err := t.client.Ping(context.Background(), &PingRequest{}, nil)
	if err != nil {
		return BOOM
	}
	return PingPongSound(ping.Sound)
}

type GRPCPlugin struct {
	Impl PingPongStub
	plugin.Plugin
}

func (p GRPCPlugin) GRPCServer(broker *plugin.GRPCBroker, s *grpc.Server) error {
	RegisterPingPongServer(s, &TestGRPCServer{Impl: p.Impl})
	return nil
}

// GRPCClient will return the Delta diff GRPC custom client
func (p GRPCPlugin) GRPCClient(ctx context.Context, broker *plugin.GRPCBroker, c *grpc.ClientConn) (interface{}, error) {
	return &TestGRPCClient{client: NewPingPongClient(c)}, nil
}

type NopGRPCPlugin struct {
	plugin.Plugin
}

func (np NopGRPCPlugin) GRPCServer(broker *plugin.GRPCBroker, s *grpc.Server) error {
	return nil
}

func (np NopGRPCPlugin) GRPCClient(ctx context.Context, broker *plugin.GRPCBroker, c *grpc.ClientConn) (interface{}, error) {
	return nil, nil
}
