package diff

import (
	"context"

	"google.golang.org/grpc/codes"

	"github.com/hashicorp/go-plugin"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
)

type GRPCClient struct {
	client DifferClient // This is the underlying GRPC API client
}

func (d *GRPCClient) Diff(ctx context.Context, paths TablePaths, s3Creds S3Creds) ([]*Diff, error) {
	dr, err := d.client.Diff(ctx, &DiffRequest{
		Paths: &DiffPaths{
			LeftPath:  paths.LeftTablePath,
			RightPath: paths.RightTablePath,
			// TODO: add base ref
		},
		GatewayConfig: &GatewayConfig{
			Key:      s3Creds.Key,
			Secret:   s3Creds.Secret,
			Endpoint: s3Creds.Endpoint,
		},
	})
	if err != nil {
		errStatus, _ := status.FromError(err)
		if codes.NotFound == errStatus.Code() {
			return nil, ErrOTFNotFound
		}
		return nil, err
	}
	return dr.Diffs, nil
}

// DeltaDiffGRPCPlugin is responsible for generating a client and a server for the Delta Diff plugin implementation.
// If the plugin server is not implemented in Go, the GRPCServer method is useless.
type DeltaDiffGRPCPlugin struct {
	// DeltaDiffGRPCPlugin must implement the Plugin interface
	plugin.Plugin
}

// GRPCServer must be implemented even though we won't use it
func (p *DeltaDiffGRPCPlugin) GRPCServer(broker *plugin.GRPCBroker, s *grpc.Server) error {
	return nil
}

// GRPCClient will return the Delta diff GRPC custom client
func (p *DeltaDiffGRPCPlugin) GRPCClient(ctx context.Context, broker *plugin.GRPCBroker, c *grpc.ClientConn) (interface{}, error) {
	return &GRPCClient{
		client: NewDifferClient(c), // This is the underlying GRPC client
	}, nil
}
