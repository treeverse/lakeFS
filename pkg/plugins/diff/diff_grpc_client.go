package diff

import (
	"context"
	"github.com/hashicorp/go-plugin"
	"google.golang.org/grpc"
)

type GrpcClient struct {
	client DifferClient // This is the underlying GRPC API client
}

func (d *GrpcClient) Diff(ctx context.Context, paths TablePaths, s3Creds S3Creds) ([]*Diff, error) {
	dr, err := d.client.Diff(ctx, &DiffRequest{
		Paths: &DiffPaths{
			LeftPath:  paths.LeftTablePath,
			RightPath: paths.RightTablePath,
		},
		GatewayConfig: &GatewayConfig{
			Key:      s3Creds.Key,
			Secret:   s3Creds.Secret,
			Endpoint: s3Creds.Endpoint,
		},
	})
	if err != nil {
		return nil, err
	}
	return dr.Diffs, nil
}

type GRPCPlugin struct {
	// GRPCPlugin must implement the Plugin interface
	plugin.Plugin
	Impl Differ
}

// GRPCServer must be implemented even though we won't use it
func (p *GRPCPlugin) GRPCServer(broker *plugin.GRPCBroker, s *grpc.Server) error {
	return nil
}

// GRPCClient will return the Delta diff GRPC custom client
func (p *GRPCPlugin) GRPCClient(ctx context.Context, broker *plugin.GRPCBroker, c *grpc.ClientConn) (interface{}, error) {
	return &GrpcClient{
		client: NewDifferClient(c), // This is the underlying GRPC client
	}, nil
}
