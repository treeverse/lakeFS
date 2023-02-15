package tablediff

import (
	"context"

	"github.com/hashicorp/go-plugin"
	"github.com/treeverse/lakefs/pkg/plugins"
	"github.com/treeverse/lakefs/pkg/plugins/internal"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type DeltaLakeDiffer struct {
	client TableDifferClient // This is the underlying GRPC API client
}

func (d *DeltaLakeDiffer) Diff(ctx context.Context, ps Params) (Response, error) {
	ltp := ps.TablePaths.Left
	rtp := ps.TablePaths.Right
	btp := ps.TablePaths.Base
	s3Creds := ps.S3Creds
	dr, err := d.client.TableDiff(ctx, &DiffRequest{
		Props: &DiffProps{
			Repo: ps.Repo,
			LeftTablePath: &TablePath{
				Path: ltp.Path,
				Ref:  ltp.Ref,
			},
			RightTablePath: &TablePath{
				Path: rtp.Path,
				Ref:  rtp.Ref,
			},
			BaseTablePath: &TablePath{
				Path: btp.Path,
				Ref:  btp.Ref,
			},
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
			return Response{}, ErrTableNotFound
		}
		return Response{}, err
	}
	return Response{
		Diffs:      buildDiffEntries(dr),
		ChangeType: ChangeType(dr.GetChangeType()),
	}, nil
}

func buildDiffEntries(dr *DiffResponse) []DiffEntry {
	result := make([]DiffEntry, 0, len(dr.GetEntries()))
	for _, diff := range dr.GetEntries() {
		result = append(result, DiffEntry{
			Version:          diff.GetId(),
			Timestamp:        diff.GetTimestamp().AsTime(),
			Operation:        diff.GetOperation(),
			OperationContent: diff.GetContent(),
		})
	}
	return result
}

// DeltaDiffGRPCPlugin is responsible for generating a client and a server for the Delta Lake Diff plugin implementation.
type DeltaDiffGRPCPlugin struct {
	// DeltaDiffGRPCPlugin must implement the Plugin interface
	plugin.Plugin
}

// GRPCServer must be implemented even though we won't use it. Delta Lake's server implementation is written in Rust.
func (p DeltaDiffGRPCPlugin) GRPCServer(broker *plugin.GRPCBroker, s *grpc.Server) error {
	return nil
}

// GRPCClient will return the Delta diff GRPC custom client
func (p DeltaDiffGRPCPlugin) GRPCClient(ctx context.Context, broker *plugin.GRPCBroker, c *grpc.ClientConn) (interface{}, error) {
	return &DeltaLakeDiffer{
		client: NewTableDifferClient(c),
	}, nil
}

func RegisterDeltaLakeDiffPlugin(ds *Service, pid plugins.PluginIdentity, handshake plugins.PluginHandshake) {
	props := internal.HCPluginProperties{
		ID:        pid,
		Handshake: handshake,
		P:         DeltaDiffGRPCPlugin{},
	}
	ds.registerDiffClient("delta", props)
}
