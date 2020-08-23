package factory

import (
	"context"
	"errors"
	"fmt"

	"github.com/treeverse/lakefs/db"

	"cloud.google.com/go/storage"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	log "github.com/sirupsen/logrus"
	"github.com/treeverse/lakefs/block"
	"github.com/treeverse/lakefs/block/gs"
	"github.com/treeverse/lakefs/block/local"
	"github.com/treeverse/lakefs/block/mem"
	"github.com/treeverse/lakefs/block/params"
	s3a "github.com/treeverse/lakefs/block/s3"
	"github.com/treeverse/lakefs/block/transient"
	"github.com/treeverse/lakefs/config"
	"github.com/treeverse/lakefs/logging"
	"google.golang.org/api/option"
)

var ErrInvalidBlockStoreType = errors.New("invalid blockstore type")

func BuildBlockAdapter(c *config.Config, dbPool db.Database) (block.Adapter, error) {
	blockstore := c.GetBlockstoreType()
	logging.Default().
		WithField("type", blockstore).
		Info("initialize blockstore adapter")
	switch blockstore {
	case local.BlockstoreType:
		p, err := c.GetBlockAdapterLocalParams()
		if err != nil {
			return nil, err
		}
		return buildLocalAdapter(p)
	case s3a.BlockstoreType:
		p, err := c.GetBlockAdapterS3Params()
		if err != nil {
			return nil, err
		}
		return buildS3Adapter(p)
	case mem.BlockstoreType, "memory":
		return mem.New(), nil
	case transient.BlockstoreType:
		return transient.New(), nil
	case gs.BlockstoreType:
		p, err := c.GetBlockAdapterGSParams()
		if err != nil {
			return nil, err
		}
		return buildGSAdapter(p, dbPool)
	default:
		return nil, fmt.Errorf("%w '%s' please choose one of %s",
			ErrInvalidBlockStoreType, blockstore, []string{local.BlockstoreType, s3a.BlockstoreType, mem.BlockstoreType, transient.BlockstoreType, gs.BlockstoreType})
	}
}

func buildLocalAdapter(params params.Local) (*local.Adapter, error) {
	adapter, err := local.NewAdapter(params.Path)
	if err != nil {
		return nil, fmt.Errorf("got error opening a local block adapter with path %s: %w", params.Path, err)
	}
	logging.Default().WithFields(logging.Fields{
		"type": "local",
		"path": params.Path,
	}).Info("initialized blockstore adapter")
	return adapter, nil
}

func buildS3Adapter(params params.S3) (*s3a.Adapter, error) {
	sess, err := session.NewSession(params.AwsConfig)
	if err != nil {
		return nil, err
	}
	sess.ClientConfig(s3.ServiceName)
	svc := s3.New(sess)
	adapter := s3a.NewAdapter(svc,
		s3a.WithStreamingChunkSize(params.StreamingChunkSize),
		s3a.WithStreamingChunkTimeout(params.StreamingChunkTimeout),
	)
	logging.Default().WithField("type", "s3").Info("initialized blockstore adapter")
	return adapter, nil
}

func buildGSAdapter(params params.GS, dbPool db.Database) (*gs.Adapter, error) {
	var opts []option.ClientOption
	if params.CredentialsFile != "" {
		opts = append(opts, option.WithCredentialsFile(params.CredentialsFile))
	}
	ctx := context.Background()
	client, err := storage.NewClient(ctx, opts...)
	if err != nil {
		return nil, err
	}
	adapter := gs.NewAdapter(client, dbPool)
	log.WithField("type", "gs").Info("initialized blockstore adapter")
	return adapter, nil
}
