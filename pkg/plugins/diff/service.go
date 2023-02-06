package table_diff

import (
	"context"
	"github.com/hashicorp/go-plugin"
	"github.com/treeverse/lakefs/pkg/plugins/internal"
	"time"
)

type Entry struct {
	Version          string
	Timestamp        time.Time
	Operation        string
	OperationContent map[string]string
}

type Response struct {
	Diffs []Entry
}

type DiffResponseTransformer func(*DiffResponse) Response

type RefPath struct {
	Ref  string
	Path string
}

type TablePaths struct {
	LeftTablePath  RefPath
	RightTablePath RefPath
	BaseTablePath  RefPath
}

type S3Creds struct {
	Key      string
	Secret   string
	Endpoint string
}

type Params struct {
	TablePaths TablePaths
	S3Creds    S3Creds
}

type Differ interface {
	Diff(context.Context, Params) (Response, error)
}

type PluginProperties struct {
	ID        internal.PluginIdentity
	Handshake internal.PluginHandshake
	P         plugin.Plugin
}

type Service struct {
	pluginManager  internal.Controller[Differ]
	closeFunctions []func()
}

func NewService() *Service {
	return &Service{
		pluginManager:  internal.NewManager[Differ](),
		closeFunctions: make([]func(), 0),
	}
}

func (s *Service) RunDiff(ctx context.Context, diffType string, diffParams Params) (Response, error) {
	d, closeClient, err := s.pluginManager.LoadPluginClient(diffType)
	if err != nil {
		return Response{}, err
	}

	s.closeFunctions = append(s.closeFunctions, closeClient)

	diffs, err := d.Diff(ctx, diffParams)
	if err != nil {
		return Response{}, err
	}
	return diffs, nil
}

func (s *Service) CloseClients() {
	for _, cf := range s.closeFunctions {
		cf()
	}
}

func (s *Service) RegisterDiffClient(diffType string, props PluginProperties) {
	s.pluginManager.RegisterPlugin(diffType, props.ID, props.Handshake, props.P)
}
