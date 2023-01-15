package diff

import (
	"context"
	"fmt"
	"github.com/treeverse/lakefs/pkg/plugins"
	"strconv"
	"time"
)

type Differ interface {
	Diff(context.Context, TablePaths, S3Creds) ([]*Diff, error)
}

const pluginType = plugins.PluginType("diff")

var errUnknownDiffType = fmt.Errorf("unknown diff type")

type TablePaths struct {
	LeftTablePath  string
	RightTablePath string
	BaseTablePath  string
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

type Service struct {
	pluginManager plugins.Manager
}

type Entry struct {
	Version          string
	Timestamp        time.Time
	Operation        string
	OperationContent map[string]string
}

func (s *Service) RunDiff(ctx context.Context, diffName string, diffParams interface{}) ([]Entry, error) {
	pn := plugins.PluginName(diffName)
	w, err := s.pluginManager.WrapPlugin(pluginType, pn)
	if err != nil {
		return nil, err
	}
	defer w.DestroyClient()
	dispenseName, err := s.pluginManager.PluginImplName(pluginType, pn)
	if err != nil {
		return nil, err
	}
	switch diffName {
	case "delta":
		params := diffParams.(Params)
		return s.runDeltaDiff(ctx, params, w, dispenseName)
	default:
		return nil, errUnknownDiffType
	}
}

func (s *Service) runDeltaDiff(ctx context.Context, params Params, w *plugins.Wrapper, implName string) ([]Entry, error) {
	grpc := *(w.GRPCClient)
	grpcRaw, err := grpc.Dispense(implName)
	if err != nil {
		return nil, err
	}
	deltaDiffer := grpcRaw.(Differ)
	diffs, err := deltaDiffer.Diff(ctx, params.TablePaths, params.S3Creds)
	result := make([]Entry, len(diffs))
	for _, diff := range diffs {
		result = append(result, Entry{
			Version:          strconv.Itoa(int(diff.Version)),
			Timestamp:        diff.Timestamp.AsTime(),
			Operation:        diff.Description,
			OperationContent: diff.Content,
		})
	}
	return result, nil
}
