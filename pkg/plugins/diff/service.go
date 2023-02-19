package tablediff

import (
	"context"
	"errors"
	"time"

	"github.com/treeverse/lakefs/pkg/plugins/internal"
)

var (
	ErrTableNotFound = errors.New("table not found")
)

const (
	DiffTypeChanged = "changed"
	DiffTypeDropped = "dropped"
	DiffTypeCreated = "created"
	OpTypeCreate    = "create"
	OpTypeUpdate    = "update"
	OpTypeDelete    = "delete"
)

func getOpType(operationType OperationType) string {
	switch operationType {
	case OperationType_CREATE:
		return OpTypeCreate
	case OperationType_DELETE:
		return OpTypeDelete
	default:
		return OpTypeUpdate
	}
}

func getDiffType(diffType DiffType) string {
	switch diffType {
	case DiffType_CREATED:
		return DiffTypeCreated
	case DiffType_DROPPED:
		return DiffTypeDropped
	default:
		return DiffTypeChanged
	}
}

type DiffEntry struct {
	Version          string
	Timestamp        time.Time
	Operation        string
	OperationContent map[string]string
	OperationType    string
}

type Response struct {
	DiffType string
	Diffs    []DiffEntry
}

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
	Repo       string
}

type Differ interface {
	Diff(context.Context, Params) (Response, error)
}

// Service is responsible for registering new Differ plugins and executing them at will.
// After initializing a Service, the CloseClients method should be called at some point to close gracefully all
// remaining plugins.
type Service struct {
	pluginHandler  internal.Handler[Differ, internal.HCPluginProperties]
	closeFunctions []func()
}

// NewService is used to initialize a new Differ service. The returned function is a closing function for the service.
func NewService() (*Service, func()) {
	service := &Service{
		pluginHandler:  internal.NewManager[Differ](),
		closeFunctions: make([]func(), 0),
	}
	return service, service.Close
}

func (s *Service) RunDiff(ctx context.Context, diffType string, diffParams Params) (Response, error) {
	d, closeClient, err := s.pluginHandler.LoadPluginClient(diffType)
	if err != nil {
		return Response{}, err
	}
	if closeClient != nil {
		s.closeFunctions = append(s.closeFunctions, closeClient)
	}

	diffResponse, err := d.Diff(ctx, diffParams)
	if err != nil {
		return Response{}, err
	}
	return diffResponse, nil
}

// Close should be called upon the destruction of the Service.
func (s *Service) Close() {
	for _, cf := range s.closeFunctions {
		cf()
	}
}

func (s *Service) registerDiffClient(diffType string, props internal.HCPluginProperties) {
	s.pluginHandler.RegisterPlugin(diffType, props)
}
