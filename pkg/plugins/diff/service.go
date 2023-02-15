package tablediff

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/treeverse/lakefs/pkg/plugins/internal"
)

var (
	ErrTableNotFound = errors.New("table not found")
)

type ChangeType int

const (
	Changed ChangeType = iota
	Dropped
	Created
)

type DiffEntry struct {
	Version          string
	Timestamp        time.Time
	Operation        string
	OperationContent map[string]string
}

type Response struct {
	ChangeType
	Diffs []DiffEntry
}

type RefPath struct {
	Ref  string
	Path string
}

type TablePaths struct {
	Left  RefPath
	Right RefPath
	Base  RefPath
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
	closeFunctions map[string]func()
	l              sync.Mutex
}

// NewService is used to initialize a new Differ service. The returned function is a closing function for the service.
func NewService() (*Service, func()) {
	service := &Service{
		pluginHandler:  internal.NewManager[Differ](),
		closeFunctions: make(map[string]func()),
	}
	return service, service.Close
}

func (s *Service) RunDiff(ctx context.Context, diffType string, diffParams Params) (Response, error) {
	d, closeClient, err := s.pluginHandler.LoadPluginClient(diffType)
	if err != nil {
		return Response{}, err
	}
	if closeClient != nil {
		s.appendClosingFunction(diffType, closeClient)
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

func (s *Service) appendClosingFunction(diffType string, f func()) {
	s.l.Lock()
	defer s.l.Unlock()
	if _, ok := s.closeFunctions[diffType]; !ok {
		s.closeFunctions[diffType] = f
	}
}
