package tablediff

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/mitchellh/go-homedir"

	"github.com/treeverse/lakefs/pkg/config"
	"github.com/treeverse/lakefs/pkg/logging"
	"github.com/treeverse/lakefs/pkg/plugins"
	"github.com/treeverse/lakefs/pkg/plugins/internal"
)

var (
	ErrTableNotFound = errors.New("table not found")
	ErrLoadingPlugin = errors.New("failed to load diff plugin")
	ErrFailedDiff    = errors.New("failed to run diff")
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
	ID               string
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

func (s *Service) RunDiff(ctx context.Context, diffType string, diffParams Params) (Response, error) {
	d, closeClient, err := s.pluginHandler.LoadPluginClient(diffType)
	if err != nil {
		logging.FromContext(ctx).WithError(err).
			WithField("type", diffType).
			WithField("params", fmt.Sprintf("%+v", diffParams)).
			Error("failed to load the plugin client")
		return Response{}, ErrLoadingPlugin
	}
	if closeClient != nil {
		s.appendClosingFunction(diffType, closeClient)
	}

	diffResponse, err := d.Diff(ctx, diffParams)
	if err != nil {
		logging.FromContext(ctx).WithError(err).
			WithField("type", diffType).
			WithField("params", fmt.Sprintf("%+v", diffParams)).
			Error("failed to perform diff")
		if errors.Is(err, ErrTableNotFound) {
			return Response{}, err
		}
		return Response{}, ErrFailedDiff
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

func (s *Service) EnabledDiffs() []string {
	return s.pluginHandler.Plugins()
}

// NewService is used to initialize a new Differ service. The returned function is a closing function for the service.
func NewService(diffProps config.DiffProps, pluginProps config.Plugins) (*Service, func()) {
	service := &Service{
		pluginHandler:  internal.NewManager[Differ](),
		closeFunctions: make(map[string]func()),
	}
	registerPlugins(service, diffProps, pluginProps)
	return service, service.Close
}

func registerPlugins(service *Service, diffProps config.DiffProps, pluginProps config.Plugins) {
	registerDefaultPlugins(service, pluginProps.DefaultPath)

	if diffProps.Delta.PluginName != "" {
		registerPlugin(service, pluginProps, RegisterDeltaLakeDiffPlugin, "delta", diffProps.Delta.PluginName)
	}
}

func registerDefaultPlugins(service *Service, pluginsPath string) {
	diffPluginsDir := diffPluginsDefaultPath(pluginsPath)
	deltaPath := filepath.Join(diffPluginsDir, "delta")
	_, err := os.Stat(deltaPath)
	if err != nil {
		if !os.IsNotExist(err) {
			logging.Default().WithError(err).Error("failed to access delta lake diff plugin")
		}
		return
	}

	pid := plugins.PluginIdentity{ProtocolVersion: 1, ExecutableLocation: deltaPath}
	pa := plugins.PluginHandshake{}
	RegisterDeltaLakeDiffPlugin(service, pid, pa)
}

func registerPlugin(service *Service, pluginProps config.Plugins, rf registrationFunc, diffType, pluginName string) {
	// If the requested plugin wasn't configured with a path, it will be defined under the default location
	pluginPath := filepath.Join(diffPluginsDefaultPath(pluginProps.DefaultPath), pluginName)
	pluginVersion := 1 // default version
	if props, ok := pluginProps.Properties[pluginName]; ok {
		pp, err := homedir.Expand(props.Path)
		if err != nil {
			logging.Default().Errorf("failed to register a plugin for an invalid path: '%s'", props.Path)
			return
		}
		pluginPath = pp
		if props.Version != 0 {
			pluginVersion = props.Version
		}
	}

	pid := plugins.PluginIdentity{ProtocolVersion: uint(pluginVersion), ExecutableLocation: pluginPath}
	pa := plugins.PluginHandshake{}
	rf(service, pid, pa)

	logging.Default().Infof("successfully registered a plugin for diff type: '%s'", diffType)
}

type registrationFunc func(ds *Service, pid plugins.PluginIdentity, handshake plugins.PluginHandshake)

func diffPluginsDefaultPath(pluginsPath string) string {
	pp, _ := homedir.Expand(pluginsPath)
	return filepath.Join(pp, "diff")
}
