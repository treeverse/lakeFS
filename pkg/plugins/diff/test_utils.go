package tablediff

import (
	"context"
	"errors"
	"strconv"
	"time"

	"github.com/treeverse/lakefs/pkg/plugins/internal"
)

var ErrNotFound = errors.New("plugin not found")
var ErrDiffFailed = errors.New("failed")

const (
	diffType             = "aDiff"
	ControllerTestPlugin = "controller"
	PluginPath           = "plugin_path"
	PluginVersion        = "plugin_version"
)

type MockHandler struct {
	differs map[string]Differ
}

func (mh *MockHandler) RegisterPlugin(name string, pp internal.HCPluginProperties) {
	mh.differs[name] = TestDiffer{pp}
}
func (mh *MockHandler) LoadPluginClient(name string) (Differ, func(), error) {
	d, ok := mh.differs[name]
	if !ok {
		return nil, nil, ErrNotFound
	}

	return d, nil, nil
}

func NewMockHandler() *MockHandler {
	mh := MockHandler{
		differs: make(map[string]Differ),
	}
	mh.differs[ControllerTestPlugin] = ControllerTestDiffer{}
	return &mh
}

type TestDiffer struct {
	props internal.HCPluginProperties
}

func (td TestDiffer) Diff(ctx context.Context, p Params) (Response, error) {
	e := ErrorFromContext(ctx)
	if e != nil {
		return Response{}, e
	}

	r := Response{
		DiffType: DiffTypeChanged,
		Diffs: []DiffEntry{
			{
				Version:   "v0",
				Timestamp: time.Unix(0, 0),
				Operation: "op",
				OperationContent: map[string]string{
					PluginPath:    td.props.ID.ExecutableLocation,
					PluginVersion: strconv.Itoa(int(td.props.ID.ProtocolVersion)),
				},
			},
		},
	}
	return r, nil
}

type ControllerTestDiffer struct{}

func (ctd ControllerTestDiffer) Diff(ctx context.Context, p Params) (Response, error) {
	left := p.TablePaths.Left.Ref
	right := p.TablePaths.Right.Ref
	notfound := "notfound"
	if left == notfound && right == notfound {
		return Response{}, ErrTableNotFound
	}

	var ct = DiffTypeChanged
	switch {
	case left == "dropped":
		ct = DiffTypeDropped
	case right == "created":
		ct = DiffTypeCreated
	}
	var diffs []DiffEntry
	if ct != DiffTypeDropped {
		diffs = generateDiffs()
	}
	r := Response{
		DiffType: ct,
		Diffs:    diffs,
	}
	return r, nil
}

func generateDiffs() []DiffEntry {
	return []DiffEntry{
		{
			Version:   "v0",
			Timestamp: time.Unix(0, 0),
			Operation: "op",
			OperationContent: map[string]string{
				"errKey": "value",
			},
		},
	}
}

func NewMockService() *Service {
	return &Service{
		pluginHandler:  NewMockHandler(),
		closeFunctions: nil,
	}
}

type keyType string

const errKey keyType = "error"

func ContextWithError(ctx context.Context, e error) context.Context {
	return context.WithValue(ctx, errKey, e)
}

func ErrorFromContext(ctx context.Context) error {
	e, ok := ctx.Value(errKey).(error)
	if !ok {
		return nil
	}
	return e
}
