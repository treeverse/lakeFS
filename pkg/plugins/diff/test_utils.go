package tablediff

import (
	"context"
	"errors"
	"time"

	"github.com/treeverse/lakefs/pkg/plugins/internal"
)

var ErrNotFound = errors.New("plugin not found")
var ErrDiffFailed = errors.New("failed")

const (
	diffType             = "aDiff"
	ControllerTestPlugin = "controller"
)

type MockHandler struct {
	differs map[string]Differ
}

func (mh *MockHandler) RegisterPlugin(name string, pp internal.HCPluginProperties) {
	mh.differs[name] = TestDiffer{}
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

type TestDiffer struct{}

func (ed TestDiffer) Diff(ctx context.Context, p Params) (Response, error) {
	e := ErrorFromContext(ctx)
	if e != nil {
		return Response{}, e
	}

	r := Response{
		ChangeType: Changed,
		Diffs:      generateDiffs(),
	}
	return r, nil
}

type ControllerTestDiffer struct{}

func (ctd ControllerTestDiffer) Diff(ctx context.Context, p Params) (Response, error) {
	left := p.TablePaths.LeftTablePath.Ref
	right := p.TablePaths.RightTablePath.Ref
	notfound := "notfound"
	if left == notfound && right == notfound {
		return Response{}, ErrTableNotFound
	}

	var ct = Changed
	switch {
	case left == "dropped":
		ct = Dropped
	case right == "created":
		ct = Created
	}
	var diffs []DiffEntry
	if ct != Dropped {
		diffs = generateDiffs()
	}
	r := Response{
		ChangeType: ct,
		Diffs:      diffs,
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

func ContextWithError(ctx context.Context, e error) context.Context {
	return context.WithValue(ctx, "error", e)
}

func ErrorFromContext(ctx context.Context) error {
	e, ok := ctx.Value("error").(error)
	if !ok {
		return nil
	}
	return e
}
