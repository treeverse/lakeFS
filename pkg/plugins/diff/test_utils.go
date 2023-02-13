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
	diffType      = "aDiff"
	DefaultPlugin = "default"
)

type MockHandler struct {
	PropertyMap map[string]internal.HCPluginProperties
}

func (mh *MockHandler) RegisterPlugin(name string, pp internal.HCPluginProperties) {
	mh.PropertyMap[name] = pp
}
func (mh *MockHandler) LoadPluginClient(name string) (Differ, func(), error) {
	_, ok := mh.PropertyMap[name]
	if !ok {
		return nil, nil, ErrNotFound
	}
	return TestDiffer{}, nil, nil
}

func NewMockHandler() *MockHandler {
	mh := MockHandler{PropertyMap: make(map[string]internal.HCPluginProperties)}
	mh.PropertyMap[DefaultPlugin] = internal.HCPluginProperties{
		ID:        internal.PluginIdentity{},
		Handshake: internal.PluginHandshake{},
		P:         nil,
	}
	return &mh
}

type TestDiffer struct{}

func (ed TestDiffer) Diff(ctx context.Context, p Params) (Response, error) {
	//e := ErrorFromContext(ctx)
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
