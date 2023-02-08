package table_diff

import (
	"context"
	"errors"
	"github.com/treeverse/lakefs/pkg/plugins/internal"
	"testing"
)

var errNotFound = errors.New("plugin not found")
var errDiffFailed = errors.New("failed")

const diffType = "aDiff"

func TestService_RunDiff(t *testing.T) {
	testCases := []struct {
		register    bool
		diffFailure bool
		description string
		expectedErr error
	}{
		{
			register:    true,
			description: "successful run",
			expectedErr: nil,
		},
		{
			register:    false,
			description: "failure - no client loaded",
			expectedErr: errNotFound,
		},
		{
			register:    true,
			diffFailure: true,
			description: "failure - internal diff failed",
			expectedErr: errDiffFailed,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			ctx := context.Background()
			service := Service{
				pluginHandler:  NewMockHandler[Differ](),
				closeFunctions: nil,
			}
			if tc.register {
				service.RegisterDiffClient(diffType, internal.HCPluginProperties{})
			}
			ctx = ContextWithFailDiff(ctx, tc.diffFailure)
			_, err := service.RunDiff(ctx, diffType, Params{})
			if err != nil && !errors.Is(err, tc.expectedErr) {
				t.Errorf("'%s' failed: %s", tc.description, err)
			}

		})
	}
}

type MockHandler[T Differ] struct {
	PropertyMap map[string]internal.HCPluginProperties
}

func (mh *MockHandler[T]) RegisterPlugin(name string, pp internal.HCPluginProperties) {
	mh.PropertyMap[name] = pp
}
func (mh *MockHandler[T]) LoadPluginClient(name string) (Differ, func(), error) {
	_, ok := mh.PropertyMap[name]
	if !ok {
		return nil, nil, errNotFound
	}
	return TestDiffer{}, nil, nil
}

func NewMockHandler[T Differ]() *MockHandler[T] {
	return &MockHandler[T]{PropertyMap: make(map[string]internal.HCPluginProperties)}
}

type TestDiffer Response

func (ed TestDiffer) Diff(ctx context.Context, p Params) (Response, error) {
	shouldFail := FailDiffFromContext(ctx)
	if shouldFail {
		return Response{}, errDiffFailed
	}
	return Response(ed), nil
}

type failureKey string

const key failureKey = "fail"

func ContextWithFailDiff(ctx context.Context, shouldFail bool) context.Context {
	return context.WithValue(ctx, key, shouldFail)
}

func FailDiffFromContext(ctx context.Context) bool {
	shouldFail, ok := ctx.Value(key).(bool)
	if !ok {
		return ok
	}
	return shouldFail
}
