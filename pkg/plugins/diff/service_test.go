package tablediff

import (
	"context"
	"errors"
	"testing"

	"github.com/treeverse/lakefs/pkg/plugins/internal"
)

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
			expectedErr: ErrNotFound,
		},
		{
			register:    true,
			diffFailure: true,
			description: "failure - internal diff failed",
			expectedErr: ErrDiffFailed,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			ctx := context.Background()
			service := NewMockService()
			if tc.register {
				service.registerDiffClient(diffType, internal.HCPluginProperties{})
			}
			ctx = contextWithError(ctx, tc.expectedErr)
			_, err := service.RunDiff(ctx, diffType, Params{})
			if err != nil && !errors.Is(err, tc.expectedErr) {
				t.Errorf("'%s' failed: %s", tc.description, err)
			}

		})
	}
}
