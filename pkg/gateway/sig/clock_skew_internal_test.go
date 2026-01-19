package sig_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/gateway/sig"
	"github.com/treeverse/lakefs/pkg/gateway/sig/sigtest"
)

func TestValidateClockSkew(t *testing.T) {
	now := time.Date(2025, 12, 12, 10, 0, 0, 0, time.UTC)

	testCases := sigtest.CommonClockSkewTestCases(sig.AmzMaxClockSkew)
	for _, tc := range testCases {
		t.Run(tc.Name, func(t *testing.T) {
			requestTime := now.Add(tc.Offset)
			err := sig.ValidateClockSkew(now, requestTime)
			require.ErrorIs(t, err, tc.ExpectedError)
		})
	}
}
