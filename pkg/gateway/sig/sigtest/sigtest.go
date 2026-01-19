package sigtest

import (
	"time"

	gatewayerrors "github.com/treeverse/lakefs/pkg/gateway/errors"
)

// ClockSkewTestCase represents a test case with that can be used to test
// clock skew validation used across different signature versions (V2, JavaV2, V4)
type ClockSkewTestCase struct {
	Name          string
	Offset        time.Duration
	ExpectedError error
}

// CommonClockSkewTestCases returns a set of clock skew boundary tests that apply
// to all AWS signature versions. These test the limits of the given clock skew window.
func CommonClockSkewTestCases(maxClockSkew time.Duration) []ClockSkewTestCase {
	if maxClockSkew <= 0 {
		panic("maxClockSkew must be positive")
	}

	const denominator = 2

	return []ClockSkewTestCase{
		{
			Name:          "valid request with current time",
			Offset:        0,
			ExpectedError: nil,
		},
		{
			Name:          "request within clock skew (past)",
			Offset:        -maxClockSkew / denominator,
			ExpectedError: nil,
		},
		{
			Name:          "request within clock skew (future)",
			Offset:        maxClockSkew / denominator,
			ExpectedError: nil,
		},
		{
			Name:          "request beyond clock skew (past)",
			Offset:        -maxClockSkew - time.Minute,
			ExpectedError: gatewayerrors.ErrRequestTimeTooSkewed,
		},
		{
			Name:          "request beyond clock skew (future)",
			Offset:        maxClockSkew + time.Minute,
			ExpectedError: gatewayerrors.ErrRequestNotReadyYet,
		},
		{
			Name:          "request just within clock skew boundary (past)",
			Offset:        -maxClockSkew + time.Second,
			ExpectedError: nil,
		},
		{
			Name:          "request just within clock skew boundary (future)",
			Offset:        maxClockSkew - time.Second,
			ExpectedError: nil,
		},
	}
}
