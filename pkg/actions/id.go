package actions

import (
	"github.com/treeverse/lakefs/pkg/graveler"
)

type IDGenerator interface {
	// NewRunID creates IDs for Runs.
	NewRunID() string
}

// DecreasingIDGenerator creates IDs that are decreasing with time
type DecreasingIDGenerator struct{}

func (gen *DecreasingIDGenerator) NewRunID() string {
	return graveler.NewRunID()
}
