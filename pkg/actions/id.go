package actions

import (
	"time"

	gonanoid "github.com/matoous/go-nanoid/v2"
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

// IncreasingIDGenerator creates IDs that are increasing with time
type IncreasingIDGenerator struct{}

func (gen *IncreasingIDGenerator) NewRunID() string {
	const nanoLen = 8
	id := gonanoid.Must(nanoLen)
	tm := time.Now().UTC().Format(graveler.RunIDTimeLayout)
	return tm + id
}
