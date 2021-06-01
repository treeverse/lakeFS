package retention

import (
	"testing"

	"github.com/treeverse/lakefs/pkg/graveler"
)

func TestBasic(t *testing.T) {
	a := &graveler.Commit{Message: "a", Parents: []graveler.CommitID{}}
	b := &graveler.Commit{Message: "b", Parents: []graveler.CommitID{"a"}}
	c := &graveler.Commit{Message: "c", Parents: []graveler.CommitID{"a"}}
	d := &graveler.Commit{Message: "d", Parents: []graveler.CommitID{"c1"}}
	e := &graveler.Commit{Message: "e", Parents: []graveler.CommitID{"c2"}}
	f := &graveler.Commit{Message: "c5", Parents: []graveler.CommitID{"c3"}}
}

func verifyResult(t *testing.T, base *graveler.Commit, expected []string) {
}
