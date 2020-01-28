package merkle_test

import (
	"strings"
	"testing"
	"time"

	"github.com/treeverse/lakefs/index/model"
)

type entryLike interface {
	GetType() model.Entry_Type
	GetName() string
	GetAddress() string
}

func compareEntries(a, b entryLike) int {
	// directories first
	if a.GetType() != b.GetType() {
		if a.GetType() < b.GetType() {
			return -1
		} else if a.GetType() > b.GetType() {
			return 1
		}
		return 0
	}
	// then sort by name
	return strings.Compare(a.GetName(), b.GetName())
}

func TestNew(t *testing.T) {
	res := compareEntries(&model.Entry{
		Name:      "hooks",
		Address:   "abc",
		Type:      model.Entry_OBJECT,
		Timestamp: time.Now().Unix(),
		Size:      300,
	}, &model.Entry{
		Name:      "hooks",
		Address:   "def",
		Type:      model.Entry_OBJECT,
		Timestamp: time.Now().Unix(),
		Size:      300,
	})

	if res != 0 {
		t.Fatalf("expected 0 got %d", res)
	}

}
