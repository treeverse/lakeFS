package testutil

import (
	"github.com/cenkalti/backoff/v4"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/graveler/mock"
	kvmock "github.com/treeverse/lakefs/pkg/kv/mock"
)

type GravelerTest struct {
	Controller               *gomock.Controller
	CommittedManager         *mock.MockCommittedManager
	RefManager               *mock.MockRefManager
	StagingManager           *mock.MockStagingManager
	ProtectedBranchesManager *mock.MockProtectedBranchesManager
	GarbageCollectionManager *mock.MockGarbageCollectionManager
	KVStore                  *kvmock.MockStore
	Sut                      *graveler.Graveler
}

func InitGravelerTest(t *testing.T) *GravelerTest {
	ctrl := gomock.NewController(t)

	test := &GravelerTest{
		Controller:               ctrl,
		CommittedManager:         mock.NewMockCommittedManager(ctrl),
		StagingManager:           mock.NewMockStagingManager(ctrl),
		RefManager:               mock.NewMockRefManager(ctrl),
		GarbageCollectionManager: mock.NewMockGarbageCollectionManager(ctrl),
		ProtectedBranchesManager: mock.NewMockProtectedBranchesManager(ctrl),
		KVStore:                  kvmock.NewMockStore(ctrl),
	}

	test.Sut = graveler.NewGraveler(test.CommittedManager, test.StagingManager, test.RefManager, test.GarbageCollectionManager, test.ProtectedBranchesManager, nil)

	return test
}

// ShortenBranchUpdateBackOff upgrade graveler branch update back-off to shorten test duration
func ShortenBranchUpdateBackOff(g *graveler.Graveler) {
	const updateRetryDuration = 200 * time.Millisecond
	g.BranchUpdateBackOff = backoff.NewConstantBackOff(updateRetryDuration)
}
