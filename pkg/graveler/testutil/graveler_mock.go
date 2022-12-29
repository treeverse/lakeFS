package testutil

import (
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/graveler/mock"
)

type GravelerTest struct {
	CommittedManager         *mock.MockCommittedManager
	RefManager               *mock.MockRefManager
	StagingManager           *mock.MockStagingManager
	ProtectedBranchesManager *mock.MockProtectedBranchesManager
	GarbageCollectionManager *mock.MockGarbageCollectionManager
	Sut                      *graveler.Graveler
}

func InitGravelerTest(t *testing.T) *GravelerTest {
	ctrl := gomock.NewController(t)

	test := &GravelerTest{
		CommittedManager:         mock.NewMockCommittedManager(ctrl),
		StagingManager:           mock.NewMockStagingManager(ctrl),
		RefManager:               mock.NewMockRefManager(ctrl),
		GarbageCollectionManager: mock.NewMockGarbageCollectionManager(ctrl),
		ProtectedBranchesManager: mock.NewMockProtectedBranchesManager(ctrl),
	}

	test.Sut = graveler.NewGraveler(test.CommittedManager, test.StagingManager, test.RefManager, test.GarbageCollectionManager, test.ProtectedBranchesManager)

	return test
}
