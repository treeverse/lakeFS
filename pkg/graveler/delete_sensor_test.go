package graveler_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/treeverse/lakefs/pkg/graveler"
)

func TestDeletedSensor(t *testing.T) {
	type commandFlow struct {
		repositoryID   graveler.RepositoryID
		branchID       graveler.BranchID
		stagingTokenID graveler.StagingToken
		count          int
	}
	tt := []struct {
		name                           string
		triggerAt                      int
		commandFlow                    []commandFlow
		expectedTriggeredBranchesCount map[string]int
	}{
		{
			name:      "trigger after 10",
			triggerAt: 10,
			commandFlow: []commandFlow{
				{repositoryID: "repo1", branchID: "branch1", stagingTokenID: "100-example-uuid", count: 2},
				{repositoryID: "repo1", branchID: "branch2", stagingTokenID: "100-example-uuid", count: 10},
			},
			expectedTriggeredBranchesCount: map[string]int{"repo1-branch2": 1},
		},
		{
			name:      "trigger two",
			triggerAt: 10,
			commandFlow: []commandFlow{
				{repositoryID: "repo1", branchID: "branch1", stagingTokenID: "555-example-uuid", count: 10},
				{repositoryID: "repo1", branchID: "branch2", stagingTokenID: "555-example-uuid", count: 11},
			},
			expectedTriggeredBranchesCount: map[string]int{"repo1-branch1": 1, "repo1-branch2": 1},
		},
		{
			name:      "trigger twice after 20",
			triggerAt: 10,
			commandFlow: []commandFlow{
				{repositoryID: "repo1", branchID: "branch1", stagingTokenID: "100-example-uuid", count: 20},
			},
			expectedTriggeredBranchesCount: map[string]int{"repo1-branch1": 2},
		},
		{
			name:      "trigger once before 20",
			triggerAt: 10,
			commandFlow: []commandFlow{
				{repositoryID: "repo1", branchID: "branch1", stagingTokenID: "444-example-uuid", count: 19},
			},
			expectedTriggeredBranchesCount: map[string]int{"repo1-branch1": 1},
		},
		{
			name:      "different repos no trigger",
			triggerAt: 10,
			commandFlow: []commandFlow{
				{repositoryID: "repo1", branchID: "branch1", stagingTokenID: "100-example-uuid", count: 9},
				{repositoryID: "repo2", branchID: "branch1", stagingTokenID: "100-example-uuid", count: 9},
				{repositoryID: "repo3", branchID: "branch1", stagingTokenID: "100-example-uuid", count: 9},
			},
		},
		{
			name:      "different repos trigger once",
			triggerAt: 10,
			commandFlow: []commandFlow{
				{repositoryID: "repo1", branchID: "branch1", stagingTokenID: "100-example-uuid", count: 8},
				{repositoryID: "repo2", branchID: "branch1", stagingTokenID: "100-example-uuid", count: 8},
				{repositoryID: "repo3", branchID: "branch1", stagingTokenID: "100-example-uuid", count: 8},
				{repositoryID: "repo2", branchID: "branch1", stagingTokenID: "100-example-uuid", count: 8},
			},
			expectedTriggeredBranchesCount: map[string]int{"repo2-branch1": 1},
		},
		{
			name:      "different staging token id trigger once",
			triggerAt: 10,
			commandFlow: []commandFlow{
				{repositoryID: "repo1", branchID: "branch1", stagingTokenID: "uuid-token-1", count: 8},
				{repositoryID: "repo1", branchID: "branch1", stagingTokenID: "uuid-token-2", count: 8},
				{repositoryID: "repo1", branchID: "branch1", stagingTokenID: "uuid-token-3", count: 8},
				{repositoryID: "repo1", branchID: "branch1", stagingTokenID: "uuid-token-3", count: 8},
			},
			expectedTriggeredBranchesCount: map[string]int{"repo1-branch1": 1},
		},
		{
			name:      "different staging token id no trigger",
			triggerAt: 10,
			commandFlow: []commandFlow{
				{repositoryID: "repo1", branchID: "branch1", stagingTokenID: "uuid-token-1", count: 8},
				{repositoryID: "repo1", branchID: "branch1", stagingTokenID: "uuid-token-2", count: 8},
				{repositoryID: "repo1", branchID: "branch1", stagingTokenID: "uuid-token-3", count: 8},
				{repositoryID: "repo1", branchID: "branch1", stagingTokenID: "uuid-token-4", count: 8},
			},
		},
	}
	ctx := context.Background()
	for _, tc := range tt {
		t.Run(tc.name, func(t *testing.T) {
			triggredBranches := make(map[string]int)
			cb := func(repositoryID graveler.RepositoryID, branchID graveler.BranchID, stagingTokenID graveler.StagingToken, inGrace bool) {
				triggredBranches[string(repositoryID)+"-"+string(branchID)]++
			}
			sensor := graveler.NewDeleteSensor(tc.triggerAt, cb)
			for _, flow := range tc.commandFlow {
				for i := 0; i < flow.count; i++ {
					sensor.CountDelete(ctx, flow.repositoryID, flow.branchID, flow.stagingTokenID)
				}
			}
			sensor.Close()
			if len(triggredBranches) != len(tc.expectedTriggeredBranchesCount) {
				t.Errorf("expected %d branches to be triggered, got %d", len(tc.expectedTriggeredBranchesCount), len(triggredBranches))
			}
			for branchID, count := range triggredBranches {
				if count != tc.expectedTriggeredBranchesCount[branchID] {
					t.Errorf("expected %s to be triggered %d times, got %d", branchID, tc.expectedTriggeredBranchesCount[branchID], count)
				}
			}
		})
	}

}

func TestDeletedSensor_Close(t *testing.T) {
	cb := func(repositoryID graveler.RepositoryID, branchID graveler.BranchID, stagingTokenID graveler.StagingToken, inGrace bool) {
	}
	sensor := graveler.NewDeleteSensor(10, cb)
	sensor.Close()
}

func TestDeletedSensor_CloseTwice(t *testing.T) {
	cb := func(repositoryID graveler.RepositoryID, branchID graveler.BranchID, stagingTokenID graveler.StagingToken, inGrace bool) {
	}
	sensor := graveler.NewDeleteSensor(10, cb)
	sensor.Close()
	sensor.Close()
}

func TestDeletedSensor_CountAfterClose(t *testing.T) {
	cb := func(repositoryID graveler.RepositoryID, branchID graveler.BranchID, stagingTokenID graveler.StagingToken, inGrace bool) {
	}
	sensor := graveler.NewDeleteSensor(10, cb)
	sensor.Close()
	ctx := context.Background()
	sensor.CountDelete(ctx, "repo1", "branch1", "uuid")
}

func TestDeletedSensor_CheckNonBlocking(t *testing.T) {
	closerCall := sync.Once{}
	closerCh := make(chan struct{})

	cb := func(repositoryID graveler.RepositoryID, branchID graveler.BranchID, stagingTokenID graveler.StagingToken, inGrace bool) {
		if inGrace {
			return
		}
		time.Sleep(5 * time.Second)
		closerCall.Do(func() {
			close(closerCh)
		})
	}
	sensor := graveler.NewDeleteSensor(1, cb, graveler.WithCBBufferSize(1))
	ctx := context.Background()
	for i := 0; i < 11; i++ {
		select {
		case <-closerCh:
			t.Fatal("should not block")
			return
		default:
			sensor.CountDelete(ctx, "repo1", "branch1", "uuid")
		}
	}
}
