package graveler

import "fmt"

type SensorCB func(repositoryID RepositoryID, branchID BranchID, stagingTokenID string)

type StagingTokenCounter struct {
	StagingTokenID string
	Counter        int
}

type DeleteSensor struct {
	cb                     SensorCB
	branchTombstoneCounter map[string]*StagingTokenCounter
	triggerAt              int
}

func getCombinedKey(repositoryID RepositoryID, branchID BranchID) string {
	return fmt.Sprintf("%s:%s", repositoryID, branchID)
}

func NewDeletedSensor(cb SensorCB, triggerAt int) *DeleteSensor {
	return &DeleteSensor{
		cb:                     cb,
		branchTombstoneCounter: make(map[string]*StagingTokenCounter),
		triggerAt:              triggerAt,
	}
}

func (s *DeleteSensor) CountDelete(repositoryID RepositoryID, branchID BranchID, stagingTokenID string) {
	combinedKey := getCombinedKey(repositoryID, branchID)
	stCounter, ok := s.branchTombstoneCounter[combinedKey]
	if !ok {
		s.branchTombstoneCounter[combinedKey] = &StagingTokenCounter{
			StagingTokenID: stagingTokenID,
			Counter:        1,
		}
		return
	}

	if stCounter.StagingTokenID != stagingTokenID {
		stCounter.StagingTokenID = stagingTokenID
		stCounter.Counter = 1
		return
	}

	if stCounter.Counter >= s.triggerAt-1 {
		stCounter.Counter = 0
		s.cb(repositoryID, branchID, stagingTokenID)
		return
	}
	// TODO(Guys): Change this to be atomic
	stCounter.Counter++
}
