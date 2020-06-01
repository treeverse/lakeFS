package loadtesting

import (
	"fmt"
	vegeta "github.com/tsenart/vegeta/v12/lib"
)

type Request struct {
	Target      vegeta.Target
	RequestType string
}

type Scenario interface {
	Play(loadTester LoadTester, stopCh chan struct{}) <-chan Request
}

type SimpleScenario struct {
}

func (s *SimpleScenario) Play(serverAddress string, repoName string, stopCh chan struct{}) <-chan Request {
	targetGenerator := TargetGenerator{ServerAddress: serverAddress}
	out := make(chan Request)
	go func() {
		defer close(out)
		for i := 0; true; i++ {
			for _, tgt := range targetGenerator.GenerateCreateFileTargets(repoName, "master", 20) {
				out <- tgt
			}
			out <- targetGenerator.GenerateCommitTarget(repoName, fmt.Sprintf("commit%d", i))
			out <- targetGenerator.GenerateBranchTarget(repoName, fmt.Sprintf("branch%d", i))

			for _, tgt := range targetGenerator.GenerateCreateFileTargets(repoName, fmt.Sprintf("branch%d", i), 20) {
				out <- tgt
			}
			out <- targetGenerator.GenerateMergeToMasterTarget(repoName, fmt.Sprintf("branch%d", i))
			select {
			case <-stopCh:
				return
			default:
			}
		}
	}()
	return out
}
