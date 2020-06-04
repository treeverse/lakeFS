package loadtest

import (
	"fmt"
)

const FileCountInCommit = 20

type Scenario interface {
	Play(loader Loader, stopCh chan struct{}) <-chan Request
}

type SimpleScenario struct {
	FileCountInCommit int
}

func (s *SimpleScenario) Play(serverAddress string, repoName string, stopCh chan struct{}) <-chan Request {
	targetGenerator := RequestGenerator{ServerAddress: serverAddress}
	out := make(chan Request)
	go func() {
		defer close(out)
		for i := 0; true; i++ {
			for _, tgt := range targetGenerator.GenerateCreateFileTargets(repoName, "master", FileCountInCommit) {
				out <- tgt
			}
			out <- targetGenerator.GenerateCommitTarget(repoName, fmt.Sprintf("commit%d", i))
			out <- targetGenerator.GenerateBranchTarget(repoName, fmt.Sprintf("branch%d", i))

			for _, tgt := range targetGenerator.GenerateCreateFileTargets(repoName, fmt.Sprintf("branch%d", i), FileCountInCommit) {
				out <- tgt
			}
			out <- targetGenerator.GenerateMergeToMasterTarget(repoName, fmt.Sprintf("branch%d", i))
			out <- targetGenerator.GenerateListTarget(repoName, "master", 100)
			out <- targetGenerator.GenerateListTarget(repoName, "master", 1000)
			out <- targetGenerator.GenerateDiffTarget(repoName, "master")

			select {
			case <-stopCh:
				return
			default:
			}
		}
	}()
	return out
}
