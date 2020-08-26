package gs

import (
	"fmt"
)

const MaxPartsInCompose = 32

type ComposeFunc func(target string, parts []string) error

func ComposeAll(target string, parts []string, composeFunc ComposeFunc) error {
	for layer := 1; len(parts) > MaxPartsInCompose; layer++ {
		var nextParts []string
		for i := 0; i < len(parts); i += MaxPartsInCompose {
			chunkSize := len(parts) - i
			if chunkSize > MaxPartsInCompose {
				chunkSize = MaxPartsInCompose
			}
			chunk := parts[i : i+chunkSize]
			if chunkSize == 1 || (chunkSize < MaxPartsInCompose && len(nextParts)+chunkSize <= MaxPartsInCompose) {
				nextParts = append(nextParts, chunk...)
			} else {
				targetName := fmt.Sprintf("%s_%d", chunk[0], layer)
				if err := composeFunc(targetName, chunk); err != nil {
					return err
				}
				nextParts = append(nextParts, targetName)
			}
		}
		parts = nextParts
	}
	return composeFunc(target, parts)
}
