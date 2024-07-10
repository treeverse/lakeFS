package gs

import (
	"fmt"

	"cloud.google.com/go/storage"
	"golang.org/x/sync/errgroup"
)

const MaxPartsInCompose = 32

type ComposeFunc func(target string, parts []string) (*storage.ObjectAttrs, error)

func ComposeAll(target string, parts []string, composeFunc ComposeFunc) (*storage.ObjectAttrs, error) {
	for layer := 1; len(parts) > MaxPartsInCompose; layer++ {
		group := errgroup.Group{}
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
				nextParts = append(nextParts, targetName)
				group.Go(func() error {
					_, err := composeFunc(targetName, chunk)
					return err
				})
			}
		}
		parts = nextParts
		// wait for layer of composes to complete
		if err := group.Wait(); err != nil {
			return nil, err
		}
	}

	return composeFunc(target, parts) // final compose
}
