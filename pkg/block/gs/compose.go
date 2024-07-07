package gs

import (
	"fmt"
	"sync"

	"cloud.google.com/go/storage"
)

const MaxPartsInCompose = 32

type ComposeFunc func(target string, parts []string) (*storage.ObjectAttrs, error)

func ComposeAll(target string, parts []string, composeFunc ComposeFunc) (*storage.ObjectAttrs, error) {
	var wg sync.WaitGroup
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
				wg.Add(1)
				go composeChunkConcurrent(targetName, chunk, composeFunc, &wg)
				nextParts = append(nextParts, targetName)
			}
		}
		parts = nextParts
	}
	wg.Wait()

	// no compose the chunks we made
	return composeFunc(target, parts)
}

func composeChunkConcurrent(target string, parts []string, composeFunc ComposeFunc, wg *sync.WaitGroup) {
	// ctx context.Context, a *Adapter, target string, parts []string, bucketName string, bucket *storage.BucketHandle, wg *sync.WaitGroup) {
	_, err := composeFunc(target, parts)
	if err != nil {
		fmt.Println("Compose error: ", err)
	}
	wg.Done()
}
