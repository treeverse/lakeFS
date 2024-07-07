package gs

import (
	"fmt"
	"strconv"
	"sync"
	"testing"

	"cloud.google.com/go/storage"
)

func TestComposeAll(t *testing.T) {
	const targetFile = "data.file"
	numberOfPartsTests := []int{1, 10, 10000}
	for _, numberOfParts := range numberOfPartsTests {
		t.Run("compose_"+strconv.Itoa(numberOfParts), func(t *testing.T) {
			// prepare data
			parts := make([]string, numberOfParts)
			for i := 0; i < numberOfParts; i++ {
				parts[i] = fmt.Sprintf("part%d", i)
			}

			// map to track
			var usedParts sync.Map
			var usedTargets sync.Map

			// compose
			_, err := ComposeAll(targetFile, parts, func(target string, parts []string) (*storage.ObjectAttrs, error) {
				for _, part := range parts {
					if _, found := usedParts.Load(part); found {
						t.Errorf("Part '%s' already composed", part)
					}
					usedParts.Store(part, struct{}{})
				}
				if _, found := usedTargets.Load(target); found {
					t.Errorf("Target '%s' already composed with %s", target, parts)
				}
				usedTargets.Store(target, struct{}{})
				return nil, nil
			})
			if err != nil {
				t.Fatal(err)
			}
			for _, part := range parts {
				if _, ok := usedParts.Load(part); !ok {
					t.Error("Missing part:", part)
				}
			}
			if _, ok := usedTargets.Load(targetFile); !ok {
				t.Error("Missing target:", targetFile)
			}
		})
	}
}
