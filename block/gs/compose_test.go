package gs

import (
	"fmt"
	"strconv"
	"testing"
)

func TestComposeAll(t *testing.T) {
	const targetFile = "data.file"
	numberOfPartsTests := []int{1, 10, 10000}
	for _, numberOfParts := range numberOfPartsTests {
		t.Run("compose_" + strconv.Itoa(numberOfParts), func(t *testing.T) {
			// prepare data
			parts := make([]string, numberOfParts)
			for i := 0; i < numberOfParts; i++ {
				parts[i] = fmt.Sprintf("part%d", i)
			}

			// map to track
			usedParts := make(map[string]struct{})
			usedTargets := make(map[string]struct{})

			// compose
			err := ComposeAll(targetFile, parts, func(target string, parts []string) error {
				for _, part := range parts {
					if _, found := usedParts[part]; found {
						t.Errorf("Part '%s' already composed", part)
					}
					usedParts[part] = struct{}{}
				}
				if _, found := usedTargets[target]; found {
					t.Errorf("Target '%s' already composed with %s", target, parts)
				}
				usedTargets[target] = struct{}{}
				return nil
			}, )
			if err != nil {
				t.Fatal(err)
			}
			for _, part := range parts {
				if _, ok := usedParts[part]; !ok {
					t.Error("Missing part:", part)
				}
			}
			if _, ok := usedTargets[targetFile]; !ok {
				t.Error("Missing target:", targetFile)
			}
		})
	}
}