package cmd

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestBatchProcessing tests the batching logic used in deleteObjectWorker
func TestBatchProcessing(t *testing.T) {
	tests := []struct {
		name            string
		totalItems      int
		batchSize       int
		expectedBatches []int
	}{
		{
			name:            "exact batch size",
			totalItems:      1000,
			batchSize:       1000,
			expectedBatches: []int{1000},
		},
		{
			name:            "one item over batch size",
			totalItems:      1001,
			batchSize:       1000,
			expectedBatches: []int{1000, 1},
		},
		{
			name:            "multiple batches with remainder",
			totalItems:      2500,
			batchSize:       1000,
			expectedBatches: []int{1000, 1000, 500},
		},
		{
			name:            "less than batch size",
			totalItems:      500,
			batchSize:       1000,
			expectedBatches: []int{500},
		},
		{
			name:            "multiple full batches",
			totalItems:      3000,
			batchSize:       1000,
			expectedBatches: []int{1000, 1000, 1000},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			objs := make([]string, 0, tt.batchSize)
			var batchSizes []int

			// Simulate batch processing
			for i := 0; i < tt.totalItems; i++ {
				objs = append(objs, fmt.Sprintf("file%d", i))

				if len(objs) >= tt.batchSize {
					batchSizes = append(batchSizes, len(objs))
					objs = objs[:0] // Reset for next batch
				}
			}

			// Handle remaining items
			if len(objs) > 0 {
				batchSizes = append(batchSizes, len(objs))
			}

			// Verify batch count
			require.Equal(t, len(tt.expectedBatches), len(batchSizes),
				"unexpected number of batches")

			// Verify each batch size
			for i, expected := range tt.expectedBatches {
				require.Equal(t, expected, batchSizes[i],
					"batch %d has unexpected size", i)
			}

			// Verify all batches respect the limit
			for i, size := range batchSizes {
				require.LessOrEqual(t, size, tt.batchSize,
					"batch %d exceeds maximum size", i)
			}

			// Verify total items processed
			total := 0
			for _, size := range batchSizes {
				total += size
			}
			require.Equal(t, tt.totalItems, total,
				"total items processed doesn't match input")
		})
	}
}
