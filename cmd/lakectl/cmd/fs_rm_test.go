package cmd

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

// Test the slice reset logic directly without needing a full mock
func TestSliceResetLogic(t *testing.T) {
	// This test verifies that our fix properly resets the slice length
	// The bug was using clear(objs) which doesn't reset length
	// The fix uses objs = objs[:0] which does reset length

	t.Run("clear does not reset length", func(t *testing.T) {
		objs := make([]string, 0, 1000)
		for i := 0; i < 1000; i++ {
			objs = append(objs, fmt.Sprintf("file%d", i))
		}
		require.Equal(t, 1000, len(objs))
		
		// Using clear zeroes elements but keeps length
		clear(objs)
		require.Equal(t, 1000, len(objs), "clear() does not reset slice length - this is the bug!")
		
		// Appending after clear would make the slice grow beyond original size
		objs = append(objs, "new-file")
		require.Equal(t, 1001, len(objs))
	})

	t.Run("slice reset with [:0] correctly resets length", func(t *testing.T) {
		objs := make([]string, 0, 1000)
		for i := 0; i < 1000; i++ {
			objs = append(objs, fmt.Sprintf("file%d", i))
		}
		require.Equal(t, 1000, len(objs))
		
		// Using [:0] resets length to 0
		objs = objs[:0]
		require.Equal(t, 0, len(objs), "[:0] correctly resets slice length")
		require.Equal(t, 1000, cap(objs), "[:0] preserves capacity")
		
		// Appending after [:0] starts from 0
		objs = append(objs, "new-file")
		require.Equal(t, 1, len(objs))
	})

	t.Run("multiple batch simulation", func(t *testing.T) {
		// Simulate what happens in deleteObjectWorker
		const batchSize = 1000
		objs := make([]string, 0, batchSize)
		batchSizes := []int{}

		// Simulate processing 2500 files
		for i := 0; i < 2500; i++ {
			objs = append(objs, fmt.Sprintf("file%d", i))
			
			if len(objs) >= batchSize {
				// Record batch size
				batchSizes = append(batchSizes, len(objs))
				
				// With the fix: objs = objs[:0]
				objs = objs[:0]
			}
		}
		
		// Handle remaining files
		if len(objs) > 0 {
			batchSizes = append(batchSizes, len(objs))
		}

		// Verify we got 3 batches: 1000, 1000, 500
		require.Equal(t, 3, len(batchSizes))
		require.Equal(t, 1000, batchSizes[0])
		require.Equal(t, 1000, batchSizes[1])
		require.Equal(t, 500, batchSizes[2])
		
		// Verify no batch exceeds the limit
		for i, size := range batchSizes {
			require.LessOrEqual(t, size, batchSize, "batch %d exceeds limit", i)
		}
	})

	t.Run("bug simulation with clear", func(t *testing.T) {
		// Simulate what would happen with the bug (using clear instead of [:0])
		const batchSize = 1000
		objs := make([]string, 0, batchSize)
		batchSizes := []int{}

		// Simulate processing 2500 files
		for i := 0; i < 2500; i++ {
			objs = append(objs, fmt.Sprintf("file%d", i))
			
			if len(objs) >= batchSize {
				// Record batch size
				batchSizes = append(batchSizes, len(objs))
				
				// With the bug: clear(objs) - doesn't reset length!
				clear(objs)
			}
		}
		
		// Handle remaining files
		if len(objs) > 0 {
			batchSizes = append(batchSizes, len(objs))
		}

		// With the bug, we'd get: 1000, 1001, 1002, ...
		require.Greater(t, len(batchSizes), 3, "bug causes many small batches")
		
		// The first batch would be 1000
		require.Equal(t, 1000, batchSizes[0])
		
		// But subsequent batches would exceed the limit due to the bug
		for i := 1; i < len(batchSizes)-1; i++ {
			require.Greater(t, batchSizes[i], batchSize, "bug causes batches %d to exceed limit", i)
		}
	})
}
