package pyramid_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/pyramid"
	"github.com/treeverse/lakefs/pkg/pyramid/params"
)

var testFileMap map[string]bool

func TestTracker(t *testing.T) {
	const filename = "test_filename"
	file1 := params.RelativePath(filename)
	file2 := params.RelativePath("dummy_file")
	testFileMap = map[string]bool{}
	tracker := pyramid.NewFileTracker(testTrackerDeleteCB)

	// Delete non existent
	tracker.Delete(file2)

	// Delete file before close
	testFileMap[string(file1)] = true
	testFileMap[string(file2)] = true
	closer := tracker.Open(file1)
	_ = tracker.Open(file1)
	_ = tracker.Open(file2)
	tracker.Delete(file1)
	require.True(t, testFileMap[string(file1)])

	// Close one reference and ensure file still not deleted
	closer()
	require.True(t, testFileMap[string(file1)])

	// Close last reference and ensure file was deleted
	closer()
	require.False(t, testFileMap[string(file1)])
	require.True(t, testFileMap[string(file2)])

	// Close deleted file - sanity
	closer()

	// Delete after close
	testFileMap[string(file1)] = true
	_ = tracker.Open(file1)
	closer()
	tracker.Delete(file1)
	require.False(t, testFileMap[string(file1)])
	require.True(t, testFileMap[string(file2)])
}

func testTrackerDeleteCB(p params.RelativePath) {
	testFileMap[string(p)] = false
}
