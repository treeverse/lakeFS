package testutil

import "github.com/treeverse/lakefs/parade"

// TaskIdSlice attaches the methods of sort.Interface to []TaskId.
type TaskIdSlice []parade.TaskID

func (p TaskIdSlice) Len() int           { return len(p) }
func (p TaskIdSlice) Less(i, j int) bool { return p[i] < p[j] }
func (p TaskIdSlice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }
