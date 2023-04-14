package ref_test

import (
	"strconv"
	"testing"
	"time"

	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/graveler/ref"
)

func TestCommitsGenerationPriorityQueue_Len(t *testing.T) {
	const maxItems = 7
	q := ref.NewCommitsGenerationPriorityQueue()
	for i := 0; i < maxItems; i++ {
		if q.Len() != i {
			t.Fatalf("Queue Len=%d, expected=%d", q.Len(), i)
		}
		id := graveler.CommitID(strconv.Itoa(i))
		q.Push(&graveler.CommitRecord{CommitID: id})
	}
	if q.Len() != maxItems {
		t.Fatalf("Queue Len=%d, expected=%d", q.Len(), maxItems)
	}
}

func TestCommitsGenerationPriorityQueue_Swap(t *testing.T) {
	rec1 := &graveler.CommitRecord{CommitID: "1"}
	rec2 := &graveler.CommitRecord{CommitID: "2"}

	q := ref.NewCommitsGenerationPriorityQueue()
	q.Push(rec1)
	q.Push(rec2)
	q.Swap(0, 1)
	if q[0].CommitID != rec2.CommitID && q[1].CommitID != rec1.CommitID {
		t.Fatal("Swap expected to replace records positions")
	}
}

func TestCommitsGenerationPriorityQueue_Push(t *testing.T) {
	q := ref.NewCommitsGenerationPriorityQueue()
	rec1 := &graveler.CommitRecord{CommitID: "1"}
	q.Push(rec1)
	if len(q) != 1 || q[0] != rec1 {
		t.Fatalf("Push() failed first record - len=%d", len(q))
	}
	rec2 := &graveler.CommitRecord{CommitID: "2"}
	q.Push(rec2)
	if len(q) != 2 || q[1] != rec2 {
		t.Fatalf("Push() failed second record - len=%d", len(q))
	}
}

func TestCommitsGenerationPriorityQueue_Pop(t *testing.T) {
	const maxItems = 7
	q := ref.NewCommitsGenerationPriorityQueue()
	for i := 0; i < maxItems; i++ {
		id := graveler.CommitID(strconv.Itoa(i))
		q.Push(&graveler.CommitRecord{CommitID: id})
	}
	for i := 0; i < maxItems; i++ {
		item := q.Pop().(*graveler.CommitRecord)
		expectedID := graveler.CommitID(strconv.Itoa(maxItems - i - 1))
		if item.CommitID != expectedID {
			t.Fatalf("Pop() item ID=%s, expected=%s", item.CommitID, expectedID)
		}
	}
	if l := q.Len(); l != 0 {
		t.Fatalf("Pop() all items - Len=%d, expected=0", l)
	}
}

func TestCommitsGenerationPriorityQueue_Less(t *testing.T) {
	ts1 := time.Now()
	ts2 := ts1.Add(time.Minute)
	tests := []struct {
		Name     string
		Commit1  graveler.Commit
		Commit2  graveler.Commit
		Expected bool
	}{
		{Name: "generation_ascend", Commit1: graveler.Commit{Generation: 0}, Commit2: graveler.Commit{Generation: 1}, Expected: false},
		{Name: "generation_descent", Commit1: graveler.Commit{Generation: 1}, Commit2: graveler.Commit{Generation: 0}, Expected: true},
		{Name: "same_generation_creation_ascend", Commit1: graveler.Commit{Generation: 0, CreationDate: ts1}, Commit2: graveler.Commit{Generation: 0, CreationDate: ts2}, Expected: false},
		{Name: "same_generation_creation_descent", Commit1: graveler.Commit{Generation: 0, CreationDate: ts2}, Commit2: graveler.Commit{Generation: 0, CreationDate: ts1}, Expected: true},
		{Name: "same_generation_and_creation", Commit1: graveler.Commit{Generation: 0, CreationDate: ts1}, Commit2: graveler.Commit{Generation: 0, CreationDate: ts1}, Expected: false},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.Name, func(t *testing.T) {
			// setup
			q := ref.NewCommitsGenerationPriorityQueue()
			q.Push(&graveler.CommitRecord{CommitID: "1", Commit: &tt.Commit1})
			q.Push(&graveler.CommitRecord{CommitID: "2", Commit: &tt.Commit2})
			result := q.Less(0, 1)
			if result != tt.Expected {
				t.Fatalf("Less() result=%t, expected=%t", result, tt.Expected)
			}
		})
	}
}
