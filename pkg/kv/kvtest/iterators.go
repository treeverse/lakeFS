package kvtest

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/go-test/deep"
	"github.com/treeverse/lakefs/pkg/kv"
)

const (
	firstPartitionKey  = "m"
	secondPartitionKey = "ma"
)

type StoreWithCounter struct {
	kv.Store
	calls map[string]int
}

func NewStoreWithCounter(store kv.Store) *StoreWithCounter {
	return &StoreWithCounter{Store: store, calls: make(map[string]int)}
}

func (swc *StoreWithCounter) Scan(ctx context.Context, partitionKey []byte, options kv.ScanOptions) (kv.EntriesIterator, error) {
	// TODO lock
	swc.calls["Scan"]++
	return swc.Store.Scan(ctx, partitionKey, options)
}

func testPartitionIterator(t *testing.T, ms MakeStore) {
	ctx := context.Background()
	store := ms(t, ctx)
	m := []TestModel{
		{Name: []byte("a")},
		{Name: []byte("aa")},
		{Name: []byte("b")},
		{Name: []byte("c")},
		{Name: []byte("d")},
	}

	// prepare data
	for i := 0; i < len(m); i++ {
		err := kv.SetMsg(ctx, store, firstPartitionKey, m[i].Name, &m[i])
		if err != nil {
			t.Fatal("failed to set model", err)
		}

		err = kv.SetMsg(ctx, store, secondPartitionKey, m[i].Name, &m[i])
		if err != nil {
			t.Fatal("failed to set model", err)
		}
	}

	t.Run("listing all values of partition", func(t *testing.T) {
		itr := kv.NewPartitionIterator(ctx, store, (&TestModel{}).ProtoReflect().Type(), firstPartitionKey, 0)
		if itr == nil {
			t.Fatalf("failed to create partition iterator")
		}
		defer itr.Close()
		names := make([]string, 0)
		for itr.Next() {
			e := itr.Entry()
			model, ok := e.Value.(*TestModel)
			if !ok {
				t.Fatalf("Failed to cast entry to TestModel")
			}
			names = append(names, string(model.Name))
		}
		if itr.Err() != nil {
			t.Fatalf("unexpected error: %v", itr.Err())
		}

		if diffs := deep.Equal(names, []string{"a", "aa", "b", "c", "d"}); diffs != nil {
			t.Fatalf("got wrong list of names: %v", diffs)
		}
	})

	t.Run("listing values SeekGE", func(t *testing.T) {
		itr := kv.NewPartitionIterator(ctx, store, (&TestModel{}).ProtoReflect().Type(), secondPartitionKey, 0)
		if itr == nil {
			t.Fatalf("failed to create partition iterator")
		}
		defer itr.Close()
		for _, seekValue := range []string{"b", "aaa", "b"} {
			itr.SeekGE([]byte(seekValue))
			names := make([]string, 0)
			for itr.Next() {
				e := itr.Entry()
				model, ok := e.Value.(*TestModel)
				if !ok {
					t.Fatalf("Failed to cast entry to TestModel")
				}
				names = append(names, string(model.Name))
			}
			if itr.Err() != nil {
				t.Fatalf("unexpected error: %v", itr.Err())
			}
			if diffs := deep.Equal(names, []string{"b", "c", "d"}); diffs != nil {
				t.Fatalf("got wrong list of names: %v", diffs)
			}
		}
	})
	t.Run("count scans on successive SeekGE operations", func(t *testing.T) {
		store := NewStoreWithCounter(store)
		itr := kv.NewPartitionIterator(ctx, store, (&TestModel{}).ProtoReflect().Type(), secondPartitionKey, 0)
		if itr == nil {
			t.Fatalf("failed to create partition iterator")
		}
		defer itr.Close()
		for _, seekValue := range []string{"b", "c", "d"} {
			itr.SeekGE([]byte(seekValue))
			if !itr.Next() {
				t.Fatalf("Expected iterator to have a value")
			}
			if itr.Err() != nil {
				t.Fatalf("unexpected error: %v", itr.Err())
			}
			k := itr.Entry().Key
			if string(k) != seekValue {
				t.Fatalf("Expected to find value %s. Found %s", seekValue, k)
			}
		}
		if store.calls["Scan"] != 1 {
			t.Fatalf("Expected exactly 1 call to Scan. got: %d", store.calls["Scan"])
		}
	})

	t.Run("failed SeekGE partition not found", func(t *testing.T) {
		itr := kv.NewPartitionIterator(ctx, store, (&TestModel{}).ProtoReflect().Type(), "", 0)
		if itr == nil {
			t.Fatalf("failed to create partition iterator")
		}
		defer itr.Close()
		itr.SeekGE([]byte("d"))
		if itr.Next() {
			t.Fatalf("next after seekGE expected to be false")
		}

		itr.SeekGE([]byte("d"))
		if itr.Next() {
			t.Fatalf("next after seekGE expected to be false")
		}

		if !errors.Is(itr.Err(), kv.ErrMissingPartitionKey) {
			t.Fatalf("expected error: %s, got %v", kv.ErrMissingPartitionKey, itr.Err())
		}
	})
}

func testPrimaryIterator(t *testing.T, ms MakeStore) {
	ctx := context.Background()
	store := ms(t, ctx)
	m := []TestModel{
		{Name: []byte("a")},
		{Name: []byte("aa")},
		{Name: []byte("b")},
		{Name: []byte("c")},
		{Name: []byte("d")},
	}

	// prepare data
	for i := 0; i < len(m); i++ {
		err := kv.SetMsg(ctx, store, firstPartitionKey, m[i].Name, &m[i])
		if err != nil {
			t.Fatal("failed to set model", err)
		}

		err = kv.SetMsg(ctx, store, secondPartitionKey, m[i].Name, &m[i])
		if err != nil {
			t.Fatal("failed to set model", err)
		}
	}

	t.Run("listing all values of partition", func(t *testing.T) {
		itr, err := kv.NewPrimaryIterator(ctx, store, (&TestModel{}).ProtoReflect().Type(),
			firstPartitionKey, []byte(""), kv.IteratorOptionsFrom([]byte("")))
		if err != nil {
			t.Fatalf("failed to create primary iterator: %v", err)
		}
		defer itr.Close()
		names := make([]string, 0)
		for itr.Next() {
			e := itr.Entry()
			model, ok := e.Value.(*TestModel)
			if !ok {
				t.Fatalf("Failed to cast entry to TestModel")
			}
			names = append(names, string(model.Name))
		}
		if itr.Err() != nil {
			t.Fatalf("unexpected error: %v", itr.Err())
		}

		if diffs := deep.Equal(names, []string{"a", "aa", "b", "c", "d"}); diffs != nil {
			t.Fatalf("got wrong list of names: %v", diffs)
		}
	})

	t.Run("listing with prefix", func(t *testing.T) {
		itr, err := kv.NewPrimaryIterator(ctx, store, (&TestModel{}).ProtoReflect().Type(),
			secondPartitionKey, []byte("a"), kv.IteratorOptionsFrom([]byte("")))
		if err != nil {
			t.Fatalf("failed to create primary iterator: %v", err)
		}
		defer itr.Close()
		names := make([]string, 0)
		for itr.Next() {
			e := itr.Entry()
			model, ok := e.Value.(*TestModel)
			if !ok {
				t.Fatalf("Failed to cast entry to TestModel")
			}
			names = append(names, string(model.Name))
		}
		if itr.Err() != nil {
			t.Fatalf("unexpected error: %v", itr.Err())
		}

		if diffs := deep.Equal(names, []string{"a", "aa"}); diffs != nil {
			t.Fatalf("got wrong list of names: %v", diffs)
		}
	})

	t.Run("listing empty value", func(t *testing.T) {
		itr, err := kv.NewPrimaryIterator(ctx, store, (&TestModel{}).ProtoReflect().Type(),
			secondPartitionKey, []byte(""), kv.IteratorOptionsAfter([]byte("d")))
		if err != nil {
			t.Fatalf("failed to create primary iterator: %v", err)
		}
		defer itr.Close()

		if itr.Next() {
			t.Fatalf("next should return false")
		}
		if itr.Err() != nil {
			t.Fatalf("unexpected error: %v", itr.Err())
		}
	})

	t.Run("listing from", func(t *testing.T) {
		itr, err := kv.NewPrimaryIterator(ctx, store, (&TestModel{}).ProtoReflect().Type(),
			firstPartitionKey, []byte(""), kv.IteratorOptionsFrom([]byte("b")))
		if err != nil {
			t.Fatalf("failed to create primary iterator: %v", err)
		}
		defer itr.Close()
		names := make([]string, 0)
		for itr.Next() {
			e := itr.Entry()
			model, ok := e.Value.(*TestModel)
			if !ok {
				t.Fatalf("Failed to cast entry to TestModel")
			}
			names = append(names, string(model.Name))
		}
		if itr.Err() != nil {
			t.Fatalf("unexpected error: %v", itr.Err())
		}

		if diffs := deep.Equal(names, []string{"b", "c", "d"}); diffs != nil {
			t.Fatalf("got wrong list of names: %v", diffs)
		}
	})

	t.Run("listing after", func(t *testing.T) {
		itr, err := kv.NewPrimaryIterator(ctx, store, (&TestModel{}).ProtoReflect().Type(),
			firstPartitionKey, []byte(""), kv.IteratorOptionsAfter([]byte("b")))
		if err != nil {
			t.Fatalf("failed to create primary iterator: %v", err)
		}
		defer itr.Close()
		names := make([]string, 0)
		for itr.Next() {
			e := itr.Entry()
			model, ok := e.Value.(*TestModel)
			if !ok {
				t.Fatalf("Failed to cast entry to TestModel")
			}
			names = append(names, string(model.Name))
		}
		if itr.Err() != nil {
			t.Fatalf("unexpected error: %v", itr.Err())
		}

		if diffs := deep.Equal(names, []string{"c", "d"}); diffs != nil {
			t.Fatalf("got wrong list of names: %v", diffs)
		}
	})
}

func testSecondaryIterator(t *testing.T, ms MakeStore) {
	ctx := context.Background()

	// prepare data
	store := ms(t, ctx)
	m := []TestModel{
		{Name: []byte("a"), JustAString: "one"},
		{Name: []byte("b"), JustAString: "two"},
		{Name: []byte("c"), JustAString: "three"},
		{Name: []byte("d"), JustAString: "four"},
		{Name: []byte("e"), JustAString: "five"},
	}

	for i := 0; i < len(m); i++ {
		primaryKey := append([]byte("name/"), m[i].Name...)
		err := kv.SetMsg(ctx, store, firstPartitionKey, primaryKey, &m[i])
		if err != nil {
			t.Fatal("failed to set model", err)
		}
		secondaryKey := append([]byte("just_a_string/"), m[i].JustAString...)
		err = kv.SetMsg(ctx, store, firstPartitionKey, secondaryKey, &kv.SecondaryIndex{PrimaryKey: primaryKey})
		if err != nil {
			t.Fatal("failed to set model", err)
		}
	}
	err := kv.SetMsg(ctx, store, secondPartitionKey, []byte("just_a_string/e"), &kv.SecondaryIndex{})
	if err != nil {
		t.Fatal("failed to set key", err)
	}
	err = kv.SetMsg(ctx, store, secondPartitionKey, []byte("just_a_string/f"), &kv.SecondaryIndex{PrimaryKey: []byte("name/no-name")})
	if err != nil {
		t.Fatal("failed to set model", err)
	}

	t.Run("listing all values of partition", func(t *testing.T) {
		itr, err := kv.NewSecondaryIterator(ctx, store, (&TestModel{}).ProtoReflect().Type(),
			firstPartitionKey, []byte("just_a_string/"), []byte(""))
		if err != nil {
			t.Fatalf("failed to create secondary iterator: %v", err)
		}
		defer itr.Close()
		var msgs []*TestModel
		for itr.Next() {
			e := itr.Entry()
			model, ok := e.Value.(*TestModel)
			if !ok {
				t.Fatalf("Failed to cast entry to TestModel")
			}
			expectedKey := append([]byte("just_a_string/"), model.JustAString...)
			if !bytes.Equal(e.Key, expectedKey) {
				t.Fatalf("Iterator key=%s, expected=%s", e.Key, expectedKey)
			}
			msgs = append(msgs, model)
		}
		if itr.Err() != nil {
			t.Fatalf("unexpected error: %v", itr.Err())
		}
		expectedMsgs := []*TestModel{
			{Name: []byte("e"), JustAString: "five"},
			{Name: []byte("d"), JustAString: "four"},
			{Name: []byte("a"), JustAString: "one"},
			{Name: []byte("c"), JustAString: "three"},
			{Name: []byte("b"), JustAString: "two"},
		}
		if diffs := deep.Equal(msgs, expectedMsgs); diffs != nil {
			t.Fatalf("Diff expected result: %v", diffs)
		}
	})

	t.Run("listing with prefix", func(t *testing.T) {
		itr, err := kv.NewSecondaryIterator(ctx, store, (&TestModel{}).ProtoReflect().Type(),
			firstPartitionKey, []byte("just_a_string/f"), []byte(""))
		if err != nil {
			t.Fatalf("failed to create secondary iterator: %v", err)
		}
		defer itr.Close()
		var msgs []*TestModel
		for itr.Next() {
			e := itr.Entry()
			model, ok := e.Value.(*TestModel)
			if !ok {
				t.Fatalf("Failed to cast entry to TestModel")
			}
			msgs = append(msgs, model)
		}
		if itr.Err() != nil {
			t.Fatalf("unexpected error: %v", itr.Err())
		}

		expectedMsgs := []*TestModel{
			{Name: []byte("e"), JustAString: "five"},
			{Name: []byte("d"), JustAString: "four"},
		}
		if diffs := deep.Equal(msgs, expectedMsgs); diffs != nil {
			t.Fatalf("Diff expected result: %v", diffs)
		}
	})

	t.Run("listing after", func(t *testing.T) {
		itr, err := kv.NewSecondaryIterator(ctx, store, (&TestModel{}).ProtoReflect().Type(),
			firstPartitionKey, []byte("just_a_string/"), []byte("just_a_string/four"))
		if err != nil {
			t.Fatalf("failed to create secondary iterator: %v", err)
		}
		defer itr.Close()
		var msgs []*TestModel
		for itr.Next() {
			e := itr.Entry()
			model, ok := e.Value.(*TestModel)
			if !ok {
				t.Fatalf("Failed to cast entry to TestModel")
			}
			msgs = append(msgs, model)
		}
		if itr.Err() != nil {
			t.Fatalf("unexpected error: %v", itr.Err())
		}

		expectedMsgs := []*TestModel{
			{Name: []byte("a"), JustAString: "one"},
			{Name: []byte("c"), JustAString: "three"},
			{Name: []byte("b"), JustAString: "two"},
		}
		if diffs := deep.Equal(msgs, expectedMsgs); diffs != nil {
			t.Fatalf("Diff expected result: %v", diffs)
		}
	})

	t.Run("key not found", func(t *testing.T) {
		itr, err := kv.NewSecondaryIterator(ctx, store, (&TestModel{}).ProtoReflect().Type(),
			secondPartitionKey, []byte("just_a_string/e"), []byte(""))
		if err != nil {
			t.Fatalf("failed to create secondary iterator: %v", err)
		}
		defer itr.Close()

		if itr.Next() {
			t.Fatalf("Next() should return false")
		}

		expectedErr := kv.ErrMissingKey
		if err := itr.Err(); !errors.Is(err, expectedErr) {
			t.Fatalf("expected error: %s, got %v", expectedErr, err)
		}
	})

	t.Run("value not found", func(t *testing.T) {
		itr, err := kv.NewSecondaryIterator(ctx, store, (&TestModel{}).ProtoReflect().Type(),
			secondPartitionKey, []byte("just_a_string/f"), []byte(""))
		if err != nil {
			t.Fatalf("failed to create secondary iterator: %v", err)
		}
		defer itr.Close()
		if itr.Next() {
			t.Fatalf("next should return false")
		}
		if err := itr.Err(); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})
}
