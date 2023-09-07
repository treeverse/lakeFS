package kvtest

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/go-test/deep"
	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/kv"
)

const (
	firstPartitionKey  = "m"
	secondPartitionKey = "ma"
)

type StoreWithCounter struct {
	kv.Store
	ScanCalls int64
}

func NewStoreWithCounter(store kv.Store) *StoreWithCounter {
	return &StoreWithCounter{Store: store}
}

func (swc *StoreWithCounter) Scan(ctx context.Context, partitionKey []byte, options kv.ScanOptions) (kv.EntriesIterator, error) {
	atomic.AddInt64(&swc.ScanCalls, 1)
	return swc.Store.Scan(ctx, partitionKey, options)
}

func testPartitionIterator(t *testing.T, ms MakeStore) {
	ctx := context.Background()
	store := ms(t, ctx)

	// prepare data
	modelNames := []string{"a", "aa", "b", "c", "d",
		"d1", "d2", "d3", "d4", "d5", "d6", "d7", "d8", "d9",
		"d10", "d11", "d12", "d13", "d14", "d15", "d16", "d17", "d18", "d19",
		"d20", "d21", "d22", "d23", "d24", "d25", "d26", "d27", "d28", "d29",
		"d30", "d31", "d32", "d33", "d34", "d35", "d36", "d37", "d38", "d39",
		"d40", "d41", "d42", "d43", "d44", "d45", "d46", "d47", "d48", "d49",
		"d50", "d51", "d52", "d53", "d54", "d55", "d56", "d57", "d58", "d59",
		"d60", "d61", "d62", "d63", "d64", "d65", "d66", "d67", "d68", "d69",
		"d70", "d71", "d72", "d73", "d74", "d75", "d76", "d77", "d78", "d79",
		"d80", "d81", "d82", "d83", "d84", "d85", "d86", "d87", "d88", "d89",
		"d90", "d91", "d92", "d93", "d94", "d95", "d96", "d97", "d98", "d99",
		"d100", "d101", "d102", "d103", "d104", "d105", "d106", "d107", "d108", "d109",
		"d110", "d111", "d112", "d113", "d114", "d115", "d116", "d117", "d118", "d119",
		"d120", "d121", "d122", "d123", "d124", "d125", "d126", "d127", "d128", "d129",
		"z"}
	for _, name := range modelNames {
		model := TestModel{Name: []byte(name)}
		for _, partitionKey := range []string{firstPartitionKey, secondPartitionKey} {
			err := kv.SetMsg(ctx, store, partitionKey, model.Name, &model)
			if err != nil {
				t.Fatalf("failed to set model (partition %s, name %s): %s", partitionKey, name, err)
			}
		}
	}

	t.Run("listing all values of partition", func(t *testing.T) {
		itr := kv.NewPartitionIterator(ctx, store, (&TestModel{}).ProtoReflect().Type(), firstPartitionKey, 0)
		if itr == nil {
			t.Fatalf("failed to create partition iterator")
		}
		defer itr.Close()
		names := scanPartitionIterator(t, itr, func(_ []byte, model *TestModel) string { return string(model.Name) })
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
		for _, seekValue := range []string{"b", "aaa", "b", "z"} {
			itr.SeekGE([]byte(seekValue))
			names := scanPartitionIterator(t, itr, func(_ []byte, model *TestModel) string { return string(model.Name) })
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
			if err := itr.Err(); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			k := itr.Entry().Key
			if string(k) != seekValue {
				t.Fatalf("Expected to find value %s. Found %s", seekValue, k)
			}
		}
		scanCalls := atomic.LoadInt64(&store.ScanCalls)
		if scanCalls != 1 {
			t.Fatalf("Expected exactly 1 call to Scan. got: %d", scanCalls)
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

		if err := itr.Err(); !errors.Is(err, kv.ErrMissingPartitionKey) {
			t.Fatalf("expected error: %s, got %v", kv.ErrMissingPartitionKey, err)
		}
	})

	t.Run("SeekGE past end", func(t *testing.T) {
		itr := kv.NewPartitionIterator(ctx, store, (&TestModel{}).ProtoReflect().Type(), firstPartitionKey, 0)
		if itr == nil {
			t.Fatalf("failed to create partition iterator")
		}
		defer itr.Close()
		itr.SeekGE([]byte("b"))
		if !itr.Next() {
			t.Fatal("expected Next to be true")
		}
		e := itr.Entry()
		model := e.Value.(*TestModel)
		if string(model.Name) != "b" {
			t.Fatalf("expected value b from iterator")
		}
		itr.SeekGE(graveler.UpperBoundForPrefix([]byte("d1")))
		if itr.Next() {
			t.Fatalf("expected Next to be false")
		}
		if err := itr.Err(); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("SeekGE seek back", func(t *testing.T) {
		itr := kv.NewPartitionIterator(ctx, store, (&TestModel{}).ProtoReflect().Type(), firstPartitionKey, 0)
		if itr == nil {
			t.Fatalf("failed to create partition iterator")
		}
		defer itr.Close()
		itr.SeekGE([]byte("z"))
		if itr.Next() {
			t.Fatal("expected Next to be false")
		}
		if err := itr.Err(); err != nil {
			t.Fatalf("unexpected error: %s", err)
		}
		itr.SeekGE([]byte("a"))
		if !itr.Next() {
			t.Fatalf("expected Next to be true")
		}
		if err := itr.Err(); err != nil {
			t.Fatalf("unexpected error: %s", err)
		}
		e := itr.Entry()
		model := e.Value.(*TestModel)
		if string(model.Name) != "a" {
			t.Fatalf("expected value a from iterator")
		}
	})
}

// scanPartitionIterator scans the iterator and returns a slice of the results of applying fn to each model.
// slice element type is based on the callback 'fn' function return type.
func scanPartitionIterator[E any](t *testing.T, itr kv.MessageIterator, fn func(key []byte, model *TestModel) E) []E {
	t.Helper()
	result := make([]E, 0)
	for itr.Next() {
		e := itr.Entry()
		model := e.Value.(*TestModel)
		result = append(result, fn(e.Key, model))
	}
	if err := itr.Err(); err != nil {
		t.Fatalf("While scan partition iterator unexpected error: %v", err)
	}
	return result
}

func testPrimaryIterator(t *testing.T, ms MakeStore) {
	ctx := context.Background()
	store := ms(t, ctx)

	// prepare data
	modelNames := []string{"a", "aa", "b", "c", "d"}
	for _, name := range modelNames {
		model := TestModel{Name: []byte(name)}
		for _, partitionKey := range []string{firstPartitionKey, secondPartitionKey} {
			err := kv.SetMsg(ctx, store, partitionKey, model.Name, &model)
			if err != nil {
				t.Fatalf("failed to set model (partition %s, name %s): %s", partitionKey, name, err)
			}
		}
	}

	t.Run("listing all values of partition", func(t *testing.T) {
		itr, err := kv.NewPrimaryIterator(ctx, store, (&TestModel{}).ProtoReflect().Type(),
			firstPartitionKey, []byte(""), kv.IteratorOptionsFrom([]byte("")))
		if err != nil {
			t.Fatalf("failed to create primary iterator: %v", err)
		}
		defer itr.Close()
		names := scanPartitionIterator(t, itr, func(_ []byte, model *TestModel) string { return string(model.Name) })
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
		names := scanPartitionIterator(t, itr, func(_ []byte, model *TestModel) string { return string(model.Name) })
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
		if err := itr.Err(); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("listing from", func(t *testing.T) {
		itr, err := kv.NewPrimaryIterator(ctx, store, (&TestModel{}).ProtoReflect().Type(),
			firstPartitionKey, []byte(""), kv.IteratorOptionsFrom([]byte("b")))
		if err != nil {
			t.Fatalf("failed to create primary iterator: %v", err)
		}
		defer itr.Close()
		names := scanPartitionIterator(t, itr, func(_ []byte, model *TestModel) string { return string(model.Name) })
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
		names := scanPartitionIterator(t, itr, func(_ []byte, model *TestModel) string { return string(model.Name) })
		if diffs := deep.Equal(names, []string{"c", "d"}); diffs != nil {
			t.Fatalf("got wrong list of names: %v", diffs)
		}
	})
}

func testSecondaryIterator(t *testing.T, ms MakeStore) {
	ctx := context.Background()
	store := ms(t, ctx)

	// prepare data
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
			t.Fatal("failed to set model (primary key)", err)
		}
		secondaryKey := append([]byte("just_a_string/"), m[i].JustAString...)
		err = kv.SetMsg(ctx, store, firstPartitionKey, secondaryKey, &kv.SecondaryIndex{PrimaryKey: primaryKey})
		if err != nil {
			t.Fatal("failed to set model (secondary key)", err)
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

		type KeyNameValue struct {
			Key   []byte
			Name  []byte
			Value string
		}
		msgs := scanPartitionIterator(t, itr, func(key []byte, model *TestModel) KeyNameValue {
			return KeyNameValue{
				Key:   key,
				Name:  model.Name,
				Value: model.JustAString,
			}
		})
		expectedMsgs := []KeyNameValue{
			{Key: []byte("just_a_string/five"), Name: []byte("e"), Value: "five"},
			{Key: []byte("just_a_string/four"), Name: []byte("d"), Value: "four"},
			{Key: []byte("just_a_string/one"), Name: []byte("a"), Value: "one"},
			{Key: []byte("just_a_string/three"), Name: []byte("c"), Value: "three"},
			{Key: []byte("just_a_string/two"), Name: []byte("b"), Value: "two"},
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
			model := e.Value.(*TestModel)
			msgs = append(msgs, model)
		}
		if err := itr.Err(); err != nil {
			t.Fatalf("unexpected error: %v", err)
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
			model := e.Value.(*TestModel)
			msgs = append(msgs, model)
		}
		if err := itr.Err(); err != nil {
			t.Fatalf("unexpected error: %v", err)
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
