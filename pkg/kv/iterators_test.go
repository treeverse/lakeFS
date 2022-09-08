package kv_test

import (
	"context"
	"testing"

	"github.com/go-test/deep"
	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/kv"
	"github.com/treeverse/lakefs/pkg/kv/kvtest"
)

const (
	firstPartitionKey  = "m"
	secondPartitionKey = "ma"
	thirdPartitionKey  = "mb"
)

func TestIterators(t *testing.T) {
	ctx := context.Background()
	store := kvtest.GetStore(ctx, t)
	sm := kv.StoreMessage{
		Store: store,
	}
	m := []kvtest.TestModel{
		{Name: []byte("a")},
		{Name: []byte("aa")},
		{Name: []byte("b")},
		{Name: []byte("c")},
		{Name: []byte("d")},
	}

	// prepare data
	for i := 0; i < len(m); i++ {
		err := sm.SetMsg(ctx, firstPartitionKey, m[i].Name, &m[i])
		if err != nil {
			t.Fatal("failed to set model", err)
		}

		err = sm.SetMsg(ctx, secondPartitionKey, m[i].Name, &m[i])
		if err != nil {
			t.Fatal("failed to set model", err)
		}
	}

	err := sm.SetMsg(ctx, thirdPartitionKey, []byte("e"), &kvtest.TestModel{Name: []byte("notin")})
	if err != nil {
		t.Fatal("failed to set model", err)
	}

	t.Run("partition iterator listing all values of partition", func(t *testing.T) {
		itr := kv.NewPartitionIterator(ctx, store, (&kvtest.TestModel{}).ProtoReflect().Type(), firstPartitionKey)
		require.NotNil(t, itr)
		names := make([]string, 0)
		for itr.Next() {
			e := itr.Entry()
			model, ok := e.Value.(*kvtest.TestModel)
			if !ok {
				t.Fatalf("cannot read from store")
			}
			names = append(names, string(model.Name))
		}
		if itr.Err() != nil {
			t.Fatalf("unexpected error: %v", itr.Err())
		}
		itr.Close()

		if diffs := deep.Equal(names, []string{"a", "aa", "b", "c", "d"}); diffs != nil {
			t.Fatalf("got wrong list of names: %v", diffs)
		}
	})

	t.Run("partition iterator listing values SeekGE", func(t *testing.T) {
		itr := kv.NewPartitionIterator(ctx, store, (&kvtest.TestModel{}).ProtoReflect().Type(), secondPartitionKey)
		require.NotNil(t, itr)
		itr.SeekGE([]byte("b"))
		names := make([]string, 0)
		for itr.Next() {
			e := itr.Entry()
			model, ok := e.Value.(*kvtest.TestModel)
			if !ok {
				t.Fatalf("cannot read from store")
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

	t.Run("primary iterator listing all values of partition", func(t *testing.T) {
		itr, err := kv.NewPrimaryIterator(ctx, store, (&kvtest.TestModel{}).ProtoReflect().Type(),
			firstPartitionKey, []byte(""), kv.IteratorOptionsFrom([]byte("")))
		require.NoError(t, err)
		require.NotNil(t, itr)
		names := make([]string, 0)
		for itr.Next() {
			e := itr.Entry()
			model, ok := e.Value.(*kvtest.TestModel)
			if !ok {
				t.Fatalf("cannot read from store")
			}
			names = append(names, string(model.Name))
		}
		if itr.Err() != nil {
			t.Fatalf("unexpected error: %v", itr.Err())
		}
		itr.Close()

		if diffs := deep.Equal(names, []string{"a", "aa", "b", "c", "d"}); diffs != nil {
			t.Fatalf("got wrong list of names: %v", diffs)
		}
	})

	t.Run("primary iterator listing with prefix", func(t *testing.T) {
		itr, err := kv.NewPrimaryIterator(ctx, store, (&kvtest.TestModel{}).ProtoReflect().Type(),
			secondPartitionKey, []byte("a"), kv.IteratorOptionsFrom([]byte("")))
		require.NoError(t, err)
		require.NotNil(t, itr)
		names := make([]string, 0)
		for itr.Next() {
			e := itr.Entry()
			model, ok := e.Value.(*kvtest.TestModel)
			if !ok {
				t.Fatalf("cannot read from store")
			}
			names = append(names, string(model.Name))
		}
		if itr.Err() != nil {
			t.Fatalf("unexpected error: %v", itr.Err())
		}
		itr.Close()

		if diffs := deep.Equal(names, []string{"a", "aa"}); diffs != nil {
			t.Fatalf("got wrong list of names: %v", diffs)
		}
	})

	t.Run("primary iterator listing empty value", func(t *testing.T) {
		itr, err := kv.NewPrimaryIterator(ctx, store, (&kvtest.TestModel{}).ProtoReflect().Type(),
			secondPartitionKey, []byte(""), kv.IteratorOptionsAfter([]byte("d")))
		require.NoError(t, err)
		require.NotNil(t, itr)
		names := make([]string, 0)

		for itr.Next() {
			e := itr.Entry()
			model, ok := e.Value.(*kvtest.TestModel)
			if !ok {
				t.Fatalf("cannot read from store")
			}
			names = append(names, string(model.Name))
		}
		if itr.Err() != nil {
			t.Fatalf("unexpected error: %v", itr.Err())
		}
		itr.Close()

		if diffs := deep.Equal(names, []string{}); diffs != nil {
			t.Fatalf("got wrong list of names: %v", diffs)
		}
	})

	t.Run("primary iterator listing from", func(t *testing.T) {
		itr, err := kv.NewPrimaryIterator(ctx, store, (&kvtest.TestModel{}).ProtoReflect().Type(),
			firstPartitionKey, []byte(""), kv.IteratorOptionsFrom([]byte("b")))
		require.NoError(t, err)
		require.NotNil(t, itr)
		names := make([]string, 0)
		for itr.Next() {
			e := itr.Entry()
			model, ok := e.Value.(*kvtest.TestModel)
			if !ok {
				t.Fatalf("cannot read from store")
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

	t.Run("primary iterator listing after", func(t *testing.T) {
		itr, err := kv.NewPrimaryIterator(ctx, store, (&kvtest.TestModel{}).ProtoReflect().Type(),
			firstPartitionKey, []byte(""), kv.IteratorOptionsAfter([]byte("b")))
		require.NoError(t, err)
		require.NotNil(t, itr)
		names := make([]string, 0)
		for itr.Next() {
			e := itr.Entry()
			model, ok := e.Value.(*kvtest.TestModel)
			if !ok {
				t.Fatalf("cannot read from store")
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

	t.Run("secondary iterator listing all values of partition", func(t *testing.T) {
		itr, err := kv.NewSecondaryIterator(ctx, store, (&kvtest.TestModel{}).ProtoReflect().Type(),
			firstPartitionKey, []byte(""), []byte(""))
		require.NoError(t, err)
		require.NotNil(t, itr)
		names := make([]string, 0)
		for itr.Next() {
			e := itr.Entry()
			model, ok := e.Value.(*kvtest.TestModel)
			if !ok {
				t.Fatalf("cannot read from store")
			}
			names = append(names, string(model.Name))
		}
		if itr.Err() != nil {
			t.Fatalf("unexpected error: %v", itr.Err())
		}
		itr.Close()

		if diffs := deep.Equal(names, []string{"a", "aa", "b", "c", "d"}); diffs != nil {
			t.Fatalf("got wrong list of names: %v", diffs)
		}
	})

	t.Run("secondary iterator listing with prefix", func(t *testing.T) {
		itr, err := kv.NewSecondaryIterator(ctx, store, (&kvtest.TestModel{}).ProtoReflect().Type(),
			firstPartitionKey, []byte("a"), []byte(""))
		require.NoError(t, err)
		require.NotNil(t, itr)
		names := make([]string, 0)
		for itr.Next() {
			e := itr.Entry()
			model, ok := e.Value.(*kvtest.TestModel)
			if !ok {
				t.Fatalf("cannot read from store")
			}
			names = append(names, string(model.Name))
		}
		if itr.Err() != nil {
			t.Fatalf("unexpected error: %v", itr.Err())
		}
		itr.Close()

		if diffs := deep.Equal(names, []string{"a", "aa"}); diffs != nil {
			t.Fatalf("got wrong list of names: %v", diffs)
		}
	})

	t.Run("secondary iterator listing after", func(t *testing.T) {
		itr, err := kv.NewSecondaryIterator(ctx, store, (&kvtest.TestModel{}).ProtoReflect().Type(),
			firstPartitionKey, []byte(""), []byte("b"))
		require.NoError(t, err)
		require.NotNil(t, itr)
		names := make([]string, 0)
		for itr.Next() {
			e := itr.Entry()
			model, ok := e.Value.(*kvtest.TestModel)
			if !ok {
				t.Fatalf("cannot read from store")
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

	t.Run("secondary iterator primary not found", func(t *testing.T) {
		itr, err := kv.NewSecondaryIterator(ctx, store, (&kvtest.TestModel{}).ProtoReflect().Type(),
			thirdPartitionKey, []byte(""), []byte(""))
		require.NoError(t, err)
		require.NotNil(t, itr)
		names := make([]string, 0)
		for itr.Next() {
			e := itr.Entry()
			model, ok := e.Value.(*kvtest.TestModel)
			if !ok {
				t.Fatalf("cannot read from store")
			}
			names = append(names, string(model.Name))
		}
		if itr.Err() != nil {
			t.Fatalf("unexpected error: %v", itr.Err())
		}

		if diffs := deep.Equal(names, []string{}); diffs != nil {
			t.Fatalf("got wrong list of names: %v", diffs)
		}
	})
}
