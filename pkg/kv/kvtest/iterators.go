package kvtest

import (
	"context"
	"errors"
	"testing"

	"github.com/go-test/deep"
	"github.com/treeverse/lakefs/pkg/kv"
)

const (
	firstPartitionKey  = "m"
	secondPartitionKey = "ma"
	thirdPartitionKey  = "mb"
)

func testIterators(t *testing.T, ms MakeStore) {
	ctx := context.Background()
	store := ms(t, ctx)
	sm := kv.StoreMessage{
		Store: store,
	}
	m := []TestModel{
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
	err := sm.SetMsg(ctx, thirdPartitionKey, []byte("e"), &TestModel{})
	if err != nil {
		t.Fatal("failed to set model", err)
	}

	err = sm.SetMsg(ctx, thirdPartitionKey, []byte("f"), &TestModel{Name: []byte("notin")})
	if err != nil {
		t.Fatal("failed to set model", err)
	}
	// first partition - (a,a) (aa,aa) (b,b) (c,c) (d,d)
	// second partition - (a,a) (aa,aa) (b,b) (c,c) (d,d)
	// third partition - (e,nil) (f,"notin")

	testPartitionIterator(t, ctx, ms)
	testPrimaryIterator(t, ctx, ms)
	testSecondaryIterator(t, ctx, ms)
}

func testPartitionIterator(t *testing.T, ctx context.Context, ms MakeStore) {
	store := ms(t, ctx)
	t.Run("partition iterator listing all values of partition", func(t *testing.T) {
		itr := kv.NewPartitionIterator(ctx, store, (&TestModel{}).ProtoReflect().Type(), firstPartitionKey)
		if itr == nil {
			t.Fatalf("failed to create partition iterator")
		}
		names := make([]string, 0)
		for itr.Next() {
			e := itr.Entry()
			model, ok := e.Value.(*TestModel)
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
		itr := kv.NewPartitionIterator(ctx, store, (&TestModel{}).ProtoReflect().Type(), secondPartitionKey)
		if itr == nil {
			t.Fatalf("failed to create partition iterator")
		}
		itr.SeekGE([]byte("b"))
		names := make([]string, 0)
		for itr.Next() {
			e := itr.Entry()
			model, ok := e.Value.(*TestModel)
			if !ok {
				t.Fatalf("cannot read from store")
			}
			names = append(names, string(model.Name))
		}
		if itr.Err() != nil {
			t.Fatalf("unexpected error: %v", itr.Err())
		}
		itr.Close()

		if diffs := deep.Equal(names, []string{"b", "c", "d"}); diffs != nil {
			t.Fatalf("got wrong list of names: %v", diffs)
		}
	})

	t.Run("partition iterator failed SeekGE", func(t *testing.T) {
		itr := kv.NewPartitionIterator(ctx, store, (&TestModel{}).ProtoReflect().Type(), "")
		if itr == nil {
			t.Fatalf("failed to create partition iterator")
		}
		itr.SeekGE([]byte("d"))
		for itr.Next() {
			e := itr.Entry()
			_, ok := e.Value.(*TestModel)
			if !ok {
				t.Fatalf("cannot read from store")
			}
		}
		itr.Close()

		if !errors.Is(itr.Err(), kv.ErrMissingPartitionKey) {
			t.Fatalf("expected error: got %v", itr.Err())
		}
	})
}

func testPrimaryIterator(t *testing.T, ctx context.Context, ms MakeStore) {
	store := ms(t, ctx)
	t.Run("primary iterator listing all values of partition", func(t *testing.T) {
		itr, err := kv.NewPrimaryIterator(ctx, store, (&TestModel{}).ProtoReflect().Type(),
			firstPartitionKey, []byte(""), kv.IteratorOptionsFrom([]byte("")))
		if err != nil {
			t.Fatalf("failed to create primary iterator: %v", err)
		}
		names := make([]string, 0)
		for itr.Next() {
			e := itr.Entry()
			model, ok := e.Value.(*TestModel)
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
		itr, err := kv.NewPrimaryIterator(ctx, store, (&TestModel{}).ProtoReflect().Type(),
			secondPartitionKey, []byte("a"), kv.IteratorOptionsFrom([]byte("")))
		if err != nil {
			t.Fatalf("failed to create primary iterator: %v", err)
		}
		names := make([]string, 0)
		for itr.Next() {
			e := itr.Entry()
			model, ok := e.Value.(*TestModel)
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
		itr, err := kv.NewPrimaryIterator(ctx, store, (&TestModel{}).ProtoReflect().Type(),
			secondPartitionKey, []byte(""), kv.IteratorOptionsAfter([]byte("d")))
		if err != nil {
			t.Fatalf("failed to create primary iterator: %v", err)
		}
		names := make([]string, 0)

		for itr.Next() {
			e := itr.Entry()
			model, ok := e.Value.(*TestModel)
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
		itr, err := kv.NewPrimaryIterator(ctx, store, (&TestModel{}).ProtoReflect().Type(),
			firstPartitionKey, []byte(""), kv.IteratorOptionsFrom([]byte("b")))
		if err != nil {
			t.Fatalf("failed to create primary iterator: %v", err)
		}
		names := make([]string, 0)
		for itr.Next() {
			e := itr.Entry()
			model, ok := e.Value.(*TestModel)
			if !ok {
				t.Fatalf("cannot read from store")
			}
			names = append(names, string(model.Name))
		}
		if itr.Err() != nil {
			t.Fatalf("unexpected error: %v", itr.Err())
		}
		itr.Close()

		if diffs := deep.Equal(names, []string{"b", "c", "d"}); diffs != nil {
			t.Fatalf("got wrong list of names: %v", diffs)
		}
	})

	t.Run("primary iterator listing after", func(t *testing.T) {
		itr, err := kv.NewPrimaryIterator(ctx, store, (&TestModel{}).ProtoReflect().Type(),
			firstPartitionKey, []byte(""), kv.IteratorOptionsAfter([]byte("b")))
		if err != nil {
			t.Fatalf("failed to create primary iterator: %v", err)
		}
		names := make([]string, 0)
		for itr.Next() {
			e := itr.Entry()
			model, ok := e.Value.(*TestModel)
			if !ok {
				t.Fatalf("cannot read from store")
			}
			names = append(names, string(model.Name))
		}
		if itr.Err() != nil {
			t.Fatalf("unexpected error: %v", itr.Err())
		}
		itr.Close()

		if diffs := deep.Equal(names, []string{"c", "d"}); diffs != nil {
			t.Fatalf("got wrong list of names: %v", diffs)
		}
	})
}

func testSecondaryIterator(t *testing.T, ctx context.Context, ms MakeStore) {
	store := ms(t, ctx)
	t.Run("secondary iterator listing all values of partition", func(t *testing.T) {
		itr, err := kv.NewSecondaryIterator(ctx, store, (&TestModel{}).ProtoReflect().Type(),
			firstPartitionKey, []byte(""), []byte(""))
		if err != nil {
			t.Fatalf("failed to create secondary iterator: %v", err)
		}
		names := make([]string, 0)
		for itr.Next() {
			e := itr.Entry()
			model, ok := e.Value.(*TestModel)
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
		itr, err := kv.NewSecondaryIterator(ctx, store, (&TestModel{}).ProtoReflect().Type(),
			firstPartitionKey, []byte("a"), []byte(""))
		if err != nil {
			t.Fatalf("failed to create secondary iterator: %v", err)
		}
		names := make([]string, 0)
		for itr.Next() {
			e := itr.Entry()
			model, ok := e.Value.(*TestModel)
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
		itr, err := kv.NewSecondaryIterator(ctx, store, (&TestModel{}).ProtoReflect().Type(),
			firstPartitionKey, []byte(""), []byte("b"))
		if err != nil {
			t.Fatalf("failed to create secondary iterator: %v", err)
		}
		names := make([]string, 0)
		for itr.Next() {
			e := itr.Entry()
			model, ok := e.Value.(*TestModel)
			if !ok {
				t.Fatalf("cannot read from store")
			}
			names = append(names, string(model.Name))
		}
		if itr.Err() != nil {
			t.Fatalf("unexpected error: %v", itr.Err())
		}
		itr.Close()

		if diffs := deep.Equal(names, []string{"c", "d"}); diffs != nil {
			t.Fatalf("got wrong list of names: %v", diffs)
		}
	})

	t.Run("secondary iterator entry not found", func(t *testing.T) {
		itr, err := kv.NewSecondaryIterator(ctx, store, (&TestModel{}).ProtoReflect().Type(),
			thirdPartitionKey, []byte(""), []byte(""))
		if err != nil {
			t.Fatalf("failed to create secondary iterator: %v", err)
		}

		for itr.Next() {
			e := itr.Entry()
			_, ok := e.Value.(*TestModel)
			if !ok {
				t.Fatalf("cannot read from store")
			}
		}
		itr.Close()

		if !errors.Is(itr.Err(), kv.ErrMissingKey) {
			t.Fatalf("expected error: got %v", itr.Err())
		}
	})

	t.Run("secondary iterator primary not found", func(t *testing.T) {
		itr, err := kv.NewSecondaryIterator(ctx, store, (&TestModel{}).ProtoReflect().Type(),
			thirdPartitionKey, []byte(""), []byte("e"))
		if err != nil {
			t.Fatalf("failed to create secondary iterator: %v", err)
		}
		names := make([]string, 0)
		for itr.Next() {
			e := itr.Entry()
			model, ok := e.Value.(*TestModel)
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
}
