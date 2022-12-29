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

func testPartitionIterator(t *testing.T, ms MakeStore) {
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
				t.Fatalf("cannot read from store")
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

		if diffs := deep.Equal(names, []string{"b", "c", "d"}); diffs != nil {
			t.Fatalf("got wrong list of names: %v", diffs)
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

		if !errors.Is(itr.Err(), kv.ErrMissingPartitionKey) {
			t.Fatalf("expected error: %s, got %v", kv.ErrMissingPartitionKey, itr.Err())
		}
	})
}

func testPrimaryIterator(t *testing.T, ms MakeStore) {
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
				t.Fatalf("cannot read from store")
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
				t.Fatalf("cannot read from store")
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
}

func testSecondaryIterator(t *testing.T, ms MakeStore) {
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

	t.Run("listing all values of partition", func(t *testing.T) {
		itr, err := kv.NewSecondaryIterator(ctx, store, (&TestModel{}).ProtoReflect().Type(),
			firstPartitionKey, []byte(""), []byte(""))
		if err != nil {
			t.Fatalf("failed to create secondary iterator: %v", err)
		}
		defer itr.Close()
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

		if diffs := deep.Equal(names, []string{"a", "aa", "b", "c", "d"}); diffs != nil {
			t.Fatalf("got wrong list of names: %v", diffs)
		}
	})

	t.Run("listing with prefix", func(t *testing.T) {
		itr, err := kv.NewSecondaryIterator(ctx, store, (&TestModel{}).ProtoReflect().Type(),
			firstPartitionKey, []byte("a"), []byte(""))
		if err != nil {
			t.Fatalf("failed to create secondary iterator: %v", err)
		}
		defer itr.Close()
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

		if diffs := deep.Equal(names, []string{"a", "aa"}); diffs != nil {
			t.Fatalf("got wrong list of names: %v", diffs)
		}
	})

	t.Run("listing after", func(t *testing.T) {
		itr, err := kv.NewSecondaryIterator(ctx, store, (&TestModel{}).ProtoReflect().Type(),
			firstPartitionKey, []byte(""), []byte("b"))
		if err != nil {
			t.Fatalf("failed to create secondary iterator: %v", err)
		}
		defer itr.Close()
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

		if diffs := deep.Equal(names, []string{"c", "d"}); diffs != nil {
			t.Fatalf("got wrong list of names: %v", diffs)
		}
	})

	t.Run("key not found", func(t *testing.T) {
		itr, err := kv.NewSecondaryIterator(ctx, store, (&TestModel{}).ProtoReflect().Type(),
			thirdPartitionKey, []byte(""), []byte(""))
		if err != nil {
			t.Fatalf("failed to create secondary iterator: %v", err)
		}
		defer itr.Close()

		if itr.Next() {
			t.Fatalf("next should return false")
		}

		if !errors.Is(itr.Err(), kv.ErrMissingKey) {
			t.Fatalf("expected error: %s, got %v", kv.ErrMissingKey, itr.Err())
		}
	})

	t.Run("value as key not found", func(t *testing.T) {
		itr, err := kv.NewSecondaryIterator(ctx, store, (&TestModel{}).ProtoReflect().Type(),
			thirdPartitionKey, []byte(""), []byte("e"))
		if err != nil {
			t.Fatalf("failed to create secondary iterator: %v", err)
		}
		defer itr.Close()
		if itr.Next() {
			t.Fatalf("next should return false")
		}
		if itr.Err() != nil {
			t.Fatalf("unexpected error: %v", itr.Err())
		}
	})
}
