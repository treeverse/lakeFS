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
		itr.SeekGE([]byte("b"))
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
	sm := kv.StoreMessage{
		Store: store,
	}
	m := []TestModel{
		{Name: []byte("a"), JustAString: "one"},
		{Name: []byte("b"), JustAString: "two"},
		{Name: []byte("c"), JustAString: "three"},
		{Name: []byte("d"), JustAString: "four"},
		{Name: []byte("e"), JustAString: "five"},
	}

	for i := 0; i < len(m); i++ {
		primaryKey := append([]byte("name/"), m[i].Name...)
		err := sm.SetMsg(ctx, firstPartitionKey, primaryKey, &m[i])
		if err != nil {
			t.Fatal("failed to set model", err)
		}
		secondaryKey := append([]byte("just_a_string/"), m[i].JustAString...)
		err = sm.SetMsg(ctx, firstPartitionKey, secondaryKey, &kv.SecondaryIndex{PrimaryKey: primaryKey})
		if err != nil {
			t.Fatal("failed to set model", err)
		}
	}
	err := sm.SetMsg(ctx, secondPartitionKey, []byte("just_a_string/e"), &kv.SecondaryIndex{})
	if err != nil {
		t.Fatal("failed to set key", err)
	}
	err = sm.SetMsg(ctx, secondPartitionKey, []byte("just_a_string/f"), &kv.SecondaryIndex{PrimaryKey: []byte("name/no-name")})
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
