package kvtest

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/hashicorp/go-multierror"
	nanoid "github.com/matoous/go-nanoid/v2"
	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/kv"
	_ "github.com/treeverse/lakefs/pkg/kv/mem"
	kvparams "github.com/treeverse/lakefs/pkg/kv/params"
)

type MakeStore func(t testing.TB, ctx context.Context) kv.Store

var runTestID = nanoid.MustGenerate("abcdef1234567890", 8) //nolint:gomnd

const (
	testPartitionKey       = "test"
	testUnusedPartitionKey = "unused"
)

func uniqueKey(k string) []byte {
	return []byte(runTestID + "-" + k)
}

func setupSampleData(t *testing.T, ctx context.Context, store kv.Store, partition, prefix string, items int) []kv.Entry {
	t.Helper()
	entries := make([]kv.Entry, 0, items)
	for i := 0; i < items; i++ {
		entry := sampleEntry(prefix, i)
		err := store.Set(ctx, []byte(partition), entry.Key, entry.Value)
		if err != nil {
			t.Fatalf("failed to setup data with '%s': %s", entry, err)
		}
		entries = append(entries, entry)
	}
	return entries
}

func sampleEntry(prefix string, n int) kv.Entry {
	k := fmt.Sprintf("%s-key-%04d", prefix, n)
	v := fmt.Sprintf("%s-value-%04d", prefix, n)
	return kv.Entry{Key: []byte(k), Value: []byte(v)}
}

func TestDriver(t *testing.T, name string, params kvparams.KV) {
	ms := MakeStoreByName(name, params)
	t.Run("Driver_Open", func(t *testing.T) { testDriverOpen(t, ms) })
	t.Run("Store_SetGet", func(t *testing.T) { testStoreSetGet(t, ms) })
	t.Run("Store_SetIf", func(t *testing.T) { testStoreSetIf(t, ms) })
	t.Run("Store_Delete", func(t *testing.T) { testStoreDelete(t, ms) })
	t.Run("Store_Scan", func(t *testing.T) { testStoreScan(t, ms) })
	t.Run("Store_MissingArgument", func(t *testing.T) { testStoreMissingArgument(t, ms) })
	t.Run("Store_ContextCancelled", func(t *testing.T) { testStoreContextCancelled(t, ms) })
	t.Run("ScanPrefix", func(t *testing.T) { testScanPrefix(t, ms) })
	t.Run("DeleteWhileIterating", func(t *testing.T) { testDeleteWhileIterPrefix(t, ms) })
	t.Run("DeleteWhileIteratingSamePrefix", func(t *testing.T) { testDeleteWhileIterSamePrefix(t, ms) })
	t.Run("PartitionIterator", func(t *testing.T) { testPartitionIterator(t, ms) })
	t.Run("PrimaryIterator", func(t *testing.T) { testPrimaryIterator(t, ms) })
	t.Run("SecondaryIterator", func(t *testing.T) { testSecondaryIterator(t, ms) })
}

func testDriverOpen(t *testing.T, ms MakeStore) {
	ctx := context.Background()
	store1 := ms(t, ctx)
	defer store1.Close()
	store2 := ms(t, ctx)
	defer store2.Close()
}

func testStoreSetGet(t *testing.T, ms MakeStore) {
	ctx := context.Background()
	store := ms(t, ctx)
	defer store.Close()

	testKey := uniqueKey("key")
	testValue1 := []byte("value")
	testValue2 := []byte("a different kind of value")

	t.Run("set_missing_partition_key", func(t *testing.T) {
		// partition key - nil (disallow)
		err := store.Set(ctx, nil, testKey, testValue1)
		if !errors.Is(err, kv.ErrMissingPartitionKey) {
			t.Fatalf("Set with nil partition key should fail - err: %v", err)
		}

		// partition key - empty slice (disallow)
		err = store.Set(ctx, []byte{}, testKey, testValue1)
		if !errors.Is(err, kv.ErrMissingPartitionKey) {
			t.Fatalf("Set with empty key should fail - err: %v", err)
		}
	})

	t.Run("set_missing_key", func(t *testing.T) {
		// key - nil (disallow)
		err := store.Set(ctx, []byte(testPartitionKey), nil, testValue1)
		if !errors.Is(err, kv.ErrMissingKey) {
			t.Fatalf("Set with nil key should fail - err: %v", err)
		}

		// key - empty slice (disallow)
		err = store.Set(ctx, []byte(testPartitionKey), []byte{}, testValue1)
		if !errors.Is(err, kv.ErrMissingKey) {
			t.Fatalf("Set with empty key should fail - err: %v", err)
		}
	})

	t.Run("set_missing_value", func(t *testing.T) {
		// value - nil (disallow)
		k := uniqueKey("key")
		err := store.Set(ctx, []byte(testPartitionKey), k, nil)
		if !errors.Is(err, kv.ErrMissingValue) {
			t.Fatalf("Set with nil value should fail - key=%s, err: %v", k, err)
		}

		// value - empty slice (allow)
		k = uniqueKey("key")
		err = store.Set(ctx, []byte(testPartitionKey), []byte{}, testValue1)
		if !errors.Is(err, kv.ErrMissingKey) {
			t.Fatalf("Set with empty value should fail - key=%s, err: %v", k, err)
		}
	})

	t.Run("set_value", func(t *testing.T) {
		// set test key with value1
		err := store.Set(ctx, []byte(testPartitionKey), testKey, testValue1)
		if err != nil {
			t.Fatalf("failed to set key '%s', to value '%s': %s", testKey, testValue1, err)
		}
	})

	t.Run("get_value", func(t *testing.T) {
		// get test key with value1
		res, err := store.Get(ctx, []byte(testPartitionKey), testKey)
		switch {
		case err != nil:
			t.Fatalf("failed to get key '%s': %s", testKey, err)
		case res == nil:
			t.Fatalf("got nil result")
		case res.Value == nil:
			t.Fatalf("got nil value")
		case !bytes.Equal(testValue1, res.Value):
			t.Fatalf("key='%s' value='%s' doesn't match, expected='%s'", testKey, res.Value, testValue1)
		}
	})

	t.Run("set_exist", func(t *testing.T) {
		// override key with value2
		err := store.Set(ctx, []byte(testPartitionKey), testKey, testValue2)
		if err != nil {
			t.Fatalf("failed to set key '%s', to value '%s': %s", testKey, testValue2, err)
		}
	})

	t.Run("get_new_value", func(t *testing.T) {
		// get test key with value2
		res, err := store.Get(ctx, []byte(testPartitionKey), testKey)
		if err != nil {
			t.Fatalf("failed to get key '%s': %s", testKey, err)
		}
		if !bytes.Equal(testValue2, res.Value) {
			t.Fatalf("key='%s' value='%s' doesn't match, expected='%s'", testKey, res.Value, testValue2)
		}
	})

	t.Run("get_missing", func(t *testing.T) {
		// get a missing key
		keyNotExists := uniqueKey("key-not-exists")
		val3, err := store.Get(ctx, []byte(testPartitionKey), keyNotExists)
		if !errors.Is(err, kv.ErrNotFound) {
			t.Fatalf("get key='%s' err=%s, expected not found", keyNotExists, err)
		}
		if val3 != nil {
			t.Fatalf("get key='%s' value='%s', expected nil", keyNotExists, val3)
		}
	})

	t.Run("get_missing_partition_key", func(t *testing.T) {
		// get a missing key
		partitionKeyNotExists := uniqueKey("partition-key-not-exists")
		val3, err := store.Get(ctx, partitionKeyNotExists, testKey)
		if !errors.Is(err, kv.ErrNotFound) {
			t.Fatalf("get key='%s' err=%s, expected not found", partitionKeyNotExists, err)
		}
		if val3 != nil {
			t.Fatalf("get key='%s' value='%s', expected nil", partitionKeyNotExists, val3)
		}
	})
}

func testStoreDelete(t *testing.T, ms MakeStore) {
	t.Parallel()
	ctx := context.Background()
	store := ms(t, ctx)
	defer store.Close()

	t.Run("exists", func(t *testing.T) {
		keyToDel := uniqueKey("key-to-delete")
		err := store.Set(ctx, []byte(testPartitionKey), keyToDel, []byte("value to delete"))
		if err != nil {
			t.Fatalf("failed to set key='%s': %s", keyToDel, err)
		}
		err = store.Delete(ctx, []byte(testPartitionKey), keyToDel)
		if err != nil {
			t.Fatalf("failed to delete key='%s': %s", keyToDel, err)
		}
	})

	t.Run("non_exists", func(t *testing.T) {
		keyToDel := uniqueKey("missing-key-to-delete")
		err := store.Delete(ctx, []byte(testPartitionKey), keyToDel)
		if err != nil {
			t.Fatalf("delete missing key '%s', err=%v expected nil", keyToDel, err)
		}
	})

	t.Run("non_exists_partition", func(t *testing.T) {
		keyToDel := uniqueKey("missing-key-to-delete")
		err := store.Delete(ctx, []byte("invalid_partition"), keyToDel)
		if err != nil {
			t.Fatalf("delete missing key '%s', err=%v expected nil", keyToDel, err)
		}
	})
}

func testStoreSetIf(t *testing.T, ms MakeStore) {
	ctx := context.Background()
	store := ms(t, ctx)
	defer store.Close()

	t.Run("set_first", func(t *testing.T) {
		key := uniqueKey("set-if-first")
		val := []byte("v")
		err := store.SetIf(ctx, []byte(testPartitionKey), key, val, nil)
		if err != nil {
			t.Fatalf("SetIf without previous key - key=%s value=%s pred=nil: %s", key, val, err)
		}
	})

	t.Run("set_exists", func(t *testing.T) {
		key := uniqueKey("set-if-exists")
		val1 := []byte("v1")
		err := store.Set(ctx, []byte(testPartitionKey), key, val1)
		if err != nil {
			t.Fatalf("Set while testing SetIf - key=%s value=%s: %s", key, val1, err)
		}

		res, err := store.Get(ctx, []byte(testPartitionKey), key)
		if err != nil {
			t.Fatalf("Get while testing SetIf - key=%s: %s", key, err)
		}

		val2 := []byte("v2")
		err = store.SetIf(ctx, []byte(testPartitionKey), key, val2, res.Predicate)
		if err != nil {
			t.Fatalf("SetIf with previous value - key=%s value=%s pred=%s: %s", key, val2, val1, err)
		}
	})

	t.Run("fail_predicate_value", func(t *testing.T) {
		key := uniqueKey("fail-predicate-value")
		val1 := []byte("v1")
		err := store.Set(ctx, []byte(testPartitionKey), key, val1)
		if err != nil {
			t.Fatalf("Set while testing fail predicate - key=%s value=%s: %s", key, val1, err)
		}

		val2 := []byte("v2")
		pred := []byte("pred")
		err = store.SetIf(ctx, []byte(testPartitionKey), key, val2, pred)
		if !errors.Is(err, kv.ErrPredicateFailed) {
			t.Fatalf("SetIf err=%v - key=%s, value=%s, pred=%s, expected err=%s", err, key, val2, val2, kv.ErrPredicateFailed)
		}
	})

	t.Run("fail_predicate_nil", func(t *testing.T) {
		key := uniqueKey("fail-predicate-nil")
		val1 := []byte("v1")
		err := store.Set(ctx, []byte(testPartitionKey), key, val1)
		if err != nil {
			t.Fatalf("Set while testing fail predicate - key=%s value=%s: %s", key, val1, err)
		}

		val2 := []byte("v2")
		err = store.SetIf(ctx, []byte(testPartitionKey), key, val2, nil)
		if !errors.Is(err, kv.ErrPredicateFailed) {
			t.Fatalf("SetIf err=%v - key=%s, value=%s, pred=nil, expected err=%s", err, key, val2, kv.ErrPredicateFailed)
		}
	})

	t.Run("update_if_exists", func(t *testing.T) {
		key := uniqueKey("set-if-exists")
		val1 := []byte("v1")
		err := store.Set(ctx, []byte(testPartitionKey), key, val1)
		if err != nil {
			t.Fatalf("Set while testing SetIf - key=%s value=%s: %s", key, val1, err)
		}

		val2 := []byte("v2")
		err = store.SetIf(ctx, []byte(testPartitionKey), key, val2, kv.PrecondConditionalExists)
		if err != nil {
			t.Fatalf("SetIf with previous value - key=%s value=%s pred=%s: %s", key, val2, val1, err)
		}

		res, err := store.Get(ctx, []byte(testPartitionKey), key)
		if err != nil {
			t.Fatalf("Get while testing SetIf - key=%s: %s", key, err)
		}
		require.Equal(t, res.Value, val2)
	})

	t.Run("update_if_exists_negative", func(t *testing.T) {
		key := uniqueKey("set-if-exists_negative")
		val2 := []byte("v2")
		err := store.SetIf(ctx, []byte(testPartitionKey), key, val2, kv.PrecondConditionalExists)
		if !errors.Is(err, kv.ErrPredicateFailed) {
			t.Fatalf("SetIf err=%v - key=%s, value=%s, pred=%v, expected err=%s", err, key, val2, kv.PrecondConditionalExists, kv.ErrPredicateFailed)
		}

		_, err = store.Get(ctx, []byte(testPartitionKey), key)
		require.ErrorIs(t, err, kv.ErrNotFound)
	})
}

func testStoreScan(t *testing.T, ms MakeStore) {
	ctx := context.Background()
	store := ms(t, ctx)
	defer store.Close()

	// setup sample data
	samplePrefix := uniqueKey("scan")
	const sampleItems = 100
	sampleData := setupSampleData(t, ctx, store, testPartitionKey, string(samplePrefix), sampleItems)
	_ = setupSampleData(t, ctx, store, testUnusedPartitionKey, string(samplePrefix), sampleItems)

	t.Run("full", func(t *testing.T) {
		scan, err := store.Scan(ctx, []byte(testPartitionKey), kv.ScanOptions{KeyStart: samplePrefix})
		if err != nil {
			t.Fatal("failed to scan", err)
		}
		defer scan.Close()

		var entries []kv.Entry
		for scan.Next() {
			ent := scan.Entry()
			switch {
			case ent == nil:
				t.Fatal("scan got nil entry")
			case ent.Key == nil:
				t.Fatal("Key is nil while scan item", len(entries))
			case ent.Value == nil:
				t.Fatal("Value is nil while scan item", len(entries))
			}
			if !bytes.HasPrefix(ent.Key, samplePrefix) {
				break
			}
			entries = append(entries, *ent)
		}
		if err := scan.Err(); err != nil {
			t.Fatal("scan ended with an error", err)
		}
		testCompareEntries(t, entries, sampleData)
	})

	t.Run("part", func(t *testing.T) {
		const fromIndex = 5
		fromKey := []byte(fmt.Sprint(string(samplePrefix), fmt.Sprintf("-key-%04d", fromIndex)))
		scan, err := store.Scan(ctx, []byte(testPartitionKey), kv.ScanOptions{KeyStart: fromKey})
		if err != nil {
			t.Fatal("failed to scan", err)
		}
		defer scan.Close()

		var entries []kv.Entry
		for scan.Next() {
			ent := scan.Entry()
			switch {
			case ent == nil:
				t.Fatal("scan got nil entry")
			case ent.Key == nil:
				t.Fatal("scan got entry with nil Key")
			case ent.Value == nil:
				t.Fatal("scan got entry with nil Value")
			}
			if !bytes.HasPrefix(ent.Key, samplePrefix) {
				break
			}
			entries = append(entries, *ent)
		}
		if err := scan.Err(); err != nil {
			t.Fatal("scan ended with an error", err)
		}
		testCompareEntries(t, entries, sampleData[fromIndex:])
	})
}

func MakeStoreByName(name string, kvParams kvparams.KV) MakeStore {
	return func(t testing.TB, ctx context.Context) kv.Store {
		t.Helper()
		kvParams.Type = name
		store, err := kv.Open(ctx, kvParams)
		if err != nil {
			t.Fatalf("failed to open kv '%s' store: %s", name, err)
		}
		t.Cleanup(store.Close)
		return store
	}
}

func testStoreMissingArgument(t *testing.T, ms MakeStore) {
	ctx := context.Background()
	store := ms(t, ctx)
	defer store.Close()

	t.Run("Get_key", func(t *testing.T) {
		for _, k := range [][]byte{nil, {}} {
			_, err := store.Get(ctx, []byte(testPartitionKey), k)
			if !errors.Is(err, kv.ErrMissingKey) {
				t.Errorf("Get using nil key - err=%v, expected %s", err, kv.ErrMissingKey)
			}
		}
	})

	t.Run("Get_partition_key", func(t *testing.T) {
		key := uniqueKey("test-missing-argument")
		for _, pk := range [][]byte{nil, {}} {
			_, err := store.Get(ctx, pk, key)
			if !errors.Is(err, kv.ErrMissingPartitionKey) {
				t.Errorf("Get using nil partition key - err=%v, expected %s", err, kv.ErrMissingPartitionKey)
			}
		}
	})

	t.Run("Set", func(t *testing.T) {
		key := uniqueKey("test-missing-argument")
		for _, pk := range [][]byte{nil, {}} {
			if err := store.Set(ctx, pk, key, []byte("v")); !errors.Is(err, kv.ErrMissingPartitionKey) {
				t.Errorf("Set using empty partition key - err=%v, expected %s", err, kv.ErrMissingPartitionKey)
			}
		}

		for _, k := range [][]byte{nil, {}} {
			if err := store.Set(ctx, []byte(testPartitionKey), k, []byte("v")); !errors.Is(err, kv.ErrMissingKey) {
				t.Errorf("Set using empty key - err=%v, expected %s", err, kv.ErrMissingKey)
			}
		}

		if err := store.Set(ctx, []byte(testPartitionKey), key, nil); !errors.Is(err, kv.ErrMissingValue) {
			t.Errorf("Set using nil value - err=%v, expected %s", err, kv.ErrMissingValue)
		}
	})

	t.Run("SetIf", func(t *testing.T) {
		key := uniqueKey("test-missing-argument")

		for _, pk := range [][]byte{nil, {}} {
			err := store.SetIf(ctx, pk, key, []byte("v"), []byte("p"))
			if !errors.Is(err, kv.ErrMissingPartitionKey) {
				t.Errorf("SetIf using empty partition key - err=%v, expected %s", err, kv.ErrMissingPartitionKey)
			}
		}

		for _, k := range [][]byte{nil, {}} {
			err := store.SetIf(ctx, []byte(testPartitionKey), k, []byte("v"), []byte("p"))
			if !errors.Is(err, kv.ErrMissingKey) {
				t.Errorf("SetIf using empty key - err=%v, expected %s", err, kv.ErrMissingKey)
			}
		}

		err := store.SetIf(ctx, []byte(testPartitionKey), key, nil, []byte("p"))
		if !errors.Is(err, kv.ErrMissingValue) {
			t.Errorf("SetIf using nil value - err=%v, expected %s", err, kv.ErrMissingValue)
		}
	})

	t.Run("Delete", func(t *testing.T) {
		key := uniqueKey("test-missing-argument")

		for _, pk := range [][]byte{nil, {}} {
			err := store.Delete(ctx, pk, key)
			if !errors.Is(err, kv.ErrMissingPartitionKey) {
				t.Errorf("Delete using empty partition key - err=%v, expected %s", err, kv.ErrMissingPartitionKey)
			}
		}

		for _, k := range [][]byte{nil, {}} {
			err := store.Delete(ctx, []byte(testPartitionKey), k)
			if !errors.Is(err, kv.ErrMissingKey) {
				t.Errorf("Delete using empty key - err=%v, expected %s", err, kv.ErrMissingKey)
			}
		}
	})

	t.Run("Scan", func(t *testing.T) {
		start := uniqueKey("test-missing-argument")
		for _, pk := range [][]byte{nil, {}} {
			_, err := store.Scan(ctx, pk, kv.ScanOptions{KeyStart: start})
			if !errors.Is(err, kv.ErrMissingPartitionKey) {
				t.Fatalf("Scan using empty partition key - err=%v, expected %s", err, kv.ErrMissingPartitionKey)
			}
		}
	})
}

func testStoreContextCancelled(t *testing.T, ms MakeStore) {
	ctx, cancel := context.WithCancel(context.Background())
	store := ms(t, ctx)
	defer store.Close()

	testKey := []byte("test-key-1")
	testValue1 := []byte("test-value-1")
	// cancel the context for all requests
	cancel()
	t.Run("Set", func(t *testing.T) {
		// set test key with value1
		err := store.Set(ctx, []byte(testPartitionKey), testKey, testValue1)
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context cancellation error, got: %s", err)
		}
	})

	t.Run("Get", func(t *testing.T) {
		res, err := store.Get(ctx, []byte(testPartitionKey), testKey)
		if err != nil {
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("expected context cancellation error, got: %s", err)
			}
		} else if res == nil {
			t.Fatal("cancelled context Get may succeed, but not with nil result")
		}
	})

	t.Run("SetIf", func(t *testing.T) {
		// creating a unique key for this test so that the predicate is nil
		uniqueSetIfKey := uniqueKey("set-if")
		err := store.SetIf(ctx, []byte(testPartitionKey), uniqueSetIfKey, testValue1, nil)
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context cancellation error, got: %s", err)
		}
	})

	t.Run("Delete", func(t *testing.T) {
		err := store.Delete(ctx, []byte(testPartitionKey), testKey)
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context cancellation error, got: %s", err)
		}
	})

	t.Run("Scan", func(t *testing.T) {
		res, err := store.Scan(ctx, []byte(testPartitionKey), kv.ScanOptions{KeyStart: []byte("")})
		if err != nil {
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("expected context cancellation error, got: %s", err)
			}
		} else if res == nil {
			t.Fatal("cancelled context Scan may succeed, but not with nil result")
		}
	})
}

func testScanPrefix(t *testing.T, ms MakeStore) {
	ctx := context.Background()
	store := ms(t, ctx)
	defer store.Close()

	// setup data - use the last set to scan
	var (
		sampleData   []kv.Entry
		samplePrefix []byte
	)
	const sampleItems = 100
	const numberOfSets = 3

	for i := 0; i < numberOfSets; i++ {
		samplePrefix = uniqueKey("scan-prefix")
		sampleData = setupSampleData(t, ctx, store, testPartitionKey, string(samplePrefix), sampleItems)

		// filing unused data under the same prefix but a different partition to check partition isolation
		_ = setupSampleData(t, ctx, store, testUnusedPartitionKey, string(samplePrefix), sampleItems)
	}

	scan, err := kv.ScanPrefix(ctx, store, []byte(testPartitionKey), samplePrefix, nil)
	if err != nil {
		t.Fatal("ScanPrefix failed", err)
	}
	defer scan.Close()

	var entries []kv.Entry
	for scan.Next() {
		ent := scan.Entry()
		switch {
		case ent == nil:
			t.Fatal("ScanPrefix got nil entry")
		case ent.Key == nil:
			t.Fatal("ScanPrefix Key is nil on entry", len(entries))
		case ent.Value == nil:
			t.Fatal("ScanPrefix Value is nil on entry", len(entries))
		}
		entries = append(entries, *ent)
	}
	if err := scan.Err(); err != nil {
		t.Fatal("ScanPrefix ended with an error", err)
	}
	testCompareEntries(t, entries, sampleData)
}

func testCompareEntries(t *testing.T, entries []kv.Entry, expected []kv.Entry) {
	t.Helper()
	if len(entries) != len(expected) {
		t.Fatalf("Entries length=%d, expected=%d", len(entries), len(expected))
	}
	for i := range entries {
		if !bytes.Equal(entries[i].Key, expected[i].Key) {
			t.Errorf("Entry[%d] key=%v, expected=%v", i, entries[i].Key, expected[i].Key)
		}
		if !bytes.Equal(entries[i].Value, expected[i].Value) {
			t.Errorf("Entry[%d] key=%v, expected=%v", i, entries[i].Key, expected[i].Key)
		}
	}
}

func testDeleteWhileIterPrefix(t *testing.T, ms MakeStore) {
	// iteration
	//            deletion
	testDeleteWhileIterPrefixSingleSequence(t, ms, "RRRRRRRDDDDDDD")
	// iteration
	//       deletion
	testDeleteWhileIterPrefixSingleSequence(t, ms, "RRRDRDRDRDRDDD")
	// iiiiiiiiiiteration
	//     deletion
	testDeleteWhileIterPrefixSingleSequence(t, ms, "RRRRRDRDRDRDRDRRRRR")
	//   iteration
	// deleeeeeeeetion
	testDeleteWhileIterPrefixSingleSequence(t, ms, "DDDDRDRDRDRDDRDDDDD")
	//     iteration
	// deletion
	testDeleteWhileIterPrefixSingleSequence(t, ms, "DDDDRDRDDRRDDDDD")
	//           iteration
	// deletion
	testDeleteWhileIterPrefixSingleSequence(t, ms, "DDDDDRRRRR")
}

// Executing a sequence of (R)ead and (D)elete operations, according to the specified sequence
func testDeleteWhileIterPrefixSingleSequence(t *testing.T, ms MakeStore, sequence string) {
	ctx := context.Background()
	store := ms(t, ctx)
	defer store.Close()

	readPref := uniqueKey("readPrefix")
	toDelPref := uniqueKey("toDelPrefix")
	naPref := uniqueKey("naPrefix")

	numRead := strings.Count(sequence, "R")
	numDel := strings.Count(sequence, "D")

	var initialStore []kv.Entry
	initialStore = append(initialStore, setupSampleData(t, ctx, store, testPartitionKey, string(readPref), numRead)...)
	initialStore = append(initialStore, setupSampleData(t, ctx, store, testPartitionKey, string(toDelPref), numDel)...)
	initialStore = append(initialStore, setupSampleData(t, ctx, store, testPartitionKey, string(naPref), numRead+numDel)...)

	chMap := make(map[byte]chan bool)
	chMap['D'] = make(chan bool)
	chMap['R'] = make(chan bool)
	chErr := make(chan error)

	// Will be filled by the scan routines, to later verify that all values are passed by scan
	readDone := make(map[string]bool)
	deleteDone := make(map[string]bool)

	const numRoutines = 2
	var wg sync.WaitGroup
	wg.Add(numRoutines)

	// Scan and read
	go func() {
		ei, err := store.Scan(ctx, []byte(testPartitionKey), kv.ScanOptions{KeyStart: readPref})
		if err != nil {
			chErr <- fmt.Errorf("unexpected error from store.Scan (read): %w", err)
		}
		defer ei.Close()
		for range chMap['R'] {
			if !ei.Next() {
				chErr <- fmt.Errorf("unexpected end of read iteration: %w", ei.Err())
				break
			}
			e := ei.Entry()
			if e == nil {
				chErr <- fmt.Errorf("unexpected nil entry read: %w", ei.Err())
				break
			}
			readDone[string(e.Key)] = true
			chErr <- nil
		}
		wg.Done()
	}()

	// Scan and delete
	go func() {
		ei, err := store.Scan(ctx, []byte(testPartitionKey), kv.ScanOptions{KeyStart: toDelPref})
		if err != nil {
			chErr <- fmt.Errorf("unexpected error from store.Scan (delete): %w", err)
		}
		defer ei.Close()
		for range chMap['D'] {
			if !ei.Next() {
				chErr <- fmt.Errorf("unexpected end of delete iteration: %w", ei.Err())
				break
			}
			e := ei.Entry()
			if e == nil {
				chErr <- fmt.Errorf("unexpected nil entry during deletion iteration: %w", ei.Err())
				break
			}
			err = store.Delete(ctx, []byte(testPartitionKey), e.Key)
			if err != nil {
				chErr <- fmt.Errorf("unexpected delete failure :%w", err)
				break
			}
			deleteDone[string(e.Key)] = true
			chErr <- nil
		}
		wg.Done()
	}()

	// iterating over the input sequence and triggering the read/delete routines accordingly
	for i := 0; i < len(sequence); i++ {
		chMap[sequence[i]] <- true
		err := <-chErr
		if err != nil {
			t.Fatal(err)
		}
	}

	close(chMap['R'])
	close(chMap['D'])
	wg.Wait()

	close(chErr)

	verifyDeleteWhileIterResults(t, ctx, store, initialStore, readDone, deleteDone, readPref, toDelPref)
}

func testDeleteWhileIterSamePrefix(t *testing.T, ms MakeStore) {
	// building the following kv keys sequence

	//       pref			prefin			prefinin
	//         |               |               |               |
	// prefix: |-----------------------------------------------|
	// internal-prefix:        |-------------------------------|
	// internal-internal-prefix:               |---------------|

	// Using the following keys (for some ints:L,M,N, and unique TESTID), that will provide access

	// TESTID-pref-key-0000
	// TESTID-pref-key-0001
	// ...
	// TESTID-pref-key-000<N>
	// TESTID-prefin-key-0000
	// TESTID-prefin-key-0001
	// ...
	// TESTID-prefin-key-000<M>
	// TESTID-prefinin-key-0000
	// TESTID-prefinin-key-0001
	// ...
	// TESTID-prefinin-key-000<L>

	extPref := uniqueKey("pref")
	inPref := uniqueKey("prefin")
	ininPref := uniqueKey("prefinin")

	allPrefs := [][]byte{extPref, inPref, ininPref}

	// delete a subsection of the read prefix
	testDeleteWhileIterSamePrefixSingleRun(t, ms, allPrefs, inPref, ininPref)

	// scan a subsection of the deleted prefix
	testDeleteWhileIterSamePrefixSingleRun(t, ms, allPrefs, ininPref, inPref)

	// scan and delete the same prefix
	testDeleteWhileIterSamePrefixSingleRun(t, ms, allPrefs, ininPref, ininPref)
}

func testDeleteWhileIterSamePrefixSingleRun(t *testing.T, ms MakeStore, prefsToCreate [][]byte, readPref, delPref []byte) {
	ctx := context.Background()
	store := ms(t, ctx)
	defer store.Close()

	// Emptying the KV store from previous entries. This is essential in these tests as
	// the verification relies on exact keys to exist
	cleanIter, err := store.Scan(ctx, []byte(testPartitionKey), kv.ScanOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer cleanIter.Close()

	var keysToClean [][]byte
	for cleanIter.Next() {
		e := cleanIter.Entry()
		if e != nil {
			keysToClean = append(keysToClean, e.Key)
		} else {
			t.Fatal("unexpected nil entry during pre test clean", cleanIter.Err())
		}
	}

	for _, k := range keysToClean {
		err = store.Delete(ctx, []byte(testPartitionKey), k)
		if err != nil {
			t.Fatal(err)
		}
	}

	// initialKv holds all the created items, to later verify that no items, that do not fit delPref, were deleted
	var initialKv []kv.Entry
	for _, pref := range prefsToCreate {
		initialKv = append(initialKv, setupSampleData(t, ctx, store, testPartitionKey, string(pref), 100)...) //nolint:gomnd
	}

	// Will be filled by the scan&read routine, to later verify that all values are passed by scan
	readDone := make(map[string]bool)
	deleteDone := make(map[string]bool)

	var g multierror.Group

	// Scan and read
	g.Go(func() error {
		ei, err := store.Scan(ctx, []byte(testPartitionKey), kv.ScanOptions{KeyStart: readPref})
		if err != nil {
			return fmt.Errorf("unexpected error from store.Scan (read): %w", err)
		}
		defer ei.Close()

		for ei.Next() {
			e := ei.Entry()
			if e != nil {
				readDone[string(e.Key)] = true
			} else if err := ei.Err(); err != nil {
				return fmt.Errorf("unexpected error during read iteration: %w", err)
			}
		}
		return nil
	})

	// Scan and delete
	g.Go(func() error {
		ei, err := store.Scan(ctx, []byte(testPartitionKey), kv.ScanOptions{KeyStart: delPref})
		if err != nil {
			return fmt.Errorf("unexpected error from store.Scan (delete): %w", err)
		}
		defer ei.Close()

		for ei.Next() {
			e := ei.Entry()
			if e == nil {
				return fmt.Errorf("unexpected nil entry during deletion interation: %w", ei.Err())
			}
			err = store.Delete(ctx, []byte(testPartitionKey), e.Key)
			if err != nil {
				return fmt.Errorf("unexpected delete failure :%w", err)
			}
			deleteDone[string(e.Key)] = true
		}
		return nil
	})

	if err := g.Wait(); err != nil {
		t.Fatal("Wait for scan read and delete", err)
	}

	verifyDeleteWhileIterResults(t, ctx, store, initialKv, readDone, deleteDone, readPref, delPref)
}

func verifyDeleteWhileIterResults(t *testing.T, ctx context.Context, store kv.Store, initialStore []kv.Entry, readDone, deleteDone map[string]bool, readPref, delPref []byte) {
	// verify all readDone entries fits readPref
	for k := range readDone {
		if strings.Index(k, string(readPref)) != 0 {
			t.Fatal("unexpected key read for prefix", k, string(readPref))
		}
	}

	// verify all delete entries fits delPref
	for k := range deleteDone {
		if strings.Index(k, string(delPref)) != 0 {
			t.Fatal("unexpected key deleted for prefix", k, string(delPref))
		}
	}

	// For each entry originally created, verify that:
	// 1. If it is under delPref, it was deleted
	// 2. Otherwise, if it is under readPref, it was accessed
	// 3. Otherwise, it is untouched
	for _, kve := range initialStore {
		if strings.Index(string(kve.Key), string(delPref)) == 0 {
			if !deleteDone[string(kve.Key)] {
				t.Fatal("entry missing from deleted list", string(kve.Key))
			}
			_, err := store.Get(ctx, []byte(testPartitionKey), kve.Key)
			if !errors.Is(err, kv.ErrNotFound) {
				t.Fatal("unexpected entry found after delete", string(kve.Key))
			}
			if !errors.Is(err, kv.ErrNotFound) {
				t.Fatal("unexpected error ")
			}
			continue
		}
		if strings.Index(string(kve.Key), string(readPref)) == 0 {
			if !readDone[string(kve.Key)] {
				t.Fatal("entry missing from read list", string(kve.Key))
			}
		} else if deleteDone[string(kve.Key)] || readDone[string(kve.Key)] {
			t.Fatal("unexpected entry access (read or delete)", string(kve.Key))
		}

		res, err := store.Get(ctx, []byte(testPartitionKey), kve.Key)
		if errors.Is(err, kv.ErrNotFound) {
			t.Fatal("expected entry not found", string(kve.Key))
		}
		if err != nil {
			t.Fatal("unexpected error getting entry", string(kve.Key), err)
		}
		if !bytes.Equal(res.Value, kve.Value) {
			t.Fatal("Unexpected value found for key", string(kve.Key))
		}
	}

	// verify all entries that fit delPref are indeed deleted, i.e. no such entry is left
	// in the store
	verifyIter, err := store.Scan(ctx, []byte(testPartitionKey), kv.ScanOptions{KeyStart: []byte{}})
	if err != nil {
		t.Fatal(err)
	}
	defer verifyIter.Close()

	for verifyIter.Next() {
		e := verifyIter.Entry()
		if e == nil {
			t.Fatal("unexpected nil entry in KV store")
		}
		if strings.Index(string(e.Key), string(delPref)) == 0 {
			t.Fatal("unexpected entry found after delete", string(e.Key), string(delPref))
		}
	}
}

// GetStore helper function to return Store object for all unit tests
func GetStore(ctx context.Context, t testing.TB) kv.Store {
	t.Helper()
	const storeType = "mem"
	store, err := kv.Open(ctx, kvparams.KV{Type: storeType})
	if err != nil {
		t.Fatalf("failed to open kv (%s) store: %s", storeType, err)
	}
	t.Cleanup(func() {
		store.Close()
	})
	return store
}
