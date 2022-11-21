package adapter_test

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/block/local"
	"github.com/treeverse/lakefs/pkg/block/mem"
)

func TestAdapter(t *testing.T, name string, params interface{}) {
	a := MakeAdapterByName(t, name, params)
	t.Run("Adapter_Walk", func(t *testing.T) { testAdapterWalk(t, a) })
}

func testAdapterWalk(t *testing.T, a block.Adapter) {
	ctx := context.Background()
	itemsNum := 10
	namespace := getStorageNamespace(a)
	objects := setupSampleData(t, ctx, a, itemsNum)
	index := 0
	_ = a.Walk(ctx, block.WalkOpts{StorageNamespace: namespace}, func(id string) error {
		require.Equal(t, fmt.Sprintf("%s:%s", namespace, objects[index].Identifier), id)
		index += 1
		return nil
	})
}

func setupSampleData(t *testing.T, ctx context.Context, a block.Adapter, items int) []block.ObjectPointer {
	t.Helper()
	entries := make([]block.ObjectPointer, 0, items)
	for i := 0; i < items; i++ {
		contents := fmt.Sprintf("file_%06d", i)
		obj := block.ObjectPointer{
			StorageNamespace: getStorageNamespace(a),
			Identifier:       contents,
			IdentifierType:   block.IdentifierTypeRelative,
		}
		err := a.Put(ctx, obj, 0, strings.NewReader(contents), block.PutOpts{})
		if err != nil {
			t.Fatalf("failed to setup data with '%v': %s", obj, err)
		}
		entries = append(entries, obj)
	}
	return entries
}

func getStorageNamespace(a block.Adapter) string {
	switch a.BlockstoreType() {
	case block.BlockstoreTypeMem:
		return ""

	case block.BlockstoreTypeLocal:
		return "local://test"

	default:
		panic("Missing adapter type")
	}
}

func MakeAdapterByName(t *testing.T, name string, params interface{}) block.Adapter {
	var a block.Adapter
	switch name {
	case block.BlockstoreTypeMem:
		p := params.([]func(a *mem.Adapter))

		a = mem.New(p...)

	case block.BlockstoreTypeLocal:
		dir, err := os.MkdirTemp("", "testing-local-adapter-*")
		if err != nil {
			t.Fatalf("failed creating adapter: %s", err)
		}

		a, err = local.NewAdapter(dir)
		if err != nil {
			t.Fatalf("failed creating adapter: %s", err)
		}

	default:
		panic("Missing adapter type")
	}
	return a
}
