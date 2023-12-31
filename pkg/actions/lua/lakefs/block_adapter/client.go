package block_adapter

import (
	"context"
	"github.com/Shopify/go-lua"
	"github.com/treeverse/lakefs/pkg/block"
	"io"
	"strings"
)

type BlockAdapter struct {
	adapter          block.Adapter
	storageNamespace string
}

func (ba *BlockAdapter) GetObject(l *lua.State) int {
	objectKey := lua.CheckString(l, 1)
	expectedSize := lua.OptInteger(l, 2, 0)
	obj, err := ba.adapter.Get(context.Background(), block.ObjectPointer{
		StorageNamespace: ba.storageNamespace,
		Identifier:       objectKey,
		IdentifierType:   block.IdentifierTypeRelative,
	}, int64(expectedSize))
	if err != nil {
		lua.Errorf(l, "%s", err.Error())
		panic("unreachable")
	}
	data, err := io.ReadAll(obj)
	defer obj.Close()
	if err != nil {
		lua.Errorf(l, "%s", err.Error())
		panic("unreachable")
	}
	l.PushString(string(data))
	l.PushBoolean(true) // exists
	return 2
}

func (ba *BlockAdapter) PutObject(l *lua.State) int {
	objectKey := lua.CheckString(l, 1)
	obj := strings.NewReader(lua.CheckString(l, 2))

	err := ba.adapter.Put(context.Background(), block.ObjectPointer{
		StorageNamespace: ba.storageNamespace,
		Identifier:       objectKey,
		IdentifierType:   block.IdentifierTypeRelative,
	}, int64(obj.Len()), obj, block.PutOpts{})
	if err != nil {
		lua.Errorf(l, "%s", err.Error())
		panic("unreachable")
	}
	return 0
}

func initBlockAdapter(l *lua.State, blockAdapter block.Adapter) *BlockAdapter {
	storageNamespace := lua.CheckString(l, 1)
	return &BlockAdapter{
		adapter:          blockAdapter,
		storageNamespace: storageNamespace,
	}
}

func newAdapter(blockAdapter block.Adapter) lua.Function {
	return func(l *lua.State) int {
		adapter := initBlockAdapter(l, blockAdapter)
		l.NewTable()
		functions := map[string]lua.Function{
			"get_object": adapter.GetObject,
			"put_object": adapter.PutObject,
		}
		for name, goFn := range functions {
			l.PushGoFunction(goFn)
			l.SetField(-2, name)
		}
		return 1
	}
}

func Open(l *lua.State, blockAdapter block.Adapter) {
	open := func(l *lua.State) int {
		lua.NewLibrary(l, []lua.RegistryFunction{
			{Name: "new", Function: newAdapter(blockAdapter)},
		})
		return 1
	}
	lua.Require(l, "lakefs/block_adapter", open, false)
	l.Pop(1)
}
