package azure

import (
	"context"

	"github.com/Shopify/go-lua"
)

func Open(l *lua.State, ctx context.Context) {
	open := func(l *lua.State) int {
		lua.NewLibrary(l, []lua.RegistryFunction{
			{Name: "blob_client", Function: newBlobClient(ctx)},
			{Name: "abfss_transform_path", Function: transformPathToAbfss},
		})
		return 1
	}
	lua.Require(l, "azure", open, false)
	l.Pop(1)
}
