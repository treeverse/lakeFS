package databricks

import (
	"context"

	"github.com/Shopify/go-lua"
)

func Open(l *lua.State, ctx context.Context) {
	open := func(l *lua.State) int {
		lua.NewLibrary(l, []lua.RegistryFunction{
			{Name: "client", Function: newClient(ctx)},
		})
		return 1
	}
	lua.Require(l, "databricks", open, false)
	l.Pop(1)
}
