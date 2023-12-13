package services

import (
	"context"

	"github.com/Shopify/go-lua"
)

func Open(l *lua.State, ctx context.Context) {
	open := func(l *lua.State) int {
		lua.NewLibrary(l, []lua.RegistryFunction{
			{Name: "databricks_client", Function: newDatabricks(ctx)},
		})
		return 1
	}
	lua.Require(l, "services", open, false)
	l.Pop(1)
}
