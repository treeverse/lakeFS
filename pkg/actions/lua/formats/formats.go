package formats

import (
	"context"

	"github.com/Shopify/go-lua"
)

func Open(l *lua.State, ctx context.Context, lakeFSAddr string) {
	open := func(l *lua.State) int {
		lua.NewLibrary(l, []lua.RegistryFunction{
			{Name: "delta_client", Function: newDelta(ctx, lakeFSAddr)},
		})
		return 1
	}
	lua.Require(l, "formats", open, false)
	l.Pop(1)
}
