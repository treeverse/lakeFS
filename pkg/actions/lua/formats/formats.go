package formats

import (
	"context"

	"github.com/Shopify/go-lua"
)

func Open(l *lua.State, ctx context.Context, serverAddress string) {
	open := func(l *lua.State) int {
		lua.NewLibrary(l, []lua.RegistryFunction{
			{Name: "delta_client", Function: newDelta(ctx, serverAddress)},
		})
		return 1
	}
	lua.Require(l, "formats", open, false)
	l.Pop(1)
}
