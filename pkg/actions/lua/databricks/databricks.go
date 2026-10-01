package databricks

import (
	"context"
	"net/http"

	"github.com/Shopify/go-lua"
)

// Open registers the databricks library.  Clients send requests using transport, the SDK default when nil.
func Open(l *lua.State, ctx context.Context, transport http.RoundTripper) {
	open := func(l *lua.State) int {
		lua.NewLibrary(l, []lua.RegistryFunction{
			{Name: "client", Function: newClient(ctx, transport)},
		})
		return 1
	}
	lua.Require(l, "databricks", open, false)
	l.Pop(1)
}
