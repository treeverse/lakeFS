package azure

import (
	"context"
	"net/http"

	"github.com/Shopify/go-lua"
)

// Open registers the azure library.  Clients send requests using transport, the SDK default when nil.
func Open(l *lua.State, ctx context.Context, transport http.RoundTripper) {
	open := func(l *lua.State) int {
		lua.NewLibrary(l, []lua.RegistryFunction{
			{Name: "blob_client", Function: newBlobClient(ctx, transport)},
			{Name: "abfss_transform_path", Function: transformPathToAbfss},
		})
		return 1
	}
	lua.Require(l, "azure", open, false)
	l.Pop(1)
}
