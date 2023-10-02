package aws

import (
	"context"

	"github.com/Shopify/go-lua"
)

func Open(l *lua.State, ctx context.Context) {
	open := func(l *lua.State) int {
		lua.NewLibrary(l, []lua.RegistryFunction{
			{Name: "s3_client", Function: newS3Client(ctx)},
			{Name: "glue_client", Function: newGlueClient(ctx)},
		})
		return 1
	}
	lua.Require(l, "aws", open, false)
	l.Pop(1)
}
