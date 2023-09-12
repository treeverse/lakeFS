package catalog_export

import (
	"context"
	"embed"
	"io/fs"
	"net/http"

	"github.com/Shopify/go-lua"
	"github.com/treeverse/lakefs/pkg/auth/model"
)

//go:embed *.lua
var modulePath embed.FS

func OpenLuaPackage(l *lua.State, ctx context.Context, user *model.User, server *http.Server) {
	loadLuaAsPackage(l, ctx, "lakefs/catalog_export/common", "common.lua", user, server)
	loadLuaAsPackage(l, ctx, "lakefs/catalog_export/table_extractor", "table_extractor.lua", user, server)
	loadLuaAsPackage(l, ctx, "lakefs/catalog_export", "lib.lua", user, server)
}

func loadLuaAsPackage(l *lua.State, ctx context.Context, importAlias, scriptName string, user *model.User, server *http.Server) {
	lua.Require(l, importAlias, func(l *lua.State) int {
		data, err := fs.ReadFile(modulePath, scriptName)
		if err != nil {
			lua.Errorf(l, err.Error())
			panic("unreachable")
		}
		if err := lua.DoString(l, string(data)); err != nil {
			lua.Errorf(l, err.Error())
			panic("unreachable")
		}
		return 1
		// TODO(isan) we want it global or not? (the `true`)
	}, true)
	l.Pop(1)
}
