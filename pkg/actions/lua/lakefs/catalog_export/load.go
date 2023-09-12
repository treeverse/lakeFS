package catalog_export

import (
	"context"
	"embed"
	"io/fs"

	"github.com/Shopify/go-lua"
)

//go:embed *.lua
var modulePath embed.FS

// OpenLuaPackage load lua code as a package in the runtime
func OpenLuaPackage(l *lua.State, ctx context.Context) {
	// order here matters each when packages rely on each other
	loadLuaAsPackage(l, ctx, "lakefs/catalog_export/common", "common.lua")
	loadLuaAsPackage(l, ctx, "lakefs/catalog_export/table_extractor", "table_extractor.lua")
	// lib.lua is high level facade for users
	loadLuaAsPackage(l, ctx, "lakefs/catalog_export", "lib.lua")
}

func loadLuaAsPackage(l *lua.State, ctx context.Context, importAlias, scriptName string) {
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
	}, true)
	l.Pop(1)
}
