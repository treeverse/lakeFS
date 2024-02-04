package storage

import "github.com/Shopify/go-lua"

type Client interface {
	GetObject(l *lua.State) int
	PutObject(l *lua.State) int
	DeleteObject(l *lua.State) int
	ListObjects(l *lua.State) int
	DeleteRecursive(l *lua.State) int
}

func InitStorageClient(l *lua.State, client Client) {
	l.NewTable()
	functions := map[string]lua.Function{
		"get_object":       client.GetObject,
		"put_object":       client.PutObject,
		"list_objects":     client.ListObjects,
		"delete_object":    client.DeleteObject,
		"delete_recursive": client.DeleteRecursive,
	}
	for name, goFn := range functions {
		l.PushGoFunction(goFn)
		l.SetField(-2, name)
	}
}
