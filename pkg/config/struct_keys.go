package config

import (
	"reflect"
	"strings"
)

const sep = "."

// GetStructKeys returns all keys in a nested struct, taking the name from the tag name or the
// field name.  It does not handle maps, does chase pointers, but does not check for loops in
// nesting.
func GetStructKeys(typ reflect.Type, tag string) []string {
	return appendStructKeys(typ, tag, nil, nil)
}

func appendStructKeys(typ reflect.Type, tag string, prefix []string, keys []string) []string {
	// finite loop: Go types are well-founded.
	for ; typ.Kind() == reflect.Ptr; typ = typ.Elem() {
	}

	if typ.Kind() != reflect.Struct {
		return append(keys, strings.Join(prefix, sep))
	}

	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		var (
			name string
			ok   bool
		)
		if name, ok = field.Tag.Lookup(tag); !ok {
			name = field.Name
		}
		
		key := append(prefix, name)
		keys = appendStructKeys(field.Type, tag, key, keys)
	}
	return keys
}
