package config

import (
	"reflect"
	"strings"
)

const sep = "."

// GetStructKeys returns all keys in a nested struct, taking the name from the tag name or the
// field name.  It handles an additional suffix squashValue like mapstructure does: if present
// on an embedded struct, name components for that embedded struct should not be included.  It
// does not handle maps, does chase pointers, but does not check for loops in nesting.
func GetStructKeys(typ reflect.Type, tag, squashValue string) []string {
	return appendStructKeys(typ, tag, ","+squashValue, nil, nil)
}

func appendStructKeys(typ reflect.Type, tag, squashValue string, prefix []string, keys []string) []string {
	// finite loop: Go types are well-founded.
	for ; typ.Kind() == reflect.Ptr; typ = typ.Elem() {
	}

	if typ.Kind() != reflect.Struct {
		return append(keys, strings.Join(prefix, sep))
	}

	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		var (
			name   string
			squash bool
			ok     bool
		)
		if name, ok = field.Tag.Lookup(tag); ok {
			if strings.HasSuffix(name, squashValue) {
				squash = true
				name = strings.TrimSuffix(name, squashValue)
			}
		} else {
			name = field.Name
		}
		key := make([]string, len(prefix))
		copy(key, prefix)
		if !squash {
			key = append(key, name)
		}
		keys = appendStructKeys(field.Type, tag, squashValue, key, keys)
	}
	return keys
}
