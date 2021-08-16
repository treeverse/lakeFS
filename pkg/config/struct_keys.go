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
	return appendStructKeys(typ, tag, ","+squashValue, nil, nil, nil)
}

func appendStructKeys(typ reflect.Type, tag, squashValue string, prefix []string, keys []string, tagPredicate func(t reflect.StructTag) bool) []string {
	// finite loop: Go types are well-founded.
	for ; typ.Kind() == reflect.Ptr; typ = typ.Elem() {
	}

	if typ.Kind() != reflect.Struct {
		return append(keys, strings.Join(prefix, sep))
	}

	for i := 0; i < typ.NumField(); i++ {
		fieldType := typ.Field(i)
		var (
			name   string
			squash bool
			ok     bool
		)
		if tagPredicate != nil && !tagPredicate(fieldType.Tag) {
			continue
		}
		if name, ok = fieldType.Tag.Lookup(tag); ok {
			if strings.HasSuffix(name, squashValue) {
				squash = true
				name = strings.TrimSuffix(name, squashValue)
			}
		} else {
			name = fieldType.Name
		}
		key := make([]string, len(prefix))
		copy(key, prefix)
		if !squash {
			key = append(key, name)
		}
		keys = appendStructKeys(fieldType.Type, tag, squashValue, key, keys, tagPredicate)
	}
	return keys
}

// GetMissingRequiredKeys returns all keys of value in GetStructKeys format that have
// an additional required tag set but are unset.
func GetMissingRequiredKeys(value interface{}, tag, squashValue, requiredTag string) []string {
	return appendStructKeysIfZero(reflect.ValueOf(value), reflect.TypeOf(value), tag, ","+squashValue, requiredTag, nil, nil)
}

// isScalar returns true if kind is "scalar", i.e. has no Elem().  This
// recites the list from reflect/type.go and may start to give incorrect
// results if new kinds are added to the language.
func isScalar(kind reflect.Kind) bool {
	switch kind {
	case reflect.Array:
	case reflect.Chan:
	case reflect.Map:
	case reflect.Ptr:
	case reflect.Slice:
		return false
	}
	return true
}

func appendStructKeysIfZero(value reflect.Value, typ reflect.Type, tag, squashValue, requiredTag string, prefix []string, keys []string) []string {
	// finite loop: Go types are well-founded.
	for ; typ.Kind() == reflect.Ptr; typ = typ.Elem() {
		if !value.IsZero() {
			// Keep going on zero values (nil pointers) so we
			// can report that all their required sub-fields are
			// missing.
			value = value.Elem()
		}
	}

	if !isScalar(typ.Kind()) {
		if !isScalar(typ.Elem().Kind()) {
			panic("No support for detecting required keys inside " + value.Kind().String() + " of structs")
		}
	}

	if typ.Kind() != reflect.Struct {
		return keys
	}

	for i := 0; i < typ.NumField(); i++ {
		fieldType := typ.Field(i)
		var fieldValue reflect.Value = value
		if !value.IsZero() {
			fieldValue = value.Field(i)
		}

		var (
			name   string
			squash bool
			ok     bool
		)
		if name, ok = fieldType.Tag.Lookup(tag); ok {
			if strings.HasSuffix(name, squashValue) {
				squash = true
				name = strings.TrimSuffix(name, squashValue)
			}
		} else {
			name = fieldType.Name
		}
		name = strings.ToLower(name)
		if _, ok = fieldType.Tag.Lookup(requiredTag); ok {
			if fieldValue.IsZero() {
				keys = append(keys, strings.Join(append(prefix, name), sep))
			}
		}

		key := make([]string, len(prefix))
		copy(key, prefix)
		if !squash {
			key = append(key, name)
		}
		keys = appendStructKeysIfZero(fieldValue, fieldType.Type, tag, squashValue, requiredTag, key, keys)
	}
	return keys
}
