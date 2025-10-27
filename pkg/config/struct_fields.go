package config

import (
	"reflect"
	"strings"

	"github.com/treeverse/lakefs/pkg/logging"
)

const (
	FieldMaskedValue   = "******"
	FieldMaskedNoValue = "------"
)

// MapLoggingFields returns all logging.Fields formatted based on our configuration keys 'dot.name.key' with
// associated values. Supports squash, and secret to skip printing out secrets.
func MapLoggingFields(value interface{}) logging.Fields {
	fields := make(logging.Fields)
	structFieldsFunc(reflect.ValueOf(value), "mapstructure", ",squash", nil, func(key string, value interface{}) {
		fields[key] = value
	})
	return fields
}

func structFieldsFunc(value reflect.Value, tag, squashValue string, prefix []string, cb func(key string, value interface{})) {
	// finite loop: Go types are well-founded.
	for value.Kind() == reflect.Ptr {
		if value.IsZero() {
			// If required, would already have errored out.
			return
		}
		value = value.Elem()
	}

	// Got to a value we like to call 'cb' with the key/value information
	if value.Kind() != reflect.Struct {
		key := strings.Join(prefix, sep)
		cb(key, value)
		return
	}

	// Scan the struct and
	for i := 0; i < value.NumField(); i++ {
		fieldType := value.Type().Field(i)
		var (
			// fieldName is the name to use for the field
			fieldName string
			// squash the sub-struct no additional accessor when true
			squash bool
			ok     bool
		)
		if fieldName, ok = fieldType.Tag.Lookup(tag); ok {
			if strings.HasSuffix(fieldName, squashValue) {
				squash = true
				fieldName = strings.TrimSuffix(fieldName, squashValue)
			}
		} else {
			fieldName = strings.ToLower(fieldType.Name)
		}
		// Update prefix to recurse into this field.
		if !squash {
			prefix = append(prefix, fieldName)
		}
		fieldValue := value.Field(i)

		switch fieldValue.Interface().(type) {
		case SecureString:
			// don't pass value of SecureString
			key := strings.Join(prefix, sep)
			val := FieldMaskedValue
			if fieldValue.IsZero() {
				val = FieldMaskedNoValue
			}
			cb(key, val)
		default:
			structFieldsFunc(fieldValue, tag, squashValue, prefix, cb)
		}
		// Restore prefix
		if !squash {
			prefix = prefix[:len(prefix)-1]
		}
	}
}

func GetSecureStringKeyPaths(value interface{}) []string {
	keys := []string{}
	getSecureStringKeys(reflect.ValueOf(value), "_", "mapstructure", ",squash", nil, func(key string) {
		keys = append(keys, strings.ToUpper(key))
	})
	return keys
}

func getSecureStringKeys(value reflect.Value, separator, tag, squashValue string, prefix []string, cb func(key string)) {
	for value.Kind() == reflect.Ptr {
		if value.IsZero() {
			return
		}
		value = value.Elem()
	}

	if value.Kind() != reflect.Struct {
		if value.Type() == reflect.TypeOf((*SecureString)(nil)).Elem() {
			key := strings.Join(prefix, separator)
			cb(key)
		}
		return
	}

	for i := 0; i < value.NumField(); i++ {
		fieldType := value.Type().Field(i)
		var (
			fieldName string
			squash    bool
		)
		fieldName, squash = parseTag(fieldType, tag, squashValue)

		if !squash {
			prefix = append(prefix, fieldName)
		}
		fieldValue := value.Field(i)

		getSecureStringKeys(fieldValue, separator, tag, squashValue, prefix, cb)

		if !squash {
			prefix = prefix[:len(prefix)-1]
		}
	}
}

func parseTag(field reflect.StructField, tag, squashValue string) (string, bool) {
	if tagValue, ok := field.Tag.Lookup(tag); ok {
		if strings.HasSuffix(tagValue, squashValue) {
			return strings.TrimSuffix(tagValue, squashValue), true
		}
		return tagValue, false
	}
	return strings.ToLower(field.Name), false
}
