package config

import (
	"errors"
	"fmt"
	"reflect"
	"strings"

	"github.com/mitchellh/mapstructure"
)

// Strings is a []string that mapstructure can deserialize from a single string or from a list
// of strings.
type Strings []string

var (
	ourStringsType  = reflect.TypeOf(Strings{})
	stringType      = reflect.TypeOf("")
	stringSliceType = reflect.TypeOf([]string{})

	ErrInvalidKeyValuePair = errors.New("invalid key-value pair")
)

// DecodeStrings is a mapstructure.HookFuncType that decodes a single string value or a slice
// of strings into Strings.
func DecodeStrings(fromValue reflect.Value, toValue reflect.Value) (interface{}, error) {
	if toValue.Type() != ourStringsType {
		return fromValue.Interface(), nil
	}
	if fromValue.Type() == stringSliceType {
		return Strings(fromValue.Interface().([]string)), nil
	}
	if fromValue.Type() == stringType {
		return Strings(strings.Split(fromValue.String(), ",")), nil
	}
	return fromValue.Interface(), nil
}

type SecureString string

// String returns an elided version.  It is safe to call for logging.
func (SecureString) String() string {
	return "[SECRET]"
}

// SecureValue returns the actual value of s as a string.
func (s SecureString) SecureValue() string {
	return string(s)
}

func (s SecureString) MarshalText() ([]byte, error) {
	if string(s) == "" {
		return []byte(""), nil
	}
	return []byte("[SECRET]"), nil
}

// OnlyString is a string that can deserialize only from a string.  Use it
// to prevent YAML configuration reading a number-like string with leading
// zeros, and then Viper using mapstructure to convert it silently back to a
// string and losing the leading zeros.
type OnlyString string

var (
	onlyStringType  = reflect.TypeOf(OnlyString(""))
	ErrMustBeString = errors.New("must be a string")
)

func (o OnlyString) String() string {
	return string(o)
}

// DecodeOnlyString is a mapstructure.HookFuncType that decodes a string
// value as an OnlyString, but fails on all other values.  It is useful to
// force parsing of a field that can contain just digits as a string, when
// the leading digit might be 0.
func DecodeOnlyString(fromValue reflect.Value, toValue reflect.Value) (interface{}, error) {
	if toValue.Type() != onlyStringType {
		// Not trying to translate to OnlyString
		return fromValue.Interface(), nil
	}
	if fromValue.Type() != stringType {
		return nil, fmt.Errorf("%w, not a %s", ErrMustBeString, fromValue.Type().String())
	}
	return OnlyString(fromValue.Interface().(string)), nil
}

// DecodeStringToMap returns a DecodeHookFunc that converts a string to a map[string]string.
// The string is expected to be a comma-separated list of key-value pairs, where the key and value
// are separated by an equal sign.
func DecodeStringToMap() mapstructure.DecodeHookFunc {
	return func(f reflect.Kind, t reflect.Kind, data interface{}) (interface{}, error) {
		// check if field is a string and target is a map
		if f != reflect.String || t != reflect.Map {
			return data, nil
		}
		// check if target is map[string]string
		if t != reflect.TypeOf(map[string]string{}).Kind() {
			return data, nil
		}

		raw := data.(string)
		if raw == "" {
			return map[string]string{}, nil
		}
		// parse raw string as key1=value1,key2=value2
		const pairSep = ","
		const valueSep = "="
		pairs := strings.Split(raw, pairSep)
		m := make(map[string]string, len(pairs))
		for _, pair := range pairs {
			key, value, found := strings.Cut(pair, valueSep)
			if !found {
				return nil, fmt.Errorf("%w: %s", ErrInvalidKeyValuePair, pair)
			}
			m[strings.TrimSpace(key)] = strings.TrimSpace(value)
		}

		return m, nil
	}
}
