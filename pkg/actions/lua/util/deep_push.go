package util

import (
	"fmt"
	"reflect"

	"github.com/Shopify/go-lua"
)

// DeepPush will put any basic Go type on the lua stack. If the value
// contains a map or a slice, it will recursively push those values as
// tables on the Lua stack.
//
// Supported types are:
//
//	| Go                       | Lua
//	|-------------------------------------------------------------------------
//	| nil                      | nil
//	| bool                     | bool
//	| string                   | string
//	| any int                  | number (float64)
//	| any float                | number (float64)
//	| any complex              | number (real value as float64)
//	|                          |
//	| map[t]t                  | table, key and val `t` recursively
//	|                          | resolved
//	|                          |
//	| []t                      | table with array properties, with `t`
//	|                          | values recursively resolved
func DeepPush(l *lua.State, v interface{}) int {
	forwardOnType(l, v)
	return 1
}

func forwardOnType(l *lua.State, val interface{}) {
	switch val := val.(type) {
	case nil:
		l.PushNil()

	case bool:
		l.PushBoolean(val)

	case string:
		l.PushString(val)

	case uint8:
		l.PushNumber(float64(val))
	case uint16:
		l.PushNumber(float64(val))
	case uint32:
		l.PushNumber(float64(val))
	case uint64:
		l.PushNumber(float64(val))
	case uint:
		l.PushNumber(float64(val))

	case int8:
		l.PushNumber(float64(val))
	case int16:
		l.PushNumber(float64(val))
	case int32:
		l.PushNumber(float64(val))
	case int64:
		l.PushNumber(float64(val))
	case int:
		l.PushNumber(float64(val))

	case float32:
		l.PushNumber(float64(val))
	case float64:
		l.PushNumber(val)

	case complex64:
		forwardOnType(l, []float32{real(val), imag(val)})
	case complex128:
		forwardOnType(l, []float64{real(val), imag(val)})

	default:
		forwardOnReflect(l, val)
	}
}

func forwardOnReflect(l *lua.State, val interface{}) {
	switch v := reflect.ValueOf(val); v.Kind() {

	case reflect.Array, reflect.Slice:
		recurseOnFuncSlice(l, func(i int) interface{} { return v.Index(i).Interface() }, v.Len())

	case reflect.Map:
		l.CreateTable(0, v.Len())
		for _, key := range v.MapKeys() {
			mapKey := key.Interface()
			mapVal := v.MapIndex(key).Interface()
			forwardOnType(l, mapKey)
			forwardOnType(l, mapVal)
			l.RawSet(-3)
		}

	default:
		lua.Errorf(l, fmt.Sprintf("contains unsupported type: %T", val))
		panic("unreachable")
	}
}

// the hack of using a func(int)interface{} makes it that it is valid for any
// type of slice
func recurseOnFuncSlice(l *lua.State, input func(int) interface{}, n int) {
	l.CreateTable(n, 0)
	luaArray(l)
	for i := 0; i < n; i++ {
		forwardOnType(l, input(i))
		l.RawSetInt(-2, i+1)
	}
}
