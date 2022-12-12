package util

import (
	"errors"
	"fmt"

	"github.com/Shopify/go-lua"
)

var (
	ErrCannotPull     = errors.New("cannot pull go type into lua")
	ErrStackExhausted = errors.New("pull table, stack exhausted")
)

func Open(l *lua.State) {
	l.Register("array", luaArray)
}

func PullStringTable(l *lua.State, idx int) (map[string]string, error) {
	if !l.IsTable(idx) {
		return nil, fmt.Errorf("need a table at index %d, got %s: %w", idx, lua.TypeNameOf(l, idx), ErrCannotPull)
	}

	// Table at idx
	l.PushNil() // Add free slot for the value, +1

	table := make(map[string]string)
	// -1:nil, idx:table
	for l.Next(idx) {
		// -1:val, -2:key, idx:table
		key, ok := l.ToString(-2)
		if !ok {
			return nil, fmt.Errorf("key should be a string (%v): %w", l.ToValue(-2), ErrCannotPull)
		}
		val, ok := l.ToString(-1)
		if !ok {
			return nil, fmt.Errorf("value for key '%s' should be a string (%v): %w", key, l.ToValue(-1), ErrCannotPull)
		}
		table[key] = val
		l.Pop(1) // remove val from top, -1
		// -1:key, idx: table
	}

	return table, nil
}

func PullTable(l *lua.State, idx int) (interface{}, error) {
	if !l.IsTable(idx) {
		return nil, fmt.Errorf("need a table at index %d, got %s: %w", idx, lua.TypeNameOf(l, idx), ErrCannotPull)
	}

	return pullTableRec(l, idx)
}

func pullTableRec(l *lua.State, idx int) (interface{}, error) {
	if !l.CheckStack(2) {
		return nil, ErrStackExhausted
	}

	idx = l.AbsIndex(idx)
	if isArray(l, idx) {
		return pullArrayRec(l, idx)
	}

	table := make(map[string]interface{})

	l.PushNil()
	for l.Next(idx) {
		// -1: value, -2: key, ..., idx: table
		key, ok := l.ToString(-2)
		if !ok {
			err := fmt.Errorf("key should be a string (%s): %w", lua.TypeNameOf(l, -2), ErrCannotPull)
			l.Pop(2)
			return nil, err
		}

		value, err := toGoValue(l, -1)
		if err != nil {
			l.Pop(2)
			return nil, err
		}

		table[key] = value

		l.Pop(1)
	}

	return table, nil
}

const arrayMarkerField = "_is_array"

func luaArray(l *lua.State) int {
	l.NewTable()
	l.PushBoolean(true)
	l.SetField(-2, arrayMarkerField)
	l.SetMetaTable(-2)
	return 1
}

func isArray(l *lua.State, idx int) bool {
	if !l.IsTable(idx) {
		return false
	}

	if !lua.MetaField(l, idx, arrayMarkerField) {
		return false
	}
	defer l.Pop(1)

	return l.ToBoolean(-1)
}

func pullArrayRec(l *lua.State, idx int) (interface{}, error) {
	table := make([]interface{}, lua.LengthEx(l, idx))

	l.PushNil()
	for l.Next(idx) {
		k, ok := l.ToInteger(-2)
		if !ok {
			l.Pop(2)
			return nil, fmt.Errorf("pull array: expected numeric index, got '%s': %w", l.TypeOf(-2), ErrCannotPull)
		}

		v, err := toGoValue(l, -1)
		if err != nil {
			l.Pop(2)
			return nil, err
		}

		table[k-1] = v
		l.Pop(1)
	}

	return table, nil
}

func toGoValue(l *lua.State, idx int) (interface{}, error) {
	t := l.TypeOf(idx)
	switch t {
	case lua.TypeBoolean:
		return l.ToBoolean(idx), nil
	case lua.TypeString:
		return lua.CheckString(l, idx), nil
	case lua.TypeNumber:
		return lua.CheckNumber(l, idx), nil
	case lua.TypeTable:
		return pullTableRec(l, idx)
	default:
		err := fmt.Errorf("pull table, unsupported type %s: %w", lua.TypeNameOf(l, idx), ErrCannotPull)
		return nil, err
	}
}
