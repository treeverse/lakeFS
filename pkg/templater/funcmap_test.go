package templater_test

import (
	"github.com/treeverse/lakefs/pkg/templater"

	"errors"
	"fmt"
	"reflect"
	"testing"
	"text/template"
)

func asError(i interface{}) error {
	if i == nil {
		return nil
	}
	return i.(error)
}

func TestWrapFuncMapWithData(t *testing.T) {
	errFailed := errors.New("(for testing)")
	baseFuncs := template.FuncMap{
		"ok": func(data string) string { return data + ":" + "yes" },
		"maybe": func(data string) (string, error) {
			if data != "" {
				return "", fmt.Errorf("Fail on %s %w", data, errFailed)
			}
			return "no data", nil
		},
		"join": func(dataSep string, a, b string) (string, error) {
			return a + dataSep + b, nil
		},
	}

	type Case struct {
		Name     string
		FuncName string
		Data     string
		Args     []interface{}
		Err      error
		Result   string
	}
	cases := []Case{
		{
			Name:     "call ok",
			FuncName: "ok",
			Data:     "data",
			Result:   "data:yes",
		}, {
			Name:     "call maybe",
			FuncName: "maybe",
			Data:     "",
			Result:   "no data",
		}, {
			Name:     "fail maybe",
			FuncName: "maybe",
			Data:     "fail",
			Err:      errFailed,
		}, {
			Name:     "join",
			FuncName: "join",
			Data:     "+",
			Args:     []interface{}{"foo", "bar"},
			Result:   "foo+bar",
		},
	}

	for _, c := range cases {
		t.Run(c.Name, func(t *testing.T) {
			funcs := templater.WrapFuncMapWithData(baseFuncs, c.Data)
			f := reflect.ValueOf(funcs[c.FuncName])
			argsVal := make([]reflect.Value, 0, len(c.Args))
			for _, arg := range c.Args {
				argsVal = append(argsVal, reflect.ValueOf(arg))
			}
			r := f.Call(argsVal)
			switch len(r) {
			case 1:
				res := r[0].Interface().(string)
				if res != c.Result {
					t.Errorf("Got result %q when expecting %q", res, c.Result)
				}
			case 2:
				res := r[0].Interface().(string)
				if res != c.Result {
					t.Errorf("Got result %q when expecting %q", res, c.Result)
				}
				err := asError(r[1].Interface())
				if !errors.Is(err, c.Err) {
					if c.Err == nil {
						t.Errorf("Got unexpected error %v", err)
					} else {
						t.Errorf("Got error %v when expecting %v", err, c.Err)
					}
				}
			default:
				t.Errorf("Got %d return values: %+v", len(r), r)
			}
		})
	}
}
