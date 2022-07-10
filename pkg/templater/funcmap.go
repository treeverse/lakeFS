package templater

import (
	"errors"
	"fmt"
	"reflect"
	"text/template"
)

var ErrBadFuncRet = errors.New("wrong number of return values")

// WrapFuncMapWithData wraps a funcMap to a FuncMap that passes data as a
// first element when calling each element.
func WrapFuncMapWithData(funcMap template.FuncMap, data interface{}) template.FuncMap {
	ret := make(template.FuncMap, len(funcMap))
	for k, v := range funcMap {
		funcValue := reflect.ValueOf(v)
		ret[k] = func(args ...interface{}) (interface{}, error) {
			argVals := make([]reflect.Value, 0, len(args)+1)
			argVals = append(argVals, reflect.ValueOf(data))
			for _, arg := range args {
				argVals = append(argVals, reflect.ValueOf(arg))
			}
			retVals := funcValue.Call(argVals)
			switch len(retVals) {
			case 1:
				return retVals[0].Interface(), nil
			case 2: //nolint:gomnd
				errInterface := retVals[1].Interface()
				var err error
				if errInterface != nil {
					err = errInterface.(error)
				}
				return retVals[0].Interface(), err
			default:
				// TODO(ariels): This is an implementation error; panic?
				return nil, fmt.Errorf("call to %s returned %d values: %w", k, len(retVals), ErrBadFuncRet)
			}
		}
	}
	return ret
}
