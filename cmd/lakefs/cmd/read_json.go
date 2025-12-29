package cmd

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"
)

var (
	ErrBadCallback    = errors.New("bad callback")
	ErrNotFunc        = fmt.Errorf("%w: not a func", ErrBadCallback)
	ErrFuncParameters = fmt.Errorf("%w: bad func parameters", ErrBadCallback)
	ErrFuncReturn     = fmt.Errorf("%w: bad func return", ErrBadCallback)
	ErrUnknownType    = errors.New("unknown type")
	ErrNoMatch        = errors.New("no callback matched")

	// Reflected type of an error.  The trick is to _get_ a value which actually is an
	// error.  https://stackoverflow.com/a/30688564
	errorInterface = reflect.TypeFor[error]()
)

func parseFuncType(t reflect.Type) (reflect.Type, error) {
	if t.Kind() != reflect.Func {
		return nil, ErrNotFunc
	}
	if t.NumIn() != 1 {
		return nil, fmt.Errorf("%w: need 1 got %d", ErrFuncParameters, t.NumIn())
	}
	argType := t.In(0)
	if t.NumOut() != 1 {
		return nil, fmt.Errorf("%w: need 1 got %d", ErrFuncReturn, t.NumIn())
	}
	if !t.Out(0).Implements(errorInterface) {
		return nil, fmt.Errorf("%w: return type %s is not an error", ErrFuncReturn, t.String())
	}
	return argType, nil
}

// ReadJSON unmarshals lines of complete JSON records from r.  Every callback should be a func
// of the form `func(*Struct) error` for some Struct; ReadJSON tries to convert each line to the
// parameter type of the each callback, calls the first callback whose argument matches, and
// returns its error.
func ReadJSON(reader *bufio.Reader, funcs ...any) error {
	line, err := reader.ReadString('\n')
	if err != nil && !(errors.Is(err, io.EOF) && line != "") {
		return err
	}
	var decodeErrs error
	for index, fn := range funcs {
		argType, err := parseFuncType(reflect.TypeOf(fn))
		if err != nil {
			return fmt.Errorf("func %d: %w", index, err)
		}
		obj := reflect.New(argType) // obj is a pointer to argType
		dec := json.NewDecoder(strings.NewReader(line))
		dec.DisallowUnknownFields()
		err = dec.Decode(obj.Interface())
		if err != nil {
			decodeErrs = errors.Join(decodeErrs, err)
			continue
		}
		// parseFunc validated fn as a callable on argType.
		fnValue := reflect.ValueOf(fn)
		results := fnValue.Call([]reflect.Value{reflect.Indirect(obj)})
		// parseFunc validated that results has exactly one value of type error.
		if results[0].IsNil() {
			return nil
		}
		return results[0].Interface().(error)
	}
	return fmt.Errorf("%s: %w (failed conversions: %w)", line, ErrNoMatch, decodeErrs)
}
