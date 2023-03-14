package validator

import (
	"errors"
	"fmt"
	"regexp"
)

var (
	ReValidRef          = regexp.MustCompile(`^[^\s]+$`)
	ReValidBranchID     = regexp.MustCompile(`^\w[-\w]*$`)
	ReValidRepositoryID = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{2,62}$`)
)

var (
	ErrInvalid       = errors.New("validation error")
	ErrInvalidType   = fmt.Errorf("invalid type: %w", ErrInvalid)
	ErrRequiredValue = fmt.Errorf("required value: %w", ErrInvalid)
	ErrInvalidValue  = fmt.Errorf("invalid value: %w", ErrInvalid)
)

type ValidateFunc func(v interface{}) error

type ValidateArg struct {
	Name  string
	Value interface{}
	Fn    ValidateFunc
}

type Secured interface {
	SecureValue() string
}

func Validate(args []ValidateArg) error {
	for _, arg := range args {
		err := arg.Fn(arg.Value)
		if err != nil {
			return fmt.Errorf("argument %s: %w", arg.Name, err)
		}
	}
	return nil
}

func MakeValidateOptional(fn ValidateFunc) ValidateFunc {
	return func(v interface{}) error {
		switch s := v.(type) {
		case string:
			if len(s) == 0 {
				return nil
			}
		case Secured:
			if len(s.SecureValue()) == 0 {
				return nil
			}
		case fmt.Stringer:
			if len(s.String()) == 0 {
				return nil
			}
		case nil:
			return nil
		}
		return fn(v)
	}
}

func ValidateRequiredString(v interface{}) error {
	s, ok := v.(string)
	if !ok {
		panic(ErrInvalidType)
	}
	if len(s) == 0 {
		return ErrRequiredValue
	}
	return nil
}

func ValidateNonNegativeInt(v interface{}) error {
	i, ok := v.(int)
	if !ok {
		panic(ErrInvalidType)
	}
	if i < 0 {
		return ErrInvalidValue
	}
	return nil
}

func ValidateNilOrNonNegativeInt(v interface{}) error {
	i, ok := v.(*int)
	if !ok {
		panic(ErrInvalidType)
	}
	if i == nil {
		return nil
	}
	if *i < 0 {
		return ErrInvalidValue
	}
	return nil
}
