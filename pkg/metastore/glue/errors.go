package glue

import "errors"

var (
	ErrExpectedType           = errors.New("expected type")
	ErrUnexpectedValue        = errors.New("unexpected value")
	ErrWrongPartitionLocation = errors.New("wrong partition location")
	ErrWrongLocationExpected  = errors.New("wrong location expected")
	ErrWrongColumnExpected    = errors.New("wrong column expected")
)
