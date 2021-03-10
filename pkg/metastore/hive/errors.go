package hive

import "errors"

var (
	ErrExpectedType           = errors.New("expected type")
	ErrWrongPartitionLocation = errors.New("wrong partition location")
	ErrWrongLocationExpected  = errors.New("wrong location expected")
	ErrWrongColumnExpected    = errors.New("wrong column expected")
)
