package model

import (
	"errors"
	"regexp"

	"github.com/aws/aws-sdk-go/aws/arn"

	"github.com/treeverse/lakefs/permissions"
)

var (
	ErrValidationError = errors.New("validation error")
	EntityIdRegexp     = regexp.MustCompile(`[a-zA-Z0-9+=.,@_\-]{1,127}`)
)

func ValidateRBACEntityId(name string) error {
	if !EntityIdRegexp.MatchString(name) {
		return ErrValidationError
	}
	return nil
}

func ValidateActionName(name string) error {
	if !permissions.IsAction(name) {
		return ErrValidationError
	}
	return nil
}

func ValidateArn(name string) error {
	if !arn.IsARN(name) {
		return ErrValidationError
	}
	return nil
}
