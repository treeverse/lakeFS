package model

import (
	"errors"
	"regexp"

	"github.com/aws/aws-sdk-go/aws/arn"
	"github.com/treeverse/lakefs/pkg/permissions"
)

var (
	ErrValidationError = errors.New("validation error")

	EntityIDRegexp = regexp.MustCompile(`^[\w+=.,@\-]{1,127}$`)
)

func ValidateAuthEntityID(name string) error {
	if !EntityIDRegexp.MatchString(name) {
		return ErrValidationError
	}
	return nil
}

func ValidateActionName(name string) error {
	return permissions.IsValidAction(name)
}

func ValidateArn(name string) error {
	if !arn.IsARN(name) && name != permissions.All {
		return ErrValidationError
	}
	return nil
}

func ValidateStatementEffect(effect string) error {
	if effect != StatementEffectDeny && effect != StatementEffectAllow {
		return ErrValidationError
	}
	return nil
}
