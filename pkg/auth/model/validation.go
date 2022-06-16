package model

import (
	"errors"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go/aws/arn"
	"github.com/treeverse/lakefs/pkg/permissions"
)

var (
	ErrValidationError = errors.New("validation error")
	Delimiter          = "/"
)

func ValidateAuthEntityID(name string) error {
	if len(name) == 0 {
		return fmt.Errorf("empty name: %w", ErrValidationError)
	}
	if strings.Contains(name, Delimiter) {
		return fmt.Errorf("name contains delimiter %s: %w", Delimiter, ErrValidationError)
	}
	return nil
}

func ValidateActionName(name string) error {
	return permissions.IsValidAction(name)
}

func ValidateArn(name string) error {
	if !arn.IsARN(name) && name != permissions.All {
		return fmt.Errorf("%w: ARN '%s'", ErrValidationError, name)
	}
	return nil
}

func ValidateStatementEffect(effect string) error {
	if effect != StatementEffectDeny && effect != StatementEffectAllow {
		return fmt.Errorf("%w: effect '%s'", ErrValidationError, effect)
	}
	return nil
}
