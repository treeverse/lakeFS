package model

import (
	"errors"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws/arn"
	"github.com/treeverse/lakefs/pkg/kv"
	"github.com/treeverse/lakefs/pkg/permissions"
)

var ErrValidationError = errors.New("validation error")

func ValidateAuthEntityID(name string) error {
	if len(name) == 0 {
		return fmt.Errorf("empty name: %w", ErrValidationError)
	}
	if strings.Contains(name, kv.PathDelimiter) {
		return fmt.Errorf("name contains delimiter %s: %w", kv.PathDelimiter, ErrValidationError)
	}
	return nil
}

func ValidateActionName(name string) error {
	err := permissions.IsValidAction(name)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrValidationError, err)
	}
	return nil
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
