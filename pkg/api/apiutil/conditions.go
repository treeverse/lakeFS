package apiutil

import (
	"errors"
	"fmt"

	"github.com/treeverse/lakefs/pkg/api/apigen"
	"github.com/treeverse/lakefs/pkg/graveler"
)

var ErrNotImplemented = errors.New("functionality not implemented")

// BuildConditionFromParams creates a graveler.ConditionFunc from upload params.
// Returns nil if no precondition is specified in the params.
// Handles IfNoneMatch (must be "*") and IfMatch (ETag validation).
func BuildConditionFromParams(ifMatch, ifNoneMatch *string) (*graveler.ConditionFunc, error) {
	var condition graveler.ConditionFunc
	switch {
	case ifMatch != nil && ifNoneMatch != nil:
		return nil, fmt.Errorf("cannot specify both If-Match and If-None-Match: %w", ErrNotImplemented)
	case ifMatch != nil:
		// Handle IfMatch: not supported
		return nil, ErrNotImplemented
	case ifNoneMatch != nil && *ifNoneMatch != "*":
		// If-None-Match only supports "*"
		return nil, fmt.Errorf("If-None-Match only supports '*': %w", ErrNotImplemented)
	case ifNoneMatch != nil:
		condition = func(currentValue *graveler.Value) error {
			if currentValue != nil {
				return graveler.ErrPreconditionFailed
			}
			return nil
		}
	}
	return &condition, nil
}

// BuildOptsFromParams builds copy object request from parameters.
func BuildOptsFromParams(params apigen.CopyObjectJSONRequestBody) ([]graveler.SetOptionsFunc, error) {
	if Value(params.Shallow) {
		// Handle Shallow: not supported
		return nil, ErrNotImplemented
	}
	return []graveler.SetOptionsFunc{graveler.WithForce(Value(params.Force))}, nil
}
