package api

import (
	"fmt"

	"github.com/treeverse/lakefs/pkg/api/gen/models"
)

func responseError(msg string, args ...interface{}) *models.Error {
	return &models.Error{Message: fmt.Sprintf(msg, args...)}
}

func responseErrorFrom(err error) *models.Error {
	return responseError(err.Error())
}
