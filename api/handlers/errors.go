package handlers

import "github.com/treeverse/lakefs/api/gen/models"

func responseError(msg string) *models.Error {
	return &models.Error{Message: msg}
}

func responseErrorFrom(err error) *models.Error {
	return responseError(err.Error())
}
