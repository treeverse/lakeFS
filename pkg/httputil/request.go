package httputil

import (
	"context"
	"errors"
	"net/http"
)

func IsRequestCanceled(r *http.Request) bool {
	if r == nil {
		return false
	}
	return errors.Is(r.Context().Err(), context.Canceled)
}
