package httputil

import (
	"context"
	"errors"
	"net/http"
)

func IsRequestCanceled(r *http.Request) bool {
	return errors.Is(r.Context().Err(), context.Canceled)
}
