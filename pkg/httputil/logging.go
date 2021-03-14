package httputil

import (
	"context"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/treeverse/lakefs/pkg/logging"
)

type contextKey string

const (
	RequestIDContextKey contextKey = "request_id"
)

type ResponseRecordingWriter struct {
	StatusCode   int
	ResponseSize int64
	Writer       http.ResponseWriter
}

func (w *ResponseRecordingWriter) Header() http.Header {
	return w.Writer.Header()
}

func (w *ResponseRecordingWriter) Write(data []byte) (int, error) {
	written, err := w.Writer.Write(data)
	w.ResponseSize += int64(written)
	return written, err
}

func (w *ResponseRecordingWriter) WriteHeader(statusCode int) {
	w.StatusCode = statusCode
	w.Writer.WriteHeader(statusCode)
}

func RequestID(r *http.Request) (*http.Request, string) {
	ctx := r.Context()
	resp := ctx.Value(RequestIDContextKey)
	var reqID string
	if resp == nil {
		// assign a request ID for this request
		reqID = uuid.New().String()
		r = r.WithContext(context.WithValue(ctx, RequestIDContextKey, reqID))
	} else {
		reqID = resp.(string)
	}
	return r, reqID
}

func DebugLoggingMiddleware(requestIDHeaderName string, fields logging.Fields) func(next http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			startTime := time.Now()
			writer := &ResponseRecordingWriter{Writer: w, StatusCode: http.StatusOK}
			r, reqID := RequestID(r)

			// add default fields to context
			requestFields := logging.Fields{
				"path":       r.RequestURI,
				"method":     r.Method,
				"host":       r.Host,
				"request_id": reqID,
			}
			for k, v := range fields {
				requestFields[k] = v
			}
			r = r.WithContext(logging.AddFields(r.Context(), requestFields))
			writer.Header().Set(requestIDHeaderName, reqID)
			next.ServeHTTP(writer, r) // handle the request

			logging.FromContext(r.Context()).WithFields(logging.Fields{
				"took":        time.Since(startTime),
				"status_code": writer.StatusCode,
				"sent_bytes":  writer.ResponseSize,
			}).Debug("HTTP call ended")
		})
	}
}

func LoggingMiddleware(requestIDHeaderName string, fields logging.Fields) func(next http.Handler) http.Handler {
	if logging.Level() == "trace" {
		return TracingMiddleware(requestIDHeaderName, fields)
	}
	return DebugLoggingMiddleware(requestIDHeaderName, fields)
}
