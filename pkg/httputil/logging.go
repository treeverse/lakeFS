package httputil

import (
	"context"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/spf13/viper"
	"github.com/treeverse/lakefs/pkg/config"
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

func DefaultLoggingMiddleware(requestIDHeaderName string, fields logging.Fields, middlewareLogLevel string) func(next http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			startTime := time.Now()
			writer := &ResponseRecordingWriter{Writer: w, StatusCode: http.StatusOK}
			r, reqID := RequestID(r)

			// add default fields to context
			requestFields := logging.Fields{
				logging.PathFieldKey:      r.RequestURI,
				logging.MethodFieldKey:    r.Method,
				logging.HostFieldKey:      r.Host,
				logging.RequestIDFieldKey: reqID,
				logging.LoggerName:        "MiddlewareLog",
			}
			for k, v := range fields {
				requestFields[k] = v
			}
			r = r.WithContext(logging.AddFields(r.Context(), requestFields))
			writer.Header().Set(requestIDHeaderName, reqID)
			next.ServeHTTP(writer, r) // handle the request

			loggingFields := logging.Fields{
				"took":        time.Since(startTime),
				"status_code": writer.StatusCode,
				"sent_bytes":  writer.ResponseSize,
			}
			endLogMsg := "HTTP call ended"

			switch strings.ToLower(middlewareLogLevel) {
			case "debug", "null", "none":
				logging.FromContext(r.Context()).WithFields(loggingFields).Debug(endLogMsg)
			case "info":
				logging.FromContext(r.Context()).WithFields(loggingFields).Info(endLogMsg)
			case "warn", "warning":
				logging.FromContext(r.Context()).WithFields(loggingFields).Warning(endLogMsg)
			case "error":
				logging.FromContext(r.Context()).WithFields(loggingFields).Error(endLogMsg)
			case "panic":
				logging.FromContext(r.Context()).WithFields(loggingFields).Panic(endLogMsg)
			}
		})
	}
}

func LoggingMiddleware(requestIDHeaderName string, fields logging.Fields, traceRequestHeaders bool) func(next http.Handler) http.Handler {
	loggingMiddlewareLevel := viper.GetString(config.LoggingMiddlewareLevel)
	if strings.ToLower(loggingMiddlewareLevel) == "trace" {
		return TracingMiddleware(requestIDHeaderName, fields, traceRequestHeaders)
	}
	return DefaultLoggingMiddleware(requestIDHeaderName, fields, loggingMiddlewareLevel)
}
