package httputil

import (
	"context"
	"net/http"
	"time"

	"github.com/treeverse/lakefs/logging"

	"github.com/treeverse/lakefs/auth"
)

const (
	RequestIdContextKey = "request_id"
	RequestIdByteLength = 8
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
	if err == nil {
		w.ResponseSize += int64(written)
	}
	return written, err
}

func (w *ResponseRecordingWriter) WriteHeader(statusCode int) {
	w.StatusCode = statusCode
	w.Writer.WriteHeader(statusCode)
}

func RequestId(r *http.Request) (*http.Request, string) {
	var reqId string
	ctx := r.Context()
	resp := ctx.Value(RequestIdContextKey)
	if resp == nil {
		// assign a request ID for this request
		reqId = auth.HexStringGenerator(RequestIdByteLength)
		r = r.WithContext(context.WithValue(ctx, RequestIdContextKey, reqId))
	} else {
		reqId = resp.(string)
	}
	return r, reqId
}

func GetRequestIdFromCtx(ctx context.Context) string {
	resp := ctx.Value(RequestIdContextKey)
	if resp == nil {
		return ""
	}
	return resp.(string)
}

func LoggingMiddleware(requestIdHeaderName string, fields logging.Fields, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		startTime := time.Now()
		writer := &ResponseRecordingWriter{Writer: w, StatusCode: http.StatusOK}
		r, reqID := RequestId(r)

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
		writer.Header().Set(requestIdHeaderName, reqID)
		next.ServeHTTP(writer, r) // handle the request

		logging.FromContext(r.Context()).WithFields(logging.Fields{
			"took":        time.Since(startTime),
			"status_code": writer.StatusCode,
			"sent_bytes":  writer.ResponseSize,
		}).Debug("HTTP call ended")
	})
}
