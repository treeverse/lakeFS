package http

import (
	"bytes"
	"context"
	"net/http"
	"time"

	"github.com/treeverse/lakefs/auth"

	log "github.com/sirupsen/logrus"
)

const (
	RequestIdContextKey = "request_id"
	RequestIdByteLength = 8
)

type ResponseRecordingWriter struct {
	StatusCode   int
	Body         bytes.Buffer
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
		w.Body.Write(data)
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

func LoggingMiddleWare(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Do stuff here
		before := time.Now()
		// Call the next handler, which can be another middleware in the chain, or the final handler.
		writer := &ResponseRecordingWriter{Writer: w, StatusCode: http.StatusOK}
		r, reqId := RequestId(r)
		next.ServeHTTP(writer, r) // handle the request
		writer.Header().Set("X-Amz-Request-Id", reqId)
		log.WithFields(log.Fields{
			"request_id":  reqId,
			"path":        r.RequestURI,
			"method":      r.Method,
			"took":        time.Since(before),
			"status_code": writer.StatusCode,
			"sent_bytes":  writer.ResponseSize,
		}).Debug("S3 gateway called")
	})
}
