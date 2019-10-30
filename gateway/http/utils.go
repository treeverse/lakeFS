package http

import (
	"bytes"
	"context"
	"net/http"
	"versio-index/auth"
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
	}
	return r, reqId
}
