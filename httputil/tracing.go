package httputil

import (
	"io"
	"net/http"
	"time"

	"github.com/treeverse/lakefs/logging"
)

const (
	MaxBodyBytes                      = 100
	RequestTracingMaxRequestBodySize  = 1024 * 1024 * 50  // 50KB
	RequestTracingMaxResponseBodySize = 1024 * 1024 * 150 // 150KB
)

type CappedBuffer struct {
	SizeBytes int
	cursor    int
	Buffer    []byte
}

func (c *CappedBuffer) Write(p []byte) (n int, err error) {
	// pretend to write the whole thing, but only write SizeBytes
	if c.cursor >= c.SizeBytes {
		return len(p), nil
	}
	if c.Buffer == nil {
		c.Buffer = make([]byte, 0)
	}
	var written int
	if len(p) > (c.SizeBytes - c.cursor) {
		c.Buffer = append(c.Buffer, p[0:(c.SizeBytes-c.cursor)]...)
		written = c.SizeBytes - c.cursor
	} else {
		c.Buffer = append(c.Buffer, p...)
		written = len(p)
	}
	c.cursor += written
	return len(p), nil
}

type responseTracingWriter struct {
	StatusCode   int
	ResponseSize int64
	BodyRecorder *CappedBuffer

	Writer      http.ResponseWriter
	multiWriter io.Writer
}

func newResponseTracingWriter(w http.ResponseWriter, sizeInBytes int) *responseTracingWriter {
	buf := &CappedBuffer{
		SizeBytes: sizeInBytes,
	}
	mw := io.MultiWriter(w, buf)
	return &responseTracingWriter{
		StatusCode:   http.StatusOK,
		BodyRecorder: buf,
		Writer:       w,
		multiWriter:  mw,
	}
}

func (w *responseTracingWriter) Header() http.Header {
	return w.Writer.Header()
}

func (w *responseTracingWriter) Write(data []byte) (int, error) {
	return w.multiWriter.Write(data)
}

func (w *responseTracingWriter) WriteHeader(statusCode int) {
	w.StatusCode = statusCode
	w.Writer.WriteHeader(statusCode)
}

type requestBodyTracer struct {
	body         io.ReadCloser
	bodyRecorder *CappedBuffer
	tee          io.Reader
}

func newRequestBodyTracer(body io.ReadCloser, sizeInBytes int) *requestBodyTracer {
	w := &CappedBuffer{
		SizeBytes: sizeInBytes,
	}
	return &requestBodyTracer{
		body:         body,
		bodyRecorder: w,
		tee:          io.TeeReader(body, w),
	}
}

func (r *requestBodyTracer) Read(p []byte) (n int, err error) {
	return r.tee.Read(p)
}

func (r *requestBodyTracer) Close() error {
	return r.body.Close()
}

func presentBody(body []byte) string {
	if len(body) > MaxBodyBytes {
		body = body[:MaxBodyBytes]
	}
	return string(body)
}

func TracingMiddleware(requestIDHeaderName string, fields logging.Fields, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		startTime := time.Now()
		responseWriter := newResponseTracingWriter(w, RequestTracingMaxResponseBodySize)
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
		responseWriter.Header().Set(requestIDHeaderName, reqID)

		// record request body as well
		requestBodyTracer := newRequestBodyTracer(r.Body, RequestTracingMaxRequestBodySize)
		r.Body = requestBodyTracer

		next.ServeHTTP(responseWriter, r) // handle the request

		logging.FromContext(r.Context()).WithFields(logging.Fields{
			"took":             time.Since(startTime),
			"status_code":      responseWriter.StatusCode,
			"sent_bytes":       responseWriter.ResponseSize,
			"request_body":     requestBodyTracer.bodyRecorder.Buffer,
			"request_headers":  r.Header,
			"response_body":    presentBody(responseWriter.BodyRecorder.Buffer),
			"response_headers": responseWriter.Header(),
		}).Trace("HTTP call ended")
	})
}
