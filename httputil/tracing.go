package httputil

import (
	"io"
	"net/http"
	"time"

	"github.com/treeverse/lakefs/logging"
)

const (
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

type ResponseTracingWriter struct {
	StatusCode   int
	ResponseSize int64
	BodyRecorder *CappedBuffer

	Writer http.ResponseWriter
}

func (w *ResponseTracingWriter) Header() http.Header {
	return w.Writer.Header()
}

func (w *ResponseTracingWriter) Write(data []byte) (int, error) {
	mw := io.MultiWriter(w.Writer, w.BodyRecorder)
	written, err := mw.Write(data)
	if err == nil {
		w.ResponseSize += int64(written)
	}
	return written, err
}

func (w *ResponseTracingWriter) WriteHeader(statusCode int) {
	w.StatusCode = statusCode
	w.Writer.WriteHeader(statusCode)
}

type requestBodyTracer struct {
	body         io.ReadCloser
	bodyRecorder *CappedBuffer
}

func (r requestBodyTracer) Read(p []byte) (n int, err error) {
	n, err = r.body.Read(p)
	_, _ = r.bodyRecorder.Write(p[0:n])
	return n, err
}

func (r requestBodyTracer) Close() error {
	return r.body.Close()
}

func TracingMiddleware(requestIdHeaderName string, fields logging.Fields, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		startTime := time.Now()
		responseWriter := &ResponseTracingWriter{
			StatusCode:   http.StatusOK,
			BodyRecorder: &CappedBuffer{SizeBytes: RequestTracingMaxResponseBodySize},
			Writer:       w,
		}
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
		responseWriter.Header().Set(requestIdHeaderName, reqID)

		// record request body as well
		requestBodyTracer := &requestBodyTracer{
			body:         r.Body,
			bodyRecorder: &CappedBuffer{SizeBytes: RequestTracingMaxRequestBodySize},
		}
		r.Body = requestBodyTracer

		next.ServeHTTP(responseWriter, r) // handle the request

		logging.FromContext(r.Context()).WithFields(logging.Fields{
			"took":             time.Since(startTime),
			"status_code":      responseWriter.StatusCode,
			"sent_bytes":       responseWriter.ResponseSize,
			"request_body":     requestBodyTracer.bodyRecorder.Buffer,
			"request_headers":  r.Header,
			"response_body":    responseWriter.BodyRecorder.Buffer,
			"response_headers": responseWriter.Header(),
		}).Trace("HTTP call ended")
	})
}
