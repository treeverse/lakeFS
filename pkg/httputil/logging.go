package httputil

import (
	"context"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"github.com/treeverse/lakefs/pkg/logging"
)

type contextKey string

const (
	RequestIDContextKey   contextKey = "request_id"
	SessionIDContextKey   contextKey = "session_id"
	UIRequestIDContextKey contextKey = "ui_request_id"
	AuditLogEndMessage    string     = "HTTP call ended"
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

func UIRequestID(r *http.Request) string {
	ctx := r.Context()
	resp := ctx.Value(UIRequestIDContextKey)
	UIReqID := ""
	if resp != nil {
		UIReqID = resp.(string)
	}
	return UIReqID
}

func SessionID(r *http.Request) (*http.Request, string) {
	ctx := r.Context()
	resp := ctx.Value(SessionIDContextKey)
	var sessionID string
	if resp == nil {
		// assign a session ID for this request
		sessionID = uuid.New().String()
		r = r.WithContext(context.WithValue(ctx, SessionIDContextKey, sessionID))
	} else {
		sessionID = resp.(string)
	}
	return r, sessionID
}

func SourceIP(r *http.Request) string {
	sourceIP, sourcePort, err := net.SplitHostPort(r.RemoteAddr)

	if err != nil {
		return err.Error()
	}
	return sourceIP + ":" + sourcePort
}

func DefaultLoggingMiddleware(requestIDHeaderName, sessionIDHeaderName string, fields logging.Fields, middlewareLogLevel string) func(next http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			startTime := time.Now()
			writer := &ResponseRecordingWriter{Writer: w, StatusCode: http.StatusOK}
			r, reqID := RequestID(r)
			r, sessionID := SessionID(r)
			uiReqID := UIRequestID(r)
			client := GetRequestLakeFSClient(r)
			sourceIP := SourceIP(r)

			// add default fields to context
			requestFields := logging.Fields{
				logging.PathFieldKey:        r.RequestURI,
				logging.MethodFieldKey:      r.Method,
				logging.HostFieldKey:        r.Host,
				logging.RequestIDFieldKey:   reqID,
				logging.SessionIDFieldKey:   sessionID,
				logging.UIRequestIDFieldKey: uiReqID,
			}
			for k, v := range fields {
				requestFields[k] = v
			}
			r = r.WithContext(logging.AddFields(r.Context(), requestFields))
			writer.Header().Set(requestIDHeaderName, reqID)
			writer.Header().Set(sessionIDHeaderName, sessionID)
			next.ServeHTTP(writer, r) // handle the request

			loggingFields := logging.Fields{
				"took":           time.Since(startTime),
				"status_code":    writer.StatusCode,
				"sent_bytes":     writer.ResponseSize,
				"client":         client,
				logging.LogAudit: true,
				"source_ip":      sourceIP,
			}

			logLevel := strings.ToLower(middlewareLogLevel)
			if logLevel == "null" || logLevel == "none" {
				logging.FromContext(r.Context()).WithFields(loggingFields).Debug(AuditLogEndMessage)
			} else {
				level, _ := logrus.ParseLevel(logLevel)
				logging.FromContext(r.Context()).WithFields(loggingFields).Log(level, AuditLogEndMessage)
			}
		})
	}
}

func LoggingMiddleware(requestIDHeaderName, sessionIDHeaderName string, fields logging.Fields, loggingMiddlewareLevel string, traceRequestHeaders bool) func(next http.Handler) http.Handler {
	if strings.ToLower(loggingMiddlewareLevel) == "trace" {
		return TracingMiddleware(requestIDHeaderName, sessionIDHeaderName, fields, traceRequestHeaders)
	}
	return DefaultLoggingMiddleware(requestIDHeaderName, sessionIDHeaderName, fields, loggingMiddlewareLevel)
}
