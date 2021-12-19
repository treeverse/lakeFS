package httputil

import (
	"context"
	"net/http"
	"time"

	"github.com/treeverse/lakefs/pkg/logging"
)

const FlusherContextKey contextKey = "flusher"

type LongPost struct {
	ctx         context.Context
	writer      http.ResponseWriter
	duration    time.Duration
	stopCh      chan struct{}
	finishedCh  chan struct{}
	statusCode  int
	flusher     http.Flusher
	log         logging.Logger
	closed      bool
	wroteHeader bool
}

func (l *LongPost) Header() http.Header {
	return l.writer.Header()
}

func (l *LongPost) Write(b []byte) (int, error) {
	l.Close()
	return l.writer.Write(b)
}

func (l *LongPost) WriteHeader(statusCode int) {
	l.Close()
	if l.wroteHeader {
		// just warn in case we already set the right status code and content-type
		if statusCode != l.statusCode {
			l.log.WithField("status_code", statusCode).Warnf("Long post '%d' already set, skipping write header with status code", l.statusCode)
		}
	} else {
		l.writer.WriteHeader(statusCode)
	}
}

func NewLongPost(ctx context.Context, w http.ResponseWriter, d time.Duration, statusCode int, log logging.Logger) *LongPost {
	lp := &LongPost{
		ctx:        ctx,
		writer:     w,
		duration:   d,
		statusCode: statusCode,
		stopCh:     make(chan struct{}),
		finishedCh: make(chan struct{}),
		log:        log,
	}
	if f, ok := ctx.Value(FlusherContextKey).(http.Flusher); ok {
		lp.flusher = f
		go lp.delayResponse()
	}
	return lp
}

func (l *LongPost) delayResponse() {
	defer close(l.finishedCh)
	ticker := time.NewTicker(l.duration)
	for {
		select {
		case <-l.ctx.Done():
			return
		case <-l.stopCh:
			return
		case <-ticker.C:
			l.log.Trace("Long post delay")
			// content type as json first time we post something back
			if !l.wroteHeader {
				l.writer.Header().Set("Content-Type", "application/json")
				l.writer.WriteHeader(l.statusCode)
				l.wroteHeader = true
			}
			if _, err := l.writer.Write([]byte{' '}); err != nil {
				l.log.WithError(err).Warn("Long post write content for delay")
			}
			l.flusher.Flush()
		}
	}
}

func (l *LongPost) Close() {
	if l.closed {
		return
	}
	close(l.stopCh)
	<-l.finishedCh
	l.closed = true
}

// FlusherCaptureHandler capture ResponseWriter Flusher implementation, keeping it on request context under
// 'FlusherContextKey'. Used by LongPost to flush content during long post operation.
func FlusherCaptureHandler(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		flusher, ok := w.(http.Flusher)
		if !ok {
			panic("Flush capture should be first in he middleware")
		}
		ctx := context.WithValue(r.Context(), FlusherContextKey, flusher)
		r = r.WithContext(ctx)
		next.ServeHTTP(w, r)
	})
}
