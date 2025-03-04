//nolint:unused
package esti

// TODO (niro): All the unused errors is because our esti tests filenames are suffixed with _test
// TODO (niro): WE will need to rename all the esti tests file names to instead using test prefix and not suffix

import (
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"time"
)

type WebhookServer struct {
	s      *http.Server
	respCh chan HookResponse
	port   int
	host   string
}

const hooksTimeout = 2 * time.Second

var ErrWebhookTimeout = errors.New("timeout passed waiting for hook")

func (s *WebhookServer) BaseURL() string {
	return fmt.Sprintf("http://%s:%d", s.host, s.port)
}

func (s *WebhookServer) Server() *http.Server {
	return s.s
}

func StartWebhookServer() (*WebhookServer, error) {
	const channelSize = 10
	respCh := make(chan HookResponse, channelSize)
	mux := http.NewServeMux()
	mux.HandleFunc("/pre-commit", hookHandlerFunc(respCh))
	mux.HandleFunc("/post-commit", hookHandlerFunc(respCh))
	mux.HandleFunc("/pre-merge", hookHandlerFunc(respCh))
	mux.HandleFunc("/post-merge", hookHandlerFunc(respCh))
	mux.HandleFunc("/pre-create-branch", hookHandlerFunc(respCh))
	mux.HandleFunc("/post-create-branch", hookHandlerFunc(respCh))
	mux.HandleFunc("/pre-delete-branch", hookHandlerFunc(respCh))
	mux.HandleFunc("/post-delete-branch", hookHandlerFunc(respCh))
	mux.HandleFunc("/pre-create-tag", hookHandlerFunc(respCh))
	mux.HandleFunc("/post-create-tag", hookHandlerFunc(respCh))
	mux.HandleFunc("/pre-delete-tag", hookHandlerFunc(respCh))
	mux.HandleFunc("/post-delete-tag", hookHandlerFunc(respCh))
	mux.HandleFunc("/timeout", timeoutHandlerFunc(respCh))
	mux.HandleFunc("/fail", failHandlerFunc(respCh))
	listener, err := net.Listen("tcp", ":0") //nolint:gosec
	if err != nil {
		return nil, err
	}

	port := listener.Addr().(*net.TCPAddr).Port
	fmt.Println("Using port:", port)
	s := &http.Server{
		ReadHeaderTimeout: time.Minute,
		Handler:           mux,
	}
	host := os.Getenv("TEST_WEBHOOK_HOST")
	if host == "" {
		host = "esti"
	}
	go func() {
		if err = s.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("listen: %s\n", err)
		}
	}()

	return &WebhookServer{
		s:      s,
		respCh: respCh,
		port:   port,
		host:   host,
	}, nil
}

func timeoutHandlerFunc(_ chan HookResponse) func(http.ResponseWriter, *http.Request) {
	const timeout = 2 * hooksTimeout
	return func(writer http.ResponseWriter, req *http.Request) {
		select {
		case <-req.Context().Done():
		case <-time.After(timeout):
		}

		writer.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(writer, "OK")
	}
}

func failHandlerFunc(_ chan HookResponse) func(http.ResponseWriter, *http.Request) {
	return func(writer http.ResponseWriter, _ *http.Request) {
		writer.WriteHeader(http.StatusInternalServerError)
		_, _ = io.WriteString(writer, "Failed")
	}
}

func hookHandlerFunc(respCh chan HookResponse) func(http.ResponseWriter, *http.Request) {
	return func(writer http.ResponseWriter, request *http.Request) {
		data, err := io.ReadAll(request.Body)
		if err != nil {
			respCh <- HookResponse{Path: request.URL.Path, Err: err}
			_, _ = io.WriteString(writer, "Failed")
			writer.WriteHeader(http.StatusInternalServerError)
			return
		}

		time.Sleep(1 * time.Second) // Added sleep to differentiate between event timestamps
		respCh <- HookResponse{Path: request.URL.Path, Data: data, QueryParams: request.URL.Query()}
		_, _ = io.WriteString(writer, "OK")
		writer.WriteHeader(http.StatusOK)
	}
}
