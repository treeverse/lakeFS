package nessie

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

type hookResponse struct {
	path        string
	err         error
	data        []byte
	queryParams map[string][]string
}

type webhookServer struct {
	s      *http.Server
	respCh chan hookResponse
	port   int
	host   string
}

func (s *webhookServer) BaseURL() string {
	return fmt.Sprintf("http://%s:%d", s.host, s.port)
}

func startWebhookServer() (*webhookServer, error) {
	respCh := make(chan hookResponse, 10)
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
	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		return nil, err
	}

	port := listener.Addr().(*net.TCPAddr).Port
	fmt.Println("Using port:", port)
	s := &http.Server{
		Handler: mux,
	}
	host := os.Getenv("TEST_WEBHOOK_HOST")
	if host == "" {
		host = "nessie"
	}
	go func() {
		if err = s.Serve(listener); err != nil && err != http.ErrServerClosed {
			log.Fatalf("listen:%s\n", err)
		}
	}()

	return &webhookServer{
		s:      s,
		respCh: respCh,
		port:   port,
		host:   host,
	}, nil
}

func timeoutHandlerFunc(chan hookResponse) func(http.ResponseWriter, *http.Request) {
	return func(writer http.ResponseWriter, req *http.Request) {
		select {
		case <-req.Context().Done():
		case <-time.After(2 * hooksTimeout):
		}

		writer.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(writer, "OK")
	}
}

func failHandlerFunc(chan hookResponse) func(http.ResponseWriter, *http.Request) {
	return func(writer http.ResponseWriter, _ *http.Request) {
		writer.WriteHeader(http.StatusInternalServerError)
		_, _ = io.WriteString(writer, "Failed")
	}
}

func hookHandlerFunc(respCh chan hookResponse) func(http.ResponseWriter, *http.Request) {
	return func(writer http.ResponseWriter, request *http.Request) {
		data, err := io.ReadAll(request.Body)
		if err != nil {
			respCh <- hookResponse{path: request.URL.Path, err: err}
			_, _ = io.WriteString(writer, "Failed")
			writer.WriteHeader(http.StatusInternalServerError)
			return
		}

		respCh <- hookResponse{path: request.URL.Path, data: data, queryParams: request.URL.Query()}
		_, _ = io.WriteString(writer, "OK")
		writer.WriteHeader(http.StatusOK)
	}
}

func responseWithTimeout(s *webhookServer, timeout time.Duration) (*hookResponse, error) {
	select {
	case res := <-s.respCh:
		return &res, nil
	case <-time.After(timeout):
		return nil, errors.New("timeout passed waiting for hook")
	}
}
