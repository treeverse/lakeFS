package nessie

import (
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"time"
)

type hookResponse struct {
	path string
	err  error
	data []byte
}

type webhookServer struct {
	s      *http.Server
	fail   bool
	respCh chan hookResponse
	port   int
}

func startWebhookServer() (*webhookServer, error) {
	respCh := make(chan hookResponse, 10)
	mux := http.NewServeMux()
	mux.HandleFunc("/pre-commit", hookHandlerFunc(respCh))
	mux.HandleFunc("/pre-merge", hookHandlerFunc(respCh))
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

	go func() {
		if err = s.Serve(listener); err != nil && err != http.ErrServerClosed {
			log.Fatalf("listen:%s\n", err)
		}
	}()

	return &webhookServer{
		s:      s,
		respCh: respCh,
		port:   port,
	}, nil
}

func timeoutHandlerFunc(chan hookResponse) func(http.ResponseWriter, *http.Request) {
	return func(writer http.ResponseWriter, _ *http.Request) {
		time.Sleep(2 * time.Minute)
		_, _ = io.WriteString(writer, "OK")
		writer.WriteHeader(http.StatusOK)
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
		data, err := ioutil.ReadAll(request.Body)
		if err != nil {
			respCh <- hookResponse{path: request.URL.Path, err: err}
			_, _ = io.WriteString(writer, "Failed")
			writer.WriteHeader(http.StatusInternalServerError)
			return
		}
		respCh <- hookResponse{path: request.URL.Path, data: data}
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
