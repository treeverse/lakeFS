package api

import (
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestNewUIHandler_StatusCodes(t *testing.T) {
	handler := NewUIHandler(nil)
	tests := []struct {
		name           string
		url            string
		wantStatusCode int
	}{
		{name: "root", url: "/", wantStatusCode: http.StatusOK},
		{name: "robots", url: "/robots.txt", wantStatusCode: http.StatusOK},
		{name: "index", url: "/index.html", wantStatusCode: http.StatusMovedPermanently},
		{name: "noplace", url: "/noplace", wantStatusCode: http.StatusTemporaryRedirect},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rr := httptest.NewRecorder()
			req, err := http.NewRequest(http.MethodGet, tt.url, nil)
			if err != nil {
				t.Fatal(err)
			}
			handler.ServeHTTP(rr, req)
			if rr.Code != tt.wantStatusCode {
				t.Fatalf("Request StatusCode=%d, expected=%d", rr.Code, tt.wantStatusCode)
			}
		})
	}
}

func TestNewUIHandler_GatewayError(t *testing.T) {
	handler := NewUIHandler([]string{"s3.lakefs.dev"})
	rr := httptest.NewRecorder()
	req, err := http.NewRequest(http.MethodGet, "/", nil)
	if err != nil {
		t.Fatal(err)
	}

	req.Header.Set("Authorization", "AWS4-HMAC-SHA256 1234567890")
	handler.ServeHTTP(rr, req)

	// verify aws xml error not found
	expectedStatusCode := http.StatusNotFound
	if rr.Code != expectedStatusCode {
		t.Fatalf("Request status=%d, expected=%d", rr.Code, expectedStatusCode)
	}

	var errMsg struct {
		XMLName xml.Name `xml:"Error"`
		Code    string   `xml:"Code"`
	}
	err = xml.Unmarshal(rr.Body.Bytes(), &errMsg)
	if err != nil {
		t.Fatal("Message unmarshal failed:", err)
	}
	const expectedErrorCode = "ERRLakeFSWrongEndpoint"
	if errMsg.Code != expectedErrorCode {
		t.Fatalf("Invalid XML Code '%s', expected '%s' - response body '%s'",
			errMsg.Code, expectedErrorCode, rr.Body.String())
	}
}
