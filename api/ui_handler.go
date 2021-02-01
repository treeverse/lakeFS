package api

import (
	"encoding/json"
	"net/http"
	"os"
	"path"
	"strings"
	"time"

	"github.com/rakyll/statik/fs"
	"github.com/treeverse/lakefs/auth"
	"github.com/treeverse/lakefs/statik"
	"gopkg.in/dgrijalva/jwt-go.v3"
)

const (
	JWTCookieName          = "access_token"
	DefaultLoginExpiration = time.Hour * 24 * 7
)

type loginData struct {
	AccessKeyID     string `json:"access_key_id"`
	AccessSecretKey string `json:"secret_access_key"`
}

var noCacheHeaders = map[string]string{
	"Expires":         time.Unix(0, 0).Format(time.RFC1123),
	"Cache-Control":   "no-store",
	"X-Accel-Expires": "0",
}

func NoCache(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		for k, v := range noCacheHeaders {
			w.Header().Set(k, v)
		}
		h.ServeHTTP(w, r)
	})
}

func UIHandler(authService auth.Service) http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("/auth/login", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}

		login := &loginData{}
		err := json.NewDecoder(r.Body).Decode(&login)
		if err != nil {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}

		// load credentials by access key
		credentials, err := authService.GetCredentials(login.AccessKeyID)
		if err != nil || credentials.AccessSecretKey != login.AccessSecretKey {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}

		// verify user associated with credentials exists
		_, err = authService.GetUserByID(credentials.UserID)
		if err != nil {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}

		loginTime := time.Now()
		expires := loginTime.Add(DefaultLoginExpiration)
		tokenString, err := CreateAuthToken(authService, login.AccessKeyID, loginTime, expires)
		if err != nil {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		http.SetCookie(w, &http.Cookie{
			Name:     JWTCookieName,
			Value:    tokenString,
			Path:     "/",
			Expires:  expires,
			HttpOnly: true,
			SameSite: http.SameSiteStrictMode,
		})
	})

	mux.HandleFunc("/auth/logout", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		http.SetCookie(w, &http.Cookie{
			Name:     JWTCookieName,
			Value:    "",
			Path:     "/",
			HttpOnly: true,
			Expires:  time.Unix(0, 0),
			SameSite: http.SameSiteStrictMode,
		})
	})

	staticFiles, _ := fs.NewWithNamespace(statik.Webui)
	mux.Handle("/", NoCache(HandlerWithDefault(staticFiles, http.FileServer(staticFiles), "/")))
	return mux
}

func CreateAuthToken(authService auth.Service, accessKeyID string, issuedAt, expiresAt time.Time) (string, error) {
	claims := &jwt.StandardClaims{
		Subject:   accessKeyID,
		IssuedAt:  issuedAt.Unix(),
		ExpiresAt: expiresAt.Unix(),
	}
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	return token.SignedString(authService.SecretStore().SharedSecret())
}

func HandlerWithDefault(root http.FileSystem, handler http.Handler, defaultPath string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		urlPath := r.URL.Path
		if !strings.HasPrefix(urlPath, "/") {
			urlPath = "/" + urlPath
			r.URL.Path = urlPath
		}
		_, err := root.Open(path.Clean(urlPath))
		if err != nil && os.IsNotExist(err) {
			r.URL.Path = defaultPath
		}
		handler.ServeHTTP(w, r)
	})
}
