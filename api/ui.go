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
	"Cache-Control":   "no-cache, private, max-age=0",
	"Pragma":          "no-cache",
	"X-Accel-Expires": "0",
}

func NoCache(h http.Handler) http.Handler {
	fn := func(w http.ResponseWriter, r *http.Request) {
		for k, v := range noCacheHeaders {
			w.Header().Set(k, v)
		}
		h.ServeHTTP(w, r)
	}
	return http.HandlerFunc(fn)
}

func UIHandler(authService auth.Service) http.Handler {
	mux := http.NewServeMux()
	staticFiles, _ := fs.NewWithNamespace(statik.Webui)
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

		// check login
		credentials, err := authService.GetCredentials(login.AccessKeyID)
		if err != nil || credentials.AccessSecretKey != login.AccessSecretKey {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		// get user
		user, err := authService.GetUserByID(credentials.UserID)
		if err != nil {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}

		loginTime := time.Now()
		expires := loginTime.Add(DefaultLoginExpiration)
		claims := &jwt.StandardClaims{
			IssuedAt:  loginTime.Unix(),
			ExpiresAt: expires.Unix(),
			Subject:   user.Username,
		}

		token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
		tokenString, err := token.SignedString(authService.SecretStore().SharedSecret())
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
			SameSite: http.SameSiteStrictMode,
		})
	})
	mux.Handle("/", NoCache(HandlerWithDefault(staticFiles, http.FileServer(staticFiles), "/")))
	return mux
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
