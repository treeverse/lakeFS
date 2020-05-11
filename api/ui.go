package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path"
	"strings"
	"time"

	"gopkg.in/dgrijalva/jwt-go.v3"

	"github.com/treeverse/lakefs/auth"

	"github.com/rakyll/statik/fs"
)

const (
	JWTCookieName          = "access_token"
	DefaultLoginExpiration = time.Hour * 24 * 7
)

type loginData struct {
	AccessKeyId     string `json:"access_key_id"`
	AccessSecretKey string `json:"secret_access_key"`
}

func UIHandler(authService auth.Service) http.Handler {
	mux := http.NewServeMux()
	staticFiles, _ := fs.NewWithNamespace("webui")
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
		credentials, err := authService.GetAPICredentials(login.AccessKeyId)
		if err != nil || credentials.AccessSecretKey != login.AccessSecretKey {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		// get user
		user, err := authService.GetUser(*credentials.UserId)
		if err != nil {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}

		loginTime := time.Now()
		expires := loginTime.Add(DefaultLoginExpiration)
		claims := &jwt.StandardClaims{
			IssuedAt:  loginTime.Unix(),
			ExpiresAt: expires.Unix(),
			Subject:   fmt.Sprintf("%d", user.Id),
		}

		token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
		tokenString, err := token.SignedString([]byte(authService.SecretStore().SharedSecret()))
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
	mux.Handle("/", HandlerWithDefault(staticFiles, http.FileServer(staticFiles), "/"))
	return mux
}

func HandlerWithDefault(root http.FileSystem, handler http.Handler, defaultPath string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upath := r.URL.Path
		if !strings.HasPrefix(upath, "/") {
			upath = "/" + upath
			r.URL.Path = upath
		}
		_, err := root.Open(path.Clean(upath))
		if err != nil && os.IsNotExist(err) {
			r.URL.Path = defaultPath
		}
		handler.ServeHTTP(w, r)
	})
}
