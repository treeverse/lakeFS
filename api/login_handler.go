package api

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/dgrijalva/jwt-go"
	"github.com/treeverse/lakefs/auth"
)

type LoginRequestData struct {
	AccessKeyID     string `json:"access_key_id"`
	SecretAccessKey string `json:"secret_access_key"`
}

type LoginResponseData struct {
	Token string `json:"token"`
}

const (
	DefaultLoginExpiration = time.Hour * 24 * 7
	JWTCookieName          = "access_token"
)

func GenerateJWT(secret []byte, accessKeyID string, issuedAt, expiresAt time.Time) (string, error) {
	claims := &jwt.StandardClaims{
		Subject:   accessKeyID,
		IssuedAt:  issuedAt.Unix(),
		ExpiresAt: expiresAt.Unix(),
	}
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	return token.SignedString(secret)
}

func NewLoginHandler(authService auth.Service) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}

		login := &LoginRequestData{}
		err := json.NewDecoder(r.Body).Decode(&login)
		if err != nil {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}

		// load credentials by access key
		credentials, err := authService.GetCredentials(login.AccessKeyID)
		if err != nil || credentials.AccessSecretKey != login.SecretAccessKey {
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
		secret := authService.SecretStore().SharedSecret()
		tokenString, err := GenerateJWT(secret, login.AccessKeyID, loginTime, expires)
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
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(LoginResponseData{
			Token: tokenString,
		})
	})
}

func NewLogoutHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
}
