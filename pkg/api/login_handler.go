package api

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/dgrijalva/jwt-go"
	"github.com/treeverse/lakefs/pkg/auth"
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
			writeError(w, http.StatusMethodNotAllowed, nil)
			return
		}

		var login LoginRequestData
		err := json.NewDecoder(r.Body).Decode(&login)
		if err != nil {
			writeError(w, http.StatusUnauthorized, nil)
			return
		}

		// load credentials by access key
		credentials, err := authService.GetCredentials(r.Context(), login.AccessKeyID)
		if err != nil || credentials.AccessSecretKey != login.SecretAccessKey {
			writeError(w, http.StatusUnauthorized, nil)
			return
		}

		// verify user associated with credentials exists
		_, err = authService.GetUserByID(r.Context(), credentials.UserID)
		if err != nil {
			writeError(w, http.StatusUnauthorized, nil)
			return
		}

		loginTime := time.Now()
		expires := loginTime.Add(DefaultLoginExpiration)
		secret := authService.SecretStore().SharedSecret()
		tokenString, err := GenerateJWT(secret, login.AccessKeyID, loginTime, expires)
		if err != nil {
			writeError(w, http.StatusUnauthorized, nil)
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
		response := LoginResponseData{
			Token: tokenString,
		}
		writeResponse(w, http.StatusOK, response)
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
