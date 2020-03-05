package utils

import (
	"net/http"
	"strings"

	"github.com/gorilla/mux"
)

func GetRepo(req *http.Request) string {
	vars := mux.Vars(req)
	return strings.ToLower(vars["repo"])
}

func GetKey(req *http.Request) string {
	vars := mux.Vars(req)
	return vars["path"]
}

func GetRef(req *http.Request) string {
	vars := mux.Vars(req)
	return vars["ref"]
}
