package utils

import (
	"github.com/gorilla/mux"
	"net/http"
	"strings"
)

func GetRepo(req *http.Request) string {
	vars := mux.Vars(req)
	return strings.ToLower(vars["repo"])
}

func GetKey(req *http.Request) string {
	vars := mux.Vars(req)
	return vars["path"]
}

func GetBranch(req *http.Request) string {
	vars := mux.Vars(req)
	return vars["refspec"]
}
