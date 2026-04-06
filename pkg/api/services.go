package api

import (
	"net/http"

	"github.com/go-chi/chi/v5"
)

// MountNotImplementedServices registers placeholder routes that return HTTP 501 for
// services not available in the open-source build.
func MountNotImplementedServices(router *chi.Mux) {
	router.Mount("/iceberg/api/", http.HandlerFunc(NotImplementedIcebergCatalogHandler))
	router.Mount("/iceberg/relative_to/", http.HandlerFunc(NotImplementedIcebergCatalogHandler))
	router.Mount("/mds/iceberg/api/", http.HandlerFunc(NotImplementedIcebergCatalogHandler))
}

// NotImplementedIcebergCatalogHandler returns HTTP 501 Not Implemented status for Iceberg REST Catalog endpoints.
func NotImplementedIcebergCatalogHandler(w http.ResponseWriter, r *http.Request) {
	http.Error(w, http.StatusText(http.StatusNotImplemented), http.StatusNotImplemented)
}
