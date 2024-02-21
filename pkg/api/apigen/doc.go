// Package apigen provides generated code for our OpenAPI
package apigen

//go:generate go run github.com/deepmap/oapi-codegen/v2/cmd/oapi-codegen@v2.1.0 -package apigen -generate "types,client,chi-server,spec" -templates ../tmpl -o lakefs.gen.go ../../../api/swagger.yml
