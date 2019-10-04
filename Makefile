# Basic ops

# .PHONY: gen-proto

gen-proto: export PATH := $(PATH):$(shell go env GOPATH)/bin
gen-proto:
	cd index/model && protoc *.proto --go_out=plugins=grpc:.


