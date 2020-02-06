# Basic ops

# .PHONY: gen-proto gen-api


DOCKER=$(shell which docker)
PWD=$(shell pwd)
SWAGGER=${DOCKER} run --rm -it -e GOPATH=${HOME}/go:/go -v ${HOME}:${HOME} -w ${PWD} quay.io/goswagger/swagger

gen-proto: export PATH := $(PATH):$(shell go env GOPATH)/bin
gen-proto:
	cd index/model && protoc *.proto --go_out=plugins=grpc:. && cd -
	cd auth/model && protoc *.proto --go_out=plugins=grpc:. && cd -


gen-api:
	rm -rf api/gen/
	mkdir -p api/gen/
	${SWAGGER} generate client -A lakefs -f ./swagger.yml -P models.User -t api/gen
	${SWAGGER} generate server -A lakefs -f ./swagger.yml -P models.User -t api/gen --exclude-main
