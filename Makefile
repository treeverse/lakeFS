
GOCMD=$(shell which go)
DOCKER=$(shell which docker)
GOBINPATH=$(shell $(GOCMD) env GOPATH)
NPM=$(shell which npm)
STATIK=$(GOBINPATH)/bin/statik

GOBUILD=$(GOCMD) build
GOCLEAN=$(GOCMD) clean
GOTOOL=$(GOCMD) tool
GOTEST=$(GOCMD) test
GOTESTRACE=$(GOTEST) -race
GOGET=$(GOCMD) get
GOFMT=$(GOCMD)fmt

SWAGGER=${DOCKER} run --rm -i -e GOPATH=$(GOBINPATH):/go -v ${HOME}:${HOME} -w $(CURDIR) quay.io/goswagger/swagger:v0.23.0
PROTOC=${DOCKER} run --rm -i -v $(CURDIR):/defs namely/protoc-all:1.28_0

BINARY_NAME=lakefs
CLI_BINARY_NAME=lakectl

UI_DIR=$(PWD)/webui
UI_BUILD_DIR=$(UI_DIR)/build

DOCKER_IMAGE=lakefs
DOCKER_TAG=dev

all: build

gen-proto: ## Build the protobuf definitions into go code (Docker required)
	$(PROTOC) -f index/model/model.proto -l go -o .
	$(PROTOC) -f auth/model/model.proto -l go -o .

gen-api:  ## Run the go-swagger code generator (Docker required)
	rm -rf api/gen/
	mkdir -p api/gen/
	$(SWAGGER) generate client -A lakefs -f ./swagger.yml -P models.User -t api/gen
	$(SWAGGER) generate server -A lakefs -f ./swagger.yml -P models.User -t api/gen --exclude-main

validate-swagger:  ## Validate swagger.yaml
	$(SWAGGER) validate  ./swagger.yml

build: gen ## Download dependecies and Build the default binary
		$(GOBUILD) -o $(BINARY_NAME) -v main.go
		$(GOBUILD) -o $(CLI_BINARY_NAME) -v cli/main.go

test: gen ## Run tests for the project
		$(GOTEST) -count=1 -coverprofile=cover.out -short -cover -failfast ./...

test-race: gen ## Run tests for the project with -race
		$(GOTEST) -count=1 -coverprofile=cover.out -race -short -cover -failfast ./...

test-html: test ## Run tests with HTML for the project
		$(GOTOOL) cover -html=cover.out

build-docker: ## Build Docker image file (Docker required)
		$(DOCKER) build -t $(DOCKER_IMAGE):$(DOCKER_TAG) .

gofmt: gen ## gofmt code formating
	@echo Running go formating with the following command:
	$(GOFMT) -e -s -w .

fmt-validator: gen ## Validate go format
	@echo checking gofmt...
	@res=$$($(GOFMT) -d -e -s $$(find . -type d \( -path ./src/vendor \) -prune -o -name '*.go' -print)); \
	if [ -n "$${res}" ]; then \
		echo checking gofmt fail... ; \
		echo "$${res}"; \
		exit 1; \
	else \
		echo Your code formating is according gofmt standards; \
	fi

checks-validator: fmt-validator validate-swagger ## Run all validation/linting steps

# UI operations
ui-build:  ## Build UI app
	cd $(UI_DIR) && $(NPM) run build && cd -

ui-bundle:  ## Bundle static built UI app
	$(STATIK) -src=$(UI_BUILD_DIR)

ui: ui-build ui-bundle

help: ## Show Help menu
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'


# helppers
gen: gen-proto gen-api ui
