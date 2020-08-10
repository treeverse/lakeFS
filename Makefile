GOCMD=$(or $(shell which go), $(error "Missing dependency - no go in PATH"))
DOCKER=$(or $(shell which docker), $(error "Missing dependency - no docker in PATH"))
GOBINPATH=$(shell $(GOCMD) env GOPATH)
NPM=$(or $(shell which npm), $(error "Missing dependency - no npm in PATH"))
STATIK=$(or $(shell test -e "$(GOBINPATH)/bin/statik" && echo "$(GOBINPATH)/bin/statik"), $(error "Missing statik pkg - get it with `go get github.com/rakyll/statik`"))

GOBUILD=$(GOCMD) build
GOCLEAN=$(GOCMD) clean
GOTOOL=$(GOCMD) tool
GOTEST=$(GOCMD) test
GOTESTRACE=$(GOTEST) -race
GOGET=$(GOCMD) get
GOFMT=$(GOCMD)fmt

GO_TEST_MODULES=$(shell $(GOCMD) list ./... | grep -v 'lakefs/api/gen/')

LAKEFS_BINARY_NAME=lakefs
LAKECTL_BINARY_NAME=lakectl

UI_DIR=webui
UI_BUILD_DIR=$(UI_DIR)/build
API_BUILD_DIR=api/gen

DOCKER_IMAGE=lakefs
DOCKER_TAG=dev
VERSION=dev
export VERSION

# This cannot detect whether untracked files have yet to be added.
# That is sort-of a git feature, but can be a limitation here.
DIRTY=$(shell git diff-index --quiet HEAD -- || echo '.with.local.changes')
GIT_REF=$(shell git rev-parse --short HEAD --)
REVISION=$(GIT_REF)$(DIRTY)
export REVISION

all: build

clean:
	@rm -rf $(API_BUILD_DIR) $(UI_BUILD_DIR) ddl/statik.go statik $(LAKEFS_BINARY_NAME) $(LAKECTL_BINARY_NAME)

docs/assets/js/swagger.yml: swagger.yml
	@cp swagger.yml docs/assets/js/swagger.yml

docs: docs/assets/js/swagger.yml

docs-serve: ### Serve local docs
	cd docs; bundle exec jekyll serve

gen-metastore: ## Run Metastore Code generation
	@thrift -r --gen go --gen go:package_prefix=github.com/treeverse/lakefs/metastore/hive/gen-go/ -o metastore/hive metastore/hive/hive_metastore.thrift

gen-api: ## Run the go-swagger code generator
	@rm -rf $(API_BUILD_DIR)
	@mkdir -p $(API_BUILD_DIR)
	@go get github.com/go-swagger/go-swagger/cmd/swagger
	swagger generate client -q -A lakefs -f ./swagger.yml -P models.User -t $(API_BUILD_DIR)
	swagger generate server -q -A lakefs -f ./swagger.yml -P models.User -t $(API_BUILD_DIR) --exclude-main

validate-swagger:  ## Validate swagger.yaml
	swagger validate swagger.yml

LD_FLAGS := "-X github.com/treeverse/lakefs/config.Version=$(VERSION)-$(REVISION)"
build: gen docs ## Download dependencies and build the default binary
	$(GOBUILD) -o $(LAKEFS_BINARY_NAME) -ldflags $(LD_FLAGS) -v ./cmd/$(LAKEFS_BINARY_NAME)
	$(GOBUILD) -o $(LAKECTL_BINARY_NAME) -ldflags $(LD_FLAGS) -v ./cmd/$(LAKECTL_BINARY_NAME)

lint: ## Lint code
	go get github.com/golangci/golangci-lint/cmd/golangci-lint
	golangci-lint run -v

test: gen run-test  ## Run tests for the project

run-test:  ## Run tests without generating anything (faster if already generated)
	$(GOTEST) -count=1 -coverprofile=cover.out -race -short -cover -failfast $(GO_TEST_MODULES)

fast-test:  ## Run tests without race detector (faster)
	$(GOTEST) -count=1 -coverprofile=cover.out -short -cover -failfast $(GO_TEST_MODULES)

test-html: test  ## Run tests with HTML for the project
	$(GOTOOL) cover -html=cover.out

build-docker: ## Build Docker image file (Docker required)
	$(DOCKER) build -t $(DOCKER_IMAGE):$(DOCKER_TAG) .

gofmt:  ## gofmt code formating
	@echo Running go formating with the following command:
	$(GOFMT) -e -s -w .

fmt-validator:  ## Validate go format
	@echo checking gofmt...
	@res=$$($(GOFMT) -d -e -s $$(find . -type d \( -path ./ddl \) -prune -o \( -path ./statik \) -prune -o \( -path ./api/gen \) -prune -o -name '*.go' -print)); \
	if [ -n "$${res}" ]; then \
		echo checking gofmt fail... ; \
		echo "$${res}"; \
		exit 1; \
	else \
		echo Your code formatting is according to gofmt standards; \
	fi

checks-validator: lint fmt-validator validate-swagger ## Run all validation/linting steps

$(UI_DIR)/node_modules:
	cd $(UI_DIR) && $(NPM) install

# UI operations
ui-build: $(UI_DIR)/node_modules  ## Build UI app
	cd $(UI_DIR) && $(NPM) run build

ui-bundle: ui-build ## Bundle static built UI app
	$(STATIK) -ns webui -m -f -src=$(UI_BUILD_DIR)

gen-ui: ui-bundle

gen-ddl:  ## Embed data migration files into the resulting binary
	$(STATIK) -ns ddl -m -f -p ddl -c "auto-generated SQL files for data migrations" -src ddl -include '*.sql'

help:  ## Show Help menu
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

# helpers
gen: gen-api gen-ui gen-ddl
