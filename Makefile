GOCMD=$(or $(shell which go), $(error "Missing dependency - no go in PATH"))
DOCKER=$(or $(shell which docker), $(error "Missing dependency - no docker in PATH"))
GOBINPATH=$(shell $(GOCMD) env GOPATH)/bin
NPM=$(or $(shell which npm), $(error "Missing dependency - no npm in PATH"))

# Protoc is a Docker dependency (since it's a pain to install locally and manage versions of it)
PROTOC_IMAGE="treeverse/protoc:3.14.0"
PROTOC=$(DOCKER) run --rm -v $(shell pwd):/mnt $(PROTOC_IMAGE)

# Same for python swagger validation
SWAGGER_VALIDATOR_IMAGE=treeverse/swagger-spec-validator:latest
SWAGGER_VALIDATOR=$(DOCKER) run --rm -v $(shell pwd):/mnt $(SWAGGER_VALIDATOR_IMAGE)

export PATH:= $(PATH):$(GOBINPATH)

GOBUILD=$(GOCMD) build
GORUN=$(GOCMD) run
GOCLEAN=$(GOCMD) clean
GOTOOL=$(GOCMD) tool
GOGENERATE=$(GOCMD) generate
GOTEST=$(GOCMD) test
GOTESTRACE=$(GOTEST) -race
GOGET=$(GOCMD) get
GOFMT=$(GOCMD)fmt

GO_TEST_MODULES=$(shell $(GOCMD) list ./... | grep -v 'lakefs/pkg/api/gen/')

LAKEFS_BINARY_NAME=lakefs
LAKECTL_BINARY_NAME=lakectl

UI_DIR=webui
UI_BUILD_DIR=$(UI_DIR)/build
API_BUILD_DIR=pkg/api/gen

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

.PHONY: all clean nessie lint test gen help
all: build

clean:
	@rm -rf $(API_BUILD_DIR) $(UI_BUILD_DIR) ddl/statik.go statik $(LAKEFS_BINARY_NAME) $(LAKECTL_BINARY_NAME) \
	    graveler/committed/mock graveler/sstable/mock actions/mock

check-licenses: check-licenses-go-mod check-licenses-npm

check-licenses-go-mod:
	go get github.com/google/go-licenses
	$(GOBINPATH)/go-licenses check ./cmd/$(LAKEFS_BINARY_NAME)
	$(GOBINPATH)/go-licenses check ./cmd/$(LAKECTL_BINARY_NAME)

check-licenses-npm:
	go get github.com/senseyeio/diligent/cmd/diligent
	# The -i arg is a workaround to ignore NPM scoped packages until https://github.com/senseyeio/diligent/issues/77 is fixed
	$(GOBINPATH)/diligent check -w permissive -i ^@[^/]+?/[^/]+ $(UI_DIR)

docs/assets/js/swagger.yml: api/swagger.yml
	@cp api/swagger.yml docs/assets/js/swagger.yml

docs: docs/assets/js/swagger.yml

docs-serve: ### Serve local docs
	cd docs; bundle exec jekyll serve

gen-docs: go-install ## Generate CLI docs automatically
	$(GOCMD) run cmd/lakectl/main.go docs > docs/reference/commands.md

gen-metastore: ## Run Metastore Code generation
	@thrift -r --gen go --gen go:package_prefix=github.com/treeverse/lakefs/pkg/metastore/hive/gen-go/ -o pkg/metastore/hive pkg/metastore/hive/hive_metastore.thrift

go-mod-download: ## Download module dependencies
	$(GOCMD) mod download

go-install: go-mod-download ## Install dependencies
	$(GOCMD) install github.com/go-swagger/go-swagger/cmd/swagger
	$(GOCMD) install github.com/golang/mock/mockgen
	$(GOCMD) install github.com/golangci/golangci-lint/cmd/golangci-lint
	$(GOCMD) install github.com/rakyll/statik
	$(GOCMD) install google.golang.org/protobuf/cmd/protoc-gen-go


gen-api: go-install del-gen-api ## Run the go-swagger code generator
	$(GOGENERATE) ./pkg/api

del-gen-api:
	@rm -rf $(API_BUILD_DIR)
	@mkdir -p $(API_BUILD_DIR)

.PHONY: gen-mockgen
gen-mockgen: go-install ## Run the generator for inline commands
	$(GOGENERATE) ./pkg/graveler/sstable
	$(GOGENERATE) ./pkg/graveler/committed
	$(GOGENERATE) ./pkg/pyramid
	$(GOGENERATE) ./pkg/onboard
	$(GOGENERATE) ./pkg/actions

validate-swagger: go-install ## Validate swagger.yaml
	$(GOBINPATH)/swagger validate api/swagger.yml
	# Run python validation as well
	$(GOBINPATH)/swagger expand --format=json api/swagger.yml > swagger.json
	$(SWAGGER_VALIDATOR) /mnt/swagger.json
	@rm swagger.json

LD_FLAGS := "-X github.com/treeverse/lakefs/pkg/config.Version=$(VERSION)-$(REVISION)"
build: gen docs ## Download dependencies and build the default binary
	$(GOBUILD) -o $(LAKEFS_BINARY_NAME) -ldflags $(LD_FLAGS) -v ./cmd/$(LAKEFS_BINARY_NAME)
	$(GOBUILD) -o $(LAKECTL_BINARY_NAME) -ldflags $(LD_FLAGS) -v ./cmd/$(LAKECTL_BINARY_NAME)

lint: go-install  ## Lint code
	$(GOBINPATH)/golangci-lint run $(GOLANGCI_LINT_FLAGS)

nessie: ## run nessie (system testing)
	$(GOTEST) -v ./nessie --args --system-tests

test: gen  ## Run tests for the project
	$(GOTEST) -count=1 -coverprofile=cover.out -race -cover -failfast $(GO_TEST_MODULES)

run-test:  ## Run tests without generating anything (faster if already generated)
	$(GOTEST) -count=1 -coverprofile=cover.out -race -short -cover -failfast $(GO_TEST_MODULES)

fast-test:  ## Run tests without race detector (faster)
	$(GOTEST) -count=1 -coverprofile=cover.out -short -cover -failfast $(GO_TEST_MODULES)

test-html: test  ## Run tests with HTML for the project
	$(GOTOOL) cover -html=cover.out

build-docker: build ## Build Docker image file (Docker required)
	$(DOCKER) build -t treeverse/$(DOCKER_IMAGE):$(DOCKER_TAG) .

gofmt:  ## gofmt code formating
	@echo Running go formating with the following command:
	$(GOFMT) -e -s -w .

validate-fmt:  ## Validate go format
	@echo checking gofmt...
	@res=$$($(GOFMT) -d -e -s $$(find . -type d \( -path ./pkg/metastore/hive/gen-go \) -prune -o \( -path ./pkg/ddl \) -prune -o \( -path ./pkg/webui \) -prune -o \( -path ./pkg/api/gen \) -prune -o -name '*.go' -print)); \
	if [ -n "$${res}" ]; then \
		echo checking gofmt fail... ; \
		echo "$${res}"; \
		exit 1; \
	else \
		echo Your code formatting is according to gofmt standards; \
	fi

.PHONY: validate-proto
validate-proto: proto  ## build proto and check if diff found
	git diff --quiet -- pkg/catalog/catalog.pb.go
	git diff --quiet -- pkg/graveler/committed/committed.pb.go
	git diff --quiet -- pkg/graveler/graveler.pb.go

checks-validator: lint validate-fmt validate-swagger validate-proto  ## Run all validation/linting steps

$(UI_DIR)/node_modules:
	cd $(UI_DIR) && $(NPM) install

# UI operations
ui-build: $(UI_DIR)/node_modules  ## Build UI app
	cd $(UI_DIR) && $(NPM) run build

ui-bundle: ui-build go-install ## Bundle static built UI app
	$(GOBINPATH)/statik -ns webui -p webui -dest pkg -c -f -src=$(UI_BUILD_DIR)

gen-ui: ui-bundle

gen-ddl: go-install ## Embed data migration files into the resulting binary
	$(GOBINPATH)/statik -ns ddl -m -f -p ddl -c "auto-generated SQL files for data migrations" -dest pkg -src pkg/ddl -include '*.sql'

proto: ## Build proto (Protocol Buffers) files
	$(PROTOC) --proto_path=pkg/catalog --go_out=pkg/catalog --go_opt=paths=source_relative catalog.proto
	$(PROTOC) --proto_path=pkg/graveler/committed --go_out=pkg/graveler/committed --go_opt=paths=source_relative committed.proto
	$(PROTOC) --proto_path=pkg/graveler --go_out=pkg/graveler --go_opt=paths=source_relative graveler.proto

help:  ## Show Help menu
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

# helpers
gen: gen-api gen-ui gen-ddl gen-mockgen
