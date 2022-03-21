GOCMD=$(or $(shell which go), $(error "Missing dependency - no go in PATH"))
DOCKER=$(or $(shell which docker), $(error "Missing dependency - no docker in PATH"))
GOBINPATH=$(shell $(GOCMD) env GOPATH)/bin
NPM=$(or $(shell which npm), $(error "Missing dependency - no npm in PATH"))

UID_GID := $(shell id -u):$(shell id -g)

# Protoc is a Docker dependency (since it's a pain to install locally and manage versions of it)
PROTOC_IMAGE="treeverse/protoc:3.14.0"
PROTOC=$(DOCKER) run --rm -v $(shell pwd):/mnt $(PROTOC_IMAGE)

CLIENT_JARS_BUCKET="s3://treeverse-clients-us-east/"

# https://openapi-generator.tech
OPENAPI_GENERATOR_IMAGE=openapitools/openapi-generator-cli:v5.3.0
OPENAPI_GENERATOR=$(DOCKER) run --user $(UID_GID) --rm -v $(shell pwd):/mnt $(OPENAPI_GENERATOR_IMAGE)

ifndef PACKAGE_VERSION
	PACKAGE_VERSION=0.1.0-SNAPSHOT
endif

PYTHON_IMAGE=python:3

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

GOTEST_PARALLELISM=4

GO_TEST_MODULES=$(shell $(GOCMD) list ./... | grep -v 'lakefs/pkg/api/gen/')

LAKEFS_BINARY_NAME=lakefs
LAKECTL_BINARY_NAME=lakectl

UI_DIR=webui
UI_BUILD_DIR=$(UI_DIR)/dist

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
	@rm -rf \
		$(LAKECTL_BINARY_NAME) \
		$(LAKEFS_BINARY_NAME) \
		$(UI_BUILD_DIR) \
		pkg/actions/mock \
		pkg/api/lakefs.gen.go \
		pkg/graveler/sstable/mock \
	    pkg/graveler/committed/mock \
	    pkg/graveler/mock

check-licenses: check-licenses-go-mod check-licenses-npm

check-licenses-go-mod:
	$(GOCMD) install github.com/google/go-licenses@latest
	$(GOBINPATH)/go-licenses check ./cmd/$(LAKEFS_BINARY_NAME)
	$(GOBINPATH)/go-licenses check ./cmd/$(LAKECTL_BINARY_NAME)

check-licenses-npm:
	$(GOCMD) install github.com/senseyeio/diligent/cmd/diligent@latest
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
	$(GOCMD) install github.com/deepmap/oapi-codegen/cmd/oapi-codegen
	$(GOCMD) install github.com/golang/mock/mockgen
	$(GOCMD) install github.com/golangci/golangci-lint/cmd/golangci-lint
	$(GOCMD) install google.golang.org/protobuf/cmd/protoc-gen-go


client-python: api/swagger.yml  ## Generate SDK for Python client
	# remove the build folder as it also holds lakefs_client folder which keeps because we skip it during find
	rm -rf clients/python/build; cd clients/python && \
		find . -depth -name lakefs_client -prune -o ! \( -name client.py -or -name Gemfile -or -name Gemfile.lock -or -name _config.yml -or -name .openapi-generator-ignore \) -delete
	$(OPENAPI_GENERATOR) generate \
		-i /mnt/$< \
		-g python \
		--package-name lakefs_client \
		--git-user-id treeverse --git-repo-id lakeFS \
		--additional-properties=infoName=Treeverse,infoEmail=services@treeverse.io,packageName=lakefs_client,packageVersion=$(PACKAGE_VERSION),projectName=lakefs-client,packageUrl=https://github.com/treeverse/lakeFS/tree/master/clients/python \
		-o /mnt/clients/python

client-java: api/swagger.yml  ## Generate SDK for Java (and Scala) client
	rm -rf clients/java
	$(OPENAPI_GENERATOR) generate \
		-i /mnt/$< \
		-g java \
		--invoker-package io.lakefs.clients.api \
		--additional-properties=hideGenerationTimestamp=true,artifactVersion=$(PACKAGE_VERSION),parentArtifactId=lakefs-parent,parentGroupId=io.lakefs,parentVersion=0,groupId=io.lakefs,artifactId='api-client',artifactDescription='lakeFS OpenAPI Java client',artifactUrl=https://lakefs.io,apiPackage=io.lakefs.clients.api,modelPackage=io.lakefs.clients.api.model,mainPackage=io.lakefs.clients.api,developerEmail=services@treeverse.io,developerName='Treeverse lakeFS dev',developerOrganization='lakefs.io',developerOrganizationUrl='https://lakefs.io',licenseName=apache2,licenseUrl=http://www.apache.org/licenses/,scmConnection=scm:git:git@github.com:treeverse/lakeFS.git,scmDeveloperConnection=scm:git:git@github.com:treeverse/lakeFS.git,scmUrl=https://github.com/treeverse/lakeFS \
		-o /mnt/clients/java

.PHONY: clients client-python client-java
clients: client-python client-java

package-python: client-python
	$(DOCKER) run --user $(UID_GID) --rm -v $(shell pwd):/mnt -e HOME=/tmp/ -w /mnt/clients/python $(PYTHON_IMAGE) /bin/bash -c \
		"python -m pip install build --user && python -m build --sdist --wheel --outdir dist/"

package: package-python

gen-api: go-install ## Run the swagger code generator
	$(GOGENERATE) ./pkg/api

.PHONY: gen-mockgen
gen-mockgen: go-install ## Run the generator for inline commands
	$(GOGENERATE) ./pkg/graveler/sstable
	$(GOGENERATE) ./pkg/graveler/committed
	$(GOGENERATE) ./pkg/graveler
	$(GOGENERATE) ./pkg/pyramid
	$(GOGENERATE) ./pkg/onboard
	$(GOGENERATE) ./pkg/actions

LD_FLAGS := "-X github.com/treeverse/lakefs/pkg/version.Version=$(VERSION)-$(REVISION)"
build: gen docs ## Download dependencies and build the default binary
	$(GOBUILD) -o $(LAKEFS_BINARY_NAME) -ldflags $(LD_FLAGS) -v ./cmd/$(LAKEFS_BINARY_NAME)
	$(GOBUILD) -o $(LAKECTL_BINARY_NAME) -ldflags $(LD_FLAGS) -v ./cmd/$(LAKECTL_BINARY_NAME)

lint: go-install  ## Lint code
	$(GOBINPATH)/golangci-lint run $(GOLANGCI_LINT_FLAGS)

nessie: ## run nessie (system testing)
	$(GOTEST) -v ./nessie --args --system-tests

test: test-go test-hadoopfs  ## Run tests for the project

test-go: gen			# Run parallelism > num_cores: most of our slow tests are *not* CPU-bound.
	$(GOTEST) -count=1 -coverprofile=cover.out -race -cover -failfast --parallel="$(GOTEST_PARALLELISM)" $(GO_TEST_MODULES)

test-hadoopfs:
	cd clients/hadoopfs && mvn test

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
	git diff --quiet -- pkg/catalog/catalog.pb.go || (echo "Modification verification failed! graveler's catalog proto"; false)
	git diff --quiet -- pkg/graveler/committed/committed.pb.go || (echo "Modification verification failed! graveler's committed proto"; false)
	git diff --quiet -- pkg/graveler/graveler.pb.go || (echo "Modification verification failed! graveler's proto"; false)
	git diff --quiet -- pkg/graveler/settings/test_settings.pb.go || (echo "Modification verification failed! graveler's settings test proto"; false)

validate-reference:
	git diff --quiet -- docs/reference/commands.md || (echo "Modification verification failed! commands docs"; false)

validate-client-python:
	git diff --quiet -- clients/python || (echo "Modification verification failed! python client"; false)

validate-client-java:
	git diff --quiet -- clients/java || (echo "Modification verification failed! java client"; false)

# Run all validation/linting steps
checks-validator: lint validate-fmt validate-proto validate-client-python validate-client-java validate-reference

$(UI_DIR)/node_modules:
	cd $(UI_DIR) && $(NPM) install

# UI operations
ui-build: $(UI_DIR)/node_modules  ## Build UI app
	cd $(UI_DIR) && $(NPM) run build

gen-ui: ui-build

proto: ## Build proto (Protocol Buffers) files
	$(PROTOC) --proto_path=pkg/catalog --go_out=pkg/catalog --go_opt=paths=source_relative catalog.proto
	$(PROTOC) --proto_path=pkg/graveler/committed --go_out=pkg/graveler/committed --go_opt=paths=source_relative committed.proto
	$(PROTOC) --proto_path=pkg/graveler --go_out=pkg/graveler --go_opt=paths=source_relative graveler.proto
	$(PROTOC) --proto_path=pkg/graveler/settings --go_out=pkg/graveler/settings --go_opt=paths=source_relative test_settings.proto

publish-scala: ## sbt publish spark client jars to nexus and s3 bucket
	cd clients/spark && sbt assembly && sbt s3Upload && sbt publishSigned
	aws s3 cp --recursive --acl public-read $(CLIENT_JARS_BUCKET) $(CLIENT_JARS_BUCKET) --metadata-directive REPLACE

publish-lakefsfs-test: ## sbt publish spark lakefsfs test jars to s3 bucket
	cd test/lakefsfs && sbt assembly && sbt s3Upload
	aws s3 cp --recursive --acl public-read $(CLIENT_JARS_BUCKET) $(CLIENT_JARS_BUCKET) --metadata-directive REPLACE

help:  ## Show Help menu
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

# helpers
gen: gen-api gen-ui gen-mockgen clients gen-docs
