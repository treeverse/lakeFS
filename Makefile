GOCMD=$(or $(shell which go), $(error "Missing dependency - no go in PATH"))
DOCKER=$(or $(shell which docker), $(error "Missing dependency - no docker in PATH"))
GOBINPATH=$(shell $(GOCMD) env GOPATH)/bin
NPM=$(or $(shell which npm), $(error "Missing dependency - no npm in PATH"))

UID_GID := $(shell id -u):$(shell id -g)

CLIENT_JARS_BUCKET="s3://treeverse-clients-us-east/"

# https://openapi-generator.tech
OPENAPI_GENERATOR_IMAGE=treeverse/openapi-generator-cli:v7.0.1.4
OPENAPI_GENERATOR=$(DOCKER) run --user $(UID_GID) --rm -v $(shell pwd):/mnt $(OPENAPI_GENERATOR_IMAGE)
OPENAPI_RUST_GENERATOR_IMAGE=openapitools/openapi-generator-cli:v7.5.0
OPENAPI_RUST_GENERATOR=$(DOCKER) run --user $(UID_GID) --rm -v $(shell pwd):/mnt $(OPENAPI_RUST_GENERATOR_IMAGE)
PY_OPENAPI_GENERATOR=$(DOCKER) run -e PYTHON_POST_PROCESS_FILE="/mnt/clients/python-static/pydantic.sh" --user $(UID_GID) --rm -v $(shell pwd):/mnt $(OPENAPI_GENERATOR_IMAGE)

GOLANGCI_LINT=github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.1.6
BUF_CLI_VERSION=v1.54.0

ifndef PACKAGE_VERSION
	PACKAGE_VERSION=0.1.0-SNAPSHOT
endif

PYTHON_IMAGE=python:3.9

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

.PHONY: all clean esti lint test gen help
all: build

clean:
	@rm -rf \
		$(LAKECTL_BINARY_NAME) \
		$(LAKEFS_BINARY_NAME) \
		$(UI_BUILD_DIR) \
		$(UI_DIR)/node_modules \
		pkg/api/apigen/lakefs.gen.go \
		pkg/auth/*.gen.go

check-licenses: check-licenses-go-mod check-licenses-npm

check-licenses-go-mod:
	$(GOCMD) install github.com/google/go-licenses@latest
	$(GOBINPATH)/go-licenses check ./cmd/$(LAKEFS_BINARY_NAME)
	$(GOBINPATH)/go-licenses check ./cmd/$(LAKECTL_BINARY_NAME)

check-licenses-npm:
	$(GOCMD) install github.com/senseyeio/diligent/cmd/diligent@latest
	# The -i arg is a workaround to ignore NPM scoped packages until https://github.com/senseyeio/diligent/issues/77 is fixed
	$(GOBINPATH)/diligent check -w permissive -i ^@[^/]+?/[^/]+ $(UI_DIR)

docs/src/assets/js/swagger.yml: api/swagger.yml
	@cp api/swagger.yml docs/src/assets/js/swagger.yml

docs/src/assets/js/authorization.yml: api/authorization.yml
	@cp api/authorization.yml docs/src/assets/js/authorization.yml

docs: docs/src/assets/js/swagger.yml docs/src/assets/js/authorization.yml

docs-serve: ### Serve local docs
	$(DOCKER) run --rm -it -p 4000:4000 -v ./docs:/docs --entrypoint /bin/sh squidfunk/mkdocs-material:9 -c "cd /docs && pip install -r requirements-docs.txt && mkdocs serve --dev-addr=0.0.0.0:4000"

gen-docs: ## Generate CLI docs automatically
	$(GOCMD) run cmd/lakectl/main.go docs > docs/src/reference/cli.md

.PHONY: tools
tools: ## Install tools
	$(GOCMD) install $(GOLANGCI_LINT)
	$(GOCMD) install github.com/bufbuild/buf/cmd/buf@$(BUF_CLI_VERSION)

client-python: api/swagger.yml  ## Generate SDK for Python client - openapi generator version 7.0.0
	rm -rf clients/python
	mkdir -p clients/python
	cp clients/python-static/.openapi-generator-ignore clients/python
	$(PY_OPENAPI_GENERATOR) generate \
		-i /mnt/$< \
		-g python \
		-t /mnt/clients/python-static/templates \
		-c /mnt/clients/python-static/python-codegen-config.yaml \
		--enable-post-process-file \
		--package-name lakefs_sdk \
		--http-user-agent "lakefs-python-sdk/$(PACKAGE_VERSION)" \
		--git-user-id treeverse --git-repo-id lakeFS \
		--additional-properties=infoName=Treeverse,infoEmail=services@treeverse.io,packageVersion=$(PACKAGE_VERSION),projectName=lakefs-sdk,packageUrl=https://github.com/treeverse/lakeFS/tree/master/clients/python \
		-o /mnt/clients/python

sdk-rust: api/swagger.yml  ## Generate SDK for Rust client - openapi generator version 7.1.0
	rm -rf clients/rust
	mkdir -p clients/rust
	$(OPENAPI_RUST_GENERATOR) generate \
		-i /mnt/api/swagger.yml \
		-g rust \
		--additional-properties=infoName=Treeverse,infoEmail=services@treeverse.io,packageName=lakefs_sdk,packageVersion=$(PACKAGE_VERSION),packageUrl=https://github.com/treeverse/lakeFS/tree/master/clients/rust \
		-o /mnt/clients/rust

client-java: api/swagger.yml api/java-gen-ignore  ## Generate SDK for Java (and Scala) client
	rm -rf clients/java
	mkdir -p clients/java
	cp api/java-gen-ignore clients/java/.openapi-generator-ignore
	$(OPENAPI_GENERATOR) generate \
		-i /mnt/api/swagger.yml \
		-g java \
		--invoker-package io.lakefs.clients.sdk \
		--http-user-agent "lakefs-java-sdk/$(PACKAGE_VERSION)-v1" \
		--additional-properties disallowAdditionalPropertiesIfNotPresent=false,useSingleRequestParameter=true,hideGenerationTimestamp=true,artifactVersion=$(PACKAGE_VERSION),parentArtifactId=lakefs-parent,parentGroupId=io.lakefs,parentVersion=0,groupId=io.lakefs,artifactId='sdk',artifactDescription='lakeFS OpenAPI Java client',artifactUrl=https://lakefs.io,apiPackage=io.lakefs.clients.sdk,modelPackage=io.lakefs.clients.sdk.model,mainPackage=io.lakefs.clients.sdk,developerEmail=services@treeverse.io,developerName='Treeverse lakeFS dev',developerOrganization='lakefs.io',developerOrganizationUrl='https://lakefs.io',licenseName=apache2,licenseUrl=http://www.apache.org/licenses/,scmConnection=scm:git:git@github.com:treeverse/lakeFS.git,scmDeveloperConnection=scm:git:git@github.com:treeverse/lakeFS.git,scmUrl=https://github.com/treeverse/lakeFS \
		-o /mnt/clients/java

.PHONY: clients client-python client-java
clients: client-python client-java sdk-rust

package-python: package-python-sdk package-python-wrapper

package-python-sdk: client-python
	$(DOCKER) run --user $(UID_GID) --rm -v $(shell pwd):/mnt -e HOME=/tmp/ -w /mnt/clients/python $(PYTHON_IMAGE) /bin/bash -c \
		"python -m pip install build --user && python -m build --sdist --wheel --outdir dist/"

package-python-wrapper:
	$(DOCKER) run --user $(UID_GID) --rm -v $(shell pwd):/mnt -e HOME=/tmp/ -w /mnt/clients/python-wrapper $(PYTHON_IMAGE) /bin/bash -c \
		"python -m pip install build --user && python -m build --sdist --wheel --outdir dist/"

package: package-python

.PHONY: gen-api
gen-api: docs/src/assets/js/swagger.yml ## Run the swagger code generator
	$(GOGENERATE) ./pkg/api/apigen ./pkg/auth ./pkg/authentication

.PHONY: gen-code
gen-code: gen-api ## Run the generator for inline commands
	$(GOGENERATE) \
		./pkg/actions \
		./pkg/auth/ \
		./pkg/authentication \
		./pkg/distributed \
		./pkg/graveler \
		./pkg/graveler/committed \
		./pkg/graveler/sstable \
		./pkg/kv \
		./pkg/permissions \
		./pkg/pyramid \
		./tools/wrapgen/testcode

LD_FLAGS := "-X github.com/treeverse/lakefs/pkg/version.Version=$(VERSION)-$(REVISION)"
build: gen docs build-binaries ## Download dependencies and build the default binary

build-binaries:
	$(GOBUILD) -o $(LAKEFS_BINARY_NAME) -ldflags $(LD_FLAGS) -v ./cmd/$(LAKEFS_BINARY_NAME)
	$(GOBUILD) -o $(LAKECTL_BINARY_NAME) -ldflags $(LD_FLAGS) -v ./cmd/$(LAKECTL_BINARY_NAME)

lint: ## Lint code
	$(GOCMD) run $(GOLANGCI_LINT) run $(GOLANGCI_LINT_FLAGS)
	npx eslint@8.57.0 $(UI_DIR)/src --ext .js,.jsx,.ts,.tsx

esti: ## run esti (system testing)
	$(GOTEST) -v ./esti --args --system-tests

test: test-go test-hadoopfs  ## Run tests for the project

test-go: gen-api			# Run parallelism > num_cores: most of our slow tests are *not* CPU-bound.
	go list -f '{{.Dir}}/...' -m | xargs $(GOTEST) -count=1 -coverprofile=cover.out -race -cover -failfast -parallel="$(GOTEST_PARALLELISM)" ./...

test-hadoopfs:
	cd clients/hadoopfs && mvn test

run-test:  ## Run tests without generating anything (faster if already generated)
	$(GOTEST) -count=1 -coverprofile=cover.out -race -short -cover -failfast ./...

fast-test:  ## Run tests without race detector (faster)
	$(GOTEST) -count=1 -coverprofile=cover.out -short -cover -failfast ./...

test-html: test  ## Run tests with HTML for the project
	$(GOTOOL) cover -html=cover.out

system-tests: # Run system tests locally
	./esti/scripts/runner.sh -r all

build-docker: build ## Build Docker image file (Docker required)
	$(DOCKER) buildx build --target lakefs -t treeverse/$(DOCKER_IMAGE):$(DOCKER_TAG) .

gofmt:  ## gofmt code formating
	@echo Running go formating with the following command:
	$(GOFMT) -e -s -w .

validate-fmt:  ## Validate go format
	@echo checking gofmt...
	@res=$$($(GOFMT) -d -e -s $$(find . -type d \( -path ./pkg/metastore/hive/gen-go \) -prune -prune -o \( -path ./pkg/api/gen \) -prune -o \( -path ./pkg/permissions/*.gen.go \) -prune -o -name '*.go' -print)); \
	if [ -n "$${res}" ]; then \
		echo checking gofmt fail... ; \
		echo "$${res}"; \
		exit 1; \
	else \
		echo Your code formatting is according to gofmt standards; \
	fi

.PHONY: validate-proto
validate-proto: gen-proto  ## build proto and check if diff found
	git diff --quiet -- pkg/actions/actions.pb.go || (echo "Modification verification failed! pkg/actions/actions.pb.go"; false)
	git diff --quiet -- pkg/auth/model/model.pb.go || (echo "Modification verification failed! pkg/auth/model/model.pb.go"; false)
	git diff --quiet -- pkg/catalog/catalog.pb.go || (echo "Modification verification failed! pkg/catalog/catalog.pb.go"; false)
	git diff --quiet -- pkg/gateway/multipart/multipart.pb.go || (echo "Modification verification failed! pkg/gateway/multipart/multipart.pb.go"; false)
	git diff --quiet -- pkg/graveler/graveler.pb.go || (echo "Modification verification failed! pkg/graveler/graveler.pb.go"; false)
	git diff --quiet -- pkg/graveler/committed/committed.pb.go || (echo "Modification verification failed! pkg/graveler/committed/committed.pb.go"; false)
	git diff --quiet -- pkg/graveler/settings/test_settings.pb.go || (echo "Modification verification failed! pkg/graveler/settings/test_settings.pb.go"; false)
	git diff --quiet -- pkg/kv/secondary_index.pb.go || (echo "Modification verification failed! pkg/kv/secondary_index.pb.go"; false)
	git diff --quiet -- pkg/kv/kvtest/test_model.pb.go || (echo "Modification verification failed! pkg/kv/kvtest/test_model.pb.go"; false)

.PHONY: validate-mockgen
validate-mockgen: gen-code
	git diff --quiet -- pkg/actions/mock/mock_actions.go || (echo "Modification verification failed! pkg/actions/mock/mock_actions.go"; false)
	git diff --quiet -- pkg/auth/mock/mock_auth_client.go || (echo "Modification verification failed! pkg/auth/mock/mock_auth_client.go"; false)
	git diff --quiet -- pkg/authentication/api/mock_authentication_client.go || (echo "Modification verification failed! pkg/authentication/api/mock_authentication_client.go"; false)
	git diff --quiet -- pkg/graveler/committed/mock/batch_write_closer.go || (echo "Modification verification failed! pkg/graveler/committed/mock/batch_write_closer.go"; false)
	git diff --quiet -- pkg/graveler/committed/mock/meta_range.go || (echo "Modification verification failed! pkg/graveler/committed/mock/meta_range.go"; false)
	git diff --quiet -- pkg/graveler/committed/mock/range_manager.go || (echo "Modification verification failed! pkg/graveler/committed/mock/range_manager.go"; false)
	git diff --quiet -- pkg/graveler/mock/graveler.go || (echo "Modification verification failed! pkg/graveler/mock/graveler.go"; false)
	git diff --quiet -- pkg/kv/mock/store.go || (echo "Modification verification failed! pkg/kv/mock/store.go"; false)
	git diff --quiet -- pkg/pyramid/mock/pyramid.go || (echo "Modification verification failed! pkg/pyramid/mock/pyramid.go"; false)

.PHONY: validate-permissions-gen
validate-permissions-gen: gen-code
	git diff --quiet -- pkg/permissions/actions.gen.go || (echo "Modification verification failed!  pkg/permissions/actions.gen.go"; false)

.PHONY: validate-wrapper
validate-wrapper: gen-code
	git diff --quiet -- pkg/auth/service_wrapper.gen.go || (echo "Modification verification failed! pkg/auth/service_wrapper.gen.go"; false)
	git diff --quiet -- pkg/auth/service_inviter_wrapper.gen.go || (echo "Modification verification failed! pkg/auth/service_inviter_wrapper.gen.go"; false)

.PHONY: validate-wrapgen-testcode
validate-wrapgen-testcode: gen-code
	git diff --quiet -- ./tools/wrapgen/testcode || (echo "Modification verification failed! tools/wrapgen/testcode"; false)

validate-reference:
	git diff --quiet -- docs/reference/cli.md || (echo "Modification verification failed! docs/reference/cli.md"; false)

validate-client-python: validate-python-sdk

validate-python-sdk:
	git diff --quiet -- clients/python || (echo "Modification verification failed! python client"; false)

validate-client-java:
	git diff --quiet -- clients/java || (echo "Modification verification failed! java client"; false)

validate-client-rust:
	git diff --quiet -- clients/rust || (echo "Modification verification failed! rust client"; false)

validate-python-wrapper:
	sphinx-apidoc -o clients/python-wrapper/docs clients/python-wrapper/lakefs sphinx-apidoc --full -A 'Treeverse' -eq
	git diff --quiet -- clients/python-wrapper || (echo 'Modification verification failed! python wrapper client'; false)

# Run all validation/linting steps
checks-validator: lint validate-fmt validate-proto \
	validate-client-python validate-client-java validate-client-rust validate-reference \
	validate-mockgen \
	validate-permissions-gen \
	validate-wrapper validate-wrapgen-testcode

python-wrapper-lint:
	$(DOCKER) run --user $(UID_GID) --rm -v $(shell pwd):/mnt -e HOME=/tmp/ -w /mnt/clients/python-wrapper $(PYTHON_IMAGE) /bin/bash -c "./pylint.sh"

python-wrapper-gen-docs:
	sphinx-build -b html -W clients/python-wrapper/docs clients/python-wrapper/_site/
	sphinx-build -b html -W clients/python-wrapper/docs clients/python-wrapper/_site/$$(python clients/python-wrapper/setup.py --version)

$(UI_DIR)/node_modules:
	cd $(UI_DIR) && $(NPM) install

gen-ui: $(UI_DIR)/node_modules  ## Build UI web app
	cd $(UI_DIR) && $(NPM) run build

gen-proto: ## Build Protocol Buffers (proto) files using Buf CLI
	go run github.com/bufbuild/buf/cmd/buf@$(BUF_CLI_VERSION) generate

.PHONY: publish-scala
publish-scala: ## sbt publish spark client jars to Maven Central and to s3 bucket
	cd clients/spark && sbt 'assembly; publishSigned; s3Upload; sonaRelease'
	aws s3 cp --recursive --acl public-read $(CLIENT_JARS_BUCKET) $(CLIENT_JARS_BUCKET) --metadata-directive REPLACE

.PHONY: publish-lakefsfs-test
publish-lakefsfs-test: ## sbt publish spark lakefsfs test jars to s3 bucket
	cd test/lakefsfs && sbt 'assembly; s3Upload'
	aws s3 cp --recursive --acl public-read $(CLIENT_JARS_BUCKET) $(CLIENT_JARS_BUCKET) --metadata-directive REPLACE

help:  ## Show Help menu
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

# helpers
gen: gen-ui gen-api gen-code clients gen-docs

validate-clients-untracked-files:
	scripts/verify_clients_untracked_files.sh
