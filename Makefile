GOCMD=$(or $(shell which go), $(error "Missing dependency - no go in PATH"))
DOCKER=$(or $(shell which docker), $(error "Missing dependency - no docker in PATH"))
GOBINPATH=$(shell $(GOCMD) env GOPATH)/bin
NPM=$(or $(shell which npm), $(error "Missing dependency - no npm in PATH"))

UID_GID := $(shell id -u):$(shell id -g)

CLIENT_JARS_BUCKET="s3://treeverse-clients-us-east/"

# https://openapi-generator.tech
OPENAPI_LEGACY_GENERATOR_IMAGE=openapitools/openapi-generator-cli:v5.3.0
OPENAPI_LEGACY_GENERATOR=$(DOCKER) run --user $(UID_GID) --rm -v $(shell pwd):/mnt $(OPENAPI_LEGACY_GENERATOR_IMAGE)
OPENAPI_GENERATOR_IMAGE=treeverse/openapi-generator-cli:v7.0.0.1
OPENAPI_GENERATOR=$(DOCKER) run --user $(UID_GID) --rm -v $(shell pwd):/mnt $(OPENAPI_GENERATOR_IMAGE)
OPENAPI_RUST_GENERATOR_IMAGE=openapitools/openapi-generator-cli:v7.5.0
OPENAPI_RUST_GENERATOR=$(DOCKER) run --user $(UID_GID) --rm -v $(shell pwd):/mnt $(OPENAPI_RUST_GENERATOR_IMAGE)
PY_OPENAPI_GENERATOR=$(DOCKER) run -e PYTHON_POST_PROCESS_FILE="/mnt/clients/python/scripts/pydantic.sh" --user $(UID_GID) --rm -v $(shell pwd):/mnt $(OPENAPI_GENERATOR_IMAGE)

GOLANGCI_LINT_VERSION=v1.53.3
BUF_CLI_VERSION=v1.28.1

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
		pkg/auth/client.gen.go

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
	cd docs; bundle exec jekyll serve --livereload

docs-serve-docker: ### Serve local docs from Docker
	docker run --rm \
			--name lakefs_docs \
			-e TZ="Etc/UTC" \
			--publish 4000:4000 --publish 35729:35729 \
			--volume="$$PWD/docs:/srv/jekyll:Z" \
			--volume="$$PWD/docs/.jekyll-bundle-cache:/usr/local/bundle:Z" \
			--interactive --tty \
			jekyll/jekyll:4.2.2 \
			jekyll serve --livereload

gen-docs: ## Generate CLI docs automatically
	$(GOCMD) run cmd/lakectl/main.go docs > docs/reference/cli.md

gen-metastore: ## Run Metastore Code generation
	@thrift -r --gen go --gen go:package_prefix=github.com/treeverse/lakefs/pkg/metastore/hive/gen-go/ -o pkg/metastore/hive pkg/metastore/hive/hive_metastore.thrift

tools: ## Install tools
	$(GOCMD) install github.com/golangci/golangci-lint/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION)
	$(GOCMD) install github.com/bufbuild/buf/cmd/buf@$(BUF_CLI_VERSION)

client-python: sdk-python-legacy sdk-python

sdk-python-legacy: api/swagger.yml  ## Generate SDK for Python client - openapi generator version 5.3.0
	# remove the build folder as it also holds lakefs_client folder which keeps because we skip it during find
	rm -rf clients/python-legacy/build; cd clients/python-legacy && \
		find . -depth -name lakefs_client -prune -o ! \( -name Gemfile -or -name Gemfile.lock -or -name _config.yml -or -name .openapi-generator-ignore -or -name templates -or -name setup.mustache -or -name client.mustache -or -name python-codegen-config.yaml \) -delete
	$(OPENAPI_LEGACY_GENERATOR) generate \
		-i /mnt/$< \
		-g python \
		-t /mnt/clients/python-legacy/templates \
		--package-name lakefs_client \
		--http-user-agent "lakefs-python-sdk/$(PACKAGE_VERSION)-legacy" \
		--git-user-id treeverse --git-repo-id lakeFS \
		--additional-properties=infoName=Treeverse,infoEmail=services@treeverse.io,packageName=lakefs_client,packageVersion=$(PACKAGE_VERSION),projectName=lakefs-client,packageUrl=https://github.com/treeverse/lakeFS/tree/master/clients/python-legacy \
		-c /mnt/clients/python-legacy/python-codegen-config.yaml \
		-o /mnt/clients/python-legacy \
		--ignore-file-override /mnt/clients/python/.openapi-generator-ignore

sdk-python: api/swagger.yml  ## Generate SDK for Python client - openapi generator version 7.0.0
	# remove the build folder as it also holds lakefs_sdk folder which keeps because we skip it during find
	rm -rf clients/python/build; cd clients/python && \
		find . -depth -name lakefs_sdk -prune -o ! \( -name Gemfile -or -name Gemfile.lock -or -name _config.yml -or -name .openapi-generator-ignore -or -name templates -or -name setup.mustache -or -name client.mustache -or -name requirements.mustache -or -name scripts -or -name pydantic.sh -or -name python-codegen-config.yaml \) -delete
	$(PY_OPENAPI_GENERATOR) generate \
		--enable-post-process-file \
		-i /mnt/$< \
		-g python \
		-t /mnt/clients/python/templates \
		--package-name lakefs_sdk \
		--http-user-agent "lakefs-python-sdk/$(PACKAGE_VERSION)" \
		--git-user-id treeverse --git-repo-id lakeFS \
		--additional-properties=infoName=Treeverse,infoEmail=services@treeverse.io,packageVersion=$(PACKAGE_VERSION),projectName=lakefs-sdk,packageUrl=https://github.com/treeverse/lakeFS/tree/master/clients/python \
		-c /mnt/clients/python/python-codegen-config.yaml \
		-o /mnt/clients/python \
		--ignore-file-override /mnt/clients/python/.openapi-generator-ignore

sdk-rust: api/swagger.yml  ## Generate SDK for Rust client - openapi generator version 7.1.0
	rm -rf clients/rust
	mkdir -p clients/rust
	$(OPENAPI_RUST_GENERATOR) generate \
		-i /mnt/api/swagger.yml \
		-g rust \
		--additional-properties=infoName=Treeverse,infoEmail=services@treeverse.io,packageName=lakefs_sdk,packageVersion=$(PACKAGE_VERSION),packageUrl=https://github.com/treeverse/lakeFS/tree/master/clients/rust \
		-o /mnt/clients/rust

client-java-legacy: api/swagger.yml api/java-gen-ignore  ## Generate legacy SDK for Java (and Scala) client
	rm -rf clients/java-legacy
	$(OPENAPI_LEGACY_GENERATOR) generate \
		-i /mnt/$< \
		--ignore-file-override /mnt/api/java-gen-ignore \
		-g java \
		--invoker-package io.lakefs.clients.api \
		--http-user-agent "lakefs-java-sdk/$(PACKAGE_VERSION)-legacy" \
		--additional-properties hideGenerationTimestamp=true,artifactVersion=$(PACKAGE_VERSION),parentArtifactId=lakefs-parent,parentGroupId=io.lakefs,parentVersion=0,groupId=io.lakefs,artifactId='api-client',artifactDescription='lakeFS OpenAPI Java client legacy SDK',artifactUrl=https://lakefs.io,apiPackage=io.lakefs.clients.api,modelPackage=io.lakefs.clients.api.model,mainPackage=io.lakefs.clients.api,developerEmail=services@treeverse.io,developerName='Treeverse lakeFS dev',developerOrganization='lakefs.io',developerOrganizationUrl='https://lakefs.io',licenseName=apache2,licenseUrl=http://www.apache.org/licenses/,scmConnection=scm:git:git@github.com:treeverse/lakeFS.git,scmDeveloperConnection=scm:git:git@github.com:treeverse/lakeFS.git,scmUrl=https://github.com/treeverse/lakeFS \
		-o /mnt/clients/java-legacy

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

.PHONY: clients client-python sdk-python-legacy sdk-python client-java client-java-legacy
clients: client-python client-java client-java-legacy sdk-rust

package-python: package-python-client package-python-sdk

package-python-client: client-python
	$(DOCKER) run --user $(UID_GID) --rm -v $(shell pwd):/mnt -e HOME=/tmp/ -w /mnt/clients/python-legacy $(PYTHON_IMAGE) /bin/bash -c \
		"python -m pip install build --user && python -m build --sdist --wheel --outdir dist/"

package-python-sdk: sdk-python
	$(DOCKER) run --user $(UID_GID) --rm -v $(shell pwd):/mnt -e HOME=/tmp/ -w /mnt/clients/python $(PYTHON_IMAGE) /bin/bash -c \
		"python -m pip install build --user && python -m build --sdist --wheel --outdir dist/"

package-python-wrapper:
	$(DOCKER) run --user $(UID_GID) --rm -v $(shell pwd):/mnt -e HOME=/tmp/ -w /mnt/clients/python-wrapper $(PYTHON_IMAGE) /bin/bash -c \
		"python -m pip install build --user && python -m build --sdist --wheel --outdir dist/"

package: package-python

.PHONY: gen-api
gen-api: docs/assets/js/swagger.yml ## Run the swagger code generator
	$(GOGENERATE) ./pkg/api/apigen ./pkg/auth ./pkg/authentication

.PHONY: gen-code
gen-code: gen-api ## Run the generator for inline commands
	$(GOGENERATE) \
		./pkg/actions \
		./pkg/auth/ \
		./pkg/authentication \
		./pkg/graveler \
		./pkg/graveler/committed \
		./pkg/graveler/sstable \
		./pkg/kv \
		./pkg/permissions \
		./pkg/pyramid

LD_FLAGS := "-X github.com/treeverse/lakefs/pkg/version.Version=$(VERSION)-$(REVISION)"
build: gen docs ## Download dependencies and build the default binary
	$(GOBUILD) -o $(LAKEFS_BINARY_NAME) -ldflags $(LD_FLAGS) -v ./cmd/$(LAKEFS_BINARY_NAME)
	$(GOBUILD) -o $(LAKECTL_BINARY_NAME) -ldflags $(LD_FLAGS) -v ./cmd/$(LAKECTL_BINARY_NAME)

lint: ## Lint code
	$(GOCMD) run github.com/golangci/golangci-lint/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION) run $(GOLANGCI_LINT_FLAGS)
	npx eslint@8.57.0 $(UI_DIR)/src --ext .js,.jsx,.ts,.tsx

esti: ## run esti (system testing)
	$(GOTEST) -v ./esti --args --system-tests

test: test-go test-hadoopfs  ## Run tests for the project

test-go: gen-api			# Run parallelism > num_cores: most of our slow tests are *not* CPU-bound.
	$(GOTEST) -count=1 -coverprofile=cover.out -race -cover -failfast -parallel="$(GOTEST_PARALLELISM)" ./...

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

validate-reference:
	git diff --quiet -- docs/reference/cli.md || (echo "Modification verification failed! docs/reference/cli.md"; false)

validate-client-python: validate-python-sdk-legacy validate-python-sdk

validate-python-sdk-legacy:
	git diff --quiet -- clients/python-legacy || (echo "Modification verification failed! python client"; false)

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
checks-validator: lint validate-fmt validate-proto validate-client-python validate-client-java validate-client-rust validate-reference validate-mockgen validate-permissions-gen

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

publish-scala: ## sbt publish spark client jars to nexus and s3 bucket
	cd clients/spark && sbt assembly && sbt s3Upload && sbt publishSigned
	aws s3 cp --recursive --acl public-read $(CLIENT_JARS_BUCKET) $(CLIENT_JARS_BUCKET) --metadata-directive REPLACE

publish-lakefsfs-test: ## sbt publish spark lakefsfs test jars to s3 bucket
	cd test/lakefsfs && sbt assembly && sbt s3Upload
	aws s3 cp --recursive --acl public-read $(CLIENT_JARS_BUCKET) $(CLIENT_JARS_BUCKET) --metadata-directive REPLACE

help:  ## Show Help menu
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

# helpers
gen: gen-ui gen-api clients gen-docs
