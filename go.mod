module github.com/treeverse/lakefs

go 1.25.5

require (
	cloud.google.com/go v0.121.2 // indirect
	cloud.google.com/go/storage v1.53.0
	github.com/apache/thrift v0.19.0 // indirect
	github.com/cockroachdb/pebble v0.0.0-20230106151110-65ff304d3d7a
	github.com/cubewise-code/go-mime v0.0.0-20200519001935-8c5762b177d8
	github.com/davecgh/go-spew v1.1.2-0.20180830191138-d8f796af33cc
	github.com/deepmap/oapi-codegen v1.5.6
	github.com/dgraph-io/ristretto v0.2.0
	github.com/fsnotify/fsnotify v1.8.0
	github.com/getkin/kin-openapi v0.131.0 // no not upgrade until the read body bug intruduced by v0.132.0 is fixed
	github.com/go-chi/chi/v5 v5.2.2
	github.com/go-openapi/swag v0.23.0
	github.com/go-test/deep v1.1.0
	github.com/gobwas/glob v0.2.3
	github.com/golang/mock v1.7.0-rc.1
	github.com/google/uuid v1.6.0
	github.com/hashicorp/go-multierror v1.1.1
	github.com/hnlq715/golang-lru v0.3.0
	github.com/jamiealquiza/tachymeter v2.0.0+incompatible
	github.com/jedib0t/go-pretty/v6 v6.6.7
	github.com/manifoldco/promptui v0.9.0
	github.com/matoous/go-nanoid/v2 v2.1.0
	github.com/minio/minio-go/v7 v7.0.63
	github.com/mitchellh/go-homedir v1.1.0
	github.com/ory/dockertest/v3 v3.12.0
	github.com/pkg/errors v0.9.1
	github.com/prometheus/client_golang v1.20.5
	github.com/rs/xid v1.5.0
	github.com/schollz/progressbar/v3 v3.13.1
	github.com/sirupsen/logrus v1.9.3
	github.com/spf13/cast v1.7.1 // indirect
	github.com/spf13/cobra v1.10.2
	github.com/spf13/pflag v1.0.10
	github.com/spf13/viper v1.20.1
	github.com/stretchr/testify v1.11.1
	github.com/thanhpk/randstr v1.0.6
	github.com/treeverse/secret-detector v0.0.0-20250429145544-8c655b974a5f
	github.com/tsenart/vegeta/v12 v12.11.1
	github.com/vbauerster/mpb/v5 v5.4.0
	github.com/xitongsys/parquet-go v1.6.2
	github.com/xitongsys/parquet-go-source v0.0.0-20241021075129-b732d2ac9c9b
	golang.org/x/crypto v0.46.0
	golang.org/x/oauth2 v0.31.0
	golang.org/x/term v0.38.0
	google.golang.org/api v0.239.0
	google.golang.org/protobuf v1.36.10
	gopkg.in/natefinch/lumberjack.v2 v2.0.0
	gopkg.in/yaml.v3 v3.0.1
)

require golang.org/x/sync v0.19.0

require (
	cloud.google.com/go/compute/metadata v0.8.0
	github.com/Azure/azure-sdk-for-go/sdk/azcore v1.16.0
	github.com/Azure/azure-sdk-for-go/sdk/azidentity v1.7.0
	github.com/Azure/azure-sdk-for-go/sdk/data/azcosmos v1.2.0
	github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/resources/armsubscriptions v1.3.0
	github.com/Azure/azure-sdk-for-go/sdk/storage/azblob v1.2.0
	github.com/IBM/pgxpoolprometheus v1.1.1
	github.com/Masterminds/sprig/v3 v3.2.3
	github.com/NVIDIA/sortedmap v1.30.0
	github.com/NYTimes/gziphandler v1.1.1
	github.com/Shopify/go-lua v0.0.0-20221004153744-91867de107cf
	github.com/alitto/pond/v2 v2.5.0
	github.com/antonmedv/expr v1.15.3
	github.com/aws/aws-sdk-go-v2 v1.39.2
	github.com/aws/aws-sdk-go-v2/config v1.31.12
	github.com/aws/aws-sdk-go-v2/credentials v1.18.16
	github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue v1.20.11
	github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression v1.8.11
	github.com/aws/aws-sdk-go-v2/feature/s3/manager v1.19.6
	github.com/aws/aws-sdk-go-v2/service/dynamodb v1.50.3
	github.com/aws/aws-sdk-go-v2/service/glue v1.129.1
	github.com/aws/aws-sdk-go-v2/service/s3 v1.88.3
	github.com/aws/aws-sdk-go-v2/service/sts v1.38.6
	github.com/aws/smithy-go v1.23.0
	github.com/benburkert/dns v0.0.0-20190225204957-d356cf78cdfc
	github.com/csimplestring/delta-go v0.0.0-20231105162402-9b93ca02cedf
	github.com/databricks/databricks-sdk-go v0.26.2
	github.com/dgraph-io/badger/v4 v4.7.0
	github.com/elnormous/contenttype v1.0.4
	github.com/georgysavva/scany/v2 v2.0.0
	github.com/go-co-op/gocron v1.35.2
	github.com/go-viper/mapstructure/v2 v2.4.0
	github.com/golang-jwt/jwt/v5 v5.3.0
	github.com/gorilla/securecookie v1.1.2
	github.com/gorilla/sessions v1.4.0
	github.com/hashicorp/go-retryablehttp v0.7.7
	github.com/hashicorp/go-version v1.8.0
	github.com/jackc/pgx/v5 v5.6.0
	github.com/matoous/go-nanoid v1.5.0
	github.com/minio/crc64nvme v1.0.1
	github.com/puzpuzpuz/xsync v1.5.2
	github.com/skratchdot/open-golang v0.0.0-20200116055534-eef842397966
	github.com/treeverse/lakefs/modules/api/factory v0.0.0-20251118224848-d2be664caf31
	github.com/treeverse/lakefs/modules/auth/factory v0.0.0-20251118224848-d2be664caf31
	github.com/treeverse/lakefs/modules/authentication/factory v0.0.0-20251118224848-d2be664caf31
	github.com/treeverse/lakefs/modules/block/factory v0.0.0-20251118224848-d2be664caf31
	github.com/treeverse/lakefs/modules/catalog/factory v0.0.0-20251118224848-d2be664caf31
	github.com/treeverse/lakefs/modules/config/factory v0.0.0-20251118224848-d2be664caf31
	github.com/treeverse/lakefs/modules/gateway/factory v0.0.0-20251118224848-d2be664caf31
	github.com/treeverse/lakefs/modules/license/factory v0.0.0-20251118224848-d2be664caf31
	github.com/treeverse/lakefs/webui v0.0.0-20251118224848-d2be664caf31
	go.uber.org/ratelimit v0.3.0
	gocloud.dev v0.34.1-0.20231122211418-53ccd8db26a1
)

require (
	4d63.com/gocheckcompilerdirectives v1.3.0 // indirect
	4d63.com/gochecknoglobals v0.2.2 // indirect
	cel.dev/expr v0.24.0 // indirect
	cloud.google.com/go/auth v0.16.5 // indirect
	cloud.google.com/go/auth/oauth2adapt v0.2.8 // indirect
	cloud.google.com/go/iam v1.5.2 // indirect
	cloud.google.com/go/monitoring v1.24.2 // indirect
	codeberg.org/chavacava/garif v0.2.0 // indirect
	dario.cat/mergo v1.0.2 // indirect
	dev.gaijin.team/go/exhaustruct/v4 v4.0.0 // indirect
	dev.gaijin.team/go/golib v0.6.0 // indirect
	github.com/4meepo/tagalign v1.4.3 // indirect
	github.com/Abirdcfly/dupword v0.1.7 // indirect
	github.com/AdminBenni/iota-mixing v1.0.0 // indirect
	github.com/AlwxSin/noinlineerr v1.0.5 // indirect
	github.com/Antonboom/errname v1.1.1 // indirect
	github.com/Antonboom/nilnil v1.1.1 // indirect
	github.com/Antonboom/testifylint v1.6.4 // indirect
	github.com/Azure/azure-sdk-for-go v68.0.0+incompatible // indirect
	github.com/Azure/azure-sdk-for-go/sdk/internal v1.10.0 // indirect
	github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/resources/armresources v1.2.0 // indirect
	github.com/Azure/go-autorest v14.2.0+incompatible // indirect
	github.com/Azure/go-autorest/autorest/to v0.4.0 // indirect
	github.com/AzureAD/microsoft-authentication-library-for-go v1.2.2 // indirect
	github.com/BurntSushi/toml v1.5.0 // indirect
	github.com/Djarvur/go-err113 v0.1.1 // indirect
	github.com/GoogleCloudPlatform/opentelemetry-operations-go/detectors/gcp v1.29.0 // indirect
	github.com/GoogleCloudPlatform/opentelemetry-operations-go/exporter/metric v0.51.0 // indirect
	github.com/GoogleCloudPlatform/opentelemetry-operations-go/internal/resourcemapping v0.51.0 // indirect
	github.com/Masterminds/goutils v1.1.1 // indirect
	github.com/Masterminds/semver/v3 v3.4.0 // indirect
	github.com/MirrexOne/unqueryvet v1.3.0 // indirect
	github.com/NVIDIA/cstruct v1.30.0 // indirect
	github.com/OpenPeeDeeP/depguard/v2 v2.2.1 // indirect
	github.com/ahmetb/go-linq/v3 v3.2.0 // indirect
	github.com/alecthomas/chroma/v2 v2.20.0 // indirect
	github.com/alecthomas/go-check-sumtype v0.3.1 // indirect
	github.com/alexkohler/nakedret/v2 v2.0.6 // indirect
	github.com/alexkohler/prealloc v1.0.0 // indirect
	github.com/alfatraining/structtag v1.0.0 // indirect
	github.com/alingse/asasalint v0.0.11 // indirect
	github.com/alingse/nilnesserr v0.2.0 // indirect
	github.com/apache/arrow/go/arrow v0.0.0-20200730104253-651201b0f516 // indirect
	github.com/ashanbrown/forbidigo/v2 v2.3.0 // indirect
	github.com/ashanbrown/makezero/v2 v2.1.0 // indirect
	github.com/aws/aws-sdk-go v1.48.11 // indirect
	github.com/aws/aws-sdk-go-v2/aws/protocol/eventstream v1.7.1 // indirect
	github.com/aws/aws-sdk-go-v2/feature/ec2/imds v1.18.9 // indirect
	github.com/aws/aws-sdk-go-v2/internal/configsources v1.4.9 // indirect
	github.com/aws/aws-sdk-go-v2/internal/endpoints/v2 v2.7.9 // indirect
	github.com/aws/aws-sdk-go-v2/internal/ini v1.8.3 // indirect
	github.com/aws/aws-sdk-go-v2/internal/v4a v1.4.9 // indirect
	github.com/aws/aws-sdk-go-v2/service/dynamodbstreams v1.30.4 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/accept-encoding v1.13.1 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/checksum v1.8.9 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/endpoint-discovery v1.11.7 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/presigned-url v1.13.9 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/s3shared v1.19.9 // indirect
	github.com/aws/aws-sdk-go-v2/service/sso v1.29.6 // indirect
	github.com/aws/aws-sdk-go-v2/service/ssooidc v1.35.1 // indirect
	github.com/aymanbagabas/go-osc52/v2 v2.0.1 // indirect
	github.com/barweiss/go-tuple v1.1.2 // indirect
	github.com/benbjohnson/clock v1.3.0 // indirect
	github.com/bkielbasa/cyclop v1.2.3 // indirect
	github.com/blizzy78/varnamelen v0.8.0 // indirect
	github.com/bombsimon/wsl/v4 v4.7.0 // indirect
	github.com/bombsimon/wsl/v5 v5.3.0 // indirect
	github.com/breml/bidichk v0.3.3 // indirect
	github.com/breml/errchkjson v0.4.1 // indirect
	github.com/butuzov/ireturn v0.4.0 // indirect
	github.com/butuzov/mirror v1.3.0 // indirect
	github.com/catenacyber/perfsprint v0.10.1 // indirect
	github.com/ccojocar/zxcvbn-go v1.0.4 // indirect
	github.com/charithe/durationcheck v0.0.11 // indirect
	github.com/charmbracelet/colorprofile v0.2.3-0.20250311203215-f60798e515dc // indirect
	github.com/charmbracelet/lipgloss v1.1.0 // indirect
	github.com/charmbracelet/x/ansi v0.8.0 // indirect
	github.com/charmbracelet/x/cellbuf v0.0.13-0.20250311204145-2c3ea96c31dd // indirect
	github.com/charmbracelet/x/term v0.2.1 // indirect
	github.com/ckaznocha/intrange v0.3.1 // indirect
	github.com/cncf/xds/go v0.0.0-20250501225837-2ac532fd4443 // indirect
	github.com/curioswitch/go-reassign v0.3.0 // indirect
	github.com/daixiang0/gci v0.13.7 // indirect
	github.com/dave/dst v0.27.3 // indirect
	github.com/deckarep/golang-set/v2 v2.8.0 // indirect
	github.com/denis-tingaikin/go-header v0.5.0 // indirect
	github.com/dgraph-io/ristretto/v2 v2.2.0 // indirect
	github.com/dlclark/regexp2 v1.11.5 // indirect
	github.com/envoyproxy/go-control-plane/envoy v1.32.4 // indirect
	github.com/envoyproxy/protoc-gen-validate v1.2.1 // indirect
	github.com/ettle/strcase v0.2.0 // indirect
	github.com/fatih/color v1.18.0 // indirect
	github.com/fatih/structtag v1.2.0 // indirect
	github.com/felixge/httpsnoop v1.0.4 // indirect
	github.com/firefart/nonamedreturns v1.0.6 // indirect
	github.com/fraugster/parquet-go v0.12.0 // indirect
	github.com/fzipp/gocyclo v0.6.0 // indirect
	github.com/getsentry/sentry-go v0.27.0 // indirect
	github.com/ghostiam/protogetter v0.3.17 // indirect
	github.com/go-critic/go-critic v0.14.2 // indirect
	github.com/go-jose/go-jose/v4 v4.1.1 // indirect
	github.com/go-logr/logr v1.4.3 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/go-toolsmith/astcast v1.1.0 // indirect
	github.com/go-toolsmith/astcopy v1.1.0 // indirect
	github.com/go-toolsmith/astequal v1.2.0 // indirect
	github.com/go-toolsmith/astfmt v1.1.0 // indirect
	github.com/go-toolsmith/astp v1.1.0 // indirect
	github.com/go-toolsmith/strparse v1.1.0 // indirect
	github.com/go-toolsmith/typep v1.1.0 // indirect
	github.com/go-xmlfmt/xmlfmt v1.1.3 // indirect
	github.com/godoc-lint/godoc-lint v0.10.2 // indirect
	github.com/gofrs/flock v0.13.0 // indirect
	github.com/golangci/asciicheck v0.5.0 // indirect
	github.com/golangci/dupl v0.0.0-20250308024227-f665c8d69b32 // indirect
	github.com/golangci/go-printf-func-name v0.1.1 // indirect
	github.com/golangci/gofmt v0.0.0-20250106114630-d62b90e6713d // indirect
	github.com/golangci/golangci-lint/v2 v2.7.2 // indirect
	github.com/golangci/golines v0.0.0-20250217134842-442fd0091d95 // indirect
	github.com/golangci/misspell v0.7.0 // indirect
	github.com/golangci/plugin-module-register v0.1.2 // indirect
	github.com/golangci/revgrep v0.8.0 // indirect
	github.com/golangci/swaggoswag v0.0.0-20250504205917-77f2aca3143e // indirect
	github.com/golangci/unconvert v0.0.0-20250410112200-a129a6e6413e // indirect
	github.com/google/flatbuffers v25.2.10+incompatible // indirect
	github.com/google/go-cmp v0.7.0 // indirect
	github.com/google/go-querystring v1.1.0 // indirect
	github.com/google/s2a-go v0.1.9 // indirect
	github.com/google/wire v0.5.0 // indirect
	github.com/googleapis/enterprise-certificate-proxy v0.3.6 // indirect
	github.com/gordonklaus/ineffassign v0.2.0 // indirect
	github.com/gostaticanalysis/analysisutil v0.7.1 // indirect
	github.com/gostaticanalysis/comment v1.5.0 // indirect
	github.com/gostaticanalysis/forcetypeassert v0.2.0 // indirect
	github.com/gostaticanalysis/nilerr v0.1.2 // indirect
	github.com/hashicorp/go-cleanhttp v0.5.2 // indirect
	github.com/hashicorp/go-immutable-radix/v2 v2.1.0 // indirect
	github.com/hashicorp/golang-lru/v2 v2.0.7 // indirect
	github.com/hexops/gotextdiff v1.0.3 // indirect
	github.com/huandu/xstrings v1.3.3 // indirect
	github.com/imdario/mergo v0.3.16 // indirect
	github.com/inhies/go-bytesize v0.0.0-20220417184213-4913239db9cf // indirect
	github.com/jackc/puddle/v2 v2.2.1 // indirect
	github.com/jgautheron/goconst v1.8.2 // indirect
	github.com/jingyugao/rowserrcheck v1.1.1 // indirect
	github.com/jjti/go-spancheck v0.6.5 // indirect
	github.com/julz/importas v0.2.0 // indirect
	github.com/karamaru-alpha/copyloopvar v1.2.2 // indirect
	github.com/kisielk/errcheck v1.9.0 // indirect
	github.com/kkHAIKE/contextcheck v1.1.6 // indirect
	github.com/klauspost/cpuid/v2 v2.2.9 // indirect
	github.com/kulti/thelper v0.7.1 // indirect
	github.com/kunwardeep/paralleltest v1.0.15 // indirect
	github.com/kylelemons/godebug v1.1.0 // indirect
	github.com/labstack/echo/v4 v4.11.4 // indirect
	github.com/lasiar/canonicalheader v1.1.2 // indirect
	github.com/ldez/exptostd v0.4.5 // indirect
	github.com/ldez/gomoddirectives v0.7.1 // indirect
	github.com/ldez/grignotin v0.10.1 // indirect
	github.com/ldez/tagliatelle v0.7.2 // indirect
	github.com/ldez/usetesting v0.5.0 // indirect
	github.com/leonklingele/grouper v1.1.2 // indirect
	github.com/lucasb-eyer/go-colorful v1.2.0 // indirect
	github.com/macabu/inamedparam v0.2.0 // indirect
	github.com/manuelarte/embeddedstructfieldcheck v0.4.0 // indirect
	github.com/manuelarte/funcorder v0.5.0 // indirect
	github.com/maratori/testableexamples v1.0.1 // indirect
	github.com/maratori/testpackage v1.1.2 // indirect
	github.com/matoous/godox v1.1.0 // indirect
	github.com/mattn/go-colorable v0.1.14 // indirect
	github.com/mattn/go-isatty v0.0.20 // indirect
	github.com/mattn/go-shellwords v1.0.12 // indirect
	github.com/mgechev/revive v1.13.0 // indirect
	github.com/mitchellh/copystructure v1.0.0 // indirect
	github.com/mitchellh/hashstructure/v2 v2.0.2 // indirect
	github.com/mitchellh/reflectwalk v1.0.0 // indirect
	github.com/moby/docker-image-spec v1.3.1 // indirect
	github.com/moby/sys/user v0.4.0 // indirect
	github.com/mohae/deepcopy v0.0.0-20170929034955-c48cc78d4826 // indirect
	github.com/moricho/tparallel v0.3.2 // indirect
	github.com/muesli/termenv v0.16.0 // indirect
	github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822 // indirect
	github.com/nakabonne/nestif v0.3.1 // indirect
	github.com/nishanths/exhaustive v0.12.0 // indirect
	github.com/nishanths/predeclared v0.2.2 // indirect
	github.com/nunnatsa/ginkgolinter v0.21.2 // indirect
	github.com/oasdiff/yaml v0.0.0-20250309154309-f31be36b4037 // indirect
	github.com/oasdiff/yaml3 v0.0.0-20250309153720-d2182401db90 // indirect
	github.com/octarinesec/secret-detector v1.0.11 // indirect
	github.com/pelletier/go-toml/v2 v2.2.4 // indirect
	github.com/perimeterx/marshmallow v1.1.5 // indirect
	github.com/pierrec/lz4/v4 v4.1.18 // indirect
	github.com/pkg/browser v0.0.0-20240102092130-5ac0b6a4141c // indirect
	github.com/planetscale/vtprotobuf v0.6.1-0.20240319094008-0393e58bdf10 // indirect
	github.com/polyfloyd/go-errorlint v1.8.0 // indirect
	github.com/quasilyte/go-ruleguard v0.4.5 // indirect
	github.com/quasilyte/go-ruleguard/dsl v0.3.23 // indirect
	github.com/quasilyte/gogrep v0.5.0 // indirect
	github.com/quasilyte/regex/syntax v0.0.0-20210819130434-b3f0c404a727 // indirect
	github.com/quasilyte/stdinfo v0.0.0-20220114132959-f7386bf02567 // indirect
	github.com/raeperd/recvcheck v0.2.0 // indirect
	github.com/repeale/fp-go v0.11.1 // indirect
	github.com/robfig/cron/v3 v3.0.1 // indirect
	github.com/rogpeppe/go-internal v1.14.1 // indirect
	github.com/rotisserie/eris v0.5.4 // indirect
	github.com/rs/dnscache v0.0.0-20211102005908-e0241e321417 // indirect
	github.com/ryancurrah/gomodguard v1.4.1 // indirect
	github.com/ryanrolds/sqlclosecheck v0.5.1 // indirect
	github.com/sagikazarmark/locafero v0.7.0 // indirect
	github.com/samber/mo v1.11.0 // indirect
	github.com/sanposhiho/wastedassign/v2 v2.1.0 // indirect
	github.com/santhosh-tekuri/jsonschema/v6 v6.0.2 // indirect
	github.com/sashamelentyev/interfacebloat v1.1.0 // indirect
	github.com/sashamelentyev/usestdlibvars v1.29.0 // indirect
	github.com/securego/gosec/v2 v2.22.11-0.20251204091113-daccba6b93d7 // indirect
	github.com/shopspring/decimal v1.3.1 // indirect
	github.com/sivchari/containedctx v1.0.3 // indirect
	github.com/sonatard/noctx v0.4.0 // indirect
	github.com/sourcegraph/conc v0.3.0 // indirect
	github.com/sourcegraph/go-diff v0.7.0 // indirect
	github.com/spiffe/go-spiffe/v2 v2.5.0 // indirect
	github.com/ssgreg/nlreturn/v2 v2.2.1 // indirect
	github.com/stbenjam/no-sprintf-host-port v0.3.1 // indirect
	github.com/stretchr/objx v0.5.2 // indirect
	github.com/tetafro/godot v1.5.4 // indirect
	github.com/timakin/bodyclose v0.0.0-20241222091800-1db5c5ca4d67 // indirect
	github.com/timonwong/loggercheck v0.11.0 // indirect
	github.com/tomarrell/wrapcheck/v2 v2.12.0 // indirect
	github.com/tommy-muehle/go-mnd/v2 v2.5.1 // indirect
	github.com/ultraware/funlen v0.2.0 // indirect
	github.com/ultraware/whitespace v0.2.0 // indirect
	github.com/ulule/deepcopier v0.0.0-20200430083143-45decc6639b6 // indirect
	github.com/uudashr/gocognit v1.2.0 // indirect
	github.com/uudashr/iface v1.4.1 // indirect
	github.com/xen0n/gosmopolitan v1.3.0 // indirect
	github.com/xhit/go-str2duration/v2 v2.1.0 // indirect
	github.com/xo/terminfo v0.0.0-20220910002029-abceb7e1c41e // indirect
	github.com/yagipy/maintidx v1.0.0 // indirect
	github.com/yeya24/promlinter v0.3.0 // indirect
	github.com/ykadowak/zerologlint v0.1.5 // indirect
	github.com/zeebo/errs v1.4.0 // indirect
	gitlab.com/bosi/decorder v0.4.2 // indirect
	go-simpler.org/musttag v0.14.0 // indirect
	go-simpler.org/sloglint v0.11.1 // indirect
	go.augendre.info/arangolint v0.3.1 // indirect
	go.augendre.info/fatcontext v0.9.0 // indirect
	go.opentelemetry.io/auto/sdk v1.1.0 // indirect
	go.opentelemetry.io/contrib/detectors/gcp v1.36.0 // indirect
	go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc v0.61.0 // indirect
	go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp v0.61.0 // indirect
	go.opentelemetry.io/otel v1.37.0 // indirect
	go.opentelemetry.io/otel/metric v1.37.0 // indirect
	go.opentelemetry.io/otel/sdk v1.37.0 // indirect
	go.opentelemetry.io/otel/sdk/metric v1.37.0 // indirect
	go.opentelemetry.io/otel/trace v1.37.0 // indirect
	go.uber.org/automaxprocs v1.6.0 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	go.uber.org/zap v1.27.0 // indirect
	go.yaml.in/yaml/v3 v3.0.4 // indirect
	golang.org/x/exp/typeparams v0.0.0-20251023183803-a4bb9ffd2546 // indirect
	golang.org/x/time v0.12.0 // indirect
	google.golang.org/genproto/googleapis/api v0.0.0-20250707201910-8d1bb00bc6a7 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20250818200422-3122310a409c // indirect
	honnef.co/go/tools v0.6.1 // indirect
	mvdan.cc/gofumpt v0.9.2 // indirect
	mvdan.cc/unparam v0.0.0-20251027182757-5beb8c8f8f15 // indirect
)

require (
	github.com/Azure/go-ansiterm v0.0.0-20250102033503-faa5f7b0171c // indirect
	github.com/DataDog/zstd v1.5.2 // indirect
	github.com/Microsoft/go-winio v0.6.2 // indirect
	github.com/Nvveen/Gotty v0.0.0-20120604004816-cd527374f1e5 // indirect
	github.com/VividCortex/ewma v1.1.1 // indirect
	github.com/acarl005/stripansi v0.0.0-20180116102854-5a71ef0e047d // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/cenkalti/backoff/v4 v4.3.0
	github.com/cespare/xxhash/v2 v2.3.0
	github.com/chzyer/readline v1.5.1 // indirect
	github.com/cockroachdb/errors v1.11.3 // indirect
	github.com/cockroachdb/logtags v0.0.0-20230118201751-21c54148d20b // indirect
	github.com/cockroachdb/redact v1.1.5 // indirect
	github.com/containerd/continuity v0.4.5 // indirect
	github.com/docker/cli v28.3.3+incompatible // indirect
	github.com/docker/docker v28.3.3+incompatible // indirect
	github.com/docker/go-connections v0.6.0 // indirect
	github.com/docker/go-units v0.5.0 // indirect
	github.com/dustin/go-humanize v1.0.1 // indirect
	github.com/go-openapi/jsonpointer v0.21.0 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang/groupcache v0.0.0-20210331224755-41bb18bfe9da // indirect
	github.com/golang/snappy v0.0.4 // indirect
	github.com/google/shlex v0.0.0-20191202100458-e7afc7fbc510 // indirect
	github.com/googleapis/gax-go/v2 v2.15.0 // indirect
	github.com/hashicorp/errwrap v1.1.0 // indirect
	github.com/inconshreveable/mousetrap v1.1.0 // indirect
	github.com/influxdata/tdigest v0.0.1 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20221227161230-091c0ba34f0a // indirect
	github.com/jmespath/go-jmespath v0.4.0 // indirect
	github.com/josharian/intern v1.0.0 // indirect
	github.com/json-iterator/go v1.1.12 // indirect
	github.com/klauspost/compress v1.18.0 // indirect
	github.com/kr/pretty v0.3.1 // indirect
	github.com/kr/text v0.2.0 // indirect
	github.com/mailru/easyjson v0.7.7 // indirect
	github.com/mattn/go-runewidth v0.0.16 // indirect
	github.com/minio/md5-simd v1.1.2 // indirect
	github.com/minio/sha256-simd v1.0.1 // indirect
	github.com/mitchellh/colorstring v0.0.0-20190213212951-d06e56a500db // indirect
	github.com/moby/term v0.5.2 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
	github.com/opencontainers/go-digest v1.0.0 // indirect
	github.com/opencontainers/image-spec v1.1.1 // indirect
	github.com/opencontainers/runc v1.3.3 // indirect
	github.com/pmezard/go-difflib v1.0.1-0.20181226105442-5d4384ee4fb2 // indirect
	github.com/prometheus/client_model v0.6.1 // indirect
	github.com/prometheus/common v0.55.0 // indirect
	github.com/prometheus/procfs v0.15.1 // indirect
	github.com/rivo/uniseg v0.4.7 // indirect
	github.com/spf13/afero v1.15.0 // indirect
	github.com/subosito/gotenv v1.6.0 // indirect
	github.com/xeipuuv/gojsonpointer v0.0.0-20190905194746-02993c407bfb // indirect
	github.com/xeipuuv/gojsonreference v0.0.0-20180127040603-bd5ef7bd5415 // indirect
	github.com/xeipuuv/gojsonschema v1.2.0 // indirect
	go.opencensus.io v0.24.0 // indirect
	go.uber.org/atomic v1.11.0
	golang.org/x/exp v0.0.0-20240909161429-701f63a606c0 // indirect
	golang.org/x/mod v0.30.0 // indirect
	golang.org/x/net v0.48.0 // indirect
	golang.org/x/sys v0.39.0 // indirect
	golang.org/x/text v0.32.0 // indirect
	golang.org/x/tools v0.39.0
	golang.org/x/xerrors v0.0.0-20240903120638-7835f813f4da // indirect
	google.golang.org/genproto v0.0.0-20250603155806-513f23925822 // indirect
	google.golang.org/grpc v1.75.0 // indirect
	gopkg.in/ini.v1 v1.67.0 // indirect
)

replace github.com/csimplestring/delta-go => github.com/treeverse/delta-go v0.0.0-20250325160917-8c0ebb032f43

tool github.com/golangci/golangci-lint/v2/cmd/golangci-lint
