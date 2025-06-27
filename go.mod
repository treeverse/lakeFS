module github.com/treeverse/lakefs

go 1.24.4

require (
	cloud.google.com/go v0.120.0 // indirect
	cloud.google.com/go/storage v1.50.0
	github.com/apache/thrift v0.19.0
	github.com/cockroachdb/pebble v0.0.0-20230106151110-65ff304d3d7a
	github.com/cubewise-code/go-mime v0.0.0-20200519001935-8c5762b177d8
	github.com/davecgh/go-spew v1.1.2-0.20180830191138-d8f796af33cc
	github.com/deepmap/oapi-codegen v1.5.6
	github.com/dgraph-io/ristretto v0.2.0
	github.com/fsnotify/fsnotify v1.8.0
	github.com/getkin/kin-openapi v0.132.0
	github.com/go-chi/chi/v5 v5.2.2
	github.com/go-openapi/swag v0.23.0
	github.com/go-test/deep v1.1.0
	github.com/gobwas/glob v0.2.3
	github.com/golang/mock v1.7.0-rc.1
	github.com/golang/protobuf v1.5.4 // indirect
	github.com/google/uuid v1.6.0
	github.com/hashicorp/go-multierror v1.1.1
	github.com/hnlq715/golang-lru v0.3.0
	github.com/jamiealquiza/tachymeter v2.0.0+incompatible
	github.com/jedib0t/go-pretty/v6 v6.6.7
	github.com/manifoldco/promptui v0.9.0
	github.com/matoous/go-nanoid/v2 v2.0.0
	github.com/minio/minio-go/v7 v7.0.63
	github.com/mitchellh/go-homedir v1.1.0
	github.com/mitchellh/mapstructure v1.5.0 // indirect
	github.com/ory/dockertest/v3 v3.10.0
	github.com/pkg/errors v0.9.1
	github.com/prometheus/client_golang v1.17.0
	github.com/rs/xid v1.5.0
	github.com/schollz/progressbar/v3 v3.13.1
	github.com/sirupsen/logrus v1.9.3
	github.com/spf13/cast v1.7.1 // indirect
	github.com/spf13/cobra v1.9.1
	github.com/spf13/pflag v1.0.6
	github.com/spf13/viper v1.20.1
	github.com/stretchr/testify v1.10.0
	github.com/thanhpk/randstr v1.0.6
	github.com/treeverse/secret-detector v0.0.0-20250429145544-8c655b974a5f
	github.com/tsenart/vegeta/v12 v12.11.1
	github.com/vbauerster/mpb/v5 v5.4.0
	github.com/xitongsys/parquet-go v1.6.2
	github.com/xitongsys/parquet-go-source v0.0.0-20241021075129-b732d2ac9c9b
	golang.org/x/crypto v0.39.0
	golang.org/x/oauth2 v0.30.0
	golang.org/x/term v0.32.0
	google.golang.org/api v0.234.0
	google.golang.org/protobuf v1.36.6
	gopkg.in/natefinch/lumberjack.v2 v2.0.0
	gopkg.in/yaml.v3 v3.0.1
)

require golang.org/x/sync v0.15.0

require (
	cloud.google.com/go/compute/metadata v0.7.0
	github.com/Azure/azure-sdk-for-go/sdk/azcore v1.16.0
	github.com/Azure/azure-sdk-for-go/sdk/azidentity v1.7.0
	github.com/Azure/azure-sdk-for-go/sdk/data/azcosmos v1.2.0
	github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/resources/armsubscriptions v1.3.0
	github.com/Azure/azure-sdk-for-go/sdk/storage/azblob v1.2.0
	github.com/IBM/pgxpoolprometheus v1.1.1
	github.com/Masterminds/sprig/v3 v3.2.3
	github.com/NYTimes/gziphandler v1.1.1
	github.com/Shopify/go-lua v0.0.0-20221004153744-91867de107cf
	github.com/alitto/pond v1.8.3
	github.com/antonmedv/expr v1.15.3
	github.com/aws/aws-sdk-go-v2 v1.23.5
	github.com/aws/aws-sdk-go-v2/config v1.25.11
	github.com/aws/aws-sdk-go-v2/credentials v1.16.9
	github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue v1.12.7
	github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression v1.6.7
	github.com/aws/aws-sdk-go-v2/feature/s3/manager v1.15.4
	github.com/aws/aws-sdk-go-v2/service/dynamodb v1.26.1
	github.com/aws/aws-sdk-go-v2/service/glue v1.71.1
	github.com/aws/aws-sdk-go-v2/service/s3 v1.47.2
	github.com/aws/aws-sdk-go-v2/service/sts v1.26.2
	github.com/aws/smithy-go v1.18.1
	github.com/benburkert/dns v0.0.0-20190225204957-d356cf78cdfc
	github.com/csimplestring/delta-go v0.0.0-20231105162402-9b93ca02cedf
	github.com/databricks/databricks-sdk-go v0.26.2
	github.com/dgraph-io/badger/v4 v4.7.0
	github.com/georgysavva/scany/v2 v2.0.0
	github.com/go-co-op/gocron v1.35.2
	github.com/go-viper/mapstructure/v2 v2.3.0
	github.com/golang-jwt/jwt/v5 v5.2.2
	github.com/gorilla/securecookie v1.1.1
	github.com/gorilla/sessions v1.2.1
	github.com/hashicorp/go-retryablehttp v0.7.7
	github.com/hashicorp/go-version v1.6.0
	github.com/jackc/pgx/v5 v5.6.0
	github.com/matoous/go-nanoid v1.5.0
	github.com/minio/crc64nvme v1.0.1
	github.com/puzpuzpuz/xsync v1.5.2
	github.com/treeverse/lakefs/modules/api/factory v0.0.0-20250627163148-5c5825612bf4
	github.com/treeverse/lakefs/modules/auth/factory v0.0.0-20250627163148-5c5825612bf4
	github.com/treeverse/lakefs/modules/authentication/factory v0.0.0-20250627163148-5c5825612bf4
	github.com/treeverse/lakefs/modules/block/factory v0.0.0-20250627163148-5c5825612bf4
	github.com/treeverse/lakefs/modules/config/factory v0.0.0-20250627163148-5c5825612bf4
	github.com/treeverse/lakefs/modules/license/factory v0.0.0-20250627163148-5c5825612bf4
	github.com/treeverse/lakefs/webui v0.0.0-20250627163148-5c5825612bf4
	go.uber.org/ratelimit v0.3.0
	gocloud.dev v0.34.1-0.20231122211418-53ccd8db26a1
)

require (
	cel.dev/expr v0.20.0 // indirect
	cloud.google.com/go/auth v0.16.1 // indirect
	cloud.google.com/go/auth/oauth2adapt v0.2.8 // indirect
	cloud.google.com/go/iam v1.5.2 // indirect
	cloud.google.com/go/monitoring v1.24.2 // indirect
	dario.cat/mergo v1.0.0 // indirect
	github.com/Azure/azure-sdk-for-go v68.0.0+incompatible // indirect
	github.com/Azure/azure-sdk-for-go/sdk/internal v1.10.0 // indirect
	github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/resources/armresources v1.2.0 // indirect
	github.com/Azure/go-autorest v14.2.0+incompatible // indirect
	github.com/Azure/go-autorest/autorest/to v0.4.0 // indirect
	github.com/AzureAD/microsoft-authentication-library-for-go v1.2.2 // indirect
	github.com/BurntSushi/toml v1.3.2 // indirect
	github.com/GoogleCloudPlatform/opentelemetry-operations-go/detectors/gcp v1.27.0 // indirect
	github.com/GoogleCloudPlatform/opentelemetry-operations-go/exporter/metric v0.50.0 // indirect
	github.com/GoogleCloudPlatform/opentelemetry-operations-go/internal/resourcemapping v0.50.0 // indirect
	github.com/Masterminds/goutils v1.1.1 // indirect
	github.com/Masterminds/semver/v3 v3.2.0 // indirect
	github.com/ahmetb/go-linq/v3 v3.2.0 // indirect
	github.com/apache/arrow/go/arrow v0.0.0-20200730104253-651201b0f516 // indirect
	github.com/aws/aws-sdk-go v1.48.11 // indirect
	github.com/aws/aws-sdk-go-v2/aws/protocol/eventstream v1.5.3 // indirect
	github.com/aws/aws-sdk-go-v2/feature/ec2/imds v1.14.9 // indirect
	github.com/aws/aws-sdk-go-v2/internal/configsources v1.2.8 // indirect
	github.com/aws/aws-sdk-go-v2/internal/endpoints/v2 v2.5.8 // indirect
	github.com/aws/aws-sdk-go-v2/internal/ini v1.7.1 // indirect
	github.com/aws/aws-sdk-go-v2/internal/v4a v1.2.8 // indirect
	github.com/aws/aws-sdk-go-v2/service/dynamodbstreams v1.18.1 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/accept-encoding v1.10.3 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/checksum v1.2.8 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/endpoint-discovery v1.8.7 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/presigned-url v1.10.8 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/s3shared v1.16.8 // indirect
	github.com/aws/aws-sdk-go-v2/service/sso v1.18.2 // indirect
	github.com/aws/aws-sdk-go-v2/service/ssooidc v1.21.2 // indirect
	github.com/barweiss/go-tuple v1.1.2 // indirect
	github.com/benbjohnson/clock v1.3.0 // indirect
	github.com/cncf/xds/go v0.0.0-20250121191232-2f005788dc42 // indirect
	github.com/deckarep/golang-set/v2 v2.6.0 // indirect
	github.com/dgraph-io/ristretto/v2 v2.2.0 // indirect
	github.com/envoyproxy/go-control-plane/envoy v1.32.4 // indirect
	github.com/envoyproxy/protoc-gen-validate v1.2.1 // indirect
	github.com/felixge/httpsnoop v1.0.4 // indirect
	github.com/fraugster/parquet-go v0.12.0 // indirect
	github.com/getsentry/sentry-go v0.27.0 // indirect
	github.com/go-jose/go-jose/v4 v4.0.5 // indirect
	github.com/go-logr/logr v1.4.3 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/google/flatbuffers v25.2.10+incompatible // indirect
	github.com/google/go-querystring v1.1.0 // indirect
	github.com/google/s2a-go v0.1.9 // indirect
	github.com/google/wire v0.5.0 // indirect
	github.com/googleapis/enterprise-certificate-proxy v0.3.6 // indirect
	github.com/hashicorp/go-cleanhttp v0.5.2 // indirect
	github.com/huandu/xstrings v1.3.3 // indirect
	github.com/imdario/mergo v0.3.16 // indirect
	github.com/inhies/go-bytesize v0.0.0-20220417184213-4913239db9cf // indirect
	github.com/jackc/puddle/v2 v2.2.1 // indirect
	github.com/klauspost/cpuid/v2 v2.2.9 // indirect
	github.com/kylelemons/godebug v1.1.0 // indirect
	github.com/labstack/echo/v4 v4.11.4 // indirect
	github.com/mattn/go-shellwords v1.0.12 // indirect
	github.com/mitchellh/copystructure v1.0.0 // indirect
	github.com/mitchellh/hashstructure/v2 v2.0.2 // indirect
	github.com/mitchellh/reflectwalk v1.0.0 // indirect
	github.com/mohae/deepcopy v0.0.0-20170929034955-c48cc78d4826 // indirect
	github.com/oasdiff/yaml v0.0.0-20250309154309-f31be36b4037 // indirect
	github.com/oasdiff/yaml3 v0.0.0-20250309153720-d2182401db90 // indirect
	github.com/octarinesec/secret-detector v1.0.11 // indirect
	github.com/pelletier/go-toml/v2 v2.2.3 // indirect
	github.com/perimeterx/marshmallow v1.1.5 // indirect
	github.com/pierrec/lz4/v4 v4.1.18 // indirect
	github.com/pkg/browser v0.0.0-20240102092130-5ac0b6a4141c // indirect
	github.com/planetscale/vtprotobuf v0.6.1-0.20240319094008-0393e58bdf10 // indirect
	github.com/repeale/fp-go v0.11.1 // indirect
	github.com/robfig/cron/v3 v3.0.1 // indirect
	github.com/rogpeppe/go-internal v1.13.1 // indirect
	github.com/rotisserie/eris v0.5.4 // indirect
	github.com/rs/dnscache v0.0.0-20211102005908-e0241e321417 // indirect
	github.com/sagikazarmark/locafero v0.7.0 // indirect
	github.com/samber/mo v1.11.0 // indirect
	github.com/shopspring/decimal v1.3.1 // indirect
	github.com/sourcegraph/conc v0.3.0 // indirect
	github.com/spiffe/go-spiffe/v2 v2.5.0 // indirect
	github.com/ulule/deepcopier v0.0.0-20200430083143-45decc6639b6 // indirect
	github.com/xhit/go-str2duration/v2 v2.1.0 // indirect
	github.com/zeebo/errs v1.4.0 // indirect
	go.opentelemetry.io/auto/sdk v1.1.0 // indirect
	go.opentelemetry.io/contrib/detectors/gcp v1.35.0 // indirect
	go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc v0.60.0 // indirect
	go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp v0.60.0 // indirect
	go.opentelemetry.io/otel v1.36.0 // indirect
	go.opentelemetry.io/otel/metric v1.36.0 // indirect
	go.opentelemetry.io/otel/sdk v1.35.0 // indirect
	go.opentelemetry.io/otel/sdk/metric v1.35.0 // indirect
	go.opentelemetry.io/otel/trace v1.36.0 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	golang.org/x/time v0.11.0 // indirect
	gonum.org/v1/gonum v0.9.3 // indirect
	google.golang.org/genproto/googleapis/api v0.0.0-20250505200425-f936aa4a68b2 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20250512202823-5a2f75b736a9 // indirect
	gotest.tools/v3 v3.5.1 // indirect
)

require (
	github.com/Azure/go-ansiterm v0.0.0-20230124172434-306776ec8161 // indirect
	github.com/DataDog/zstd v1.5.2 // indirect
	github.com/Microsoft/go-winio v0.6.2 // indirect
	github.com/Nvveen/Gotty v0.0.0-20120604004816-cd527374f1e5 // indirect
	github.com/VividCortex/ewma v1.1.1 // indirect
	github.com/acarl005/stripansi v0.0.0-20180116102854-5a71ef0e047d // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/cenkalti/backoff/v4 v4.2.1
	github.com/cespare/xxhash/v2 v2.3.0
	github.com/chzyer/readline v1.5.1 // indirect
	github.com/cockroachdb/errors v1.11.3 // indirect
	github.com/cockroachdb/logtags v0.0.0-20230118201751-21c54148d20b // indirect
	github.com/cockroachdb/redact v1.1.5 // indirect
	github.com/containerd/continuity v0.4.3 // indirect
	github.com/docker/cli v25.0.1+incompatible // indirect
	github.com/docker/docker v25.0.6+incompatible // indirect
	github.com/docker/go-connections v0.5.0 // indirect
	github.com/docker/go-units v0.5.0 // indirect
	github.com/dustin/go-humanize v1.0.1 // indirect
	github.com/go-openapi/jsonpointer v0.21.0 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang/groupcache v0.0.0-20210331224755-41bb18bfe9da // indirect
	github.com/golang/snappy v0.0.4 // indirect
	github.com/google/shlex v0.0.0-20191202100458-e7afc7fbc510 // indirect
	github.com/googleapis/gax-go/v2 v2.14.2 // indirect
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
	github.com/matttproud/golang_protobuf_extensions v1.0.4 // indirect
	github.com/minio/md5-simd v1.1.2 // indirect
	github.com/minio/sha256-simd v1.0.1 // indirect
	github.com/mitchellh/colorstring v0.0.0-20190213212951-d06e56a500db // indirect
	github.com/moby/term v0.5.0 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
	github.com/opencontainers/go-digest v1.0.0 // indirect
	github.com/opencontainers/image-spec v1.0.2 // indirect
	github.com/opencontainers/runc v1.1.14 // indirect
	github.com/pmezard/go-difflib v1.0.1-0.20181226105442-5d4384ee4fb2 // indirect
	github.com/prometheus/client_model v0.6.1 // indirect
	github.com/prometheus/common v0.44.0 // indirect
	github.com/prometheus/procfs v0.11.1 // indirect
	github.com/rivo/uniseg v0.4.7 // indirect
	github.com/spf13/afero v1.12.0 // indirect
	github.com/subosito/gotenv v1.6.0 // indirect
	github.com/xeipuuv/gojsonpointer v0.0.0-20190905194746-02993c407bfb // indirect
	github.com/xeipuuv/gojsonreference v0.0.0-20180127040603-bd5ef7bd5415 // indirect
	github.com/xeipuuv/gojsonschema v1.2.0 // indirect
	go.opencensus.io v0.24.0 // indirect
	go.uber.org/atomic v1.11.0
	golang.org/x/exp v0.0.0-20240719175910-8a7402abbf56 // indirect
	golang.org/x/mod v0.25.0 // indirect
	golang.org/x/net v0.41.0
	golang.org/x/sys v0.33.0 // indirect
	golang.org/x/text v0.26.0 // indirect
	golang.org/x/tools v0.33.0
	golang.org/x/xerrors v0.0.0-20240903120638-7835f813f4da // indirect
	google.golang.org/genproto v0.0.0-20250505200425-f936aa4a68b2 // indirect
	google.golang.org/grpc v1.72.1 // indirect
	gopkg.in/ini.v1 v1.67.0 // indirect
	gopkg.in/yaml.v2 v2.4.0 // indirect
)

replace github.com/csimplestring/delta-go => github.com/treeverse/delta-go v0.0.0-20250325160917-8c0ebb032f43
