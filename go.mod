module github.com/treeverse/lakefs

go 1.16

require (
	cloud.google.com/go v0.88.0
	cloud.google.com/go/storage v1.14.0
	github.com/Azure/azure-pipeline-go v0.2.3
	github.com/Azure/azure-storage-blob-go v0.14.0
	github.com/Azure/go-autorest/autorest v0.11.18
	github.com/Azure/go-autorest/autorest/azure/auth v0.5.7
	github.com/DataDog/zstd v1.4.8 // indirect
	github.com/Masterminds/squirrel v1.5.0
	github.com/Microsoft/go-winio v0.5.1 // indirect
	github.com/apache/thrift v0.14.1
	github.com/aws/aws-sdk-go v1.37.26
	github.com/cockroachdb/errors v1.8.2 // indirect
	github.com/cockroachdb/pebble v0.0.0-20210308135031-3c37882d2ac8
	github.com/cockroachdb/redact v1.0.9 // indirect
	github.com/containerd/continuity v0.2.1 // indirect
	github.com/cubewise-code/go-mime v0.0.0-20200519001935-8c5762b177d8
	github.com/cznic/mathutil v0.0.0-20180504122225-ca4c9f2c1369
	github.com/davecgh/go-spew v1.1.1
	github.com/deepmap/oapi-codegen v1.5.6
	github.com/dgraph-io/ristretto v0.0.4-0.20210108140656-b1486d8516f2
	github.com/dgryski/go-gk v0.0.0-20200319235926-a69029f61654 // indirect
	github.com/dlmiddlecote/sqlstats v1.0.2
	github.com/docker/cli v20.10.10+incompatible // indirect
	github.com/docker/docker v20.10.10+incompatible // indirect
	github.com/fsnotify/fsnotify v1.4.9
	github.com/georgysavva/scany v0.2.7
	github.com/getkin/kin-openapi v0.53.0
	github.com/go-chi/chi/v5 v5.0.0
	github.com/go-ldap/ldap/v3 v3.4.1
	github.com/go-openapi/swag v0.19.14
	github.com/go-test/deep v1.0.7
	github.com/gobwas/glob v0.2.3
	github.com/golang-jwt/jwt v3.2.2+incompatible
	github.com/golang-migrate/migrate/v4 v4.15.1
	github.com/golang/mock v1.6.0
	github.com/golang/protobuf v1.5.2
	github.com/golangci/golangci-lint v1.38.0
	github.com/google/uuid v1.3.0
	github.com/gopherjs/gopherjs v0.0.0-20200217142428-fce0ec30dd00 // indirect
	github.com/hashicorp/go-multierror v1.1.0
	github.com/hnlq715/golang-lru v0.3.0
	github.com/influxdata/tdigest v0.0.1 // indirect
	github.com/jackc/pgconn v1.8.0
	github.com/jackc/pgerrcode v0.0.0-20201024163028-a0d42d470451
	github.com/jackc/pgproto3/v2 v2.0.7
	github.com/jackc/pgx/v4 v4.10.1
	github.com/jamiealquiza/tachymeter v2.0.0+incompatible
	github.com/jedib0t/go-pretty/v6 v6.2.4
	github.com/johannesboyne/gofakes3 v0.0.0-20210217223559-02ffa763be97
	github.com/lunixbochs/vtclean v1.0.0 // indirect
	github.com/magiconair/properties v1.8.4 // indirect
	github.com/manifoldco/promptui v0.8.0
	github.com/matoous/go-nanoid/v2 v2.0.0
	github.com/minio/minio-go/v7 v7.0.13
	github.com/mitchellh/go-homedir v1.1.0
	github.com/mitchellh/mapstructure v1.4.2
	github.com/ory/dockertest/v3 v3.8.0
	github.com/pkg/errors v0.9.1
	github.com/prometheus/client_golang v1.9.0
	github.com/rakyll/statik v0.1.7
	github.com/rs/xid v1.2.1
	github.com/schollz/progressbar/v3 v3.7.4
	github.com/scritchley/orc v0.0.0-20201124122313-cba38e582ef9
	github.com/sirupsen/logrus v1.8.1
	github.com/smartystreets/assertions v1.1.1 // indirect
	github.com/spf13/afero v1.4.1 // indirect
	github.com/spf13/cast v1.3.1
	github.com/spf13/cobra v1.1.3
	github.com/spf13/jwalterweatherman v1.1.0 // indirect
	github.com/spf13/pflag v1.0.5
	github.com/spf13/viper v1.7.1
	github.com/stretchr/testify v1.7.0
	github.com/thanhpk/randstr v1.0.4
	github.com/tsenart/vegeta/v12 v12.8.4
	github.com/vbauerster/mpb/v5 v5.4.0
	github.com/xeipuuv/gojsonpointer v0.0.0-20190905194746-02993c407bfb // indirect
	github.com/xitongsys/parquet-go v1.6.0
	github.com/xitongsys/parquet-go-source v0.0.0-20201108113611-f372b7d813be
	golang.org/x/crypto v0.0.0-20210921155107-089bfa567519
	golang.org/x/exp v0.0.0-20210220032938-85be41e4509f // indirect
	golang.org/x/net v0.0.0-20211101193420-4a448f8816b3 // indirect
	golang.org/x/oauth2 v0.0.0-20210628180205-a41e5a781914
	golang.org/x/sys v0.0.0-20211102061401-a2f17f7b995c // indirect
	golang.org/x/term v0.0.0-20210220032956-6a3ed077a48d
	google.golang.org/api v0.51.0
	google.golang.org/protobuf v1.27.1
	gopkg.in/ini.v1 v1.62.0 // indirect
	gopkg.in/natefinch/lumberjack.v2 v2.0.0
	gopkg.in/retry.v1 v1.0.3
	gopkg.in/yaml.v3 v3.0.0-20210107192922-496545a6307b
	pgregory.net/rapid v0.4.0 // indirect
)
