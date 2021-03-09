module github.com/treeverse/lakefs

go 1.16

require (
	cloud.google.com/go v0.78.0
	cloud.google.com/go/storage v1.14.0
	github.com/Azure/azure-pipeline-go v0.2.3
	github.com/Azure/azure-storage-blob-go v0.13.0
	github.com/Azure/go-autorest/autorest v0.11.18
	github.com/Azure/go-autorest/autorest/adal v0.9.13
	github.com/Azure/go-autorest/autorest/azure/auth v0.5.7
	github.com/DataDog/zstd v1.4.8 // indirect
	github.com/Masterminds/squirrel v1.5.0
	github.com/Microsoft/go-winio v0.4.16 // indirect
	github.com/apache/thrift v0.14.1
	github.com/avast/retry-go v2.6.1+incompatible
	github.com/aws/aws-sdk-go v1.37.26
	github.com/cenkalti/backoff/v3 v3.2.2 // indirect
	github.com/cockroachdb/errors v1.8.2 // indirect
	github.com/cockroachdb/pebble v0.0.0-20210308135031-3c37882d2ac8 // indirect
	github.com/cockroachdb/redact v1.0.9 // indirect
	github.com/containerd/continuity v0.0.0-20201208142359-180525291bb7 // indirect
	github.com/coreos/go-oidc v2.2.1+incompatible // indirect
	github.com/cznic/mathutil v0.0.0-20180504122225-ca4c9f2c1369
	github.com/davecgh/go-spew v1.1.1
	github.com/dgraph-io/ristretto v0.0.4-0.20210108140656-b1486d8516f2
	github.com/dgrijalva/jwt-go v3.2.0+incompatible
	github.com/dgryski/go-gk v0.0.0-20200319235926-a69029f61654 // indirect
	github.com/dlmiddlecote/sqlstats v1.0.2
	github.com/georgysavva/scany v0.2.7
	github.com/go-openapi/errors v0.20.0
	github.com/go-openapi/loads v0.20.2
	github.com/go-openapi/runtime v0.19.26
	github.com/go-openapi/spec v0.20.3
	github.com/go-openapi/strfmt v0.20.0
	github.com/go-openapi/swag v0.19.14
	github.com/go-openapi/validate v0.20.2
	github.com/go-swagger/go-swagger v0.26.1
	github.com/go-test/deep v1.0.7
	github.com/gofrs/uuid v3.3.0+incompatible // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang-migrate/migrate/v4 v4.14.1
	github.com/golang/mock v1.5.0
	github.com/golang/protobuf v1.4.3
	github.com/golang/snappy v0.0.3 // indirect
	github.com/golangci/errcheck v0.0.0-20181223084120-ef45e06d44b6 // indirect
	github.com/golangci/gocyclo v0.0.0-20180528144436-0a533e8fa43d // indirect
	github.com/golangci/golangci-lint v1.38.0
	github.com/golangci/ineffassign v0.0.0-20190609212857-42439a7714cc // indirect
	github.com/golangci/prealloc v0.0.0-20180630174525-215b22d4de21 // indirect
	github.com/google/uuid v1.2.0
	github.com/gopherjs/gopherjs v0.0.0-20200217142428-fce0ec30dd00 // indirect
	github.com/hashicorp/go-multierror v1.1.0
	github.com/hnlq715/golang-lru v0.3.0
	github.com/influxdata/tdigest v0.0.1 // indirect
	github.com/jackc/pgconn v1.8.0
	github.com/jackc/pgerrcode v0.0.0-20201024163028-a0d42d470451
	github.com/jackc/pgproto3/v2 v2.0.7
	github.com/jackc/pgx/v4 v4.10.1
	github.com/jamiealquiza/tachymeter v2.0.0+incompatible
	github.com/jedib0t/go-pretty v4.3.0+incompatible
	github.com/jessevdk/go-flags v1.4.0
	github.com/jmoiron/sqlx v1.2.1-0.20190826204134-d7d95172beb5 // indirect
	github.com/johannesboyne/gofakes3 v0.0.0-20210217223559-02ffa763be97
	github.com/klauspost/compress v1.11.12 // indirect
	github.com/kr/pretty v0.2.1 // indirect
	github.com/lib/pq v1.9.0 // indirect
	github.com/lunixbochs/vtclean v1.0.0 // indirect
	github.com/manifoldco/promptui v0.8.0
	github.com/matoous/go-nanoid v1.5.0
	github.com/mitchellh/go-homedir v1.1.0
	github.com/ory/dockertest/v3 v3.6.3
	github.com/prometheus/client_golang v1.9.0
	github.com/rakyll/statik v0.1.7
	github.com/rs/xid v1.2.1
	github.com/schollz/progressbar/v3 v3.7.4
	github.com/scritchley/orc v0.0.0-20201124122313-cba38e582ef9
	github.com/shirou/gopsutil v0.0.0-20190901111213-e4ec7b275ada // indirect
	github.com/shopspring/decimal v1.2.0 // indirect
	github.com/sirupsen/logrus v1.8.0
	github.com/smartystreets/assertions v1.1.1 // indirect
	github.com/spf13/cast v1.3.1
	github.com/spf13/cobra v1.1.3
	github.com/spf13/pflag v1.0.5
	github.com/spf13/viper v1.7.1
	github.com/stretchr/testify v1.7.0
	github.com/thanhpk/randstr v1.0.4
	github.com/tidwall/pretty v1.0.1 // indirect
	github.com/tommy-muehle/go-mnd v1.3.1-0.20201008215730-16041ac3fe65 // indirect
	github.com/tsenart/vegeta/v12 v12.8.4
	github.com/vbauerster/mpb/v5 v5.4.0
	github.com/xitongsys/parquet-go v1.6.0
	github.com/xitongsys/parquet-go-source v0.0.0-20201108113611-f372b7d813be
	golang.org/x/crypto v0.0.0-20210220033148-5ea612d1eb83
	golang.org/x/exp v0.0.0-20210220032938-85be41e4509f // indirect
	golang.org/x/mod v0.4.1 // indirect
	golang.org/x/net v0.0.0-20210226172049-e18ecbb05110
	golang.org/x/oauth2 v0.0.0-20210220000619-9bb904979d93
	golang.org/x/sys v0.0.0-20210308170721-88b6017d0656 // indirect
	golang.org/x/term v0.0.0-20210220032956-6a3ed077a48d
	golang.org/x/tools v0.1.0
	gonum.org/v1/netlib v0.0.0-20200603212716-16abd5ac5bc7 // indirect
	google.golang.org/api v0.40.0
	google.golang.org/protobuf v1.25.0
	gopkg.in/dgrijalva/jwt-go.v3 v3.2.0
	gopkg.in/yaml.v3 v3.0.0-20210107192922-496545a6307b
	pgregory.net/rapid v0.4.0 // indirect
)
