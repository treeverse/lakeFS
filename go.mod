module github.com/treeverse/lakefs

go 1.15

require (
	cloud.google.com/go v0.63.0
	cloud.google.com/go/storage v1.10.0
	github.com/Masterminds/squirrel v1.4.0
	github.com/Microsoft/go-winio v0.4.16 // indirect
	github.com/apache/thrift v0.13.0
	github.com/avast/retry-go v2.6.1+incompatible
	github.com/aws/aws-sdk-go v1.34.0
	github.com/cenkalti/backoff/v3 v3.2.2 // indirect
	github.com/cockroachdb/pebble v0.0.0-20201130172119-f19faf8529d6
	github.com/containerd/containerd v1.3.6 // indirect
	github.com/containerd/continuity v0.0.0-20201208142359-180525291bb7 // indirect
	github.com/cznic/mathutil v0.0.0-20180504122225-ca4c9f2c1369
	github.com/davecgh/go-spew v1.1.1
	github.com/dgraph-io/ristretto v0.0.4-0.20201207174236-c72a155bcf05
	github.com/dgryski/go-gk v0.0.0-20200319235926-a69029f61654 // indirect
	github.com/dlmiddlecote/sqlstats v1.0.1
	github.com/georgysavva/scany v0.2.6
	github.com/go-openapi/errors v0.19.6
	github.com/go-openapi/loads v0.19.5
	github.com/go-openapi/runtime v0.19.20
	github.com/go-openapi/spec v0.19.9
	github.com/go-openapi/strfmt v0.19.5
	github.com/go-openapi/swag v0.19.9
	github.com/go-openapi/validate v0.19.10
	github.com/go-swagger/go-swagger v0.25.0
	github.com/go-test/deep v1.0.7
	github.com/gofrs/uuid v3.3.0+incompatible // indirect
	github.com/golang-migrate/migrate/v4 v4.12.2
	github.com/golang/mock v1.4.4
	github.com/golang/protobuf v1.4.2
	github.com/golangci/golangci-lint v1.30.0
	github.com/google/uuid v1.1.1
	github.com/gopherjs/gopherjs v0.0.0-20200217142428-fce0ec30dd00 // indirect
	github.com/hashicorp/go-multierror v1.1.0
	github.com/hnlq715/golang-lru v0.2.0
	github.com/influxdata/tdigest v0.0.1 // indirect
	github.com/jackc/pgconn v1.6.4
	github.com/jackc/pgerrcode v0.0.0-20190803225404-afa3381909a6
	github.com/jackc/pgproto3/v2 v2.0.4
	github.com/jackc/pgtype v1.4.2
	github.com/jackc/pgx/v4 v4.8.1
	github.com/jamiealquiza/tachymeter v2.0.0+incompatible
	github.com/jedib0t/go-pretty v4.3.0+incompatible
	github.com/jessevdk/go-flags v1.4.0
	github.com/johannesboyne/gofakes3 v0.0.0-20200716060623-6b2b4cb092cc
	github.com/klauspost/compress v1.10.10 // indirect
	github.com/lib/pq v1.8.0
	github.com/lunixbochs/vtclean v1.0.0 // indirect
	github.com/mailru/easyjson v0.7.2 // indirect
	github.com/manifoldco/promptui v0.7.0
	github.com/matoous/go-nanoid v1.4.1
	github.com/mitchellh/go-homedir v1.1.0
	github.com/mitchellh/mapstructure v1.3.3 // indirect
	github.com/mr-tron/base58 v1.2.0
	github.com/opencontainers/go-digest v1.0.0 // indirect
	github.com/ory/dockertest v3.3.5+incompatible // indirect
	github.com/ory/dockertest/v3 v3.6.0
	github.com/prometheus/client_golang v1.7.1
	github.com/prometheus/common v0.11.1 // indirect
	github.com/rakyll/statik v0.1.7
	github.com/rs/xid v1.2.1
	github.com/schollz/progressbar/v3 v3.3.4
	github.com/scritchley/orc v0.0.0-20200625081059-e6fcbf41b2c2
	github.com/shopspring/decimal v1.2.0 // indirect
	github.com/sirupsen/logrus v1.7.0
	github.com/smartystreets/assertions v1.1.1 // indirect
	github.com/spf13/afero v1.3.4 // indirect
	github.com/spf13/cast v1.3.1
	github.com/spf13/cobra v1.0.0
	github.com/spf13/pflag v1.0.5
	github.com/spf13/viper v1.7.1
	github.com/streadway/handy v0.0.0-20190108123426-d5acb3125c2a
	github.com/stretchr/testify v1.6.1
	github.com/thanhpk/randstr v1.0.4
	github.com/tidwall/pretty v1.0.1 // indirect
	github.com/tsenart/vegeta/v12 v12.8.3
	github.com/vbauerster/mpb/v5 v5.3.0
	github.com/xitongsys/parquet-go v1.5.2
	github.com/xitongsys/parquet-go-source v0.0.0-20200805105948-52b27ba08556
	go.mongodb.org/mongo-driver v1.4.0 // indirect
	golang.org/x/crypto v0.0.0-20200728195943-123391ffb6de
	golang.org/x/net v0.0.0-20201216054612-986b41b23924
	golang.org/x/oauth2 v0.0.0-20200107190931-bf48bf16ab8d
	golang.org/x/sys v0.0.0-20201214210602-f9fddec55a1e // indirect
	gonum.org/v1/netlib v0.0.0-20200603212716-16abd5ac5bc7 // indirect
	google.golang.org/api v0.30.0
	google.golang.org/genproto v0.0.0-20200815001618-f69a88009b70 // indirect
	google.golang.org/protobuf v1.25.0
	gopkg.in/airbrake/gobrake.v2 v2.0.9 // indirect
	gopkg.in/dgrijalva/jwt-go.v3 v3.2.0
	gopkg.in/gemnasium/logrus-airbrake-hook.v2 v2.1.2 // indirect
	pgregory.net/rapid v0.4.0 // indirect
)
