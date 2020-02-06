package main

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/treeverse/lakefs/block"
	"os"
	"os/user"
	"path"
	"runtime"
	"strings"

	"github.com/treeverse/lakefs/auth"
	"github.com/treeverse/lakefs/auth/model"
	db2 "github.com/treeverse/lakefs/db"
	"github.com/treeverse/lakefs/gateway"
	"github.com/treeverse/lakefs/gateway/permissions"
	"github.com/treeverse/lakefs/index"
	"github.com/treeverse/lakefs/index/store"

	log "github.com/sirupsen/logrus"

	"github.com/dgraph-io/badger"
)

const (
	ModuleName           = "github.com/treeverse/lakefs"
	ProjectDirectoryName = "lakefs"

	EnvVarS3AccessKeyId = "AWS_ADAPTER_ACCESS_KEY_ID"
	EnvVarS3SecretKey   = "AWS_ADAPTER_SECRET_ACCESS_KEY"
	EnvVarS3Region      = "AWS_ADAPTER_REGION"
	EnvVarS3Token       = "AWS_ADAPTER_TOKEN"
)

var (
	DefaultBlockLocation    = path.Join(home(), "tv_state", "blocks")
	DefaultMetadataLocation = path.Join(home(), "tv_state", "kv")
)

func setupLogger() {
	// logger
	log.SetReportCaller(true)
	log.SetFormatter(&log.TextFormatter{
		ForceColors:   true,
		FullTimestamp: true,
		CallerPrettyfier: func(frame *runtime.Frame) (function string, file string) {
			// file relative to "lakefs"
			indexOfModule := strings.Index(strings.ToLower(frame.File), ProjectDirectoryName)
			if indexOfModule != -1 {
				file = frame.File[indexOfModule+len(ProjectDirectoryName):]
			} else {
				file = frame.File
			}
			file = fmt.Sprintf("%s:%d", strings.TrimPrefix(file, string(os.PathSeparator)), frame.Line)
			function = strings.TrimPrefix(frame.Function, fmt.Sprintf("%s%s", ModuleName, string(os.PathSeparator)))
			return
		},
	})
	log.SetOutput(os.Stdout)
	log.SetLevel(log.TraceLevel) // for now
}

func setupBadger() (*badger.DB, error) {
	opts := badger.DefaultOptions(DefaultMetadataLocation).
		WithTruncate(true).
		WithLogger(db2.NewBadgerLoggingAdapter(log.WithField("subsystem", "badger")))
	return badger.Open(opts)
}

func setUpS3Adapter() (block.Adapter, error) {
	sess := session.Must(session.NewSession(&aws.Config{
		Region:      aws.String(os.Getenv(EnvVarS3Region)),
		Credentials: credentials.NewStaticCredentials(os.Getenv(EnvVarS3AccessKeyId), os.Getenv(EnvVarS3SecretKey), os.Getenv(EnvVarS3Token))}))
	svc := s3.New(sess)
	return block.NewS3Adapter(svc)
}
func home() string {
	u, err := user.Current()
	if err != nil {
		panic(err)
	}
	return u.HomeDir
}

func createCreds() {
	// init db
	setupLogger()
	db, err := setupBadger()
	if err != nil {
		panic(err)
	}
	// init auth
	authService := auth.NewKVAuthService(db)
	err = authService.CreateUser(&model.User{
		Id:       "exampleuid",
		Email:    "ozkatz100@gmail.com",
		FullName: "Oz Katz",
	})
	if err != nil {
		panic(err)
	}

	err = authService.CreateRole(&model.Role{
		Id:   "examplerid",
		Name: "AdminRole",
		Policies: []*model.Policy{
			{
				Permission: permissions.PermissionManageRepos,
				Arn:        "arn:treeverse:repos:::*",
			},
			{
				Permission: permissions.PermissionReadRepo,
				Arn:        "arn:treeverse:repos:::*",
			},
			{
				Permission: permissions.PermissionWriteRepo,
				Arn:        "arn:treeverse:repos:::*",
			},
		},
	})
	if err != nil {
		panic(err)
	}

	err = authService.AssignRoleToUser("examplerid", "exampleuid")
	if err != nil {
		panic(err)
	}

	creds, err := authService.CreateUserCredentials(&model.User{Id: "exampleuid"})
	if err != nil {
		panic(err)
	}

	fmt.Printf("creds:\naccess: %s\nsecret: %s\n", creds.GetAccessKeyId(), creds.GetAccessSecretKey())
}

func Run() {
	setupLogger()

	db, err := setupBadger()
	if err != nil {
		panic(err)
	}

	// init index
	meta := index.NewKVIndex(store.NewKVStore(db))

	// init mpu manager
	mpu := index.NewKVMultipartManager(store.NewKVStore(db))

	// init block store
	blockStore, err := block.NewLocalFSAdapter(DefaultBlockLocation)
	//blockStore, err := setUpS3Adapter()
	if err != nil {
		panic(err)
	}

	// init authentication
	authService := auth.NewKVAuthService(db)

	// init gateway server
	server := gateway.NewServer("us-east-1", meta, blockStore, authService, mpu, "0.0.0.0:8000", "s3.local:8000")
	panic(server.Listen())
}

func keys() {
	// init db
	// todo: add .WithTruncate(true), like in other places
	setupLogger()
	db, err := setupBadger()
	if err != nil {
		panic(err)
	}
	err = db.View(func(tx *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		iter := tx.NewIterator(opts)
		defer iter.Close()
		for iter.Rewind(); iter.Valid(); iter.Next() {
			item := iter.Item()
			key := item.Key()
			k := db2.KeyFromBytes(key)
			fmt.Printf("%s\n", k)
		}
		return nil
	})
	if err != nil {
		panic(err)
	}
	err = db.Close()
	if err != nil {
		panic(err)
	}
}

func tree(repoId, branch string) {
	// logger
	setupLogger()
	db, err := setupBadger()
	if err != nil {
		panic(err)
	}

	// init index
	meta := index.NewKVIndex(store.NewKVStore(db))
	err = meta.Tree(repoId, branch)
	if err != nil {
		panic(err)
	}
}

func main() {
	switch os.Args[1] {
	case "run":
		Run()
	case "creds":
		createCreds()
	case "keys":
		keys()
	case "tree":
		tree(os.Args[2], os.Args[3])
	}
}
