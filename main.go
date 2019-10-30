package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/http/httputil"
	"os"
	"time"
	"versio-index/auth"
	"versio-index/auth/model"
	"versio-index/block"
	"versio-index/gateway"
	"versio-index/gateway/permissions"
	"versio-index/index"
	"versio-index/index/store"

	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/aws/aws-sdk-go/aws/session"
	v4 "github.com/aws/aws-sdk-go/aws/signer/v4"
)

func headBucket() {
	sess := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}))
	signer := v4.NewSigner(sess.Config.Credentials)
	req, _ := http.NewRequest("HEAD", "http://foobar.s3.local:8000/", nil)
	_, err := signer.Sign(req, nil, "s3", "us-east-1", time.Now())
	if err != nil {
		panic(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		panic(err)
	}
	dump, err := httputil.DumpResponse(resp, true)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("%s", dump)
}

func listBucket() {
	sess := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}))

	signer := v4.NewSigner(sess.Config.Credentials)
	//req, _ := http.NewRequest("GET", "http://foobar.s3.local:8000/", nil)
	req, _ := http.NewRequest("GET", "https://shmeps123123123.s3.amazonaws.com/?list-type=2&delimiter=/&prefix=photos/", nil)
	_, err := signer.Sign(req, nil, "s3", "us-west-2", time.Now())
	if err != nil {
		panic(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		panic(err)
	}
	//dump, err := httputil.DumpResponse(resp, true)
	//if err != nil {
	//	log.Fatal(err)
	//}
	d, _ := ioutil.ReadAll(resp.Body)
	os.Stderr.WriteString(fmt.Sprintf("status code: %d\n", resp.StatusCode))
	fmt.Printf("%s", d)
}

func Run() {
	// init fdb
	fdb.MustAPIVersion(600)
	db := fdb.MustOpenDefault()
	authdir, err := directory.CreateOrOpen(db, []string{"auth"}, nil)
	if err != nil {
		panic(err)
	}
	indexdir, err := directory.CreateOrOpen(db, []string{"index"}, nil)
	if err != nil {
		panic(err)
	}

	// init index
	meta := index.NewKVIndex(store.NewKVStore(db, indexdir))

	// init block store
	blockStore, err := block.NewLocalFSAdapter("/tmp/blocks")
	if err != nil {
		panic(err)
	}

	// init authentication
	authService := auth.NewKVAuthService(db, authdir)

	// init gateway server
	server := gateway.NewServer("us-east-1", meta, blockStore, authService, "0.0.0.0:8000", "s3.local:8000")
	panic(server.Listen())
}

func createCreds() {
	// init fdb
	fdb.MustAPIVersion(600)
	db := fdb.MustOpenDefault()
	dir, err := directory.CreateOrOpen(db, []string{"auth"}, nil)
	if err != nil {
		panic(err)
	}

	// init auth
	authService := auth.NewKVAuthService(db, dir)
	err = authService.CreateClient(&model.Client{
		Id:   "examplecid",
		Name: "exampleuid",
	})
	if err != nil {
		panic(err)
	}
	err = authService.CreateUser(&model.User{
		ClientId: "examplecid",
		Id:       "exampleuid",
		Email:    "ozkatz100@gmail.com",
		FullName: "Oz Katz",
	})
	if err != nil {
		panic(err)
	}

	err = authService.CreateRole(&model.Role{
		ClientId: "examplecid",
		Id:       "examplerid",
		Name:     "AdminRole",
		Policies: []*model.Policy{
			{
				Permission: permissions.PermissionManageRepos,
				Arn:        "arn:versio:repos:::*",
			},
			{
				Permission: permissions.PermissionReadRepo,
				Arn:        "arn:versio:repos:::*",
			},
			{
				Permission: permissions.PermissionWriteRepo,
				Arn:        "arn:versio:repos:::*",
			},
		},
	})
	if err != nil {
		panic(err)
	}

	err = authService.AssignRoleToUser("examplecid", "examplerid", "exampleuid")
	if err != nil {
		panic(err)
	}

	creds, err := authService.CreateUserCredentials(&model.User{
		ClientId: "examplecid",
		Id:       "exampleuid",
	})
	if err != nil {
		panic(err)
	}
	fmt.Printf("creds:\naccess: %s\nsecret: %s\n", creds.GetAccessKeyId(), creds.GetAccessSecretKey())
}

func getuser() {
	fdb.MustAPIVersion(600)
	db := fdb.MustOpenDefault()
	dir, err := directory.CreateOrOpen(db, []string{"auth"}, nil)
	if err != nil {
		panic(err)
	}

	// init auth
	authService := auth.NewKVAuthService(db, dir)
	user, err := authService.GetUser("examplecid", "exampleuid")
	if err != nil {
		panic(err)
	}
	fmt.Printf("user: %+v\n", user)
}

func main() {
	switch os.Args[1] {
	case "head":
		headBucket()
	case "list":
		listBucket()
	case "run":
		Run()
	case "user":
		getuser()
	case "creds":
		createCreds()
	}
}
