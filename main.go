package main

import (
	"fmt"
	"log"
	"net/http"
	"net/http/httputil"
	"os"
	"time"
	"versio-index/gateway"
	"versio-index/index"
	"versio-index/index/store"

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

func Run() {
	fdb.MustAPIVersion(600)
	str := store.NewKVStore(fdb.MustOpenDefault())
	ind := index.NewKVIndex(str)
	server := gateway.NewServer(ind, "0.0.0.0:8000", "s3.local:8000")
	panic(server.Listen())
}

func main() {
	switch os.Args[1] {
	case "head":
		headBucket()
	case "run":
		Run()
	}
}
