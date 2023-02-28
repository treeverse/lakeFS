package azure_test

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/benburkert/dns"
	"github.com/ory/dockertest/v3"
)

const (
	azuriteContainerTimeoutSeconds = 10 * 60 // 10 min
	containerName                  = "container1"
	accountName                    = "account1"
	accountKey                     = "key1"
)

var (
	pool     *dockertest.Pool
	blockURL string
)

func createDNSResolver(domain string) {
	zone := dns.Zone{
		Origin: domain + ".",
		TTL:    5 * time.Minute,
		RRs: dns.RRSet{
			accountName: map[dns.Type][]dns.Record{
				dns.TypeA: {
					&dns.A{A: net.IPv4(127, 0, 0, 1).To4()},
				},
			},
		},
	}

	mux := new(dns.ResolveMux)
	mux.Handle(dns.TypeANY, zone.Origin, &zone)
	client := &dns.Client{
		Resolver: mux,
	}
	net.DefaultResolver = &net.Resolver{
		PreferGo: true,
		Dial:     client.Dial,
	}
}

func runAzurite(dockerPool *dockertest.Pool) (string, func()) {
	ctx := context.Background()
	resource, err := dockerPool.Run("mcr.microsoft.com/azure-storage/azurite", "3.22.0", []string{
		fmt.Sprintf("AZURITE_ACCOUNTS=%s:%s", accountName, accountKey),
	})

	domain := "azurite.test" // TLD for test
	accountHost := accountName + "." + domain

	createDNSResolver(domain)

	// set cleanup
	closer := func() {
		err := dockerPool.Purge(resource)
		if err != nil {
			panic("could not kill postgres containerName")
		}
	}

	// expire, just to make sure
	err = resource.Expire(azuriteContainerTimeoutSeconds)
	if err != nil {
		panic("could not expire Azurite container")
	}

	// create connection and test container
	port := resource.GetPort("10000/tcp")
	url := fmt.Sprintf("http://%s:%s", accountHost, port)
	cred, err := azblob.NewSharedKeyCredential(accountName, accountKey)
	if err != nil {
		panic(err)
	}

	blob, err := azblob.NewClientWithSharedKeyCredential(url, cred, nil)
	if err != nil {
		panic(err)
	}
	_, err = blob.CreateContainer(ctx, containerName, nil)
	if err != nil {
		panic(err)
	}

	// return container URL
	return url, closer
}

func TestMain(m *testing.M) {
	var err error
	pool, err = dockertest.NewPool("")
	if err != nil {
		log.Fatalf("Could not connect to Docker: %s", err)
	}
	var cleanup func()
	blockURL, cleanup = runAzurite(pool)
	code := m.Run()
	cleanup()
	os.Exit(code)
}
