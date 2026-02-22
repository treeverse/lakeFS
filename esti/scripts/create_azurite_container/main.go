package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
)

func envOrDefault(key, defaultVal string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return defaultVal
}

func main() {
	endpoint := envOrDefault("AZURITE_ENDPOINT", "http://account1.azurite.test:10000")
	account := envOrDefault("AZURITE_ACCOUNT", "account1")
	key := envOrDefault("AZURITE_KEY", "key1")
	container := envOrDefault("AZURITE_CONTAINER", "esti-system-testing")

	cred, err := azblob.NewSharedKeyCredential(account, key)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create credential: %v\n", err)
		os.Exit(1)
	}

	client, err := azblob.NewClientWithSharedKeyCredential(endpoint, cred, nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create client: %v\n", err)
		os.Exit(1)
	}

	// Retry until Azurite is ready
	for i := 0; i < 30; i++ {
		_, err = client.CreateContainer(context.Background(), container, nil)
		if err == nil {
			fmt.Printf("Created container %q in Azurite at %s\n", container, endpoint)
			return
		}
		fmt.Fprintf(os.Stderr, "Attempt %d: waiting for Azurite: %v\n", i+1, err)
		time.Sleep(time.Second)
	}
	fmt.Fprintf(os.Stderr, "Failed to create container after 30 attempts: %v\n", err)
	os.Exit(1)
}
