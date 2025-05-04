package main

import (
	"context"
	"fmt"
	"github.com/deepmap/oapi-codegen/pkg/securityprovider"
	"github.com/treeverse/lakefs/pkg/api/apigen"
	"log"
	"time"
)

func main() {
	ctx := context.Background()

	baseURL := "https://<org>.<region>.lakefscloud.io/api/v1" // e.g. https://lakefs.example.com
	accessKeyID := ""
	secretAccessKey := ""
	repo := "<repo>"
	startTime := time.Date(2025, time.April, 1, 0, 0, 0, 0, time.UTC)
	endTime := time.Date(2025, time.May, 1, 0, 0, 0, 0, time.UTC)

	basicAuthProvider, err := securityprovider.NewSecurityProviderBasicAuth(accessKeyID, secretAccessKey)
	if err != nil {
		panic(err)
	}

	client, err := apigen.NewClientWithResponses(baseURL, apigen.WithRequestEditorFn(basicAuthProvider.Intercept))
	if err != nil {
		panic(err)
	}

	amount := apigen.PaginationAmount(1000)
	after := apigen.PaginationAfter("")
	count := 0

	for {
		resp, err := client.ListRepositoryRunsWithResponse(ctx, repo, &apigen.ListRepositoryRunsParams{
			Amount: &amount,
			After:  &after,
		})
		if err != nil {
			log.Fatalf("API call failed: %v", err)
		}
		if resp.JSON200 == nil {
			log.Fatalf("Unexpected response: %v", resp.StatusCode())
		}

		for _, run := range resp.JSON200.Results {
			if run.Status == "failed" && run.StartTime.After(startTime) && run.StartTime.Before(endTime) {
				count++
				fmt.Printf("%3d. RunID: %s | Status: %s | Time: %s\n",
					count, run.RunId, run.Status, run.StartTime)
			}
			after = apigen.PaginationAfter(run.RunId)
		}

		if !resp.JSON200.Pagination.HasMore {
			break
		}
	}
}
