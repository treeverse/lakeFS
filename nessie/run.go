package nessie

import (
	"context"
	"github.com/go-openapi/runtime"
	"github.com/go-openapi/strfmt"
	"github.com/treeverse/lakefs/api/gen/client"
)

// Config contains configuration needed for running the tests
type Config struct {
	// BaseURL is the base address of the lakeFS endpoint
	BaseURL string
}

//
type testCase func(context.Context) error

// Run runs system tests and reports on failures
func Run(ctx context.Context, config Config) error {
	// initialize the env/repo
	client := client.NewHTTPClientWithConfig(strfmt.Default, &client.TransportConfig{
		Host:     config.BaseURL,
		BasePath: "",
		Schemes:  nil,
	})

	// run tests one by one

	// aggregate and return the errors

	return nil
}
