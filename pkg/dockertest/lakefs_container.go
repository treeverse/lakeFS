// Package dockertest provides a wrapper for running a lakeFS container in
// integration tests.
package dockertest

import (
	"context"
	"fmt"
	"net/http"
	"testing"

	"github.com/treeverse/lakefs/pkg/api/apigen"
	"github.com/treeverse/lakefs/pkg/api/helpers"

	"github.com/deepmap/oapi-codegen/pkg/securityprovider"
	"github.com/ory/dockertest/v3"
)

const (
	DockerLakeFSRepository  = "treeverse/lakefs"
	ContainerTimeoutSeconds = 5 * 60
	username                = "test user"
)

// waitForHealth waits until GETting URL returns 200.
func waitForHealth(ctx context.Context, url string, pool *dockertest.Pool) error {
	return pool.Retry(func() error {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return err
		}
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			return err
		}
		if res != nil && res.Body != nil {
			defer res.Body.Close()
		}
		return helpers.HTTPResponseAsError(res)
	})
}

func New() *Build {
	return &Build{}
}

// Build holds parameters for building a lakeFS dockertest container.
type Build struct {
	Repository string
	// DockerTag is the docker tag to fetch.  It is required or New() will
	// fail.
	DockerTag string
	// LocalDirectory is the directory to mount into the container and
	// use for the lakeFS local block adapter.  By default it is a
	// temporary test directory.
	LocalDirectory string
	// Env sets the environment.
	Env []string
}

func (b *Build) WithRepository(repository string) *Build {
	b.Repository = repository
	return b
}

func (b *Build) WithTag(tag string) *Build {
	b.DockerTag = tag
	return b
}

func (b *Build) WithLocalDirectory(dir string) *Build {
	b.LocalDirectory = dir
	return b
}

func (b *Build) WithEnv(e ...string) *Build {
	b.Env = append(b.Env, e...)
	return b
}

// TODO(ariels): Add RunOptions?  Add HostConfigs?
func (b *Build) New(ctx context.Context, t testing.TB, pool *dockertest.Pool) *Container {
	t.Helper()
	if b.Repository == "" {
		b.Repository = DockerLakeFSRepository
	}
	if b.DockerTag == "" {
		t.Fatal("Tag not specified")
	}
	if b.LocalDirectory == "" {
		b.LocalDirectory = t.TempDir()
	}
	return newLakeFSContainer(ctx, b, t, pool)
}

// Container holds a lakeFS dockertest container.
type Container struct {
	Resource *dockertest.Resource

	// Client is an authenticated client attached to the Endpoint.
	Client apigen.ClientWithResponsesInterface
	// Endpoint is the endpoint to use to contact the container.  It
	// includes /api/v1.
	Endpoint string
	// LocalDirectory is the local directory that is mounted into the
	// container and used by the lakeFS local block adapter.
	LocalDirectory string
	// AccessKeyID is the access key for authenticating as the admin.
	AccessKeyID string
	// SecretAccessKey is the secret access key for authenticating as
	// the admin.
	SecretAccessKey string

	// Closer must be called at the end of the test.  Close() is a func
	// that calls it.
	Closer func() error
}

// Close calls Closer and must be called at the end of the test.
func (c *Container) Close() error {
	return c.Closer()
}

func newLakeFSContainer(ctx context.Context, b *Build, t testing.TB, pool *dockertest.Pool) *Container {
	var (
		ret Container
		err error
	)

	defer func() {
		if err != nil && ret.Resource != nil {
			closeErr := ret.Resource.Close()
			if closeErr != nil {
				t.Logf("While closing lakeFS container after error: %s", closeErr)
			}
		}
	}()

	env := make([]string, len(b.Env))
	copy(env, b.Env)
	env = append(env,
		"LAKEFS_STATS_ENABLED=0",
		"LAKEFS_AUTH_ENCRYPT_SECRET_KEY=shh...",
		"LAKEFS_BLOCKSTORE_TYPE=mem",
		"LAKEFS_DATABASE_TYPE=local",
	)

	ret.Resource, err = pool.RunWithOptions(&dockertest.RunOptions{
		Repository: b.Repository,
		Tag:        b.DockerTag,
		Env:        env,
		Cmd:        []string{"run"},
	})
	if err != nil {
		t.Fatalf("Failed to start lakeFS container: %s", err)
	}

	err = ret.Resource.Expire(ContainerTimeoutSeconds)
	if err != nil {
		t.Errorf("Set lakeFS container expiry to %d: %s (keep going, it's already running)", ContainerTimeoutSeconds, err)
	}

	ep := ret.Resource.GetHostPort("8000/tcp")

	ret.Endpoint = fmt.Sprintf("http://%s/api/v1", ep)
	ret.LocalDirectory = b.LocalDirectory

	err = waitForHealth(ctx, fmt.Sprintf("http://%s/_health", ep), pool)
	if err != nil {
		t.Fatalf("Unhealthy: %s", err)
	}

	// Setup to create access keys!
	client, err := apigen.NewClientWithResponses(ret.Endpoint)
	if err != nil {
		t.Fatalf("New client: %s", err)
	}
	creds, err := client.SetupWithResponse(ctx, apigen.SetupJSONRequestBody{Username: username})
	if err != nil {
		t.Fatalf("Setup: %s", err)
	}
	if err = helpers.ResponseAsError(creds); err != nil {
		t.Fatalf("Setup: %s", err)
	}
	ret.AccessKeyID = creds.JSON200.AccessKeyId
	ret.SecretAccessKey = creds.JSON200.SecretAccessKey

	// Build a client.
	basicAuthProvider, err := securityprovider.NewSecurityProviderBasicAuth(ret.AccessKeyID, ret.SecretAccessKey)
	if err != nil {
		t.Fatalf("Security provider: %s", err)
	}

	ret.Client, err = apigen.NewClientWithResponses(
		ret.Endpoint,
		apigen.WithRequestEditorFn(basicAuthProvider.Intercept),
	)
	if err != nil {
		t.Fatalf("NewClient: %s", err)
	}

	ret.Closer = func() error {
		return pool.Purge(ret.Resource)
	}

	return &ret
}
