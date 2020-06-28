package onboard

import (
	"context"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"testing"
)

func TestFetchManifest(t *testing.T) {
	sess := session.Must(session.NewSession(aws.NewConfig().WithRegion("us-east-1")))
	sess.ClientConfig(s3.ServiceName)
	svc := s3.New(sess)
	chn, err := fetchManifest(context.Background(), svc, "s3://yoni-test3/inventory/lakefs-example-data/my_inventory/2020-06-24T00-00Z/manifest.json")
	if err != nil {
		t.Fatal("failed")
	}
	for r := range chn {
		print(r.Key)
	}
}
