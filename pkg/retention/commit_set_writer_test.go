package retention

import (
	"testing"

	"github.com/go-openapi/swag"

	"github.com/treeverse/lakefs/pkg/graveler"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"

	"github.com/aws/aws-sdk-go/service/s3"
)

func Test(t *testing.T) {
	awsConfig := &aws.Config{Region: swag.String("us-east-1")}
	sess, err := session.NewSession(awsConfig)
	if err != nil {
		panic(err)
	}
	client := s3.New(sess, awsConfig)
	w := NewCommitSetWriter("yoni-test3", "retention_test/1", client)
	w.Write(&Commits{
		Expired: map[graveler.CommitID]bool{"a": true, "b": true},
		Active:  map[graveler.CommitID]bool{"c": true, "d": true},
	})
}
