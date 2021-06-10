package retention

import (
	"testing"

	"github.com/treeverse/lakefs/pkg/graveler"
)

func Test(t *testing.T) {
	//awsConfig := &aws.Config{Region: swag.String("us-east-1")}
	//sess, err := session.NewSession(awsConfig)
	//if err != nil {
	//	panic(err)
	//}
	//client := s3.New(sess, awsConfig)
	w := NewCommitSetWriter(nil)
	w.Write(&Commits{
		Expired: map[graveler.CommitID]bool{"a": true, "b": true},
		Active:  map[graveler.CommitID]bool{"c": true, "d": true},
	}, nil)
}
