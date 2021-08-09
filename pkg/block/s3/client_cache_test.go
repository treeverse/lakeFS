package s3_test

//
//import (
//	"context"
//	"testing"
//
//	"github.com/aws/aws-sdk-go/aws/client/metadata"
//
//	"github.com/aws/aws-sdk-go/aws/request"
//	awss3 "github.com/aws/aws-sdk-go/service/s3"
//
//	"github.com/aws/aws-sdk-go/aws"
//	"github.com/aws/aws-sdk-go/aws/client"
//	"github.com/aws/aws-sdk-go/aws/session"
//	"github.com/aws/aws-sdk-go/service/s3/s3iface"
//	s3 "github.com/treeverse/lakefs/pkg/block/s3"
//	"github.com/treeverse/lakefs/pkg/testutil"
//)
//
//type s3ClientMock struct {
//	s3iface.S3API
//	headCalls int
//}
//
//func (s *s3ClientMock) HeadBucketRequest(*awss3.HeadBucketInput) (*request.Request, *awss3.HeadBucketOutput) {
//	s.headCalls++
//	return request.New(aws.Config{}, metadata.ClientInfo{}, request.Handlers{Send: request.HandlerList{}, nil, &request.Operation{}, nil, nil), nil
//}
//
//func TestCache(t *testing.T) {
//	sess, err := session.NewSession() // this session will be discarded
//	testutil.Must(t, err)
//	c := s3.NewClientCache(sess)
//	s3Mock := &s3ClientMock{}
//	c = c.WithClientFactory(func(awsSession client.ConfigProvider, cfgs ...*aws.Config) s3iface.S3API {
//		return s3Mock
//	})
//	c.Get(context.Background(), "boo")
//
//}
