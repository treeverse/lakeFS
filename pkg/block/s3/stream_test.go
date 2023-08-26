package s3_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"

	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3a "github.com/treeverse/lakefs/pkg/block/s3"
)

func TestS3StreamingReader_Read(t *testing.T) {
	// generate test data
	cases := []struct {
		Name      string
		Input     []byte
		ChunkSize int
		Expected  []byte
		Delay     bool
	}{
		{
			Name:      "chunk5_data10",
			Input:     mustReadFile(t, "testdata/chunk5_data10.input"),
			ChunkSize: 5,
			Expected:  mustReadFile(t, "testdata/chunk5_data10.output"),
		},
		{
			Name:      "chunk250_data500",
			Input:     mustReadFile(t, "testdata/chunk250_data500.input"),
			ChunkSize: 250,
			Expected:  mustReadFile(t, "testdata/chunk250_data500.output"),
		},
		{
			Name:      "chunk250_data510",
			Input:     mustReadFile(t, "testdata/chunk250_data510.input"),
			ChunkSize: 250,
			Expected:  mustReadFile(t, "testdata/chunk250_data510.output"),
		},
		{
			Name:      "chunk600_data240",
			Input:     mustReadFile(t, "testdata/chunk600_data240.input"),
			ChunkSize: 250,
			Expected:  mustReadFile(t, "testdata/chunk600_data240.output"),
		},
		{
			Name:      "chunk3000_data10",
			Input:     mustReadFile(t, "testdata/chunk3000_data10.input"),
			ChunkSize: 250,
			Expected:  mustReadFile(t, "testdata/chunk3000_data10.output"),
		},
		{
			Name:      "chunk5_data0",
			Input:     mustReadFile(t, "testdata/chunk5_data0.input"),
			ChunkSize: 5,
			Expected:  mustReadFile(t, "testdata/chunk5_data0.output"),
		},
		{
			Name:      "delayed_chunk5_data0",
			Input:     mustReadFile(t, "testdata/chunk5_data0.input"),
			ChunkSize: 5,
			Expected:  mustReadFile(t, "testdata/chunk5_data0.output"),
			Delay:     true,
		},
	}

	ctx := context.Background()
	creds := aws.Credentials{
		AccessKeyID:     "AKIAIOSFODNN7EXAMPLE",
		SecretAccessKey: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
	}

	for _, cas := range cases {
		t.Run(cas.Name, func(t *testing.T) {
			// this is just boilerplate to create a signature
			sigTime, _ := time.Parse("Jan 2 15:04:05 2006 -0700", "Apr 7 15:13:13 2005 -0700")
			req, _ := http.NewRequest(http.MethodPut, "https://s3.amazonaws.com/example/foo", nil)
			req.Header.Set("Content-Encoding", "aws-chunked")
			req.Header.Set("Transfer-Encoding", "chunked")
			req.Header.Set("x-amz-content-sha", "STREAMING-AWS4-HMAC-SHA256-PAYLOAD")
			req.Header.Set("x-amz-decoded-content-length", fmt.Sprintf("%d", len(cas.Input)))
			req.Header.Set("Expect", "100-Continue")

			signer := v4.NewSigner()
			err := signer.SignHTTP(ctx, creds, req, "", s3.ServiceID, "us-east-1", sigTime)
			if err != nil {
				t.Fatal(err)
			}

			sigSeed, err := v4.GetSignedRequestSignature(req)
			if err != nil {
				t.Fatal(err)
			}

			r := io.NopCloser(bytes.NewBuffer(cas.Input))
			timeout := 300 * time.Second
			if cas.Delay {
				r = io.NopCloser(&delayReader{
					r:    bytes.NewBuffer(cas.Input),
					wait: 2 * time.Millisecond,
				})
				timeout = time.Millisecond
			}

			data := &s3a.StreamingReader{
				Reader:       r,
				Size:         int64(len(cas.Input)),
				StreamSigner: v4.NewStreamSigner(creds, s3.ServiceID, "us-east-1", sigSeed),
				Time:         sigTime,
				ChunkSize:    cas.ChunkSize,
				ChunkTimeout: timeout,
			}

			out, err := io.ReadAll(data)
			if err != nil {
				t.Fatal(err)
			}

			if !bytes.Equal(out, cas.Expected) {
				t.Fatalf("got wrong chunked data. Got:\n%s\nExpected:\n%s\n", out, cas.Expected)
			}
		})
	}
}

func mustReadFile(t *testing.T, path string) []byte {
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

type delayReader struct {
	r    io.Reader
	wait time.Duration
}

func (d *delayReader) Read(p []byte) (n int, err error) {
	time.Sleep(d.wait)
	return d.r.Read(p)
}
